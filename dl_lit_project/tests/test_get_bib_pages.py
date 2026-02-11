import unittest
from unittest.mock import patch, MagicMock, call
import os
import json
import pikepdf
from click.testing import CliRunner

from dl_lit.cli import cli
from dl_lit.get_bib_pages import extract_reference_sections


class TestGetBibPages(unittest.TestCase):

    def setUp(self):
        self.runner = CliRunner()

    @patch('dl_lit.cli.extract_reference_sections')
    def test_cli_extract_bib_pages_single_file(self, mock_extract):
        """Test the CLI command with a single PDF file."""
        with self.runner.isolated_filesystem() as td:
            # Create a dummy PDF file and output directory
            dummy_pdf_path = os.path.join(td, 'dummy.pdf')
            output_dir_path = os.path.join(td, 'out')
            with open(dummy_pdf_path, 'w') as f:
                f.write('dummy content')

            result = self.runner.invoke(cli, ['extract-bib-pages', dummy_pdf_path, '--output-dir', output_dir_path])

            self.assertEqual(result.exit_code, 0, f"CLI exited with code {result.exit_code}\n{result.output}")
            mock_extract.assert_called_once_with(dummy_pdf_path, output_dir_path)

    @patch('dl_lit.cli.extract_reference_sections')
    def test_cli_extract_bib_pages_directory(self, mock_extract):
        """Test the CLI command with a directory input."""
        with self.runner.isolated_filesystem() as td:
            # Create a dummy directory structure with PDFs
            input_dir = os.path.join(td, 'test_dir')
            output_dir = os.path.join(td, 'out')
            os.makedirs(input_dir)
            pdf1_path = os.path.join(input_dir, 'dummy1.pdf')
            pdf2_path = os.path.join(input_dir, 'dummy2.pdf')
            with open(pdf1_path, 'w') as f: f.write('pdf1')
            with open(pdf2_path, 'w') as f: f.write('pdf2')

            result = self.runner.invoke(cli, ['extract-bib-pages', input_dir, '--output-dir', output_dir])

            self.assertEqual(result.exit_code, 0, f"CLI exited with code {result.exit_code}\n{result.output}")
            self.assertEqual(mock_extract.call_count, 2)
            mock_extract.assert_has_calls([
                call(pdf1_path, output_dir),
                call(pdf2_path, output_dir)
            ], any_order=True)

    @patch('dl_lit.get_bib_pages.api_client')
    @patch('dl_lit.get_bib_pages.pikepdf')
    @patch('dl_lit.get_bib_pages.os.makedirs')
    @patch('dl_lit.get_bib_pages.open', new_callable=unittest.mock.mock_open)
    def test_extract_reference_sections_small_pdf(self, mock_open, mock_makedirs, mock_pikepdf, mock_api_client):
        """Test the core extraction logic for a small PDF that doesn't need chunking."""
        # Mock Gemini API response
        mock_response = MagicMock()
        mock_response.text = json.dumps([{'start_page': 10, 'end_page': 15}])
        mock_api_client.models.generate_content.return_value = mock_response
        uploaded_file = MagicMock(name='uploaded_file')
        uploaded_file.name = 'files/test'
        mock_api_client.files.upload.return_value = uploaded_file

        # Mock Pikepdf
        mock_pdf_doc_opened = MagicMock()
        mock_pdf_doc_opened.pages = [MagicMock()] * 20 # 20 pages
        mock_pikepdf.Pdf.open.return_value.__enter__.return_value = mock_pdf_doc_opened
        
        mock_pdf_doc_new = MagicMock()
        mock_pikepdf.Pdf.new.return_value = mock_pdf_doc_new

        extract_reference_sections('dummy.pdf', 'out')

        # Verify GenAI was called correctly
        self.assertEqual(mock_api_client.files.upload.call_count, 1)
        mock_api_client.models.generate_content.assert_called_once()

        # Verify output pages were created (10-15 inclusive => 6 pages)
        out_calls = [
            c for c in mock_pdf_doc_new.save.call_args_list
            if '/dummy/' in str(c.args[0]).replace('\\', '/')
        ]
        self.assertEqual(len(out_calls), 6)

    @patch('dl_lit.get_bib_pages.api_client')
    @patch('dl_lit.get_bib_pages.pikepdf')
    @patch('dl_lit.get_bib_pages.tempfile.mkdtemp')
    @patch('dl_lit.get_bib_pages.os.makedirs')
    @patch('dl_lit.get_bib_pages.open', new_callable=unittest.mock.mock_open)
    def test_extract_reference_sections_large_pdf_with_chunking(self, mock_open, mock_makedirs, mock_mkdtemp, mock_pikepdf, mock_api_client):
        """Test the core extraction logic for a large PDF that requires chunking."""
        mock_mkdtemp.return_value = '/tmp/dummy_dir'

        # Mock Gemini API response
        mock_response = MagicMock()
        mock_response.text = json.dumps([{'start_page': 5, 'end_page': 10}])
        mock_api_client.models.generate_content.return_value = mock_response
        uploaded_file = MagicMock(name='uploaded_file')
        uploaded_file.name = 'files/test'
        mock_api_client.files.upload.return_value = uploaded_file

        # Mock Pikepdf for a large PDF (e.g., 75 pages, CHUNK_SIZE=50)
        mock_pdf_doc_opened = MagicMock()
        mock_pdf_doc_opened.pages = [MagicMock()] * 75
        mock_pikepdf.Pdf.open.return_value.__enter__.return_value = mock_pdf_doc_opened

        mock_pdf_doc_new = MagicMock()
        mock_pikepdf.Pdf.new.return_value = mock_pdf_doc_new

        extract_reference_sections('large_dummy.pdf', 'out')

        # Verify chunking logic was triggered
        self.assertEqual(mock_api_client.files.upload.call_count, 2) # Called for 2 chunks
        self.assertEqual(mock_api_client.models.generate_content.call_count, 2)

        # Check that the final PDFs were saved for each page in each section.
        final_save_calls = [
            c for c in mock_pdf_doc_new.save.call_args_list
            if '/large_dummy/' in str(c.args[0]).replace('\\', '/')
        ]
        self.assertEqual(len(final_save_calls), 12)

        # Also check that the correct filenames were used
        saved_files = {str(c.args[0]).replace('\\', '/') for c in final_save_calls}
        expected_suffixes = {f'large_dummy/large_dummy_refs_physical_p{n}.pdf' for n in range(5, 11)}
        expected_suffixes |= {f'large_dummy/large_dummy_refs_physical_p{n}.pdf' for n in range(55, 61)}
        for suffix in expected_suffixes:
            self.assertTrue(any(path.endswith(suffix) for path in saved_files), f"Missing expected file suffix: {suffix}")

if __name__ == '__main__':
    unittest.main()
