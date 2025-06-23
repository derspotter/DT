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

    @patch('dl_lit.get_bib_pages.genai')
    @patch('dl_lit.get_bib_pages.pikepdf')
    @patch('dl_lit.get_bib_pages.os.makedirs')
    @patch('dl_lit.get_bib_pages.open', new_callable=unittest.mock.mock_open)
    def test_extract_reference_sections_small_pdf(self, mock_open, mock_makedirs, mock_pikepdf, mock_genai):
        """Test the core extraction logic for a small PDF that doesn't need chunking."""
        # Mock Gemini API response
        mock_model = MagicMock()
        mock_response = MagicMock()
        mock_response.text = json.dumps([{'start_page': 10, 'end_page': 15}])
        mock_model.generate_content.return_value = mock_response
        mock_genai.GenerativeModel.return_value = mock_model
        mock_genai.upload_file.return_value = MagicMock(name='uploaded_file')

        # Mock Pikepdf
        mock_pdf_doc_opened = MagicMock()
        mock_pdf_doc_opened.pages = [MagicMock()] * 20 # 20 pages
        mock_pikepdf.Pdf.open.return_value.__enter__.return_value = mock_pdf_doc_opened
        
        mock_pdf_doc_new = MagicMock()
        mock_pikepdf.Pdf.new.return_value = mock_pdf_doc_new

        extract_reference_sections('dummy.pdf', 'out')

        # Verify GenAI was called correctly
        mock_genai.upload_file.assert_called_once_with('dummy.pdf')
        mock_model.generate_content.assert_called_once()

        # Verify PDF was created
        mock_pdf_doc_new.save.assert_called_once()

    @patch('dl_lit.get_bib_pages.genai')
    @patch('dl_lit.get_bib_pages.pikepdf')
    @patch('dl_lit.get_bib_pages.tempfile.mkdtemp')
    @patch('dl_lit.get_bib_pages.os.makedirs')
    def test_extract_reference_sections_large_pdf_with_chunking(self, mock_makedirs, mock_mkdtemp, mock_pikepdf, mock_genai):
        """Test the core extraction logic for a large PDF that requires chunking."""
        mock_mkdtemp.return_value = '/tmp/dummy_dir'

        # Mock Gemini API response
        mock_model = MagicMock()
        mock_response = MagicMock()
        mock_response.text = json.dumps([{'start_page': 5, 'end_page': 10}])
        mock_model.generate_content.return_value = mock_response
        mock_genai.GenerativeModel.return_value = mock_model
        mock_genai.upload_file.return_value = MagicMock(name='uploaded_file')

        # Mock Pikepdf for a large PDF (e.g., 75 pages, CHUNK_SIZE=50)
        mock_pdf_doc_opened = MagicMock()
        mock_pdf_doc_opened.pages = [MagicMock()] * 75
        mock_pikepdf.Pdf.open.return_value.__enter__.return_value = mock_pdf_doc_opened

        mock_pdf_doc_new = MagicMock()
        mock_pikepdf.Pdf.new.return_value = mock_pdf_doc_new

        extract_reference_sections('large_dummy.pdf', 'out')

        # Verify chunking logic was triggered
        self.assertEqual(mock_genai.upload_file.call_count, 2) # Called for 2 chunks
        self.assertEqual(mock_model.generate_content.call_count, 2)

        # Check that the final PDF was saved twice (one for each extracted section)
        # This depends on how many sections are found in each chunk.
        # In this mock, one section is found per chunk.
        # Assert that the final extracted PDFs are saved correctly by checking the filenames.
        # This is more robust than checking the total save count, which includes temp files.
        final_save_calls = [
            c for c in mock_pdf_doc_new.save.call_args_list
            if c.args[0].startswith('out/')
        ]
        self.assertEqual(len(final_save_calls), 2)

        # Also check that the correct filenames were used
        saved_files = {c.args[0] for c in final_save_calls}
        expected_files = {
            'out/large_dummy_refs_physical_p5-10.pdf',
            'out/large_dummy_refs_physical_p55-60.pdf'
        }
        self.assertEqual(saved_files, expected_files)

if __name__ == '__main__':
    unittest.main()
