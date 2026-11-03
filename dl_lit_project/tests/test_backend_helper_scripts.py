import importlib.util
import json
from pathlib import Path

import pikepdf

SCRIPTS_DIR = Path(__file__).resolve().parents[2] / 'backend' / 'scripts'


def _load(monkeypatch, name):
    monkeypatch.syspath_prepend(str(SCRIPTS_DIR))
    spec = importlib.util.spec_from_file_location(f'{name}_script', SCRIPTS_DIR / f'{name}.py')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _make_pdf(path, pages=3):
    pdf = pikepdf.new()
    for _ in range(pages):
        pdf.add_blank_page()
    pdf.save(path)
    pdf.close()
    return path


class _Uploaded:
    def __init__(self, name):
        self.name = name


class _FakeFiles:
    def __init__(self):
        self.uploads = []
        self.deleted = []

    def upload(self, file, config):
        self.uploads.append({'file': str(file), 'display_name': config['display_name'], 'exists': Path(file).exists()})
        return _Uploaded('files/x')

    def delete(self, name):
        self.deleted.append(name)


class FakeClient:
    def __init__(self):
        self.files = _FakeFiles()


def test_pdf_page_count_prints_count(monkeypatch, tmp_path, capsys):
    mod = _load(monkeypatch, 'pdf_page_count')
    pdf = _make_pdf(tmp_path / 'three.pdf')
    mod.main([str(pdf)])
    assert capsys.readouterr().out.strip() == '3'


def test_llm_upload_pdf_prints_name_and_copies_non_ascii(monkeypatch, tmp_path, capsys):
    mod = _load(monkeypatch, 'llm_upload_pdf')
    ascii_pdf = _make_pdf(tmp_path / 'plain.pdf')
    client = FakeClient()
    mod.main([str(ascii_pdf)], client=client)
    assert json.loads(capsys.readouterr().out.strip()) == {'name': 'files/x'}
    assert client.files.uploads[0]['file'] == str(ascii_pdf)
    assert client.files.uploads[0]['display_name'] == 'plain.pdf'

    umlaut_pdf = _make_pdf(tmp_path / 'Übersicht Bücher.pdf')
    client = FakeClient()
    mod.main([str(umlaut_pdf)], client=client)
    assert json.loads(capsys.readouterr().out.strip()) == {'name': 'files/x'}
    upload = client.files.uploads[0]
    assert upload['display_name'].isascii()
    assert upload['display_name'] == 'Ubersicht_Bucher.pdf'
    assert upload['file'] != str(umlaut_pdf)
    assert upload['file'].isascii()
    assert upload['exists'] is True
    # The temporary ASCII copy is removed after the upload.
    assert not Path(upload['file']).exists()


def test_llm_delete_upload_deletes_by_name(monkeypatch, capsys):
    mod = _load(monkeypatch, 'llm_delete_upload')
    client = FakeClient()
    mod.main(['files/x'], client=client)
    assert client.files.deleted == ['files/x']
    assert capsys.readouterr().out.strip() == 'ok'
