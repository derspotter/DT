# Development Notes

## Canonical Package
Use `dl_lit_project/dl_lit` as the authoritative Python package for all CLI and pipeline work.
The root-level `dl_lit/` directory is legacy and should not be imported or modified for Phase 1.

## Entry Points
- CLI module: `python -m dl_lit.cli`
- Tests target: `dl_lit_project/tests/`

## Library Notes
- `fitz` is the import name for **PyMuPDF** (same library, different module name).
