# Legacy Scripts And Artifacts

This directory exists for historical reasons. It contains legacy standalone scripts and runtime artifacts from earlier iterations of the project.

Canonical, maintained code lives in `dl_lit_project/dl_lit/` and should be used via:

- CLI: `python -m dl_lit.cli ...`
- Web backend: runs scripts from `dl_lit_project/dl_lit/` and stores outputs under `dl_lit_project/artifacts/`.

If you are adding features or fixing bugs, prefer changing code in `dl_lit_project/dl_lit/`.

