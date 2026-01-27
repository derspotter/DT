# Repository Guidelines

## Project Structure & Module Organization
- `backend/`: Node.js/Express API server (WebSocket + file processing). Entry: `backend/src/index.js`.
- `frontend/`: Vite + VanJS frontend. Entry: `frontend/index.html` with `frontend/script.js` and `frontend/style.css`.
- `dl_lit/` and `dl_lit_project/`: Python tooling for bibliography extraction, duplicate detection, and CLI workflows.
- `ocr/`, `output-dir/`, `logs/`: generated artifacts and runtime outputs; avoid committing large outputs unless required.
- Top-level `requirements.txt` and `dl_lit_project/requirements*.txt` capture Python dependencies.

## Build, Test, and Development Commands
Run commands from the relevant directory.
- Backend dev server: `cd backend && npm run dev` (nodemon auto-reload).
- Backend start: `cd backend && npm start` (production-style server).
- Backend tests: `cd backend && npm test` (Jest).
- Frontend dev server: `cd frontend && npm run dev` (Vite + hot reload).
- Frontend build/preview: `cd frontend && npm run build` / `cd frontend && npm run preview`.
- Python tests: `pytest dl_lit_project/tests` (uses pytest).

## Coding Style & Naming Conventions
- Follow existing file style: backend JS uses 2-space indentation; frontend JS/CSS uses 4-space indentation.
- Prefer ES modules (see `backend/src/index.js` and Vite config).
- Keep file and function names descriptive and consistent with current modules (e.g., `test_cli_*.py`).
- No repo-wide formatter config detected; if you introduce one, align with current indentation.

## Testing Guidelines
- Backend: Jest with `npm test` from `backend/`.
- Python: pytest in `dl_lit_project/tests/` (fixtures and CLI tests).
- Frontend: Vitest is installed but no npm script is defined; use `cd frontend && npx vitest` if adding unit tests.

## Commit & Pull Request Guidelines
- Commit history shows a mix of conventional commits (e.g., `feat:`) and short imperative messages. Prefer conventional commits when practical: `feat:`, `fix:`, `chore:`.
- PRs should include a clear description, testing notes (commands run), and screenshots for UI changes.
- Link related issues or tasks when applicable.

## Configuration & Security Notes
- Store secrets in `.env` files; `backend` already uses `dotenv`.
- Avoid committing generated PDFs, test artifacts, or large model outputs unless explicitly required.
