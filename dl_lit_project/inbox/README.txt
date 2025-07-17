PDF Processing Inbox
====================

Drop your PDF files here for automated processing!

How it works:
1. Place PDF files in this 'inbox' folder
2. Run one of these commands:

   # Process all PDFs once:
   python -m dl_lit.cli process-folder inbox/

   # Watch folder continuously (processes new PDFs automatically):
   python -m dl_lit.cli process-folder inbox/ --watch

3. Processed PDFs will be moved to:
   - ../completed/ (if successful)
   - ../failed/ (if errors occurred)

4. Downloaded papers will be saved to:
   - ../pdf_library/

Options:
--------
--no-fetch-references    Don't fetch referenced works
--fetch-citations        Also fetch works that cite the papers
--max-citations 50       Limit citations to fetch (default: 100)
--interval 10           Check for new files every 10 seconds (default: 5)

Example with options:
python -m dl_lit.cli process-folder inbox/ --watch --fetch-citations --max-citations 50

Note: Make sure you're in the virtual environment before running commands!