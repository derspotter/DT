#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

# --- Configuration ---
DB_PATH="/home/jay/DT/dl_lit_project/data/literature.db"
BIBTEX_FILE="/home/jay/DT/dl_lit_project/data/metadata.bib"
JSON_DIR="/home/jay/openalex_done"
PROJECT_DIR="/home/jay/DT/dl_lit_project"

# --- Colors for output ---
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# --- Test Execution ---
echo -e "${YELLOW}--- Starting Duplicate Detection Test ---${NC}"

# 1. Clean up previous database file for a fresh test
echo -e "\n${YELLOW}[Step 1/3] Deleting old database file...${NC}"
rm -f "$DB_PATH"
echo -e "${GREEN}Done.${NC}"

# 2. Import the initial set of references from the BibTeX file
echo -e "\n${YELLOW}[Step 2/3] Importing references from BibTeX file: $BIBTEX_FILE...${NC}"
python -m dl_lit.cli import-bib "$BIBTEX_FILE" --db-path "$DB_PATH"
echo -e "${GREEN}BibTeX import completed.${NC}"

# 3. Attempt to add references from the JSON directory (expecting duplicates)
echo -e "\n${YELLOW}[Step 3/3] Adding references from JSON directory: $JSON_DIR...${NC}"
echo -e "${YELLOW}Expecting to see 'Logged duplicate' messages below.${NC}"
python -m dl_lit.cli add-json "$JSON_DIR" --db-path "$DB_PATH"
echo -e "${GREEN}JSON import process completed.${NC}"

echo -e "\n${GREEN}--- Test Finished ---${NC}"
echo "Review the output above to verify that duplicates were correctly detected and logged."
