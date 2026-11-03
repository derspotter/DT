"""Print the page count of a PDF: pdf_page_count.py <pdf>."""

import sys

import pikepdf


def main(argv, client=None):
    pdf = pikepdf.Pdf.open(argv[0])
    try:
        print(len(pdf.pages))
    finally:
        pdf.close()


if __name__ == '__main__':
    main(sys.argv[1:])
