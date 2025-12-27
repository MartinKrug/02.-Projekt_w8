"""Utility to extract text from PDF files and store it in an SQLite database.

The script scans a directory for PDF files, reads up to the requested number,
extracts their page contents, and persists them into a table so the data can be
queried later.
"""

import argparse
import sqlite3
from pathlib import Path
from typing import Iterable, List, Tuple

from PyPDF2 import PdfReader


def collect_pdf_files(source_dir: Path, limit: int) -> List[Path]:
    """Return up to ``limit`` PDF files sorted by name from ``source_dir``."""
    pdf_files = sorted(source_dir.glob("*.pdf"))
    return pdf_files[:limit]


def extract_pages(pdf_path: Path) -> Iterable[Tuple[int, str]]:
    """Yield page number and text for each page in ``pdf_path``."""
    reader = PdfReader(str(pdf_path))
    for index, page in enumerate(reader.pages, start=1):
        text = page.extract_text() or ""
        yield index, text.strip()


def store_pages_in_db(records: Iterable[Tuple[str, int, str]], db_path: Path) -> None:
    """Persist extracted pages into an SQLite database.

    Records are tuples of ``(file_name, page_number, content)``.
    """
    connection = sqlite3.connect(db_path)
    with connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS pdf_pages (
                file_name TEXT NOT NULL,
                page_number INTEGER NOT NULL,
                content TEXT,
                PRIMARY KEY (file_name, page_number)
            )
            """
        )
        connection.executemany(
            """
            INSERT OR REPLACE INTO pdf_pages (file_name, page_number, content)
            VALUES (?, ?, ?)
            """,
            records,
        )


def ingest_pdfs(source_dir: Path, db_path: Path, limit: int = 5) -> None:
    """Read PDFs from ``source_dir`` and store their contents in ``db_path``."""
    pdf_files = collect_pdf_files(source_dir, limit)
    if not pdf_files:
        raise FileNotFoundError(f"Keine PDF-Dateien in {source_dir} gefunden.")

    records: List[Tuple[str, int, str]] = []
    for pdf_file in pdf_files:
        for page_number, content in extract_pages(pdf_file):
            records.append((pdf_file.name, page_number, content))

    store_pages_in_db(records, db_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Liest bis zu 5 PDF-Dateien aus einem Ordner aus und speichert den "
            "Inhalt in einer SQLite-Datenbank."
        )
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        required=True,
        help="Pfad zum Ordner mit den PDF-Dateien",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("pdf_data.db"),
        help="Pfad zur SQLite-Datenbank (Standard: pdf_data.db)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Anzahl der PDFs, die eingelesen werden (Standard: 5)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.source_dir.exists() or not args.source_dir.is_dir():
        raise NotADirectoryError(f"{args.source_dir} ist kein gültiger Ordner.")

    ingest_pdfs(args.source_dir, args.db_path, args.limit)
    print(
        f"Daten aus bis zu {args.limit} PDF-Dateien aus {args.source_dir} "
        f"wurden in {args.db_path} gespeichert."
    )


if __name__ == "__main__":
    main()
