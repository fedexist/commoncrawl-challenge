import os
from utils import get_db_connection

from pathlib import Path


def load_links_via_copy(links_file: str | Path):
    """
    Loads links (one per line) into the external_links table using PostgreSQL COPY.
    """
    path = Path(links_file)
    if not path.exists():
        print(f"File {links_file} does not exist.")
        return
    if not path.is_file():
        print(f"{links_file} is not a file.")
        return

    conn = get_db_connection()
    cur = conn.cursor()

    # Create the table if it doesn't exist
    with open("sql/create_tables.sql", "r") as f:
        create_table_sql = f.read()
    cur.execute(create_table_sql)

    # For a file that is simply one link per line, we can do:
    # "COPY external_links(link) FROM STDIN" expects the file lines to be valid for insertion.
    copy_sql = """
        CREATE TEMP TABLE external_links_staging (
            link TEXT
        );
        COPY external_links_staging(link)
        FROM STDIN
        WITH (
            FORMAT csv,
            DELIMITER E',',
            QUOTE E'\x22'
        );
        INSERT INTO external_links(link)
        SELECT link FROM external_links_staging
        WHERE link IS NOT NULL;
    """

    # Alternatively, you might do a simpler "COPY ... FROM STDIN WITH DELIMITER '\n';"
    # but the CSV approach can help if some lines contain special characters.

    with path.open("r", encoding="utf-8") as f:
        cur.copy_expert(copy_sql, f)

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    segments_folder = os.getenv("SEGMENTS_FOLDER")
    if not segments_folder:
        print("Please set the SEGMENTS_FOLDER environment variable.")
        exit(1)
    links_file = os.path.join(segments_folder, "extracted_links.txt")
    load_links_via_copy(links_file)
