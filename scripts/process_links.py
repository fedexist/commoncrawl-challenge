import psycopg2

from utils import is_homepage, get_db_connection

def update_homepage_flag():
    conn = get_db_connection()
    cur = conn.cursor()

    # Fetch all rows once
    cur.execute("SELECT url, link FROM external_links;")
    rows = cur.fetchall()

    updates = []
    for url, link in rows:
        flag, subsection = is_homepage(url, link)
        updates.append((flag, subsection, url, link))

    # Perform batch update
    cur.executemany("""
        UPDATE external_links
        SET is_homepage_or_subsection = %s, subsection = %s
        WHERE url = %s AND link = %s
    """, updates)

    conn.commit()
    cur.close()
    conn.close()


def update_country_codes():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT link FROM external_links;")
    rows = cur.fetchall()

    for (link,) in rows:
        domain = extract_domain(link)
        country_code = extract_country_from_domain(domain) or 'UNKNOWN'
        cur.execute("""
            UPDATE external_links
            SET country_code = %s
            WHERE link = %s
        """, (country_code, link))

    conn.commit()
    cur.close()
    conn.close()

def update_site_categories():
    conn = psycopg2.connect(...)
    cur = conn.cursor()
    cur.execute("ALTER TABLE external_links ADD COLUMN IF NOT EXISTS category TEXT;")
    conn.commit()

    cur.execute("SELECT link FROM external_links;")
    rows = cur.fetchall()

    for (link,) in rows:
        domain = extract_domain(link)
        category = get_site_category_from_api(domain)
        cur.execute("""
            UPDATE external_links
            SET category = %s
            WHERE link = %s
        """, (category, link))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    update_homepage_flag()
    # update_country_codes()
    # update_site_categories()