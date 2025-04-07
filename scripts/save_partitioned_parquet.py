import psycopg2
import polars as pl
import os

from scripts.utils import get_db_connection

def save_partitioned_parquet():
    conn = get_db_connection()
    query = "SELECT link, is_homepage, category, country_code, is_ad_domain FROM external_links;"
    df = pl.read_database_uri(query=query, connection=conn.connect())
    conn.close()
    
    

    # We’ll do a partitioned dataset, by country_code:
    partition_cols = ["country_code"]
    output_dir = "output"

    df.write_parquet(f"{output_dir}/external_links.parquet", partition_by=partition_cols)

if __name__ == "__main__":
    save_partitioned_parquet()
