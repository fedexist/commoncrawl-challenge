import asyncio
from utils import (
    compute_metrics,
    extract_domain,
    get_all_categories,
    is_ad_domain,
    is_homepage,
    get_db_connection,
    extract_country,
    load_ad_domains,
    load_tlds_from_file,
)
import polars as pl
from urllib.parse import urlparse
import os


def add_is_homepage_flag(df: pl.DataFrame):
    results = [is_homepage(link) for link in df["link"]]

    # Unzip the results into two separate lists
    flags, subsections = zip(*results) if results else ([], [])

    # Add the new columns to the DataFrame
    df = df.with_columns(
        [pl.Series("is_homepage", flags), pl.Series("subsection", subsections)]
    )

    return df


def extract_primary_link(link: str) -> str | None:
    try:
        parsed = urlparse(link)
        return f"{parsed.scheme}://{parsed.netloc}"
    except ValueError:
        # the url is invalid
        return None


def aggregate_links(df: pl.DataFrame) -> pl.DataFrame:
    # Step 1: Create a new column with the primary domain
    primary_links = [extract_primary_link(link) for link in df["link"]]
    df = df.with_columns(pl.Series("primary_link", primary_links))
    # We filter out the rows where primary_link is None
    df = df.filter(pl.col("primary_link").is_not_null())
    # Step 2: Filter subsections only for non-homepage links
    subsection_df: pl.DataFrame = (
        df.filter(~df["is_homepage"])
        .group_by("primary_link")
        .agg([pl.col("subsection").unique().alias("subsections")])
    )

    # Step 3: Count total occurrences of each primary link
    count_df: pl.DataFrame = df.group_by("primary_link").agg(
        [pl.len().alias("frequency")]
    )

    # Step 4: Join the two results together
    result_df: pl.DataFrame = count_df.join(
        subsection_df, on="primary_link", how="left"
    )

    return result_df


def add_country_codes(df: pl.DataFrame) -> pl.DataFrame:
    tlds: dict[str, str] = load_tlds_from_file()

    # We use the TLD to determine the country of the website, even though it won't be correct 100% of the time
    # Ideal scenario is using a WHOIS service to make API calls
    country_codes = [
        extract_country(extract_domain(link).suffix, tlds)
        for link in df["primary_link"]
    ]
    df = df.with_columns(pl.Series("country_code", country_codes))

    return df


def add_categories_adbased_domain(df: pl.DataFrame) -> pl.DataFrame:
    api_key = os.getenv("WHOIS_API_KEY")
    if not api_key:
        raise ValueError("Missing WHOIS_API_KEY")

    ad_domains = load_ad_domains()

    # Extract unique domains to categorize
    primary_links = df["primary_link"].to_list()
    unique_domains = list({extract_domain(link).fqdn for link in primary_links})

    # Run categorization concurrently
    domain_to_category = asyncio.run(get_all_categories(unique_domains, api_key))

    # Map categories to links
    categories = [
        domain_to_category.get(extract_domain(link).fqdn)
        for link in primary_links
    ]
    df = df.with_columns(pl.Series("category", categories))

    # Check ad domain flag for links with missing category
    null_category_df = df.filter(pl.col("category").is_null())
    is_ad_flags = [
        is_ad_domain(link, ad_domains)
        for link in null_category_df["primary_link"]
    ]
    df = df.join(
        null_category_df.with_columns(pl.Series("is_ad_domain", is_ad_flags)),
        on="primary_link",
        how="left"
    )

    return df

def process_external_links():
    conn = get_db_connection()

    # Read all the data from the external_links table
    print("Reading from psql...")
    df = pl.read_database(query="SELECT link FROM external_links;", connection=conn)

    # Process the DataFrame
    print("Processing is_homepage and subsections...")
    df = add_is_homepage_flag(df)
    print("Aggregating on primary links...")
    df = aggregate_links(df)
    print("Fetching country codes...")
    df = add_country_codes(df)
    print("Fetching categories and flagging ad-based domains...")
    df = add_categories_adbased_domain(df)

    # Compute the metrics, more details about them inside
    print("Computing metrics...")
    metrics_dict = compute_metrics(df)

    # Output to parquet
    print("Writing to parquet...")
    df.write_parquet("output/external_links.parquet", partition_by=["country_code", "category"])

    for metric_name, metric_df in metrics_dict.items():
        # Save each metric DataFrame to a separate Parquet file
        metric_df.write_parquet(f"output/{metric_name}.parquet")

    conn.close()


if __name__ == "__main__":
    process_external_links()

    # Testing the API call
    # api_key = os.getenv("WHOIS_API_KEY")
    # category = get_site_category_from_api("www.google.com", api_key=api_key)
    # print(category)
