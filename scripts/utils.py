from functools import lru_cache
from typing import Tuple
from urllib.parse import quote, urlparse, urlunparse
import aiohttp
import asyncio
import os
import tldextract
import polars as pl
import json

from diskcache import Cache
cache = Cache("site-category-cache")


async def get_all_categories(domains: list[str], api_key: str, max_concurrent: int = 10) -> dict[str, str | None]:
    """
    Concurrently fetch categories for a list of domains.
    Returns a dictionary: domain -> category
    """
    connector = aiohttp.TCPConnector(limit=max_concurrent)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [get_site_category_from_api(session, domain, api_key) for domain in domains]
        results = await asyncio.gather(*tasks)
        return dict(results)


def is_homepage(link) -> Tuple[bool, str | None]:
    """
    Determines if the link points to the homepage of the same website as source_url.
    Returns a tuple: (is_homepage: bool, subsection: str | None)
    """
    from urllib.parse import urlparse

    parsed_link = urlparse(link)

    # Check if it's the homepage
    if parsed_link.path in ["", "/"]:
        return (True, None)

    # It's a subsection or homepage of the same site
    return (False, parsed_link.path)


# TODO: use an actual cache to store the results from the API
async def get_site_category_from_api(session: aiohttp.ClientSession, domain: str, api_key: str) -> tuple[str, str | None]:    
    """
    Sends a POST request to WhoisXML API to categorize the given URL.

    The response from the API is something like:

    {
        "as": {
            "asn": 54113,
            "domain": "https://www.fastly.com",
            "name": "FASTLY",
            "route": "151.101.128.0/22",
            "type": "Content"
        },
        "domainName": "cnn.com",
        "categories": [
            {
                "confidence": 1,
                "id": 379,
                "name": "News and Politics"
            },
            {
                "confidence": 0.95,
                "id": 382,
                "name": "International News"
            },
            {
                "confidence": 0.98,
                "id": 385,
                "name": "National News"
            },
            ...
        ],
        "createdDate": "1993-09-22T04:00:00+00:00",
        "websiteResponded": true,
        "apiVersion": "v3"
    }

    The function will return the category with the highest confidence score.

    :param url_to_categorize: The full URL you want to categorize
    :param api_key: Your WhoisXML API key
    :return: The parsed JSON response from the API, or None if an error occurs
    """
    if domain in cache:
        return domain, cache[domain]

    endpoint = "https://website-categorization.whoisxmlapi.com/api/v3"
    try:
        async with session.get(endpoint, params={"apiKey": api_key, "url": domain}, timeout=10) as resp:
            resp.raise_for_status()
            data = await resp.json()
            categories = data.get("categories", [])
            if categories:
                best = max(categories, key=lambda x: x["confidence"])
                category = best["name"]
                cache[domain] = category
                return domain, category
    except Exception as e:
        print(f"❌ Error for domain '{domain}': {e}")
    
    cache[domain] = None
    return domain, None


def load_ad_domains(hosts_file_path: str = "data/hosts") -> set:
    """
    Loads ad domains from the hosts file into a Python set.
    """
    ad_domains = set()
    if not os.path.exists(hosts_file_path):
        print(f"Hosts file not found at {hosts_file_path}")
        return ad_domains

    with open(hosts_file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) >= 2:
                host = parts[1].lower()
                ad_domains.add(host)

    return ad_domains


def is_ad_domain(domain: str, ad_domains: set) -> bool:
    """
    Checks if 'domain' is contained in the provided set of ad domains.
    """
    return domain.lower() in ad_domains


def extract_domain(url: str) -> tldextract.tldextract.ExtractResult:
    """
    Extracts the domain from a given URL.
    """
    return tldextract.extract(url)


def load_tlds_from_file(file_path: str = "data/tlds.csv") -> dict[str, str]:
    """
    Loads a list of TLDs from a file.
    Each row may contain multiple TLDs separated by ";" mapped to a single country code.
    """
    df = pl.read_csv(file_path, truncate_ragged_lines=True)

    tld_map = {}
    for row in df.iter_rows(named=True):
        tlds = row["tld"].split(";")
        code = row["code"]
        for tld in tlds:
            tld_map[tld.strip()] = code

    return tld_map


def extract_country(tld: str, mapping: dict) -> str | None:
    """
    Extracts the country code from the tld
    """
    # This is a placeholder function. You might want to use a library or a mapping
    # to get the country code from the TLD.

    if not mapping:
        mapping = load_tlds_from_file()

    if "." in tld:
        tld = tld.split(".")[-1]

    tld = tld.lower()
    if tld in {"com", "org", "net"}:
        return "US"

    if not tld.startswith("."):
        tld = "." + tld

    return mapping.get(tld, None)


def get_db_connection():
    """
    Placeholder function to get a database connection.
    Replace with actual implementation.
    """
    import psycopg2
    import os

    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "cc_db"),
        user=os.getenv("POSTGRES_USER", "cc_user"),
        password=os.getenv("POSTGRES_PASSWORD", "cc_pass"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
    )
    return conn

def encode_url_path_only(url) -> str | None:
    try:
        parsed = urlparse(url)
    except ValueError:
        # the url is invalid
        return None
        
    encoded_path = quote(parsed.path)
    encoded_url = urlunparse((
            parsed.scheme,
            parsed.netloc,
            encoded_path,
            parsed.params,
            parsed.query,
            parsed.fragment
    ))
    # Fallback to the original URL if encoding fails
    return encoded_url

def clean_link(link: str) -> str:
    """
    Cleans the link by removing unwanted characters and encoding the URL path.
    """
    # Remove unwanted characters
    link = link.strip().replace("\n", "").replace("\r", "")
    if "\"" in link:
        link = link.replace("\"", "")
    if "," in link:
        link = f"\"{link}\""
    # Encode the URL path
    return link # encode_url_path_only(link)


def compute_metrics(df: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """
    Placeholder function to compute metrics from the DataFrame.
    Replace with actual implementation.
    """
    total_count = df.height

    # 1. How many websites have we managed to categorize?
    category_coverage = df.select(
        (pl.col("category").is_not_null().sum() / df.height).alias(
            "non_null_category_rate"
        )
    )

    # 2. How many links are there by category?
    links_by_category = (
        df.group_by("category")
        .agg(pl.count().alias("link_count"))
        .sort("link_count", descending=True)
    )

    # 3. Top not US 10 countries
    top_countries = (
        df.filter(df["country_code"] != "US").group_by("country_code")
        .agg(pl.count().alias("cnt"))
        .sort("cnt", descending=True)
        .head(10)
    )

    # 4. Ratio of ad-based domains
    ad_based_ratio = df.select(
        (pl.col("is_ad_domain").sum() / total_count).alias("ad_domain_ratio")
    )

    # 5. Ratio of ad-based domains by country
    ad_domain_by_country = (
        df.group_by("country_code")
        .agg(
            [pl.count().alias("total"), pl.col("is_ad_domain").sum().alias("ad_based")]
        )
        .with_columns((pl.col("ad_based") / pl.col("total")).alias("ad_based_ratio"))
        .select(["country_code", "ad_based_ratio"])
    )

    return {
        "category_coverage": category_coverage,
        "links_by_category": links_by_category,
        "top_countries": top_countries,
        "ad_based_ratio": ad_based_ratio,
        "ad_domain_by_country": ad_domain_by_country,
    }


# Example usage:
if __name__ == "__main__":
    ad_domains_set = load_ad_domains("data/hosts")
    test_domain = "ad.doubleclick.net"
    print(is_ad_domain(test_domain, ad_domains_set))  # True or False
