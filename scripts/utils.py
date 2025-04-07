from typing import Tuple
import requests
import os

def is_homepage(source_url, link) -> Tuple[bool, str | None]:
    """
    Determines if the link points to the homepage of the same website as source_url.
    Returns a tuple: (is_homepage: bool, subsection: str | None)
    """
    from urllib.parse import urlparse

    parsed_source = urlparse(source_url)
    parsed_link = urlparse(link)

    # Check if same domain
    if parsed_source.netloc != parsed_link.netloc:
        return (False, None)  # External link

    # Check if it's the homepage
    if parsed_link.path in ["", "/"]:
        return (True, None)

    # It's a subsection or homepage of the same site
    return (True, parsed_link.path)


def klazify_categorize(url_to_categorize: str, api_key: str):
    """
    Sends a POST request to Klazify to categorize the given URL.
    
    :param url_to_categorize: The full URL you want to categorize
    :param api_key: Your Klazify API key (Bearer token)
    :return: The parsed JSON response from the API, or None if an error occurs
    """
    endpoint = "https://www.klazify.com/api/categorize"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    data = {
        "url": url_to_categorize
    }
    
    try:
        response = requests.post(endpoint, headers=headers, data=data)
        response.raise_for_status()  # Raises an HTTPError if the status is 4xx or 5xx
        return response.json()       # Return the parsed JSON from the response
    except requests.exceptions.RequestException as e:
        print(f"Request to Klazify failed: {e}")
        return None

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

def get_db_connection():
    """
    Placeholder function to get a database connection.
    Replace with actual implementation.
    """
    import psycopg2
    conn = psycopg2.connect(
        dbname="cc_db",
        user="cc_user",
        password="cc_pass",
        host="localhost",
        port=5432
    )
    return conn


# Example usage:
if __name__ == "__main__":
    ad_domains_set = load_ad_domains("data/hosts")
    test_domain = "ad.doubleclick.net"
    print(is_ad_domain(test_domain, ad_domains_set))  # True or False
