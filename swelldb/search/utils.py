import logging

from bs4 import BeautifulSoup


def clean_html(raw_html):
    """
    Cleans the raw HTML content by removing scripts, styles, and other non-essential elements.
    Args:
        raw_html (str): The raw HTML content to clean.
    """
    soup = BeautifulSoup(raw_html, "html.parser")

    # Remove scripts, styles, and hidden elements
    for tag in soup(["script", "style", "noscript", "meta", "head"]):
        tag.decompose()

    # Optional: remove elements with low value
    for tag in soup.find_all(['nav', 'footer', 'aside', 'form', 'input']):
        tag.decompose()

    return soup


def crawl(link: str):
    """
    Crawls the given link and returns the cleaned HTML content.
    Args:
        link (str): The URL to crawl.
    """
    import requests

    response = requests.get(link)
    if response.status_code == 200:
        # Assuming the content is text/html, you can parse it as needed
        return response.text
    else:
        logging.error(f"Failed to fetch content from {link}, status code: {response.status_code}")
        return None