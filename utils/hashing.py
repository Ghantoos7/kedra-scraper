"""
Content hashing utility.

Provides a stable hash function that normalizes HTML content before
hashing. This ensures the same logical content produces the same hash
regardless of dynamic elements (session tokens, cookies, timestamps,
navigation bars) that change between requests.

Used by both:
  - The spider (Phase 3): to detect if a document has changed between runs
  - The transformation (Phase 4): to detect if transformation output changed

Industry-standard approach:
  1. Parse HTML with BeautifulSoup
  2. Remove volatile elements (scripts, styles, nav, footer, cookies)
  3. Extract text content only
  4. Normalize whitespace
  5. SHA-256 hash the normalized text

Reference: "Detecting Silent Content Changes - Hashing Strategies for Web Monitoring"
"""

import hashlib
from bs4 import BeautifulSoup


def compute_content_hash(raw_bytes: bytes, file_type: str = "html") -> str:
    """
    Compute a stable SHA-256 hash of the content.

    For HTML: normalizes by stripping dynamic elements and extracting
    text only, producing a hash that is stable across requests.

    For PDF/DOC: hashes the raw bytes directly since binary formats
    don't contain dynamic web elements.

    Args:
        raw_bytes: The raw file content as bytes
        file_type: "html", "pdf", or "doc"

    Returns:
        64-character hexadecimal SHA-256 hash string
    """
    if file_type in ("pdf", "doc"):
        # Binary formats are stable — hash directly
        return hashlib.sha256(raw_bytes).hexdigest()

    # For HTML: normalize before hashing
    normalized_text = normalize_html_content(raw_bytes)
    return hashlib.sha256(normalized_text.encode("utf-8")).hexdigest()


def normalize_html_content(raw_bytes: bytes) -> str:
    """
    Extract and normalize the meaningful text content from an HTML page.

    Removes all volatile/dynamic elements that change between requests:
    - Scripts and styles
    - Navigation bars and menus
    - Cookie banners and consent dialogs
    - Headers and footers
    - Hidden form fields (CSRF tokens, session data)
    - Social media widgets
    - "Return to Search" links
    - Binder/bookmark buttons

    Then extracts all remaining text, normalizes whitespace, and returns
    a clean string suitable for hashing.

    Args:
        raw_bytes: Raw HTML as bytes

    Returns:
        Normalized text content as a string
    """
    soup = BeautifulSoup(raw_bytes, "lxml")

    # Remove tags that contain volatile/dynamic content
    volatile_tags = ["script", "style", "nav", "footer", "header", "noscript", "iframe", "link", "meta"]
    for tag_name in volatile_tags:
        for tag in soup.find_all(tag_name):
            tag.decompose()

    # Remove elements by CSS class/id that contain dynamic content
    volatile_selectors = [
        ".cookie-banner",
        ".navbar",
        ".footer-one",
        ".footer-two",
        ".footer-nav",
        ".social-banner",
        ".no-print",
        ".return-to-search",
        "#binderFixed",
        ".ptools-pager",
        "#advancedSearchControls",
        "#searchPnl",
        ".aspNetHidden",           # ASP.NET hidden fields (CSRF, ViewState)
        "input[type='hidden']",    # Hidden form inputs
    ]
    for selector in volatile_selectors:
        for element in soup.select(selector):
            element.decompose()

    # Remove "Return to Search" links
    for a_tag in soup.find_all("a", string=lambda s: s and "Return to Search" in s):
        a_tag.decompose()

    # Extract all remaining text
    text = soup.get_text(separator=" ", strip=True)

    # Normalize whitespace: collapse multiple spaces/newlines into single space
    normalized = " ".join(text.split())

    return normalized
