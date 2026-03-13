"""
news_scraper.py

Fetches, filters, and persists headlines from RSS feeds.
Run directly to populate data/headlines.json.
"""

import feedparser
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests
from urllib.parse import urlparse
import os, json, time, sys
from scraper_report import ScraperReport


FEEDS = [
    ('Reuters Tech',  'https://news.google.com/rss/search?q=nvidia+OR+semiconductor+OR+AI&hl=en-US&gl=US&ceid=US:en'),
    ('TechCrunch',    'https://techcrunch.com/feed/'),
    ('Seeking Alpha', 'https://seekingalpha.com/market_currents.xml'),
    ('CNBC Tech',     'https://www.cnbc.com/id/19854910/device/rss/rss.html'),
    # ('Bad URL', 'not_a_url') # bad URL
    # ('Dead Host', 'https://this-domain-does-not-exist-xyz123.com/feed') # an unreachable host
]

KEYWORDS = ['nvidia', 'micron', 'microsoft', 'artificial intelligence','ai',
            'chip', 'semiconductor', 'gpu', 'llm', 'openai', 'hugging face']

OUTPUT_PATH = Path('data/headlines.json')

RATE_LIMIT_SECONDS = 2.0

_last_request: dict[str, float] = {}

CACHE_DIR = Path('data/cache')

CACHE_TTL_SECONDS = 4 * 3600  # 4 hours


def is_relevant(text: str) -> bool:
    """Return True if the text contains at least one keyword.

    Args:
        text: Concatenated article title and summary.

    Returns:
        True if any keyword from KEYWORDS is found, False otherwise.
    """
    return any(kw in text.lower() for kw in KEYWORDS)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    # Only retries on ConnectionError / Timeout (i.e. internet is down / slow)
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
)
def fetch_with_retry(url: str) -> requests.Response:
    """Fetch a URL with exponential backoff retry on transient network errors.

    Retries up to 3 times with exponential backoff (2s, 4s, 8s, capped at 30s).
    Only retries on ConnectionError and Timeout — HTTP errors (4xx/5xx) fail
    immediately since retrying them will not produce a different result.

    Args:
        url: The URL to fetch.

    Returns:
        A successful requests.Response object.

    Raises:
        requests.HTTPError: If the server returns a 4xx or 5xx status code.
        requests.ConnectionError: If the host is unreachable after all retries.
        requests.Timeout: If the server does not respond within 10 seconds after all retries.
    """
    response = requests.get(url, timeout=10, headers={'User-Agent': 'research-bot/1.0'})
    response.raise_for_status() # Raises HTTPError for any 4xx or 5xx response
    return response


def rate_limited_fetch(url: str) -> requests.Response:
    """Fetch a URL while enforcing a per-domain minimum request interval.

    Checks how long ago the same domain was last requested. If less than
    RATE_LIMIT_SECONDS have passed, sleeps for the remaining time before
    proceeding. This prevents hammering a single server with rapid requests.

    Args:
        url: The URL to fetch.

    Returns:
        A successful requests.Response object.

    Raises:
        requests.HTTPError: Propagated from fetch_with_retry.
        requests.ConnectionError: Propagated from fetch_with_retry.
        requests.Timeout: Propagated from fetch_with_retry.
    """
    domain = urlparse(url).netloc # Extracts the domain from a full URL
    elapsed_time = time.time() - _last_request.get(domain, 0) # Now - Last Visit (default: 0)
    if elapsed_time < RATE_LIMIT_SECONDS:
        time.sleep(RATE_LIMIT_SECONDS-elapsed_time)
    _last_request[domain] = time.time() # Update the Memory & Executing
    return fetch_with_retry(url)


def fetch_feed(name: str, url: str) -> list[dict]:
    """Fetch and parse a single RSS feed, returning only keyword-relevant articles.

    Args:
        name: Human-readable source label, e.g. 'Reuters Tech'.
        url:  RSS feed URL.

    Returns:
        List of article dicts with keys: title, link, date, source, summary.
        Only articles whose title or summary matches at least one KEYWORD are included.

    Raises:
        requests.HTTPError: If the feed URL returns a 4xx or 5xx status code.
        requests.ConnectionError: If the host is unreachable after all retries.
        requests.Timeout: If the server does not respond within the timeout after all retries.
    """
    response = rate_limited_fetch(url)
    feed = feedparser.parse(response.text)  # feedparser can parse a raw string too

    articles = []

    for entry in feed.entries:
        title = entry.get('title', '')
        summary = entry.get('summary', '')

        if not is_relevant(title + ' ' + summary):
            continue # Only include articles have keywords

        articles.append({
            'title': title,
            'link': entry.get('link', ''),
            'date': entry.get('published', ''),
            'source': name,
            'summary': summary[:300],
        })

    return articles


def save_headlines(new_articles: list[dict]) -> None:
    """Append new articles to OUTPUT_PATH, skipping any whose URL already exists.

    Loads the existing headlines file (if present), deduplicates against it by
    article URL, merges the unique new articles, and overwrites the file.

    Args:
        new_articles: List of article dicts to persist. Each dict must contain
            at least a 'link' key used as the deduplication identifier.

    Returns:
        None. Prints a summary of new and total article counts to stdout.
    """
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Load existing articles (empty list if file doesn't exist yet)
    if OUTPUT_PATH.exists():
        existing = json.loads(OUTPUT_PATH.read_text())
    else:
        existing = []

    # Build a set of already-seen URLs
    existing_urls = {a['link'] for a in existing}

    # Only keep articles whose URL we haven't seen before
    new_unique = [a for a in new_articles if a['link'] not in existing_urls]

    # Merge and atomic save — write to tmp first, validate, then replace
    all_articles = existing + new_unique
    tmp_path = OUTPUT_PATH.with_suffix('.tmp.json')
    tmp_path.write_text(json.dumps(all_articles, indent=2, default=str))
    loaded = json.loads(tmp_path.read_text())  # validate tmp is readable before committing
    assert len(loaded) == len(all_articles), 'Tmp file corrupted — aborting save'
    os.replace(tmp_path, OUTPUT_PATH)  # atomic on all OSes

    print(f'Saved {len(new_unique)} new articles ({len(all_articles)} total in file)')


def load_or_fetch(name: str, url: str) -> list[dict]:
    """Return cached articles for a feed if fresh, otherwise fetch and cache them.

    Checks whether a cache file for the given source exists and is younger than
    CACHE_TTL_SECONDS. On a cache hit, reads and returns the cached data without
    making any network request. On a cache miss, calls fetch_feed, writes the
    result to the cache file, and returns the fresh data.

    Args:
        name: Human-readable source label used as the cache filename, e.g. 'Reuters Tech'.
        url:  RSS feed URL passed to fetch_feed on a cache miss.

    Returns:
        List of article dicts with keys: title, link, date, source, summary.

    Raises:
        requests.HTTPError: Propagated from fetch_feed on a cache miss.
        requests.ConnectionError: Propagated from fetch_feed on a cache miss.
        requests.Timeout: Propagated from fetch_feed on a cache miss.
    """
    cache_file = CACHE_DIR / f'{name.lower().replace(" ", "_")}.json'
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    if cache_file.exists():
        age_seconds = time.time() - os.path.getmtime(cache_file) # Right Now - Last Modification Time
        if age_seconds < CACHE_TTL_SECONDS: # Fresh file
            print("Cache hit")
            return json.loads(cache_file.read_text())

    # Cache miss — fetch from network
    data = fetch_feed(name, url)
    cache_file.write_text(json.dumps(data, indent=2, default=str))
    return data


if __name__ == '__main__':
    report = ScraperReport()
    start_time = time.time()
    all_articles = []

    for name, url in FEEDS:
        try:
            articles = load_or_fetch(name, url)
            report.sources_scraped.append(name)
            report.articles_filtered += len(articles)
            print(f'{name}: {len(articles)} articles after filtering')
            all_articles.extend(articles)
        except Exception as e:
            report.errors.append(f'{name}: {e}')
            print(f'{name}: FAILED — {e}')

    report.runtime_seconds = time.time() - start_time

    # Minimum article count guard — a result of zero is almost certainly a bug
    if len(all_articles) < 5:
        print(f'ERROR: Only {len(all_articles)} articles collected — possible feed failure', file=sys.stderr)
        sys.exit(1)

    save_headlines(all_articles)

    # Prove no duplicates exist in the saved file
    saved = json.loads(OUTPUT_PATH.read_text())
    assert len(saved) == len({a['link'] for a in saved}), 'Duplicate URLs detected!'
    print('Deduplication assertion passed.')
    print(report.summary())
