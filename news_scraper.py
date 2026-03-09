"""
news_scraper.py

Fetches, filters, and persists headlines from RSS feeds.
Run directly to populate data/headlines.json.
"""

import feedparser
import json
from pathlib import Path

FEEDS = [
    ('Reuters Tech',  'https://feeds.reuters.com/reuters/technologyNews'),
    ('TechCrunch',    'https://techcrunch.com/feed/'),
    ('Seeking Alpha', 'https://seekingalpha.com/market_currents.xml'),
    ('CNBC Tech',     'https://www.cnbc.com/id/19854910/device/rss/rss.html'),
]

KEYWORDS = ['nvidia', 'micron', 'microsoft', 'artificial intelligence','ai',
            'chip', 'semiconductor', 'gpu', 'llm', 'openai', 'hugging face']

OUTPUT_PATH = Path('data/headlines.json')


def is_relevant(text: str) -> bool:
    """Return True if the text contains at least one keyword.

    Args:
        text: Concatenated article title and summary.

    Returns:
        True if any keyword from KEYWORDS is found, False otherwise.
    """
    return any(kw in text.lower() for kw in KEYWORDS)


def fetch_feed(name: str, url: str) -> list[dict]:
    """Fetch and parse a single RSS feed.

    Args:
        name: Human-readable source label, e.g. 'Reuters Tech'.
        url:  RSS feed URL.

    Returns:
        List of article dicts with keys: title, link, date, source, summary.
    """
    feed = feedparser.parse(url)
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
            'summary': summary[:300]
        })

    return articles


def save_headlines(new_articles: list[dict]) -> None:
    """Append new articles to headlines.json, skipping duplicates by URL."""
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

    # Merge and save
    all_articles = existing + new_unique
    OUTPUT_PATH.write_text(json.dumps(all_articles, indent=2, default=str))

    print(f'Saved {len(new_unique)} new articles ({len(all_articles)} total in file)')


if __name__ == '__main__':
    all_articles = []
    for name, url in FEEDS:
        articles = fetch_feed(name, url)
        print(f'{name}: {len(articles)} articles after filtering')
        all_articles.extend(articles)

    save_headlines(all_articles)

    # Prove no duplicates exist in the saved file
    saved = json.loads(OUTPUT_PATH.read_text())
    assert len(saved) == len({a['link'] for a in saved}), 'Duplicate URLs detected!'
    print('Deduplication assertion passed.')