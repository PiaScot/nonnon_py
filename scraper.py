import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple, cast
from urllib.parse import urljoin

import feedparser
import httpx
from bs4 import BeautifulSoup
from feedparser.util import FeedParserDict

from extract import process_article_html
from logger import logger
from models import Article, Site
from playw import ScraperService
from repositories import ArticleRepository
from utils import fetch_html_text, random_mobile_ua


async def scrape_site(
    scraper_service: ScraperService,
    site: Site,
    general_remove_tags: List[str],
    allowed_hosts: Set[str],
    article_repo: ArticleRepository,
) -> Tuple[int, int]:
    """Orchestrates the scraping process for a single site."""
    if not site.rss or not site.domain:
        logger.warning(f"[SKIP] RSS or Domain not registered for siteId={site.id}")
        return (0, 0)

    feed = await fetch_rss_feed(site)
    if not feed:
        return (0, 0)

    articles_to_insert = await process_feed_entries(
        scraper_service, feed, site, general_remove_tags, allowed_hosts, article_repo
    )

    if not articles_to_insert:
        logger.info(f"No new articles to insert for site: {site.title}")
        return (0, 0)

    count = await article_repo.insert_many(articles_to_insert)
    return (count, len(articles_to_insert))


async def fetch_rss_feed(site: Site) -> Optional[FeedParserDict]:
    """Fetches and parses the RSS feed for a given site."""
    assert site.rss is not None, f"RSS URL is None for site ID {site.id}"
    headers = {
        "User-Agent": random_mobile_ua(),
        "Accept": "application/rss+xml,application/xml",
    }
    async with httpx.AsyncClient(follow_redirects=True) as client:
        try:
            response = await client.get(site.rss, headers=headers, timeout=20.0)
            response.raise_for_status()
            return feedparser.parse(response.text)
        except httpx.HTTPStatusError as e:
            logger.warning(
                f"HTTP {e.response.status_code} for RSS: {site.rss} (Site ID: {site.id})"
            )
        except httpx.RequestError as e:
            logger.error(f"Request failed for RSS: {site.rss} - {e}")
    return None


async def process_feed_entries(
    scraper_service: ScraperService,
    feed: FeedParserDict,
    site: Site,
    general_remove_tags: List[str],
    allowed_hosts: Set[str],
    article_repo: ArticleRepository,
) -> List[Dict[str, Any]]:
    """Processes each entry in the RSS feed and returns a list of articles to insert."""
    articles_to_insert = []
    start_time = time.perf_counter()

    for item in feed.entries:
        link = item.link.split("?")[0].strip() if isinstance(item.link, str) else ""
        if not link:
            continue

        if await article_repo.check_exists_by_url(link):
            logger.info(f"Article already exists, skipping. URL: {link}")
            continue

        article = await process_single_article(
            scraper_service, item, link, site, general_remove_tags, allowed_hosts
        )
        if article:
            articles_to_insert.append(article.model_dump(exclude_none=True))

    logger.info(
        f"Processed {len(feed.entries)} feed entries in {(time.perf_counter() - start_time) * 1000:.2f} ms"
    )
    return articles_to_insert


async def process_single_article(
    scraper_service: ScraperService,
    item: FeedParserDict,
    link: str,
    site: Site,
    general_remove_tags: List[str],
    allowed_hosts: Set[str],
) -> Optional[Article]:
    """Processes a single article from the feed."""

    mobile_html = await fetch_html_text(link, "mobile")
    if not mobile_html:
        return None

    remove_selectors = (
        site.scrape_options.remove_selector_tags if site.scrape_options else []
    )

    final_remove_selectors = list(set(general_remove_tags + remove_selectors))

    content = await process_article_html(
        mobile_html,
        link,
        final_remove_selectors,
        allowed_hosts,
        scraper_service,
    )
    if not content:
        logger.error(f"Failed to extract content for: {link}")
        return None

    soup = BeautifulSoup(content, "html.parser")
    assert site.domain is not None, f"Domain is None for site ID {site.id}"
    thumbnail = find_thumbnail(soup, link, site.domain)
    pub_date = get_publication_date(item)
    title = cast(str, item.get("title", f"No Title Found for {link}"))

    return Article(
        site_id=site.id,
        title=title,
        url=link,
        category=site.category,
        content=content,
        pub_date=pub_date,
        thumbnail=thumbnail,
    )


def find_thumbnail(soup: BeautifulSoup, page_url: str, domain: str) -> str:
    """Finds a suitable thumbnail from the article content."""
    for img in soup.select("img.my-formatted:not([src^='data:'])"):
        src = img.get("src")
        if isinstance(src, str):
            abs_src = urljoin(page_url, src)
            if domain in abs_src and "logo" not in abs_src.lower():
                return abs_src

    first_img = soup.select_one("img.my-formatted:not([src^='data:'])")
    if first_img:
        src = first_img.get("src")
        if isinstance(src, str):
            return urljoin(page_url, src)

    return ""


def get_publication_date(item: FeedParserDict) -> str:
    """Extracts and formats the publication date from a feed item."""
    pub_date_parsed = item.get("published_parsed") or item.get("updated_parsed")
    if isinstance(pub_date_parsed, time.struct_time):
        return datetime.fromtimestamp(
            time.mktime(pub_date_parsed), tz=timezone.utc
        ).isoformat()
    return datetime.now(timezone.utc).isoformat()
