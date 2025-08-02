import random
import re
from typing import Literal

import httpx
from loguru import logger

PC_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

MOBILE_USER_AGENTS = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.78 Mobile Safari/537.36",
    "Mozilla/5.0 (Android 14; Mobile; rv:126.0) Gecko/126.0 Firefox/126.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/124.0.6367.62 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Samsung Galaxy S23) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/25.0 Chrome/124.0.6367.78 Mobile Safari/537.36",
]


def random_pc_ua() -> str:
    """Returns a random PC user-agent string."""
    return random.choice(PC_USER_AGENTS)


def random_mobile_ua() -> str:
    """Returns a random mobile user-agent string."""
    return random.choice(MOBILE_USER_AGENTS)


def _remove_duplicate_empty_line(text: str) -> str:
    return re.sub(r"\n\s*\n+", "\n", text)


async def fetch_html_text(url: str, ua: Literal["mobile", "pc"]) -> str:
    headers = {
        "User-Agent": random_mobile_ua() if ua == "mobile" else random_pc_ua(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ja-JP,ja;q=0.9",
    }
    async with httpx.AsyncClient(follow_redirects=True) as client:
        try:
            response = await client.get(url, headers=headers, timeout=20.0)
            response.raise_for_status()
            return _remove_duplicate_empty_line(response.text)
        except httpx.HTTPStatusError as e:
            logger.warning(f"HTTP {e.response.status_code} for url: {url}")
        except httpx.RequestError as e:
            logger.error(f"Request failed for url: {url} - {e}")
    return ""
