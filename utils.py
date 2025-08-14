import random
import re
from typing import Literal

import httpx
from loguru import logger
import config


def random_pc_ua() -> str:
    """Returns a random PC user-agent string."""
    return random.choice(config.PC_USER_AGENTS)


def random_mobile_ua() -> str:
    """Returns a random mobile user-agent string."""
    return random.choice(config.MOBILE_USER_AGENTS)


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