import os
import re
from typing import Optional

# --- Supabase Credentials ---
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_ROLE_KEY = os.getenv("SUPABASE_ROLE_KEY", "")

# --- Database Tables ---
ARTICLE_TABLE = "articles"
SITE_TABLE = "antena_sites"
CATEGORY_TABLE = "antena_sites_category"
BOOKMARK_TABLE = "bookmark_articles"
ALLOW_HOST_TABLE = "allowed_embed_hosts"
GENERAL_REMOVE_TAGS_TABLE = "general_remove_tags"
BAKUSAI_THREAD_TABLE = "threads"
BAKUSAI_RES_TABLE = "res_comments"

# --- RPC Names ---
GET_SITES_TO_SCRAPE_RPC = "get_sites_to_scrape"

# --- Application Settings ---
MAX_ARTICLES = int(os.getenv("MAX_ARTICLES", 10000))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 500))

# --- User-Agents ---
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

# --- Logging ---
LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
)

# --- Playwright ---
ALLOWED_SCRIPT_HOSTS = {"twitter.com", "platform.twitter.com"}

# --- Extractor ---
LAZY_ATTRS = ["data-src", "data-lazy-src", "data-original"]
MEDIA_RE = re.compile(r"\.(jpe?g|png|gif|webp|mp4|webm|mov|m4v)(\?.*)?$", re.IGNORECASE)
VIDEO_RE = re.compile(r"\.(mp4|webm|mov|m4v)$", re.IGNORECASE)

# --- Bakusai Scraper ---
BAKUSAI_BASE_URL = "https://bakusai.com"
BAKUSAI_SHOP_LIST_PATH = "/thr_tl/acode=2/ctgid=103/bid=286/"
BAKUSAI_KOJIN_LIST_PATH = "/thr_tl/acode=2/ctrid=0/ctgid=103/bid=956/"


def convert_maru_char_to_int(char: str) -> Optional[int]:
    """
    丸数字1文字を整数に変換する。対応範囲外の場合はNoneを返す。
    """
    if len(char) != 1:
        return None

    code = ord(char)

    # Unicodeブロック1: ① (U+2460) から ⑳ (U+2473)
    if 0x2460 <= code <= 0x2473:
        return code - 0x2460 + 1

    # Unicodeブロック2: ㉑ (U+3251) から ㉟(U+325F)
    if 0x3251 <= code <= 0x325F:
        return code - 0x3251 + 21

    # Unicodeブロック3: ㊱(U+32B1) から ㊿ (U+32BF)
    if 0x32B1 <= code <= 0x32BF:
        return code - 0x32B1 + 35

    return None
