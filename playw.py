from typing import Optional
from urllib.parse import urlparse
from bs4 import BeautifulSoup, Tag
from playwright_stealth import Stealth
from playwright.async_api import (
    async_playwright,
    Playwright,
    Browser,
    Route,
)
from logger import logger
from utils import random_mobile_ua

ALLOWED_SCRIPT_HOSTS = {"twitter.com", "platform.twitter.com"}


async def handle_route(route: Route) -> None:
    resource_type = route.request.resource_type
    url = route.request.url
    if resource_type == "document":
        await route.continue_()
        return
    if resource_type == "script":
        try:
            hostname = urlparse(url).hostname
            if hostname and hostname in ALLOWED_SCRIPT_HOSTS:
                logger.debug(f"Allowing whitelisted script: {url}")
                await route.continue_()
                return
        except Exception:
            await route.abort()
            return
    logger.debug(f"Blocking by default: {url} (type: {resource_type})")
    await route.abort()


class ScraperService:
    """Playwrightのライフサイクルを管理し、HTML取得機能を提供するサービス"""

    def __init__(self):
        self.playwright: Optional[Playwright] = None
        self.browser: Optional[Browser] = None
        self.stealth = Stealth()

    async def start(self):
        """ブラウザを非同期で起動する"""
        if not self.browser:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(headless=True)

    async def stop(self):
        """ブラウザを非同期で停止する"""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def _convert_twitter_cards(
        self,
        soup: BeautifulSoup,
    ) -> None:
        """Finds all un-rendered twitter-tweets and replaces them one by one."""

        twitter_script = soup.find(
            "script", attrs={"src": "https://platform.twitter.com/widgets.js"}
        )
        if not twitter_script:
            return

        script_html = str(twitter_script)
        blockquotes_to_process = list(
            soup.find_all("blockquote", class_="twitter-tweet")
        )
        if not blockquotes_to_process:
            return

        logger.debug(f"found twitter-card {len(blockquotes_to_process)}")

        for blockquote in blockquotes_to_process:
            if not isinstance(blockquote, Tag):
                continue
            # --- ▼ 修正点 ▼ ---
            # blockquote内のaタグが空の場合、hrefのURLをテキストとして挿入する
            link_tag = blockquote.find("a")
            # aタグが存在し、かつその中身が空（スペースなども除く）の場合
            if (
                link_tag
                and isinstance(link_tag, Tag)
                and not link_tag.get_text(strip=True)
            ):
                href = link_tag.get("href")
                # hrefがあれば、それをテキストとして設定
                if isinstance(href, str):
                    link_tag.string = href
            # --- ▲ 修正ここまで ▲ ---

            rendered_card_html = await self.render_twitter_card(
                str(blockquote), script_html
            )

            if rendered_card_html:
                new_card_content = BeautifulSoup(rendered_card_html, "html.parser")
                blockquote.replace_with(new_card_content)

        for script in soup.find_all(
            "script", attrs={"src": "https://platform.twitter.com/widgets.js"}
        ):
            script.decompose()

    async def render_twitter_card(
        self, blockquote_html: str, script_html: str
    ) -> Optional[str]:
        """
        単一のTwitterカード（blockquote）をレンダリングして、そのHTMLを返す。
        """
        if not self.browser:
            raise RuntimeError("ScraperService has not been started.")

        full_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>Twitter Card Render</title>
        </head>
        <body>
            {blockquote_html}
            {script_html}
        </body>
        </html>
        """
        context = None
        try:
            context = await self.browser.new_context(user_agent=random_mobile_ua())
            page = await context.new_page()
            await page.set_content(full_html, wait_until="domcontentloaded")

            # レンダリングされたiframeを待つ
            rendered_iframe_selector = "iframe[data-tweet-id]"
            await page.wait_for_selector(rendered_iframe_selector, timeout=15000)

            # iframe内のコンテンツを取得しようとすると複雑になるため、
            # ここではレンダリングが完了した後のコンテナ要素を取得する
            # 親要素や特定のラッパー要素などをセレクタで指定する
            # この例では body 全体を返す
            content = await page.content()
            soup = BeautifulSoup(content, "html.parser")
            return str(soup.body)

        except Exception as e:
            logger.warning(f"Failed to render twitter card: {e}")
            return None
        finally:
            if context:
                await context.close()
