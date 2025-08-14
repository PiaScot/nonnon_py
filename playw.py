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
import config


async def handle_route(route: Route) -> None:
    resource_type = route.request.resource_type
    url = route.request.url
    if resource_type == "document":
        await route.continue_()
        return
    if resource_type == "script":
        try:
            hostname = urlparse(url).hostname
            if hostname and hostname in config.ALLOWED_SCRIPT_HOSTS:
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
            link_tag = blockquote.find("a")
            if (
                link_tag
                and isinstance(link_tag, Tag)
                and not link_tag.get_text(strip=True)
            ):
                href = link_tag.get("href")
                if isinstance(href, str):
                    link_tag.string = href

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

    # async def render_twitter_card(
    #     self, blockquote_html: str, script_html: str
    # ) -> Optional[str]:
    #     """
    #     単一のTwitterカード（blockquote）をレンダリングして、そのHTMLを返す。
    #     """
    #     if not self.browser:
    #         raise RuntimeError("ScraperService has not been started.")
    #
    #     full_html = f"""
    #     <!DOCTYPE html>
    #     <html>
    #     <head>
    #         <meta charset="utf-8">
    #         <title>Twitter Card Render</title>
    #     </head>
    #     <body>
    #         {blockquote_html}
    #         {script_html}
    #     </body>
    #     </html>
    #     """
    #     context = None
    #     try:
    #         context = await self.browser.new_context(user_agent=random_mobile_ua())
    #         page = await context.new_page()
    #         await page.set_content(full_html, wait_until="domcontentloaded")
    #
    #         rendered_iframe_selector = "iframe[data-tweet-id]"
    #         await page.wait_for_selector(rendered_iframe_selector, timeout=15000)
    #
    #         content = await page.content()
    #         soup = BeautifulSoup(content, "html.parser")
    #         return str(soup.body)
    #
    #     except Exception as e:
    #         logger.warning(f"Failed to render twitter card: {e}")
    #         return None
    #     finally:
    #         if context:
    #             await context.close()

    # playw.py 内の render_twitter_card 関数

    async def render_twitter_card(
        self, blockquote_html: str, script_html: str
    ) -> Optional[str]:
        """
        与えられたblockquoteからレンダリングされたTwitterカードのHTMLを生成する。
        iframe内のコンテンツの高さを測定し、動的に高さを設定する。
        """
        if not self.browser or not self.playwright:
            raise RuntimeError("ScraperService has not been started.")

        HTML_TEMPLATE = f"""
        <!DOCTYPE html><html><head><meta charset="utf-8">
        <title>Twitter Card Renderer</title></head>
        <body>{blockquote_html}{script_html}</body></html>
        """

        context = None
        try:
            device_settings = self.playwright.devices["iPhone 14"]
            context_options = {
                **device_settings,
                "locale": "ja-JP",
                "timezone_id": "Asia/Tokyo",
            }
            context = await self.browser.new_context(**context_options)
            page = await context.new_page()
            await page.set_content(HTML_TEMPLATE)

            rendered_iframe_selector = "iframe[data-tweet-id]"
            await page.wait_for_selector(rendered_iframe_selector, timeout=15000)
            iframe_handle = await page.query_selector(rendered_iframe_selector)

            if not iframe_handle:
                return None

            iframe_content = await iframe_handle.content_frame()
            if not iframe_content:
                logger.warning("Could not get iframe content frame.")
                return None

            await iframe_content.wait_for_load_state()

            article_locator = iframe_content.locator("article")
            if await article_locator.count() == 0:
                logger.warning("Tweet seems to be deleted (no <article> tag found).")
                return None

            content_height = await iframe_content.evaluate(
                "() => document.body.scrollHeight"
            )
            if not content_height or content_height < 100:
                content_height = 275

            # --- ここからが修正点 ---

            # 測定した高さに25%の余分な高さを追加し、整数に丸める
            final_height = round(content_height * 1.15)

            # logger.info(
            #     f"Original height: {content_height}px, Final height (+25%): {final_height}px"
            # )

            return await iframe_handle.evaluate(
                """(element, measuredHeight) => {
                    const parentDiv = element.parentElement;
                    if (parentDiv) {
                        parentDiv.style.width = 'auto';
                        parentDiv.style.height = 'auto';
                        parentDiv.style.maxWidth = '100%';
                        parentDiv.style.marginTop = '12px';
                    }
                    element.style.width = '100%';
                    
                    // 変更後の高さをピクセル単位で設定
                    element.style.height = measuredHeight + 'px';
                    
                    element.style.border = 'none';
                    
                    return parentDiv ? parentDiv.outerHTML : element.outerHTML;
                }""",
                # 渡す変数を変更後の final_height にする
                final_height,
            )
            # --- ここまでが修正点 ---

        except Exception as e:
            logger.warning(f"Failed to render Twitter card: {e}")
            return None
        finally:
            if context:
                await context.close()
