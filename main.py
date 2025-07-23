from bs4 import BeautifulSoup
import asyncio
from playwright.async_api import async_playwright, Route

# 取得したいウェブページのURL
TARGET_URL = "http://otakomu.jp/archives/38977232.html"

# 実行を許可したいスクリプトのURLに含まれるキーワード
# このキーワードが含まれるスクリプトだけが実行される
ALLOW_KEYWORD = "twitter"

# 保存するファイル名
OUTPUT_FILE = "html/output_with_twitter.html"


async def handle_route(route: Route):
    """ネットワークリクエストを処理する関数"""
    resource_type = route.request.resource_type
    request_url = route.request.url

    # リクエストがスクリプトの場合のみ処理
    if resource_type == "script":
        # 許可キーワードがURLに含まれていれば通信を続行
        if ALLOW_KEYWORD in request_url:
            print(f"✅ 許可: {request_url}")
            await route.continue_()
        # 含まれていなければ通信を中断
        else:
            print(f"❌ ブロック: {request_url}")
            await route.abort()
    # スクリプト以外（HTML, CSS, 画像など）はすべて許可
    else:
        await route.continue_()


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        iphone_14 = p.devices["iPhone 14"]
        context = await browser.new_context(**iphone_14)
        page = await context.new_page()

        # すべてのネットワークリクエストを 'handle_route' 関数で処理するよう設定
        await page.route("**/*", handle_route)

        print(f"ページにアクセスします: {TARGET_URL}")
        try:
            # ページに移動し、ネットワークが落ち着くまで待つ
            await page.goto(TARGET_URL, wait_until="networkidle", timeout=60000)

            # ページの最終的なHTMLコンテンツを取得
            final_html = await page.content()

            # 1. HTML文字列をBeautiful Soupオブジェクトに変換
            soup = BeautifulSoup(final_html, "lxml")  # 高速なlxmlパーサーを使用

            # 例A: 特定の要素をすべて削除する (例: class="ad" を持つdivタグ)
            for ad_element in soup.find_all("div", class_="ad"):
                ad_element.decompose()

            # 例B: 特定の要素のテキストを取得する (例: 最初のh1タグ)
            first_h1 = soup.find("h1")
            if first_h1:
                print(f"H1のテキスト: {first_h1.get_text()}")

            # 3. 操作後のHTMLを取得
            modified_html = soup.prettify()

            print(f"length -> {len(modified_html)}")
            print(modified_html)

        except Exception as e:
            print(f"エラーが発生しました: {e}")

        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
