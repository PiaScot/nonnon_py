import re
from typing import List, Optional, Set
from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup, Tag, element
from bs4.element import NavigableString, PageElement
from logger import logger
from playw import ScraperService
from utils import fetch_html_text

LAZY_ATTRS = ["data-src", "data-lazy-src", "data-original"]
MEDIA_RE = re.compile(r"\.(jpe?g|png|gif|webp|mp4|webm|mov|m4v)(\?.*)?$", re.IGNORECASE)
VIDEO_RE = re.compile(r"\.(mp4|webm|mov|m4v)$", re.IGNORECASE)


def _extract_imgur_id(iframe_tag: element.Tag) -> Optional[str]:
    """
    Attempts to extract an Imgur image ID from an iframe's attributes.
    Tries src, id, and class attributes in order of reliability.
    """
    src = iframe_tag.get("src")
    if isinstance(src, str):
        match = re.search(r"imgur\.com/([a-zA-Z0-9]{5,})", src)
        if match:
            return match.group(1)

    id_attr = iframe_tag.get("id")
    if isinstance(id_attr, str):
        parts = id_attr.split("-")
        if len(parts) > 1 and len(parts[-1]) >= 5 and parts[-1].isalnum():
            return parts[-1]

    id_attr = iframe_tag.get("data-id")
    if isinstance(id_attr, str):
        parts = id_attr.split("-")
        if len(parts) > 1 and len(parts[-1]) >= 5 and parts[-1].isalnum():
            return parts[-1]

    class_list = iframe_tag.get("class", "")
    if class_list is None:
        return None

    for class_name in class_list:
        parts = class_name.split("-")
        for part in parts:
            if len(part) >= 5 and part.isalnum():
                return part

    return None


def _create_imgur_img_tag(soup: BeautifulSoup, img_id: str) -> Tag:
    """Creates a new <img> tag for an Imgur image."""
    return soup.new_tag(
        "img",
        attrs={
            "src": f"https://i.imgur.com/{img_id}.jpeg",
            "alt": "imgur の画像",
            "loading": "lazy",
            "referrerpolicy": "no-referrer",
            "style": "max-width:100%;height:auto;display:block",
            "class": "my-formatted",
        },
    )


def _unwrap_imgur(soup: BeautifulSoup) -> None:
    """Finds all Imgur unwrap tags and replaces them with <img> tags."""
    for iframe in soup.select('iframe[src*="imgur.com"]'):
        if not isinstance(iframe, element.Tag):
            continue

        img_id = _extract_imgur_id(iframe)

        if img_id:
            new_img = _create_imgur_img_tag(soup, img_id)
            iframe.replace_with(new_img)

    for blockquote in soup.select("blockquote.imgur-embed-pub[data-id]"):
        if not isinstance(blockquote, Tag):
            continue

        # data-id属性から直接IDを取得
        img_id_val = blockquote.get("data-id")

        # IDが文字列として取得できた場合のみ処理
        if isinstance(img_id_val, str) and img_id_val.strip():
            img_id = img_id_val.strip()

            # <img>タグを作成
            new_img = _create_imgur_img_tag(soup, img_id)
            blockquote.replace_with(new_img)


def _get_proxied_url(original_url: str) -> str:
    """If the URL is http, convert it to a proxied URL."""
    if original_url and original_url.startswith("http://"):
        return f"/api/image-proxy?url={original_url}"
    return original_url


def _find_valid_media_url(tag: element.Tag) -> str:
    """Finds a valid media URL from lazy loading attributes or src."""
    try:
        for attr in LAZY_ATTRS:
            lazy_src = tag.get(attr)
            if isinstance(lazy_src, str):
                clean_src = lazy_src.strip()
                if MEDIA_RE.search(clean_src):
                    return clean_src

        src = tag.get("src")
        if isinstance(src, str):
            clean_src = src.strip()
            if MEDIA_RE.search(clean_src) and not clean_src.startswith("data:image"):
                return clean_src

    except Exception as e:
        logger.warning(e)
        return ""

    return ""


def _absolutize_paths(soup: BeautifulSoup, page_url: str) -> None:
    """Converts all src and href attributes to absolute URLs."""
    for tag in soup.find_all(attrs={"src": True}) + soup.find_all(attrs={"href": True}):
        if not isinstance(tag, element.Tag):
            continue

        for attr in ["src", "href"]:
            original_path = tag.get(attr)
            if not isinstance(original_path, str) or original_path.startswith(
                ("javascript:", "#")
            ):
                continue

            trimmed_path = original_path.strip()
            if re.match(r"^https?://", trimmed_path, re.IGNORECASE):
                continue

            try:
                absolute_url = urljoin(page_url, trimmed_path)
                tag[attr] = absolute_url
            except Exception:
                logger.warning(
                    f'Could not absolutize malformed path: "{trimmed_path}" on page {page_url}'
                )


def _remove_scripts(soup: BeautifulSoup, allow_hosts: Set[str]) -> None:
    """Removes script tags, except for those from allowed hosts."""
    for script in soup.find_all("script"):
        if not isinstance(script, element.Tag):
            continue

        src = script.get("src")
        if not isinstance(src, str) or not src.strip():
            script.decompose()
            continue

        try:
            hostname = urlparse(src.strip()).hostname
            if hostname not in allow_hosts:
                script.decompose()
        except Exception:
            script.decompose()


def _remove_selectors(soup: BeautifulSoup, selectors: List[str]) -> None:
    """Removes elements matching a list of CSS selectors."""
    for selector in selectors:
        for el in soup.select(selector):
            try:
                el.decompose()
            except Exception as e:
                logger.warning(e)
                return


def _normalize_images(soup: BeautifulSoup) -> None:
    """Normalizes all <img> tags."""
    for img in soup.find_all("img", class_=lambda c: c != "my-formatted"):
        if not isinstance(img, element.Tag):
            continue

        src = _find_valid_media_url(img)
        if not src:
            img.decompose()
            continue

        proxied_src = _get_proxied_url(src)
        new_img = soup.new_tag(
            "img",
            attrs={
                "src": proxied_src,
                "loading": "lazy",
                "referrerpolicy": "no-referrer",
                "style": "max-width:100%;height:auto;display:block",
                "class": "my-formatted",
            },
        )
        img.replace_with(new_img)


def _cleanup_empty_tags(soup: BeautifulSoup) -> None:
    """Removes <p> tags that are visually empty."""
    for p in soup.find_all("p"):
        if not isinstance(p, element.Tag):
            continue

        if not p.text.strip() and not p.find(("a", "img", "video", "iframe", "input")):
            p.decompose()


async def _convert_twitter_cards(
    soup: BeautifulSoup, scraper_service: ScraperService
) -> None:
    """Finds all un-rendered twitter-tweets and replaces them one by one."""

    twitter_script = soup.find(
        "script", attrs={"src": "https://platform.twitter.com/widgets.js"}
    )
    if not twitter_script:
        return

    script_html = str(twitter_script)
    blockquotes_to_process = list(soup.find_all("blockquote", class_="twitter-tweet"))
    if not blockquotes_to_process:
        return

    for blockquote in blockquotes_to_process:
        if not isinstance(blockquote, Tag):
            continue

        rendered_card_html = await scraper_service.render_twitter_card(
            str(blockquote), script_html
        )

        if rendered_card_html:
            new_card_content = BeautifulSoup(rendered_card_html, "html.parser")
            blockquote.replace_with(new_card_content)

    for script in soup.find_all(
        "script", attrs={"src": "https://platform.twitter.com/widgets.js"}
    ):
        script.decompose()


def _unwrap_anchored_media(soup: BeautifulSoup):
    """
    CheerioのunwrapAnchoredMediaをBeautifulSoupで再現。
    メディアへのリンクやメディアを内包する要素を、単一の<img>または<video>タグに置き換える。
    """
    for elem in soup.select("a, p, div.wp-video"):
        if not isinstance(elem, Tag):
            continue

        url = ""

        if elem.name == "a":
            href_val = elem.get("href")
            href = href_val.strip() if isinstance(href_val, str) else ""
            url_found = False

            try:
                if href:
                    parsed_url = urlparse(href)
                    params = parse_qs(parsed_url.query)
                    for values in params.values():
                        for value in values:
                            if value.lower().startswith("http") and MEDIA_RE.search(
                                value
                            ):
                                url = value
                                url_found = True
                                break
                        if url_found:
                            break
            except Exception:
                pass

            if not url_found and MEDIA_RE.search(href):
                url = href
                url_found = True

            if not url_found:
                nested_media = elem.find(("img", "video", "source"))
                if nested_media and isinstance(nested_media, Tag):
                    found = _find_valid_media_url(nested_media)
                    if found:
                        url = found
                        url_found = True

            if not url_found:
                text_content = elem.get_text(strip=True)
                if text_content.lower().startswith("http") and MEDIA_RE.search(
                    text_content
                ):
                    url = text_content
        else:
            has_significant_text = any(
                isinstance(n, NavigableString) and n.strip() for n in elem.contents
            )
            if has_significant_text:
                continue

            for media_el in elem.find_all(("img", "video", "source")):
                if isinstance(media_el, Tag):
                    found_url = _find_valid_media_url(media_el)
                    if found_url:
                        url = found_url
                        break

        if url and MEDIA_RE.search(url):
            proxied_url = _get_proxied_url(url)
            if VIDEO_RE.search(proxied_url):
                replacement_html = f'<video src="{proxied_url}" class="my-formatted" referrerpolicy="no-referrer" controls playsinline style="width:100%;height:auto;display:block;"></video>'
            else:
                replacement_html = f'<img src="{proxied_url}" class="my-formatted" referrerpolicy="no-referrer" style="width:100%;height:auto;display:block;" loading="lazy" />'

            new_tag = BeautifulSoup(replacement_html, "html.parser").contents[0]
            elem.replace_with(new_tag)

    for video_el in soup.select("video:has(source)"):
        if not isinstance(video_el, Tag) or video_el.get("src"):
            continue

        source = video_el.find("source", src=True)
        if source and isinstance(source, Tag):
            src_val = source.get("src")
            if isinstance(src_val, str):
                source_src = src_val.strip()
                proxied_url = _get_proxied_url(source_src)

                video_el["src"] = proxied_url
                class_list = video_el.get("class")
                if class_list is None:
                    class_list = []
                if not isinstance(class_list, list):
                    class_list = [str(class_list)]
                if "my-formatted" not in class_list:
                    class_list.append("my-formatted")
                video_el["class"] = " ".join(class_list)
                video_el["controls"] = ""
                video_el["playsinline"] = ""
                video_el["style"] = "width:100%;height:auto;display:block;"
                video_el.clear()


def _convert_video_js(soup: BeautifulSoup):
    """
    CheerioのconvertVideoJsをBeautifulSoupで再現。
    <video-js>カスタムタグを標準の<video>タグに変換する。
    """
    for vjs_element in soup.select("video-js"):
        if not isinstance(vjs_element, Tag):
            continue

        source = vjs_element.find("source", {"type": "video/mp4"}, src=True)
        src = ""
        if source and isinstance(source, Tag):
            src_val = source.get("src")
            if isinstance(src_val, str):
                src = src_val.strip()

        poster_val = vjs_element.get("poster")
        poster = poster_val.strip() if isinstance(poster_val, str) else ""

        if not src:
            vjs_element.decompose()
            continue

        replacement_html = f'<video src="{src}" poster="{poster}" class="my-formatted" controls playsinline style="width:100%;height:auto;display:block;" referrerpolicy="no-referrer"></video>'

        new_tag = BeautifulSoup(replacement_html, "html.parser").contents[0]
        vjs_element.replace_with(new_tag)


def _check_paging_contents(soup: BeautifulSoup) -> bool:
    """
    HTMLコンテンツを分析し、特定のセレクタの出現回数が条件を満たしていれば
    次ページが存在し、ページング処理が可能と判断する。
    """
    return (
        len(soup.select("div#article-contents")) == 1
        and len(soup.select("div.article-body")) == 1
        and len(soup.select("div.article-inner-pager")) == 2
        and len(soup.select("p.next > a.pagingNav")) == 1
    )


async def _process_paging(soup: BeautifulSoup, page_url: str) -> None:
    """
    次ページが出現しなくなるまで、 div#article-contents の子要素に次ページで獲得したコンテンツの内容を追加、拡張する
    """
    # 2ページ目以降のコンテンツを格納するリスト
    next_page_contents: List[PageElement] = []

    # 次ページのURLを取得
    next_page_link = soup.select_one("p.next > a.pagingNav")

    current_url = page_url

    while next_page_link and isinstance(next_page_link, Tag):
        href = next_page_link.get("href")
        if not href or not isinstance(href, str):
            break

        next_page_url = urljoin(current_url, href)
        logger.info(f"次のページを取得中: {next_page_url}")

        # 次のページのHTMLを取得
        next_page_html = await fetch_html_text(next_page_url, "mobile")
        if not next_page_html:
            logger.warning(f"ページの取得に失敗しました: {next_page_url}")
            break

        next_soup = BeautifulSoup(next_page_html, "html.parser")

        article_body = next_soup.select_one("div#article-contents, div.article-body")
        if article_body:
            next_page_contents.extend(article_body.contents)

        next_page_link = next_soup.select_one("p.next > a.pagingNav")
        current_url = next_page_url

    main_article_body = soup.select_one("div#article-contents, div.article-body")

    if main_article_body:
        for content in next_page_contents:
            main_article_body.append(content)

    for pager in soup.select("div.article-inner-pager"):
        pager.decompose()


async def process_article_html(
    html: str,
    page_url: str,
    remove_selectors_list: List[str],
    allow_hosts: Set[str],
    scraper_service: ScraperService,
) -> str:
    """Main processing function for an article's HTML."""
    soup = BeautifulSoup(html, "html.parser")

    _absolutize_paths(soup, page_url)

    if _check_paging_contents(soup):
        logger.debug("start _process_paging")
        await _process_paging(soup, page_url)

    _remove_scripts(soup, allow_hosts)
    _remove_selectors(soup, remove_selectors_list)

    await _convert_twitter_cards(soup, scraper_service)

    _unwrap_anchored_media(soup)
    _convert_video_js(soup)
    _unwrap_imgur(soup)
    _normalize_images(soup)
    _cleanup_empty_tags(soup)

    return str(soup.prettify(formatter="html5"))
