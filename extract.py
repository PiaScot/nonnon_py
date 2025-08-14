import re
from typing import List, Optional, Set
from urllib.parse import urljoin, urlparse, parse_qs
from bs4 import BeautifulSoup, Tag, element
from bs4.element import NavigableString, PageElement
from logger import logger
from playw import ScraperService
from utils import fetch_html_text
import config


def _extract_imgur_id(wrapper_tag: element.Tag) -> Optional[str]:
    """
    Attempts to extract an Imgur image ID from an specified tag's attributes.
    Tries src, id, data-id, and class attributes in order of reliability.
    """
    src = wrapper_tag.get("src")
    if isinstance(src, str):
        match = re.search(r"imgur\.com/([a-zA-Z0-9]{5,})", src)
        if match:
            return match.group(1)

    id_attr = wrapper_tag.get("id")
    if isinstance(id_attr, str):
        parts = id_attr.split("-")
        if len(parts) > 1 and len(parts[-1]) >= 5 and parts[-1].isalnum():
            return parts[-1]

    id_attr = wrapper_tag.get("data-id")
    if isinstance(id_attr, str):
        parts = id_attr.split("-")
        if len(parts) > 1 and len(parts[-1]) >= 5 and parts[-1].isalnum():
            return parts[-1]

    class_list = wrapper_tag.get("class", "")
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
            "alt": f"imgur ID:{img_id} image",
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

        img_id_val = blockquote.get("data-id")

        if isinstance(img_id_val, str) and img_id_val.strip():
            img_id = img_id_val.strip()

            new_img = _create_imgur_img_tag(soup, img_id)
            blockquote.replace_with(new_img)


def _normalize_iframes(soup: BeautifulSoup, allow_hosts: Set[str]) -> None:
    """
    Finds all iframes from allowed hosts and makes them responsive.
    """
    # src属性を持つすべてのiframeを見つける
    for iframe in soup.find_all("iframe", src=True):
        if not isinstance(iframe, element.Tag):
            continue

        src = iframe.get("src")
        if not isinstance(src, str) or not src.strip():
            continue

        try:
            # srcからホスト名を取得
            hostname = urlparse(src.strip()).hostname

            # ホスト名が存在し、かつ許可されたホストのリストに含まれているかチェック
            if hostname and hostname in allow_hosts:
                # logger.info(f"_normalize_iframes: hostname => {hostname}")
                if hostname == "platform.twitter.com":
                    # logger.info(
                    #     "Skip _normalize_iframes due to twitter iframe should be process with playw.render_twitter_card"
                    # )
                    continue

                logger.info(f"Normalizing iframe from allowed host: {hostname}")

                # width, height, style 属性をレスポンシブな値に上書き・設定する
                iframe["width"] = "100%"
                iframe["height"] = "auto"

                # アスペクト比を16:9に設定し、レスポンシブ対応させる
                # 多くの動画埋め込みで一般的な比率のため、デフォルトとして採用
                iframe["style"] = "aspect-ratio: 16 / 9; width: 100%; height: auto;"

        except Exception as e:
            logger.warning(f"Could not parse iframe src: {src} - Error: {e}")


def _find_valid_media_url(tag: element.Tag) -> str:
    """Finds a valid media URL from lazy loading attributes or src."""
    try:
        for attr in config.LAZY_ATTRS:
            lazy_src = tag.get(attr)
            if isinstance(lazy_src, str):
                clean_src = lazy_src.strip()
                if config.MEDIA_RE.search(clean_src):
                    return clean_src

        src = tag.get("src")
        if isinstance(src, str):
            clean_src = src.strip()
            if config.MEDIA_RE.search(clean_src) and not clean_src.startswith(
                "data:image"
            ):
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
    """
    Normalizes all media tags (img, video).
    Finds non-formatted <img> tags and replaces them with appropriate
    <video> or <img> tags based on the source URL.
    """
    # 関数名が実態と合わなくなるため、docstringも修正するとより親切です。

    for img in soup.find_all("img", class_=lambda c: c != "my-formatted"):
        if not isinstance(img, element.Tag):
            continue

        src = _find_valid_media_url(img)
        if not src:
            img.decompose()
            continue

        # --- ここから修正 ---

        # URLがビデオかどうかを判定
        if config.VIDEO_RE.search(src):
            # ビデオの場合、<video>タグを生成する
            new_tag = soup.new_tag(
                "video",
                attrs={
                    "src": src,
                    "controls": "",  # controls属性を追加
                    "playsinline": "",
                    "style": "width:100%;height:auto;display:block;",
                    "class": "my-formatted",
                    "loading": "lazy",
                    "referrerpolicy": "no-referrer",
                },
            )
        else:
            # 画像の場合、既存のロジック通り<img>タグを生成する
            new_tag = soup.new_tag(
                "img",
                attrs={
                    "src": src,
                    "loading": "lazy",
                    "referrerpolicy": "no-referrer",
                    "style": "max-width:100%;height:auto;display:block",
                    "class": "my-formatted",
                },
            )

        img.replace_with(new_tag)


def _cleanup_empty_tags(soup: BeautifulSoup) -> None:
    """Removes <p> tags that are visually empty."""
    for p in soup.find_all("p"):
        if not isinstance(p, element.Tag):
            continue

        if not p.text.strip() and not p.find(("a", "img", "video", "iframe", "input")):
            p.decompose()


def _collapse_excessive_brs(soup: BeautifulSoup, max_consecutive: int = 2) -> None:
    """
    Finds consecutive <br> tags and collapses them to a specified maximum.
    """
    # すべての <br> タグを一度に見つける
    br_tags = soup.find_all("br")

    # 連続する<br>タグのシーケンスを検出して処理
    if not br_tags:
        return

    # 連続する<br>タグのグループを保持するリスト
    consecutive_groups = []
    current_group = [br_tags[0]]

    for i in range(1, len(br_tags)):
        prev_br = br_tags[i - 1]
        current_br = br_tags[i]

        # 間に他の要素がなく、隣接しているかチェック
        # find_next_sibling() は間の空白文字などをスキップしてくれる
        if prev_br.find_next_sibling() == current_br:
            current_group.append(current_br)
        else:
            # 連続が途切れたら、グループを保存して新しいグループを開始
            if len(current_group) > 1:
                consecutive_groups.append(current_group)
            current_group = [current_br]

    # 最後のグループをチェック
    if len(current_group) > 1:
        consecutive_groups.append(current_group)

    # 各グループで、最大数を超える<br>タグを削除
    for group in consecutive_groups:
        if len(group) > max_consecutive:
            # 最初のmax_consecutive個を残し、残りを削除
            for br_to_remove in group[max_consecutive:]:
                br_to_remove.decompose()


async def _convert_twitter_cards(
    soup: BeautifulSoup, scraper_service: ScraperService
) -> None:
    """
    Finds all un-rendered twitter-tweets and replaces them one by one.
    The widgets.js script is injected programmatically as it may not exist in the source HTML.
    """

    blockquotes_to_process = list(soup.find_all("blockquote", class_="twitter-tweet"))
    if not blockquotes_to_process:
        return

    twitter_script_url = "https://platform.twitter.com/widgets.js"
    script_html_to_inject = (
        f'<script async src="{twitter_script_url}" charset="utf-8"></script>'
    )

    # logger.info(
    #     f"Found {len(blockquotes_to_process)} twitter-tweet blockquotes to process."
    # )

    for blockquote in blockquotes_to_process:
        if not isinstance(blockquote, Tag):
            continue

        link_tag = blockquote.find("a")
        if link_tag and isinstance(link_tag, Tag) and not link_tag.get_text(strip=True):
            href = link_tag.get("href")
            if isinstance(href, str):
                link_tag.string = href

        rendered_card_html = await scraper_service.render_twitter_card(
            str(blockquote), script_html_to_inject
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
    メディアへのリンクやメディアを内包する要素を、単一の<img>または<video>タグに置き換える。
    """
    for elem in soup.select("a, p, div.wp-video"):
        if not isinstance(elem, Tag):
            continue

        if elem.find("iframe"):
            continue

        url = ""

        if elem.name == "a":
            href_val = elem.get("href")
            href = ""
            if isinstance(href_val, str):
                href = href_val.strip()
            url_found = False

            try:
                if href:
                    parsed_url = urlparse(href)
                    params = parse_qs(parsed_url.query)
                    for values in params.values():
                        for value in values:
                            if value.lower().startswith(
                                "http"
                            ) and config.MEDIA_RE.search(value):
                                url = value
                                url_found = True
                                break
                        if url_found:
                            break
            except Exception:
                pass

            if not url_found and config.MEDIA_RE.search(href):
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
                if text_content.lower().startswith("http") and config.MEDIA_RE.search(
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

        # --- ここから修正 ---
        if url and config.MEDIA_RE.search(url):
            # URLのパス部分を取得し、動画拡張子で終わるかチェック
            path_lower = urlparse(url).path.lower()
            is_video_by_extension = path_lower.endswith(
                (".mp4", ".webm", ".mov", ".ogv")
            )

            # 正規表現での判定、または拡張子での判定がTrueならビデオとみなす
            if config.VIDEO_RE.search(url) or is_video_by_extension:
                new_tag = soup.new_tag(
                    "video",
                    attrs={
                        "src": url,
                        "controls": "",
                        "playsinline": "",
                        "style": "width:100%;height:auto;display:block;",
                        "class": "my-formatted",
                        "loading": "lazy",
                        "referrerpolicy": "no-referrer",
                    },
                )
            else:
                new_tag = soup.new_tag(
                    "img",
                    attrs={
                        "src": url,
                        "loading": "lazy",
                        "referrerpolicy": "no-referrer",
                        "style": "max-width:100%;height:auto;display:block",
                        "class": "my-formatted",
                    },
                )
            elem.replace_with(new_tag)

    for video_el in soup.select("video:has(source)"):
        if not isinstance(video_el, Tag) or video_el.get("src"):
            continue

        source = video_el.find("source", src=True)
        if source and isinstance(source, Tag):
            src_val = source.get("src")
            if isinstance(src_val, str):
                source_src = src_val.strip()

                video_el["src"] = source_src
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
    if not soup:
        logger.error("Null soup")
        return False

    div_contents = len(soup.select("div#article-contents"))
    div_article_bodies = len(soup.select("div.article-body"))
    # div_inner_pagers = len(soup.select("div.article-inner-pager"))
    a_pagingNav = len(soup.select("p.next > a.pagingNav"))

    # logger.info(f"div#article-contents => {div_contents}")
    # logger.info(f"div_article_bodies => {div_article_bodies}")
    # logger.info(f"div_inner_pagers => {div_inner_pagers}")
    # logger.info(f"a_pagingNav => {a_pagingNav}")

    return div_contents >= 1 and div_article_bodies >= 1 and a_pagingNav >= 1


async def _process_paging(soup: BeautifulSoup, page_url: str) -> None:
    """
    次ページが出現しなくなるまで、 div#article-contents の子要素に次ページで獲得したコンテンツの内容を追加、拡張する
    """
    next_page_contents: List[PageElement] = []

    next_page_link = soup.select_one("p.next > a.pagingNav")

    current_url = page_url

    while next_page_link and isinstance(next_page_link, Tag):
        href = next_page_link.get("href")
        if not href or not isinstance(href, str):
            break

        next_page_url = urljoin(current_url, href)
        # logger.info(f"次のページを取得中: {next_page_url}")

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
    _normalize_iframes(soup, allow_hosts)
    _normalize_images(soup)
    _cleanup_empty_tags(soup)
    _collapse_excessive_brs(soup)

    return str(soup.prettify(formatter="html5"))
