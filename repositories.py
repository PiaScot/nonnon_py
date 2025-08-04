from datetime import datetime, timezone
import os
from typing import List, Optional, Set
from urllib.parse import urlparse
from loguru import logger
from pydantic import TypeAdapter
from postgrest.exceptions import APIError
from postgrest import CountMethod
from supabase import create_async_client, AsyncClient
from models import Article, Site


MAX_ARTICLES = 10000
BATCH_SIZE = 500

ARTICLE_TABLE = "articles"
SITE_TABLE = "antena_sites"
BOOKMARK_TABLE = "bookmark_articles"

ALLOW_HOST_TABLE = "allowed_embed_hosts"
GENERAL_REMOVE_TAGS_TABLE = "general_remove_tags"
GET_SITES_TO_SCRAPE_RPC = "get_sites_to_scrape"


class SupabaseClientManager:
    """SupabaseのAsyncClientインスタンスを管理するシングルトン"""

    _client: AsyncClient | None = None

    async def get_client(self) -> AsyncClient:
        if self._client is None:
            self._client = await create_async_client(
                supabase_url=os.getenv("SUPABASE_URL") or "",
                supabase_key=os.getenv("SUPABASE_ROLE_KEY") or "",
            )
        return self._client


supabase_manager = SupabaseClientManager()


### CRUD ARTICLE_TABLE
async def _get_total_article_count() -> int:
    """現在の総記事数を取得する"""
    supabase = await supabase_manager.get_client()
    res = (
        await supabase.table(ARTICLE_TABLE)
        .select("id", count=CountMethod.exact)
        .execute()
    )
    assert res.count is not None, "Article count should not be None"
    return res.count


async def _update_article_content(article_id: int, new_content: str) -> bool:
    """指定の記事IDのコンテンツをnew_content に更新する"""
    supabase = await supabase_manager.get_client()
    res = (
        await supabase.table(ARTICLE_TABLE)
        .update({"content": new_content})
        .eq("id", article_id)
        .execute()
    )
    return True if res.data else False


async def _get_article_by_id(id: int) -> Optional[Article]:
    """指定のIDの記事を取得する"""
    supabase = await supabase_manager.get_client()
    res = (
        await supabase.table(ARTICLE_TABLE).select("*").eq("id", id).limit(1).execute()
    )
    if not res.data:
        return None

    article_data = res.data[0]

    article_adapter = TypeAdapter(Article)
    return article_adapter.validate_python(article_data)


async def _get_latest_n_articles(n: int) -> List[Article]:
    """
    公開日が最新の記事をn件取得する
    """
    supabase = await supabase_manager.get_client()
    res = (
        await supabase.table(ARTICLE_TABLE)
        .select("*")
        .order("pub_date", desc=True)
        .limit(n)
        .execute()
    )

    if not res.data:
        return []

    articles_adapter = TypeAdapter(List[Article])
    return articles_adapter.validate_python(res.data)


async def _fetch_oldest_article_ids(
    limit: int, exclude_ids: Set[int] = set()
) -> List[int]:
    """
    最も古い記事のIDを指定された件数だけ取得する。
    ブックマークされたIDは除外する。
    """
    client = await supabase_manager.get_client()
    query = client.table(ARTICLE_TABLE).select("id").order("created_at", desc=False)

    if exclude_ids:
        query = query.not_.in_("id", list(exclude_ids))

    res = await query.limit(limit).execute()
    return [article["id"] for article in res.data]


async def _delete_articles_by_ids(ids: List[int]) -> int:
    """IDのリストに基づいて記事を削除し、削除件数を返す"""
    if not ids:
        return 0

    supabase = await supabase_manager.get_client()
    total_deleted_count = 0
    logger.info(
        f"Starting deletion of {len(ids)} articles in batches of {BATCH_SIZE}..."
    )

    for i in range(0, len(ids), BATCH_SIZE):
        batch = ids[i : i + BATCH_SIZE]
        try:
            res = (
                await supabase.table(ARTICLE_TABLE).delete().in_("id", batch).execute()
            )
            deleted_in_batch = len(res.data)
            total_deleted_count += deleted_in_batch
            logger.info(
                f"Batch {i // BATCH_SIZE + 1}: Successfully deleted {deleted_in_batch} articles."
            )
        except APIError as e:
            logger.error(
                f"Batch {i // BATCH_SIZE + 1}: Failed to delete articles. Error: {str(e)}"
            )
            # エラーが発生しても処理を続行する
            continue
    logger.info(
        f"Finished deletion process. Total deleted: {total_deleted_count} articles."
    )
    return total_deleted_count


async def _insert_articles(articles: List[dict]) -> int:
    if not articles:
        return 0
    try:
        supabase = await supabase_manager.get_client()
        res = await supabase.table(ARTICLE_TABLE).insert(articles).execute()
        return len(res.data)
    except APIError as e:
        # 23505 はユニークキー制約違反のエラーコード
        if e.code == "23505":
            logger.warning(
                f"Skipped inserting articles due to duplicate entries. Details: {e.message}"
            )
            return 0  # 重複はエラーではないので、0件処理として正常に返す
        else:
            # それ以外のAPIエラーは、これまで通り例外を発生させる
            raise


async def _check_has_article_by_url(article_url: str) -> bool:
    """指定されたURLの記事がすでに存在するかどうかを確認します。"""
    supabase = await supabase_manager.get_client()
    try:
        res = (
            await supabase.table(ARTICLE_TABLE)
            .select("id", count=CountMethod.exact)
            .eq("url", article_url)
            .limit(1)
            .execute()
        )
        return res.count is not None and res.count > 0
    except APIError as e:
        logger.error(
            f"URL存在チェック中にDBエラーが発生しました: {e.message}, URL: {article_url}"
        )
        # チェック自体が失敗した場合は、安全のため「存在しない」とみなし、
        # 新規記事の処理をブロックしないようにします。
        return False


async def _get_random_articles_by_site_id(
    site_id: int, limit: int = 3
) -> List[Article]:
    """
    指定されたサイトIDの記事をランダムに取得する。
    ランダムサンプリングの要件を満たすために作成。
    """
    supabase = await supabase_manager.get_client()
    try:
        res = (
            await supabase.table(ARTICLE_TABLE)
            .select("*")
            .eq("site_id", site_id)
            .order("id", desc=False)
            .limit(100)
            .execute()
        )
        if not res.data:
            return []

        import random

        random.shuffle(res.data)

        articles_data = res.data[:limit]
        articles_adapter = TypeAdapter(List[Article])
        return articles_adapter.validate_python(articles_data)

    except Exception as e:
        logger.error(f"Failed to get random articles for site {site_id}: {e}")
        return []


### CRUD SITE


async def _update_site_last_access(site_id: int) -> None:
    """update site_id timestamp to current current time"""
    client = await supabase_manager.get_client()
    await (
        client.table(SITE_TABLE)
        .update({"last_access": datetime.now(timezone.utc).isoformat()})
        .eq("id", site_id)
        .execute()
    )


async def _fetch_site_by_id(site_id: int) -> Optional[Site]:
    """fetch site by arg site id"""
    client = await supabase_manager.get_client()
    res = (
        await client.table(SITE_TABLE).select("*").eq("id", site_id).limit(1).execute()
    )

    if not res.data:
        return None

    site_data = res.data[0]

    site_adapter = TypeAdapter(Site)
    return site_adapter.validate_python(site_data)


async def _get_all_sites() -> List[Site]:
    """
    antena_sitesテーブルから全てのサイト情報を取得する。
    サイトIDの昇順でソートする。
    """
    supabase = await supabase_manager.get_client()
    res = await supabase.table(SITE_TABLE).select("*").order("id", desc=False).execute()
    if not res.data:
        return []
    site_list_adapter = TypeAdapter(List[Site])
    return site_list_adapter.validate_python(res.data)


async def _get_site_by_url(url: str) -> Optional[Site]:
    """
    記事URLのドメイン情報からSite情報を抜き出す。
    """
    parsed_url = urlparse(url)
    domain = parsed_url.scheme + "://" + parsed_url.netloc
    logger.info(f"domain -> {domain}")
    supabase = await supabase_manager.get_client()
    res = (
        await supabase.table(SITE_TABLE)
        .select("*")
        .eq("url", domain)
        .limit(1)
        .execute()
    )

    if not res.data:
        return None

    site_adapter = TypeAdapter(Site)
    return site_adapter.validate_python(res.data[0])


### CRUD BOOKMARK_TABLE


async def _get_bookmarked_article_ids() -> Set[int]:
    """ブックマークされているすべての記事IDのセットを取得する"""
    client = await supabase_manager.get_client()
    res = await client.table(BOOKMARK_TABLE).select("id").execute()
    if not res.data:
        return set()
    return {item["id"] for item in res.data}


async def _get_bookmarked_articles() -> List[Article]:
    """ブックマーク済み記事の一覧を取得する"""
    client = await supabase_manager.get_client()
    res = await client.table(BOOKMARK_TABLE).select("*").execute()
    if not res.data:
        return []

    article_list_adapter = TypeAdapter(List[Article])
    return article_list_adapter.validate_python(res.data)


async def _get_bookmarked_articles_by_site(site_id: int) -> List[Article]:
    """指定されたサイトIDのブックマーク済み記事を取得する"""
    client = await supabase_manager.get_client()
    res = (
        await client.table(BOOKMARK_TABLE).select("*").eq("site_id", site_id).execute()
    )
    if not res.data:
        return []

    article_list_adapter = TypeAdapter(List[Article])
    return article_list_adapter.validate_python(res.data)


### CRUD ALLOW_HOST


async def _get_allowed_hosts() -> Set[str]:
    """Fetches the set of allowed embed hosts from the database."""
    supabase = await supabase_manager.get_client()
    res = await supabase.table(ALLOW_HOST_TABLE).select("hostname").execute()
    assert res.data is not None, f"{ALLOW_HOST_TABLE} should not be None"

    return {h["hostname"] for h in res.data}


### CRUD GENERAL_REMOVE_TAGS


async def _get_general_remove_tags() -> List[str]:
    """Fetches the list of general remove tags from the database."""
    supabase = await supabase_manager.get_client()
    res = await supabase.table(GENERAL_REMOVE_TAGS_TABLE).select("selector").execute()
    assert res.data is not None, f"{GENERAL_REMOVE_TAGS_TABLE} should not be None"

    return [t["selector"] for t in res.data] if res.data else []


### RPC
async def _get_sites_to_scrape() -> List[Site]:
    """Fetches the list of sites that need to be scraped."""
    supabase = await supabase_manager.get_client()
    res = await supabase.rpc(GET_SITES_TO_SCRAPE_RPC).execute()

    if not res.data:
        return []

    site_list_adapter = TypeAdapter(List[Site])
    return site_list_adapter.validate_python(res.data)
