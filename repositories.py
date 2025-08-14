import random
from datetime import datetime, timezone
from typing import Any, List, Optional, Set
from urllib.parse import urlparse

from loguru import logger
from pydantic import TypeAdapter
from postgrest import CountMethod
from postgrest.exceptions import APIError
from supabase import AsyncClient, create_async_client

import config
from models import Article, BakusaiResInfo, BakusaiThreadInfo, Site


class SupabaseClientManager:
    _client: AsyncClient | None = None

    async def get_client(self) -> AsyncClient:
        if self._client is None:
            self._client = await create_async_client(
                supabase_url=config.SUPABASE_URL,
                supabase_key=config.SUPABASE_ROLE_KEY,
            )
        return self._client


supabase_manager = SupabaseClientManager()


class BaseRepository:
    def __init__(self, table_name: str):
        self.table_name = table_name

    async def _get_client(self) -> AsyncClient:
        return await supabase_manager.get_client()


class ArticleRepository(BaseRepository):
    def __init__(self):
        super().__init__(config.ARTICLE_TABLE)

    async def get_total_count(self) -> int:
        client = await self._get_client()
        res = (
            await client.table(self.table_name)
            .select("id", count=CountMethod.exact)
            .execute()
        )
        return res.count or 0

    async def update_content(self, article_id: int, new_content: str) -> bool:
        client = await self._get_client()
        res = (
            await client.table(self.table_name)
            .update({"content": new_content})
            .eq("id", article_id)
            .execute()
        )
        return bool(res.data)

    async def get_by_id(self, article_id: int) -> Optional[Article]:
        client = await self._get_client()
        res = (
            await client.table(self.table_name)
            .select("*")
            .eq("id", article_id)
            .limit(1)
            .execute()
        )
        if not res.data:
            return None
        return TypeAdapter(Article).validate_python(res.data[0])

    async def get_latest(self, n: int) -> List[Article]:
        client = await self._get_client()
        res = (
            await client.table(self.table_name)
            .select("*")
            .order("pub_date", desc=True)
            .limit(n)
            .execute()
        )
        return TypeAdapter(List[Article]).validate_python(res.data) if res.data else []

    async def fetch_oldest_ids(
        self, limit: int, exclude_ids: Set[int] = set()
    ) -> List[int]:
        client = await self._get_client()
        query = (
            client.table(self.table_name).select("id").order("created_at", desc=False)
        )
        if exclude_ids:
            query = query.not_.in_("id", list(exclude_ids))
        res = await query.limit(limit).execute()
        return [article["id"] for article in res.data]

    async def delete_by_ids(self, ids: List[int]) -> int:
        if not len(ids):
            return 0
        client = await self._get_client()
        total_deleted = 0
        for i in range(0, len(ids), config.BATCH_SIZE):
            batch = ids[i : i + config.BATCH_SIZE]
            try:
                res = (
                    await client.table(self.table_name)
                    .delete()
                    .in_("id", batch)
                    .execute()
                )
                total_deleted += len(res.data)
            except APIError as e:
                logger.error(f"Failed to delete articles batch: {e}")
        return total_deleted

    async def insert_many(self, articles: List[dict]) -> int:
        if not articles:
            return 0
        client = await self._get_client()
        try:
            res = await client.table(self.table_name).insert(articles).execute()
            return len(res.data)
        except APIError as e:
            if e.code == "23505":
                logger.warning(f"Skipped inserting duplicate articles: {e.message}")
                return 0
            raise

    async def check_exists_by_url(self, url: str) -> bool:
        client = await self._get_client()
        try:
            res = (
                await client.table(self.table_name)
                .select("id", count=CountMethod.exact)
                .eq("url", url)
                .limit(1)
                .execute()
            )
            return (res.count or 0) > 0
        except APIError as e:
            logger.error(f"DB error checking article existence by URL: {e}")
            return False

    async def get_random_by_site_id(
        self, site_id: int, limit: int = 3
    ) -> List[Article]:
        client = await self._get_client()
        res = (
            await client.table(self.table_name)
            .select("*")
            .eq("site_id", site_id)
            .limit(100)
            .execute()
        )
        if not res.data:
            return []

        random.shuffle(res.data)
        return TypeAdapter(List[Article]).validate_python(res.data[:limit])

    async def get_latest_by_site_id(self, site_id: int, n: int) -> List[Article]:
        client = await self._get_client()
        res = (
            await client.table(self.table_name)
            .select("*")
            .eq("site_id", site_id)
            .order("pub_date", desc=True)
            .limit(n)
            .execute()
        )
        return TypeAdapter(List[Article]).validate_python(res.data) if res.data else []


class SiteRepository(BaseRepository):
    def __init__(self):
        super().__init__(config.SITE_TABLE)

    async def update_last_access(self, site_id: int) -> None:
        client = await self._get_client()
        await (
            client.table(self.table_name)
            .update({"last_access": datetime.now(timezone.utc).isoformat()})
            .eq("id", site_id)
            .execute()
        )

    async def get_by_id(self, site_id: int) -> Optional[Site]:
        client = await self._get_client()
        res = (
            await client.table(self.table_name)
            .select("*")
            .eq("id", site_id)
            .limit(1)
            .execute()
        )
        if not res.data:
            return None
        return TypeAdapter(Site).validate_python(res.data[0])

    async def get_all(self) -> List[Site]:
        client = await self._get_client()
        res = await client.table(self.table_name).select("*").order("id").execute()
        return TypeAdapter(List[Site]).validate_python(res.data) if res.data else []

    async def get_by_url(self, url: str) -> Optional[Site]:
        client = await self._get_client()
        domain = urlparse(url).netloc
        res = (
            await client.table(self.table_name)
            .select("*")
            .eq("domain", domain)
            .limit(1)
            .execute()
        )
        if not res.data:
            return None
        return TypeAdapter(Site).validate_python(res.data[0])

    async def get_sites_to_scrape(self) -> List[Site]:
        client = await self._get_client()
        res = await client.rpc(config.GET_SITES_TO_SCRAPE_RPC).execute()
        return TypeAdapter(List[Site]).validate_python(res.data) if res.data else []


class BookmarkRepository(BaseRepository):
    def __init__(self):
        super().__init__(config.BOOKMARK_TABLE)

    async def get_bookmarked_ids(self) -> Set[int]:
        client = await self._get_client()
        res = await client.table(self.table_name).select("id").execute()
        return {item["id"] for item in res.data} if res.data else set()

    async def get_bookmarked_articles(self) -> List[Article]:
        client = await self._get_client()
        res = await client.table(self.table_name).select("*").execute()
        return TypeAdapter(List[Article]).validate_python(res.data) if res.data else []

    async def get_bookmarked_articles_by_site(self, site_id: int) -> List[Article]:
        client = await self._get_client()
        res = (
            await client.table(self.table_name)
            .select("*")
            .eq("site_id", site_id)
            .execute()
        )
        return TypeAdapter(List[Article]).validate_python(res.data) if res.data else []


class ConfigRepository:
    async def _get_client(self) -> AsyncClient:
        return await supabase_manager.get_client()

    async def get_allowed_hosts(self) -> Set[str]:
        client = await self._get_client()
        res = await client.table(config.ALLOW_HOST_TABLE).select("hostname").execute()
        return {h["hostname"] for h in res.data} if res.data else set()

    async def get_general_remove_tags(self) -> List[str]:
        client = await self._get_client()
        res = (
            await client.table(config.GENERAL_REMOVE_TAGS_TABLE)
            .select("selector")
            .execute()
        )
        return [t["selector"] for t in res.data] if res.data else []


class BakusaiRepository(BaseRepository):
    def __init__(self):
        super().__init__(config.BAKUSAI_THREAD_TABLE)

    async def _get_thread_by_link_base(
        self, thread_link: str, select_query: str = "*"
    ) -> Optional[Any]:
        """スレッドをリンクで検索する共通ロジック"""
        try:
            supabase = await self._get_client()
            response = (
                await supabase.table(self.table_name)
                .select(select_query)
                .eq("link", thread_link)
                .single()
                .execute()
            )
            return response.data
        except APIError as e:
            if e.code == "PGRST116":  # 見つからないのは正常なケース
                return None
            logger.error(
                f"DB search error (_get_thread_by_link_base for link {thread_link}): {e}"
            )
            raise

    async def get_thread_by_link(self, thread_link: str) -> Optional[dict]:
        """idとres_countのみ取得"""
        return await self._get_thread_by_link_base(thread_link, "id, res_count")

    async def get_thread_info_by_link(
        self, thread_link: str
    ) -> Optional[BakusaiThreadInfo]:
        """スレッドの全情報を取得"""
        data = await self._get_thread_by_link_base(thread_link, "*")
        if data:
            return TypeAdapter(BakusaiThreadInfo).validate_python(data)
        return None

    async def create_thread(self, thread: BakusaiThreadInfo) -> Optional[int]:
        try:
            supabase = await self._get_client()
            insert_data = {
                "name": thread.name,
                "category": thread.category,
                "number": thread.number,
                "link": thread.link,
                "last_commented": thread.last_commented.isoformat(),
                "viewer": thread.viewer,
                "res_count": thread.res_count,
            }
            response = (
                await supabase.table(self.table_name).insert(insert_data).execute()
            )
            return response.data[0]["id"] if response.data else None
        except APIError as e:
            logger.error(f"DB insert error (create_thread): {e}")
            raise

    async def get_max_res_number(self, thread_id: int) -> int:
        try:
            supabase = await self._get_client()
            response = (
                await supabase.table(config.BAKUSAI_RES_TABLE)
                .select("res_number")
                .eq("thread_id", thread_id)
                .order("res_number", desc=True)
                .limit(1)
                .single()
                .execute()
            )
            return response.data["res_number"] if response.data else 0
        except APIError as e:
            if e.code == "PGRST116":  # まだコメントがない場合
                return 0
            logger.error(f"DB search error (get_max_res_number): {e}")
            raise

    async def get_res_count(self, thread_id: int) -> int:
        """指定されたthread_idに紐づくレスの総数をDBから取得する"""
        try:
            supabase = await self._get_client()
            response = (
                await supabase.table(config.BAKUSAI_RES_TABLE)
                .select("id", count=CountMethod.exact)
                .eq("thread_id", thread_id)
                .execute()
            )
            return response.count if response.count is not None else 0
        except APIError as e:
            logger.error(f"DB error counting comments (get_res_comment_count): {e}")
            raise

    async def bulk_insert_res_comments(
        self, thread_id: int, comments: List[BakusaiResInfo]
    ) -> int:
        if not comments:
            return 0

        supabase = await self._get_client()
        total_inserted_count = 0

        # res_idの昇順でソートしてから挿入する
        comments.sort(key=lambda r: r.res_id)

        for i in range(0, len(comments), config.BATCH_SIZE):
            chunk = comments[i : i + config.BATCH_SIZE]
            insert_rows = [
                {
                    "thread_id": thread_id,
                    "res_number": res.res_id,
                    "reply_to_res_number": res.reply_to_id,
                    "comment_time": res.comment_time.isoformat(),
                    "body": res.comment_text,
                    "name": res.typed_name,
                }
                for res in chunk
            ]

            try:
                response = (
                    await supabase.table(config.BAKUSAI_RES_TABLE)
                    .insert(insert_rows)
                    .execute()
                )
                total_inserted_count += len(response.data)
            except APIError as e:
                logger.error(f"Comment batch save error: {e}")
                # バッチ処理中にエラーが起きたら、そこで処理を中断
                break

        return total_inserted_count

    async def update_thread_stats(
        self, thread_id: int, res_count: int, viewer: int, last_commented: datetime
    ) -> None:
        try:
            supabase = await self._get_client()
            update_data = {
                "res_count": res_count,
                "viewer": viewer,
                "last_commented": last_commented.isoformat(),
            }
            await (
                supabase.table(self.table_name)
                .update(update_data)
                .eq("id", thread_id)
                .execute()
            )
        except APIError as e:
            logger.error(f"Thread stats update error: {e}")
            raise


class CategoryRepository(BaseRepository):
    def __init__(self):
        super().__init__(config.CATEGORY_TABLE)

    async def get_id_by_label(self, label: str) -> Optional[str]:
        """
        カテゴリのラベル名からIDを取得します。
        見つからない場合はNoneを返します。
        """
        client = await self._get_client()
        try:
            res = (
                await client.table(self.table_name)
                .select("id")
                .eq("label", label)
                .limit(1)
                .single()
                .execute()
            )
            return res.data["id"] if res.data else None
        except APIError as e:
            if e.code == "PGRST116":  # Not a single row was found
                logger.info(f"Category with label '{label}' not found.")
                return None
            logger.error(f"Error fetching category ID for label '{label}': {e}")
            raise
