from logger import logger
from repositories import ArticleRepository, BookmarkRepository
import config


async def maintain_article_limit() -> None:
    """
    記事数が上限を超えないように維持する。
    ブックマークされた記事は削除しない。
    """
    logger.info("Starting to check and maintain article limit...")
    try:
        article_repo = ArticleRepository()
        bookmark_repo = BookmarkRepository()

        all_count = await article_repo.get_total_count()
        if all_count <= config.MAX_ARTICLES:
            logger.info(
                "The number of articles is within the limit. No cleanup needed."
            )
            return

        logger.info(
            f"Article count ({all_count}) exceeds limit ({config.MAX_ARTICLES})."
        )

        articles_to_delete_count = all_count - config.MAX_ARTICLES
        bookmarked_ids = await bookmark_repo.get_bookmarked_ids()

        logger.info(f"Found {len(bookmarked_ids)} bookmarked articles to exclude.")
        logger.info(f"Need to delete {articles_to_delete_count} articles.")

        stale_article_ids = await article_repo.fetch_oldest_ids(
            articles_to_delete_count, exclude_ids=bookmarked_ids
        )

        if not stale_article_ids:
            logger.info("No un-bookmarked old articles found to delete.")
            return

        logger.info(f"Found {len(stale_article_ids)} stale articles to delete.")
        delete_count = await article_repo.delete_by_ids(stale_article_ids)

        if delete_count > 0:
            logger.info(f"Successfully deleted {delete_count} articles.")
        else:
            logger.warning(f"Failed to delete article IDs: {stale_article_ids}")

    except Exception as e:
        logger.error(f"Error in maintain_article_limit: {str(e)}", exc_info=True)
        raise
