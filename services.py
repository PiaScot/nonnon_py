from logger import logger

from repositories import (
    _get_total_article_count,
    _get_bookmarked_article_ids,
    _fetch_oldest_article_ids,
    _delete_articles_by_ids,
)


MAX_ARTICLES = 10000
BATCH_SIZE = 500


async def maintain_article_limit() -> None:
    """
    記事数が上限を超えないように維持する。
    ブックマークされた記事は削除しない。
    """
    logger.info("Starting to check and maintain article limit...")
    try:
        all_count = await _get_total_article_count()
        if all_count <= MAX_ARTICLES:
            logger.info(
                "The number of articles is within the limit. No cleanup needed."
            )
            return

        logger.info(f"article all num => {all_count}")

        articles_to_delete_count = all_count - MAX_ARTICLES
        bookmarked_ids = await _get_bookmarked_article_ids()

        logger.info(f"bookmarked_ids => {bookmarked_ids}")
        logger.info(f"articles_to_delete_count => {articles_to_delete_count}")

        stale_articles_id = await _fetch_oldest_article_ids(
            articles_to_delete_count, exclude_ids=bookmarked_ids
        )

        logger.info(f"stale_articles_id => {stale_articles_id}")
        if not stale_articles_id:
            logger.info("No un-bookmarked old articles found to delete.")
            return

        delete_count = await _delete_articles_by_ids(stale_articles_id)
        if delete_count > 0:
            logger.info(f"Successfully deleted {delete_count} articles.")
        else:
            logger.error(f"Failed to delete article ids: {stale_articles_id}")

    except Exception as e:
        logger.error(f"Error in maintain_article_limit: {str(e)}", exc_info=True)
        raise
