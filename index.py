import asyncio
from typing import List, Set, NamedTuple

from logger import logger
from models import Site
from repositories import (
    ArticleRepository,
    SiteRepository,
    ConfigRepository,
)
from scraper import scrape_site
from services import maintain_article_limit
from playw import ScraperService


class ScrapingContext(NamedTuple):
    sites_to_scrape: List[Site]
    general_remove_tags: List[str]
    allowed_hosts: Set[str]


async def prepare(
    site_repo: SiteRepository, config_repo: ConfigRepository
) -> ScrapingContext | None:
    """DBからスクレイピングに必要なデータをすべて取得・準備する"""
    logger.info("Preparing data for scraping...")

    allowed_hosts = await config_repo.get_allowed_hosts()
    logger.info(f"Loaded {len(allowed_hosts)} allowed hosts.")

    general_remove_tags = await config_repo.get_general_remove_tags()
    logger.info(f"Loaded {len(general_remove_tags)} general remove tags.")

    sites_to_scrape = await site_repo.get_sites_to_scrape()
    if not sites_to_scrape:
        logger.info("No sites to scrape at this time.")
        return None

    logger.info(f"Found {len(sites_to_scrape)} sites to scrape.")
    return ScrapingContext(sites_to_scrape, general_remove_tags, allowed_hosts)


async def run() -> None:
    """スクレイピング処理全体を実行するメイン関数"""
    logger.info("🚀 Starting scraping process...")

    scraper_service = ScraperService()
    article_repo = ArticleRepository()
    site_repo = SiteRepository()
    config_repo = ConfigRepository()

    try:
        await scraper_service.start()

        context = await prepare(site_repo, config_repo)
        if context is None:
            return

        sites = context.sites_to_scrape
        general_tags = context.general_remove_tags
        hosts = context.allowed_hosts

        logger.info(
            f"Found {len(sites)} sites to scrape. Starting parallel processing..."
        )

        tasks = [
            asyncio.create_task(
                scrape_site_and_update_timestamp(
                    scraper_service, site, general_tags, hosts, article_repo, site_repo
                )
            )
            for site in sites
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)
        log_scraping_results(results, sites)

        await maintain_article_limit()

    except Exception as e:
        logger.critical(f"❌ Fatal error in run: {str(e)}", exc_info=True)
    finally:
        await scraper_service.stop()
        logger.info("🔚 Scraping process finished.")


async def scrape_site_and_update_timestamp(
    scraper_service: ScraperService,
    site: Site,
    general_tags: List[str],
    allowed_hosts: Set[str],
    article_repo: ArticleRepository,
    site_repo: SiteRepository,
) -> int:
    """単一サイトをスクレイピングし、最終アクセス時刻を更新する"""
    try:
        inserted_count, got_articles_num = await scrape_site(
            scraper_service, site, general_tags, allowed_hosts, article_repo
        )
        if inserted_count >= 0:
            logger.info(
                f"{site.id} {site.title}: Inserted data({inserted_count}) got articles({got_articles_num})"
            )
            await site_repo.update_last_access(site.id)
            logger.info(
                f"✅ Successfully scraped and updated timestamp for site ID: {site.id} ({site.title})"
            )
        else:
            logger.info("No data to insert table")
        return site.id
    except Exception as e:
        logger.error(
            f"❌ Failed to process site {site.id} ({site.title}): {e}", exc_info=False
        )
        raise


def log_scraping_results(results: List, sites: List[Site]) -> None:
    """スクレイピング結果のサマリーをログに出力する"""
    logger.info("--- Scraping Results ---")

    success_ids = {r for r in results if isinstance(r, int)}
    successful_sites = [s.title for s in sites if s.id in success_ids and s.title]
    failure_count = len(sites) - len(success_ids)

    success_msg = f"✅ Success ({len(success_ids)})"
    if successful_sites:
        success_msg += f": {', '.join(successful_sites)}"
    logger.info(success_msg)

    if failure_count > 0:
        failed_sites_map = {
            s.id: s.title or f"(ID:{s.id})" for s in sites if s.id not in success_ids
        }
        logger.error(f"❌ Failure ({failure_count}):")
        logger.error(f"  Failed sites: {', '.join(failed_sites_map.values())}")

        logger.error("--- Failure Details ---")
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                site = sites[i]
                site_identifier = site.title or f"ID:{site.id}"
                logger.opt(exception=result).error(
                    f"  - Exception occurred for site: '{site_identifier}'"
                )
        logger.error("-----------------------")

    logger.info("------------------------")
    logger.info(
        f"✨ Process summary. Success: {len(success_ids)}, Failure: {failure_count}."
    )


if __name__ == "__main__":
    asyncio.run(run())
