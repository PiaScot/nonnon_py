from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class ScrapeOptions(BaseModel):
    removeSelectorTags: List[str] = Field(default_factory=list)
    display_mode: Literal["in_app", "direct_link"] = "in_app"


class Site(BaseModel):
    id: int
    url: Optional[str] = None
    domain: Optional[str] = None
    title: Optional[str] = None
    rss: Optional[str] = None
    category: Optional[str] = None
    last_access: str
    duration_access: Optional[int] = None
    scrape_options: Optional[ScrapeOptions] = None


class Article(BaseModel):
    id: Optional[int] = None
    site_id: int
    title: str
    url: str
    category: Optional[str] = None
    content: str
    pub_date: str
    thumbnail: str = ""
    created_at: Optional[str] = None
