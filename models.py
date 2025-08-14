from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class ScrapeOptions(BaseModel):
    remove_selector_tags: List[str] = Field(default_factory=list)
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


class BakusaiResInfo(BaseModel):
    res_id: int
    reply_to_id: Optional[int] = None
    comment_time: datetime
    comment_text: str
    typed_name: str


class BakusaiThreadInfo(BaseModel):
    id: Optional[int] = None
    category: Optional[str] = None
    name: str
    number: int
    link: str
    last_commented: datetime
    viewer: int
    res_count: int
    comments: List[BakusaiResInfo] = Field(default_factory=list)
