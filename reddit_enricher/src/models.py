from pydantic import BaseModel, Field
from typing import Optional, List, Literal
from datetime import datetime
from enum import Enum


class Language(str, Enum):
    ENGLISH = "en"
    FRENCH = "fr"
    ARABIC = "ar"
    DARIJA_ARABIC = "darija_ar"
    DARIJA_LATIN = "darija_lat"


class Entity(BaseModel):
    name: str
    type: str


class Enrichment(BaseModel):
    languages: Optional[List[Language]] = None
    translation: Optional[str] = None
    sentiment_score: Optional[float] = None
    intent: Optional[str] = None
    topics: Optional[List[str]] = None
    entities: Optional[List[Entity]] = None


class RedditEvent(BaseModel):
    event_id: str
    event_type: Literal["POST", "COMMENT"]
    posted_in_subreddit: str
    author: str
    url: str
    title: Optional[str] = None
    content: Optional[str] = None
    timestamp: datetime

    # Media fields
    has_media: bool = False
    media_urls: List[str] = Field(default_factory=list)

    # Engagement metrics
    score: Optional[int] = None
    upvote_ratio: Optional[float] = None
    num_comments: Optional[int] = None

    # Post metadata
    is_crosspost: bool = False
    original_subreddit: Optional[str] = None

    # Enrichment (added by enricher service)
    enrichment: Optional[Enrichment] = None
