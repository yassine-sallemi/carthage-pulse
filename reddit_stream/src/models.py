from pydantic import BaseModel, Field
from typing import Optional, List, Literal
from datetime import datetime


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

    @staticmethod
    def _extract_media(data: dict, event_type: str) -> List[str]:
        media_urls = []
        if event_type == "POST":
            if data.get("is_video"):
                vid_url = (
                    data.get("secure_media", {})
                    .get("reddit_video", {})
                    .get("fallback_url")
                )
                if vid_url:
                    media_urls.append(vid_url)
            elif data.get("is_gallery"):
                media_dict = data.get("media_metadata", {})
                for key, val in media_dict.items():
                    if "s" in val and "u" in val["s"]:
                        clean_url = val["s"]["u"].replace("&amp;", "&")
                        media_urls.append(clean_url)
            elif data.get("post_hint") in ["image", "link"]:
                url = data.get("url", "")
                if url.endswith((".jpg", ".png", ".gif", ".jpeg")):
                    media_urls.append(url)
        elif event_type == "COMMENT":
            media_dict = data.get("media_metadata", {})
            for key, val in media_dict.items():
                if "s" in val:
                    if "u" in val["s"]:
                        clean_url = val["s"]["u"].replace("&amp;", "&")
                        media_urls.append(clean_url)
                    elif "gif" in val["s"]:
                        clean_url = val["s"]["gif"].replace("&amp;", "&")
                        media_urls.append(clean_url)

        return media_urls

    @classmethod
    def from_reddit_api(
        cls, item: dict, event_type: Literal["POST", "COMMENT"]
    ) -> "RedditEvent":
        data = item.get("data", item)
        media_urls = cls._extract_media(data, event_type)
        timestamp = datetime.fromtimestamp(data.get("created_utc", 0))
        return cls(
            event_id=data.get("name", data.get("id", "")),
            event_type=event_type,
            posted_in_subreddit=data.get("subreddit", ""),
            author=data.get("author", "[deleted]"),
            url=data.get("url") if event_type == "POST" else data.get("link_url", ""),
            title=(
                data.get("title")
                if event_type == "POST"
                else data.get("link_title", "")
            ),
            content=(
                data.get("selftext") if event_type == "POST" else data.get("body", "")
            ),
            timestamp=timestamp,
            has_media=len(media_urls) > 0,
            media_urls=media_urls,
            score=data.get("score"),
            upvote_ratio=data.get("upvote_ratio"),
            num_comments=data.get("num_comments"),
            is_crosspost=data.get("is_crosspost", False),
            original_subreddit=(
                data.get("crosspost_parent_list", [{}])[0].get("subreddit")
                if data.get("is_crosspost")
                else None
            ),
        )
