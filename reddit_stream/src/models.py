from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class RedditEvent(BaseModel):
    event_id: str
    event_type: str
    posted_in_subreddit: str
    author: str
    url: str
    title: Optional[str] = None
    content: Optional[str] = None
    timestamp: datetime
