"""Reddit API client for fetching posts and comments"""

import logging
import requests
from typing import Optional, List
from .config import (
    load_config,
    get_subreddits,
    get_poll_interval,
    get_fetch_limit,
    get_user_agent,
    get_endpoints,
    build_subreddit_url,
    get_initial_fetch,
)
from src.shared_utils import RedditEvent

logger = logging.getLogger(__name__)


class RedditClient:
    """Fetches Reddit posts and comments from specified subreddits"""

    def __init__(
        self,
        user_agent: Optional[str] = None,
        poll_interval: int = 30,
        config: Optional[dict] = None,
    ):
        config = config or load_config()
        self.subreddits = get_subreddits(config)
        self.poll_interval = get_poll_interval(config)
        self.fetch_limit = get_fetch_limit(config)
        self.user_agent = user_agent or get_user_agent(config)
        self.endpoints = get_endpoints(config)
        self.api_url = (
            f"https://www.reddit.com/r/{build_subreddit_url(self.subreddits)}"
        )
        self.do_initial_fetch = get_initial_fetch(config)
        self.last_seen = {"new": None, "comments": None}
        logger.info(
            f"Client initialized: r/{'+'.join(self.subreddits)} ({', '.join(self.endpoints)})"
        )

    def fetch(self, endpoint: str = "new") -> List[RedditEvent]:
        """Fetch new posts/comments from Reddit API"""
        url = f"{self.api_url}/{endpoint}.json"
        params = {"limit": self.fetch_limit}
        headers = {"User-Agent": self.user_agent}

        last_id = self.last_seen.get(endpoint)
        if last_id:
            params["before"] = last_id

        try:
            logger.debug(f"🌐 Fetching {endpoint} endpoint...")
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()

            events = []
            for child in data.get("data", {}).get("children", []):
                post = child.get("data", {})

                try:
                    if endpoint == "new":
                        event = RedditEvent.from_reddit_api(post, "POST")
                    elif endpoint == "comments":
                        event = RedditEvent.from_reddit_api(post, "COMMENT")
                    else:
                        logger.warning(f"⚠️  Unknown endpoint: {endpoint}")
                        continue

                    events.append(event)

                    if event.has_media:
                        logger.debug(
                            f"📷 {event.event_id}: {len(event.media_urls)} media files"
                        )

                except Exception as e:
                    logger.debug(f"⏭️  Skipped item {post.get('name', 'unknown')}")
                    continue

            if events:
                self.last_seen[endpoint] = events[0].event_id
                media_count = sum(1 for e in events if e.has_media)
                emojis = f"📷 ({media_count})" if media_count > 0 else ""
                logger.info(f"📦 Fetched {len(events)} items from {endpoint} {emojis}")

            return events

        except requests.RequestException as e:
            logger.error(f"🌐 Network error fetching {endpoint}: {str(e)[:50]}")
            return []
        except Exception as e:
            logger.error(f"💥 Unexpected error: {str(e)[:50]}")
            return []

    def initial_fetch(self) -> List[RedditEvent]:
        """Perform initial fetch from all endpoints"""
        events = []
        for endpoint in self.endpoints:
            events.extend(self.fetch(endpoint))
        logger.info(f"Initial fetch complete: {len(events)} total events")
        return events

    def poll(self) -> List[RedditEvent]:
        """Poll for new events"""
        return self.initial_fetch()
