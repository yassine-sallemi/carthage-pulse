import requests
from datetime import datetime
from typing import Optional
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
from .models import RedditEvent


class RedditStreamer:
    def __init__(self, user_agent: Optional[str] = None, poll_interval: int = 30):
        config = load_config()
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

    def fetch(self, endpoint: str = "new") -> list[RedditEvent]:
        url = f"{self.api_url}/{endpoint}.json"
        params = {"limit": self.fetch_limit}
        headers = {"User-Agent": self.user_agent}

        last_id = self.last_seen.get(endpoint)
        if last_id:
            params["before"] = last_id

        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()

            events = []
            for child in data.get("data", {}).get("children", []):
                post = child.get("data", {})
                event_id = post.get("name")

                if endpoint == "new":
                    events.append(
                        RedditEvent(
                            event_id=event_id,
                            event_type="POST",
                            posted_in_subreddit=post.get("subreddit", ""),
                            author=post.get("author", ""),
                            url=post.get("url", ""),
                            title=post.get("title", ""),
                            content=post.get("selftext", ""),
                            timestamp=datetime.fromtimestamp(post.get("created_utc", 0)),
                        )
                    )
                elif endpoint == "comments":
                    events.append(
                        RedditEvent(
                            event_id=event_id,
                            event_type="COMMENT",
                            posted_in_subreddit=post.get("subreddit", ""),
                            author=post.get("author", ""),
                            url=post.get("link_url", ""),
                            title=post.get("link_title", ""),
                            content=post.get("body", ""),
                            timestamp=datetime.fromtimestamp(post.get("created_utc", 0)),
                        )
                    )

            if events:
                self.last_seen[endpoint] = events[0].event_id

            return events

        except requests.RequestException as e:
            print(f"Error fetching {endpoint}: {e}")
            return []

    def initial_fetch(self) -> list[RedditEvent]:
        events = []
        for endpoint in self.endpoints:
            events.extend(self.fetch(endpoint))
        return events

    def poll(self) -> list[RedditEvent]:
        return self.initial_fetch()
