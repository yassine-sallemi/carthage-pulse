CREATE TABLE IF NOT EXISTS enriched_events (
    event_id VARCHAR PRIMARY KEY,
    event_type VARCHAR,
    posted_in_subreddit VARCHAR,
    author VARCHAR,
    url VARCHAR,
    title VARCHAR,
    content TEXT,
    timestamp TIMESTAMP,
    has_media BOOLEAN,
    media_urls TEXT[],
    score INTEGER,
    upvote_ratio FLOAT,
    num_comments INTEGER,
    is_crosspost BOOLEAN,
    original_subreddit VARCHAR,
    languages TEXT[],
    translation TEXT,
    sentiment_score FLOAT,
    intent VARCHAR,
    topics TEXT[],
    entities TEXT
);


CREATE TABLE IF NOT EXISTS raw_events (
    event_id VARCHAR PRIMARY KEY,
    event_type VARCHAR,
    posted_in_subreddit VARCHAR,
    author VARCHAR,
    url VARCHAR,
    title VARCHAR,
    content TEXT,
    timestamp TIMESTAMP,
    has_media BOOLEAN,
    media_urls TEXT[],
    score INTEGER,
    upvote_ratio FLOAT,
    num_comments INTEGER,
    is_crosspost BOOLEAN,
    original_subreddit VARCHAR
);
