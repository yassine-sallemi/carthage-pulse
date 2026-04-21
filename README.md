# Reddit Analytics

Real-time Reddit analytics pipeline with sentiment analysis and topic extraction.

## Architecture

```
Reddit API
    ↓
reddit_stream (fetch from Reddit)
    ↓
reddit_enricher (LLM enrichment)
```

## Services

| Service | Description |
|---------|-------------|
| `reddit_stream` | Fetch posts/comments from Reddit API |
| `reddit_enricher` | LLM enrichment (sentiment, topics, translation) |
| `compose` | Kafka infrastructure |

## Quick Start

### 1. Start Kafka
```bash
docker compose up -d
```
Kafka UI is available at: http://localhost:8080

### 2. Run Streamer
```bash
cd reddit_stream
cp config.yaml.example config.yaml
# Edit config.yaml
uv sync
uv run main.py
```

### 3. Run Enricher
```bash
cd reddit_enricher
cp config.yaml.example config.yaml
# Edit config.yaml with your API key
uv sync
uv run main.py
```

## Project Structure

```
.
├── compose.yaml        # Kafka infrastructure
├── reddit_stream/       # Python Reddit fetcher
│   ├── config.yaml.example
│   └── src/
└── reddit_enricher/     # Python LLM enrichment
    ├── config.yaml.example
    └── src/
```