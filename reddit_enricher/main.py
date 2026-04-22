import logging
from src.consumer import RedditEnricherConsumer
from src.logging_config import setup_logging

logger = setup_logging(logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)


def main():
    logger.info("Initializing Reddit Enricher")
    consumer = RedditEnricherConsumer()
    logger.info("Starting enrichment pipeline")
    consumer.run()


if __name__ == "__main__":
    main()
