import logging
import time
from src.streamer import RedditStreamer
from src.kafka_producer import RedditKafkaProducer
from src.config import load_config
from src.logging_config import setup_logging

logger = setup_logging(logging.INFO)


def main():
    logger.info("Initializing Reddit Streamer")

    kafka_producer = None
    streamer = None

    try:
        user_agent = "AcademicBigDataProject/1.0 (by /u/YourRedditUsername)"
        config = load_config()

        streamer = RedditStreamer(
            user_agent=user_agent, poll_interval=30, config=config
        )
        kafka_producer = RedditKafkaProducer(config)

        subs = "+".join(streamer.subreddits)
        logger.info(f"Streaming r/{subs} → Kafka topic: {kafka_producer.topic}")

        if streamer.do_initial_fetch:
            events = streamer.initial_fetch()
            media_count = sum(1 for e in events if e.has_media)
            logger.info(
                f"Initial fetch: {len(events)} items ({media_count} with media)"
            )

            sent = kafka_producer.send_batch(events)
            logger.info(f"Sent {sent}/{len(events)} events to Kafka")
        else:
            logger.info("Initial fetch skipped")

        logger.info("Entering streaming loop...\n")

        while True:
            new_events = streamer.poll()

            if new_events:
                media_count = sum(1 for e in new_events if e.has_media)
                logger.info(
                    f"Found {len(new_events)} new events ({media_count} with media)!"
                )
                sent = kafka_producer.send_batch(new_events)
                for event in new_events:
                    icon = (
                        "📷"
                        if event.has_media
                        else ("📝" if event.event_type == "POST" else "💬")
                    )
                    display_text = (
                        event.title if event.event_type == "POST" else event.content
                    ) or ""
                    media_info = (
                        f" [{len(event.media_urls)} media]" if event.has_media else ""
                    )
                    logger.info(
                        f"[{icon}] r/{event.posted_in_subreddit} {event.author} ({event.score or 0}↑): {display_text[:50]}...{media_info}"
                    )
            else:
                logger.debug("No new items, sleeping...")

            time.sleep(streamer.poll_interval)

    except KeyboardInterrupt:
        logger.info("Streamer stopped by user.")
    except Exception as e:
        logger.exception(f"Unexpected error in main: {e}")
        raise
    finally:
        # Ensure producer is safely closed even if initialization failed
        if kafka_producer is not None:
            try:
                kafka_producer.flush()
                kafka_producer.close()
                logger.info("Kafka producer closed successfully")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")


if __name__ == "__main__":
    main()
