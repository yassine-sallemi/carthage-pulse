import time
from src.streamer import RedditStreamer
from src.kafka_producer import RedditKafkaProducer


def main():
    user_agent = "AcademicBigDataProject/1.0 (by /u/YourRedditUsername)"
    streamer = RedditStreamer(user_agent=user_agent, poll_interval=30)
    kafka_producer = RedditKafkaProducer()

    print(f"Starting r/{'+'.join(streamer.subreddits)} Event Streamer...")
    print("Press Ctrl+C to stop.\n" + "-" * 40)

    if streamer.do_initial_fetch:
        events = streamer.initial_fetch()
        print(f"Initial fetch: {len(events)} items")

        sent = kafka_producer.send_batch(events)
        print(f"Sent {sent} events to Kafka")

        for event in events:
            icon = "POST" if event.event_type == "POST" else "COMMENT"
            display_text = (
                event.title if event.event_type == "POST" else event.content
            ) or ""
            print(
                f"[{icon}] r/{event.posted_in_subreddit} {event.author}: {display_text[:60]}..."
            )

        print("-" * 40)
        print(f"Anchors set: {streamer.last_seen}")
    else:
        print("Skipping initial fetch")

    print("\nEntering streaming loop...\n")

    try:
        while True:
            new_events = streamer.poll()

            if new_events:
                print(f"Found {len(new_events)} new events!")
                sent = kafka_producer.send_batch(new_events)
                print(f"Sent {sent} events to Kafka")
                for event in new_events:
                    icon = "POST" if event.event_type == "POST" else "COMMENT"
                    display_text = (
                        event.title if event.event_type == "POST" else event.content
                    ) or ""
                    print(
                        f"[{icon}] r/{event.posted_in_subreddit} {event.author}: {display_text[:60]}..."
                    )
                    print(f"   {event.url}")
                    print("-" * 40)
            else:
                print("No new items, sleeping...")

            time.sleep(streamer.poll_interval)

    except KeyboardInterrupt:
        print("\nStreamer stopped by user.")
        kafka_producer.flush()
        kafka_producer.close()


if __name__ == "__main__":
    main()
