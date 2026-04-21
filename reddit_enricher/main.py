from src.consumer import RedditEnricherConsumer


def main():
    consumer = RedditEnricherConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
