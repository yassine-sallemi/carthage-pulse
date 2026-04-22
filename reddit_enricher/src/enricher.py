import logging
from typing import Optional, List
from pydantic import ValidationError
from .providers import LLMProvider
from .models import RedditEvent, Enrichment

logger = logging.getLogger(__name__)


class TextEnricher:
    def __init__(self, provider: LLMProvider):
        self.provider = provider

    @staticmethod
    def _extract_text(event: RedditEvent) -> str:
        return f"{event.title or ''}\n\n{event.content or ''}".strip()

    def enrich_batch(self, events: List[RedditEvent]) -> List[RedditEvent]:
        if not events:
            return []
        texts = [self._extract_text(event) for event in events]

        try:
            results = self.provider.enrich(texts)
        except Exception as e:
            logger.warning(f"Provider error: {type(e).__name__}")
            return events

        if not results or not isinstance(results, list):
            logger.warning("Invalid provider response")
            return events

        if len(results) != len(events):
            logger.warning(
                f"Result count mismatch: got {len(results)}, expected {len(events)}"
            )

        enriched = []
        for idx, (event, result) in enumerate(zip(events, results)):
            try:
                if result and isinstance(result, dict):
                    enrichment = Enrichment(**result)
                    enriched.append(event.model_copy(update={"enrichment": enrichment}))
                else:
                    logger.debug(f"Event {event.event_id}: empty or null result")
                    enriched.append(event.model_copy(update={"enrichment": None}))
            except ValidationError as e:
                logger.warning(
                    f"Event {event.event_id}: validation error - {e.error_count()} issues"
                )
                logger.debug(f"Failed result: {result}")
                enriched.append(event.model_copy(update={"enrichment": None}))
            except Exception as e:
                logger.warning(f"Event {event.event_id}: {type(e).__name__}")
                enriched.append(event.model_copy(update={"enrichment": None}))

        return enriched

    def enrich(self, event: RedditEvent) -> Optional[RedditEvent]:
        text = self._extract_text(event)
        if not text:
            return event.model_copy(update={"enrichment": None})

        try:
            result = self.provider.enrich([text])
        except Exception as e:
            logger.debug(f"Provider error: {type(e).__name__}")
            return None

        if result and isinstance(result, list) and len(result) > 0:
            item = result[0]
            if isinstance(item, dict):
                try:
                    enrichment = Enrichment(**item)
                    return event.model_copy(update={"enrichment": enrichment})
                except ValidationError as e:
                    logger.debug(
                        f"Event {event.event_id}: validation error - {e.error_count()} issues"
                    )
                    logger.debug(f"Response was: {item}")
                    return event.model_copy(update={"enrichment": None})
            else:
                logger.debug(f"Event {event.event_id}: non-dict result")
        else:
            logger.debug(f"Event {event.event_id}: empty result")
        return None
