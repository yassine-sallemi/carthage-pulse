from typing import Optional
from .providers import LLMProvider


class TextEnricher:
    def __init__(self, provider: LLMProvider):
        self.provider = provider

    def enrich_batch(self, events: list[dict]) -> list[dict]:
        texts = [
            f"{event.get('title', '')}\n\n{event.get('content', '')}".strip()
            for event in events
        ]

        results = self.provider.enrich(texts)

        if not results or not isinstance(results, list):
            return events

        enriched = []
        for event, result in zip(events, results):
            if result:
                enriched.append({**event, "enrichment": result})
            else:
                enriched.append({**event, "enrichment": None})

        return enriched

    def enrich(self, event: dict) -> Optional[dict]:
        text = f"{event.get('title', '')}\n\n{event.get('content', '')}".strip()

        if not text:
            return {**event, "enrichment": None}

        result = self.provider.enrich([text])
        if result and isinstance(result, list) and len(result) > 0:
            return {**event, "enrichment": result[0]}
        return None