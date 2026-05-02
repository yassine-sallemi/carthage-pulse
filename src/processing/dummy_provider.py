"""Dummy LLM provider for local testing without external API calls."""

from typing import Optional

from src.shared_utils import Enrichment, Entity, Language

from .llm_service import LLMProvider


class DummyProvider(LLMProvider):
    """Deterministic provider that returns synthetic enrichment data."""

    def enrich(self, texts: list[str]) -> list[Optional[dict]]:
        if not texts:
            return []

        results: list[Optional[dict]] = []
        for index, _text in enumerate(texts, 1):
            enrichment = Enrichment(
                languages=[Language.ENGLISH],
                translation=f"Dummy translation for item {index}",
                sentiment_score=0.0,
                intent="testing",
                topics=["testing", "dummy-data", "workflow-validation"],
                entities=[
                    Entity(name="Dummy Entity", type="Organization"),
                    Entity(name="Test Location", type="Location"),
                ],
            )
            results.append(enrichment.model_dump(mode="json"))

        return results


