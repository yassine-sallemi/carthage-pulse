"""LLM-based text enrichment service"""

import json
import logging
import os
import re
from typing import Optional, List
from openai import OpenAI
from src.shared_utils import RedditEvent, Enrichment

logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)


def parse_json_response(content: str, num_items: int) -> list:
    """Parse JSON array from LLM response with fallback strategies"""
    if not content:
        return [None] * num_items

    content = content.strip()

    try:
        result = json.loads(content)
        if isinstance(result, list):
            logger.debug(f"Parsed {len(result)} items from LLM")
            return result
        elif isinstance(result, dict):
            return [result]
    except json.JSONDecodeError as e:
        logger.debug(f"JSON parsing failed at line {e.lineno}: {e.msg}")

    try:
        matches = re.findall(r"\{[^{}]*\}", content)
        results = []
        for m in matches:
            try:
                results.append(json.loads(m))
            except json.JSONDecodeError:
                continue
        if results:
            logger.debug(f"Recovered {len(results)} items via regex")
            return results[:num_items]
    except Exception as e:
        logger.debug(f"Regex recovery failed: {type(e).__name__}")

    logger.warning(f"Failed to parse LLM response, returning {num_items} None values")
    return [None] * num_items


class LLMProvider:
    """Base class for LLM providers"""

    def __init__(self, api_key: str, model: str = "gpt-4o-mini", prompt: str = ""):
        self.api_key = api_key
        self.model = model
        self.prompt = prompt

    def enrich(self, texts: list[str]) -> list[Optional[dict]]:
        raise NotImplementedError


class OpenAIProvider(LLMProvider):
    """OpenAI API provider for enrichment"""

    def __init__(self, api_key: str, model: str = "gpt-4o-mini", prompt: str = ""):
        super().__init__(api_key, model, prompt)
        self.client = OpenAI(api_key=api_key)

    def enrich(self, texts: list[str]) -> list[Optional[dict]]:
        if not texts:
            return []

        combined = "\n\n".join(
            [f"Item {i}:\n{text}" for i, text in enumerate(texts, 1)]
        )
        system_prompt = (
            self.prompt
            or "Extract sentiment, entities, and topics from each text. Return JSON array."
        )
        logger.debug(f"Processing {len(texts)} items with {self.model}")

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user",
                        "content": f"Process these {len(texts)} items:\n\n" + combined,
                    },
                ],
                max_tokens=4000,
                timeout=60,
            )
            content = response.choices[0].message.content
            result = parse_json_response(content, len(texts))
            logger.debug(f"Processed {len(texts)} items")
            return result
        except Exception as e:
            logger.warning(f"API error: {type(e).__name__}")
            return [None] * len(texts)


class OpenRouterProvider(LLMProvider):
    """OpenRouter API provider for enrichment"""

    def __init__(
        self,
        api_key: str,
        model: str = "meta-llama/llama-3.3-70b-instruct:free",
        prompt: str = "",
    ):
        super().__init__(api_key, model, prompt)
        self.client = OpenAI(
            api_key=api_key,
            base_url="https://openrouter.ai/api/v1",
            default_headers={
                "HTTP-Referer": "https://reddit-enricher",
                "X-Title": "RedditEnricher",
            },
        )

    def enrich(self, texts: list[str]) -> list[Optional[dict]]:
        if not texts:
            return []

        combined = "\n\n".join(
            [f"Item {i}:\n{text}" for i, text in enumerate(texts, 1)]
        )
        system_prompt = self.prompt

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user",
                        "content": f"Process these {len(texts)} items:\n\n" + combined,
                    },
                ],
                max_tokens=4000,
                timeout=60,
            )
            content = response.choices[0].message.content
            if not content:
                logger.warning("OpenRouter returned empty content")
                return [None] * len(texts)

            result = parse_json_response(content, len(texts))
            logger.debug(f"OpenRouter: successfully processed {len(texts)} items")
            return result
        except json.JSONDecodeError as e:
            logger.error(f"OpenRouter JSON decode error: {e}")
            return [None] * len(texts)
        except Exception as e:
            logger.error(f"OpenRouter error: {e}")
            return [None] * len(texts)


def get_provider(
    provider: str = "openai",
    api_key: str = "",
    model: str = "gpt-4o-mini",
    prompt: str = "",
) -> LLMProvider:
    """Factory function to create LLM provider instances"""
    logger.debug(f"Initializing provider: {provider}")

    if provider == "openai":
        return OpenAIProvider(api_key or os.getenv("OPENAI_API_KEY", ""), model, prompt)
    if provider == "openrouter":
        return OpenRouterProvider(
            api_key or os.getenv("OPENROUTER_API_KEY", ""), model, prompt
        )

    logger.error(f"Unknown provider: {provider}")
    raise ValueError(f"Unknown provider: {provider}")


class LLMService:
    """Service for enriching text using LLM providers"""

    def __init__(self, provider: LLMProvider):
        self.provider = provider

    @staticmethod
    def _extract_text(event: RedditEvent) -> str:
        """Extract text content from Reddit event"""
        return f"{event.title or ''}\n\n{event.content or ''}".strip()

    def enrich_batch(self, events: List[RedditEvent]) -> List[RedditEvent]:
        """Enrich a batch of events"""
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
            except Exception as e:
                logger.warning(f"Event {event.event_id}: {type(e).__name__}")
                enriched.append(event.model_copy(update={"enrichment": None}))

        return enriched

    def enrich(self, event: RedditEvent) -> Optional[RedditEvent]:
        """Enrich a single event"""
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
                except Exception as e:
                    logger.debug(f"Event {event.event_id}: validation error")
                    return event.model_copy(update={"enrichment": None})
            else:
                logger.debug(f"Event {event.event_id}: invalid result format")
                return event.model_copy(update={"enrichment": None})

        return event.model_copy(update={"enrichment": None})
