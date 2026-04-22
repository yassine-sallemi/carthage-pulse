import json
import logging
import os
import re
from typing import Optional
from openai import OpenAI

logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)


def parse_json_response(content: str, num_items: int) -> list:
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
    def __init__(self, api_key: str, model: str = "gpt-4o-mini", prompt: str = ""):
        self.api_key = api_key
        self.model = model
        self.prompt = prompt

    def enrich(self, texts: list[str]) -> list[Optional[dict]]:
        raise NotImplementedError


class OpenAIProvider(LLMProvider):
    """OpenAI API provider for enrichment."""

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
    """OpenRouter API provider for enrichment."""

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
    """Factory function to create LLM provider instances.

    Args:
        provider: Provider name ('openai' or 'openrouter')
        api_key: API key (uses env var if not provided)
        model: Model name to use
        prompt: System prompt for enrichment

    Returns:
        LLMProvider instance

    Raises:
        ValueError: If provider is unknown
    """
    logger.debug(f"Initializing provider: {provider}")

    if provider == "openai":
        return OpenAIProvider(api_key or os.getenv("OPENAI_API_KEY", ""), model, prompt)
    if provider == "openrouter":
        return OpenRouterProvider(
            api_key or os.getenv("OPENROUTER_API_KEY", ""), model, prompt
        )

    logger.error(f"Unknown provider: {provider}")
    raise ValueError(f"Unknown provider: {provider}")
