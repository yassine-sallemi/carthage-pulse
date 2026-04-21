from typing import Optional
import json
import os
import re
from openai import OpenAI


def parse_json_response(content: str, num_items: int) -> list:
    content = content.strip()
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        pass
    try:
        matches = re.findall(r'\{[^{}]*\}', content)
        results = []
        for m in matches:
            try:
                results.append(json.loads(m))
            except json.JSONDecodeError:
                continue
        if results:
            return results[:num_items]
    except Exception:
        pass
    return [None] * num_items


class LLMProvider:
    def __init__(self, api_key: str, model: str = "gpt-4o-mini", prompt: str = ""):
        self.api_key = api_key
        self.model = model
        self.prompt = prompt

    def enrich(self, texts: list[str]) -> list[Optional[dict]]:
        raise NotImplementedError


class OpenAIProvider(LLMProvider):
    def __init__(self, api_key: str, model: str = "gpt-4o-mini", prompt: str = ""):
        super().__init__(api_key, model, prompt)
        self.client = OpenAI(api_key=api_key)

    def enrich(self, texts: list[str]) -> list[Optional[dict]]:
        if not texts:
            return []

        combined = "\n\n".join([f"Item {i}:\n{text}" for i, text in enumerate(texts, 1)])
        system_prompt = self.prompt or "Extract sentiment, entities, and topics from each text. Return JSON array."

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Process these {len(texts)} items:\n\n" + combined},
                ],
                max_tokens=4000,
                timeout=60,
            )
            content = response.choices[0].message.content
            return parse_json_response(content, len(texts))
        except Exception as e:
            print(f"Error calling OpenAI: {e}")
            return [None] * len(texts)


class OpenRouterProvider(LLMProvider):
    def __init__(self, api_key: str, model: str = "meta-llama/llama-3.3-70b-instruct:free", prompt: str = ""):
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

        combined = "\n\n".join([f"Item {i}:\n{text}" for i, text in enumerate(texts, 1)])
        system_prompt = self.prompt or "Extract sentiment, entities, and topics from each text. Return JSON array."

        print(f"DEBUG: system_prompt length: {len(system_prompt)}")
        print(f"DEBUG: combined text length: {len(combined)}")
        print(f"DEBUG: num items: {len(texts)}")
        print(f"DEBUG: SYSTEM PROMPT:\n{system_prompt}")
        print(f"DEBUG: USER CONTENT:\nProcess these {len(texts)} items:\n\n{combined}")

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Process these {len(texts)} items:\n\n" + combined},
                ],
                max_tokens=4000,
                timeout=60,
            )
            content = response.choices[0].message.content
            print(f"DEBUG: OpenRouter response: {content!r}")
            if not content:
                print("OpenRouter returned empty content!")
                return [None] * len(texts)
            return parse_json_response(content, len(texts))
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}, response: {content!r}")
            return [None] * len(texts)
        except Exception as e:
            print(f"Error calling OpenRouter: {e}")
            return [None] * len(texts)


def get_provider(
    provider: str = "openai", api_key: str = "", model: str = "gpt-4o-mini", prompt: str = ""
) -> LLMProvider:
    if provider == "openai":
        return OpenAIProvider(api_key or os.getenv("OPENAI_API_KEY", ""), model, prompt)
    if provider == "openrouter":
        return OpenRouterProvider(api_key or os.getenv("OPENROUTER_API_KEY", ""), model, prompt)
    raise ValueError(f"Unknown provider: {provider}")