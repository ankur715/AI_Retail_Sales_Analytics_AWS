"""Claude on Amazon Bedrock provider (anthropic SDK), the same setup as the
Member Engagement project: AWS credentials from the normal boto3 chain
(AWS_PROFILE), no API key, pay per token, no free-tier daily cap.

  BEDROCK_MODEL          default claude-opus-5-5; a value containing a dot
                         is used verbatim (a full Bedrock id or inference
                         profile, e.g. us.anthropic.claude-haiku-4-5-20251001-v1:0)
  LLM_BEDROCK_ENDPOINT   mantle (Messages API endpoint, newest models) or
                         runtime (bedrock-runtime InvokeModel; needed for
                         models served only through inference profiles)

Structured outputs come back as validated Pydantic objects (messages.parse).
The SDK retries 429 / 5xx / connection errors itself; what's left after that
becomes LLMUnavailable, which the API turns into a clear 503.
"""
from functools import lru_cache

import anthropic
from pydantic import BaseModel

from chatbot.llm_types import LLMUnavailable
from etl import config

MAX_TOKENS = 16000   # a cap, not a target: route/SQL/summary answers are a few hundred tokens


@lru_cache(maxsize=1)
def client():
    if config.LLM_BEDROCK_ENDPOINT == "runtime":
        return anthropic.AnthropicBedrock(aws_region=config.AWS_REGION, max_retries=3)
    return anthropic.AnthropicBedrockMantle(aws_region=config.AWS_REGION, max_retries=3)


def model_name() -> str:
    """Bedrock ids take an 'anthropic.' prefix; full ids / inference profiles are used as given."""
    model = config.BEDROCK_MODEL
    return model if "." in model else f"anthropic.{model}"


def _call(fn, **kwargs):
    try:
        response = fn(model=model_name(), max_tokens=MAX_TOKENS, **kwargs)
    except anthropic.RateLimitError as e:                 # still throttled after the SDK's retries
        raise LLMUnavailable(f"Bedrock throttled {model_name()}: {e.message}") from e
    except (anthropic.AuthenticationError, anthropic.PermissionDeniedError, anthropic.NotFoundError) as e:
        raise LLMUnavailable(f"Bedrock can't use {model_name()} with these AWS credentials "
                             f"(model access / IAM): {e.message}", kind="config") from e
    except anthropic.APIStatusError as e:
        kind = "busy" if e.status_code >= 500 else "config"
        raise LLMUnavailable(f"Bedrock error {e.status_code}: {e.message}", kind=kind) from e
    except anthropic.APIConnectionError as e:
        raise LLMUnavailable(f"Can't reach Bedrock: {e}") from e
    if response.stop_reason == "refusal":
        raise LLMUnavailable("The model declined this request", kind="declined")
    return response


def json_call(prompt: str, schema: type[BaseModel]) -> BaseModel:
    response = _call(client().messages.parse, output_format=schema,
                     messages=[{"role": "user", "content": prompt}])
    if response.parsed_output is None:
        raise LLMUnavailable("Bedrock returned no structured output", kind="busy")
    return response.parsed_output


def text_call(prompt: str) -> str:
    response = _call(client().messages.create, messages=[{"role": "user", "content": prompt}])
    return "".join(b.text for b in response.content if b.type == "text").strip()
