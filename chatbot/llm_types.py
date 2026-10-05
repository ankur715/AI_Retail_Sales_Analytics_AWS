"""Types shared by the LLM facade (llm.py) and the provider modules."""
from pydantic import BaseModel, Field


class LLMUnavailable(RuntimeError):
    """The LLM can't answer right now. `kind` drives the message the user sees:
    busy (overload / rate limit, retry soon), quota (daily quota used up),
    config (credentials, permissions, model access), declined (model refused)."""

    def __init__(self, message: str, kind: str = "busy"):
        super().__init__(message)
        self.kind = kind


class Route(BaseModel):
    views: list[str] = Field(description="Views needed to answer, from the list given. Empty if none fit.")
    reason: str = Field(description="One short sentence.")


class SqlPlan(BaseModel):
    sql: str = Field(description="One Redshift SELECT query, or empty if the question can't be answered.")
    explanation: str = Field(description="One sentence: what the query computes.")
    unanswerable_reason: str = Field(default="", description="Why it can't be answered, if sql is empty.")
