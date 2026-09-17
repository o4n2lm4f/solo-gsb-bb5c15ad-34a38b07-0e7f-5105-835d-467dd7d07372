"""Pydantic request/response models."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


class SubscriptionCreate(BaseModel):
    target_url: str = Field(min_length=1, max_length=2048)
    secret: str = Field(min_length=1, max_length=256)

    @field_validator("target_url")
    @classmethod
    def must_be_http(cls, v: str) -> str:
        v = v.strip()
        if not (v.startswith("http://") or v.startswith("https://")):
            raise ValueError("target_url must start with http:// or https://")
        return v


class SubscriptionOut(BaseModel):
    id: str
    target_url: str
    created_at: str


class EventCreate(BaseModel):
    event_id: str = Field(min_length=1, max_length=128)
    subscription_ids: list[str] = Field(min_length=1, max_length=100)
    payload: dict[str, Any]


class IngestEvent(BaseModel):
    """Body of an HMAC-protected inbound event on POST /ingest.

    The whole body (including any extra fields) is forwarded verbatim to
    targets; only subscription_ids is consumed by the relay itself.
    """

    model_config = {"extra": "allow"}

    subscription_ids: list[str] = Field(min_length=1, max_length=100)


class TaskRef(BaseModel):
    task_id: int
    subscription_id: str
    status: str


class EventAccepted(BaseModel):
    event_id: str
    created: list[TaskRef]
    duplicates: list[TaskRef]


class AttemptOut(BaseModel):
    attempt_no: int
    status_code: int | None
    error: str | None
    started_at: str
    finished_at: str
    duration_ms: int


class TaskStatus(BaseModel):
    task_id: int
    subscription_id: str
    target_url: str | None
    status: str
    attempts: int
    max_attempts: int
    next_attempt_at: str | None
    last_status_code: int | None
    last_error: str | None
    result: str | None
    history: list[AttemptOut]


class EventStatus(BaseModel):
    event_id: str
    created_at: str
    targets: list[TaskStatus]
