from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class UserAction(str, Enum):
    VIEWED = "VIEWED"
    PURCHASED = "PURCHASED"


class TrackEventRequest(BaseModel):
    user_id: str = Field(..., min_length=1)
    product_id: str = Field(..., min_length=1)
    action: UserAction


class ProductOut(BaseModel):
    id: str
    name: str
    category: str
    price: float


class TrackEventResponse(BaseModel):
    status: str
    topic: str


class RecommendationOut(ProductOut):
    score: float
    supporting_users: int


class SimulateTrafficRequest(BaseModel):
    viewed_count: int = Field(default=100, ge=0, le=5000)
    purchased_count: int = Field(default=20, ge=0, le=5000)
    user_start: int = Field(default=2, ge=1, le=10_000)
    user_end: int = Field(default=10, ge=1, le=10_000)
    product_start: int = Field(default=1, ge=1, le=100_000)
    product_end: int = Field(default=50, ge=1, le=100_000)
    seed: int = Field(default=42)


class SimulateTrafficResponse(BaseModel):
    status: str
    topic: str
    total_events: int
    viewed_events: int
    purchased_events: int


class TriggerPipelineResponse(BaseModel):
    status: str
    dag_id: str
    dag_run_id: str
    state: str | None = None
    message: str
