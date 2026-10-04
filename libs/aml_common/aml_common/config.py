"""Central configuration. Every value comes from environment variables (see .env.example)."""
from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    log_level: str = "INFO"

    # Graph
    neo4j_uri: str = "bolt://neo4j:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "aml_password_123"

    # Streaming
    kafka_bootstrap: str = "redpanda:9092"
    topic_raw: str = "transactions.raw"
    topic_truth: str = "transactions.truth"
    topic_decisions: str = "transactions.decisions"
    topic_alerts: str = "alerts"
    topic_dlq: str = "transactions.dlq"
    topic_network_alerts: str = "alerts.network"

    # Cache / case store
    redis_url: str = "redis://redis:6379/0"
    postgres_dsn: str = "postgresql://aml:aml_password_123@postgres:5432/aml"

    # Observability (Phase 5)
    otel_enabled: bool = False                       # tracing is opt-in; Prometheus metrics are always on
    otel_exporter_otlp_endpoint: str = "http://otel-collector:4317"
    otel_trace_sample_ratio: float = 1.0             # head sampling in the SDK; tail sampling is in the collector
    service_name: str = "aml-guard"
    langfuse_host: str = "http://langfuse:3000"
    langfuse_public_key: str = ""
    langfuse_secret_key: str = ""

    # LLM - provider-agnostic (used from Phase 4)
    llm_provider: str = "ollama"          # ollama | openai | anthropic | openai_compatible
    llm_model: str = "llama3.1:8b"
    llm_base_url: str = "http://ollama:11434"
    llm_api_key: str = ""
    llm_temperature: float = 0.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
