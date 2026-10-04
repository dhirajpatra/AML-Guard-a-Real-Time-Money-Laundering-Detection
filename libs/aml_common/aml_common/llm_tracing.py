"""Langfuse tracing for LLM / LangGraph runs (used by the Phase 4 agent service).

    handler = langfuse_handler(settings)
    config = {"callbacks": [handler]} if handler else {}
    graph.invoke(state, config=config)

Returns None when keys or the SDK are missing, so agents run fine without Langfuse.
Use the `langfuse<3` SDK with the bundled Langfuse v2 server (v3 SDKs need a v3 server).
"""
from __future__ import annotations

import logging

log = logging.getLogger("llm_tracing")


def langfuse_handler(settings):
    if not (settings.langfuse_public_key and settings.langfuse_secret_key):
        return None
    try:
        try:
            from langfuse.callback import CallbackHandler          # SDK v2
        except ImportError:
            from langfuse.langchain import CallbackHandler         # SDK v3
    except ImportError:
        log.warning("langfuse SDK not installed; LLM tracing disabled")
        return None
    return CallbackHandler(public_key=settings.langfuse_public_key, secret_key=settings.langfuse_secret_key,
                           host=settings.langfuse_host)
