"""
modules/llm_connector.py
========================
LangChain-based Azure OpenAI GPT-4o connector.

Provides ``LLM_cls`` with an **async** ``simple_query(prompt) -> str`` method
used by ``ETAChecker`` (and any other module that needs LLM access).

Because ``simple_query`` is a coroutine, callers must ``await`` it::

    reply = await llm.simple_query("What date is 'Mar.3' in 2026?")

Configuration
-------------
Set the following environment variables (or fill them in config.py):

    AZURE_OPENAI_API_KEY       your Azure OpenAI key
    AZURE_OPENAI_ENDPOINT      e.g. https://<resource>.openai.azure.com
    AZURE_OPENAI_DEPLOYMENT    your deployment name (e.g. "gpt-4o")
    AZURE_OPENAI_API_VERSION   e.g. "2024-02-01"

Usage
-----
    from modules.llm_connector import LLM_cls
    llm   = LLM_cls()
    reply = await llm.simple_query("What date is 'Mar.3' in 2026?")
"""

import logging
import os
import httpx
import certifi

from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import AzureChatOpenAI


logger = logging.getLogger(__name__)
client = httpx.Client(
    verify=certifi.where()
)
httpx_client = httpx.AsyncClient(
    verify=False
)


class LLM_cls:
    """
    Thin wrapper around LangChain's AzureChatOpenAI for simple single-turn
    async prompting.

    The model name and endpoint are read from environment variables at
    construction time.  No ``deployment`` or ``temperature`` parameters
    are exposed; set them via env vars or edit the ``AzureChatOpenAI``
    constructor below to match your Azure resource configuration.
    """

    def __init__(
        self,
    ):
        try:
            from langchain_openai import AzureChatOpenAI     # langchain-openai >= 0.1
        except ImportError:
            raise ImportError(
                "Install langchain-openai: pip install langchain-openai"
            )

        # self._deployment = (
        #     deployment
        #     or os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
        # )

        self._llm = AzureChatOpenAI(
            # azure_deployment   = self._deployment,
            azure_endpoint     = os.environ.get("AZURE_OPENAI_ENDPOINT", ""),
            model              = 'aide-gpt-4o-mini',
            api_key            = os.environ.get("AZURE_OPENAI_API_KEY", ""),
            api_version        = os.environ.get("api_version", "2024-02-01"),
            extra_headers      = {"X-User-Id": os.environ.get("azure_user_id", "xxxxxxx")},
            http_client        = client,        # sync httpx.Client (for blocking calls)
            http_async_client  = httpx_client,  # async httpx.AsyncClient (for ainvoke)
            # temperature        = temperature,
        )
        logger.info("[LLM] AzureChatOpenAI initialised (model=aide-gpt-4o-mini)")

    # ----------------------------------------------------------
    async def simple_query(self, query: str) -> str:
        """
        Send *query* to the LLM and return the text reply.

        Uses a ``ChatPromptTemplate`` with a strict **system** persona so the
        model knows it is a date-parsing specialist, and a **human** slot that
        carries the actual question.  This role-separation consistently
        produces more focused, JSON-only replies even from smaller deployments.
        """
        prompt = ChatPromptTemplate.from_messages([
            (
                "system",
                """\
You are a precise date-parsing assistant specialised in interpreting freeform \
date strings written by hardware design engineers in a project tracker.

Your rules:
1. Always respond with a single JSON object and **nothing else** — no prose, \
no markdown fences, no explanation.
2. The JSON must have exactly one key: "date" whose value is an ISO-8601 string \
(YYYY-MM-DD), OR null if the intent cannot be determined at all.
3. If the input is clearly "done", "v", "N/A", "x" or a similar completion \
marker, set "date" to the string "done".
4. When multiple dates appear (e.g. "3/2 -> 3/4" or "3/4(3/2)"), the \
last / outermost value is the **current** ETA; use that one.
5. If the year is ambiguous, assume the current or next calendar year so \
that the resulting date is not more than 60 days in the past.

Examples:
  "Mar/4th"    -> {{"date": "2026-03-04"}}
  "4 Mar"      -> {{"date": "2026-03-04"}}
  "4/Mar"      -> {{"date": "2026-03-04"}}
  "03.04"      -> {{"date": "2026-03-04"}}
  "3/2 -> 3/4" -> {{"date": "2026-03-04"}}
  "3/4(3/2)"   -> {{"date": "2026-03-04"}}
  "done"       -> {{"date": "done"}}
  "v"          -> {{"date": "done"}}
  ""           -> {{"date": null}}\
""",
            ),
            (
                "human",
                "Parse the following ETA field value and return the JSON result:\n{message}",
            ),
        ])

        chain    = prompt | self._llm
        response = await chain.ainvoke({"message": query})
        reply    = response.content   # BaseMessage uses .content (lowercase)
        logger.info(
            f"[LLM] simple_query reply: {reply[:120]!r}"
            f"{'...' if len(reply) > 120 else ''}"
        )
        return reply