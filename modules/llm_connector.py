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
from rich import print as pprint

from langchain.prompts import PromptTemplate
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
        prompt = PromptTemplate(
            input_variables=["query"],
            template=""" 你是一位智能助理，請協助回答使用者問題，
            請只要提供被```包含在內的資訊，不要提供額外資訊
            以下為user的問題: {query} """)
        chain    = prompt | self._llm
        response = await chain.ainvoke({"query": query})
        pprint(query)
        pprint(prompt)
        pprint(response)
        reply = response.content          # LangChain BaseMessage uses .content (lowercase)
        logger.info(
            f"[LLM] simple_query reply: {reply[:120]!r}"
            f"{'...' if len(reply) > 120 else ''}"
        )
        return reply