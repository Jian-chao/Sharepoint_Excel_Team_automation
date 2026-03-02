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

logger = logging.getLogger(__name__)


class LLM_cls:
    """
    Thin wrapper around LangChain's AzureChatOpenAI for simple single-turn
    prompting.

    Parameters
    ----------
    deployment : str, optional
        Azure OpenAI deployment name.  Defaults to the
        ``AZURE_OPENAI_DEPLOYMENT`` environment variable.
    temperature : float
        Sampling temperature.  Use 0 for deterministic outputs (ETA parsing).
    """

    def __init__(
        self,
        deployment:   str   = "",
        temperature:  float = 0.0,
    ):
        try:
            from langchain_openai import AzureChatOpenAI     # langchain-openai >= 0.1
        except ImportError:
            raise ImportError(
                "Install langchain-openai: pip install langchain-openai"
            )

        self._deployment = (
            deployment
            or os.environ.get("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
        )

        self._llm = AzureChatOpenAI(
            azure_deployment   = self._deployment,
            azure_endpoint     = os.environ.get("AZURE_OPENAI_ENDPOINT", ""),
            api_key            = os.environ.get("AZURE_OPENAI_API_KEY", ""),
            api_version        = os.environ.get("AZURE_OPENAI_API_VERSION", "2024-02-01"),
            temperature        = temperature,
        )
        logger.info(
            f"[LLM] AzureChatOpenAI initialised (deployment={self._deployment!r})"
        )

    # ----------------------------------------------------------
    async def simple_query(self, prompt: str) -> str:
        """
        Send a single-turn prompt **asynchronously** and return the model's
        reply as a string.  The caller must ``await`` this coroutine.

        Uses ``AzureChatOpenAI.ainvoke`` so the asyncio event loop is
        **never blocked** while waiting for the Azure OpenAI response.

        Parameters
        ----------
        prompt : str
            The full prompt text.

        Returns
        -------
        str
            The model's text response (content of the first message).
        """
        from langchain_core.messages import HumanMessage

        logger.debug(f"[LLM] simple_query prompt ({len(prompt)} chars)")
        messages = [HumanMessage(content=prompt)]
        # ainvoke returns a coroutine — await it without blocking the loop
        response = await self._llm.ainvoke(messages)
        reply    = response.content
        logger.debug(
            f"[LLM] simple_query reply: {reply[:120]!r}"
            f"{'...' if len(reply) > 120 else ''}"
        )
        return reply
