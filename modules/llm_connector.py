"""
modules/llm_connector.py
========================
LangChain-based Azure OpenAI GPT-4o connector.

Provides ``LLM_cls`` with an **async** ``simple_query(prompt) -> str`` method
used by ``ETAChecker`` (and any other module that needs LLM access).

Because ``simple_query`` is a coroutine, callers must ``await`` it::

    reply = await llm.simple_query("What date is 'Mar.3' in 2026?")

    # Generic / non-date use: supply your own system_prompt
    reply = await llm.simple_query(prompt_text, system_prompt="You are ...")

Configuration
-------------
Set the following environment variables (or fill them in config.py):

    AZURE_OPENAI_API_KEY       your Azure OpenAI key
    AZURE_OPENAI_ENDPOINT      e.g. https://<resource>.openai.azure.com
    AZURE_OPENAI_DEPLOYMENT    your deployment name (e.g. "gpt-4o")
    AZURE_OPENAI_API_VERSION   e.g. "2024-02-01"
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


# ─────────────────────────────────────────────────────────────────────────────
# Module-level constants
# ─────────────────────────────────────────────────────────────────────────────

# Default system prompt used when simple_query is called without system_prompt.
# Instructs the model to behave as a JSON-only date-parsing specialist.
_DATE_SYSTEM_PROMPT = (
    "You are a precise date-parsing assistant specialised in interpreting freeform "
    "date strings written by hardware design engineers in a project tracker.\n\n"
    "Your rules:\n"
    "1. Always respond with a single JSON object and nothing else -- "
    "no prose, no markdown fences, no explanation.\n"
    "2. The JSON must have exactly one key: \"date\" whose value is an ISO-8601 string "
    "(YYYY-MM-DD), OR null if the intent cannot be determined at all.\n"
    "3. If the input is clearly \"done\", \"v\", \"N/A\", \"x\" or a similar completion "
    "marker, set \"date\" to the string \"done\".\n"
    "4. When multiple dates appear (e.g. \"3/2 -> 3/4\" or \"3/4(3/2)\"), the "
    "last / outermost value is the current ETA; use that one.\n"
    "5. If the year is ambiguous, assume the current or next calendar year so "
    "that the resulting date is not more than 60 days in the past.\n\n"
    "Examples:\n"
    "  \"Mar/4th\"    -> {\"date\": \"2026-03-04\"}\n"
    "  \"4 Mar\"      -> {\"date\": \"2026-03-04\"}\n"
    "  \"4/Mar\"      -> {\"date\": \"2026-03-04\"}\n"
    "  \"03.04\"      -> {\"date\": \"2026-03-04\"}\n"
    "  \"3/2 -> 3/4\" -> {\"date\": \"2026-03-04\"}\n"
    "  \"3/4(3/2)\"   -> {\"date\": \"2026-03-04\"}\n"
    "  \"done\"       -> {\"date\": \"done\"}\n"
    "  \"v\"          -> {\"date\": \"done\"}\n"
    "  \"\"           -> {\"date\": null}"
)

# Human-slot templates paired with the two modes above.
_DATE_HUMAN_TEMPLATE    = "Parse the following ETA field value and return the JSON result:\n{message}"
_GENERIC_HUMAN_TEMPLATE = "{message}"


# ─────────────────────────────────────────────────────────────────────────────
# LLM_cls
# ─────────────────────────────────────────────────────────────────────────────

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

        self._llm = AzureChatOpenAI(
            azure_endpoint     = os.environ.get("AZURE_OPENAI_ENDPOINT", ""),
            model              = 'aide-gpt-4o-mini',
            api_key            = os.environ.get("AZURE_OPENAI_API_KEY", ""),
            api_version        = os.environ.get("api_version", "2024-02-01"),
            extra_headers      = {"X-User-Id": os.environ.get("azure_user_id", "xxxxxxx")},
            http_client        = client,        # sync httpx.Client (for blocking calls)
            http_async_client  = httpx_client,  # async httpx.AsyncClient (for ainvoke)
        )
        logger.info("[LLM] AzureChatOpenAI initialised (model=aide-gpt-4o-mini)")

    # ----------------------------------------------------------
    async def simple_query(
        self,
        query: str,
        system_prompt: str | None = None,
    ) -> str:
        """
        Send *query* to the LLM and return the text reply.

        Parameters
        ----------
        query : str
            The user-facing message / question.
        system_prompt : str, optional
            Override the assistant's persona and output rules.

            * ``None`` (default) — uses ``_DATE_SYSTEM_PROMPT`` so that
              callers in ``ETAChecker._llm_parse`` continue to receive
              JSON-only date answers with the specialised human template:
              "Parse the following ETA field value …".

            * Any non-None string — uses that string as the system role
              and sends *query* verbatim as the human message.  Use this
              for non-date tasks such as generating ETA-request message
              templates (``teams_notifier._get_eta_message_template``).

        Returns
        -------
        str
            The model's reply (``response.content``).
        """
        if system_prompt is None:
            sys_msg       = _DATE_SYSTEM_PROMPT
            human_tmpl    = _DATE_HUMAN_TEMPLATE
        else:
            sys_msg       = system_prompt
            human_tmpl    = _GENERIC_HUMAN_TEMPLATE

        prompt = ChatPromptTemplate.from_messages([
            ("system", sys_msg),
            ("human",  human_tmpl),
        ])

        chain    = prompt | self._llm
        response = await chain.ainvoke({"message": query})
        reply    = response.content   # BaseMessage uses .content (lowercase)
        logger.info(
            f"[LLM] simple_query reply: {reply[:120]!r}"
            f"{'...' if len(reply) > 120 else ''}"
        )
        return reply