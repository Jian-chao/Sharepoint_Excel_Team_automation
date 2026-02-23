"""
modules/splunk_connector.py
============================
Two connector classes:

  MockSplunkConnector   — returns hardcoded sample data so the system
                          runs on the current machine without Splunk.

  RemoteSplunkConnector — sample showing the REAL Splunk REST API flow
                          using httplib2 as specified by the user.
                          Search for "--- REMOTE ONLY ---" to find every
                          placeholder to replace on your remote machine.

Usage:
    from modules.splunk_connector import get_connector
    splunk = get_connector()
    df     = splunk.query()
    status = splunk.get_latest_status("venc_top_par_wrap")
    # → {"netlist": True, "sdc": False, "ccf": True, "upf": False}
"""

import io
import json
import logging
import time
import urllib.parse

import pandas as pd

import config

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Factory
# ─────────────────────────────────────────────────────────────

def get_connector():
    if config.USE_MOCK_SPLUNK:
        return MockSplunkConnector()
    return RemoteSplunkConnector(                               # --- REMOTE ONLY ---
        base_url  = config.SPLUNK_BASE_URL,
        username  = config.SPLUNK_USERNAME,
        password  = config.SPLUNK_PASSWORD,
        query     = config.SPLUNK_SEARCH_QUERY,
    )


# ─────────────────────────────────────────────────────────────
# Shared base
# ─────────────────────────────────────────────────────────────

_DELIVERABLE_COLS = ["netlist", "sdc", "ccf", "upf"]

class _BaseConnector:
    def get_latest_status(self, module_name: str) -> dict:
        """
        Filter the Splunk CSV data for a specific MODULE name,
        pick the row(s) with the latest Timestamp, and return
        upload status for each deliverable.

        Returns
        -------
        dict  e.g. {"netlist": True, "sdc": False, "ccf": True, "upf": True}
              True  → Viol == 1 (file exists in perforce)
              False → Viol == 0 (not uploaded yet)
              None  → no Splunk data found for this module
        """
        df = self.query()
        if df.empty:
            return {col: None for col in _DELIVERABLE_COLS}

        df_mod = df[df["MODULE"].str.lower() == module_name.lower()].copy()
        if df_mod.empty:
            logger.warning(f"[Splunk] No data found for MODULE={module_name!r}")
            return {col: None for col in _DELIVERABLE_COLS}

        # Always use the latest timestamp
        df_mod["Timestamp"] = pd.to_numeric(df_mod["Timestamp"], errors="coerce")
        latest_ts = df_mod["Timestamp"].max()
        df_latest = df_mod[df_mod["Timestamp"] == latest_ts]

        result = {}
        for col in _DELIVERABLE_COLS:
            rows = df_latest[df_latest["SUB_GROUP"].str.lower() == col.lower()]
            if rows.empty:
                result[col] = None
            else:
                viol_val = rows["Viol"].iloc[0]
                result[col] = int(viol_val) == 1
        return result

    def query(self) -> pd.DataFrame:
        raise NotImplementedError


# ─────────────────────────────────────────────────────────────
# MOCK connector
# ─────────────────────────────────────────────────────────────

_MOCK_CSV = """\
Timestamp,PROJ_NAME,PROJ_NO,TOOL_NAME,SUB_GROUP,MODULE,OWNER,Viol
1771814309,Disney,A11345,REQ_DEF,netlist,venc_top_par_wrap,chien-hung.chen,0
1771814309,Disney,A11345,REQ_DEF,sdc,venc_top_par_wrap,chien-hung.chen,1
1771814309,Disney,A11345,REQ_DEF,ccf,venc_top_par_wrap,chien-hung.chen,0
1771814309,Disney,A11345,REQ_DEF,upf,venc_top_par_wrap,chien-hung.chen,1
1771814309,Disney,A11345,REQ_DEF,netlist,venc_core1_top_par_wrap,yen-chen.chung,1
1771814309,Disney,A11345,REQ_DEF,sdc,venc_core1_top_par_wrap,yen-chen.chung,1
1771814309,Disney,A11345,REQ_DEF,ccf,venc_core1_top_par_wrap,yen-chen.chung,0
1771814309,Disney,A11345,REQ_DEF,upf,venc_core1_top_par_wrap,yen-chen.chung,0
1771814309,Disney,A11345,REQ_DEF,netlist,svppsys_top_par_wrap,ankit.kumar,1
1771814309,Disney,A11345,REQ_DEF,sdc,svppsys_top_par_wrap,ankit.kumar,1
1771814309,Disney,A11345,REQ_DEF,ccf,svppsys_top_par_wrap,ankit.kumar,1
1771814309,Disney,A11345,REQ_DEF,upf,svppsys_top_par_wrap,ankit.kumar,1
1771814309,Disney,A11345,REQ_DEF,netlist,usb_0_ext_par_wrap,candy.li,0
1771814309,Disney,A11345,REQ_DEF,sdc,usb_0_ext_par_wrap,candy.li,0
1771814309,Disney,A11345,REQ_DEF,ccf,usb_0_ext_par_wrap,candy.li,0
1771814309,Disney,A11345,REQ_DEF,upf,usb_0_ext_par_wrap,candy.li,0
1771814309,Disney,A11345,REQ_DEF,netlist,ufs_0_ext_par_wrap,yc.song,1
1771814309,Disney,A11345,REQ_DEF,sdc,ufs_0_ext_par_wrap,yc.song,0
1771814309,Disney,A11345,REQ_DEF,ccf,ufs_0_ext_par_wrap,yc.song,1
1771814309,Disney,A11345,REQ_DEF,upf,ufs_0_ext_par_wrap,yc.song,0
"""

class MockSplunkConnector(_BaseConnector):
    """Returns the hardcoded sample CSV data as a DataFrame."""

    def query(self) -> pd.DataFrame:
        logger.info("[MockSplunk] Returning mock data.")
        return pd.read_csv(io.StringIO(_MOCK_CSV))


# ─────────────────────────────────────────────────────────────
# REMOTE connector  (sample — activate on remote machine)
# ─────────────────────────────────────────────────────────────

class RemoteSplunkConnector(_BaseConnector):                   # --- REMOTE ONLY ---
    """
    Real Splunk REST API connector using httplib2 as specified by the user.

    Flow:
      1. POST /services/auth/login            → get sessionKey
      2. POST /services/search/jobs           → submit search, get sid
      3. Poll  GET  /services/search/jobs/<sid>  until isDone=True
      4. GET  /services/search/jobs/<sid>/results?output_mode=csv&count=0
      5. Parse bytes → pandas DataFrame via io.BytesIO
    """

    def __init__(self, base_url: str, username: str, password: str, query: str):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.query    = query
        self._http    = None

    def _get_http(self):                                       # --- REMOTE ONLY ---
        import httplib2
        if self._http is None:
            self._http = httplib2.Http(disable_ssl_certificate_validation=True)
        return self._http

    def _login(self) -> str:                                   # --- REMOTE ONLY ---
        """Authenticate and return a session key."""
        http = self._get_http()
        _, content = http.request(
            self.base_url + "/services/auth/login",
            "POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body=urllib.parse.urlencode(
                {"username": self.username, "password": self.password, "output_mode": "json"}
            ),
        )
        data = json.loads(content)
        return data["sessionKey"]

    def query(self) -> pd.DataFrame:                           # --- REMOTE ONLY ---
        """Run the configured Splunk search and return results as a DataFrame."""
        http = self._get_http()
        session_key = self._login()
        auth_header = {"Authorization": f"Splunk {session_key}"}

        # 1. Submit search job
        search_body = urllib.parse.urlencode({"search": "search " + self.query})
        _, content  = http.request(
            self.base_url + "/services/search/jobs",
            "POST",
            headers={**auth_header, "Content-Type": "application/x-www-form-urlencoded"},
            body=search_body,
        )
        sid = json.loads(content)["sid"]
        logger.info(f"[RemoteSplunk] Search job submitted, sid={sid}")

        # 2. Poll until done
        for _ in range(60):                                    # max 60 polls (5 min)
            time.sleep(5)
            _, status_content = http.request(
                self.base_url + f"/services/search/jobs/{sid}?output_mode=json",
                "GET",
                headers=auth_header,
            )
            status = json.loads(status_content)
            is_done = status["entry"][0]["content"]["isDone"]
            if is_done:
                break
            logger.debug(f"[RemoteSplunk] Polling sid={sid} ...")
        else:
            raise TimeoutError(f"Splunk search {sid} did not complete within 5 minutes")

        # 3. Download CSV results
        _, csv_content = http.request(
            self.base_url + f"/services/search/jobs/{sid}/results?output_mode=csv&count=0",
            "GET",
            headers=auth_header,
        )
        df = pd.read_csv(io.BytesIO(csv_content))
        logger.info(f"[RemoteSplunk] Downloaded {len(df)} rows for sid={sid}")
        return df
