"""
modules/sharepoint_connector.py
================================
Provides two connector classes:

  LocalSharePointConnector  — reads/writes from a local folder tree,
                              simulating SharePoint behaviour on the
                              current machine.

  RemoteSharePointConnector — sample class for the REAL SharePoint REST
                              API (NTLM auth).  Use on remote machine.
                              Search for "--- REMOTE ONLY ---" to find
                              every placeholder you must fill in.

Usage (local simulation):
    from modules.sharepoint_connector import get_connector
    sp = get_connector()
    excel_bytes = sp.download_excel()
    sp.upload_file("fp_guide/foo.pdf", open("foo.pdf","rb").read())
    print(sp.list_folder("fp_guide"))
"""

import os
import shutil
import logging
from pathlib import Path

import config

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Factory
# ─────────────────────────────────────────────────────────────

def get_connector():
    """Return the appropriate connector based on config.USE_LOCAL_SHAREPOINT."""
    if config.USE_LOCAL_SHAREPOINT:
        return LocalSharePointConnector()
    else:
        return RemoteSharePointConnector(                      # --- REMOTE ONLY ---
            url      = config.SHAREPOINT_URL,
            username = config.SHAREPOINT_USERNAME,
            password = config.SHAREPOINT_PASSWORD,
            root     = config.ROOT_FOLDER,
        )


# ─────────────────────────────────────────────────────────────
# LOCAL connector  (simulation)
# ─────────────────────────────────────────────────────────────

class LocalSharePointConnector:
    """
    Simulates SharePoint by reading/writing to LOCAL_SHAREPOINT_ROOT.

    Folder structure mirrors a real SharePoint document library:
        local_sharepoint/
            excel/                ← excel file lives here
            fp_guide/             ← FE floor-plan guide uploads
    """

    def __init__(self, root: str = config.LOCAL_SHAREPOINT_ROOT):
        self.root = Path(root)
        self._ensure_dirs()

    def _ensure_dirs(self):
        (self.root / config.EXCEL_SP_PATH.strip("/")).mkdir(parents=True, exist_ok=True)
        (self.root / config.FP_GUIDE_SP_PATH.strip("/")).mkdir(parents=True, exist_ok=True)

    # ----------------------------------------------------------
    def download_excel(self) -> bytes:
        """
        Return the Excel file bytes.
        First tries the local SharePoint simulation folder;
        falls back to LOCAL_EXCEL_PATH (the real local spreadsheet).
        """
        sp_path = self.root / config.EXCEL_SP_PATH.strip("/") / config.EXCEL_FILENAME
        if sp_path.exists():
            logger.info(f"[LOCAL SP] Reading Excel from simulated SharePoint: {sp_path}")
            return sp_path.read_bytes()

        # Fallback to the original local file
        logger.info(f"[LOCAL SP] Falling back to: {config.LOCAL_EXCEL_PATH}")
        return Path(config.LOCAL_EXCEL_PATH).read_bytes()

    # ----------------------------------------------------------
    def upload_file(self, remote_relative_path: str, data: bytes) -> bool:
        """
        Write `data` to local_sharepoint/<remote_relative_path>.
        Creates intermediate directories as needed.
        """
        dest = self.root / remote_relative_path.lstrip("/")
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        logger.info(f"[LOCAL SP] Uploaded → {dest}")
        return True

    # ----------------------------------------------------------
    def list_folder(self, folder_relative_path: str) -> list[dict]:
        """
        List files in a simulated SharePoint folder.
        Returns list of dicts: {name, size, path}.
        """
        folder = self.root / folder_relative_path.lstrip("/")
        if not folder.exists():
            return []
        return [
            {"name": f.name, "size": f.stat().st_size, "path": str(f)}
            for f in folder.iterdir()
            if f.is_file()
        ]


# ─────────────────────────────────────────────────────────────
# REMOTE connector  (sample — activate on remote machine)
# ─────────────────────────────────────────────────────────────

class RemoteSharePointConnector:                               # --- REMOTE ONLY ---
    """
    Real SharePoint REST API connector using requests + NTLM authentication.

    Required packages (already in requirements.txt):
        pip install requests requests_ntlm

    Replace the placeholder values in config.py with your real credentials.
    """

    def __init__(self, url: str, username: str, password: str, root: str):
        try:
            import requests
            from requests_ntlm import HttpNtlmAuth                # --- REMOTE ONLY ---
        except ImportError as e:
            raise ImportError("Install requests and requests_ntlm: pip install requests requests_ntlm") from e

        self.url      = url.rstrip("/")
        self.root     = root.rstrip("/")
        self.session  = requests.Session()
        self.session.auth = HttpNtlmAuth(username, password)     # --- REMOTE ONLY ---
        self.session.headers.update({
            "Accept":       "application/json;odata=verbose",
            "Content-Type": "application/json;odata=verbose",
        })

    # ----------------------------------------------------------
    def download_excel(self) -> bytes:                           # --- REMOTE ONLY ---
        """Download Excel file bytes from SharePoint."""
        file_server_path = f"{self.root}/{config.EXCEL_SP_PATH.strip('/')}/{config.EXCEL_FILENAME}"
        api_url = (
            f"{self.url}/_api/web"
            f"/GetFileByServerRelativeUrl('{file_server_path}')/$value"
        )
        resp = self.session.get(api_url)
        resp.raise_for_status()
        logger.info(f"[REMOTE SP] Downloaded Excel from {api_url}")
        return resp.content

    # ----------------------------------------------------------
    def upload_file(self, remote_relative_path: str, data: bytes) -> bool:  # --- REMOTE ONLY ---
        """Upload bytes to a SharePoint folder."""
        parts  = remote_relative_path.lstrip("/").rsplit("/", 1)
        folder = f"{self.root}/{parts[0]}" if len(parts) == 2 else self.root
        fname  = parts[-1]

        # Ensure folder exists
        self._ensure_folder(folder)

        api_url = (
            f"{self.url}/_api/web"
            f"/GetFolderByServerRelativeUrl('{folder}')"
            f"/Files/Add(url='{fname}', overwrite=true)"
        )
        headers = dict(self.session.headers)
        headers["Content-Type"] = "application/octet-stream"
        resp = self.session.post(api_url, data=data, headers=headers)
        resp.raise_for_status()
        logger.info(f"[REMOTE SP] Uploaded → {folder}/{fname}")
        return True

    # ----------------------------------------------------------
    def list_folder(self, folder_relative_path: str) -> list[dict]:  # --- REMOTE ONLY ---
        """List files in a SharePoint folder."""
        folder = f"{self.root}/{folder_relative_path.strip('/')}"
        api_url = (
            f"{self.url}/_api/web"
            f"/GetFolderByServerRelativeUrl('{folder}')/Files"
        )
        resp = self.session.get(api_url)
        resp.raise_for_status()
        items = resp.json().get("d", {}).get("results", [])
        return [{"name": i["Name"], "size": i["Length"], "path": i["ServerRelativeUrl"]}
                for i in items]

    # ----------------------------------------------------------
    def _ensure_folder(self, server_relative_path: str):         # --- REMOTE ONLY ---
        """Create SharePoint folder if it doesn't exist."""
        api_url = f"{self.url}/_api/web/folders"
        payload = {
            "__metadata": {"type": "SP.Folder"},
            "ServerRelativeUrl": server_relative_path,
        }
        import json
        self.session.post(api_url, data=json.dumps(payload))
        # Ignore errors — folder might already exist
