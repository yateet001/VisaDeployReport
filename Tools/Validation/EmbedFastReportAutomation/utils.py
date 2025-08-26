import re
from pathlib import Path

def resolve_edge_profile_dir(profile_dir: str) -> str:
    p = Path(profile_dir).resolve()
    p.mkdir(parents=True, exist_ok=True)
    return str(p)

def likely_auth_url(u: str) -> bool:
    u = (u or "").lower()
    return any(s in u for s in [
        "login.microsoftonline.com", "login.microsoft.com",
        "sts.", "adfs.", "sso.", "auth."
    ])

def sanitize_filename(s: str) -> str:
    return (
        (s or "")
        .replace("/", "_").replace("\\", "_")
        .replace(":", "_").replace("?", "_")
        .replace("*", "_").replace("|", "_")
        .replace('"', "_").replace("<", "_").replace(">", "_")
        .replace(" ", "_").strip("_")
    )

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()