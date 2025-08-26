import os
from typing import Dict
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential

def get_pbi_headers() -> Dict[str, str]:
    """
    Build a Power BI REST API token using Service Principal.
    Requires TENANT_ID, CLIENT_ID, CLIENT_SECRET in env / .env.
    """
    load_dotenv()
    tenant = os.getenv("TENANT_ID") or ""
    client = os.getenv("CLIENT_ID") or ""
    secret = os.getenv("CLIENT_SECRET") or ""
    if not (tenant and client and secret):
        raise ValueError("TENANT_ID, CLIENT_ID, and CLIENT_SECRET must be set for Power BI auth.")

    cred = ClientSecretCredential(tenant_id=tenant, client_id=client, client_secret=secret)
    scope = "https://analysis.windows.net/powerbi/api/.default"
    token = cred.get_token(scope).token
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}