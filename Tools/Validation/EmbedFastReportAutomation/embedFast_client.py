from typing import Dict, List, Set, Tuple
import requests
import pandas as pd

BASE_PBI = "https://api.powerbi.com/v1.0/myorg"

def _pbi_get(url: str, headers: Dict[str, str]) -> Dict:
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def list_workspaces(headers: Dict[str, str]) -> List[Dict]:
    url = f"{BASE_PBI}/groups"
    items: List[Dict] = []
    while url:
        data = _pbi_get(url, headers)
        items.extend(data.get("value", []))
        url = data.get("@odata.nextLink")
    return items

def list_reports_in_workspace(group_id: str, headers: Dict[str, str]) -> List[Dict]:
    url = f"{BASE_PBI}/groups/{group_id}/reports"
    items: List[Dict] = []
    while url:
        data = _pbi_get(url, headers)
        items.extend(data.get("value", []))
        url = data.get("@odata.nextLink")
    return items

def resolve_ids_for_names(df_names: pd.DataFrame, headers: Dict[str, str]) -> Dict[Tuple[str, str], Tuple[str, str]]:
    """
    df_names must include columns: workspace_name, report_name
    Returns mapping: {(workspace_name, report_name) -> (group_id, report_id)}
    Enforces uniqueness for workspace name & report name within workspace.
    """
    needed_ws = sorted(set(df_names["workspace_name"].dropna().astype(str).map(str.strip)))
    groups = list_workspaces(headers)

    ws_name_to_ids: Dict[str, Set[str]] = {}
    for g in groups:
        nm = (g.get("name") or "").strip()
        gid = g.get("id")
        if nm and gid:
            ws_name_to_ids.setdefault(nm.casefold(), set()).add(gid)

    ws_name_to_id: Dict[str, str] = {}
    dup_ws = []
    for ws in needed_ws:
        ids = list(ws_name_to_ids.get(ws.casefold(), []))
        if not ids:
            raise ValueError(f"Workspace '{ws}' not found in tenant.")
        if len(ids) > 1:
            dup_ws.append((ws, ids))
        else:
            ws_name_to_id[ws] = ids[0]
    if dup_ws:
        raise ValueError(
            "Multiple workspaces share the same name; disambiguate by ID:\n" +
            "\n".join(f"  - {ws}: {ids}" for ws, ids in dup_ws)
        )

    pair_to_ids: Dict[Tuple[str, str], Tuple[str, str]] = {}
    for ws_name, gid in ws_name_to_id.items():
        reports = list_reports_in_workspace(gid, headers)
        rep_name_to_ids: Dict[str, List[str]] = {}
        for r in reports:
            rn = (r.get("name") or "").casefold()
            rep_name_to_ids.setdefault(rn, []).append(r.get("id"))

        mask = df_names["workspace_name"].map(str.strip) == ws_name
        for rn in df_names.loc[mask, "report_name"].dropna().astype(str).map(str.strip).unique():
            ids = rep_name_to_ids.get(rn.casefold(), [])
            if not ids:
                raise ValueError(f"Report '{rn}' not found in workspace '{ws_name}'.")
            if len(ids) > 1:
                raise ValueError(f"Multiple reports named '{rn}' in workspace '{ws_name}': {ids}")
            pair_to_ids[(ws_name, rn)] = (gid, ids[0])

    return pair_to_ids