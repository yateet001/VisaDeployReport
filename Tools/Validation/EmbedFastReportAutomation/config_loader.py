from typing import List, Dict, Tuple, Optional, Set
import os
import pandas as pd
from .authentication import get_pbi_headers
from .embedFast_client import resolve_ids_for_names
from .utils import norm, sanitize_filename

def load_reports_from_excel(xlsx_path: str, sheet_name: Optional[str] = None) -> List[dict]:
    """
    Excel columns: workspace_name, report_name, page_name, visual_name
    Returns:
    [
      {
        "name": <report_name>,
        "url":  "https://embedfastdev-app.azurewebsites.net/groups/<group_id>/reports/<report_id>",
        "pages": [{"name": <page>, "visuals": [<visual>, ...]}, ...],
      }, ...
    ]
    """
    sheet = sheet_name or "Config_file"
    if not os.path.isfile(xlsx_path):
        raise FileNotFoundError(f"Excel config '{xlsx_path}' not found.")

    df = pd.read_excel(xlsx_path, sheet_name=sheet, dtype=str).fillna("")
    required = {"workspace_name", "report_name"}
    missing = required - set(map(str, df.columns))
    if missing:
        raise ValueError(f"Excel sheet is missing required columns: {sorted(missing)}")

    headers = get_pbi_headers()
    name_id_map = resolve_ids_for_names(df[["workspace_name", "report_name"]], headers)

    grouped: Dict[Tuple[str, str], Dict[str, List[str]]] = {}
    for _, r in df.iterrows():
        ws = str(r.get("workspace_name", "")).strip()
        rp = str(r.get("report_name", "")).strip()
        pg = str(r.get("page_name", "")).strip()
        vs = str(r.get("visual_name", "")).strip()
        if not (ws and rp):
            continue
        key = (ws, rp)
        grouped.setdefault(key, {})
        if pg:
            grouped[key].setdefault(pg, [])
            if vs:
                grouped[key][pg].append(vs)

    reports: List[dict] = []
    for (ws_name, rp_name), pages in grouped.items():
        gid, rid = name_id_map[(ws_name, rp_name)]
        url = f"https://embedfastdev-app.azurewebsites.net/groups/{gid}/reports/{rid}"

        page_list = []
        for pg_name, visuals in pages.items():
            seen: Set[str] = set()
            vs_out: List[str] = []
            for v in visuals:
                k = norm(v)
                if k not in seen:
                    seen.add(k)
                    vs_out.append(v)
            page_list.append({"name": pg_name, "visuals": vs_out})

        reports.append({"name": rp_name, "url": url, "pages": page_list})

    return reports

def load_visual_mapping(xlsx_path: str, sheet_name: str = "mapping") -> Dict[Tuple[str, str], List[dict]]:
    """
    Sheet (default 'mapping') with columns:
      report_name, page_name, pbi_visual, mstr_visual

    Returns:
      {
        (report_key, page_key): [
          {
            "pbi_key":   <lowercased sanitized visual base for lookup>,
            "mstr_key":  <lowercased sanitized visual base for lookup>,
            "pbi_label": <sanitized (case kept) for filename>,
            "mstr_label":<sanitized (case kept) for filename>,
          }, ...
        ]
      }

    Keys used for dictionary lookups:
      report_key = sanitize_filename(report_name).lower()
      page_key   = sanitize_filename(page_name).lower()
    Visual lookup keys are also sanitized+lowercased to match scan_data_tree().
    """
    if not os.path.isfile(xlsx_path):
        return {}

    try:
        df = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str).fillna("")
    except ValueError:
        # Sheet not present
        return {}

    required = {"report_name", "page_name", "pbi_visual", "mstr_visual"}
    if not required.issubset(set(map(str, df.columns))):
        return {}

    mapping: Dict[Tuple[str, str], List[dict]] = {}
    for _, r in df.iterrows():
        rep_raw  = str(r.get("report_name", "")).strip()
        page_raw = str(r.get("page_name", "")).strip()
        pbi_raw  = str(r.get("pbi_visual", "")).strip()
        mstr_raw = str(r.get("mstr_visual", "")).strip()
        if not (rep_raw and page_raw and pbi_raw and mstr_raw):
            continue

        # Sanitize report/page like the export code does for folder names
        rep_key  = sanitize_filename(rep_raw).lower()
        page_key = sanitize_filename(page_raw).lower()

        mapping.setdefault((rep_key, page_key), []).append({
            "pbi_key":    sanitize_filename(pbi_raw).lower(),
            "mstr_key":   sanitize_filename(mstr_raw).lower(),
            "pbi_label":  sanitize_filename(pbi_raw),
            "mstr_label": sanitize_filename(mstr_raw),
        })
    return mapping