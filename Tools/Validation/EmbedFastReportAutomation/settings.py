import os
from pathlib import Path

# Roots & thresholds
OUTPUT_ROOT = os.environ.get("POWERBI_OUTPUT_DIR", "PowerBI_reports")
MSTR_OUTPUT_ROOT = os.environ.get("MSTR_OUTPUT_DIR", "MSTR_reports")
COMPARISONS_DIR = "comparisons"
HIGHLIGHT_COLOR = "FFFF0000"  # red
MATCHING_THRESHOLD = 50       # %

# Report config (Excel)
REPORT_CONFIG_XLSX = os.environ.get("REPORT_CONFIG_XLSX", "reports.xlsx")
REPORT_CONFIG_SHEET = os.environ.get("REPORT_CONFIG_SHEET", "Config_file")
REPORT_NAME_FILTER = os.environ.get("REPORT_NAME", "").strip()

# Visual mapping (xlsx + sheet)
MAPPING_XLSX  = os.environ.get("VISUAL_MAPPING_XLSX", "mapping.xlsx")
MAPPING_SHEET = os.environ.get("VISUAL_MAPPING_SHEET", "mapping")

# Browser profile for SSO reuse
EDGE_PROFILE_DIR = ".edge-user-data"

def ensure_dir(p: str) -> str:
    Path(p).mkdir(parents=True, exist_ok=True)
    return p