import os
import re
import json
import asyncio
import logging
import time
from typing import List, Dict, Set, Tuple, Optional
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
from .settings import (
    REPORT_CONFIG_XLSX, REPORT_CONFIG_SHEET, REPORT_NAME_FILTER,
    OUTPUT_ROOT, MSTR_OUTPUT_ROOT, COMPARISONS_DIR, ensure_dir, EDGE_PROFILE_DIR, MAPPING_XLSX, MAPPING_SHEET,
)
from .config_loader import load_reports_from_excel, load_visual_mapping
from .utils import resolve_edge_profile_dir, likely_auth_url, norm
from .worker import SingleReportWorker
from .compare import scan_data_tree, compare_files
import pandas as pd

logger = logging.getLogger(__name__)

class ReportsRunner:
    def __init__(self):
        all_reports = load_reports_from_excel(REPORT_CONFIG_XLSX, sheet_name=REPORT_CONFIG_SHEET)
        if not all_reports:
            raise ValueError("Excel config has no valid rows (check required columns and data).")
        if REPORT_NAME_FILTER:
            filtered = [r for r in all_reports if norm(r.get("name")) == norm(REPORT_NAME_FILTER)]
            if not filtered:
                raise ValueError(f"REPORT_NAME='{REPORT_NAME_FILTER}' not found in Excel config.")
            self.reports = filtered
        else:
            self.reports = all_reports

    async def run(self):
        async with async_playwright() as p:
            profile_path = resolve_edge_profile_dir(EDGE_PROFILE_DIR)
            logger.info(f"Using persistent profile at: {profile_path}")

            try:
                context = await p.chromium.launch_persistent_context(
                    user_data_dir=profile_path,
                    channel="msedge",
                    headless=False,
                    viewport={"width": 1920, "height": 1080},
                    accept_downloads=True,
                    ignore_https_errors=True,
                    args=["--no-first-run", "--no-default-browser-check"],
                )
                logger.info(f"Launched Edge with profile at '{profile_path}'")
            except Exception as e:
                raise RuntimeError(
                    "Could not launch Microsoft Edge with persistent profile.\n"
                    f"Underlying error: {e}"
                )

            page = context.pages[0] if context.pages else await context.new_page()

            for idx, report in enumerate(self.reports):
                name = report.get("name") or f"report_{idx+1}"
                url = report.get("url")
                pages = report.get("pages", [])
                if not url:
                    logger.warning(f"Report '{name}' missing URL. Skipping."); continue

                logger.info(f"=== [{idx+1}/{len(self.reports)}] Opening report: {name} ===")
                await page.goto(url, wait_until="domcontentloaded")
                try:
                    await page.wait_for_load_state("domcontentloaded", timeout=20000)
                except PWTimeout:
                    pass

                if likely_auth_url(page.url):
                    logger.info("Sign-in detected. Waiting for auth completion...")
                    try:
                        not_auth = re.compile(
                            r"^(?!.*(login\.microsoftonline\.com|login\.microsoft\.com|sts\.|adfs\.|sso\.|auth\.)).*",
                            re.I
                        )
                        await page.wait_for_url(not_auth, timeout=240000)
                        logger.info("Authentication completed.")
                    except PWTimeout:
                        logger.warning("Authentication timeout. Please complete login manually.")
                        continue

                try:
                    # small manual gate (keeps your original UX)
                    try:
                        prompt_msg = "\nPress ENTER when the report is fully loaded to proceed… "
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(None, lambda: input(prompt_msg))
                    except EOFError:
                        logger.info("stdin not available; continuing automatically in 5 seconds.")
                        await page.wait_for_timeout(5000)

                    await page.wait_for_selector("iframe, #pvExplorationHost, [data-testid='artifact-info-title']")
                    iframe = await page.query_selector("iframe")
                    if iframe:
                        logger.info("Report detected in iframe, switching context...")
                        frame = await iframe.content_frame()
                        if frame:
                            worker = SingleReportWorker(
                                page=page, frame=frame,
                                config_report_name=name,
                                pages_order=[p.get("name", f"page_{i+1}") for i, p in enumerate(pages)],
                                page_visuals={
                                    norm(p.get("name", f"page_{i+1}")):
                                        ({"__ALL__"} if any(norm(v) == "all" for v in (p.get("visuals") or []))
                                         else {norm(v) for v in (p.get("visuals") or [])})
                                    for i, p in enumerate(pages)
                                }
                            )
                            await worker.run_for_current_report(url)
                            continue
                        else:
                            logger.warning("Failed to switch to iframe context")
                            continue
                except PWTimeout:
                    logger.warning("Report surface not detected. Please check if report loaded correctly.")
                    continue

                worker = SingleReportWorker(
                    page=page,
                    config_report_name=name,
                    pages_order=[p.get("name", f"page_{i+1}") for i, p in enumerate(pages)],
                    page_visuals={
                        norm(p.get("name", f"page_{i+1}")): {norm(v) for v in (p.get("visuals") or [])}
                        for i, p in enumerate(pages)
                    }
                )
                await worker.run_for_current_report(url)

            await context.close()

            # -------- Comparisons (directory-based) --------
            pbi_root  = os.path.join(os.path.abspath(os.getcwd()), OUTPUT_ROOT)
            mstr_root = os.path.join(os.path.abspath(os.getcwd()), MSTR_OUTPUT_ROOT)

            pbi_map  = scan_data_tree(pbi_root)
            mstr_map = scan_data_tree(mstr_root)

            vis_map = load_visual_mapping(MAPPING_XLSX, sheet_name=MAPPING_SHEET)

            # Stopwatch start
            start_time = time.time()
            print("⏱️ Starting comparisons (directory-based)...")

            comparison_results = {"Comparison": {}}
            print("Starting comparisons (directory-based)...")
            page_summary: Dict[Tuple[str, str], Dict[str, object]] = {}

            for report in sorted(set(mstr_map.keys()) & set(pbi_map.keys())):
                comparison_results["Comparison"].setdefault(report, {})
                m_pages = mstr_map[report]
                p_pages = pbi_map[report]

                for page_name in sorted(set(m_pages.keys()) & set(p_pages.keys())):
                    comparison_results["Comparison"][report].setdefault(page_name, {})
                    m_visuals = m_pages[page_name]  # { mstr_key_lower: path }
                    p_visuals = p_pages[page_name]  # { pbi_key_lower:  path }

                    pairs_to_compare: List[Tuple[str, str, str, bool]] = []  # (mstr_file, pbi_file, label_base, is_mapped)

                    # Case-insensitive mapping key uses sanitized folder names
                    map_key = (report.lower(), page_name.lower())

                    # 1) If mapping exists for (report, page), honor ONLY mapped pairs that exist on disk
                    if map_key in vis_map:
                        for item in vis_map[map_key]:
                            mstr_file = m_visuals.get(item["mstr_key"])
                            pbi_file  = p_visuals.get(item["pbi_key"])
                            if not mstr_file:
                                logger.warning(f"[Mapping] MSTR visual not found on disk: report='{report}' page='{page_name}' mstr='{item['mstr_label']}'")
                            if not pbi_file:
                                logger.warning(f"[Mapping] PBI visual not found on disk: report='{report}' page='{page_name}' pbi='{item['pbi_label']}'")
                            if mstr_file and pbi_file:
                                label_base = f'{item["pbi_label"]}VS{item["mstr_label"]}'
                                pairs_to_compare.append((mstr_file, pbi_file, label_base, True))

                    # 2) If NO mapped pairs were found for this (report,page), fallback to same-name comparison
                    if not pairs_to_compare:
                        for visual_key, mstr_file in m_visuals.items():
                            pbi_file = p_visuals.get(visual_key)
                            if pbi_file:
                                # keep legacy naming for fallback scenario
                                import os as _os
                                label_base = _os.path.splitext(_os.path.basename(mstr_file))[0]
                                pairs_to_compare.append((mstr_file, pbi_file, label_base, False))

                    # 3) Run comparisons for collected pairs
                    for mstr_file, pbi_file, label_base, is_mapped in pairs_to_compare:
                        out_dir = ensure_dir(os.path.join(COMPARISONS_DIR, report, page_name))
                        out_name = f"{label_base}.xlsx" if is_mapped else f"{label_base}_comparison.xlsx"
                        output_path = os.path.join(out_dir, out_name)

                        cmp_path, is_match = compare_files(mstr_file, pbi_file, output_path)
                        comparison_results["Comparison"][report][page_name][label_base] = cmp_path

                        key = (report, page_name)
                        if key not in page_summary:
                            page_summary[key] = {"matched": 0, "not_matched": 0, "not_matched_paths": []}
                        if is_match:
                            page_summary[key]["matched"] += 1
                        else:
                            page_summary[key]["not_matched"] += 1
                            page_summary[key]["not_matched_paths"].append(cmp_path)


            rows = []
            for (rep, pg), agg in page_summary.items():
                rows.append({
                    "Reports": rep,
                    "Page": pg,
                    "Not Matched (Count of visuals)": agg.get("not_matched", 0),
                    "Matched (Count of visuals)": agg.get("matched", 0),
                    "Not Matched Visual Paths": "\n".join(agg.get("not_matched_paths", [])),
                })

            if rows:
                ensure_dir(COMPARISONS_DIR)
                summary_path = os.path.join(COMPARISONS_DIR, "summary.xlsx")
                pd.DataFrame(rows, columns=[
                    "Reports", "Page",
                    "Not Matched (Count of visuals)",
                    "Matched (Count of visuals)",
                    "Not Matched Visual Paths",
                ]).to_excel(summary_path, sheet_name="Summary", index=False)
                print(f"Summary saved: {summary_path}")
            else:
                print("No comparison pairs found; summary not created.")

            with open("comparison_results.json", "w", encoding="utf-8") as f:
                json.dump(comparison_results, f, indent=2)
            print("\n✅ All comparisons done (directory-based). Results mapping saved in comparison_results.json")
            elapsed = time.time() - start_time
            print(f"✅ All comparisons finished in {elapsed:.2f} seconds")