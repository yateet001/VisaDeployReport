import re
import os
from typing import List, Dict, Set, Optional, Tuple
from playwright.async_api import Page, Frame, Download
from .utils import norm, sanitize_filename
from .settings import OUTPUT_ROOT
import logging
logger = logging.getLogger(__name__)

class SingleReportWorker:
    def __init__(self, page: Page, config_report_name: str, pages_order: List[str], 
                 page_visuals: Dict[str, Set[str]], frame: Frame = None):
        self.page = page
        self.frame = frame
        self.context = frame if frame else page
        self.config_report_name = config_report_name
        self.pages_order = pages_order or []
        self.page_visuals = page_visuals or {}
        self.report_name: Optional[str] = None
        self.download_dir: Optional[str] = None

    async def run_for_current_report(self, url: str):
        await self._setup_report_folder()
        # Try get pages nav
        try:
            await self.context.wait_for_selector('[data-testid="pages-navigation-list"]', timeout=30000)
            pages_pane = await self.context.query_selector('[data-testid="pages-navigation-list"]')
            page_items = await pages_pane.query_selector_all('[data-testid="pages-navigation-list-items"]')
        except Exception as e:
            logger.warning(f"Could not find pages navigation list: {e}. Operating on current page.")
            page_items = []

        if self.pages_order and page_items:
            tabs = []
            for it in page_items:
                label = (await it.get_attribute("aria-label") or "")
                clean = re.sub(r'\s*selected\s*$', '', label, flags=re.IGNORECASE).strip()
                tabs.append((it, clean))

            for cfg_page_name in self.pages_order:
                norm_cfg = norm(cfg_page_name)
                safe_page_name = sanitize_filename(cfg_page_name)

                match_el = None
                for it, lab in tabs:
                    if norm(lab) == norm_cfg:
                        match_el = it; break
                if not match_el:
                    logger.warning(f"Page '{cfg_page_name}' not found; skipping.")
                    continue

                logger.info(f"[{self.config_report_name}] Switching to page: {cfg_page_name}")
                try:
                    await match_el.click()
                    await self.context.wait_for_timeout(1500)
                except Exception as e:
                    logger.warning(f"Failed to click page '{cfg_page_name}': {e}")
                    continue

                await self._capture_page_screenshot(safe_page_name)
                allowed_visuals = self.page_visuals.get(norm_cfg, set())
                await self._export_visuals_on_current_page(safe_page_name, allowed_visuals)
        else:
            logger.info(f"[{self.config_report_name}] Current page (no tabs / no pages in config).")
            await self._capture_page_screenshot("current_page")
            allowed = set()
            for vset in self.page_visuals.values():
                allowed |= vset
            await self._export_visuals_on_current_page("current_page", allowed)

    async def _setup_report_folder(self):
        safe_report_name = sanitize_filename(self.config_report_name) or "PowerBI_Report"
        self.report_name = safe_report_name
        root = os.path.join(os.path.abspath(os.getcwd()), OUTPUT_ROOT)
        os.makedirs(root, exist_ok=True)
        self.download_dir = os.path.join(root, self.report_name)
        os.makedirs(self.download_dir, exist_ok=True)
        logger.info(f"Output root: {root} | Report folder: {self.download_dir}")

    async def _capture_page_screenshot(self, safe_page_name: str):
        screenshot_dir = os.path.join(self.download_dir, safe_page_name, "screenshot")
        os.makedirs(screenshot_dir, exist_ok=True)
        screenshot_path = os.path.join(screenshot_dir, f"{safe_page_name}.png")
        try:
            await self.page.screenshot(path=screenshot_path, full_page=True)
            logger.info(f"Screenshot saved: {screenshot_path}")
        except Exception as e:
            logger.error(f"Failed to save screenshot for {safe_page_name}: {e}")

    async def _get_visual_title_for_matching(self, container) -> str:
        try:
            title_el = await container.query_selector("div[data-testid='visual-title']")
            if title_el:
                t = await title_el.get_attribute("title")
                if t and t.strip():
                    return t.strip()
        except: pass
        try:
            al = await container.get_attribute("aria-label")
            if al and al.strip():
                return al.strip()
        except: pass
        return ""

    async def _get_visual_name_for_files(self, container, index: int) -> str:
        name = ""
        try:
            title = await container.query_selector("div[data-testid='visual-title']")
            if title:
                t = await title.get_attribute("title")
                if t:
                    name = t.strip()
        except: pass
        if not name:
            name = (await container.get_attribute("aria-label") or "").strip()
        if not name:
            rd = (await container.get_attribute("aria-roledescription") or "visual").strip()
            name = f"{rd}_{index+1}"
        return sanitize_filename(name)

    async def _select_export_type_excel_by_id(self):
        dlg = self.page.locator(".ms-Dialog-main").last
        await dlg.wait_for(state="visible", timeout=10000)

        dd = dlg.locator("#Dropdown486")
        if await dd.count() == 0:
            lab = dlg.locator("label#Dropdown486-label, label:has-text('Export type')")
            if await lab.count() > 0:
                dd = lab.nth(0).locator("xpath=following::*[@role='combobox'][1]")
        if await dd.count() == 0:
            raise RuntimeError("Export type combobox (#Dropdown486) not found.")
        await dd.first.click()

        candidates = [self.page, self.context if self.context != self.page else None]
        found_list = None
        for ctx in candidates:
            if not ctx: continue
            lst = ctx.locator("#Dropdown486-list")
            if await lst.count() > 0:
                try:
                    await lst.first.wait_for(state="visible", timeout=5000)
                    found_list = lst.first; break
                except: pass
        if not found_list:
            for ctx in candidates:
                if not ctx: continue
                role_list = ctx.get_by_role("listbox")
                if await role_list.count() > 0:
                    found_list = role_list.first; break
        if not found_list:
            raise RuntimeError("Export type list (#Dropdown486-list) did not appear.")

        opt = found_list.locator("button:has-text('Microsoft Excel (.xlsx)')").first
        if await opt.count() == 0:
            ctx = found_list.page if hasattr(found_list, "page") else self.page
            opt = ctx.get_by_role("option", name=re.compile(r"Microsoft\s+Excel\s*\(\.xlsx\)", re.I)).first
        if await opt.count() == 0:
            raise RuntimeError("Export type 'Microsoft Excel (.xlsx)' not found.")
        await opt.click()

    async def _click_commandbar_export(self):
        async def _try_in_ctx(ctx) -> bool:
            if not ctx: return False
            try:
                try: await ctx.evaluate("() => window.scrollTo(0, 0)")
                except: pass

                bar = ctx.get_by_role("menubar").first
                if await bar.count() == 0:
                    bar = ctx.locator("div[title='CommandBar'], .ms-CommandBar").first
                    if await bar.count() == 0: return False

                export_btn = bar.get_by_role("menuitem", name=re.compile(r"^export$", re.I)).first
                if await export_btn.count() > 0 and await export_btn.is_visible():
                    await export_btn.click(); return True

                export_btn2 = bar.locator("button[title='Export']").first
                if await export_btn2.count() > 0 and await export_btn2.is_visible():
                    await export_btn2.click(); return True

                overflow_triggers = [
                    bar.get_by_role("button", name=re.compile(r"(more( commands| options)?|see more)", re.I)).first,
                    bar.locator("button[aria-haspopup='true']").filter(
                        has=bar.locator("i[data-icon-name='ChevronDown']")
                    ).first,
                ]
                for trigger in overflow_triggers:
                    if await trigger.count() > 0:
                        try:
                            await trigger.click()
                            menu = ctx.get_by_role("menu").last
                            if await menu.count() == 0:
                                menu = ctx.locator(".ms-ContextualMenu, .ms-Callout").last
                            await menu.wait_for(state="visible", timeout=5000)
                            item = menu.get_by_role("menuitem", name=re.compile(r"^export$", re.I)).first
                            if await item.count() == 0:
                                item = menu.get_by_text(re.compile(r"^export$", re.I)).first
                            if await item.count() > 0:
                                await item.click(); return True
                        except: pass
                return False
            except: return False

        if await _try_in_ctx(self.page): return
        if await _try_in_ctx(self.context if self.context != self.page else None): return
        raise RuntimeError("CommandBar 'Export' button not found/visible.")

    async def _select_combobox(self, label_text_regex: str, option_text_regex: str):
        cb = self.page.get_by_role("combobox", name=re.compile(label_text_regex, re.I))
        await cb.first.click()
        opt = self.page.get_by_role("option", name=re.compile(option_text_regex, re.I)).first
        await opt.click()

    async def _select_all_fields_in_dialog(self):
        dlg = self.page.locator(".ms-Dialog-main").last
        await dlg.wait_for(state="visible", timeout=10000)
        await self.page.wait_for_timeout(80)

        name_rx = re.compile(r"^select\s*fields$", re.I)
        label = dlg.locator("label.ms-Dropdown-label", has_text=name_rx).first
        await label.wait_for(state="visible", timeout=6000)
        label_id = await label.get_attribute("id")
        base_id  = (label_id or "").replace("-label", "")
        combo = dlg.locator(f"div#{base_id}[role='combobox']").first if base_id else None
        if not combo or await combo.count() == 0:
            combo = label.locator("xpath=following-sibling::*[@role='combobox'][1]").first
        if await combo.count() == 0:
            logger.warning("'Select fields' combobox not found."); return

        if (await combo.get_attribute("aria-expanded") or "false") != "true":
            await combo.click(); await self.page.wait_for_timeout(40)

        list_id = (await combo.get_attribute("aria-controls")) or (await combo.get_attribute("aria-owns")) or (f"{base_id}-list" if base_id else None)
        listbox = self.page.locator(f"#{list_id}").first if list_id else None
        if not listbox or await listbox.count() == 0:
            listbox = self.page.get_by_role("listbox").last
        await listbox.wait_for(state="visible", timeout=4000)

        passes, total_changed, max_passes = 0, 0, 8
        try: await listbox.evaluate("el => { el.scrollTop = 0; }")
        except: pass

        while passes < max_passes:
            passes += 1
            changed = await listbox.evaluate("""
                (root) => {
                    let changed = 0;
                    const items = root.querySelectorAll('.ms-Checkbox.ms-Dropdown-item');
                    items.forEach(host => {
                        const input = host.querySelector('input[type="checkbox"]');
                        if (input && !input.checked) {
                            const lbl = host.querySelector('label');
                            if (lbl) lbl.click(); else host.click();
                            changed++;
                        }
                    });
                    return changed;
                }
            """) or 0
            total_changed += changed

            remaining = 0
            try:
                remaining = await listbox.locator('input[type="checkbox"]:not(:checked)').count()
            except: remaining = 0
            if remaining == 0: break

            try: await listbox.evaluate("el => el.scrollBy(0, el.clientHeight)")
            except:
                try: await listbox.focus(); await self.page.keyboard.press("PageDown")
                except: pass
            await self.page.wait_for_timeout(40)

        logger.info(f"[SelectFields] Passes={passes}, toggled={total_changed}")
        try: await self.page.keyboard.press("Escape")
        except:
            try: await combo.click()
            except: pass

    async def _enable_large_exports_checkbox(self):
        dlg = self.page.locator(".ms-Dialog-main").last
        await dlg.wait_for(state="visible", timeout=5000)
        name_rx = re.compile(r"enable\s+large\s+exports", re.I)
        target = dlg.get_by_label(name_rx).first
        if await target.count() == 0:
            target = self.page.get_by_label(name_rx).first
        if await target.count() == 0:
            logger.warning("[EnableLargeExports] Checkbox not found by label"); return
        try:
            if await target.is_checked():
                logger.info("[EnableLargeExports] Already checked"); return
        except: pass
        await target.set_checked(True, force=True)
        try:
            if await target.is_checked():
                logger.info("[EnableLargeExports] Checked via label.set_checked(force=True)")
        except: pass

    async def _click_export_and_download(self, data_dir: str, file_base: str):
        os.makedirs(data_dir, exist_ok=True)
        dlg = self.page.locator(".ms-Dialog-main").last
        await dlg.wait_for(state="visible", timeout=10000)
        export_btn = dlg.get_by_role("button", name=re.compile(r"^export$", re.I)).first
        await export_btn.wait_for(state="visible", timeout=10000)
        for _ in range(40):
            try:
                if await export_btn.is_enabled(): break
            except: pass
            await self.page.wait_for_timeout(500)
        else:
            raise RuntimeError("Export button did not become enabled in time.")

        async with self.page.expect_download() as dl_info:
            await export_btn.click()
        dl: Download = await dl_info.value

        suggested = dl.suggested_filename or "export.xlsx"
        _, ext = os.path.splitext(suggested)
        ext = ext if ext.lower() in (".xlsx", ".csv") else ".xlsx"

        safe_base = sanitize_filename(file_base) or "visual"
        out_path = os.path.join(data_dir, f"{safe_base}{ext}")
        try:
            if os.path.exists(out_path): os.remove(out_path)
        except: pass

        await dl.save_as(out_path)
        logger.info(f"Export saved → {out_path}")
        try: await self._dismiss_export_toast(6000)
        except: pass
        await self.page.wait_for_timeout(150)
        return out_path

    async def _dismiss_export_toast(self, timeout_ms: int = 6000):
        toast_container = self.page.locator(".ms-MessageBar")
        try:
            await toast_container.first.wait_for(state="visible", timeout=timeout_ms)
        except Exception:
            return
        try:
            count = await toast_container.count()
        except Exception:
            count = 1
        for i in range(count):
            toast = toast_container.nth(i)
            try:
                close_btn = toast.locator("button.ms-MessageBar-dismissal, button[title='Close'], button[aria-label='Close']").first
                if await close_btn.count() > 0:
                    try: await close_btn.click()
                    except:
                        try: await close_btn.click(force=True)
                        except: pass
                else:
                    try: await self.page.keyboard.press("Escape")
                    except: pass
                try: await toast.wait_for(state="detached", timeout=1500)
                except:
                    try:
                        await self.page.evaluate("() => { document.querySelectorAll('.ms-MessageBar').forEach(n => n.remove()); }")
                    except: pass
            except:
                try:
                    await self.page.evaluate("() => { document.querySelectorAll('.ms-MessageBar').forEach(n => n.remove()); }")
                except: pass
        await self.page.wait_for_timeout(100)

    async def _export_via_menubar(self, safe_page_name: str, visual_title: str, data_dir: str, file_base: str, visual_index: int):
        await self._click_commandbar_export()
        dlg = self.page.locator(".ms-Dialog-main").last
        await dlg.wait_for(state="visible", timeout=10000)

        await self._select_combobox(r"^export with", r"^current view$")
        await self._select_export_type_excel_by_id()

        if (visual_title or "").strip():
            await self._select_combobox(r"^select visual", re.escape(visual_title))
        else:
            combo = self.page.get_by_role("combobox", name=re.compile(r"^select\s+visual", re.I)).first
            await combo.click(); await self.page.wait_for_timeout(50)
            listbox = self.page.get_by_role("listbox").last
            await listbox.wait_for(state="visible", timeout=4000)
            options = listbox.get_by_role("option")
            count = await options.count()
            if count > 0:
                idx = visual_index if visual_index < count else count - 1
                await options.nth(idx).click()

        await self._enable_large_exports_checkbox()
        await self._select_all_fields_in_dialog()
        await self._click_export_and_download(data_dir, file_base)
        await self.page.wait_for_timeout(300)

    async def _export_visuals_on_current_page(self, safe_page_name: str, allowed_visuals: Set[str]):
        allowed_visuals = allowed_visuals or set()
        if not allowed_visuals:
            logger.info("No allowed visuals configured; skipping.")
            return

        selectors = [
            ".visualContainer[role='group']",
            ".visual-container",
            "[class*='visual'][role='group']",
            "[class*='visual-container']",
        ]
        containers, all_containers = [], []
        for selector in selectors:
            try:
                await self.context.wait_for_selector(selector, timeout=5000)
                all_containers = await self.context.query_selector_all(selector)
                for c in all_containers:
                    try:
                        if await c.is_visible():
                            containers.append(c)
                    except: continue
                if containers:
                    logger.info(f"Found visuals using selector: {selector}")
                    break
            except Exception as e:
                continue

        logger.info(f"Visible containers: {len(containers)}")
        if not containers:
            logger.warning("No visible visuals found on this page.")
            return

        page_dir = os.path.join(self.download_dir, safe_page_name)
        visuals_dir = os.path.join(page_dir, "visuals")
        data_dir    = os.path.join(page_dir, "data")
        os.makedirs(visuals_dir, exist_ok=True)
        os.makedirs(data_dir, exist_ok=True)

        processed_any = False
        for i, container in enumerate(containers):
            try:
                human_title = await self._get_visual_title_for_matching(container)
                export_all = "__ALL__" in allowed_visuals or "all" in allowed_visuals
                if not export_all and norm(human_title) not in allowed_visuals:
                    continue

                processed_any = True
                visual_name = await self._get_visual_name_for_files(container, i)

                wrapper = await container.query_selector("[data-testid='visual-style'].visualWrapper, .visualWrapper")
                target_for_shot = wrapper or container

                img_path = os.path.join(visuals_dir, f"{visual_name}.png")
                await target_for_shot.screenshot(path=img_path)
                logger.info(f"[{self.config_report_name} | {safe_page_name} | {human_title}] Screenshot → {img_path}")

                try:
                    await self._export_via_menubar(safe_page_name, human_title, data_dir, visual_name, i)
                except Exception as e:
                    logger.warning(f"[{self.config_report_name} | {safe_page_name} | {human_title}] Menubar export failed: {e}")

                await self.page.wait_for_timeout(150)
            except Exception as e:
                logger.error(f"Failed to process a visual: {e}")
                continue

        if not processed_any:
            logger.info("No visuals matched the whitelist on this page.")