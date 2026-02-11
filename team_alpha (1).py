# -*- coding: utf-8 -*-
"""
IWIT-APP — Faster MFA,  snappier login & warehouse selection.

Changes in this version:
- Robust login waits + iframe handling (credentials now reliably typed).
- Always-set Chrome download prefs (PDFs download also in UI mode).
- Adds --remote-allow-origins=* (Chrome/Selenium compatibility).
- Safer extension-error detection to prevent false positives & reload loops.
- Small timeout cushions across login/MFA & HOME verification.
"""
import os
import io
import re
import time
import threading
import sys
import platform
import subprocess
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
import traceback
from typing import Optional, List, Dict

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.common.keys import Keys

# webdriver_manager
from webdriver_manager.chrome import ChromeDriverManager




# === OB2 OUTBOUND scrape helpers (added by Copilot) ===
from selenium.webdriver.common.by import By as _OB2_By
from selenium.webdriver.support.ui import WebDriverWait as _OB2_Wait
from selenium.webdriver.support import expected_conditions as _OB2_EC
from selenium.common.exceptions import TimeoutException as _OB2_Timeout, NoSuchElementException as _OB2_NoSuch
from selenium.webdriver.common.keys import Keys as _OB2_Keys
import os as _ob2_os
import re as _ob2_re
try:
    import sqlite3 as _ob2_sqlite3
except Exception:
    _ob2_sqlite3 = None

# Dedicated DB for OUTBOUND scraping
# === Single-table configuration (injected clean) ===
try:
    STN_FINAL_DB_PATH = os.getenv('STN_FINAL_DB_PATH') or 'stn_final.db'
except Exception:
    STN_FINAL_DB_PATH = 'stn_final.db'
STN_FINAL_TABLE = 'stn_final'
_OB2_DB_PATH = STN_FINAL_DB_PATH

# URL (fallback)

try:
    _OB2_OUTBOUND_TRACK_URL = OUTBOUND_TRACK_URL
except Exception:
    _OB2_OUTBOUND_TRACK_URL = 'http://10.24.1.53/outbound_requests/track_request'

# Selectors
_OB2_OUTBOUND_ID_ID_PRIMARY = 'filters_outbound_request_id'
_OB2_OUTBOUND_ID_NAME_FALLBACK = 'filters[outbound_request_id]'
_OB2_TRACK_REQUEST_BUTTON_NAME = 'searchbtn'
_OB2_TABLE_TOGGLE_DIV_CSS = 'div.table-toggle.collapsed'
_OB2_TABLE_TOGGLE_ANY_CSS = 'div.table-toggle'
_OB2_TRANSFER_LIST_LINK_XPATH = "//a[starts-with(normalize-space(text()), 'TL')]"

# Timeouts
_OB2_TIMEOUT_SHORT = 10
_OB2_TIMEOUT_MED = 20
_OB2_TIMEOUT_LONG = 35

# Helper waits/clicks to make the flow smoother and sequential

def _ob2_wait_ready(driver, timeout=30):
    try:
        _OB2_Wait(driver, timeout).until(lambda d: d.execute_script('return document.readyState') == 'complete')
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass

def _ob2_js_click(driver, el):
    try:
        driver.execute_script('arguments[0].scrollIntoView({block:"center"});', el)
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass
    try:
        el.click(); return True
    except Exception:
        try:
            driver.execute_script('arguments[0].click();', el)
            return True
        except Exception:
            return False

# ---------- DB helpers ----------

def _ob2_ensure_table(db_path: str):
    # Single-table mode: no-op
    return

def _ob2_insert_rows(db_path: str, rows):
    # Single-table mode: no-op
    return

def _ob2__try_find_stn_input_in_context(driver):
    locators = [
        (_OB2_By.ID, _OB2_OUTBOUND_ID_ID_PRIMARY),
        (_OB2_By.NAME, _OB2_OUTBOUND_ID_NAME_FALLBACK),
        (_OB2_By.CSS_SELECTOR, f"input#{_OB2_OUTBOUND_ID_ID_PRIMARY}"),
        (_OB2_By.XPATH, f"//input[@id='{_OB2_OUTBOUND_ID_ID_PRIMARY}']"),
        (_OB2_By.XPATH, "//input[@name='filters[outbound_request_id]']"),
    ]
    for by, sel in locators:
        try:
            el = _OB2_Wait(driver, 2).until(_OB2_EC.presence_of_element_located((by, sel)))
            if el:
                return el
        except Exception:
            continue
    return None

def _ob2_find_stn_input(driver):
    try:
        driver.switch_to.default_content()
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass
    el = _ob2__try_find_stn_input_in_context(driver)
    if el:
        return el
    frames = driver.find_elements(_OB2_By.TAG_NAME, 'iframe')
    for fr in frames:
        try:
            driver.switch_to.default_content(); driver.switch_to.frame(fr)
            el = _ob2__try_find_stn_input_in_context(driver)
            if el:
                return el
        except Exception:
            continue
    try:
        driver.switch_to.default_content()
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass
    return None

def _ob2_find_track_button(driver):
    try:
        return _OB2_Wait(driver, 2).until(_OB2_EC.element_to_be_clickable((_OB2_By.NAME, _OB2_TRACK_REQUEST_BUTTON_NAME)))
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass
    alts = [
        (_OB2_By.XPATH, "//input[@type='submit' and @name='searchbtn']"),
        (_OB2_By.XPATH, "//input[@type='submit' and contains(@value,'Track Request')]")
    ]
    for by, sel in alts:
        try:
            return _OB2_Wait(driver, 2).until(_OB2_EC.element_to_be_clickable((by, sel)))
        except Exception:
            continue
    frames = driver.find_elements(_OB2_By.TAG_NAME, 'iframe')
    for fr in frames:
        try:
            driver.switch_to.default_content(); driver.switch_to.frame(fr)
            for by, sel in [(_OB2_By.NAME, _OB2_TRACK_REQUEST_BUTTON_NAME), (_OB2_By.XPATH, "//input[@type='submit' and contains(@value,'Track Request')]")]:
                try:
                    return _OB2_Wait(driver, 2).until(_OB2_EC.element_to_be_clickable((by, sel)))
                except Exception:
                    continue
        except Exception:
            continue
    try:
        driver.switch_to.default_content()
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass
    return None

# ---------- TL page scrapers ----------
_OB2_wid_re = _ob2_re.compile(r"\bWID:\s*([\w\-]+)", _ob2_re.I)
_OB2_fsn_re = _ob2_re.compile(r"\bFSN:\s*([\w\-]+)", _ob2_re.I)
_OB2_title_re = _ob2_re.compile(r"\bTitle:\s*(.*)")
_OB2_category_re = _ob2_re.compile(r"\bCategory:\s*([^\n\r]+)")
_OB2_shelf_re = _ob2_re.compile(r"\bShelf:\s*([^\n\r]+)")
_OB2_only_digits = _ob2_re.compile(r"\d+")


def _ob2_extract_item_rows(driver):
    rows = []
    try:
        table = driver.find_element(
            _OB2_By.XPATH,
            "//table[.//th[contains(normalize-space(.), 'WID/FSN')]][.//th[normalize-space()='Item Description']]",
        )
    except _OB2_NoSuch:
        try:
            table = driver.find_element(
                _OB2_By.XPATH,
                "(//h1|//h2|//div)[contains(normalize-space(.), 'Transfer List Item Details')]/following::table[1]"
            )
        except Exception:
            return rows
    data_rows = table.find_elements(_OB2_By.XPATH, ".//tr[td]")
    for tr in data_rows:
        tds = tr.find_elements(_OB2_By.XPATH, ".//td")
        if len(tds) < 5:
            continue
        col1 = (tds[0].text or '').strip()
        col2 = (tds[1].text or '').strip()
        col3 = (tds[2].text or '').strip()
        col5 = (tds[4].text or '').strip()
        wid = _OB2_wid_re.search(col1)
        fsn = _OB2_fsn_re.search(col1)
        title = _OB2_title_re.search(col2)
        category = _OB2_category_re.search(col2)
        shelf = _OB2_shelf_re.search(col5)
        qty_match = _OB2_only_digits.search(col3)
        qty_val = int(qty_match.group(0)) if qty_match else 0
        rows.append((
            wid.group(1) if wid else '',
            fsn.group(1) if fsn else '',
            (title.group(1).strip() if title else ''),
            (category.group(1).strip() if category else ''),
            qty_val,
            (shelf.group(1).strip() if shelf else ''),
        ))
    return rows


def _ob2_track_request_for_stn(driver, stn: str) -> bool:
    """On OUTBOUND track page: paste STN, click Track, click '+' (collapsed toggle), then wait for TL links.
    Robust: ensures input value is set, uses JS click/scroll if needed, retries toggle open, waits for TL links.
    """
    _ob2_wait_ready(driver, 20)
    try:
        driver.switch_to.default_content()
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass

    inp = _ob2_find_stn_input(driver)
    if not inp:
        return False

    try:
        inp.click()
        try:
            inp.clear()
        except Exception:
            pass
        inp.send_keys(_OB2_Keys.CONTROL, 'a')
        inp.send_keys(_OB2_Keys.DELETE)
        inp.send_keys(stn)
        try:
            driver.execute_script("arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('change', {bubbles:true}));", inp, stn)
        except Exception:
            pass
    except Exception:
        return False

    btn = _ob2_find_track_button(driver)
    if not btn:
        return False
    if not _ob2_js_click(driver, btn):
        return False

    try:
        _OB2_Wait(driver, 15).until(_OB2_EC.presence_of_element_located((_OB2_By.CSS_SELECTOR, _OB2_TABLE_TOGGLE_ANY_CSS)))
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass

    opened = False
    for _ in range(3):
        try:
            toggles = driver.find_elements(_OB2_By.CSS_SELECTOR, _OB2_TABLE_TOGGLE_DIV_CSS)
            if not toggles:
                toggles = driver.find_elements(_OB2_By.CSS_SELECTOR, _OB2_TABLE_TOGGLE_ANY_CSS)
            if toggles:
                t = toggles[0]
                _ob2_js_click(driver, t)
                try:
                    _OB2_Wait(driver, 12).until(_OB2_EC.presence_of_element_located((_OB2_By.XPATH, _OB2_TRANSFER_LIST_LINK_XPATH)))
                    opened = True
                    break
                except Exception:
                    pass
        except Exception:
            pass
    if not opened:
        try:
            _OB2_Wait(driver, 8).until(_OB2_EC.presence_of_element_located((_OB2_By.XPATH, _OB2_TRANSFER_LIST_LINK_XPATH)))
            opened = True
        except Exception:
            return False

    return True


def _ob2_collect_tl_links(driver):
    links = driver.find_elements(_OB2_By.XPATH, _OB2_TRANSFER_LIST_LINK_XPATH)
    out = []
    for a in links:
        try:
            tl_text = (a.text or a.get_attribute('innerText') or '').strip()
            href = a.get_attribute('href') or a.get_attribute('data-href')
            if tl_text and href:
                out.append((tl_text, href))
        except Exception:
            continue
    return out


def _ob2_process_single_stn(driver, stn: str, db_path: str):
    """Open OUTBOUND_TRACK_URL, search for STN, expand (+), then open each TL and store items sequentially."""
    try:
        driver.get(_OB2_OUTBOUND_TRACK_URL)
    except Exception:
        return
    _ob2_wait_ready(driver, 25)

    if not _ob2_track_request_for_stn(driver, stn):
        return

    links = _ob2_collect_tl_links(driver)
    for tl_text, tl_href in links:
        tl_id = (tl_text or '').strip()
        try:
            driver.get(tl_href)
            _ob2_wait_ready(driver, 25)
        except Exception:
            continue
        item_rows = _ob2_extract_item_rows(driver)
        payload = [(tl_id, wid, fsn, title, category, qty, shelf) for (wid, fsn, title, category, qty, shelf) in item_rows]
        _ob2_insert_rows(db_path, payload)
        try:
            driver.back()
            _ob2_wait_ready(driver, 15)
            _OB2_Wait(driver, 8).until(_OB2_EC.presence_of_element_located((_OB2_By.XPATH, _OB2_TRANSFER_LIST_LINK_XPATH)))
        except Exception:
            _ob2_track_request_for_stn(driver, stn)


def _ob2_scrape_using_existing_driver(driver, csv_path: str, db_path: str, status_cb=None):
    """Reads STNs from csv_path, navigates in the SAME TAB to OUTBOUND track URL, and stores TL items into SQLite sequentially."""
    if status_cb:
        try: status_cb('Starting OUTBOUND scrape/store...')
        except Exception: pass
    _ob2_ensure_table(db_path)

    stns = []
    try:
        import csv as _ob2_csv
        with open(csv_path, 'r', newline='', encoding='utf-8-sig') as f:
            r = _ob2_csv.reader(f)
            for row in r:
                if not row: continue
                v = (row[0] or '').replace('\ufeff','').strip()
                if not v or v.lower()=='stn':
                    continue
                if v not in stns:
                    stns.append(v)
    except Exception as e:
        if status_cb:
            try: status_cb(f'CSV read error: {e}', is_error=True)
            except Exception: pass
        return

    total = len(stns)
    for i, stn in enumerate(stns, start=1):
        if status_cb:
            try: status_cb(f'[{i}/{total}] OUTBOUND scrape: {stn}...')
            except Exception: pass
        try:
            _ob2_process_single_stn(driver, stn, db_path)
        except Exception as e:
            if status_cb:
                try: status_cb(f'Error on {stn}: {e}', is_error=True)
                except Exception: pass
        try:
            import time as _t; _t.sleep(0.6)
        except Exception:
            pass

    if status_cb:
        try: status_cb(f'OUTBOUND scrape/store completed. STNs processed: {total}')
        except Exception: pass

# ============================ FAST HELPERS (non-breaking) =====================

def _set_value_fast(driver, el, val: str):
    """Set input/select value via native JS setter + dispatch input/change events.
    Drop-in speed boost without changing external logic.
    """
    try:
        driver.execute_script(
            r"""
            const el = arguments[0];
            const val = arguments[1];
            const tag = (el && el.tagName ? el.tagName.toLowerCase() : '');
            function setInput(e, v) {
                const proto = Object.getPrototypeOf(e);
                const desc = Object.getOwnPropertyDescriptor(proto, 'value');
                if (desc && desc.set) { desc.set.call(e, v); } else { e.value = v; }
                e.dispatchEvent(new Event('input', { bubbles: true }));
                e.dispatchEvent(new Event('change', { bubbles: true }));
            }
            if (tag === 'input' || tag === 'textarea') {
                setInput(el, val);
            } else if (tag === 'select') {
                el.value = val;
                el.dispatchEvent(new Event('input', { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
            } else {
                // best-effort for custom widgets
                if ('value' in el) { el.value = val; }
                el.dispatchEvent(new Event('input', { bubbles: true }));
                el.dispatchEvent(new Event('change', { bubbles: true }));
            }
            return true;
            """,
            el, val
        )
        return True
    except Exception:
        try:
            el.clear()
        except Exception:
            pass
        try:
            el.send_keys(val)
            return True
        except Exception:
            return False

def _select_by_visible_text_fast(driver, select_el, visible_text: str):
    """Fast path: choose option by visible text via JS; fallback to Selenium Select.
    Keeps the same observable behavior (fires change events).
    """
    try:
        tag = (select_el.tag_name or '').lower()
        if tag != 'select':
            return False
        found = driver.execute_script(
            r"""
            const sel = arguments[0];
            const txt = (arguments[1] || '').trim().toLowerCase();
            let idx = -1;
            for (let i = 0; i < sel.options.length; i++) {
                if ((sel.options[i].textContent || '').trim().toLowerCase() === txt) { idx = i; break; }
            }
            if (idx >= 0) {
                sel.selectedIndex = idx;
                sel.dispatchEvent(new Event('input', { bubbles: true }));
                sel.dispatchEvent(new Event('change', { bubbles: true }));
                return true;
            }
            return false;
            """,
            select_el, visible_text
        )
        return bool(found)
    except Exception:
        return False

# Tkinter
import tkinter as tk

# === NEW: Strict 'Others' click helper (avoid clicking 'Returns') ===
def _click_others_title_exact(driver, status_cb=None):
    def status(m):
        try:
            status_cb and status_cb(m)
        except Exception:
            pass
    js = r"""
    (function(){
      function norm(s){return (s||'').replace(/\s+/g,' ').trim();}
      var cand = null;
      var nodes = document.querySelectorAll('.react-sanfona-item-title[id^="react-safona-item-title-"]');
      for (var i=0;i<nodes.length;i++){
        var t = norm(nodes[i].innerText||nodes[i].textContent||'');
        if (t === 'Others') { cand = nodes[i]; break; }
      }
      if(!cand){
        nodes = document.querySelectorAll('.react-sanfona-item-title, [id^="react-safona-item-title-"]');
        for (var j=0;j<nodes.length;j++){
          var tt = norm(nodes[j].innerText||nodes[j].textContent||'');
          if (tt === 'Others') { cand = nodes[j]; break; }
        }
      }
      if(!cand) return false;
      var txt = norm(cand.innerText||cand.textContent||'');
      if (txt && txt.toLowerCase().indexOf('returns') !== -1) return false;
      try { cand.scrollIntoView({block:'center'}); } catch(e){}
      try { cand.click && cand.click(); } catch(e){}
      var bodyId = cand.getAttribute('aria-controls');
      if(bodyId){
        var body = document.getElementById(bodyId);
        if(body){
          var cs = window.getComputedStyle(body);
          if(cs && (cs.maxHeight==='0px' || cs.display==='none' || cs.overflow==='hidden')){
            try{ body.style.maxHeight='1000px'; }catch(e){}
            try{ body.style.display='block'; }catch(e){}
            try{ body.style.overflow='visible'; }catch(e){}
          }
        }
      }
      return true;
    })();
    """
    try:
        if bool(driver.execute_script(js)):
            status("Clicked exact 'Others' title.")
            return True
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass
    return False
# === END NEW: Strict 'Others' click helper ===

from tkinter import ttk  # Combobox

# === Auto-complete Combobox for warehouse search (type-to-filter) ===
try:
    import tkinter as _ac_tk
    from tkinter import ttk as _ac_ttk
except Exception:
    _ac_tk = None; _ac_ttk = None

class AutoCompleteCombobox(_ac_ttk.Combobox):
    """A Combobox that lets users type to filter values for WH search.
    **Only short warehouse codes are shown** in the dropdown.
    Typing still matches both short code and full name.
    """
    def __init__(self, master=None, **kwargs):
        kwargs.setdefault('state', 'normal')  # allow typing
        super().__init__(master, **kwargs)
        self._all_keys = []
        self._display = []
        self._map = {}
        # bindings
        try:
            self.bind('<KeyRelease>', self._on_keyrelease, add='+')
            self.bind('<<ComboboxSelected>>', self._on_selected, add='+')
        except Exception:
            pass

    def set_choices(self, keys, key_to_fullname: dict):
        # Store mapping but DISPLAY ONLY SHORT CODES in the dropdown
        self._all_keys = list(keys or [])
        self._map = dict(key_to_fullname or {})
        # Only short codes should be visible
        self._display = list(self._all_keys)
        try:
            self.configure(values=self._display)
        except Exception:
            pass

    def _filtered(self, typed: str):
        t = (typed or '').strip().lower()
        if not t:
            return self._all_keys
        out = []
        for k in self._all_keys:
            full = (self._map.get(k, '') or '')
            if t in k.lower() or t in full.lower():
                out.append(k)
        return out

    def _on_keyrelease(self, _evt=None):
        try:
            typed = self.get()
            vals = self._filtered(typed)
            self.configure(values=vals)
            # open dropdown if there are suggestions
            if vals:
                try: self.event_generate('<Down>')
                except Exception: pass
        except Exception:
            pass

    def _on_selected(self, _evt=None):
        # Ensure selected value remains the short code
        try:
            sel = self.get()
            code = (sel or '').strip()
            if code:
                self.set(code)
        except Exception:
            pass

    def current(self, newindex=None):
        # On programmatic selection, keep entry text as short code only.
        try:
            res = super().current(newindex) if newindex is not None else super().current()
            if newindex is not None:
                try:
                    sel = self.get()
                    code = (sel or '').strip()
                    if code:
                        self.set(code)
                except Exception:
                    pass
            return res
        except Exception:
            try:
                return super().current(newindex) if newindex is not None else super().current()
            except Exception:
                return None

def make_wh_autocomplete(parent, textvariable=None, width=40):
    """Factory: create AutoCompleteCombobox preloaded with warehouses."""
    cb = AutoCompleteCombobox(parent, textvariable=textvariable, width=width)
    try:
        cb.set_choices(_WAREHOUSE_SHORTS, _WH_MAP)
    except Exception:
        # fallback to plain list if mapping missing
        cb.configure(values=list(_WAREHOUSE_SHORTS))
    return cb
# === End auto-complete ===

# ============================ PDF libs (robust import) =========================
try:
    from PyPDF2 import PdfReader, PdfWriter
except ImportError:
    from pypdf import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.lib.colors import black, white
from reportlab.graphics.barcode import qr
from reportlab.graphics.shapes import Drawing
from reportlab.graphics import renderPDF

# ============================ Configuration & URLs =============================
HOME_URL = "http://10.24.1.53/home"
LOGIN_URL_10X = "http://10.24.1.53/"
FIND_REQUEST_URL = "http://10.24.1.53/outbound_requests/find_request"
BOX_CREATION_URL = "http://10.24.1.53/storage_locations/create_new_boxes"
OUTBOUND_TRACK_URL = "http://10.24.1.53/outbound_requests/track_request"
CONSIGNMENT_BOX_URL = "http://10.24.1.53/consignment/add_consignment_boxes"
PACK_BOX_DIV_ID = "pack_box_div"

# Headless mode:
# "0" = UI mode (default)
# "1" = headless basic
# "2" = headless advanced (perf tweaks)
HEADLESS_ENV = os.getenv("IWIT_HEADLESS", "2")
DOWNLOAD_FOLDER = Path(os.getenv("IWIT_DOWNLOAD_DIR", str(Path.home() / "Downloads")))
LOCAL_CHROMEDRIVER_PATH = os.getenv("CHROMEDRIVER_PATH")
PDF_DOWNLOAD_TIMEOUT_SEC = 120  # shorter timeout than before

# Button colors
PRINT_DEFAULT_BG = "#007BFF"
PACK_DEFAULT_BG = "#00BFFF"
ERROR_BG = "#F44336"

# Progress circle colors
PROGRESS_GREEN = "#22C55E"
PROGRESS_BG_GRAY = "#E5E7EB"

# TOTE PACKING SELECTORS
WEIGHT_INPUT_ID = "weight_hidden"
BOX_ID_INPUT_ID = "tote_id"
PACK_BOX_BUTTON_ID = "pack_box"
PACKING_SLIP_BUTTON_ID = "print_label"
PRINT_QUANTITY_INPUT_ID = "print_quantity"
SECURITY_SLIP_BUTTON_ID = "print_security_label"
PACK_BOX_MSG_ID = "pack_box_msg"
PACK_BOX_FAIL_TEXT = "Failed to pack box"

OUTBOUND_ID_INPUT_ID = "filters[outbound_request_id]"
OUTBOUND_ID_LABEL_TEXT = "Outbound Request ID"
TRACK_REQUEST_BUTTON_NAME = "searchbtn"

TABLE_TOGGLE_DIV_CSS = "div.table-toggle.collapsed"
TABLE_TOGGLE_ANY_CSS = "div.table-toggle"
TRANSFER_LIST_LINK_XPATH = "//a[starts-with(normalize-space(text()), 'TL')]"

# ============================ Embedded Warehouse Map (Short -> Full) ==========
WH_MAP_SHORT_TO_FULL: Dict[str, str] = {
 "del_frn_07L": "Farukhnagar 07 Large Warehouse"    
}
WH_SHORT_CODES: List[str] = sorted(list(WH_MAP_SHORT_TO_FULL.keys()))

# ============================ Shared State ====================================

# GO PICK runtime state (added)
GO_PICK_STATE = {"tl": None, "shelf": None, "box": None}
GO_PICK_NEXT_SHELF_CALLBACK = None
GUI_ROOT = None
GUI_ACTIVE_DRIVER = None
GUI_MSG_TOTE = None
SELECTED_WH_SHORT: Optional[str] = None
SELECTED_WH_FULL: Optional[str] = None

# Track the temp Chrome profile dir for cleanup on logout
CHROME_PROFILE_DIR_TEMP: Optional[str] = None

# ============================ Wait / Perf Profiles ============================
# Poll frequency shorter in advanced mode
def build_wait(driver, base_timeout: int):
    poll = 0.20 if HEADLESS_ENV == "2" else 0.35
    return WebDriverWait(driver, base_timeout, poll_frequency=poll)

# Default timeouts (slightly cushioned)
TIMEOUT_SHORT = 7 if HEADLESS_ENV == "2" else 9
TIMEOUT_MED   = 10 if HEADLESS_ENV == "2" else 12
TIMEOUT_LONG  = 20 if HEADLESS_ENV == "2" else 26

# ============================ GUI Helpers =====================================
def load_image_logo():
    try:
        img_path = Path(__file__).resolve().parent / "Flipkart_Logo.png"
        if not img_path.exists():
            img_path = Path(__file__).resolve().parent / "Flipkart_Logo.gif"
        if img_path.exists():
            return tk.PhotoImage(file=str(img_path))
        else:
            return None
    except Exception:
        return None

def draw_header(root, title):
    header_frame = tk.Frame(root, bg="#007BFF", height=40)
    header_frame.pack(fill="x")
    logo = getattr(root, '_flipkart_logo_img', None)
    if logo:
        tk.Label(header_frame, image=logo, bg="#007BFF").pack(side="left", padx=15, pady=5)
    else:
        tk.Label(header_frame, text="Flipkart", fg="white", bg="#007BFF",
                 font=("Arial", 16, "bold italic")).pack(side="left", padx=15, pady=5)
    tk.Label(header_frame, text=title, fg="white", bg="#007BFF",
             font=("Arial", 10, "normal")).pack(side="right", padx=15, pady=5)
    return header_frame

def log_info(title: str, message: str):
    print(f"[{title}] {message}")

def log_warn(title: str, message: str):
    print(f"⚠️ [Warning: {title}] {message}")

def log_error(title: str, message: str, step=None, flow="BOX_CREATION"):
    print(f"❌ [Error: {title}] {message}")

def gui_append_msg(text: str, flow="BOX_CREATION"):
    try:
        if GUI_ROOT is None:
            print(text)
            return

        def _append():
            target = GUI_MSG_TOTE if flow == "TOTE_PACKING" else None
            if target:
                ts = datetime.now().strftime("%H:%M:%S")
                target.insert("end", f"[{ts}] {text}\n")
                target.see("end")

        GUI_ROOT.after(0, _append)
    except Exception as e:
        print(f"⚠️ GUI message append failed: {e}. Fallback console: {text}")

def gui_clear_msgs(flow="BOX_CREATION"):
    try:
        if GUI_ROOT is None:
            return

        def _clear():
            target = GUI_MSG_TOTE if flow == "TOTE_PACKING" else None
            if target:
                target.delete("1.0", "end")

        GUI_ROOT.after(0, _clear)
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass

# ============================ Circular Progress Indicator =====================
class CircleProgressIndicator:
    def __init__(self, parent, size_px: int = 42, ring_width: int = 6,
                 color_fg: str = PROGRESS_GREEN, color_bg: str = PROGRESS_BG_GRAY,
                 color_text: str = "#111"):
        self.parent = parent
        self.container = tk.Frame(parent)
        self.row = tk.Frame(self.container)
        self.status_lbl = tk.Label(self.row, text="", font=("Arial", 9), fg="#444")
        self.status_lbl.pack(side="left", padx=(0, 8))
        self.canvas = tk.Canvas(self.row, width=size_px, height=size_px, bg="white", highlightthickness=0)
        self.canvas.pack(side="left")
        self.row.pack(anchor="w")

        self.size = size_px
        self.ring_w = ring_width
        self.color_fg = color_fg
        self.color_bg = color_bg
        self.color_text = color_text
        self._pack_kwargs = {"side": "left", "padx": 12}

    def attach_next_to_button(self, btn_widget, padx=12):
        self._pack_kwargs = {"side": "left", "padx": padx}

    def show(self):
        self.container.pack(**self._pack_kwargs)

    def hide(self):
        try:
            self.container.pack_forget()
        except Exception:
            pass

    def _draw(self, pct: float):
        c = self.canvas
        c.delete("all")
        size = self.size
        m = self.ring_w // 2 + 3

        c.create_oval(m, m, size - m, size - m, outline=self.color_bg, width=self.ring_w)

        extent = max(0.0, min(pct, 100.0)) * 3.6
        c.create_arc(m, m, size - m, size - m, start=-90, extent=extent, style="arc",
                     outline=self.color_fg, width=self.ring_w)

        c.create_text(size / 2, size / 2, text=f"{int(pct)}%", font=("Arial", 10, "bold"),
                      fill=self.color_text)

    def start(self, initial_status: str = "Starting…"):
        self.show()
        self._draw(0.0)
        self.set_status(initial_status)

    def set_percent(self, pct: float):
        self._draw(max(0.0, min(pct, 100.0)))

    def set_status(self, status_text: str):
        try:
            self.status_lbl.config(text=status_text)
        except Exception:
            pass

    def stop(self, success: bool, final_status: str = None):
        self._draw(100.0 if success else 0.0)
        self.set_status(final_status or ("Done" if success else "Failed"))
        self.container.after(800, self.hide)

    def reset(self):
        self.hide()
        try:
            self.status_lbl.config(text="")
        except Exception:
            pass

# ============================ Driver Helpers (extensions disabled) ============
def _build_chrome_options(profile_dir: str):
    options = webdriver.ChromeOptions()

    # --- Mixed/insecure content allowances (permanent) ---
    # Allow HTTPS pages to load HTTP frames/scripts (mixed content)
    if all(('--allow-running-insecure-content' not in a) for a in getattr(options, 'arguments', [])):
        options.add_argument('--allow-running-insecure-content')
    # Relax block on insecure private network requests from secure contexts (10.x/172.x/192.168.x)
    if all(('--disable-features=BlockInsecurePrivateNetworkRequests' not in a) for a in getattr(options, 'arguments', [])):
        options.add_argument('--disable-features=BlockInsecurePrivateNetworkRequests')

    # Headless/basic/advanced
    if HEADLESS_ENV in ("1", "2"):
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--window-size=1920,1080")
        options.page_load_strategy = "none"

    # Common perf/behavior flags
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-backgrounding-occluded-windows")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--no-default-browser-check")
    options.add_argument("--no-first-run")
    options.add_argument("--disable-extensions")
    options.add_argument("--force-device-scale-factor=1")
    options.add_argument("--remote-allow-origins=*")  # compatibility with recent Chrome/Selenium

    # Always-set download prefs (works in UI and headless)
    prefs = {
        "plugins.always_open_pdf_externally": True,
        "download.prompt_for_download": False,
        "download.default_directory": str(DOWNLOAD_FOLDER),
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False,
        "autofill.profile_enabled": False,
        "autofill.credit_card_enabled": False,
        "autofill.address_enabled": False,
        "profile.managed_default_content_settings.images": 2,
        "profile.default_content_setting_values.notifications": 2
    }
    # Ensure insecure/mixed content is allowed via profile settings
    try:
        prefs.setdefault('profile.default_content_setting_values', {})
        pdcsv = prefs['profile.default_content_setting_values']
        pdcsv['mixed_script'] = 1
        pdcsv['insecure_content'] = 1
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass
    options.add_experimental_option("prefs", prefs)

    # Fresh temp profile
    options.add_argument(f"user-data-dir={profile_dir}")

    # Reduce automation banners (best effort)
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    # Faster navigations: don't wait for full page load; rely on element waits (advanced only)
    if HEADLESS_ENV == "2":
        options.page_load_strategy = "none"

    return options

def launch_browser():
    global CHROME_PROFILE_DIR_TEMP
    try:
        DOWNLOAD_FOLDER.mkdir(parents=True, exist_ok=True)
        CHROME_PROFILE_DIR_TEMP = tempfile.mkdtemp(prefix="iwit_chrome_")
        options = _build_chrome_options(CHROME_PROFILE_DIR_TEMP)
        driver = None
        try:
            driver = webdriver.Chrome(options=options)
            print("🎉 Launched Chrome via Selenium Manager (fresh temp profile).")
        except Exception as sm_err:
            print(f"⚠️ Selenium Manager launch failed: {sm_err}")

        if driver is None:
            try:
                driver_path = ChromeDriverManager().install()
                driver = webdriver.Chrome(service=ChromeService(executable_path=driver_path), options=options)
                print("🎉 Launched Chrome via webdriver_manager (fresh temp profile).")
            except Exception as wdm_err:
                print(f"⚠️ webdriver_manager launch failed: {wdm_err}")

        if driver is None and LOCAL_CHROMEDRIVER_PATH and Path(LOCAL_CHROMEDRIVER_PATH).exists():
            driver = webdriver.Chrome(service=ChromeService(executable_path=LOCAL_CHROMEDRIVER_PATH), options=options)
            print("🎉 Launched Chrome via LOCAL_CHROMEDRIVER_PATH (fresh temp profile).")

        if driver is None:
            try:
                shutil.rmtree(CHROME_PROFILE_DIR_TEMP, ignore_errors=True)
            except Exception:
                pass
            raise WebDriverException("Unable to launch Chrome.")

        # Tighter timeouts
        driver.set_page_load_timeout(18 if HEADLESS_ENV == "2" else 25)
        driver.set_script_timeout(12 if HEADLESS_ENV == "2" else 20)
        return driver, build_wait(driver, TIMEOUT_MED)
    except WebDriverException as e:
        log_error("WebDriver Error", f"Failed to launch browser: {e}")
        return None, None

def cleanup_profile_dir():
    global CHROME_PROFILE_DIR_TEMP
    if CHROME_PROFILE_DIR_TEMP and os.path.isdir(CHROME_PROFILE_DIR_TEMP):
        try:
            shutil.rmtree(CHROME_PROFILE_DIR_TEMP, ignore_errors=True)
            print(f"🧹 Deleted temp Chrome profile: {CHROME_PROFILE_DIR_TEMP}")
        except Exception as e:
            print(f"⚠️ Could not delete temp profile dir: {e}")
    CHROME_PROFILE_DIR_TEMP = None

# ============================ Popup Handling ==================================
def _click_many(driver, locators, max_clicks_per_pass=10):
    clicked = 0
    for by, sel in locators:
        try:
            elems = driver.find_elements(by, sel)
        except Exception:
            elems = []
        for el in elems[:max_clicks_per_pass]:
            try:
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", el)
                driver.execute_script("arguments[0].click();", el)
                clicked += 1
            except Exception:
                continue
    return clicked

def handle_popups(driver, wait_duration=1, loops=1):
    total_clicked = 0
    # Native JS alerts
    try:
        for _ in range(wait_duration):
            try:
                WebDriverWait(driver, 0.5).until(EC.alert_is_present())
                alert = driver.switch_to.alert
                txt = alert.text
                alert.accept()
                print(f"☑️ JS alert accepted: {txt}")
                total_clicked += 1
            except TimeoutException:
                break
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass

    # DOM modals
    candidates_primary = [
        (By.XPATH, "//button[normalize-space()='OK']"),
        (By.XPATH, "//button[normalize-space()='Close']"),
        (By.CSS_SELECTOR, ".swal-button--confirm"),
        (By.CSS_SELECTOR, ".modal-footer .btn-primary"),
        (By.CSS_SELECTOR, "[aria-label='Close'], .close, .modal-header .btn-close"),
    ]
    for _ in range(loops):
        clicked = _click_many(driver, candidates_primary)
        total_clicked += clicked
        if clicked == 0:
            candidates_fallback = [
                (By.XPATH, "//button[contains(translate(., 'OK','ok'),'ok')]"),
                (By.XPATH, "//button[contains(translate(., 'CLOSE','close'),'close')]"),
            ]
            clicked += _click_many(driver, candidates_fallback)
            total_clicked += clicked
            if clicked == 0:
                break

    if total_clicked > 0:
        print(f"☑️ Popups auto-cleared: {total_clicked} click(s).")

class PopupWatcher:
    def __init__(self, driver, interval_sec=0.7 if HEADLESS_ENV == "2" else 0.9):
        self.driver = driver
        self.interval = interval_sec
        self._running = False
        self._thread = None

    def _loop(self):
        while self._running:
            try:
                handle_popups(self.driver, wait_duration=1, loops=1)
            except Exception:
                pass
            time.sleep(self.interval)

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        print("👀 Popup watcher started.")

    def stop(self):
        if self._running:
            self._running = False
            print("🛑 Popup watcher stopped.")

# ============================ Navigation Helpers + HOME retry =================
def navigate_to_url(driver, url: str, wait: WebDriverWait) -> Optional[str]:
    try:
        driver.get(url)
        driver.switch_to.window(driver.current_window_handle)
        return driver.current_window_handle
    except Exception as e:
        log_error("Tab Navigation", f"Failed to navigate to {url}: {e}")
        return None

def navigate_if_needed(driver, target_url: str, wait: WebDriverWait) -> bool:
    try:
        cur = driver.current_url or ""
    except Exception:
        cur = ""
    if target_url and target_url in cur:
        return True
    return navigate_to_url(driver, target_url, wait) is not None

def is_extension_error_page(driver) -> bool:
    """Safer detection to avoid false positives on auth pages."""
    try:
        cur_url = (driver.current_url or "").lower()
        body_text = (driver.find_element(By.TAG_NAME, "body").text or "").lower()
        if cur_url.startswith("chrome-error://") or cur_url.startswith("chrome-extension://"):
            return True
        return ("can't find extension" in body_text) or ("extension" in body_text and "can't be located" in body_text)
    except Exception:
        return False

def ensure_home_loaded(driver, wait, retries: int = 4, sleep_between: float = 0.7):
    for attempt in range(1, retries + 1):
        if not navigate_if_needed(driver, HOME_URL, wait):
            time.sleep(sleep_between)
            continue

        if is_extension_error_page(driver):
            print(f"⚠️ HOME shows extension error (attempt {attempt}/{retries}). Retrying…")
            try:
                driver.execute_script("location.reload()")
            except Exception:
                pass
            time.sleep(sleep_between)
            continue

        try:
            WebDriverWait(driver, TIMEOUT_SHORT).until(EC.presence_of_element_located((By.ID, "select-warehouse")))
            return True
        except TimeoutException:
            time.sleep(sleep_between)
            continue
    return False

# ============================ Auth / MFA ======================================
class _SimpleGetter:
    def __init__(self, value: str):
        self._val = value or ""
    def get(self) -> str:
        return self._val

def _switch_into_login_iframe_if_any(driver):
    """Try common iframes that host login inputs."""
    try:
        frames = driver.find_elements(By.TAG_NAME, "iframe")
        for idx, fr in enumerate(frames):
            try:
                driver.switch_to.default_content()
                driver.switch_to.frame(fr)
                # Check if inside we can see a login input
                if driver.find_elements(By.XPATH, "//input[@type='text' or @type='email']") or \
                   driver.find_elements(By.XPATH, "//input[@type='password']"):
                    return True
            except Exception:
                continue
        driver.switch_to.default_content()
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass
    return False

def _locate_login_fields(driver, timeout=TIMEOUT_MED):
    """Return (user_el, pass_el, submit_el, used_iframe)"""
    used_iframe = False

    def _find_all():
        user_candidates = [
            (By.ID, "username"),
            (By.NAME, "username"),
            (By.CSS_SELECTOR, "input#username"),
            (By.CSS_SELECTOR, "input[name='username']"),
            (By.XPATH, "//input[@type='text' or @type='email'][@id='username' or @name='username']"),
        ]
        pass_candidates = [
            (By.ID, "password"),
            (By.NAME, "password"),
            (By.CSS_SELECTOR, "input#password"),
            (By.CSS_SELECTOR, "input[name='password']"),
            (By.XPATH, "//input[@type='password'][@id='password' or @name='password']"),
        ]
        submit_candidates = [
            (By.ID, "loginSubmit"),
            (By.NAME, "submit"),
            (By.CSS_SELECTOR, "input[type='submit']"),
            (By.XPATH, "//button[normalize-space()='Submit']"),
            (By.XPATH, "//input[@type='submit' and (contains(@value,'Submit') or contains(@id,'login'))]"),
        ]

        user_el = pass_el = submit_el = None

        for by, sel in user_candidates:
            try:
                user_el = WebDriverWait(driver, timeout).until(EC.visibility_of_element_located((by, sel)))
                break
            except TimeoutException:
                continue

        for by, sel in pass_candidates:
            try:
                pass_el = WebDriverWait(driver, timeout).until(EC.visibility_of_element_located((by, sel)))
                break
            except TimeoutException:
                continue

        for by, sel in submit_candidates:
            try:
                submit_el = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((by, sel)))
                break
            except TimeoutException:
                continue

        return user_el, pass_el, submit_el

    # Try in default content first
    user_el, pass_el, submit_el = _find_all()
    if user_el and pass_el and submit_el:
        return user_el, pass_el, submit_el, used_iframe

    # Try switching into iframes
    if _switch_into_login_iframe_if_any(driver):
        used_iframe = True
        user_el, pass_el, submit_el = _find_all()
        if user_el and pass_el and submit_el:
            return user_el, pass_el, submit_el, used_iframe

    # Nothing found
    driver.switch_to.default_content()
    return None, None, None, False

class AuthHelper:
    def __init__(self, driver, user_ent, pass_ent):
        self.driver = driver
        self.user_ent = user_ent
        self.pass_ent = pass_ent

    def do_login(self):
        """Open LOGIN_URL_10X, follow redirects, type creds, submit."""
        # If already on HOME and warehouse selector is visible, skip re-login
        try:
            self.driver.switch_to.default_content()
        except Exception:
            pass
        try:
            WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.ID, 'select-warehouse')))
            print('ℹ️ Session already authenticated; skipping re-login.')
            return
        except Exception:
            pass
        # Navigate to base URL only if not already on HOME
        try:
            cur = (self.driver.current_url or '')
            if not cur.startswith(HOME_URL):
                self.driver.get(LOGIN_URL_10X)
        except Exception:
            pass

        # Wait a bit for any redirection
        time.sleep(0.8 if HEADLESS_ENV == "2" else 1.0)

        # Find login fields (with iframe support)
        user_el, pass_el, submit_el, used_iframe = _locate_login_fields(self.driver, timeout=TIMEOUT_MED)
        if not (user_el and pass_el and submit_el):
            raise TimeoutException("Login fields not found (even after iframe scan).")

        # Type credentials safely
        try:
            user_el.clear()
        except Exception:
            pass
        _set_value_fast(self.driver, user_el, self.user_ent.get())

        try:
            pass_el.clear()
        except Exception:
            pass
        _set_value_fast(self.driver, pass_el, self.pass_ent.get())

        # Click submit
        try:
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", submit_el)
        except Exception:
            pass
        submit_el.click()

        # Return to default content after submit
        if used_iframe:
            try:
                self.driver.switch_to.default_content()
            except Exception:
                pass

        # Give the page some time to transition
        time.sleep(1.0 if HEADLESS_ENV == "2" else 1.2)

def on_mfa_page(driver) -> bool:
    try:
        url = (driver.current_url or "").lower()
        page_text = (driver.page_source or "").lower()
        if "2ndfactor" in url or "2nd factor authentication" in page_text:
            return True
        # direct field detection
        try:
            driver.find_element(By.ID, "emailOtp")
            return True
        except Exception:
            pass
        # presence of EMAIL/SMS controls
        candidates = [
            (By.PARTIAL_LINK_TEXT, "EMAIL"),
            (By.ID, "sendEMAIL"),
            (By.XPATH, "//div[normalize-space()='EMAIL']"),
            (By.XPATH, "//button[normalize-space()='EMAIL']"),
            (By.XPATH, "//span[normalize-space()='EMAIL']"),
            (By.XPATH, "//button[normalize-space()='Verify OTP']"),
        ]
        for by, sel in candidates:
            try:
                el = driver.find_element(by, sel)
                if el:
                    return True
            except Exception:
                continue
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass
    return False

# Helper: find element with iframe support

def _find_element_with_iframe_support(driver, locator, timeout):
    """Try default content then search through iframes. Returns (element, used_iframe)."""
    try:
        driver.switch_to.default_content()
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass
    try:
        el = WebDriverWait(driver, timeout).until(EC.presence_of_element_located(locator))
        return el, False
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass
    # scan iframes
    try:
        frames = driver.find_elements(By.TAG_NAME, 'iframe')
    except Exception:
        frames = []
    for fr in frames:
        try:
            driver.switch_to.default_content()
            driver.switch_to.frame(fr)
            el = WebDriverWait(driver, timeout).until(EC.presence_of_element_located(locator))
            return el, True
        except Exception:
            continue
    try:
        driver.switch_to.default_content()
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass
    return None, False

def click_email_option(driver):
    candidates = [
        (By.XPATH, "//div[normalize-space()='EMAIL']"),
        (By.XPATH, "//button[normalize-space()='EMAIL']"),
        (By.PARTIAL_LINK_TEXT, "EMAIL"),
        (By.XPATH, "//li[normalize-space()='EMAIL']"),
        (By.XPATH, "//a[contains(.,'EMAIL')]"),
        (By.XPATH, "//span[normalize-space()='EMAIL']"),
    ]
    for by, sel in candidates:
        try:
            el = WebDriverWait(driver, TIMEOUT_SHORT).until(EC.element_to_be_clickable((by, sel)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            el.click()
            print("✅ Clicked EMAIL option on MFA page.")
            return True
        except Exception:
            continue
    return False

def click_send_email(driver):
    candidates = [
        (By.ID, "sendEMAIL"),
        (By.XPATH, "//button[contains(.,'Send OTP')]"),
        (By.XPATH, "//button[contains(translate(., 'send','SEND'),'SEND')]"),
        (By.XPATH, "//input[@type='button' and (contains(@id,'send') or contains(@value,'Send'))]"),
    ]
    for by, sel in candidates:
        try:
            btn = WebDriverWait(driver, TIMEOUT_SHORT).until(EC.element_to_be_clickable((by, sel)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            btn.click()
            print("✅ Clicked Send EMAIL (Send OTP).")
            return True
        except Exception:
            continue
    # Sometimes emailOtp already present
    try:
        driver.find_element(By.ID, "emailOtp")
        print("ℹ️ emailOtp field already present; skipping Send.")
        return True
    except Exception:
        return False

def wait_for_email_otp_field(driver, timeout=TIMEOUT_MED):
    # try default + iframe
    el, used_iframe = _find_element_with_iframe_support(driver, (By.ID, "emailOtp"), timeout)
    if el:
        return el
    # fallback: any otp-like input
    try:
        el, used_iframe = _find_element_with_iframe_support(
            driver,
            (By.XPATH, "//input[@type='text' and (contains(@id,'otp') or contains(@name,'otp'))]"),
            timeout,
        )
        return el
    except Exception:
        return None

def perform_email_mfa_submit(driver, otp_value: str, remember: bool = True) -> bool:
    """Submit EMAIL OTP from GUI entry: fills site field and clicks Verify/Submit."""
    otp_input = wait_for_email_otp_field(driver, timeout=TIMEOUT_SHORT)
    if not otp_input:
        return False
    used_iframe = False
    try:
        # detect if we're in an iframe
        try:
            driver.switch_to.default_content()
            # if otp_input becomes stale when switching, we must re-find inside frames
            pass
        except Exception:
            pass
        # re-find with iframe support to know flag
        otp_input, used_iframe = _find_element_with_iframe_support(driver, (By.ID, "emailOtp"), TIMEOUT_SHORT)
        if not otp_input:
            otp_input, used_iframe = _find_element_with_iframe_support(
                driver,
                (By.XPATH, "//input[@type='text' and (contains(@id,'otp') or contains(@name,'otp'))]"),
                TIMEOUT_SHORT,
            )
        if not otp_input:
            return False
        otp_input.clear()
        otp_input.send_keys(otp_value.strip())
        # Remember me (email)
        try:
            rem, _ = _find_element_with_iframe_support(driver, (By.ID, "rememberMeEMAIL"), TIMEOUT_SHORT)
            if remember and rem and not rem.is_selected():
                driver.execute_script("arguments[0].click();", rem)
        except Exception:
            pass
        # submit buttons: support 'Verify OTP' and generic Submit
        candidates_submit = [
            (By.ID, "emailSubmit"),
            (By.XPATH, "//button[normalize-space()='Verify OTP']"),
            (By.XPATH, "//button[normalize-space()='Submit']"),
            (By.XPATH, "//input[@type='submit']"),
            (By.XPATH, "//input[@type='button' and (contains(@value,'Verify') or contains(@value,'Submit'))]"),
        ]
        for by, sel in candidates_submit:
            try:
                btn, _ = _find_element_with_iframe_support(driver, (by, sel), TIMEOUT_SHORT)
                if not btn:
                    continue
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                try:
                    btn.click()
                except Exception:
                    driver.execute_script("arguments[0].click();", btn)
                print("✅ Submitted EMAIL OTP.")
                # after submit, go back to default content to continue
                try:
                    driver.switch_to.default_content()
                except Exception:
                    pass
                # wait briefly for MFA page to disappear
                try:
                    WebDriverWait(driver, 6).until(lambda d: not on_mfa_page(d))
                except Exception:
                    pass
                return True
            except Exception:
                continue
        return False
    except Exception:
        return False

# --- SMS MFA helpers ---
SMS_PANEL_ID = "panel2"
SMS_SEND_BUTTON_ID = "sendSMS"
SMS_REMEMBER_CHECK_ID = "rememberMeSMS"
SMS_SUBMIT_BUTTON_ID = "smsSubmit"
SMS_OTP_INPUT_IDS = ["smsOtp", "otpSMS", "mobileOtp"]

def click_sms_option(driver):
    candidates = [
        (By.ID, SMS_PANEL_ID),
        (By.XPATH, "//div[@id='panel2']//div[contains(@class,'panel-heading') or contains(@class,'panel-title') or contains(.,'SMS') ]"),
        (By.XPATH, "//div[normalize-space()='SMS']"),
        (By.XPATH, "//button[normalize-space()='SMS']"),
        (By.PARTIAL_LINK_TEXT, "SMS"),
        (By.XPATH, "//li[normalize-space()='SMS']"),
        (By.XPATH, "//span[normalize-space()='SMS']"),
    ]
    for by, sel in candidates:
        try:
            el = WebDriverWait(driver, TIMEOUT_SHORT).until(EC.element_to_be_clickable((by, sel)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            el.click()
            print("✅ Clicked SMS option on MFA page.")
            return True
        except Exception:
            continue
    return False

def click_send_sms(driver):
    candidates = [
        (By.ID, SMS_SEND_BUTTON_ID),
        (By.XPATH, "//button[contains(@id,'sendSMS') or contains(.,'Send OTP') or contains(translate(.,'send','SEND'),'SEND')]"),
        (By.XPATH, "//input[@type='button' and (contains(@id,'sendSMS') or contains(@value,'Send'))]"),
    ]
    for by, sel in candidates:
        try:
            btn = WebDriverWait(driver, TIMEOUT_SHORT).until(EC.element_to_be_clickable((by, sel)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            btn.click()
            print("✅ Clicked Send SMS (Send OTP).")
            return True
        except Exception:
            continue
    try:
        for otp_id in SMS_OTP_INPUT_IDS:
            if driver.find_elements(By.ID, otp_id):
                print("ℹ️ SMS OTP field already present; skipping Send.")
                return True
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass
    return False

def wait_for_sms_otp_field(driver, timeout=TIMEOUT_MED):
    try:
        for otp_id in SMS_OTP_INPUT_IDS:
            try:
                el = WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.ID, otp_id)))
                return el
            except TimeoutException:
                continue
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass
    try:
        el = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.XPATH, "//input[@type='text' and (contains(@id,'otp') or contains(@name,'otp'))]"))
        )
        return el
    except TimeoutException:
        return None

def perform_sms_mfa_submit(driver, otp_value: str, remember: bool = True) -> bool:
    otp_input = wait_for_sms_otp_field(driver, timeout=TIMEOUT_SHORT)
    if not otp_input:
        return False
    try:
        otp_input.clear()
        otp_input.send_keys(otp_value.strip())
        try:
            rem = driver.find_element(By.ID, SMS_REMEMBER_CHECK_ID)
            if remember and rem and not rem.is_selected():
                driver.execute_script("arguments[0].click();", rem)
        except Exception:
            pass
        candidates_submit = [
            (By.ID, SMS_SUBMIT_BUTTON_ID),
            (By.XPATH, "//button[@type='submit' and (contains(@id,'smsSubmit') or contains(.,'Submit'))]"),
            (By.XPATH, "//input[@type='submit']"),
        ]
        for by, sel in candidates_submit:
            try:
                btn = WebDriverWait(driver, TIMEOUT_SHORT).until(EC.element_to_be_clickable((by, sel)))
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                btn.click()
                print("✅ Submitted SMS OTP.")
                return True
            except Exception:
                continue
        return False
    except Exception:
        return False

RESULTS_TABLE_BASE_XPATH =  (
    "//table["
    ".//th[.//a[normalize-space(.)='Qty'] or normalize-space(.)='Qty'] and "
    ".//th[.//a[normalize-space(.)='Warehouse'] or normalize-space(.)='Warehouse'] and "
    ".//th[.//a[normalize-space(.)='Destination Party'] or normalize-space(.)='Destination Party']"
    "]"
)
QTY_TD_INDEX = 7
WAREHOUSE_TD_INDEX = 8
DEST_PARTY_TD_INDEX = 9

def _tbody_cell_xpath(col_index: int) -> str:
    return f"(({RESULTS_TABLE_BASE_XPATH})[1]//tbody/tr[1]/td[{col_index}])"

EXTERNAL_ID_INPUT_NAME = "filters[external_id]"
EXTERNAL_ID_INPUT_CLASS = "searchTextClass"
EXTERNAL_ID_LABEL_TEXT = "External ID"

def _get_element_text(driver, xpath, wait_time=TIMEOUT_MED):
    elem = WebDriverWait(driver, wait_time).until(
        EC.presence_of_element_located((By.XPATH, xpath)),
        message=f"Timeout waiting for element with XPath: {xpath}"
    )
    return (elem.text or "").strip()

def fill_external_id_and_find_request(driver, wait, stn_number: str):
    input_candidates = [
        (By.NAME, EXTERNAL_ID_INPUT_NAME),
        (By.CSS_SELECTOR, f"input.{EXTERNAL_ID_INPUT_CLASS}[name='{EXTERNAL_ID_INPUT_NAME}']"),
        (By.XPATH, f"//span[@class='formlabel' and normalize-space(.)='{EXTERNAL_ID_LABEL_TEXT}']/following-sibling::input[@type='text']"),
        (By.XPATH, "//div[contains(@class,'formField')][.//span[@class='formlabel' and normalize-space(.)='External ID']]//input[@type='text']")
    ]
    external_input = None
    for by, sel in input_candidates:
        try:
            external_input = WebDriverWait(driver, TIMEOUT_SHORT).until(EC.element_to_be_clickable((by, sel)))
            break
        except TimeoutException:
            continue
    if not external_input:
        log_error("Find Request", "Unable to locate 'External ID' input field.", 4)
        return False

    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", external_input)
        external_input.clear()
        external_input.send_keys(stn_number)
        print(f"📝 Entered STN into 'External ID': {stn_number}")
    except Exception as e:
        log_error("Find Request", f"Failed typing External ID: {e}", 4)
        return False

    button_candidates = [
        (By.CSS_SELECTOR, "input.uiButton[value*='Find Request']"),
        (By.XPATH, "//input[@type='button' and contains(normalize-space(@value),'Find Request')]"),
    ]
    find_btn = None
    for by, sel in button_candidates:
        try:
            find_btn = WebDriverWait(driver, TIMEOUT_SHORT).until(EC.element_to_be_clickable((by, sel)))
            break
        except TimeoutException:
            continue
    if not find_btn:
        log_error("Find Request", "Unable to locate 'Find Request' button.", 4)
        return False

    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", find_btn)
        find_btn.click()
        print("🔎 Clicked 'Find Request'.")
        handle_popups(driver)
    except Exception as e:
        log_error("Find Request", f"Failed clicking 'Find Request': {e}", 4)
        return False

    return True
    # Robust wait for results to appear (expand toggles if collapsed)
    try:
        WebDriverWait(driver, TIMEOUT_MED).until(
            EC.presence_of_element_located((By.XPATH, RESULTS_TABLE_BASE_XPATH))
        )
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass

    try:
        _click_table_toggle(driver, wait)
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass

    had_row = False
    for attempt in range(2):
        try:
            WebDriverWait(driver, TIMEOUT_MED).until(
                EC.presence_of_element_located((By.XPATH, f"({RESULTS_TABLE_BASE_XPATH})[1]//tbody/tr[1]"))
            )
            had_row = True
            break
        except TimeoutException:
            if attempt == 0:
                try:
                    handle_popups(driver)
                    _click_table_toggle(driver, wait)
                except Exception:
                    pass
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", find_btn)
                    find_btn.click()
                    print("[Retry] Clicking 'Find Request' again...")
                except Exception:
                    pass

    if not had_row:
        pass
    return True

def scrape_find_request_details(driver, wait):
    try:
        WebDriverWait(driver, TIMEOUT_MED).until(
            EC.presence_of_element_located((By.XPATH, RESULTS_TABLE_BASE_XPATH))
        )
        WebDriverWait(driver, TIMEOUT_SHORT).until(
            EC.presence_of_element_located((By.XPATH, f"({RESULTS_TABLE_BASE_XPATH})[1]//tbody/tr[1]"))
        )
        qty = _get_element_text(driver, _tbody_cell_xpath(QTY_TD_INDEX), wait_time=TIMEOUT_SHORT)
        wh = _get_element_text(driver, _tbody_cell_xpath(WAREHOUSE_TD_INDEX), wait_time=TIMEOUT_SHORT)
        dest = _get_element_text(driver, _tbody_cell_xpath(DEST_PARTY_TD_INDEX), wait_time=TIMEOUT_SHORT)

        print("--- Find Request Scrape Results ---")
        print(f"Qty (td[{QTY_TD_INDEX}]): {qty}")
        print(f"Warehouse (td[{WAREHOUSE_TD_INDEX}]): {wh}")
        print(f"Destination Party (td[{DEST_PARTY_TD_INDEX}]): {dest}")
        print("-----------------------------------")

        return qty, wh, dest
    except TimeoutException:
        log_error("Scrape Error", "Results did not load or no rows in Find Request.", 4)
        return None, None, None
    except Exception as e:
        print(f"❌ Scrape failed: {e}")
        log_error("Find Request Results", f"Scrape failed: {e}", 4)
        return None, None, None


# ============================ Track Request: scrape Source/Destination & update STN-Final ============================

def scrape_track_request_source_dest(driver, timeout=TIMEOUT_MED):
    try:
        row = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.XPATH, "//table[contains(@class,'sticky-header')]//tbody/tr[1]"))
        )
        tds = row.find_elements(By.XPATH, './td')
        wh = (tds[3].text or '').strip() if len(tds) >= 5 else ''
        dest = (tds[4].text or '').strip() if len(tds) >= 5 else ''
        return wh, dest
    except Exception as e:
        print(f"[TrackRequest][WARN] scrape source/destination failed: {e}")
        return None, None


def stn_final_update_source_destination(stn: str, source: str = None, destination: str = None):
    if not stn:
        return 0
    try:
        import sqlite3
        with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(f'UPDATE {STN_FINAL_TABLE} SET "Source" = ?, "Destination" = ? WHERE UPPER("STN") = UPPER(?)',
                        ((source or ''), (destination or ''), stn))
            affected = cur.rowcount or 0
            if affected == 0:
                cur.execute(
                    f'INSERT INTO {STN_FINAL_TABLE} ("user-id","STN","TL-Id","Qty","Shelf","Catagory","WID","FSN","EAN","Model-Id","Source","Destination","Box-Id","Pick","TL-Id status","Pack","Consignment-Id","Dispatch") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    ('', stn, '', None, None, None, None, None, None, None, (source or ''), (destination or ''), None, None, None, None, None, None)
                )
                affected = 1
            conn.commit()
            print(f"[STN-Final] Source/Destination updated for STN={stn}: {source} / {destination}")
            return affected
    except Exception as e:
        print('[STN-Final][ERROR] update Source/Destination failed:', e)
        return 0
# ============================ PDF Helpers & Stamping ==========================
LINE_SPACING = 1.0
SOURCE_DEST_FONT_PT = 3
SOURCE_DEST_BOLD_PASSES = 3
SOURCE_DEST_BOLD_OFFSET_PT = 0.15
STN_FONT_PT = 6
INNER_MARGIN_PT = 1.2
TIMESTAMP_FONT_PT = 4

SRC_RECT_X_RATIO = 0.06
SRC_RECT_Y_RATIO = 0.39
SRC_RECT_W_RATIO = 0.38
SRC_RECT_H_RATIO = 0.045

DST_RECT_X_RATIO = 0.56
DST_RECT_Y_RATIO = 0.39
DST_RECT_W_RATIO = 0.38
DST_RECT_H_RATIO = 0.045

LB1_Y_RATIO = 0.14
STN_LB1_RECT_W_RATIO = 0.62
STN_LB1_RECT_H_RATIO = 0.177
STN_LB1_OFFSET_DOWN_RATIO = 0.20

QR_X_RATIO = 0.06
QR_Y_RATIO = 0.05
QR_SIZE_RATIO = 0.18

TIMESTAMP_HIGHLIGHT_GAP_RATIO = 0.020
TIMESTAMP_HIGHLIGHT_H_RATIO = 0.065

SERIAL_FONT_MAX_PT = 5
SERIAL_FONT_MIN_PT = 2
SERIAL_FONT_NAME = "Helvetica-Bold"
SERIAL_TEXT_MARGIN_PT = 0.0
SERIAL_TEXT_X_SHIFT_PT = 8.0
SERIAL_TEXT_Y_SHIFT_PT = 4.0



def stn_final_get_tl_for_stn(stn: str):
    """Return latest non-empty TL-Id for a given STN from table stn_final."""
    try:
        import sqlite3
        with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            sql = (
                f'SELECT "TL-Id" FROM {STN_FINAL_TABLE} '
                f'WHERE UPPER("STN")=UPPER(?) AND TRIM(COALESCE("TL-Id","")) <> "" '
                f'ORDER BY ROWID DESC LIMIT 1;'
            )
            cur.execute(sql, (stn or '',))
            row = cur.fetchone()
            return (row[0].strip() if row and row[0] else None)
    except Exception as e:
        print('[STN-Final][WARN] get TL for STN failed:', e)
        return None


def recover_tl_if_missing(possible_tl: str, stn: str):
    """Return TL if present else try to fetch from stn_final."""
    tl = (possible_tl or '').strip()
    if tl:
        return tl
    found = stn_final_get_tl_for_stn(stn)
    if found:
        print('[STN-Final][INFO] Fetched TL from stn_final for STN', stn, ':', found)
        return found
    return None
def _list_pdfs(folder: Path):
    return sorted([p for p in folder.glob("*.pdf") if p.is_file()], key=lambda x: x.stat().st_mtime)

def wait_for_new_pdf(download_dir: Path, baseline_names=None, start_ts=None, wait_duration: int = PDF_DOWNLOAD_TIMEOUT_SEC):
    download_dir.mkdir(parents=True, exist_ok=True)
    baseline = set(baseline_names or [p.name for p in _list_pdfs(download_dir)])
    start = time.time() if start_ts is None else start_ts
    candidate = None
    last_size = -1
    stable_count = 0
    deadline = start + wait_duration
    while time.time() < deadline:
        pdfs = _list_pdfs(download_dir)
        candidate = None
        for p in pdfs:
            if p.name not in baseline:
                candidate = p
                break
        if not candidate and pdfs:
            latest = pdfs[-1]
            try:
                if latest.stat().st_mtime >= start:
                    candidate = latest
            except Exception:
                pass
        if candidate:
            crdl = download_dir / (candidate.name + ".crdownload")
            try:
                size_now = candidate.stat().st_size
            except Exception:
                size_now = -1
            if size_now > 0 and size_now == last_size and not crdl.exists():
                stable_count += 1
                if stable_count >= 1:  # fewer confirmations for speed
                    return candidate
            else:
                stable_count = 0
                last_size = size_now
        time.sleep(0.4 if HEADLESS_ENV == "2" else 0.5)
    return None

def stamp_pdf_all_pages_split(input_pdf: Path, output_pdf: Path, s_value: str, d_value: str, stn_id: str):
    ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    reader = PdfReader(str(input_pdf))
    writer = PdfWriter()
    serial_counter = 1

    for page in reader.pages:
        width = float(page.mediabox.width)
        height = float(page.mediabox.height)

        src_x = width * SRC_RECT_X_RATIO
        src_y = height * SRC_RECT_Y_RATIO
        src_w = width * SRC_RECT_W_RATIO
        src_h = height * SRC_RECT_H_RATIO

        dst_x = width * DST_RECT_X_RATIO
        dst_y = height * DST_RECT_Y_RATIO
        dst_w = width * DST_RECT_W_RATIO
        dst_h = height * DST_RECT_H_RATIO

        lb1_rect_w = width * STN_LB1_RECT_W_RATIO
        lb1_rect_h = height * STN_LB1_RECT_H_RATIO
        lb1_x = max((width - lb1_rect_w) / 2.0, 2.0)
        lb1_y = max((height * LB1_Y_RATIO) - (height * STN_LB1_OFFSET_DOWN_RATIO), 2.0)

        qr_size = min(width, height) * QR_SIZE_RATIO
        qr_x = width * QR_X_RATIO
        qr_y = height * QR_Y_RATIO

        ts_box_h = height * TIMESTAMP_HIGHLIGHT_H_RATIO
        ts_gap = height * TIMESTAMP_HIGHLIGHT_GAP_RATIO
        ts_x = lb1_x
        ts_w = lb1_rect_w
        ts_y = max(lb1_y - ts_box_h - ts_gap, 2.0)

        buf = io.BytesIO()
        c = canvas.Canvas(buf, pagesize=(width, height))

        def _wrap_line_to_width(cnv, text, font_name, font_pt, max_w):
            width_func = cnv.stringWidth
            if width_func(text, font_name, font_pt) <= max_w:
                return [text]
            words = (text or "").split()
            if words:
                lines, cur = [], ""
                for w in words:
                    test = (cur + " " + w).strip()
                    if width_func(test, font_name, font_pt) <= max_w:
                        cur = test
                    else:
                        if cur: lines.append(cur)
                        cur = w
                if cur: lines.append(cur)
                return lines
            lines, chunk = [], ""
            for ch in text or "":
                test = chunk + ch
                if width_func(test, font_name, font_pt) <= max_w:
                    chunk = test
                else:
                    if chunk: lines.append(chunk)
                    chunk = ch
            if chunk: lines.append(chunk)
            return lines

        def _draw_text_centered_heavy_no_box(cnv, area_x, area_y, area_w, area_h, lines, font_name, font_size_pt,
                                             line_spacing=LINE_SPACING, passes=SOURCE_DEST_BOLD_PASSES,
                                             offset_pt=SOURCE_DEST_BOLD_OFFSET_PT):
            total_text_h = font_size_pt * len(lines) * line_spacing
            start_y = area_y + (area_h - total_text_h) / 2.0 + (font_size_pt * 0.85)
            cnv.setFillColor(black)
            cnv.setFont(font_name, font_size_pt)
            offsets = [(0, 0)]
            if passes >= 2: offsets.append((offset_pt, 0))
            if passes >= 3: offsets.append((0, offset_pt))
            if passes >= 4: offsets.append((-offset_pt, 0))
            if passes >= 5: offsets.append((0, -offset_pt))
            for idx, l in enumerate(lines):
                line_w = cnv.stringWidth(l, font_name, font_size_pt)
                base_x = area_x + (area_w - line_w) / 2.0
                base_y = start_y - (font_size_pt * idx * line_spacing)
                for dx, dy in offsets:
                    cnv.drawString(base_x + dx, base_y + dy, l)

        def _draw_filled_rect_thin_border_centered_text(cnv, rect_x, rect_y, rect_w, rect_h, lines,
                                                        font_name, font_size_pt, line_spacing=LINE_SPACING,
                                                        fill_color=white, border_color=white, border_width_pt=0.15):
            cnv.setFillColor(fill_color)
            cnv.setStrokeColor(border_color)
            cnv.setLineWidth(border_width_pt)
            cnv.rect(rect_x, rect_y, rect_w, rect_h, stroke=1 if border_width_pt > 0 else 0, fill=1)
            total_text_h = font_size_pt * len(lines) * line_spacing
            start_y = rect_y + (rect_h - total_text_h) / 2.0 + (font_size_pt * 0.85)
            cnv.setFillColor(black)
            cnv.setFont(font_name, font_size_pt)
            for idx, l in enumerate(lines):
                line_w = cnv.stringWidth(l, font_name, font_size_pt)
                text_x = rect_x + (rect_w - line_w) / 2.0
                text_y = start_y - (font_size_pt * idx * line_spacing)
                cnv.drawString(text_x, text_y, l)

        def _draw_qr_square(cnv, data: str, x: float, y: float, size_pt: float):
            if not data:
                return
            try:
                qr_code = qr.QrCodeWidget(data)
                bounds = qr_code.getBounds()
                w = bounds[2] - bounds[0]
                h = bounds[3] - bounds[1]
                scale_x = size_pt / float(w)
                scale_y = size_pt / float(h)
                d = Drawing(size_pt, size_pt)
                d.add(qr_code)
                d.scale(scale_x, scale_y)
                renderPDF.draw(d, cnv, x, y)
            except Exception as e:
                print(f"⚠️ QR draw failed: {e}")

        def _draw_timestamp_highlight_bar(cnv, area_x, area_y, area_w, area_h, text, font_name="Helvetica-Bold",
                                          font_size_pt=TIMESTAMP_FONT_PT):
            cnv.setFillColor(white)
            cnv.setStrokeColor(white)
            cnv.setLineWidth(0.0)
            cnv.rect(area_x, area_y, area_w, area_h, stroke=0, fill=1)
            cnv.setFillColor(black)
            cnv.setFont(font_name, font_size_pt)
            line_w = cnv.stringWidth(text, font_name, font_size_pt)
            text_x = area_x + (area_w - line_w) / 2.0
            text_y = area_y + (area_h - font_size_pt) / 2.0 + (font_size_pt * 0.85)
            cnv.drawString(text_x, text_y, text)

        def _draw_serial_number_only(cnv, serial_text: str, page_w: float, page_h: float):
            if not serial_text:
                return
            soft_max_w = max(page_w * 0.08, 24.0)
            font_pt = SERIAL_FONT_MAX_PT
            while font_pt >= SERIAL_FONT_MIN_PT:
                text_w = cnv.stringWidth(serial_text, SERIAL_FONT_NAME, font_pt)
                if text_w <= soft_max_w:
                    break
                font_pt -= 1
            if font_pt < SERIAL_FONT_MIN_PT:
                font_pt = SERIAL_FONT_MIN_PT
                text_w = cnv.stringWidth(serial_text, SERIAL_FONT_NAME, font_pt)
            cnv.setFillColor(black)
            cnv.setFont(SERIAL_FONT_NAME, font_pt)
            text_x = max(page_w - SERIAL_TEXT_MARGIN_PT - text_w - SERIAL_TEXT_X_SHIFT_PT, 0.0)
            baseline_correction = font_pt * 0.15
            text_y = min(page_h - SERIAL_TEXT_MARGIN_PT + SERIAL_TEXT_Y_SHIFT_PT - baseline_correction, page_h - font_pt)
            cnv.drawString(text_x, text_y, serial_text)

        # Compose overlay
        font_name_sd = "Helvetica-Bold"
        sd_src_line = f"S: {s_value or ''}"
        sd_dst_line = f"D: {d_value or ''}"
        src_lines = _wrap_line_to_width(c, sd_src_line, font_name_sd, SOURCE_DEST_FONT_PT, max(src_w - 2, 6.0))
        dst_lines = _wrap_line_to_width(c, sd_dst_line, font_name_sd, SOURCE_DEST_FONT_PT, max(dst_w - 2, 6.0))
        _draw_text_centered_heavy_no_box(c, src_x, src_y, src_w, src_h, src_lines, font_name_sd, SOURCE_DEST_FONT_PT)
        _draw_text_centered_heavy_no_box(c, dst_x, dst_y, dst_w, dst_h, dst_lines, font_name_sd, SOURCE_DEST_FONT_PT)

        font_name_stn = "Helvetica-Bold"
        stn_line = f"{stn_id or ''}"
        stn_lines = _wrap_line_to_width(c, stn_line, font_name_stn, STN_FONT_PT, max(lb1_rect_w - 2, 6.0))
        _draw_filled_rect_thin_border_centered_text(c, lb1_x, lb1_y, lb1_rect_w, lb1_rect_h,
                                                    stn_lines, font_name_stn, STN_FONT_PT)

        if stn_id:
            _draw_qr_square(c, data=stn_id, x=qr_x, y=qr_y, size_pt=qr_size)

        _draw_timestamp_highlight_bar(c, area_x=ts_x, area_y=ts_y, area_w=ts_w, area_h=ts_box_h, text=ts_str)
        _draw_serial_number_only(c, str(serial_counter), width, height)

        c.showPage()
        c.save()
        buf.seek(0)
        overlay_reader = PdfReader(buf)
        overlay_page = overlay_reader.pages[0]
        try:
            page.merge_page(overlay_page)
        except AttributeError:
            page.mergePage(overlay_page)
        writer.add_page(page)
        serial_counter += 1

    with open(output_pdf, "wb") as f_out:
        writer.write(f_out)

def _sanitize_qty_to_int_str(qty_text: Optional[str]) -> Optional[str]:
    if not qty_text:
        return None
    m = re.search(r"\b\d{1,3}(?:,\d{3})*\b", qty_text)
    if m:
        return m.group(0).replace(",", "")
    simple_digits = re.search(r"\d+", qty_text)
    if simple_digits:
        return simple_digits.group(0)
    return None

def _open_file_in_default_app(filepath: Path):
    print(f"📄 Attempting to open file: {filepath.name}")
    try:
        if platform.system() == "Windows":
            os.startfile(str(filepath))
        elif platform.system() == "Darwin":
            subprocess.call(('open', str(filepath)))
        else:
            subprocess.call(('xdg-open', str(filepath)))
    except Exception as e:
        log_warn("File Open", f"Failed to auto-open PDF: {e}")

def select_warehouse_by_name(driver, wait, full_name: Optional[str]):
    if not full_name:
        print("❌ No Warehouse Full Name to select.")
        return False
    try:
        driver.switch_to.default_content()
        dropdown = WebDriverWait(driver, TIMEOUT_MED).until(EC.presence_of_element_located((By.ID, "select-warehouse")))
        tag = (dropdown.tag_name or "").lower()
        if tag == "select":
            (_select_by_visible_text_fast(driver, dropdown, full_name) or Select(dropdown).select_by_visible_text(full_name))
            print(f"✅ Selected warehouse: {full_name}")
            return True
        else:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", dropdown)
            dropdown.click()
            candidates = [
                (By.XPATH, f"//*[@id='select-warehouse']//div[contains(@class,'option') and normalize-space(.)='{full_name}']"),
                (By.XPATH, f"//*[@id='select-warehouse']//li[normalize-space(.)='{full_name}']"),
                (By.XPATH, f"//*[@id='select-warehouse']//option[normalize-space(.)='{full_name}']"),
                (By.XPATH, f"//li[normalize-space(.)='{full_name}']"),
                (By.XPATH, f"//div[contains(@class,'option') and normalize-space(.)='{full_name}']"),
                (By.XPATH, f"//span[normalize-space(.)='{full_name}']"),
            ]
            for by, sel in candidates:
                try:
                    option = WebDriverWait(driver, TIMEOUT_SHORT).until(EC.element_to_be_clickable((by, sel)))
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", option)
                    option.click()
                    print(f"✅ Selected warehouse (custom): {full_name}")
                    return True
                except TimeoutException:
                    continue
            print(f"❌ Could not locate warehouse option: {full_name}")
            log_error("Selection Error", "Warehouse option not found in dropdown.", 3)
            return False
    except Exception as e:
        print(f"❌ Warehouse selection failed: {e}")
        log_error("Selection Error", f"Warehouse selection failed: {e}", 3)
        return False

def proceed_box_creation_generate_print_and_stamp(driver, wait, requested_qty_text, stn_number, s_value_from_td8, d_value_from_td9, indicator: CircleProgressIndicator):
    indicator.set_status("Preparing…")
    indicator.set_percent(5)

    qty_value = _sanitize_qty_to_int_str(requested_qty_text)
    if not qty_value:
        indicator.set_status("Invalid Qty")
        indicator.set_percent(0)
        log_error("Boxes Count", f"Scraped Qty '{requested_qty_text}' could not be parsed.", 5, flow="BOX_CREATION")
        return False

    indicator.set_status("Opening Box Creation")
    ok_nav = navigate_if_needed(driver, BOX_CREATION_URL, wait)
    indicator.set_percent(50 if ok_nav else 10)
    if not ok_nav:
        indicator.set_status("Nav failed")
        log_error("Navigation Error", "Failed to navigate to Box Creation URL.", 5, flow="BOX_CREATION")
        return False

    handle_popups(driver)

    try:
        quantity_field = WebDriverWait(driver, TIMEOUT_MED).until(EC.presence_of_element_located((By.ID, "quantity")))
        quantity_field.clear()
        quantity_field.send_keys(qty_value)
        indicator.set_status("Generating boxes")
        indicator.set_percent(60)
    except Exception as e:
        indicator.set_status("Qty input error")
        indicator.set_percent(10)
        log_error("Boxes Count", f"Failed to type quantity: {e}", 5, flow="BOX_CREATION")
        return False

    try:
        generate_button = WebDriverWait(driver, TIMEOUT_MED).until(
            EC.element_to_be_clickable((By.XPATH, "//input[@type='submit' and @value='Generate']"))
        )
        driver.execute_script("arguments[0].click();", generate_button)
        handle_popups(driver)
        indicator.set_status("Boxes generated")
        indicator.set_percent(70)
    except Exception as e:
        indicator.set_status("Generate failed")
        indicator.set_percent(20)
        log_error("Generate Boxes", f"Failed to click Generate: {e}", 5, flow="BOX_CREATION")
        return False

    try:
        print_qty_field = WebDriverWait(driver, TIMEOUT_MED).until(EC.presence_of_element_located((By.ID, "print_quantity")))
        print_qty_field.clear()
        print_qty_field.send_keys("1")
        print_button = WebDriverWait(driver, TIMEOUT_MED).until(
            EC.element_to_be_clickable((By.XPATH, "//button[normalize-space()='Print All Box Labels']"))
        )
        indicator.set_status("Ready to print")
        indicator.set_percent(80)
    except Exception as e:
        indicator.set_status("Print setup failed")
        indicator.set_percent(30)
        log_error("Print Setup", f"Failed preparing print: {e}", 6, flow="BOX_CREATION")
        return False

    baseline_names = [p.name for p in _list_pdfs(DOWNLOAD_FOLDER)]
    start_ts = time.time()
    try:
        driver.execute_script("arguments[0].click();", print_button)
        handle_popups(driver)
        indicator.set_status("Printing labels…")
        indicator.set_percent(88)
    except Exception as e:
        indicator.set_status("Print click failed")
        indicator.set_percent(40)
        log_error("Print Click", f"Failed clicking print: {e}", 6, flow="BOX_CREATION")
        return False

    pdf_path = wait_for_new_pdf(DOWNLOAD_FOLDER, baseline_names=baseline_names, start_ts=start_ts, wait_duration=PDF_DOWNLOAD_TIMEOUT_SEC)
    indicator.set_status("Download " + ("ok" if pdf_path else "timeout"))
    indicator.set_percent(95 if pdf_path else 45)
    if not pdf_path:
        log_error("PDF Download Error", "Timed out waiting for the labels PDF to download.", 6, flow="BOX_CREATION")
        return False

    try:
        stamped_path = pdf_path.with_name(pdf_path.stem + "_stamped.pdf")
        indicator.set_status("Stamping labels")
        stamp_pdf_all_pages_split(
            input_pdf=pdf_path,
            output_pdf=stamped_path,
            s_value=s_value_from_td8,
            d_value=d_value_from_td9,
            stn_id=stn_number
        )
        _open_file_in_default_app(stamped_path)
        indicator.set_status("Done")
        indicator.set_percent(100)
        return True
    except Exception as stamp_err:
        indicator.set_status("Stamp failed")
        indicator.set_percent(60)
        log_error("Stamping Error", f"Failed to stamp the PDF: {stamp_err}", 6, flow="BOX_CREATION")
        return False

# ============================ LOGOUT ==========================================

# ============================ PACK Flow: Selenium helpers ======================
from selenium.webdriver.common.by import By as _PK_BY
from selenium.webdriver.support.ui import WebDriverWait as _PK_Wait
from selenium.webdriver.support import expected_conditions as _PK_EC
from selenium.webdriver.common.keys import Keys as _PK_KEYS

def open_consign_page_in_home_tab(driver, wait) -> bool:
    """Open consignment add boxes URL in the SAME (original) tab after ensuring HOME/warehouse."""
    try:
        print('[PACK] Navigating to HOME…')
        if not ensure_home_loaded(driver, wait, retries=5, sleep_between=1.0):
            print('[PACK] HOME not ready.'); return False
        print('[PACK] Opening Consignment URL…')
        ok = navigate_if_needed(driver, CONSIGNMENT_BOX_URL, wait)
        if not ok:
            print('[PACK] Navigation to Consignment URL failed.')
        return bool(ok)
    except Exception as e:
        print('[PACK] open_consign_page_in_home_tab error:', e)
        return False

def _pk_wait_present(driver, by, sel, to=TIMEOUT_MED):
    return _PK_Wait(driver, to).until(_PK_EC.presence_of_element_located((by, sel)))

def _pk_wait_click(driver, by, sel, to=TIMEOUT_MED):
    return _PK_Wait(driver, to).until(_PK_EC.element_to_be_clickable((by, sel)))

def process_box_on_consign_page(driver, wait, box_id: str, weight_default: str='10', print_qty: str='1') -> str:
    """Run the PACK sequence for a single Box-Id on the Consignment page.
    Returns scraped success message text (Pack message) or empty string if not found.
    Steps:
      1) Type Box-Id into #tote_id and press ENTER
      2) Set weight (default 10) into #weight_hidden
      3) Click Pack Box (#pack_box)
      4) Click Packing Slip (#print_label)
      5) Set print qty (#print_quantity) to 1
      6) Click Security Slip (#print_security_label)
      7) Scrape success message from #pack_box_msg
    """
    msg_text = ''
    try:
        print(f"[PACK][{box_id}] Step1: Focus Box ID field and type + ENTER")
        box_input = _pk_wait_present(driver, _PK_BY.ID, BOX_ID_INPUT_ID, to=TIMEOUT_LONG)
        try:
            box_input.clear()
        except Exception:
            pass
        box_input.send_keys((box_id or '').strip())
        box_input.send_keys(_PK_KEYS.ENTER)
        handle_popups(driver)

        print(f"[PACK][{box_id}] Step2: Set default weight={weight_default}")
        try:
            wt_input = _pk_wait_present(driver, _PK_BY.ID, WEIGHT_INPUT_ID, to=TIMEOUT_MED)
            _set_value_fast(driver, wt_input, str(weight_default))
        except Exception as e:
            print(f"[PACK][{box_id}] WARN: weight input set failed: {e}")

        print(f"[PACK][{box_id}] Step3: Click Pack Box button")
        try:
            pack_btn = _pk_wait_click(driver, _PK_BY.ID, PACK_BOX_BUTTON_ID, to=TIMEOUT_MED)
            driver.execute_script("arguments[0].click();", pack_btn)
            handle_popups(driver)
        except Exception as e:
            print(f"[PACK][{box_id}] WARN: Pack Box click failed: {e}")

        print(f"[PACK][{box_id}] Step4: Click Packing Slip button")
        try:
            slip_btn = _pk_wait_click(driver, _PK_BY.ID, PACKING_SLIP_BUTTON_ID, to=TIMEOUT_MED)
            driver.execute_script("arguments[0].click();", slip_btn)
            handle_popups(driver)
        except Exception as e:
            print(f"[PACK][{box_id}] WARN: Packing Slip click failed: {e}")

        print(f"[PACK][{box_id}] Step5: Set print quantity={print_qty}")
        try:
            pq_input = _pk_wait_present(driver, _PK_BY.ID, PRINT_QUANTITY_INPUT_ID, to=TIMEOUT_MED)
            _set_value_fast(driver, pq_input, str(print_qty))
        except Exception as e:
            print(f"[PACK][{box_id}] WARN: print quantity set failed: {e}")

        print(f"[PACK][{box_id}] Step6: Click Security Slip button")
        try:
            sec_btn = _pk_wait_click(driver, _PK_BY.ID, SECURITY_SLIP_BUTTON_ID, to=TIMEOUT_MED)
            driver.execute_script("arguments[0].click();", sec_btn)
            handle_popups(driver)
        except Exception as e:
            print(f"[PACK][{box_id}] WARN: Security Slip click failed: {e}")

        print(f"[PACK][{box_id}] Step7: Read success message")
        try:
            msg_el = _pk_wait_present(driver, _PK_BY.ID, PACK_BOX_MSG_ID, to=TIMEOUT_LONG)
            msg_text = (msg_el.text or '').strip()
            print(f"[PACK][{box_id}] Message: {msg_text}")
        except Exception as e:
            print(f"[PACK][{box_id}] WARN: success message not found: {e}")
        return msg_text or ''
    except Exception as e:
        print(f"[PACK][{box_id}] ERROR in process_box_on_consign_page: {e}")
        return msg_text or ''


def process_packing_for_completed_tls(driver, wait, weight_default='10', print_qty='1') -> int:
    """If all TLs are marked TL Complete, iterate Box-Ids with empty Pack and process each.
    Returns number of boxes successfully processed (updated in DB).
    """
    try:
        print('[PACK] Checking if all TLs are complete…')
        if not stn_final_all_tls_complete():
            print('[PACK] Not all TLs are TL Complete yet. Aborting pack run.')
            return 0
        box_ids = stn_final_box_ids_to_pack()
        print(f"[PACK] Box-Ids to process: {len(box_ids)}")
        if not box_ids:
            return 0
        # Ensure consignment page is open in same tab
        if not open_consign_page_in_home_tab(driver, wait):
            return 0
        processed = 0
        for i, bid in enumerate(box_ids, start=1):
            try:
                print(f"[PACK] ({i}/{len(box_ids)}) Processing Box: {bid}")
                msg = process_box_on_consign_page(driver, wait, bid, weight_default=weight_default, print_qty=print_qty)
                if msg:
                    stn_final_update_pack_by_box(bid, msg)
                    processed += 1
                else:
                    # Still record an attempt with a placeholder
                    stn_final_update_pack_by_box(bid, 'Pack attempted: message not found')
            except Exception as e:
                print(f"[PACK] ERROR on box {bid}: {e}")
                try:
                    stn_final_update_pack_by_box(bid, f'Pack error: {e}')
                except Exception:
                    pass
                # keep going
        print(f"[PACK] Completed. Boxes processed: {processed}")
        return processed
    except Exception as e:
        print('[PACK] FATAL process_packing_for_completed_tls:', e)
        return 0

def logout_session(driver_instance):
    global GUI_ACTIVE_DRIVER
    if driver_instance:
        try:
            driver_instance.quit()
            print("✅ WebDriver session closed (Logged out).")
        except Exception as e:
            print(f"⚠️ Warning: Error closing WebDriver session: {e}")
    GUI_ACTIVE_DRIVER = None
    cleanup_profile_dir()

# ============================ GUI (Login, Settings & Main) ====================
class SettingsBar(tk.Frame):
    """
    Settings bar with 'Headless (Advanced)' checkbox.
    Restart browser in-place when toggled.
    """
    def __init__(self, master, on_toggle_headless):
        super().__init__(master, bg="#eef", padx=8, pady=4)
        self.on_toggle = on_toggle_headless
        self.var_headless_adv = tk.BooleanVar(value=(HEADLESS_ENV == "2"))
        tk.Label(self, text="Settings:", bg="#eef", fg="#333").pack(side="left", padx=(0, 8))
        self.cb = tk.Checkbutton(self, text="Headless (Advanced)", bg="#eef",
                                 variable=self.var_headless_adv, command=self._toggled)
        self.cb.pack(side="left")

    def _toggled(self):
        self.on_toggle("2" if self.var_headless_adv.get() else "0")

class LoginFrame(tk.Frame):
    """
    Login UI with:
    - Username, Password
    - WH short-code dropdown (searchable) + instant suggestions list
    - Status line
    - OTP section with user-selectable method (SMS or EMAIL), auto-send on selection
    """
    def __init__(self, master, on_submit_callback):
        super().__init__(master, padx=20, pady=20)
        self.on_submit = on_submit_callback
        self.driver_ref = None
        self.mfa_method_var = tk.StringVar(value="EMAIL")
        self._build()

    def _build(self):
        tk.Label(self, text="Login", font=("Arial", 14, "bold")).pack(pady=(0, 5))
        tk.Label(self, text="Enter LDAP credentials & select Warehouse", fg="#666").pack(pady=(0, 12))

        tk.Label(self, text="User Name").pack(anchor="w")
        self.username_entry = tk.Entry(self, width=35)
        self.username_entry.pack(fill="x")

        tk.Label(self, text="Password").pack(anchor="w", pady=(10, 0))
        self.password_entry = tk.Entry(self, width=35, show="•")
        self.password_entry.pack(fill="x")

        tk.Label(self, text="Select Warehouse (Short Code) — type to search").pack(anchor="w", pady=(10, 0))
        self.wh_var = tk.StringVar()
        self.wh_combo = ttk.Combobox(self, textvariable=self.wh_var, width=35)
        self.wh_combo['values'] = WH_SHORT_CODES
        self.wh_combo.pack(fill="x")

        # --- Autocomplete suggestion list (hidden initially) ---
        self.suggest_frame = tk.Frame(self)
        self.suggest_list = tk.Listbox(self.suggest_frame, height=6)
        self.suggest_scroll = tk.Scrollbar(self.suggest_frame, command=self.suggest_list.yview)
        self.suggest_list.configure(yscrollcommand=self.suggest_scroll.set)
        self.suggest_list.pack(side="left", fill="x", expand=True)
        self.suggest_scroll.pack(side="right", fill="y")
        self.suggest_frame.pack_forget()

        # Full name label sits below suggestions
        self.wh_full_lbl = tk.Label(self, text="Full Name: —", fg="#666")
        self.wh_full_lbl.pack(anchor="w", pady=(6, 0))

        # Status line
        self.status_lbl = tk.Label(self, text="", fg="#007BFF", font=("Arial", 10, "bold"))
        self.status_lbl.pack(anchor="w", pady=(10, 6))

        self.submit_btn = tk.Button(self, text="Submit", command=self._submit,
                                    bg="#007BFF", fg="white", font=("Arial", 11, "bold"))
        self.submit_btn.pack(pady=(6, 2))

        # OTP section (hidden initially)
        self.otp_frame = tk.Frame(self, relief=tk.GROOVE, borderwidth=1, padx=10, pady=10)
        tk.Label(self.otp_frame, text="2nd Factor Authentication", font=("Arial", 11, "bold")).pack(anchor="w")
        tk.Label(self.otp_frame, text="Choose an option to receive OTP", fg="#555").pack(anchor="w")

        method_row = tk.Frame(self.otp_frame)
        method_row.pack(anchor="w", pady=(6, 4))
        tk.Radiobutton(method_row, text="SMS", variable=self.mfa_method_var, value="SMS",
                       command=self._on_mfa_method_changed).pack(side="left", padx=(0, 8))
        tk.Radiobutton(method_row, text="EMAIL", variable=self.mfa_method_var, value="EMAIL",
                       command=self._on_mfa_method_changed).pack(side="left")

        self.otp_entry = tk.Entry(self.otp_frame, width=20)
        self.otp_entry.pack(anchor="w", pady=(6, 4))
        self.remember_var = tk.BooleanVar(value=True)
        tk.Checkbutton(self.otp_frame, text="Remember this browser for 30 days",
                       variable=self.remember_var).pack(anchor="w")
        button_row = tk.Frame(self.otp_frame)
        button_row.pack(anchor="w", pady=(8, 0))
        self.btn_verify_otp = tk.Button(button_row, text="Verify OTP", bg="#4CAF50", fg="white",
                                        command=self._verify_otp)
        self.btn_verify_otp.pack(side="left", padx=(0, 8))
        self.btn_resend_otp = tk.Button(button_row, text="Resend OTP", command=self._resend_otp)
        self.btn_resend_otp.pack(side="left")
        self.otp_frame.pack_forget()

        # Bindings for autocomplete & suggestions
        self.wh_combo.bind("<KeyRelease>", self._on_wh_typed)
        self.wh_combo.bind("<FocusIn>", lambda e: self._on_wh_typed())
        self.wh_combo.bind("<FocusOut>", self._maybe_hide_suggestions)
        self.suggest_list.bind("<FocusOut>", self._maybe_hide_suggestions)
        self.suggest_list.bind("<ButtonRelease-1>", self._on_suggestion_click)
        self.suggest_list.bind("<Return>", self._on_suggestion_enter)
        self.suggest_list.bind("<Escape>", lambda e: self._hide_suggestions())
        self.suggest_list.bind("<Up>", self._on_suggestion_move)
        self.suggest_list.bind("<Down>", self._on_suggestion_move)

    def set_status(self, text: str, is_error: bool = False):
        self.status_lbl.config(text=text, fg=("#F44336" if is_error else "#007BFF"))

    # --- Autocomplete helpers ---
    def _filter_codes(self, typed: str):
        t = (typed or "").strip().lower()
        if not t:
            return WH_SHORT_CODES[:]
        return [c for c in WH_SHORT_CODES if t in c.lower()]

    def _show_suggestions(self, values):
        if not values:
            self._hide_suggestions()
            return
        self.suggest_list.delete(0, tk.END)
        for v in values:
            self.suggest_list.insert(tk.END, v)
        try:
            self.suggest_frame.pack_forget()
        except Exception:
            pass
        self.suggest_frame.pack(fill="x", padx=0, pady=(0, 6), before=self.wh_full_lbl)
        try:
            self.suggest_list.selection_clear(0, tk.END)
            self.suggest_list.selection_set(0)
            self.suggest_list.activate(0)
        except Exception:
            pass

    def _hide_suggestions(self):
        try:
            self.suggest_frame.pack_forget()
        except Exception:
            pass

    def _maybe_hide_suggestions(self, event=None):
        try:
            current = self.focus_get()
        except Exception:
            return
        if current not in (self.wh_combo, self.suggest_list):
            self._hide_suggestions()

    def _on_wh_typed(self, event=None):
        typed = self.wh_var.get().strip()
        matches = self._filter_codes(typed)
        self.wh_combo['values'] = matches
        full = WH_MAP_SHORT_TO_FULL.get(typed, "—")
        self.wh_full_lbl.config(text=f"Full Name: {full}")
        self._show_suggestions(matches)
        if event and getattr(event, 'keysym', '') == 'Down' and matches:
            self.suggest_list.focus_set()

    def _on_suggestion_click(self, event=None):
        try:
            sel = self.suggest_list.curselection()
            if not sel:
                return
            value = self.suggest_list.get(sel[0])
        except Exception:
            return
        self._apply_suggestion(value)

    def _on_suggestion_enter(self, event=None):
        try:
            sel = self.suggest_list.curselection()
            if not sel:
                return
            value = self.suggest_list.get(sel[0])
        except Exception:
            return
        self._apply_suggestion(value)

    def _on_suggestion_move(self, event):
        try:
            cur = self.suggest_list.curselection()
            cur_idx = cur[0] if cur else 0
            delta = -1 if event.keysym == 'Up' else 1
            new_idx = max(0, min(self.suggest_list.size() - 1, cur_idx + delta))
            self.suggest_list.selection_clear(0, tk.END)
            self.suggest_list.selection_set(new_idx)
            self.suggest_list.activate(new_idx)
            self.suggest_list.see(new_idx)
        except Exception:
            pass

    def _apply_suggestion(self, value: str):
        try:
            self.wh_var.set(value)
            full = WH_MAP_SHORT_TO_FULL.get(value, "—")
            self.wh_full_lbl.config(text=f"Full Name: {full}")
            self._hide_suggestions()
            self.wh_combo.icursor(tk.END)
            self.wh_combo.focus_set()
        except Exception:
            pass

    # --- MFA method and OTP interactions ---
    def _on_mfa_method_changed(self):
        if not self.driver_ref:
            return
        method = (self.mfa_method_var.get() or "").upper()
        try:
            if method == "SMS":
                ok_opt = click_sms_option(self.driver_ref)
                ok_send = click_send_sms(self.driver_ref) if ok_opt or True else False
                self.set_status("SMS selected. Sending OTP…" if ok_send else "Failed to send SMS OTP.", is_error=not ok_send)
            else:
                ok_opt = click_email_option(self.driver_ref)
                ok_send = click_send_email(self.driver_ref) if ok_opt or True else False
                self.set_status("EMAIL selected. Sending OTP…" if ok_send else "Failed to send EMAIL OTP.", is_error=not ok_send)
        except Exception as e:
            self.set_status(f"MFA option error: {e}", is_error=True)

    def show_otp_section(self, driver):
        self.driver_ref = driver
        self.otp_entry.delete(0, tk.END)
        self.otp_frame.pack(fill="x", pady=(10, 0))
        self._on_mfa_method_changed()

    def hide_otp_section(self):
        try:
            self.otp_frame.pack_forget()
        except Exception:
            pass

    def _submit(self):
        username = self.username_entry.get().strip()
        password = self.password_entry.get().strip()
        wh_short = self.wh_var.get().strip()
        if not username or not password or not wh_short:
            self.set_status("Please enter Username, Password, and select Warehouse.", is_error=True)
            return
        wh_full = WH_MAP_SHORT_TO_FULL.get(wh_short)
        if not wh_full:
            self.set_status(f"Selected WH code '{wh_short}' not found.", is_error=True)
            return
        self._hide_suggestions()
        global SELECTED_WH_SHORT, SELECTED_WH_FULL
        SELECTED_WH_SHORT = wh_short
        SELECTED_WH_FULL = wh_full
        self.set_status("Logging in…")
        self.on_submit(username, password)

    def _verify_otp(self):
        if not self.driver_ref:
            self.set_status("Driver not ready for OTP.", is_error=True)
            return
        otp_text = self.otp_entry.get().strip()
        if not otp_text:
            self.set_status("Please enter the OTP.", is_error=True)
            return
        method = (self.mfa_method_var.get() or "").upper()
        if method == "SMS":
            ok = perform_sms_mfa_submit(self.driver_ref, otp_text, self.remember_var.get())
        else:
            ok = perform_email_mfa_submit(self.driver_ref, otp_text, self.remember_var.get())
        if not ok:
            self.set_status("OTP submit failed. Check the code and try again.", is_error=True)
            return
        try:
            WebDriverWait(self.driver_ref, 6).until(lambda d: not on_mfa_page(d))
        except TimeoutException:
            time.sleep(0.25)
        if on_mfa_page(self.driver_ref):
            self.set_status("OTP verification did not complete yet. If needed, try Resend.", is_error=True)
        else:
            self.set_status("OTP verified. Proceeding…", is_error=False)
            self.hide_otp_section()

    def _resend_otp(self):
        if not self.driver_ref:
            self.set_status("Driver not ready to resend OTP.", is_error=True)
            return
        method = (self.mfa_method_var.get() or "").upper()
        sent = click_send_sms(self.driver_ref) if method == "SMS" else click_send_email(self.driver_ref)
        if sent:
            self.set_status("OTP resent. Enter the new code.", is_error=False)
        else:
            self.set_status("Unable to resend OTP. Try again.", is_error=True)

class MainFrame(tk.Frame):
    def __init__(self, master):
        super().__init__(master, padx=20, pady=20)
        self._last_completed_stn = None
        self._auto_clear_pending = False
        self._build()

    def _build(self):
        """Two-page UX: menu (3 buttons) + STN pages without a heading. Back buttons bottom-right with a little margin from the bottom."""
        global GUI_MSG_TOTE
        # --- Top bar (Logout) ---
        top_bar = tk.Frame(self)
        top_bar.pack(fill="x")
        logout_btn = tk.Button(top_bar, text="Logout", command=self._logout,
                               bg="#f44336", fg="white", font=("Arial", 10), padx=8, pady=2)
        logout_btn.pack(side="right")

        self.status_lbl = tk.Label(self, text="", fg="#007BFF", font=("Arial", 10, "bold"))
        self.status_lbl.pack(anchor="w")

        # Top navigation row: Back button under the Settings bar (left side)
        self._nav_row = tk.Frame(self)
        self._nav_row.pack(fill="x", pady=(6, 6))
        self.back_btn_top = tk.Button(
            self._nav_row, text="◀ Back", command=self.show_menu,
            bg="#E5F3FF", fg="#0A5CC2", padx=12, pady=6
        )
        # Default hidden on Menu; shown on Box/Tote pages
        self.back_btn_top.pack_forget()

        # Containers (stacked views)
        self.menu_view = tk.Frame(self, padx=16, pady=10)
        self.box_view  = tk.Frame(self, padx=16, pady=10)
        self.tms_view = tk.Frame(self, padx=16, pady=10)
        self.tc_view  = tk.Frame(self, padx=16, pady=10)
        self.stn_tl_view = tk.Frame(self, padx=16, pady=10)
        self.go_pick_view = tk.Frame(self, padx=16, pady=10)
# ---------------- Home / Menu (no heading) ----------------
        def make_menu_button(parent, text, cmd):
            btn = tk.Button(parent, text=text, command=cmd,
                            font=("Arial", 14, "bold"), bg=PACK_DEFAULT_BG, fg="white",
                            activebackground="#38BDF8", relief="flat", padx=18, pady=12)
            btn.pack(fill="x", pady=10)
            return btn
        make_menu_button(self.menu_view, "STN upload & TL assign", lambda: self.show_stn_tl_view())
        make_menu_button(self.menu_view, "GO PICK", lambda: self.show_go_pick_view())
        make_menu_button(self.menu_view, "PACK", lambda: self.start_pack_flow())
        make_menu_button(self.menu_view, "PRINT BOX-ID", lambda: self.show_box_view())
        make_menu_button(self.menu_view, "TMS", lambda: self.show_tms_view())
        make_menu_button(self.menu_view, "TC", lambda: self.show_tc_view())


        # ---------------- STN upload & TL assign view ----------------
        tk.Label(self.stn_tl_view, text="STN Upload & TL Assign", font=("Arial", 14, "bold")).pack(anchor="w", pady=(4, 10))
        self._stn_tl_csv_path = tk.StringVar(value="")
        upload_row = tk.Frame(self.stn_tl_view)
        upload_row.pack(anchor="w", fill="x", pady=(0, 6))
        tk.Label(upload_row, text="Upload STN .csv:", font=("Arial", 12)).pack(side="left")
        tk.Button(upload_row, text="Choose CSV", command=self._stn_tl_choose_csv, bg=PACK_DEFAULT_BG, fg="white").pack(side="left", padx=(8, 0))
        self._stn_tl_file_lbl = tk.Label(self.stn_tl_view, text="No file selected", font=("Arial", 10), fg="#666")
        self._stn_tl_file_lbl.pack(anchor="w", pady=(2, 8))
        self._btn_assign_tl = tk.Button(self.stn_tl_view, text="Assign TL", state=tk.DISABLED,
                                        command=self._stn_tl_assign_open_link, font=("Arial", 14, "bold"),
                                        bg=PACK_DEFAULT_BG, fg="white", activebackground="#38BDF8", padx=18, pady=10)
        self._btn_assign_tl.pack(anchor="w", pady=(0, 6))

        # ---------------- STN view for PRINT BOX-ID ----------------
        tk.Label(self.box_view, text="Enter STN Number:", font=("Arial", 12)).pack(anchor="w", pady=(6, 4))
        self.stn_entry_box = tk.Entry(self.box_view, width=32, font=("Arial", 12))
        self.stn_entry_box.pack(fill="x")
        self.stn_entry_box.bind("<FocusIn>", lambda e: self._on_any_field_focus())
        self.last_stn_box_lbl = tk.Label(self.box_view, text="Last STN (Print): —", font=("Arial", 9), fg="#666")
        self.last_stn_box_lbl.pack(anchor="w", pady=(4, 10))
                # --- Submit row for PRINT BOX-ID (button + loader side-by-side) ---
        self.box_submit_row = tk.Frame(self.box_view)
        self.box_submit_row.pack(anchor="w", pady=(0, 4))
        self.btn_box_creation = tk.Button(
            self.box_submit_row, text="Submit",
            command=lambda: self._start_automation(self.stn_entry_box.get(), flow="BOX_CREATION"),
            font=("Arial", 14, "bold"), bg=PACK_DEFAULT_BG, fg="white",
            activebackground="#38BDF8", padx=20, pady=10
        )
        self.btn_box_creation.pack(side="left")
        self.box_circle = CircleProgressIndicator(self.box_submit_row)
        self.box_circle.attach_next_to_button(self.btn_box_creation)
        # Back button moved to top navigation row (see back_btn_top)

        # Default landing after packing container
        self.show_menu()
    def show_menu(self):
        for f in (self.box_view, getattr(self,'tote_view', None), getattr(self,'stn_tl_view', None), getattr(self,'go_pick_view', None)):
            try: f.pack_forget()
            except Exception: pass
        self.menu_view.pack(fill="both", expand=True)
        self.set_status("")
        try: self.back_btn_top.pack_forget()
        except Exception: pass

    def show_box_view(self):
        try: self.menu_view.pack_forget()
        except Exception: pass
        try: self.tote_view.pack_forget()
        except Exception: pass
        self.box_view.pack(fill="both", expand=True)
        self.stn_entry_box.focus_set()
        # Show top Back when entering Box page (yellow area)
        try:
            if not self.back_btn_top.winfo_ismapped():
                self.back_btn_top.pack(side="left")
        except Exception: pass



    def show_stn_tl_view(self):
        try: self.menu_view.pack_forget()
        except Exception: pass
        try: self.box_view.pack_forget()
        except Exception: pass
        try: self.tote_view.pack_forget()
        except Exception: pass
        try: self.tms_view.pack_forget()
        except Exception: pass
        try: self.tc_view.pack_forget()
        except Exception: pass
        try: self.stn_tl_view.pack(fill="both", expand=True)
        except Exception: pass
        try:
            if not self.back_btn_top.winfo_ismapped():
                self.back_btn_top.pack(side="left")
        except Exception: pass

    def _stn_tl_choose_csv(self):
        try:
            from tkinter import filedialog
            path = filedialog.askopenfilename(title="Select STN CSV",
                                              filetypes=[("CSV files", "*.csv"), ("All files", "*.*")])
            if path:
                self._stn_tl_csv_path.set(path)
                import os
                self._stn_tl_file_lbl.config(text=f"Selected: {os.path.basename(path)}")
                try: self._btn_assign_tl.config(state=tk.NORMAL)
                except Exception: pass
            else:
                self._stn_tl_file_lbl.config(text="No file selected")
                try: self._btn_assign_tl.config(state=tk.DISABLED)
                except Exception: pass
        except Exception as e:
            try: self.set_status(f"File chooser error: {e}", is_error=True)
            except Exception: pass


    def _stn_tl_assign_open_link(self):
        """
        SINGLE-TAB flow (robust):
         - For each STN in the uploaded CSV:
           1) Open TL Search (same tab)
           2) Enter External ID -> Find Transfer List
           3) Select All -> set Assigned To -> Assign TL
           4) Re-click Find to refresh
           5) For each TL link: open IN SAME TAB (robust open), scrape items, store to SQLite
           6) Back to results and continue
        """
        try:
            from selenium.common.exceptions import TimeoutException
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            import csv, time

            tl_search_url = "http://10.24.1.53/transfer_list/search_transfer_list"
            home_url = HOME_URL if 'HOME_URL' in globals() else "http://10.24.1.53/home"

            global GUI_ACTIVE_DRIVER, SELECTED_WH_FULL, SELECTED_WH_SHORT
            drv = GUI_ACTIVE_DRIVER
            if drv is None:
                self.set_status("No active browser session. Please login first.", is_error=True)
                return

            # Resolve WH
            wh_full = SELECTED_WH_FULL
            try:
                if (not wh_full) and SELECTED_WH_SHORT:
                    wh_full = WH_MAP_SHORT_TO_FULL.get(SELECTED_WH_SHORT)
            except Exception:
                pass
            if not wh_full:
                self.set_status("No warehouse selected. Please choose a warehouse first.", is_error=True)
                return

            # Read STNs from CSV
            csv_path = self._stn_tl_csv_path.get()
            if not csv_path or not Path(csv_path).exists():
                self.set_status("Please choose a valid STN CSV first.", is_error=True)
                return
            stns = []
            try:
                with open(csv_path, 'r', newline='', encoding='utf-8-sig') as f:
                    r = csv.reader(f)
                    for row in r:
                        if not row: continue
                        v = (row[0] or '').replace('\ufeff','').strip()
                        if not v or v.lower() == 'stn':
                            continue
                        stns.append(v)
            except Exception as e:
                self.set_status(f"CSV read error: {e}", is_error=True)
                return
            # Dedupe (order-preserving)
            seen, uniq = set(), []
            for s in stns:
                if s and s not in seen:
                    seen.add(s); uniq.append(s)
            stns = uniq
            if not stns:
                self.set_status("No STNs found in CSV.", is_error=True)
                return

            # Username to assign
            try:
                assign_user = (self.master.login_frame.username_entry.get() or '').strip()
            except Exception:
                assign_user = ''
            if not assign_user:
                self.set_status("No username available to assign TL.", is_error=True)
                return

            wait = build_wait(drv, TIMEOUT_MED)

            # Ensure HOME + warehouse once
            if not navigate_if_needed(drv, home_url, wait):
                self.set_status("Could not open HOME.", is_error=True); return
            if not ensure_home_loaded(drv, wait, retries=5, sleep_between=1.0):
                self.set_status("HOME not ready.", is_error=True); return
            if not select_warehouse_by_name(drv, wait, wh_full):
                self.set_status(f"Warehouse not selectable: {wh_full}", is_error=True); return

            processed_count = 0
            stored_items = 0

            def _wait_for_tl_links(timeout=TIMEOUT_MED):
                """Wait until TL anchors appear OR table renders; returns anchors list (may be empty)."""
                # First wait for page readiness
                try:
                    WebDriverWait(drv, timeout).until(lambda d: d.execute_script('return document.readyState') == 'complete')
                except Exception:
                    pass
                # Then try to wait for anchors a bit
                end = time.time() + timeout
                anchors = []
                while time.time() < end:
                    anchors = drv.find_elements(By.XPATH, "//a[starts-with(normalize-space(text()), 'TL') and (contains(@href, '/transfer_list/TL') or @href='javascript:void(0);' or string-length(@href)=0)]")
                    if anchors:
                        return anchors
                    # check table presence to avoid tight loop
                    try:
                        if drv.find_elements(By.XPATH, "//table//tr"):  # some rows exist
                            pass
                    except Exception:
                        pass
                    time.sleep(0.25)
                return anchors

            def _open_tl_same_tab(anchor_el):
                """Navigate to the TL details page in the SAME TAB. Multiple strategies."""
                tl_text = ''
                tl_href = ''
                try:
                    tl_text = (anchor_el.text or anchor_el.get_attribute('innerText') or '').strip()
                except Exception:
                    pass
                try:
                    tl_href = anchor_el.get_attribute('href') or anchor_el.get_attribute('data-href') or ''
                except Exception:
                    tl_href = ''
                # Normalize TL id from text
                tl_id = ''
                try:
                    import re as _re
                    m = _re.search(r"\bTL\d+\b", tl_text or '', flags=_re.I)
                    if m:
                        tl_id = m.group(0).upper()
                except Exception:
                    pass
                # Strategy 1: direct href navigation if link contains TL path
                try:
                    if tl_href and '/transfer_list/TL' in tl_href:
                        drv.get(tl_href)
                        _ob2_wait_ready(drv, 25)
                        return tl_id
                except Exception:
                    pass
                # Strategy 2: JS click to trigger any inline navigation
                try:
                    drv.execute_script("arguments[0].scrollIntoView({block:'center'});", anchor_el)
                except Exception:
                    pass
                try:
                    drv.execute_script("arguments[0].click();", anchor_el)
                    # wait for TL URL
                    WebDriverWait(drv, TIMEOUT_MED).until(lambda d: '/transfer_list/TL' in (d.current_url or ''))
                    # Remember last known TL id for auto-fill flows
                    try:
                        globals()['_LAST_KNOWN_TL_ID'] = tl_id
                    except Exception:
                        pass
                    _ob2_wait_ready(drv, 25)
                    return tl_id
                except Exception:
                    pass







                # Strategy 3: construct URL from visible TL id as last resort
                try:
                    if tl_id:
                        base = 'http://10.24.1.53'
                        drv.get(f"{base}/transfer_list/{tl_id}")
                        _ob2_wait_ready(drv, 25)
                        return tl_id
                except Exception:
                    pass
                return tl_id  # may be ''

            for idx, stn in enumerate(stns, start=1):
                stn = (stn or '').replace('\ufeff','').strip()
                try:
                    self.set_status(f"[{idx}/{len(stns)}] Processing STN: {stn}")

                    # 1) TL search page
                    if not navigate_if_needed(drv, tl_search_url, wait):
                        self.set_status("TL Search not reachable.", is_error=True)
                        continue

                    # 2) External ID
                    try:
                        external_input = WebDriverWait(drv, TIMEOUT_LONG).until(
                            EC.presence_of_element_located((By.NAME, "filters[external_id]"))
                        )
                        drv.execute_script("arguments[0].scrollIntoView({block:'center'});", external_input)
                        try:
                            external_input.clear()
                        except Exception:
                            pass
                        external_input.send_keys((stn or '').replace('\ufeff','').strip())
                    except TimeoutException:
                        self.set_status("External ID input not found.", is_error=True)
                        continue

                    # 3) Find Transfer List
                    find_btn = None
                    for by, sel in [
                        (By.CSS_SELECTOR, "input.uiButton[value*='Find Transfer List']"),
                        (By.XPATH, "//input[@type='button' and contains(normalize-space(@value),'Find Transfer List')]")
                    ]:
                        try:
                            find_btn = WebDriverWait(drv, TIMEOUT_SHORT).until(EC.element_to_be_clickable((by, sel)))
                            break
                        except TimeoutException:
                            continue
                    if not find_btn:
                        self.set_status("Find Transfer List button not found.", is_error=True)
                        continue
                    drv.execute_script("arguments[0].click();", find_btn)
                    handle_popups(drv)

                    # Wait results (select_all present or at least table render)
                    try:
                        WebDriverWait(drv, TIMEOUT_LONG).until(EC.presence_of_element_located((By.ID, 'select_all')))
                    except TimeoutException:
                        pass

                    # 4) Select All
                    try:
                        select_all = drv.find_element(By.ID, 'select_all')
                        if not select_all.is_selected():
                            drv.execute_script("arguments[0].scrollIntoView({block:'center'});", select_all)
                            select_all.click()
                    except Exception:
                        # fallback
                        try:
                            che = drv.find_element(By.CSS_SELECTOR, "input.fk-table-selectall#select_all")
                            if not che.is_selected():
                                che.click()
                        except Exception:
                            pass

                    # 5) Assign user
                    try:
                        assn = drv.find_element(By.ID, 'assigned_to')
                        _set_value_fast(drv, assn, assign_user)
                    except Exception:
                        pass

                    # 6) Assign TL
                    assign_btn = None
                    for by, sel in [
                        (By.CSS_SELECTOR, "input.uiButton.lmargin5[value*='Assign TL']"),
                        (By.XPATH, "//input[@type='button' and contains(@value,'Assign TL')]")
                    ]:
                        try:
                            assign_btn = WebDriverWait(drv, TIMEOUT_SHORT).until(EC.element_to_be_clickable((by, sel)))
                            break
                        except TimeoutException:
                            continue
                    if assign_btn:
                        try:
                            drv.execute_script("arguments[0].click();", assign_btn)
                            handle_popups(drv)
                        except Exception:
                            pass
                        # Wait for overlay/message to disappear
                        try:
                            WebDriverWait(drv, 20).until(
                                lambda d: not d.find_elements(By.CSS_SELECTOR, '.modal-backdrop, .mdc-dialog--open, .fk-overlay, .ui-loader, .loading')
                            )
                        except Exception:
                            pass

                    # 7) Refresh results via Find again (ensures assigned filter applied)
                    try:
                        if find_btn:
                            drv.execute_script("arguments[0].scrollIntoView({block:'center'});", find_btn)
                            drv.execute_script("arguments[0].click();", find_btn)
                            handle_popups(drv)
                    except Exception:
                        pass

                    # 8) Wait & collect TL anchors (robust)
                    anchors = _wait_for_tl_links(timeout=TIMEOUT_LONG)
                    if not anchors:
                        # try a quick retry once
                        try:
                            if find_btn:
                                drv.execute_script("arguments[0].click();", find_btn)
                                handle_popups(drv)
                        except Exception:
                            pass
                        anchors = _wait_for_tl_links(timeout=TIMEOUT_MED)

                    # Build stable list (text, element) at this time to avoid stale refs later
                    tl_elems = []
                    for a in anchors:
                        try:
                            txt = (a.text or a.get_attribute('innerText') or '').strip()
                            tl_elems.append((txt, a))
                        except Exception:
                            continue

                    if not tl_elems:
                        self.set_status(f"No TL found for STN {stn}.", is_error=False)
                        # === TRACK REQUEST: open and scrape Source/Destination for this STN ===
                        try:
                            if navigate_if_needed(drv, OUTBOUND_TRACK_URL, wait):
                                if _ob2_track_request_for_stn(drv, stn):
                                    src_val, dst_val = scrape_track_request_source_dest(drv, timeout=TIMEOUT_LONG)
                                    if (src_val or dst_val):
                                        try:
                                            stn_final_update_source_destination(stn, src_val, dst_val)
                                            self.set_status(f"STN {stn}: updated Source/Destination from Track Request.", is_error=False)
                                        except Exception as _e_ud:
                                            print("[STN-Final][WARN] update Source/Dest failed:", _e_ud)
                        except Exception as _e_tr:
                            print("[TrackRequest][WARN] STN", stn, "track scrape failed:", _e_tr)
                        processed_count += 1
                        continue

                    # NEW: Pre-store STN→TL map BEFORE scraping
                    try:
                        import re as _pre_re
                        for _txt, _ in tl_elems:
                            _m = _pre_re.search(r'\bTL\d+\b', (_txt or ''), flags=_pre_re.I)
                            if _m:
                                _db_put_tl_map(_m.group(0).upper(), stn=stn)
                    except Exception:
                        pass

                    # 9) For each TL: open same tab -> scrape -> store -> back
                    total_for_stn = 0
                    for tl_text, a in tl_elems:
                        try:
                            tl_id = _open_tl_same_tab(a)  # navigates
                        except Exception:
                            tl_id = ''
                        # If still not on TL page, skip
                        try:
                            cur_url = drv.current_url or ''
                        except Exception:
                            cur_url = ''
                        if '/transfer_list/TL' not in cur_url:
                            continue
                        # Scrape & store
                        try:
                            items = _ob2_extract_item_rows(drv) or []
                            # If we failed to infer ID from link text, parse from URL
                            if not tl_id:
                                try:
                                    import re as _re
                                    m = _re.search(r"/transfer_list/(TL\d+)", cur_url, flags=_re.I)
                                    if m: tl_id = m.group(1).upper()
                                except Exception:
                                    tl_id = ''
                            payload = []
                            for (wid, fsn, title, category, qty, shelf) in items:
                                payload.append((tl_id or tl_text, wid, fsn, title, category, qty, shelf))
                            _ob2_insert_rows(_OB2_DB_PATH, payload)
                            # --- Also store to STN-Final DB (split Qty into per-row=1)
                            try:
                                if items:
                                    stn_final_insert_item_rows(assign_user, stn, tl_id or (tl_text if 'tl_text' in locals() else ''), items)
                            except Exception as _sf_e:
                                print('[STN-Final][WARN] could not insert rows:', _sf_e)
                            total_for_stn += len(payload)
                            stored_items += len(payload)
                            # NEW: Attach STN to this TL's scraped rows immediately after insert
                            try:
                                if tl_id:
                                    _db_update_scrape_with_stn_source_dest(tl_id, stn=(stn or '').replace('\ufeff','').strip())
                            except Exception:
                                pass
                        except Exception:
                            pass
                        # Back to results page for the next TL
                        try:
                            drv.back(); _ob2_wait_ready(drv, 15)
                            # Wait for TL list again before moving on
                            _ = _wait_for_tl_links(timeout=TIMEOUT_MED)
                        except Exception:
                            # If history is broken, rebuild the results by re-searching this STN
                            try:
                                if navigate_if_needed(drv, tl_search_url, wait):
                                    ext = WebDriverWait(drv, TIMEOUT_MED).until(EC.presence_of_element_located((By.NAME, "filters[external_id]")))
                                    ext.clear(); ext.send_keys(stn)
                                    fb = WebDriverWait(drv, TIMEOUT_SHORT).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input.uiButton[value*='Find Transfer List']")))
                                    drv.execute_script("arguments[0].click();", fb)
                                    _ = _wait_for_tl_links(timeout=TIMEOUT_MED)
                            except Exception:
                                pass

                    
                    # === TRACK REQUEST: open and scrape Source/Destination for this STN ===
                    try:
                        if navigate_if_needed(drv, OUTBOUND_TRACK_URL, wait):
                            if _ob2_track_request_for_stn(drv, stn):
                                src_val, dst_val = scrape_track_request_source_dest(drv, timeout=TIMEOUT_LONG)
                                if (src_val or dst_val):
                                    try:
                                        stn_final_update_source_destination(stn, src_val, dst_val)
                                        self.set_status(f"STN {stn}: updated Source/Destination from Track Request.", is_error=False)
                                    except Exception as _e_ud:
                                        print("[STN-Final][WARN] update Source/Dest failed:", _e_ud)
                    except Exception as _e_tr:
                        print("[TrackRequest][WARN] STN", stn, "track scrape failed:", _e_tr)
                    processed_count += 1
                    self.set_status(f"[{idx}/{len(stns)}] STN {stn}: stored {total_for_stn} item(s)")
                except Exception as _err_stn:
                    self.set_status(f"STN {stn} failed: {_err_stn}", is_error=True)
                    continue

            self.set_status(f"Completed. STNs processed: {processed_count}; items stored: {stored_items}")
            # === After all STNs processed: open Box Creation, sum Qty across CSV STNs, generate, scrape and assign Box-Ids ===
            try:
                csv_stns = stns[:] if 'stns' in locals() else []
                if csv_stns:
                    total_qty = stn_final_count_rows_for_stns(csv_stns)
                    self.set_status(f"Total Qty across CSV STNs: {total_qty}")
                    ok_gen = open_box_creation_and_generate(drv, wait, total_qty)
                    if ok_gen:
                        box_ids = scrape_generated_box_ids(drv, timeout=TIMEOUT_LONG)
                        if box_ids:
                            assigned = stn_final_assign_box_ids_to_stns(box_ids, csv_stns)
                            self.set_status(f"Assigned {assigned} Box-Id(s) across current CSV STNs.")
                        else:
                            self.set_status("No Box-Ids scraped after Generate.", is_error=True)
                    else:
                        self.set_status("Box Creation generate failed or table not rendered.", is_error=True)
                else:
                    self.set_status("No STNs in CSV context to create boxes.", is_error=True)
            except Exception as _e_boxes:
                print('[BOX-CREATE][ERROR] Post-CSV box workflow failed:', _e_boxes)

        except Exception as e:
            try:
                self.set_status(f"Unexpected error: {e}", is_error=True)
            except Exception:
                pass

    def _logout(self):
        self.master.restart_browser_for_login()
        self.reset_controls(clear_fields=True)

    def reset_controls(self, clear_fields: bool):
        self.btn_box_creation.config(bg=PRINT_DEFAULT_BG, state=tk.NORMAL)
        self.box_circle.reset()
        if clear_fields:
            try:
                self.stn_entry_box.delete(0, tk.END)
            except Exception:
                pass
        self._last_completed_stn = None
        self._auto_clear_pending = False
        self.last_stn_box_lbl.config(text="Last STN (Print): —")
        gui_clear_msgs()
        self.status_lbl.config(text="", fg="#007BFF")

    def _on_any_field_focus(self):
        if self._auto_clear_pending and self._last_completed_stn:
            cur_box = (self.stn_entry_box.get() or "").strip()
            cur_tote = ''
            if cur_box == self._last_completed_stn or cur_tote == self._last_completed_stn:
                try:
                    self.stn_entry_box.delete(0, tk.END)
                except Exception:
                    pass
                self._auto_clear_pending = False

    def set_status(self, text: str, is_error: bool = False):
        self.status_lbl.config(text=text, fg=("#F44336" if is_error else "#007BFF"))
    def _start_automation(self, stn_number, flow):
        stn_number = stn_number.strip()
        if not stn_number:
            self.set_status("Please enter an STN Number.", is_error=True)
            return
        if GUI_ACTIVE_DRIVER is None:
            self.set_status("Session closed. Please Logout and re-login.", is_error=True)
            return

        gui_clear_msgs(flow=flow)

        if flow == "BOX_CREATION":
            self.last_stn_box_lbl.config(text=f"Last STN (Print): {stn_number}")

        button = self.btn_box_creation
        indicator = self.box_circle

        indicator.reset()
        indicator.start("Starting…")
        # Disable the Box STN entry while loading is active
        entry_widget = self.stn_entry_box
        try:
            entry_widget.config(state=tk.DISABLED)
        except Exception:
            pass
        original_bg = PRINT_DEFAULT_BG

        def run_in_thread():
            button.config(bg="#4CAF50", state=tk.DISABLED)
            success = False
            try:
                success = self._run_automation_flow(stn_number, flow, indicator)
            finally:
                # Stop progress ring (draws 100% if success), then re-enable STN box
                indicator.stop(success, "Done" if success else "Failed")
                try:
                    entry_widget.config(state=tk.NORMAL)
                except Exception:
                    pass
                final_bg = (original_bg if success else ERROR_BG)
                button.config(bg=final_bg, state=tk.NORMAL)
                if success:
                    self._last_completed_stn = stn_number
                    self._auto_clear_pending = True
                    try:
                        self.stn_entry_box.delete(0, tk.END); self.stn_entry_box.insert(0, stn_number)
                    except Exception:
                        pass

        threading.Thread(target=run_in_thread, daemon=True).start()

    def _run_automation_flow(self, stn_number, flow, indicator: CircleProgressIndicator) -> bool:
        global GUI_ROOT, GUI_ACTIVE_DRIVER, SELECTED_WH_FULL
        driver = GUI_ACTIVE_DRIVER
        wait = build_wait(driver, TIMEOUT_MED)
        # watcher disabled per requirement: no background watcher
        # (previously: watcher = PopupWatcher(driver); watcher.start())
        completed = False
        try:
            if flow == "BOX_CREATION":
                indicator.set_status("Opening HOME")
                indicator.set_percent(5)
                if not ensure_home_loaded(driver, wait, retries=5, sleep_between=1.0):
                    indicator.set_status("HOME error (extensions)")
                    indicator.set_percent(0); return False

                indicator.set_status("Selecting warehouse")
                if not select_warehouse_by_name(driver, wait, SELECTED_WH_FULL):
                    indicator.set_status("WH select failed"); indicator.set_percent(15); return False
                indicator.set_percent(40)

                indicator.set_status("Opening Find Request")
                if not navigate_if_needed(driver, FIND_REQUEST_URL, wait):
                    indicator.set_status("Find Req nav failed"); indicator.set_percent(30); return False
                indicator.set_percent(65)

                indicator.set_status("Searching External ID")
                if not fill_external_id_and_find_request(driver, wait, stn_number):
                    indicator.set_status("Search failed"); indicator.set_percent(35); return False
                indicator.set_percent(75)

                indicator.set_status("Scraping results")
                qty_text, wh_text, dest_text = scrape_find_request_details(driver, wait)
                indicator.set_percent(85 if qty_text else 45)
                if not qty_text:
                    indicator.set_status("No rows")
                    return False

                completed = proceed_box_creation_generate_print_and_stamp(
                    driver, wait, qty_text, stn_number, wh_text, dest_text, indicator
                )

            return bool(completed)
        except Exception as overall_e:
            print(f"❌ An overall error occurred: {overall_e}")
            gui_append_msg(f"❌ [Fatal Error] Unexpected error: {overall_e}", flow=flow)
            indicator.set_status("Error")
            indicator.set_percent(60)
            self.set_status(f"Unexpected error: {overall_e}", is_error=True)
            return False
        finally:
            print("🪪 Automation run finished.")
            GUI_ROOT.update_idletasks()

    def start_pack_flow(self):
        """Open Consignment page in the original tab and process all Box-Ids once TLs are complete."""
        global GUI_ACTIVE_DRIVER, SELECTED_WH_FULL
        drv = GUI_ACTIVE_DRIVER
        if drv is None:
            try: self.set_status('No active browser session. Please login first.', is_error=True)
            except Exception: pass
            print('[PACK] No active driver.')
            return
        wait = build_wait(drv, TIMEOUT_MED)
        try:
            self.set_status('PACK: preparing…')
        except Exception: pass
        print('[PACK] Start pack flow.')
        # Ensure HOME & WH
        if not ensure_home_loaded(drv, wait, retries=5, sleep_between=1.0):
            try: self.set_status('PACK: HOME not ready', is_error=True)
            except Exception: pass
            print('[PACK] HOME not ready.'); return
        if not select_warehouse_by_name(drv, wait, SELECTED_WH_FULL):
            try: self.set_status('PACK: Warehouse select failed', is_error=True)
            except Exception: pass
            print('[PACK] Warehouse selection failed.'); return
        if not open_consign_page_in_home_tab(drv, wait):
            try: self.set_status('PACK: Consignment page not reachable', is_error=True)
            except Exception: pass
            print('[PACK] Consignment page not reachable.'); return
        # Run processing
        try:
            self.set_status('PACK: Checking TL status…')
        except Exception: pass
        processed = process_packing_for_completed_tls(drv, wait, weight_default='10', print_qty='1')
        try:
            self.set_status(f'PACK: Completed. Boxes processed: {processed}', is_error=False)
        except Exception: pass
        print(f'[PACK] Flow finished. Boxes processed: {processed}')

# ============================ TMS/TC Integrated Module (inline v12) ============================
import sqlite3 as _tctms_sqlite3
import secrets as _tctms_secrets
import re as _tctms_re
from datetime import datetime as _tctms_datetime, timezone as _tctms_timezone
from tkinter import messagebox as _tctms_messagebox
from tkinter import ttk as _tctms_ttk
import tkinter as _tk

# Warehouse map (short codes)
try:
    _WH_MAP = dict(WH_MAP_SHORT_TO_FULL)
except Exception:
    _WH_MAP = {"del_frn_07L": "Farukhnagar 07 Large Warehouse","del_jjr_wh_f_08l": "3pl_mp8_fur_del_furniture"}
_WAREHOUSE_SHORTS = sorted(list(_WH_MAP.keys()))

DB_PATH = STN_FINAL_DB_PATH
GSTIN_REGEX = _tctms_re.compile(r'^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][1-9A-Z]Z[0-9A-Z]$')
def validate_vehicle_no(val: str):
    val = (val or '').strip()
    if not val:
        return 'Vehicle No is required.'
    if len(val) < 5 or len(val) > 32:
        return 'Vehicle No length must be between 5 and 32 characters.'
    return None

def validate_transporter_name(val: str):
    val = (val or '').strip()
    if not val:
        return 'Transporter Name is required.'
    if len(val) > 128:
        return 'Transporter Name must be 128 characters or fewer.'
    return None

def validate_gstin(val: str):
    val = (val or '').strip().upper()
    if not val:
        return 'GSTIN is required.'
    if len(val) != 15:
        return 'GSTIN must be exactly 15 characters.'
    if not GSTIN_REGEX.match(val):
        return 'GSTIN format appears invalid.'
    return None

def validate_trip_id(val: str):
    val = (val or '').strip().upper()
    if not val:
        return 'Trip-Id is required.'
    if not _tctms_re.fullmatch(r'VRN\d{6}', val):
        return 'Trip-Id must look like VRN123456 (VRN + 6 digits).'
    return None

def get_conn():
    conn = _tctms_sqlite3.connect(DB_PATH)
    try:
        conn.execute('PRAGMA journal_mode=WAL;')
        conn.execute('PRAGMA synchronous=NORMAL;')
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass
    return conn

def _column_exists(conn, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table});")
    cols = [row[1] for row in cur.fetchall()]
    return column in cols

def init_db():
    with get_conn() as conn:
        conn.execute(
            '-- removed table creation (single-table policy)'
        )
        conn.commit()
        for col in ('trip_id','src_wh','dst_wh'):
            if not _column_exists(conn, 'transport_entries', col):
                try:
                    conn.execute(f'ALTER TABLE transport_entries ADD COLUMN {col} TEXT;')
                    conn.commit()
                except Exception:
                    pass
        conn.execute('CREATE INDEX IF NOT EXISTS ix_transport_entries_id_desc ON transport_entries(id DESC);')
        conn.execute('CREATE INDEX IF NOT EXISTS ix_transport_entries_created_at ON transport_entries(created_at);')
        conn.execute('CREATE INDEX IF NOT EXISTS ix_transport_entries_vehicle_no ON transport_entries(UPPER(vehicle_no));')
        conn.execute('CREATE INDEX IF NOT EXISTS ix_transport_entries_src_wh ON transport_entries(UPPER(src_wh));')
        conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS uq_transport_entries_trip_id ON transport_entries(trip_id);')
        conn.commit()

# ---- TC scanned boxes audit (box scans during Trip Check-in) ----
_TC_SCAN_TABLE = 'tc_scanned_boxes'

def _ensure_tc_scan_table():
    try:
        with get_conn() as conn:
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {_TC_SCAN_TABLE} ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                "box_id TEXT NOT NULL,"
                "stn TEXT,"
                "src_wh TEXT,"
                "vehicle_no TEXT,"
                "trip_id TEXT,"
                "matched INTEGER NOT NULL,"
                "note TEXT,"
                "created_at TEXT NOT NULL"
                ");"
            )
            conn.execute(f"CREATE INDEX IF NOT EXISTS ix_{_TC_SCAN_TABLE}_box ON {_TC_SCAN_TABLE}(UPPER(box_id));")
            conn.execute(f"CREATE INDEX IF NOT EXISTS ix_{_TC_SCAN_TABLE}_trip ON {_TC_SCAN_TABLE}(UPPER(trip_id));")
            conn.commit()
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass

def _tc_maybe_add_stn_column():
    """Ensure tc_scanned_boxes has STN column (safe no-op if it already exists)."""
    try:
        with get_conn() as conn:
            cur = conn.execute(f"PRAGMA table_info({_TC_SCAN_TABLE});")
            cols = [r[1] for r in cur.fetchall()]
            if 'stn' not in cols:
                try:
                    conn.execute(f"ALTER TABLE {_TC_SCAN_TABLE} ADD COLUMN stn TEXT;")
                    conn.execute(f"CREATE INDEX IF NOT EXISTS ix_{_TC_SCAN_TABLE}_stn ON {_TC_SCAN_TABLE}(UPPER(stn));")
                    conn.commit()
                except Exception:
                    pass
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass

def record_tc_scan(box_id: str, src_wh: str = None, vehicle_no: str = None, trip_id: str = None, stn: str = None, matched: bool = True, note: str = None):
    box_id = (box_id or '').strip().upper()
    src_wh = (src_wh or '').strip()
    vehicle_no = (vehicle_no or '').strip().upper()
    trip_id = (trip_id or '').strip().upper() or None
    stn = (stn or '').strip().upper() or None
    if not box_id:
        return
    _ensure_tc_scan_table(); _tc_maybe_add_stn_column()
    try:
        with get_conn() as conn:
            conn.execute(
                f"INSERT INTO {_TC_SCAN_TABLE} (box_id, stn, src_wh, vehicle_no, trip_id, matched, note, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
                (box_id, stn, src_wh, vehicle_no, trip_id, 1 if matched else 0, (note or ''), _tctms_datetime.now(_tctms_timezone.utc).isoformat(timespec='seconds'))
            )
            conn.commit()
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass



def _ensure_pack_table():
    try:
        with get_conn() as conn:
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {_PACK_TABLE} ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                "stn TEXT NOT NULL,"
                "tl_id TEXT NOT NULL,"
                "box_id TEXT NOT NULL,"
                "created_at TEXT NOT NULL"
                ");"
            )
            conn.execute(f"CREATE INDEX IF NOT EXISTS ix_{_PACK_TABLE}_stn ON {_PACK_TABLE}(UPPER(stn));")
            conn.execute(f"CREATE INDEX IF NOT EXISTS ix_{_PACK_TABLE}_tl ON {_PACK_TABLE}(UPPER(tl_id));")
            conn.commit()
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass




def record_pack_stn(stn: str, tl_id: str, box_id: str):
    stn = (stn or '').strip(); tl_id = (tl_id or '').strip(); box_id = (box_id or '').strip()
    if not (stn and tl_id and box_id):
        return
    try:
        import sqlite3
        with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(
                f'SELECT ROWID FROM {STN_FINAL_TABLE} '
                f'WHERE UPPER("STN")=UPPER(?) AND UPPER("TL-Id")=UPPER(?) '
                f'AND ("Box-Id" IS NULL OR TRIM("Box-Id")="") ORDER BY ROWID ASC LIMIT 1;',
                (stn, tl_id),
            )
            row = cur.fetchone()
            if row:
                cur.execute(
                    f'UPDATE {STN_FINAL_TABLE} SET "Box-Id"=? WHERE ROWID=?;',
                    (box_id, row[0]),
                )
            conn.commit()
    except Exception as e:
        print('[STN-Final][WARN] record_pack_stn failed:', e)
        return
def _db_get_suggested_wid_for_shelf(shelf: str):
    shelf = (shelf or '').strip()
    if not shelf:
        return None
    try:
        import sqlite3
        with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            sql = (
                f'SELECT "WID" FROM {STN_FINAL_TABLE} '
                f'WHERE UPPER("Shelf")=UPPER(?) AND TRIM(COALESCE("WID","")) <> "" '
                f'ORDER BY ROWID DESC LIMIT 1;'
            )
            cur.execute(sql, (shelf,))
            row = cur.fetchone()
            return (row[0].strip() if row and row[0] else None)
    except Exception as e:
        print('[STN-Final][WARN] wid-for-shelf failed:', e)
        return None




def _db_get_box_for_tl_preferring_shelf(tl: str, shelf: str = None):
    tl = (tl or '').strip()
    shelf = (shelf or '').strip() if shelf else ''
    if tl and shelf:
        try:
            bid = stn_final_next_unpicked_box_for_tl_shelf(tl, shelf)
            if bid:
                return bid
        except Exception:
            pass
    # Fallback: last known box for TL
    try:
        return _db_get_last_box_for_tl(tl)
    except Exception:
        return None
def _db_get_last_box_for_tl(tl: str):
    try:
        if not tl:
            return None
        import sqlite3
        with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            sql = (
                f'SELECT "Box-Id" FROM {STN_FINAL_TABLE} '
                f'WHERE UPPER("TL-Id")=UPPER(?) AND TRIM(COALESCE("Box-Id","")) <> "" '
                f'ORDER BY ROWID DESC LIMIT 1;'
            )
            cur.execute(sql, (tl.strip(),))
            row = cur.fetchone()
            val = (row[0].strip() if row and row[0] else None)
            print('[GO PICK] [STN-Final] latest Box-Id for TL', tl, ':', val)
            return val
    except Exception as e:
        print('[GO PICK] [STN-Final][WARN] read last box failed:', e)
        return None
def generate_unique_vrn(prefix: str = 'VRN', digits: int = 6, max_attempts: int = 100) -> str:
    with get_conn() as conn:
        for _ in range(max_attempts):
            suffix = ''.join(_tctms_secrets.choice('0123456789') for _ in range(digits))
            candidate = f'{prefix}{suffix}'
            cur = conn.execute('SELECT 1 FROM transport_entries WHERE trip_id = ? LIMIT 1;', (candidate,))
            if cur.fetchone() is None:
                return candidate
    raise RuntimeError('Could not generate a unique VRN. Please try again.')

def insert_entry(vehicle_no: str, transporter_name: str, gstin: str, trip_id: str, src_wh: str, dst_wh: str):
    with get_conn() as conn:
        conn.execute(
            'INSERT INTO transport_entries (vehicle_no, transporter_name, gstin, created_at, trip_id, src_wh, dst_wh) VALUES (?, ?, ?, ?, ?, ?, ?);',
            (
                vehicle_no.strip(),
                ' '.join((transporter_name or '').strip().split()),
                (gstin or '').strip().upper(),
                _tctms_datetime.now(_tctms_timezone.utc).isoformat(timespec='seconds'),
                trip_id,
                (src_wh or '').strip(),
                (dst_wh or '').strip(),
            ),
        )
        conn.commit()

def fetch_recent(limit: int = 50):
    with get_conn() as conn:
        cur = conn.execute(
            'SELECT id, trip_id, vehicle_no, transporter_name, gstin, created_at FROM transport_entries ORDER BY id DESC LIMIT ?;',
            (limit,),
        )
        return cur.fetchall()

def fetch_latest_trip_id_by_vehicle(vehicle_no: str, src_wh: str = None):
    vehicle_no = (vehicle_no or '').strip()
    src_wh = (src_wh or '').strip()
    if not vehicle_no:
        return None
    with get_conn() as conn:
        if src_wh:
            cur = conn.execute(
                "SELECT trip_id FROM transport_entries WHERE UPPER(vehicle_no) = UPPER(?) AND UPPER(src_wh)=UPPER(?) AND trip_id IS NOT NULL AND TRIM(trip_id) <> '' ORDER BY id DESC LIMIT 1;",
                (vehicle_no, src_wh),
            )
        else:
            cur = conn.execute(
                "SELECT trip_id FROM transport_entries WHERE UPPER(vehicle_no) = UPPER(?) AND trip_id IS NOT NULL AND TRIM(trip_id) <> '' ORDER BY id DESC LIMIT 1;",
                (vehicle_no,),
            )
        row = cur.fetchone()
        return row[0] if row else None

# ---- Back mixin ----
class _BackMixin:
    def _go_back(self):
        try: self.unbind_all('<Return>')
        except Exception: pass
        try: self.pack_forget()
        except Exception: pass
        def _switch_to_menu(main):
            try:
                for name in ('tms_view','tc_view','box_view','tote_view'):
                    v = getattr(main, name, None)
                    if v:
                        try: v.pack_forget()
                        except Exception: pass
                mv = getattr(main, 'menu_view', None)
                if mv:
                    try: mv.pack(fill='both', expand=True); mv.tkraise()
                    except Exception: pass
                sm = getattr(main, 'show_menu', None)
                if callable(sm): main.after_idle(sm)
                return True
            except Exception:
                return False
        cb = getattr(self, 'on_back_callback', None)
        if callable(cb):
            try:
                p = self
                while True:
                    p = getattr(p, 'master', None)
                    if not p: break
                    if hasattr(p, 'show_menu') and hasattr(p, 'menu_view'):
                        _switch_to_menu(p); return
            except Exception: pass
            try: self.after_idle(cb); return
            except Exception: pass
        try:
            p = self
            while True:
                p = getattr(p, 'master', None)
                if not p: break
                if hasattr(p, 'show_menu') and hasattr(p, 'menu_view'):
                    if _switch_to_menu(p): return
        except Exception: pass
        def _find_menu_owner(w):
            try:
                if hasattr(w, 'show_menu') and hasattr(w, 'menu_view'): return w
                for ch in w.winfo_children():
                    found = _find_menu_owner(ch)
                    if found: return found
            except Exception: return None
            return None
        try:
            top = self.winfo_toplevel(); owner = _find_menu_owner(top)
            if owner: _switch_to_menu(owner); return
        except Exception: pass
        try:
            top = self.winfo_toplevel()
            if hasattr(top, 'show_menu'): top.after_idle(top.show_menu)
        except Exception: pass

# ---- TMS UI ----
class TMSFrame(_BackMixin, _tctms_ttk.Frame):
    def __init__(self, master, on_back_callback=None):
        super().__init__(master, padding=16)
        self.on_back_callback = on_back_callback
        header = _tctms_ttk.Frame(self); header.pack(fill='x', pady=(0, 8))
        _tctms_ttk.Label(header, text='Transport Entry', font=('Segoe UI', 12, 'bold')).pack(side='left')
        _tctms_ttk.Button(header, text='Back', command=self._go_back).pack(side='right')

        form = _tctms_ttk.LabelFrame(self, text='Add New Entry', padding=12); form.pack(fill='x')
        _tctms_ttk.Label(form, text='Source Warehouse:').grid(row=0, column=0, sticky='w', padx=(0,8), pady=6)
        self.src_wh_var = _tk.StringVar(); self.src_wh_combo = make_wh_autocomplete(form, textvariable=self.src_wh_var, width=40)
        self.src_wh_combo.grid(row=0, column=1, sticky='w', pady=6)
        if _WAREHOUSE_SHORTS:
            try: self.src_wh_combo.current(0)
            except Exception: pass
        _tctms_ttk.Label(form, text='Destination Warehouse:').grid(row=1, column=0, sticky='w', padx=(0,8), pady=6)
        self.dst_wh_var = _tk.StringVar(); self.dst_wh_combo = make_wh_autocomplete(form, textvariable=self.dst_wh_var, width=40)
        self.dst_wh_combo.grid(row=1, column=1, sticky='w', pady=6)
        if _WAREHOUSE_SHORTS:
            try: self.dst_wh_combo.current(0)
            except Exception: pass
        _tctms_ttk.Label(form, text='Vehicle No:').grid(row=2, column=0, sticky='w', padx=(0,8), pady=6)
        self.vehicle_var = _tk.StringVar(); self.vehicle_entry = _tctms_ttk.Entry(form, textvariable=self.vehicle_var, width=40)
        self.vehicle_entry.grid(row=2, column=1, sticky='w', pady=6)
        _tctms_ttk.Label(form, text='Transporter Name:').grid(row=3, column=0, sticky='w', padx=(0,8), pady=6)
        self.transporter_var = _tk.StringVar(); self.transporter_entry = _tctms_ttk.Entry(form, textvariable=self.transporter_var, width=40)
        self.transporter_entry.grid(row=3, column=1, sticky='w', pady=6)
        _tctms_ttk.Label(form, text='GSTIN:').grid(row=4, column=0, sticky='w', padx=(0,8), pady=6)
        self.gstin_var = _tk.StringVar(); self.gstin_entry = _tctms_ttk.Entry(form, textvariable=self.gstin_var, width=40)
        self.gstin_entry.grid(row=4, column=1, sticky='w', pady=6)
        btns = _tctms_ttk.Frame(form); btns.grid(row=5, column=0, columnspan=2, pady=(8,0), sticky='w')
        self.create_btn = _tctms_ttk.Button(btns, text='Create Trip-Id', command=self.on_create_trip); self.create_btn.grid(row=0, column=0, padx=(0,8))
        self.clear_btn = _tctms_ttk.Button(btns, text='Clear', command=self.on_clear); self.clear_btn.grid(row=0, column=1, padx=(0,8))

        self.status_var = _tk.StringVar(value='Ready')
        status = _tctms_ttk.Label(self, textvariable=self.status_var, anchor='w')
        # We'll pack status at the end to keep consistency with TC

        table_frame = _tctms_ttk.LabelFrame(self, text='Recent Entries (latest 50)', padding=8)
        table_frame.pack(fill='both', expand=True, pady=(8,0))
        columns = ('id','trip_id','vehicle_no','transporter_name','gstin','created_at')
        self.tree = _tctms_ttk.Treeview(table_frame, columns=columns, show='headings', height=12)
        for col, text2 in zip(columns, ['ID','Trip-Id','Vehicle No','Transporter Name','GSTIN','Created At (UTC)']): self.tree.heading(col, text=text2)
        self.tree.column('id', width=60, anchor='center'); self.tree.column('trip_id', width=120, anchor='center'); self.tree.column('vehicle_no', width=140); self.tree.column('transporter_name', width=180); self.tree.column('gstin', width=160); self.tree.column('created_at', width=180)
        vsb = _tctms_ttk.Scrollbar(table_frame, orient='vertical', command=self.tree.yview); self.tree.configure(yscroll=vsb.set); self.tree.pack(side='left', fill='both', expand=True); vsb.pack(side='right', fill='y')

        init_db(); self.refresh_table(); self.vehicle_entry.focus_set(); self.bind_all('<Return>', self._enter_handler)
        status.pack(fill='x', pady=(6,4))

    def destroy(self):
        try: self.unbind_all('<Return>')
        except Exception: pass
        super().destroy()
    def _enter_handler(self, _event=None): self.on_create_trip()
    def on_create_trip(self):
        vehicle_no = (self.vehicle_var.get() or '').strip(); transporter_name = (self.transporter_var.get() or '').strip(); gstin = (self.gstin_var.get() or '').strip().upper(); src_wh = (self.src_wh_var.get() or '').strip(); dst_wh = (self.dst_wh_var.get() or '').strip()
        errs = list(filter(None, ['Source Warehouse is required.' if not src_wh else None, 'Destination Warehouse is required.' if not dst_wh else None, validate_vehicle_no(vehicle_no), validate_transporter_name(transporter_name), validate_gstin(gstin)]))
        if errs:
            _tctms_messagebox.showerror('Validation Error', '\n'.join(errs)); self.status_var.set('Validation failed'); return
        try:
            self.create_btn.configure(state='disabled')
            trip_id = generate_unique_vrn(prefix='VRN', digits=6)
            insert_entry(vehicle_no, transporter_name, gstin, trip_id, src_wh, dst_wh)
            _tctms_messagebox.showinfo('Trip Created', f'Trip-Id created: {trip_id}')
            self.status_var.set(f'Created Trip-Id: {trip_id}'); self.refresh_table(); self.on_clear()
        except _tctms_sqlite3.IntegrityError as e:
            _tctms_messagebox.showerror('Create Trip Failed', f'Trip-Id collision detected. Please try again.\n\n{e}'); self.status_var.set('Create Trip failed (collision)')
        except Exception as e:
            _tctms_messagebox.showerror('Create Trip Failed', f'Could not create trip.\n\n{e}'); self.status_var.set('Create Trip failed')
        finally:
            self.create_btn.configure(state='normal')
    def on_clear(self):
        self.vehicle_var.set(''); self.transporter_var.set(''); self.gstin_var.set('')
        try: self.src_wh_combo.current(0); self.dst_wh_combo.current(0)
        except Exception: pass
        self.vehicle_entry.focus_set()
    def refresh_table(self):
        for row in self.tree.get_children(): self.tree.delete(row)
        try:
            rows = fetch_recent(limit=50)
            for r in rows: self.tree.insert('', 'end', values=r)
            self.status_var.set(f'Loaded {len(rows)} entries')
        except Exception as e:
            _tctms_messagebox.showerror('Load Failed', f'Could not load entries.\n\n{e}'); self.status_var.set('Load failed')

# ---- TC UI ----
class TCFrame(_BackMixin, _tctms_ttk.Frame):
    def __init__(self, master, on_back_callback=None):
        super().__init__(master, padding=16)
        self.on_back_callback = on_back_callback
        header = _tctms_ttk.Frame(self); header.pack(fill='x', pady=(0,8))
        _tctms_ttk.Label(header, text='TC Module', font=('Segoe UI', 12, 'bold')).pack(side='left')
        _tctms_ttk.Button(header, text='Back', command=self._go_back).pack(side='right')

        form = _tctms_ttk.LabelFrame(self, text='Trip Check-In', padding=12); form.pack(fill='x')
        _tctms_ttk.Label(form, text='Source Warehouse:').grid(row=0, column=0, sticky='w', padx=(0,8), pady=6)
        self.tc_src_wh_var = _tk.StringVar(); self.tc_src_wh_combo = make_wh_autocomplete(form, textvariable=self.tc_src_wh_var, width=40)
        self.tc_src_wh_combo.grid(row=0, column=1, sticky='w', pady=6)
        if _WAREHOUSE_SHORTS:
            try: self.tc_src_wh_combo.current(0)
            except Exception: pass
        _tctms_ttk.Label(form, text='Vehicle No:').grid(row=1, column=0, sticky='w', padx=(0,8), pady=6)
        self.tc_vehicle_var = _tk.StringVar(); self.tc_vehicle_entry = _tctms_ttk.Entry(form, textvariable=self.tc_vehicle_var, width=40)
        self.tc_vehicle_entry.grid(row=1, column=1, sticky='w', pady=6)
        self.tc_vehicle_entry.bind('<Return>', self._vehicle_enter_handler)
        _tctms_ttk.Label(form, text='Trip-Id:').grid(row=2, column=0, sticky='w', padx=(0,8), pady=6)
        self.tc_trip_var = _tk.StringVar(); self.tc_trip_entry = _tctms_ttk.Entry(form, textvariable=self.tc_trip_var, width=40, state='readonly')
        self.tc_trip_entry.grid(row=2, column=1, sticky='w', pady=6)
        self.tc_trip_entry.bind('<Return>', self._submit_enter_handler)
        upload_row = _tctms_ttk.Frame(form); upload_row.grid(row=3, column=0, columnspan=2, sticky='w', pady=(4,0))
        _tctms_ttk.Label(upload_row, text='Upload file:').pack(side='left', padx=(0,8))
        self.tc_file_path = _tk.StringVar()
        def _pick_file():
            try:
                from tkinter import filedialog as _tctms_filedialog
                path = _tctms_filedialog.askopenfilename(title='Select file')
                if path:
                    self.tc_file_path.set(path)
                    self.status_var.set(f'File selected: {path.split('/')[-1]}')
            except Exception as e:
                self.status_var.set(f'File selection failed: {e}')
        _tctms_ttk.Button(upload_row, text='Choose…', command=_pick_file).pack(side='left')
        _tctms_ttk.Label(upload_row, textvariable=self.tc_file_path).pack(side='left', padx=(8,0))
        btns = _tctms_ttk.Frame(form); btns.grid(row=4, column=0, columnspan=2, pady=(8,0), sticky='w')
        self.submit_btn = _tctms_ttk.Button(btns, text='Submit', command=self.on_submit); self.submit_btn.grid(row=0, column=0, padx=(0,8))
        self.clear_btn = _tctms_ttk.Button(btns, text='Clear', command=self.on_clear); self.clear_btn.grid(row=0, column=1, padx=(0,8))

        # --- Center Scan Box panel (outside form, middle of page) ---
        self.tc_scan_box_var = _tk.StringVar()
        self.tc_scan_panel = _tctms_ttk.Frame(self, padding=12)
        self.tc_scan_panel.pack(fill='x', pady=(18, 8))
        _tctms_ttk.Label(self.tc_scan_panel, text='Scan box id:', font=('Segoe UI', 11, 'bold')).pack(anchor='center')
        self.tc_scan_box_entry = _tctms_ttk.Entry(self.tc_scan_panel, textvariable=self.tc_scan_box_var, width=56)
        self.tc_scan_box_entry.pack(anchor='center', pady=(6,0), ipady=6)
        self.tc_scan_box_entry.bind('<Return>', self._submit_enter_handler)
        # --- Progress (white → green) & gating ---
        self.tc_scan_ready = False
        try: self.tc_scan_box_entry.configure(state='disabled')
        except Exception: pass
        self.tc_expected_box_ids = []
        self.tc_expected_box_ids_set = set()
        self.tc_matched_box_ids = set()
        self.tc_total_boxes = 0
        self.tc_progress_label = _tctms_ttk.Label(self.tc_scan_panel, text='0/0', anchor='center')
        self.tc_progress_label.pack(anchor='center', pady=(10,2))
        self.tc_progress_canvas = _tk.Canvas(self.tc_scan_panel, height=12, bg='#FFFFFF', highlightthickness=1, highlightbackground='#BBBBBB')
        self.tc_progress_canvas.pack(fill='x', padx=8, pady=(0,4))
        self._tc_prog_segments = []

        self.status_var = _tk.StringVar(value='Ready')
        status = _tctms_ttk.Label(self, textvariable=self.status_var, anchor='w')
        status.pack(fill='x', pady=(6,4))
        self.tc_vehicle_entry.focus_set()

    def _vehicle_enter_handler(self, _event=None):
        vehicle = (self.tc_vehicle_var.get() or '').strip(); src_wh = (self.tc_src_wh_var.get() or '').strip()
        if not vehicle:
            self.status_var.set('Enter Vehicle No'); return
        if not src_wh:
            self.status_var.set('Select Source Warehouse'); return
        trip = fetch_latest_trip_id_by_vehicle(vehicle, src_wh=src_wh)
        if trip:
            self.tc_trip_var.set(trip); self.status_var.set(f'Trip-Id auto-filled: {trip}')
            self.tc_trip_entry.focus_set()
            try: self.tc_trip_entry.icursor('end')
            except Exception: pass
        else:
            self.tc_trip_var.set(''); _tctms_messagebox.showwarning('Trip-Id Not Found', f'No Trip-Id found for vehicle: {vehicle}\nunder Source: {src_wh}.\nCreate a trip in TMS first.'); self.status_var.set('Trip-Id not found for vehicle/source')
    def _submit_enter_handler(self, _event=None):
        code = (self.tc_scan_box_var.get() or '').strip()
        print(f"[TC][SCAN] Enter pressed code='{code}' ready={getattr(self,'tc_scan_ready',False)}")
        if not getattr(self, 'tc_scan_ready', False):
            self.status_var.set('Scan disabled: Click Submit after uploading STNs')
            return
        if code:
            self._tc_handle_scan(code)
        try: self.tc_scan_box_var.set('')
        except Exception: pass

    def on_submit(self):
        """Load STNs from uploaded file, collect all BOX-IDs from PACK STN DB, enable scanning."""
        print('[TC][SUBMIT] Started')
        path = (self.tc_file_path.get() or '').strip()
        print(f"[TC][SUBMIT] File path: {path!r}")
        if not path:
            _tctms_messagebox.showerror('Upload Required', 'Please choose a file that contains STN numbers (one per line).')
            self.status_var.set('No file selected'); print('[TC][SUBMIT] No file; abort'); return
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                raw = f.read().splitlines()
            stns = [s.strip() for s in raw if s and s.strip()]
            print(f"[TC][SUBMIT] STNs parsed: {len(stns)}")
            self.tc_box_to_stn = {}  # map BOX_ID -> STN
        except Exception as e:
            _tctms_messagebox.showerror('Read Error', f'Failed to read file: {e}')
            self.status_var.set('Read error'); print(f"[TC][SUBMIT] Read error: {e}"); return
        if not stns:
            _tctms_messagebox.showwarning('Empty File', 'The selected file does not contain any STN values.')
            self.status_var.set('Empty STN file'); print('[TC][SUBMIT] Empty STN list'); return
        all_boxes, missing = [], []
        try:
            with get_conn() as conn:
                cur = conn.cursor()
                for stn in stns:
                    u = stn.upper().strip(); print(f"[TC][SUBMIT] Query STN: {u}")
                    cur.execute(f"SELECT box_id FROM {_PACK_TABLE} WHERE UPPER(stn)=?", (u,))
                    rows = [r[0] for r in cur.fetchall() if r and r[0]]
                    print(f"[TC][SUBMIT]   → {len(rows)} box(es)")
                    if rows:
                        all_boxes.extend(rows)
                        for __b in rows:
                            try:
                                self.tc_box_to_stn[str(__b).strip().upper()] = u
                            except Exception:
                                pass
                    else:
                        missing.append(stn)
        except Exception as e:
            _tctms_messagebox.showerror('Database Error', f'Failed while fetching BOX-IDs: {e}')
            self.status_var.set('DB error'); print(f"[TC][SUBMIT] DB error: {e}"); return
        seen, ordered = set(), []
        for b in all_boxes:
            nb = (str(b).strip().upper())
            if nb and nb not in seen:
                seen.add(nb); ordered.append(nb)
        self.tc_expected_box_ids = ordered
        self.tc_expected_box_ids_set = set(ordered)
        self.tc_matched_box_ids = set()
        self.tc_total_boxes = len(ordered)
        print(f"[TC][SUBMIT] Total BOX-IDs after dedupe: {self.tc_total_boxes}")
        self._tc_init_progress()
        msg = f"Total STNs: {len(stns)}\nBOX-IDs found: {self.tc_total_boxes}"
        if missing:
            msg += f"\nNo records for {len(missing)} STN(s): " + ', '.join(missing[:5]) + ('' if len(missing)<=5 else ' …')
        _tctms_messagebox.showinfo('Loaded BOX-IDs', msg)
        self.status_var.set('BOX-IDs loaded; ready to scan')
        try: self.tc_scan_box_entry.configure(state='normal')
        except Exception: pass
        self.tc_scan_ready = True
        print('[TC][SUBMIT] Scanning enabled')
        try: self.tc_scan_box_entry.focus_set()
        except Exception: pass
    def on_clear(self):
        print('[TC][CLEAR] Reset state')
        self.tc_vehicle_var.set(''); self.tc_trip_var.set(''); self.tc_scan_box_var.set(''); self.tc_file_path.set('')
        self.tc_expected_box_ids = []; self.tc_expected_box_ids_set = set(); self.tc_matched_box_ids = set(); self.tc_total_boxes = 0
        self.tc_scan_ready = False
        try: self.tc_src_wh_combo.current(0)
        except Exception: pass
        self._tc_init_progress()
        try: self.tc_scan_box_entry.configure(state='disabled')
        except Exception: pass
        try: self.tc_vehicle_entry.focus_set()
        except Exception: pass

        self.tc_vehicle_var.set(''); self.tc_trip_var.set(''); self.tc_scan_box_var.set(''); self.tc_file_path.set('')
        try: self.tc_src_wh_combo.current(0)
        except Exception: pass
        self.tc_vehicle_entry.focus_set()
    # ---------------- TC helpers for STN/BOX scanning -----------------
    def _tc_init_progress(self):
        total = int(getattr(self, 'tc_total_boxes', 0) or 0)
        matched = len(getattr(self, 'tc_matched_box_ids', set()) or [])
        try: self.tc_progress_label.config(text=f"{matched}/{total}")
        except Exception: pass
        canvas = getattr(self, 'tc_progress_canvas', None)
        if not canvas: return
        canvas.delete('all'); self._tc_prog_segments = []
        if total <= 0:
            w = max(200, int(canvas.winfo_width() or 400))
            canvas.create_rectangle(2, 2, w-2, 10, outline='#CCCCCC', fill='#FFFFFF'); return
        padding = 8
        try: w = int(canvas.winfo_width() or 400)
        except Exception: w = 400
        avail = max(50, w - 2*padding); gap = 2
        seg_w = max(4, int((avail - gap*(total-1)) / total)); x = padding
        for _i in range(total):
            rect = canvas.create_rectangle(x, 2, x+seg_w, 10, outline='#DDDDDD', fill='#FFFFFF')
            self._tc_prog_segments.append(rect); x += seg_w + gap
        self._tc_update_progress()
    def _tc_update_progress(self):
        canvas = getattr(self, 'tc_progress_canvas', None)
        total = int(getattr(self, 'tc_total_boxes', 0) or 0)
        matched = len(getattr(self, 'tc_matched_box_ids', set()) or [])
        try: self.tc_progress_label.config(text=f"{matched}/{total}")
        except Exception: pass
        if total <= 0 or not canvas: return
        for idx, rect in enumerate(self._tc_prog_segments):
            try:
                fill = '#22C55E' if idx < matched else '#FFFFFF'
                canvas.itemconfig(rect, fill=fill, outline='#10A64A' if idx < matched else '#DDDDDD')
            except Exception: pass
        if matched >= total:
            try: _tctms_messagebox.showinfo('Done', 'All Box-Id loaded successfully')
            except Exception: pass
            print('[TC][SCAN] All Box-Id loaded successfully')
    def _tc_handle_scan(self, code: str):
        nb = (code or '').strip().upper()
        print(f"[TC][SCAN] Received: {nb}")
        if not nb: return
        if not getattr(self, 'tc_scan_ready', False):
            print('[TC][SCAN] Not ready; ignore'); return
        if not getattr(self, 'tc_expected_box_ids', []):
            self.status_var.set('No BOX-IDs loaded. Upload STN file and click Submit.')
            print('[TC][SCAN] No expected list; ignore'); return
        if nb in self.tc_expected_box_ids_set:
            if nb not in self.tc_matched_box_ids:
                self.tc_matched_box_ids.add(nb)
                try:
                    record_tc_scan(nb, src_wh=(self.tc_src_wh_var.get() or ''), vehicle_no=(self.tc_vehicle_var.get() or ''), trip_id=(self.tc_trip_var.get() or ''), stn=(self.tc_box_to_stn.get(nb) if hasattr(self, 'tc_box_to_stn') else None), matched=True, note='in-list')
                except Exception:
                    pass
                self.status_var.set(f'Matched: {nb}')
                print(f"[TC][SCAN] Matched: {nb}  ({len(self.tc_matched_box_ids)}/{self.tc_total_boxes})")
                self._tc_update_progress()
            else:
                self.status_var.set(f'Already scanned: {nb}')
                print(f"[TC][SCAN] Already scanned: {nb}")
        else:
            try:
                record_tc_scan(nb, src_wh=(self.tc_src_wh_var.get() or ''), vehicle_no=(self.tc_vehicle_var.get() or ''), trip_id=(self.tc_trip_var.get() or ''), stn=(self.tc_box_to_stn.get(nb) if hasattr(self, 'tc_box_to_stn') else None), matched=False, note='not-in-list')
            except Exception:
                pass
            self.status_var.set(f'Not in list: {nb}')
            print(f"[TC][SCAN] Not in expected list: {nb}")

# ---- Integration hooks ----

def _attach_tms_tc_to_mainframe():
    try:
        mf = MainFrame
    except Exception:
        return
    def _pack_only(self, target):
        for v in [getattr(self, 'menu_view', None), getattr(self, 'box_view', None), getattr(self, 'tote_view', None), getattr(self, 'tms_view', None), getattr(self, 'tc_view', None), getattr(self, 'go_pick_view', None)]:
            try: v.pack_forget()
            except Exception: pass
        try: target.pack(fill='both', expand=True)
        except Exception:
            try: target.tkraise()
            except Exception: pass
        try: self.back_btn_top.pack_forget()
        except Exception: pass
    def show_tms_view(self):
        try:
            for ch in list(self.tms_view.winfo_children()): ch.destroy()
            frame = TMSFrame(self.tms_view, on_back_callback=self.show_menu)
            frame.pack(fill='both', expand=True)
            if hasattr(self, 'status_lbl'): self.status_lbl.config(text='')
            _pack_only(self, self.tms_view)
        except Exception as e:
            print(f'[TMS] Failed to render TMS view: {e}')
    def show_tc_view(self):
        try:
            for ch in list(self.tc_view.winfo_children()): ch.destroy()
            frame = TCFrame(self.tc_view, on_back_callback=self.show_menu)
            frame.pack(fill='both', expand=True)
            if hasattr(self, 'status_lbl'): self.status_lbl.config(text='')
            _pack_only(self, self.tc_view)
        except Exception as e:
            print(f'[TC] Failed to render TC view: {e}')
    
    def show_go_pick_view(self):
        try: self.menu_view.pack_forget()
        except Exception: pass
        try: self.box_view.pack_forget()
        except Exception: pass
        try: self.tote_view.pack_forget()
        except Exception: pass
        try: self.tms_view.pack_forget()
        except Exception: pass
        try: self.tc_view.pack_forget()
        except Exception: pass
        try: self.stn_tl_view.pack_forget()
        except Exception: pass
        try: self.go_pick_view.pack(fill='both', expand=True)
        except Exception: pass
        try:
            if not self.back_btn_top.winfo_ismapped():
                self.back_btn_top.pack(side='left')
        except Exception: pass
        try:
            for ch in list(self.go_pick_view.winfo_children()): ch.destroy()
        except Exception: pass
        _build_go_pick_panel(self.go_pick_view)
    mf.show_tms_view = show_tms_view; mf.show_tc_view = show_tc_view; mf.show_go_pick_view = show_go_pick_view

# -- attach TMS/TC into MainFrame (early) --
try:
    _attach_tms_tc_to_mainframe()
except Exception as _e:
    print('[TMS/TC] Early attach warning:', _e)


# === STN-Final helpers for GO PICK ===
import sqlite3 as _gpf_sqlite3
try: STN_FINAL_DB_PATH
except NameError: STN_FINAL_DB_PATH = 'STN-Final'
try: STN_FINAL_TABLE
except NameError: STN_FINAL_TABLE = 'stn_final'

def stn_final_distinct_shelves_latest_first(limit: int = 500):
    try:
        with _gpf_sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            sql = ('SELECT s FROM (SELECT TRIM(COALESCE("Shelf","")) AS s, MAX(ROWID) AS r '
                   'FROM ' + STN_FINAL_TABLE + ' WHERE TRIM(COALESCE("Shelf",""))<>"" '
                   'GROUP BY UPPER(s) ORDER BY r DESC LIMIT ?)')
            cur.execute(sql, (limit,))
            return [r[0] for r in cur.fetchall() if r and r[0]]
    except Exception as e:
        print('[GO PICK][DB] shelves(latest) query failed:', e)
        return []

def stn_final_latest_shelf():
    try:
        with _gpf_sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            sql = ('SELECT s FROM (SELECT TRIM(COALESCE("Shelf","")) AS s, MAX(ROWID) AS r '
                   'FROM ' + STN_FINAL_TABLE + ' WHERE TRIM(COALESCE("Shelf",""))<>"" '
                   'GROUP BY UPPER(s) ORDER BY r DESC LIMIT 1);')
            cur.execute(sql)
            row = cur.fetchone()
            return (row[0] or '').strip() if row and row[0] else ''
    except Exception as e:
        print('[GO PICK][DB] latest shelf query failed:', e)
        return ''



def stn_final_first_shelf_sorted():
    """Return the first shelf label in case-insensitive ascending order from STN-Final.stn_final."""
    try:
        with _gpf_sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            sql = (
                'SELECT s FROM ('
                '  SELECT TRIM(COALESCE("Shelf","")) AS s, MIN(ROWID) AS r'
                '  FROM ' + STN_FINAL_TABLE + ' WHERE TRIM(COALESCE("Shelf",""))<>""'
                '  GROUP BY UPPER(s)'
                ') ORDER BY UPPER(s) ASC LIMIT 1;'
            )
            cur.execute(sql)
            row = cur.fetchone()
            return (row[0] or '').strip() if row and row[0] else ''
    except Exception as e:
        print('[GO PICK][DB] first shelf(sorted) query failed:', e)
        return ''
def stn_final_tl_for_shelf(shelf: str):
    shelf = (shelf or '').strip()
    if not shelf:
        return None
    try:
        with _gpf_sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()

            sql1 = (
                'SELECT "TL-Id", COUNT(*) AS c FROM ' + STN_FINAL_TABLE +
                ' WHERE UPPER("Shelf") = UPPER(?) '
                'AND TRIM(COALESCE("TL-Id","")) <> "" '
                'GROUP BY "TL-Id" ORDER BY c DESC LIMIT 1;'
            )
            cur.execute(sql1, (shelf,))
            row = cur.fetchone()
            if row and row[0]:
                return row[0]

            sql2 = (
                'SELECT "TL-Id" FROM ' + STN_FINAL_TABLE +
                ' WHERE UPPER("Shelf") = UPPER(?) '
                'AND TRIM(COALESCE("TL-Id","")) <> "" '
                'ORDER BY ROWID DESC LIMIT 1;'
            )
            cur.execute(sql2, (shelf,))
            row = cur.fetchone()
            return row[0] if row else None

    except Exception as e:
        print('[GO PICK][DB] tl-for-shelf failed:', e)
        return None



def stn_final_next_unpicked_box_for_tl_shelf(tl: str, shelf: str):
    tl = (tl or '').strip(); shelf = (shelf or '').strip()
    try:
        import sqlite3
        with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            sql1 = (
                'SELECT "Box-Id" FROM ' + STN_FINAL_TABLE +
                ' WHERE UPPER("TL-Id")=UPPER(?) AND UPPER("Shelf")=UPPER(?)'
                ' AND LENGTH(TRIM(COALESCE("Box-Id", ?))) > 0'
                ' ORDER BY rowid DESC LIMIT 1;'
            )
            cur.execute(sql1, (tl, shelf, ''))
            row = cur.fetchone()
            if row and row[0]:
                return row[0]
            sql2 = (
                'SELECT "Box-Id" FROM ' + STN_FINAL_TABLE +
                ' WHERE UPPER("TL-Id")=UPPER(?)'
                ' AND LENGTH(TRIM(COALESCE("Box-Id", ?))) > 0'
                ' ORDER BY rowid DESC LIMIT 1;'
            )
            cur.execute(sql2, (tl, ''))
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as e:
        print('[STN-Final][WARN] box-for-tl-shelf failed:', e)
        return None





def stn_final_next_unpicked_box_for_tl_shelf(tl: str, shelf: str):
    """Return the earliest Box-Id for the given TL & Shelf whose Pick is empty (unprocessed).
    Falls back to latest known Box-Id if none are unpicked.
    """
    tl = (tl or '').strip()
    shelf = (shelf or '').strip()
    try:
        import sqlite3
        with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            # Prefer the row that has not been picked yet
            sql1 = (
                'SELECT "Box-Id" FROM ' + STN_FINAL_TABLE +
                ' WHERE UPPER("TL-Id")=UPPER(?) AND UPPER("Shelf")=UPPER(?)'
                ' AND LENGTH(TRIM(COALESCE("Box-Id", ?))) > 0'
                ' AND ("Pick" IS NULL OR TRIM("Pick") = "")'
                ' ORDER BY ROWID ASC LIMIT 1;'
            )
            cur.execute(sql1, (tl, shelf, ''))
            row = cur.fetchone()
            if row and row[0]:
                return row[0]
            # Fallback: any known Box-Id for TL+Shelf (latest)
            sql2 = (
                'SELECT "Box-Id" FROM ' + STN_FINAL_TABLE +
                ' WHERE UPPER("TL-Id")=UPPER(?) AND UPPER("Shelf")=UPPER(?)'
                ' AND LENGTH(TRIM(COALESCE("Box-Id", ?))) > 0'
                ' ORDER BY ROWID DESC LIMIT 1;'
            )
            cur.execute(sql2, (tl, shelf, ''))
            row = cur.fetchone()
            return row[0] if row and row[0] else None
    except Exception as e:
        print('[STN-Final][WARN] next-unpicked-box failed:', e)
        return None

def stn_final_set_pick_by_tl_shelf_box(tl: str, shelf: str, box_id: str, message: str) -> int:
    """Set Pick message for the single row identified by TL, Shelf and Box-Id using ROWID.
    Returns the number of rows updated (0 or 1).
    """
    try:
        import sqlite3
        with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(
                'SELECT ROWID FROM ' + STN_FINAL_TABLE +
                ' WHERE UPPER("TL-Id")=UPPER(?) AND UPPER("Shelf")=UPPER(?) AND UPPER("Box-Id")=UPPER(?)'
                ' ORDER BY ROWID ASC LIMIT 1;',
                (tl or '', shelf or '', (box_id or '').strip())
            )
            row = cur.fetchone()
            if not row:
                return 0
            cur.execute(
                'UPDATE ' + STN_FINAL_TABLE + ' SET "Pick"=? WHERE ROWID=?;',
                (message or '', row[0])
            )
            conn.commit()
            return cur.rowcount or 0
    except Exception as e:
        print('[STN-Final][ERROR] set Pick by TL/Shelf/Box failed:', e)
        return 0


# === NEW: DB helpers for Box/TL success message updates (robust) ===
def stn_final_update_pick_by_box(box_id: str, message: str) -> int:
    """Set Pick message for all rows matching the given Box-Id.
    Returns number of rows updated.
    """
    try:
        import sqlite3
        box = (box_id or '').strip()
        if not box:
            return 0
        with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(
                'UPDATE ' + STN_FINAL_TABLE + ' SET "Pick"=? WHERE UPPER("Box-Id")=UPPER(?)',
                (message or '', box)
            )
            conn.commit()
            return cur.rowcount or 0
    except Exception as e:
        print('[STN-Final][ERROR] update Pick by Box-Id failed:', e)
        return 0

def stn_final_get_tl_by_box(box_id: str):
    """Return TL-Id for a given Box-Id from stn_final (latest row)."""
    try:
        import sqlite3
        box = (box_id or '').strip()
        if not box:
            return None
        with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            sql = (
                'SELECT "TL-Id" FROM ' + STN_FINAL_TABLE + ' '
                'WHERE UPPER("Box-Id")=UPPER(?) AND TRIM(COALESCE("TL-Id","")) <> "" '
                'ORDER BY ROWID DESC LIMIT 1;'
            )
            cur.execute(sql, (box,))
            row = cur.fetchone()
            return (row[0].strip() if row and row[0] else None)
    except Exception as e:
        print('[STN-Final][WARN] get TL by Box failed:', e)
        return None

def stn_final_update_tl_status(tl_id: str, status: str = 'TL Complete') -> int:
    """Update column 'TL-Id status' for all rows with the given TL-Id.
    If the column does not exist, attempt to add it.
    Returns affected row count (best-effort).
    """
    try:
        import sqlite3
        tl = (tl_id or '').strip()
        if not tl:
            return 0
        with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            # ensure column exists (safe no-op if already there)
            try:
                cur.execute('PRAGMA table_info(' + STN_FINAL_TABLE + ');')
                cols = [r[1] for r in cur.fetchall()]
                if 'TL-Id status' not in cols:
                    try:
                        cur.execute('ALTER TABLE ' + STN_FINAL_TABLE + ' ADD COLUMN "TL-Id status" TEXT;')
                        conn.commit()
                    except Exception:
                        pass
            except Exception:
                pass
            cur.execute(
                'UPDATE ' + STN_FINAL_TABLE + ' SET "TL-Id status"=? WHERE UPPER("TL-Id")=UPPER(?)',
                (status or 'TL Complete', tl)
            )
            conn.commit()
            return cur.rowcount or 0
    except Exception as e:
        print('[STN-Final][ERROR] update TL-Id status failed:', e)
        return 0
# === END NEW DB helpers ===


# ============================ PACK Flow: DB helpers ============================
import sqlite3 as _pack_sqlite3

def stn_final_all_tls_complete():
    """Return True if every row that has a TL-Id also has TL-Id status == 'TL Complete' (case-insensitive)."""
    try:
        with _pack_sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            sql = ' '.join([
                'SELECT COUNT(1) FROM ' + STN_FINAL_TABLE,
                'WHERE LENGTH(TRIM(COALESCE("TL-Id",""))) > 0',
                'AND ("TL-Id status" IS NULL OR TRIM(UPPER("TL-Id status")) <> "TL COMPLETE")'
            ])
            cur.execute(sql)
            row = cur.fetchone()
            cnt = row[0] if row else 0
            return (cnt == 0)
    except Exception as e:
        print('[PACK][DB] all_tls_complete check failed:', e)
        return False


def stn_final_box_ids_to_pack(limit=None):
    """Return list of Box-Id to process where TL-Id status is TL Complete and Pack column empty."""
    try:
        with _pack_sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            sql = ' '.join([
                'SELECT DISTINCT TRIM(COALESCE("Box-Id", "")) AS box_id',
                'FROM ' + STN_FINAL_TABLE,
                'WHERE LENGTH(TRIM(COALESCE("Box-Id","")))>0',
                'AND TRIM(UPPER(COALESCE("TL-Id status","")))=\'TL COMPLETE\'',
                'AND ("Pack" IS NULL OR TRIM("Pack")=\'\')',
                'ORDER BY ROWID ASC'
            ])
            if isinstance(limit, int) and limit>0:
                sql = sql + ' LIMIT ' + str(int(limit))
            cur.execute(sql)
            return [r[0] for r in cur.fetchall() if r and r[0]]
    except Exception as e:
        print('[PACK][DB] fetch box ids failed:', e)
        return []

def stn_final_update_pack_by_box(box_id: str, message: str) -> int:
    """Update Pack column with message for rows matching Box-Id. Returns rows affected."""
    box = (box_id or '').strip()
    if not box:
        return 0
    try:
        with _pack_sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(
                'UPDATE ' + STN_FINAL_TABLE + ' SET "Pack"=? WHERE UPPER("Box-Id")=UPPER(?)',
                ((message or ''), box)
            )
            conn.commit()
            return cur.rowcount or 0
    except Exception as e:
        print('[PACK][DB] update Pack failed:', e)
        return 0


def stn_final_next_unpicked_shelf():
    """Return next shelf label whose rows have empty Pick (unprocessed), preferring earliest ROWID.
    If none found, returns empty string.
    """
    try:
        import sqlite3
        with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            sql = (
                'SELECT s FROM ('
                ' SELECT TRIM(COALESCE("Shelf","")) AS s, MIN(ROWID) AS r'
                ' FROM ' + STN_FINAL_TABLE +
                ' WHERE TRIM(COALESCE("Shelf","")) <> ""'
                '   AND ("Pick" IS NULL OR TRIM("Pick") = "")'
                ' GROUP BY UPPER(s)'
                ') ORDER BY r ASC LIMIT 1;'
            )
            cur.execute(sql)
            row = cur.fetchone()
            return (row[0] or '').strip() if row and row[0] else ''
    except Exception as e:
        print('[GO PICK][DB] next-unpicked shelf query failed:', e)
        return ''

def stn_final_wids_for_tl_shelf(tl: str, shelf: str, limit: int = 50):
    tl = (tl or '').strip(); shelf = (shelf or '').strip()
    if not tl or not shelf:
        return []
    try:
        import sqlite3
        with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            sql = (
                'SELECT DISTINCT TRIM(COALESCE("WID", ?)) AS wid FROM ' + STN_FINAL_TABLE +
                ' WHERE UPPER("TL-Id")=UPPER(?) AND UPPER("Shelf")=UPPER(?)'
                ' AND LENGTH(TRIM(COALESCE("WID", ?))) > 0'
                ' LIMIT ?;'
            )
            cur.execute(sql, ('', tl, shelf, '', int(limit)))
            return [r[0] for r in cur.fetchall() if r and r[0]]
    except Exception as e:
        print('[STN-Final][WARN] wids-for-tl-shelf failed:', e)
        return []










def stn_final_match_any_code(tl: str, shelf: str, text: str):
    q_raw = (text or '').strip(); q = q_raw.upper()
    tl = (tl or '').strip(); shelf = (shelf or '').strip()
    if not (q and tl and shelf):
        return False
    try:
        import sqlite3
        with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            sql = f"""
SELECT 1
FROM {STN_FINAL_TABLE}
WHERE UPPER("TL-Id") = UPPER(?) AND UPPER("Shelf") = UPPER(?)
  AND (
        UPPER(COALESCE("FSN", ?)) = ?
     OR UPPER(COALESCE("EAN", ?)) = ?
     OR UPPER(COALESCE("Model-Id", ?)) = ?
     OR UPPER(COALESCE("FSN", ?))       LIKE '%'||?||'%'
     OR UPPER(COALESCE("EAN", ?))       LIKE '%'||?||'%'
     OR UPPER(COALESCE("Model-Id", ?))  LIKE '%'||?||'%'
     OR REPLACE(REPLACE(UPPER(COALESCE("EAN", ?)), '-', ''), ' ', '') LIKE '%' || REPLACE(REPLACE(?, '-', ''), ' ', '') || '%'
  )
LIMIT 1;
"""
            params = (
                tl, shelf,
                '', q,   # exact FSN
                '', q,   # exact EAN
                '', q,   # exact Model-Id
                '', q,   # fuzzy FSN
                '', q,   # fuzzy EAN
                '', q,   # fuzzy Model-Id
                '', q    # sanitized contains
            )
            cur.execute(sql, params)
            return cur.fetchone() is not None
    except Exception as e:
        print('[GO PICK][DB] match-any-code (fuzzy) failed:', e)
        return False





def stn_final_update_pick_message(tl: str, shelf: str, message: str):
    try:
        with _gpf_sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            sql = ('UPDATE ' + STN_FINAL_TABLE + ' SET "Pick"=? WHERE UPPER("TL-Id")=UPPER(?) AND UPPER("Shelf")=UPPER(?)')
            cur.execute(sql, (message or '', tl or '', shelf or ''))
            conn.commit()
            return cur.rowcount or 0
    except Exception as e:
        print('[GO PICK][DB] update Pick failed:', e)
        return 0

from tkinter import ttk as _gp_ttk
from selenium.webdriver.common.by import By as _GP_BY
from selenium.webdriver.support import expected_conditions as _GP_EC
from selenium.webdriver.support.ui import WebDriverWait as _GP_Wait
from selenium.webdriver.common.keys import Keys as _GP_KEYS
def _build_go_pick_panel(parent):
    frm = _gp_ttk.LabelFrame(parent, text='GO PICK', padding=10)
    frm.pack(fill='x', pady=(4,8))
    status_var = tk.StringVar(value='Opening Confirm TL page...')
    _gp_ttk.Label(frm, textvariable=status_var).pack(anchor='w', pady=(0,6))

    # Suggested Shelf (auto from STN-Final)
    row1 = _gp_ttk.Frame(frm); row1.pack(fill='x', pady=4)
    _gp_ttk.Label(row1, text='Suggested Shelf:').pack(side='left')
    shelf_var = tk.StringVar()
    shelf_label = _gp_ttk.Label(row1, textvariable=shelf_var, font=('Arial', 11, 'bold'))
    shelf_label.pack(side='left', padx=(8,0))
    try:
        latest = stn_final_first_shelf_sorted()
        shelf_var.set(latest or '')
    except Exception:
        shelf_var.set('')

    # Scan Shelf (user scans the label they see)
    row2 = _gp_ttk.Frame(frm); row2.pack(fill='x', pady=4)
    _gp_ttk.Label(row2, text='Scan Shelf:').pack(side='left')
    shelf_scan_var = tk.StringVar(); shelf_scan_entry = _gp_ttk.Entry(row2, textvariable=shelf_scan_var, width=40)
    # Expose next-shelf setter to global so after-code can update GUI
    def _set_next_shelf_label(_lbl: str):
        try: shelf_var.set(_lbl or '')
        except Exception: pass
        try: shelf_scan_var.set(_lbl or '')
        except Exception: pass
    globals()['GO_PICK_NEXT_SHELF_CALLBACK'] = _set_next_shelf_label
    shelf_scan_entry.pack(side='left', padx=(8,0))

    # Suggested WID (from TL+Shelf)
    row2b = _gp_ttk.Frame(frm); row2b.pack(fill='x', pady=4)
    _gp_ttk.Label(row2b, text='Suggested WID:').pack(side='left')
    wid_sugg_var = tk.StringVar()
    wid_sugg_label = _gp_ttk.Label(row2b, textvariable=wid_sugg_var, font=('Arial', 11, 'bold'))
    wid_sugg_label.pack(side='left', padx=(8,0))

# Scan WID
    row3 = _gp_ttk.Frame(frm); row3.pack(fill='x', pady=4)
    _gp_ttk.Label(row3, text='Scan WID:').pack(side='left')
    wid_var = tk.StringVar(); wid_entry = _gp_ttk.Entry(row3, textvariable=wid_var, width=40)
    wid_entry.pack(side='left', padx=(8,0))

    # Scan FSN/EAN/Model-Id
    row4 = _gp_ttk.Frame(frm); row4.pack(fill='x', pady=4)
    _gp_ttk.Label(row4, text='Scan FSN/EAN/Model-Id:').pack(side='left')
    code_var = tk.StringVar(); code_entry = _gp_ttk.Entry(row4, textvariable=code_var, width=40)
    code_entry.pack(side='left', padx=(8,0))

    # Buttons — only Mark as Lost
    btns = _gp_ttk.Frame(frm); btns.pack(fill='x', pady=(8,0))
    _gp_ttk.Button(btns, text='Mark as Lost', command=lambda: _gp_mark_lost(status_var, shelf_var.get() or shelf_scan_var.get())).pack(side='left')

    def _refresh_wid_suggestions_for_shelf(shelf_label: str):
        shelf = (shelf_label or '').strip()
        if not shelf:
            wid_sugg_var.set('')
            return
        tl = stn_final_tl_for_shelf(shelf)
        wid_list= stn_final_wids_for_tl_shelf(tl or '', shelf) if tl else []
        wid=wid_list[0] if wid_list else None
        if not wid:
            wid = _db_get_suggested_wid_for_shelf(shelf)
        wid_sugg_var.set(wid or '')

        # Initial suggestions
    try:
        _refresh_wid_suggestions_for_shelf(shelf_var.get())
    except Exception:
        pass

    # Bindings — dropdown is only visual; flow starts on scan
    shelf_scan_entry.bind('<Return>', lambda _e=None: (_refresh_wid_suggestions_for_shelf(shelf_scan_var.get()), _gp_after_shelf_selected(status_var, shelf_scan_var.get())))
    wid_entry.bind('<Return>', lambda _e=None: _gp_after_wid_scanned(status_var, shelf_scan_var.get(), wid_var.get()))
    code_entry.bind('<Return>', lambda _e=None: _gp_after_code_scanned(status_var, shelf_scan_var.get(), code_var.get()))
    # Auto-open Confirm TL disabled; will open on each Shelf scan.
    # Start background poll to capture 'Close Tote/Box' success messages and update DB
    try:
        _gp_start_close_message_poll_v2(frm, status_var)
    except Exception:
        pass



def _get_active_driver_wait():


    global GUI_ACTIVE_DRIVER
    drv = GUI_ACTIVE_DRIVER
    if not drv:
        raise RuntimeError('No active browser session. Please login.')
    return drv, build_wait(drv, TIMEOUT_MED)

# First click only opens Confirm TL




# === NEW: GO PICK fast click utilities & v2 handlers ===
from selenium.common.exceptions import ElementClickInterceptedException, TimeoutException as _GP_TO


def _gp_fast_click(drv, locator, timeout=3.0, post_wait=None):
    """Robust, low-latency click with native->JS fallback and micro-confirm."""
    by, sel = locator
    try:
        end = drv.execute_script('return Date.now()') + int(timeout*1000)
    except Exception:
        import time; end = int((time.time()+timeout)*1000)
    last_err = None
    while True:
        try:
            now = drv.execute_script('return Date.now()')
        except Exception:
            import time; now = int(time.time()*1000)
        if now >= end:
            break
        try:
            el = _GP_Wait(drv, 0.7).until(_GP_EC.element_to_be_clickable((by, sel)))
            try:
                drv.execute_script('arguments[0].scrollIntoView({block:"center"});', el)
            except Exception:
                pass
            try:
                el.click(); ok=True
            except ElementClickInterceptedException:
                try:
                    drv.execute_script('arguments[0].click();', el); ok=True
                except Exception as e:
                    ok=False; last_err=e
            except Exception as e:
                try:
                    drv.execute_script('arguments[0].click();', el); ok=True
                except Exception as e2:
                    ok=False; last_err=e2
            if ok:
                return True
        except _GP_TO as e:
            last_err = e
        except Exception as e:
            last_err = e
    if last_err:
        print('[GO PICK] fast_click failed:', last_err)
    return False


def _tight_confirm(drv, cond_fn, budget_ms=400):
    import time
    deadline = time.time() + (budget_ms/1000.0)
    while time.time() < deadline:
        try:
            if cond_fn(drv):
                return True
        except Exception:
            pass
        time.sleep(0.03)
    return False

_GP_LOC_PICK_BTN       = (_GP_BY.XPATH, "//button[normalize-space()='Pick'] | //input[@type='button' and @value='Pick']")
_GP_LOC_PICK_ITEMS_BTN = (_GP_BY.XPATH, "//button[normalize-space()='Pick Items'] | //input[@type='button' and contains(@value,'Pick Items')]")
_GP_LOC_CLOSE_BTN      = (_GP_BY.XPATH, "//button[contains(.,'Close Tote') or contains(.,'Close Box') or contains(.,'Close Tote/Box')] | //input[@type='button' and contains(@value,'Close')]")


def _gp_click_sequence_after_scan(status_var=None):
    try:
        drv, _ = _get_active_driver_wait()
    except Exception:
        return False
    def set_status(msg):
        try: status_var and status_var.set(msg)
        except Exception: pass
    if _gp_fast_click(drv, _GP_LOC_PICK_BTN, timeout=2.0): set_status('Picked')
    if _gp_fast_click(drv, _GP_LOC_PICK_ITEMS_BTN, timeout=2.0): set_status('Pick Items clicked')
    if _gp_fast_click(drv, _GP_LOC_CLOSE_BTN, timeout=2.0): set_status('Close initiated')
    # Post-close immediate collection + DB update
    try:
        found = _gp_collect_transfer_complete_messages(drv)
        for box_id, msg_text in found:
            try:
                rc = stn_final_update_pick_by_box(box_id, msg_text)
                print('[GO PICK][PostClose] Updated Pick for Box-Id:', box_id, 'rows:', rc)
            except Exception as _e1:
                print('[GO PICK][PostClose] Pick update failed:', _e1)
            tl_id = None
            try: tl_id = (GO_PICK_STATE.get('tl') if isinstance(GO_PICK_STATE, dict) else None)
            except Exception: tl_id = None
            if not tl_id:
                try:
                    cur_url = drv.current_url or ''
                    m2 = _GP_RE.search(r'/transfer_list/(?:confirm_transfer_list/)?(TL\d+)', cur_url, flags=_GP_RE.I)
                    if m2: tl_id = m2.group(1).upper()
                except Exception: pass
            if not tl_id:
                try: tl_id = stn_final_get_tl_by_box(box_id)
                except Exception: tl_id = None
            if tl_id:
                try:
                    rc2 = stn_final_update_tl_status(tl_id, 'TL Complete')
                    print('[GO PICK][PostClose] TL Complete set for', tl_id, 'rows:', rc2)
                except Exception as e:
                    print('[GO PICK][PostClose] TL-Id status update failed:', e)
    except Exception as _e_post:
        print('[GO PICK][PostClose] scan failed:', _e_post)
    return True


def _gp_after_code_scanned_v2(status_var, shelf_label: str, code_text: str):
    code = (code_text or '').strip()
    if not code:
        try: status_var and status_var.set('Empty code scanned')
        except Exception: pass
        return
    try:
        tl = (GO_PICK_STATE.get('tl') if isinstance(GO_PICK_STATE, dict) else None) or ''
        ok_match = True
        if tl and shelf_label:
            try: ok_match = stn_final_match_any_code(tl, shelf_label, code)
            except Exception: ok_match = True
        if not ok_match:
            try: status_var and status_var.set('Scanned code not expected for shelf/TL')
            except Exception: pass
        else:
            try: status_var and status_var.set('Code OK; executing fast pick...')
            except Exception: pass
            _gp_click_sequence_after_scan(status_var)
    except Exception as e:
        print('[GO PICK] after_code_scanned_v2 error:', e)

# --- Enhanced success message collector ---

def _gp_collect_transfer_complete_messages(drv):
    """Collect (box_id, message_text) entries from visible DOM + iframes; fallback to page_source text scan."""
    import re as _R
    msgs = []
    try:
        selectors = ['#success-message', '.line.msg.done', '.line.msg.fk-hidden.done', '.msg.done', '.alert-success', '.success', "div[id*='success']", '[id^=success]']
        nodes = []
        for sel in selectors:
            try: nodes.extend(drv.find_elements(_GP_BY.CSS_SELECTOR, sel))
            except Exception: pass
        # Also scan iframes
        try: frames = drv.find_elements(_GP_BY.TAG_NAME, 'iframe')
        except Exception: frames = []
        for fr in frames:
            try:
                drv.switch_to.default_content(); drv.switch_to.frame(fr)
                for sel in selectors:
                    try: nodes.extend(drv.find_elements(_GP_BY.CSS_SELECTOR, sel))
                    except Exception: pass
            except Exception: continue
        try: drv.switch_to.default_content()
        except Exception: pass
        # Dedup nodes
        try:
            seen=set(); uni=[]
            for n in nodes:
                try: sid=n.id
                except Exception: sid=id(n)
                if sid in seen: continue
                seen.add(sid); uni.append(n)
            nodes=uni
        except Exception: pass
        # Extract messages
        pat = _R.compile(r'(?:Tote/Box|Tote|Box)\s*([A-Za-z0-9_-]+).*?(?:is|has been)?\s*closed\s*successfully', _R.I|_R.S)
        for el in nodes:
            try:
                style=(el.get_attribute('style') or '').lower()
                if 'display: none' in style: continue
                t=(el.text or el.get_attribute('innerText') or el.get_attribute('textContent') or '').strip()
                if not t: continue
                if 'transfer complete' in t.lower():
                    for m in pat.finditer(t): msgs.append((m.group(1).upper(), t))
            except Exception: continue
        # Fallback: page_source text (outer doc only)
        if not msgs:
            try:
                html=drv.page_source or ''
                txt=_R.sub(r'<[^>]+>', ' ', html)
                if 'transfer complete' in txt.lower() and 'closed successfully' in txt.lower():
                    for m in pat.finditer(txt):
                        bid=m.group(1).upper(); msgs.append((bid, f'Tote/Box {bid} is closed successfully'))
            except Exception: pass
    except Exception: pass
    # Dedup pairs
    out=[]; seen=set()
    for bid, t in msgs:
        key=(bid, (t or '').strip())
        if bid and key not in seen:
            seen.add(key); out.append((bid, (t or '').strip()))
    return out

import re as _GP_RE


def _gp_start_close_message_poll_v2(parent_widget, status_var=None, interval_ms: int = 600):
    """Start a Tkinter .after()-based poller watching for the 'Transfer Complete' close message.
    It collects messages from #success-message / common success selectors *and* the legacy global-message path,
    then persists them into stn_final.Pick and marks TL as 'TL Complete'.
    Idempotent per Box-Id within a session.
    """
    processed_boxes = set()
    def _persist_from_pairs(pairs):
        for box_id, msg_text in pairs or []:
            if not box_id or box_id in processed_boxes:
                continue
            try:
                rc = stn_final_update_pick_by_box(box_id, msg_text)
                print('[GO PICK][Watcher] Updated Pick for Box-Id:', box_id, 'rows:', rc)
            except Exception as _e1:
                print('[GO PICK][Watcher] Pick update failed:', _e1)
            # Also set TL Complete
            tl_id = None
            try:
                tl_id = (GO_PICK_STATE.get('tl') if isinstance(GO_PICK_STATE, dict) else None)
            except Exception:
                tl_id = None
            if not tl_id:
                try:
                    drv = globals().get('GUI_ACTIVE_DRIVER')
                    cur_url = getattr(drv, 'current_url', '') or ''
                    m2 = _GP_RE.search(r'/transfer_list/(?:confirm_transfer_list/)?(TL\d+)', cur_url, flags=_GP_RE.I)
                    if m2: tl_id = m2.group(1).upper()
                except Exception:
                    pass
            if not tl_id:
                try:
                    tl_id = stn_final_get_tl_by_box(box_id)
                except Exception:
                    tl_id = None
            if tl_id:
                try:
                    rc2 = stn_final_update_tl_status(tl_id, 'TL Complete')
                    print('[GO PICK][Watcher] TL Complete set for', tl_id, 'rows:', rc2)
                    try:
                        status_var and status_var.set(f'{tl_id}: TL Complete')
                    except Exception:
                        pass
                except Exception as e:
                    print('[GO PICK][Watcher] TL-Id status update failed:', e)
            processed_boxes.add(box_id)
    def _tick():
        try:
            drv = globals().get('GUI_ACTIVE_DRIVER')
            if not drv:
                parent_widget.after(interval_ms, _tick); return
            # Primary: DOM/iframe + page_source collector
            try:
                pairs = _gp_collect_transfer_complete_messages(drv)
            except Exception:
                pairs = []
            _persist_from_pairs(pairs)
            # Secondary: legacy single-message path
            try:
                _gp_try_capture_and_persist_close_message(status_var=status_var, timeout=0)
            except Exception:
                pass
        except Exception as e:
            print('[GO PICK][Watcher] tick error:', e)
        finally:
            try:
                parent_widget.after(interval_ms, _tick)
            except Exception:
                pass
    try:
        parent_widget.after(interval_ms, _tick)
        print('[GO PICK][Watcher] started (robust).')
    except Exception as _se:
        print('[GO PICK][Watcher] start failed:', _se)



def _gp_open_confirm_tl(status_var, force=False, shelf=None):
    """Open the Confirm TL page. If force=True, always navigate to the URL,
    otherwise only navigate when needed. Optionally annotate status with shelf.
    """
    drv, wait = _get_active_driver_wait()
    shelf_note = f" for shelf {shelf}" if shelf else ""
    try:
        status_var.set(f'Opening Confirm TL page{shelf_note}...')
    except Exception:
        pass

    ok = False
    for _attempt in range(2):
        try:
            if force:
                # Force fresh navigation each time a shelf is scanned
                navigate_to_url(drv, CONFIRM_TL_URL, wait)
            else:
                navigate_if_needed(drv, CONFIRM_TL_URL, wait)
            try:
                _ob2_wait_ready(drv, 25)
            except Exception:
                pass
            try:
                cur = (drv.current_url or '').lower()
            except Exception:
                cur = ''
            if '/transfer_list/confirm_transfer_list' in cur:
                ok = True
                break
        except Exception:
            pass
    try:
        status_var.set('Confirm TL is open.' if ok else 'Failed to open Confirm TL page')
    except Exception:
        pass
    return ok

def _type_js(drv, el, text):
    try:
        drv.execute_script("arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('input',{bubbles:true})); arguments[0].dispatchEvent(new Event('change',{bubbles:true}));", el, text)
    except Exception:
        try: el.clear()
        except Exception: pass
        el.send_keys(text)




def _gp_after_shelf_selected(status_var, shelf_label):
    shelf = (shelf_label or '').strip()
    # Always re-open Confirm TL page for each shelf scan
    try:
        _gp_open_confirm_tl(status_var, force=True, shelf=shelf)
    except Exception:
        pass

    drv, _ = _get_active_driver_wait()
    tl = stn_final_tl_for_shelf(shelf)
    if not tl:
        status_var.set(f'No TL found for shelf {shelf}')
        return
    status_var.set(f'TL: {tl} for Shelf: {shelf} → Finding...')
    try:
        tl_input = _GP_Wait(drv, TIMEOUT_MED).until(_GP_EC.presence_of_element_located((_GP_BY.ID, 'filters_transfer_list_id')))
        _type_js(drv, tl_input, tl)
    except Exception as e:
        status_var.set(f'TL input not found: {e}')
        return
    try:
        find_btn = _GP_Wait(drv, TIMEOUT_MED).until(_GP_EC.element_to_be_clickable((_GP_BY.CSS_SELECTOR, "input.uiButton.align-search-btn[name='commit'][type='submit']")))
        drv.execute_script('arguments[0].click();', find_btn)
    except Exception as e:
        status_var.set(f'Find click failed: {e}')
        return
    try:
        _GP_Wait(drv, TIMEOUT_LONG).until(lambda d: 'confirm_transfer_list' in (d.current_url or ''))
        box_id = stn_final_next_unpicked_box_for_tl_shelf(tl, shelf)
        if box_id:
            tote = _GP_Wait(drv, TIMEOUT_MED).until(_GP_EC.presence_of_element_located((_GP_BY.ID, 'tote')))
            _type_js(drv, tote, box_id)
            status_var.set(f'Box auto-filled: {box_id}. Now scan WID or Item.')
        _gp_select_shelf_via_multiselect(drv, shelf)
        try:
            GO_PICK_STATE.update({"tl": tl, "shelf": shelf, "box": box_id})
        except Exception:
            pass
    except Exception as e:
        status_var.set(f'Tote/Shelf step failed: {e}')


from selenium.webdriver.common.by import By as _GP_BY
from selenium.webdriver.support.ui import WebDriverWait as _GP_Wait
from selenium.webdriver.support import expected_conditions as _GP_EC


def _gp_try_capture_and_persist_close_message(status_var=None, timeout=0):
    """If a global success message for closing Tote/Box is present, parse its Box-Id and
    write the message into stn_final.Pick for the specific row (TL,Shelf,Box-Id).
    If written, trigger next-shelf GUI update.
    This can be polled periodically (timeout=0) or waited a few seconds.
    """
    import time, re as _re
    drv = globals().get('GUI_ACTIVE_DRIVER')
    if not drv:
        return False
    pattern = _re.compile(r'Tote/Box\s+(\S+)\s+is closed successfully', _re.I)
    end = time.time() + (timeout or 0)
    while True:
        try:
            elems = drv.find_elements(By.ID, 'global-message') + drv.find_elements(By.ID, 'success-message')
        except Exception:
            elems = []
        text_msg = ''
        for el in elems:
            try:
                cls = (el.get_attribute('class') or '').lower()
                if 'done' in cls or 'msg' in cls:
                    t = (el.text or el.get_attribute('innerText') or '').strip()
                    if t:
                        text_msg = t
                        break
            except Exception:
                continue
        if text_msg:
            m = pattern.search(text_msg)
            if m:
                closed_box = m.group(1).strip().upper()
                state = globals().get('GO_PICK_STATE', {}) or {}
                tl = (state.get('tl') or '').strip()
                shelf = (state.get('shelf') or '').strip()
                expected_box = (state.get('box') or '').strip().upper()
                if expected_box and expected_box != closed_box:
                    try:
                        if status_var: status_var.set(f'Warning: Closed box {closed_box} != expected {expected_box}. Using actual.')
                    except Exception:
                        pass
                try:
                    rows = stn_final_set_pick_by_tl_shelf_box(tl, shelf, closed_box, text_msg)
                except Exception:
                    rows = 0
                if rows > 0:
                    try:
                        globals()['GO_PICK_STATE']['last_closed'] = closed_box
                    except Exception:
                        pass
                    try:
                        nxt = stn_final_next_unpicked_shelf()
                        cb = globals().get('GO_PICK_NEXT_SHELF_CALLBACK')
                        if callable(cb):
                            cb(nxt)
                        if status_var:
                            status_var.set(f'Closed {closed_box}. Updated DB. Next shelf: {nxt or "—"}')
                    except Exception:
                        pass
                    return True
                else:
                    try:
                        if status_var:
                            status_var.set(f'Close message seen for {closed_box} but DB row not found.')
                    except Exception:
                        pass
                    return False
        if timeout <= 0 or time.time() >= end:
            break
        time.sleep(0.25)
    return False




def _find_multiselect_search_input(drv, timeout=10):
    """Locate the visible <input type='search' placeholder='Enter keywords'> used by the multiselect.
    Tries several strategies including iframes and visibility checks. Returns the WebElement or None.
    """
    try:
        drv.switch_to.default_content()
    except Exception:
        pass
    candidates = [
        (_GP_BY.CSS_SELECTOR, "div.ui-multiselect-menu:not([style*='display: none']) input[type='search'][placeholder*='Enter keywords']"),
        (_GP_BY.XPATH, "//div[contains(@class,'ui-multiselect-menu')][not(contains(@style,'display: none'))]//input[@type='search' and contains(@placeholder,'Enter keywords')]"),
        (_GP_BY.CSS_SELECTOR, "input[type='search'][placeholder*='Enter keywords']"),
        (_GP_BY.XPATH, "//input[@type='search' and contains(@placeholder,'Enter keywords')]")
    ]
    for by, sel in candidates:
        try:
            el = _GP_Wait(drv, timeout).until(_GP_EC.visibility_of_element_located((by, sel)))
            return el
        except Exception:
            continue
    try:
        frames = drv.find_elements(_GP_BY.TAG_NAME, 'iframe')
    except Exception:
        frames = []
    for fr in frames:
        try:
            drv.switch_to.default_content(); drv.switch_to.frame(fr)
            for by, sel in candidates:
                try:
                    el = _GP_Wait(drv, 2).until(_GP_EC.visibility_of_element_located((by, sel)))
                    return el
                except Exception:
                    continue
        except Exception:
            continue
    try:
        drv.switch_to.default_content()
    except Exception:
        pass
    return None



def _gp_select_shelf_via_multiselect(drv, shelf_label):
    """Open the multiselect, paste shelf text, then robustly send ENTER+TAB+TAB+ENTER
    with verification and multiple fallbacks. As a last resort, click the matching label/checkbox.
    """
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.keys import Keys as _K
    from selenium.webdriver.common.by import By as _BY
    from selenium.webdriver.support.ui import WebDriverWait as _WAIT
    from selenium.webdriver.support import expected_conditions as _EC
    import time

    text = (shelf_label or '').strip()
    if not text:
        return

    # 1) Open the multiselect dropdown
    try:
        try:
            btn = _WAIT(drv, TIMEOUT_MED).until(_EC.element_to_be_clickable((_BY.CSS_SELECTOR, 'button.ui-multiselect')))
        except Exception:
            btn = _WAIT(drv, TIMEOUT_MED).until(_EC.element_to_be_clickable((_BY.XPATH, "//button[contains(@class,'ui-multiselect')]")))
        drv.execute_script('arguments[0].scrollIntoView({block:"center"});', btn)
        drv.execute_script('arguments[0].click();', btn)
    except Exception:
        # If not found, it may already be open; continue
        pass

    def _menu_open():
        try:
            el = drv.find_element(_BY.XPATH, "//div[contains(@class,'ui-multiselect-menu')]")
            style = el.get_attribute('style') or ''
            return 'display: none' not in style
        except Exception:
            return False

    def _container():
        try:
            return drv.find_element(_BY.XPATH, "//div[contains(@class,'ui-multiselect-menu') and not(contains(@style,'display: none'))]")
        except Exception:
            return None

    def _is_selected():
        # Check checkbox state; fallback to button text containing the selection
        try:
            cont = _container()
            if cont:
                lab = cont.find_element(_BY.XPATH, ".//label[normalize-space()='%s']" % text)
                cb = lab.find_element(_BY.XPATH, ".//input[@type='checkbox']")
                if cb.is_selected():
                    return True
        except Exception:
            pass
        try:
            btxt = (btn.text or btn.get_attribute('innerText') or '').strip()
            return text.lower() in btxt.lower()
        except Exception:
            return False

    # 2) Focus search input and type the text
    search = None
    try:
        search = _find_multiselect_search_input(drv, timeout=TIMEOUT_LONG)
    except Exception:
        search = None
    if not search:
        # Best effort: try again after ensuring menu open
        if not _menu_open():
            try:
                drv.execute_script('arguments[0].click();', btn)
            except Exception:
                pass
        try:
            search = _find_multiselect_search_input(drv, timeout=TIMEOUT_MED)
        except Exception:
            search = None

    if not search:
        # As a last resort, click the label directly if present
        try:
            cont = _container()
            if cont:
                lab = cont.find_element(_BY.XPATH, ".//label[normalize-space()='%s']" % text)
                drv.execute_script('arguments[0].click();', lab)
                return
        except Exception:
            return

    try:
        drv.execute_script('arguments[0].focus();', search)
    except Exception:
        pass
    try:
        search.clear()
    except Exception:
        pass
    try:
        drv.execute_script("arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('input',{bubbles:true})); arguments[0].dispatchEvent(new Event('change',{bubbles:true}));", search, text)
    except Exception:
        try:
            search.send_keys(text)
        except Exception:
            pass

    # 3) Define multiple ways to send the key sequence
    def _seq_on(el):
        try:
            el.send_keys(_K.ENTER)
            time.sleep(0.06)
            el.send_keys(_K.TAB)
            time.sleep(0.06)
            el.send_keys(_K.TAB)
            time.sleep(0.06)
            el.send_keys(_K.ENTER)
            return True
        except Exception:
            return False

    def _seq_actions():
        try:
            ActionChains(drv).send_keys(_K.ENTER).pause(0.08).send_keys(_K.TAB).pause(0.08).send_keys(_K.TAB).pause(0.08).send_keys(_K.ENTER).perform()
            return True
        except Exception:
            return False

    def _seq_active():
        try:
            active = drv.switch_to.active_element
            return _seq_on(active)
        except Exception:
            return False

    def _seq_body():
        try:
            body = drv.find_element(_BY.TAG_NAME, 'body')
            return _seq_on(body)
        except Exception:
            return False

    def _seq_js(el):
        js = (
            "(function(el){"
            "function fire(t,k){var e=new KeyboardEvent(t,{key:k,bubbles:true,cancelable:true});el.dispatchEvent(e);}" \
            "['keydown','keyup'].forEach(function(t){fire(t,'Enter')});" \
            "['keydown','keyup'].forEach(function(t){fire(t,'Tab')});" \
            "['keydown','keyup'].forEach(function(t){fire(t,'Tab')});" \
            "['keydown','keyup'].forEach(function(t){fire(t,'Enter')});" \
            "return true;})(arguments[0]);"
        )
        try:
            return bool(drv.execute_script(js, el))
        except Exception:
            return False

    # 4) Try sequence with retries and verification
    attempts = 6
    for i in range(attempts):
        if not _menu_open():
            try:
                drv.execute_script('arguments[0].click();', btn)
            except Exception:
                pass
            time.sleep(0.1)
        # refresh search ref if it became stale
        try:
            _ = search.is_displayed()
        except Exception:
            try:
                search = _find_multiselect_search_input(drv, timeout=2)
            except Exception:
                search = None
        sent = False
        if search and _seq_on(search):
            sent = True
        elif _seq_actions():
            sent = True
        elif _seq_active():
            sent = True
        elif _seq_body():
            sent = True
        else:
            _seq_js(search or drv.find_element(_BY.TAG_NAME, 'body'))
        time.sleep(0.18)
        if _is_selected():
            break
        # Try direct label click as hard fallback each loop
        try:
            cont = _container()
            if cont:
                lab = cont.find_element(_BY.XPATH, ".//label[normalize-space()='%s']" % text)
                drv.execute_script('arguments[0].click();', lab)
                time.sleep(0.12)
                if _is_selected():
                    break
        except Exception:
            # try first checkbox
            try:
                cb = cont.find_element(_BY.XPATH, ".//input[@type='checkbox']")
                drv.execute_script('arguments[0].click();', cb)
            except Exception:
                pass

    # 5) Close the menu if still open (toggle button)
    try:
        if _menu_open():
            drv.execute_script('arguments[0].click();', btn)
    except Exception:
        pass




def _gp_after_wid_scanned(status_var, shelf_label, wid_text):
    drv, _ = _get_active_driver_wait()
    try:
        wid_input = _GP_Wait(drv, TIMEOUT_MED).until(_GP_EC.presence_of_element_located((_GP_BY.ID, 'input_box')))
        _type_js(drv, wid_input, (wid_text or '').strip())
        status_var.set(f'WID entered: {(wid_text or "").strip()} → Now scan FSN/EAN/Model-Id')
    except Exception as e:
        status_var.set(f'WID input failed: {e}')






# === Added: _gp_type_item_code (fast, parser-safe) ===
def _gp_type_item_code(drv, code_text: str):
    """Type the scanned FSN/EAN/Model-Id into the Confirm TL item code field quickly.
    Tries common ids/names and simple aria-label placeholders; uses JS set for speed.
    Returns True on success, else False.
    """
    from selenium.webdriver.common.by import By as _BY
    from selenium.webdriver.support.ui import WebDriverWait as _WAIT
    from selenium.webdriver.support import expected_conditions as _EC

    code = (code_text or '').strip()
    if not code:
        return False

    locators = [
        (_BY.ID, 'item_barcode'),
        (_BY.NAME, 'item_barcode'),
        (_BY.ID, 'fsn_ean_model'),
        (_BY.NAME, 'fsn_ean_model'),
        (_BY.XPATH, "//input[@type='text' and (contains(@aria-label,'FSN') or contains(@aria-label,'fsn') or contains(@placeholder,'FSN'))]"),
        (_BY.XPATH, "//input[@type='text' and (contains(@aria-label,'EAN') or contains(@aria-label,'ean') or contains(@placeholder,'EAN'))]"),
        (_BY.XPATH, "//input[@type='text' and (contains(@aria-label,'Model') or contains(@aria-label,'model') or contains(@placeholder,'Model'))]")
    ]

    el = None
    try:
        drv.switch_to.default_content()
    except Exception:
        pass

    for by, sel in locators:
        try:
            el = _WAIT(drv, 4).until(_EC.presence_of_element_located((by, sel)))
            if el:
                break
        except Exception:
            el = None

    if el is None:
        # Last resort: active element
        try:
            el = drv.switch_to.active_element
        except Exception:
            el = None
    if el is None:
        return False

    try:
        # JS set is faster and triggers input/change
        drv.execute_script(
            "arguments[0].value = arguments[1];"
            "arguments[0].dispatchEvent(new Event('input',{bubbles:true}));"
            "arguments[0].dispatchEvent(new Event('change',{bubbles:true}));",
            el, code
        )
        return True
    except Exception:
        try:
            try:
                el.clear()
            except Exception:
                pass
            el.send_keys(code)
            return True
        except Exception:
            return False
# === End: _gp_type_item_code ===


# === Added: fast click sequence for GO PICK ===
def _gp_fast_click_sequence(drv, ids=('pick_button','complete_transfer_list','close_tote')):
    """Click Pick -> Complete -> Close quickly using JS, minimal waits.
    Returns True if at least one click succeeded.
    """
    from selenium.webdriver.common.by import By as _BY
    from selenium.webdriver.support.ui import WebDriverWait as _WAIT
    from selenium.webdriver.support import expected_conditions as _EC
    import time

    success = False
    # try each id with short waits, use JS click for speed
    for _id in ids:
        try:
            el = _WAIT(drv, 4).until(_EC.element_to_be_clickable((_BY.ID, _id)))
            try:
                drv.execute_script('arguments[0].scrollIntoView({block:"center"});', el)
            except Exception:
                pass
            try:
                drv.execute_script('arguments[0].click();', el)
            except Exception:
                try:
                    el.click()
                except Exception:
                    continue
            success = True
            # tiny pause so server registers action; keep snappy
            time.sleep(0.15)
        except Exception:
            # fallback: try common XPaths by value/text
            try:
                xp_map = {
                    'pick_button': "//button[@id='pick_button' or contains(.,'Pick')]",
                    'complete_transfer_list': "//*[@id='complete_transfer_list' or contains(.,'Complete')]",
                    'close_tote': "//*[@id='close_tote' or contains(.,'Close') or contains(.,'Close Tote')]",
                }
                xp = xp_map.get(_id, None)
                if xp:
                    el = _WAIT(drv, 3).until(_EC.element_to_be_clickable((_BY.XPATH, xp)))
                    drv.execute_script('arguments[0].click();', el)
                    success = True
                    time.sleep(0.15)
            except Exception:
                continue
    return success
# === End: fast click sequence ===
def _gp_after_code_scanned(status_var, shelf_label, any_code):
    shelf = (shelf_label or '').strip()
    drv, _ = _get_active_driver_wait()
    try:
        tl_input = drv.find_element(_GP_BY.ID, 'filters_transfer_list_id')
        tl = (tl_input.get_attribute('value') or '').strip()
    except Exception:
        tl = stn_final_tl_for_shelf(shelf)

    _ = _gp_type_item_code(drv, (any_code or '').strip())

    ok = stn_final_match_any_code(tl or '', shelf, any_code)
    if not ok:
        status_var.set(f'{any_code} not found for TL/Shelf; verify item or refresh STN-Final data')
        return

    status_var.set(f'{any_code} scanned Successfully → Picking...')

    # Fast sequence: Pick -> Complete -> Close
    try:
        ok_clicks = _gp_fast_click_sequence(drv, ids=('pick_button','complete_transfer_list','close_tote'))
        if not ok_clicks:
            status_var.set('Pick flow clickers not found')
            return
    except Exception as e:
        status_var.set(f'Pick flow failed: {e}')
        return
        

    msg_text = ''
    try:
        m = _GP_Wait(drv, TIMEOUT_LONG).until(_GP_EC.presence_of_element_located((_GP_BY.ID, 'global-message')))
        msg_text = (m.text or '').strip()
    except Exception:
        pass

    if not msg_text:
        status_var.set('Close Tote message not found; cannot update DB')
        return

    import re as _re
    m2 = _re.search(r'Tote/Box\s+([A-Za-z0-9_-]+)\s+is closed successfully', msg_text, flags=_re.I)
    found_box = (m2.group(1) if m2 else '').strip()
    auto_box = None
    try:
        auto_box = (GO_PICK_STATE.get('box') or '').strip()
    except Exception:
        auto_box = ''

    if found_box and auto_box and found_box.upper() == auto_box.upper():
        try:
            stn_final_update_pick_message_for_box(tl or '', shelf, auto_box, msg_text)
            status_var.set(f'Updated Pick for {auto_box}.')
        except Exception:
            status_var.set('DB update failed (by box)')
    else:
        status_var.set('Message box id did not match the auto-filled Box-Id; skipping DB update')

    # Suggest next shelf
    def _compute_next_shelf(cur: str) -> str:
        try:
            lst = stn_final_distinct_shelves_latest_first(limit=500) or []
            if not lst:
                return ''
            idx = next((i for i, s in enumerate(lst) if (s or '').strip().upper() == (cur or '').strip().upper()), None)
            if idx is None:
                return lst[0]
            return lst[(idx + 1) % len(lst)]
        except Exception:
            return ''
    nxt = _compute_next_shelf(shelf)
    if nxt:
        try:
            if 'GO_PICK_NEXT_SHELF_CALLBACK' in globals() and callable(GO_PICK_NEXT_SHELF_CALLBACK):
                GO_PICK_NEXT_SHELF_CALLBACK(nxt)
                status_var.set(f'Done. Next Shelf: {nxt}')
            else:
                status_var.set(f'Done. Next Shelf: {nxt} (scan to proceed)')
        except Exception:
            status_var.set(f'Done. Next Shelf: {nxt}')
    else:
        status_var.set('Done. No further shelves suggested')



def _gp_mark_lost(status_var, shelf_label):
    drv, _ = _get_active_driver_wait()
    try:
        btn = _GP_Wait(drv, TIMEOUT_MED).until(_GP_EC.element_to_be_clickable((_GP_BY.ID, 'mark_lost')))
        drv.execute_script('arguments[0].click();', btn)
        try:
            tl_input = drv.find_element(_GP_BY.ID, 'filters_transfer_list_id')
            tl = (tl_input.get_attribute('value') or '').strip()
        except Exception:
            tl = stn_final_tl_for_shelf((shelf_label or '').strip())
        stn_final_update_pick_message(tl, (shelf_label or '').strip(), 'Mark as Lost')
        status_var.set('Marked as Lost and updated DB')
    except Exception as e:
        status_var.set(f'Mark as Lost failed: {e}')
class AppWindow(tk.Toplevel):
    """Single top-level window. Has Settings bar, Login and Main frames."""
    def __init__(self, master):
        super().__init__(master)
        self.title("IWIT-APP")
        self.geometry("840x720")
        self.resizable(False, False)
        header = draw_header(self, "IWIT Console")
        self.settings_bar = SettingsBar(self, self._on_toggle_headless)
        self.settings_bar.pack(fill="x")
        self.login_frame = LoginFrame(self, self._on_submit_credentials)
        self.main_frame = MainFrame(self)
        self.show_login()

    def show_login(self):
        self.main_frame.pack_forget()
        self.login_frame.pack(fill="both", expand=True)

    def show_main(self):
        self.login_frame.pack_forget()
        self.main_frame.reset_controls(clear_fields=True)
        self.main_frame.pack(fill="both", expand=True)
        try:
            self.main_frame.show_menu()
        except Exception:
            pass
        try:
            start_boxid_watcher_if_ready()
        except Exception:
            pass

    def _detect_login_error(self, driver) -> Optional[str]:
        """Try to read common error messages when login fails (invalid credentials, etc.)"""
        try:
            candidates = [
                (By.CSS_SELECTOR, ".error, .alert-danger, .alert-error"),
                (By.XPATH, "//div[contains(@class,'error') or contains(@class,'alert')]"),
                (By.XPATH, "//*[contains(text(),'Invalid') or contains(text(),'incorrect') or contains(text(),'failed')]"),
            ]
            for by, sel in candidates:
                try:
                    el = driver.find_element(by, sel)
                    txt = (el.text or "").strip()
                    if txt:
                        return txt
                except Exception:
                    continue
        except Exception:
            pass
        return None

    
    def _on_submit_credentials(self, username, password):
        global GUI_ACTIVE_DRIVER, SELECTED_WH_FULL, SELECTED_WH_SHORT
        def run_auth():
            driver = GUI_ACTIVE_DRIVER
            try:
                self.login_frame.set_status("Opening login page…")
                auth = AuthHelper(driver, _SimpleGetter(username), _SimpleGetter(password))
                # Always fresh login: no session reuse
                auth.do_login()
                time.sleep(0.8)
                err = self._detect_login_error(driver)
                if err:
                    self.login_frame.set_status(f"Login error: {err}", is_error=True)
                    return
                if on_mfa_page(driver):
                    self.login_frame.set_status("MFA detected. Choose SMS or EMAIL to receive OTP.")
                    self.login_frame.show_otp_section(driver)
                    deadline = time.time() + 200
                    while time.time() < deadline and on_mfa_page(driver):
                        time.sleep(0.6)
                    if on_mfa_page(driver):
                        self.login_frame.set_status("MFA not completed in time. Please try again.", is_error=True)
                        return
                self.login_frame.set_status("Verifying warehouse access in WMS…")
                if not ensure_home_loaded(driver, build_wait(driver, TIMEOUT_MED), retries=4, sleep_between=0.7):
                    self.login_frame.set_status("HOME did not load after login.", is_error=True)
                    return
                self.show_main()
            except SystemExit:
                pass
            except Exception as e:
                self.login_frame.set_status(f"Unexpected error: {e}", is_error=True)
        threading.Thread(target=run_auth, daemon=True).start()

    def _on_toggle_headless(self, mode: str):
        """
        Toggle headless advanced mode from Settings bar and restart the browser.
        mode: "2" for advanced headless, "0" for UI.
        """
        global HEADLESS_ENV
        HEADLESS_ENV = mode
        self.restart_browser_for_login()

    def restart_browser_for_login(self):
        global GUI_ACTIVE_DRIVER
        try:
            logout_session(GUI_ACTIVE_DRIVER)
        except Exception:
            pass
        driver, wait = launch_browser()
        if not driver:
            # Reflect in login GUI (no pop-up)
            try:
                self.login_frame.set_status("Could not launch Chrome. Please check console.", is_error=True)
            except Exception:
                print("Could not launch Chrome. Please check console.")
            return
        GUI_ACTIVE_DRIVER = driver
        self.show_login()


# === BEGIN: BOX-ID attach & schema/index helpers (patched by Copilot) ===
# Definitive patch v3: single-row UPDATE or INSERT for BOX-ID, partial unique index,
# route_code support, and explicit logging (no silent swallow).

# Upgrade table by adding any missing columns (safe no-op if present)
def _ob2_upgrade_table_add_missing_columns(db_path: str = None):
    db_path = db_path or _OB2_DB_PATH
    try:
        if _ob2_sqlite3 is None:
            return
    except NameError:
        return
    with _ob2_sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        def _has(col):
            cur.execute("PRAGMA table_info(stn_tl_scrape);")
            return col.lower() in [r[1].lower() for r in cur.fetchall()]
        def _add(col, ddl):
            try:
                if not _has(col):
                    cur.execute(f"ALTER TABLE stn_tl_scrape ADD COLUMN {ddl};")
            except Exception:
                pass
        _add('stn', 'stn TEXT')
        _add('source', 'source TEXT')
        _add('destination', 'destination TEXT')
        _add('box_id', 'box_id TEXT')
        _add('pick', 'pick INTEGER DEFAULT 0')
        _add('tl_complete_status', 'tl_complete_status TEXT')
        _add('pack', 'pack INTEGER DEFAULT 0')
        _add('dispatch', 'dispatch INTEGER DEFAULT 0')
        _add('route_code', 'route_code TEXT')
        conn.commit()

# Create indexes and PARTIAL UNIQUE index (only when box_id is non-empty)
def _ob2_ensure_indexes(db_path: str = None):
    db_path = db_path or _OB2_DB_PATH
    try:
        if _ob2_sqlite3 is None:
            return
    except NameError:
        return
    with _ob2_sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        try:
            cur.execute("CREATE INDEX IF NOT EXISTS ix_scrape_tl ON stn_tl_scrape(UPPER(tl));")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_scrape_box ON stn_tl_scrape(UPPER(box_id));")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_scrape_stn ON stn_tl_scrape(UPPER(stn));")
            cur.execute("CREATE INDEX IF NOT EXISTS ix_scrape_route ON stn_tl_scrape(UPPER(route_code));")
            # Drop broader unique index if it exists
            try:
                cur.execute("DROP INDEX IF EXISTS uq_scrape_tl_box;")
            except Exception:
                pass
            # Partial unique to avoid blocking empty box_id rows
            cur.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_scrape_tl_box_nonempty "
                "ON stn_tl_scrape(UPPER(tl), UPPER(box_id)) "
                "WHERE TRIM(COALESCE(box_id,''))<>'';"
            )
        except Exception:
            pass
        conn.commit()

# Optional: update route_code for existing TL rows (first empty route only)
def _db_update_scrape_with_route(tl: str, route_code: str = None, db_path: str = None):
    db_path = db_path or _OB2_DB_PATH
    try:
        if _ob2_sqlite3 is None or not tl or not route_code:
            return False
    except NameError:
        return False
    try:
        _ob2_ensure_table(db_path)
        _ob2_upgrade_table_add_missing_columns(db_path)
    except Exception:
        pass
    try:
        with _ob2_sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE stn_tl_scrape SET route_code=? WHERE UPPER(tl)=UPPER(?) AND (route_code IS NULL OR TRIM(route_code)='')",
                ((route_code or '').strip(), (tl or '').strip())
            )
            conn.commit()
            return True
    except Exception as e:
        print('[DB][WARN] _db_update_scrape_with_route:', e)
        return False

# Robust attach that always persists a new box-id; updates ONE empty-slot row else inserts

def _db_attach_box_id_to_scrape(
    tl: str,
    box_id: str,
    stn: str = None,
    source: str = None,
    destination: str = None,
    shelf: str = None,
    route_code: str = None,
    db_path: str = None
) -> bool:
    db_path = db_path or _OB2_DB_PATH
    try:
        if _ob2_sqlite3 is None:
            print('[DB][ERROR] sqlite3 module not available')
            return False
    except NameError:
        print('[DB][ERROR] sqlite3 handle missing')
        return False
    if not tl or not box_id:
        print('[DB][WARN] attach_box_id: missing tl or box_id')
        return False
    try:
        _ob2_ensure_table(db_path)
        _ob2_upgrade_table_add_missing_columns(db_path)
        _ob2_ensure_indexes(db_path)
    except Exception as e:
        print('[DB][WARN] ensure schema/index:', e)

    tl_u = (tl or '').strip()
    bx_u = (box_id or '').strip()
    stn_v = (stn or '').strip() or None
    src_v = (source or '').strip() or None
    dst_v = (destination or '').strip() or None
    shelf_v = (shelf or '').strip() or None
    route_v = (route_code or '').strip() or None

    try:
        with _ob2_sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            # Select exactly ONE candidate empty-slot row for this TL (optionally matching shelf)
            if shelf_v:
                cur.execute(
                    """
                    SELECT id FROM stn_tl_scrape
                    WHERE UPPER(tl)=UPPER(?)
                      AND (box_id IS NULL OR TRIM(box_id)='')
                      AND (shelf IS NULL OR UPPER(shelf)=UPPER(?))
                    ORDER BY id ASC
                    LIMIT 1;
                    """,
                    (tl_u, shelf_v)
                )
            else:
                cur.execute(
                    """
                    SELECT id FROM stn_tl_scrape
                    WHERE UPPER(tl)=UPPER(?)
                      AND (box_id IS NULL OR TRIM(box_id)='')
                    ORDER BY id ASC
                    LIMIT 1;
                    """,
                    (tl_u,)
                )
            row = cur.fetchone()

            if row and row[0]:
                rid = int(row[0])
                set_cols = ["box_id=?"]
                set_vals = [bx_u]
                if stn_v: set_cols.append("stn=?"); set_vals.append(stn_v)
                if src_v: set_cols.append("source=?"); set_vals.append(src_v)
                if dst_v: set_cols.append("destination=?"); set_vals.append(dst_v)
                if route_v: set_cols.append("route_code=?"); set_vals.append(route_v)
                cur.execute(f"UPDATE stn_tl_scrape SET {', '.join(set_cols)} WHERE ROWID=?;", set_vals + [rid])
                conn.commit()
                print(f"[DB] stn_tl_scrape updated id={rid} tl={tl_u} box_id={bx_u}")
                return True

            # No empty-slot row → INSERT a new historical row for this TL/box
            cur.execute(
                """
                INSERT INTO stn_tl_scrape
                (tl, wid, fsn, title, category, qty, shelf, stn, source, destination, box_id, pick, tl_complete_status, pack, dispatch, route_code)
                VALUES (?, NULL, NULL, NULL, NULL, NULL, ?, ?, ?, ?, ?, 0, NULL, 0, 0, ?)
                """,
                (tl_u, shelf_v, stn_v, src_v, dst_v, bx_u, route_v)
            )
            conn.commit()
            print(f"[DB] stn_tl_scrape inserted tl={tl_u} box_id={bx_u}")
            return True
    except Exception as e:
        print(f"[DB][ERROR] _db_attach_box_id_to_scrape: {e}")
        return False

# Backfill boxes from pack table into stn_tl_scrape (optional one-time)

def backfill_box_ids_from_pack_table(db_path: str = None):
    db_path = db_path or _OB2_DB_PATH
    try:
        if _ob2_sqlite3 is None:
            return 0
    except NameError:
        return 0
    try:
        _ob2_ensure_table(db_path)
        _ob2_upgrade_table_add_missing_columns(db_path)
        _ob2_ensure_indexes(db_path)
    except Exception:
        pass
    imported = 0
    try:
        with _ob2_sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            try:
                pack_tbl = _PACK_TABLE
            except Exception:
                pack_tbl = 'pack_stn_items'
            try:
                cur.execute(f"SELECT stn, tl_id, box_id FROM {pack_tbl} WHERE TRIM(COALESCE(box_id,''))<>'';")
                rows = cur.fetchall()
            except Exception:
                rows = []
            for stn, tl, box in rows:
                if tl and box:
                    if _db_attach_box_id_to_scrape(tl, box, stn=stn):
                        imported += 1
    except Exception as e:
        print('[DB][ERROR] backfill_box_ids_from_pack_table:', e)
    print(f"[DB] Backfill complete: {imported} BOX-ID(s) attached to stn_tl_scrape.")
    return imported
# === END PATCH: BOX-ID attach & schema/index helpers ===


# ============================ STN-Final DB (create-once)

# --- STN-Final inserts (row-per-quantity) -------------------------------------
# For each scraped line-item, we will split Qty into N rows (Qty=1 per row)
# and store user-id, STN, TL-Id, Shelf, Catagory, WID, FSN, and Title->EAN.

def stn_final_insert_item_rows(user_id: str, stn: str, tl_id: str, items):
    """Insert rows into STN-Final.stn_final.
    items: iterable of tuples -> (wid, fsn, title, category, qty, shelf)
    For each item, split Qty into N rows of Qty=1, duplicating other fields.
    """
    import sqlite3
    try:
        with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            sql = """
            INSERT INTO stn_final ("user-id","STN","TL-Id","Qty","Shelf","Catagory","WID","FSN","EAN")
            VALUES (?,?,?,?,?,?,?,?,?)
            """
            total_rows = 0
            for (wid, fsn, title, category, qty, shelf) in (items or []):
                try:
                    n = int(qty) if qty is not None else 0
                except Exception:
                    n = 0
                n = n if n > 0 else 1
                for _ in range(n):
                    cur.execute(
                        sql,
                        (
                            (user_id or '').strip(),
                            (stn or '').strip(),
                            (tl_id or '').strip(),
                            1,  # per requirement: store 1 in each row
                            (shelf or ''),
                            (category or ''),
                            (wid or ''),
                            (fsn or ''),
                            (title or ''),  # Title goes into EAN column as requested
                        ),
        )
                    total_rows += 1
            conn.commit()
            try:
                stn_final_sort_by_shelf_az()
            except Exception as _e_sort:
                print('[STN-Final][WARN] post-insert sort failed:', _e_sort)
        print(f"[STN-Final] Inserted {total_rows} row(s) for STN={stn} TL={tl_id} user={user_id}.")
        return total_rows
    except Exception as e:
        print('[STN-Final][ERROR] insert failed:', e)
        return 0
# =======================
# Creates a separate SQLite database named "STN-Final" the first time the app runs
# and reuses it on subsequent runs. Only schema is created here—no data is inserted.
STN_FINAL_DB_PATH = 'STN-Final'  # keep filename exactly as requested
STN_FINAL_TABLE = 'stn_final'

def init_stn_final_db():
    import sqlite3, os
    created = not os.path.exists(STN_FINAL_DB_PATH)
    with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS stn_final (
                "user-id" TEXT,
                "STN" TEXT,
                "TL-Id" TEXT,
                "Qty" INTEGER,
                "Shelf" TEXT,
                "Catagory" TEXT,
                "WID" TEXT,
                "FSN" TEXT,
                "EAN" TEXT,
                "Model-Id" TEXT,
                "Source" TEXT,
                "Destination" TEXT,
                "Box-Id" TEXT,
                "Pick" INTEGER,
                "TL-Id status" TEXT,
                "Pack" INTEGER,
                "Consignment-Id" TEXT,
                "Dispatch" INTEGER
            );
            """
        )
        conn.commit()
    if created:
        print("✅ Created new SQLite database 'STN-Final' and table 'stn_final'.")
    else:
        print("ℹ️ Using existing SQLite database 'STN-Final'.")

# ============================ Main Application Entry ==========================



# === NEW: Persistent Shelf A→Z sorter for STN-Final ===

def stn_final_sort_by_shelf_az(db_path: str = STN_FINAL_DB_PATH, table: str = STN_FINAL_TABLE) -> bool:
    """Permanently reorder rows in STN-Final.stn_final by Shelf (A–Z), irrespective of STN/TL/etc.)."""
    import sqlite3, re as _re
    if not db_path:
        return False
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute('PRAGMA foreign_keys=OFF;')
            cur = conn.cursor()
            # Capture schema & indexes
            cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?;", (table,))
            row = cur.fetchone()
            if not row or not row[0]:
                print('[STN-Final][WARN] Table not found for sorting:', table)
                return False
            create_sql = row[0]
            cur.execute("SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL;", (table,))
            index_ddls = cur.fetchall() or []
            temp_table = f"{table}__sorted"
            try:
                cur.execute(f'DROP TABLE IF EXISTS {temp_table};')
            except Exception:
                pass
            # Build temp table DDL
            create_sql_new = _re.sub(r'^(\s*CREATE\s+TABLE\s+)"?'+_re.escape(STN_FINAL_TABLE)+r'"?(\s*\()', r"\1"+temp_table+r"\2", create_sql, flags=_re.I)
            if create_sql_new == create_sql:
                create_sql_new = create_sql.replace(STN_FINAL_TABLE, temp_table, 1)
            cur.execute(create_sql_new)
            # Insert sorted
            cur.execute(
                f'INSERT INTO {temp_table} SELECT * FROM {table} ' +
                'ORDER BY (CASE WHEN "Shelf" IS NULL OR TRIM("Shelf")="" THEN 1 ELSE 0 END), ' +
                'UPPER("Shelf") ASC, rowid ASC;'
            )
            cur.execute(f'DROP TABLE {table};')
            cur.execute(f'ALTER TABLE {temp_table} RENAME TO {table};')
            # Recreate indexes
            for name, ddl in index_ddls:
                try:
                    cur.execute(ddl)
                except Exception:
                    try:
                        cur.execute(ddl.replace(temp_table, table))
                    except Exception:
                        pass
            conn.commit()
            print('[STN-Final] Sorted by Shelf (A–Z) and saved.')
            return True
    except Exception as e:
        print('[STN-Final][ERROR] sort by Shelf failed:', e)
        return False

# ============================ CSV-run Box Creation Helpers ============================

def stn_final_count_rows_for_stns(stns):
    if not stns:
        return 0
    try:
        import sqlite3
        with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            qmarks = ','.join(['?'] * len(stns))
            cur.execute(f'SELECT COUNT(*) FROM {STN_FINAL_TABLE} WHERE UPPER("STN") IN (' + qmarks + ')',
                        tuple(s.upper().strip() for s in stns))
            row = cur.fetchone()
            return int(row[0] or 0)
    except Exception as e:
        print('[STN-Final][ERROR] count rows for STNs failed:', e)
        return 0


def stn_final_fetch_rowids_for_stns_without_box(stns, limit=None):
    out = []
    if not stns:
        return out
    try:
        import sqlite3
        with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            order_cases = ' '.join([f"WHEN UPPER(\"STN\")=UPPER(?) THEN {i}" for i,_ in enumerate(stns)])
            params = [s for s in stns]
            sql = (
                f'SELECT rowid, "STN" FROM {STN_FINAL_TABLE} '
                f'WHERE UPPER("STN") IN (' + ','.join(['UPPER(?)']*len(stns)) + ') '
                f'AND ("Box-Id" IS NULL OR TRIM("Box-Id")="") '
                f'ORDER BY CASE {order_cases} ELSE {len(stns)} END, rowid'
            )
            params += stns
            if limit:
                sql += f' LIMIT {int(limit)}'
            cur.execute(sql, tuple(params))
            for r in cur.fetchall():
                out.append((int(r['rowid']), r['STN']))
    except Exception as e:
        print('[STN-Final][ERROR] fetch rowids without Box-Id failed:', e)
    return out


def stn_final_assign_box_ids_to_stns(box_ids, stns):
    if not box_ids or not stns:
        return 0
    try:
        import sqlite3
        with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            targets = stn_final_fetch_rowids_for_stns_without_box(stns, limit=len(box_ids))
            updated = 0
            for (rowid, _stn), bx in zip(targets, box_ids):
                try:
                    cur.execute(f'UPDATE {STN_FINAL_TABLE} SET "Box-Id"=? WHERE rowid=?', (bx, rowid))
                    updated += 1
                except Exception as _e:
                    print('[STN-Final][WARN] single row update failed:', _e)
            conn.commit()
            if updated < len(box_ids):
                print(f"[STN-Final][INFO] Only {updated}/{len(box_ids)} box ids were assigned (fewer empty rows).")
            else:
                print(f"[STN-Final] Assigned {updated} box ids to STN-Final rows.")
            return updated
    except Exception as e:
        print('[STN-Final][ERROR] assign box ids failed:', e)
        return 0


def open_box_creation_and_generate(driver, wait, total_qty):
    if not total_qty or int(total_qty) <= 0:
        return False
    if not navigate_if_needed(driver, BOX_CREATION_URL, wait):
        return False
    try:
        qty_el = WebDriverWait(driver, TIMEOUT_LONG).until(EC.presence_of_element_located((By.ID, 'quantity')))
        try:
            qty_el.clear()
        except Exception:
            pass
        qty_el.send_keys(str(int(total_qty)))
        gen_btn = None
        for by, sel in [
            (By.XPATH, "//input[@type='submit' and @value='Generate']"),
            (By.CSS_SELECTOR, "input.uiButton.align-search-btn[name='commit'][value='Generate']"),
        ]:
            try:
                gen_btn = WebDriverWait(driver, TIMEOUT_MED).until(EC.element_to_be_clickable((by, sel)))
                break
            except Exception:
                continue
        if not gen_btn:
            return False
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", gen_btn)
        driver.execute_script("arguments[0].click();", gen_btn)
        handle_popups(driver)
        WebDriverWait(driver, TIMEOUT_LONG).until(
            EC.presence_of_element_located((By.XPATH, "//table[contains(@class,'sticky-header')]//tbody/tr[1]"))
        )
        return True
    except Exception as e:
        print('[BOX-CREATE][ERROR] open/generate failed:', e)
        return False


def scrape_generated_box_ids(driver, timeout=TIMEOUT_LONG):
    out = []
    try:
        rows = WebDriverWait(driver, timeout).until(
            EC.presence_of_all_elements_located((By.XPATH, "//table[contains(@class,'sticky-header')]//tbody/tr"))
        )
        for tr in rows:
            try:
                tds = tr.find_elements(By.XPATH, './td')
                if len(tds) >= 2:
                    bx = (tds[1].text or '').strip()
                    if bx:
                        out.append(bx)
            except Exception:
                continue
        return out
    except Exception as e:
        print('[BOX-CREATE][WARN] scrape box ids failed:', e)
        return out
def start_application():

    # Ensure scraper DB indexes exist (box-id updates are fast and robust)
    try:
        _ob2_ensure_indexes()
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass
    global GUI_ROOT, GUI_ACTIVE_DRIVER
    root = tk.Tk()
    GUI_ROOT = root
    root.withdraw()
    try:
        root._flipkart_logo_img = load_image_logo()
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass

    driver, wait = launch_browser()
    if not driver:
        print("Launch Failed: Could not launch Chrome. Please check console.")
        sys.exit(1)

    GUI_ACTIVE_DRIVER = driver
    app = AppWindow(root)
    root.mainloop()


# ============================ PATCH: STN-TL-Sort & robust scraping =============
# Sorting helpers (Shelf a-z,0-9; then Category a-z)
import re as _patch_re

def _ob2_natural_key(text: str):
    s = (text or '').strip().lower()
    parts = _patch_re.split(r'(\d+)', s)
    return tuple(int(p) if p.isdigit() else p for p in parts)

def _ob2_sort_key_shelf_category(shelf: str, category: str):
    return (
        0 if (shelf or '').strip() else 1,
        _ob2_natural_key(shelf),
        (category or '').strip().lower(),
    )

# Ensure table + sort-before-insert in one place
try:
    _old__ob2_insert_rows = _ob2_insert_rows
except NameError:
    _old__ob2_insert_rows = None

def _ob2_insert_rows(db_path: str, rows):
    # Single-table mode: no-op
    return

def _ob2_get_tl_id_from_url(driver):
    try:
        url = driver.current_url or ''
    except Exception:
        url = ''
    m = _TL_URL_RE.search(url)
    return (m.group(1).upper() if m else '')



# ============== NEW HELPERS: Track Request auto-fill from stored STN ==============
try:
    _LAST_KNOWN_TL_ID
except NameError:
    _LAST_KNOWN_TL_ID = None

def open_track_request_with_stn_for_tl(driver, tl_id, wait):
    """Navigate to OUTBOUND_TRACK_URL and auto-fill the STN stored for this TL.
    Returns True on success (input filled and Track clicked)."""
    try:
        tl = (tl_id or '').strip().upper()
        if not tl:
            return False
        stn = _db_get_stn_for_tl(tl)
        if not stn:
            try:
                tl2 = _ob2_get_tl_id_from_url(driver)
                if tl2 and tl2.upper() == tl:
                    s2 = _gp_scrape_stn_from_tl_page(driver, tl)
                    if s2:
                        _db_put_tl_map(tl, stn=s2)
                        stn = s2
            except Exception:
                pass
        if not stn:
            return False
        if not navigate_if_needed(driver, OUTBOUND_TRACK_URL, wait):
            return False
        return _ob2_track_request_for_stn(driver, stn)
    except Exception:
        return False

def open_track_request_for_current_tl(driver, wait):
    """Convenience: Resolve TL from current URL, look up stored STN, then open/auto-fill Track page."""
    try:
        tl = _ob2_get_tl_id_from_url(driver)
        if not tl:
            tl = globals().get('_LAST_KNOWN_TL_ID')
        return open_track_request_with_stn_for_tl(driver, tl, wait)
    except Exception:
        return False

    """Convenience: Resolve TL from current URL, look up stored STN, then open/auto-fill Track page."""
    try:
        tl = _ob2_get_tl_id_from_url(driver)
        if not tl:
            tl = globals().get('_LAST_KNOWN_TL_ID')
        return open_track_request_with_stn_for_tl(driver, tl, wait)
    except Exception:
        return False
def _ob2_scrape_current_tl_page(driver, db_path):
    tl_id = _ob2_get_tl_id_from_url(driver)
    if not tl_id:
        return 0
    try:
        _ob2_wait_ready(driver, 20)
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass
    items = _ob2_extract_item_rows(driver) or []
    if not items:
        return 0
    payload = [(tl_id, wid, fsn, title, category, qty, shelf)
               for (wid, fsn, title, category, qty, shelf) in items]
    _ob2_insert_rows(db_path, payload)
    return len(payload)

# Auto-scrape watcher for any TL page opened manually
class _TLWatcher:
    def __init__(self, driver, db_path, status_cb=None, interval=0.8):
        self.driver = driver
        self.db_path = db_path
        self.status_cb = status_cb
        self.interval = interval
        self._running = False
        self._last_url = None
        self._th = None
    def _log(self, msg):
        try:
            self.status_cb and self.status_cb(msg)
        except Exception:
            pass
    def _loop(self):
        import time
        while self._running:
            try:
                url = ''
                try:
                    url = self.driver.current_url or ''
                except Exception:
                    url = ''
                if url and '/transfer_list/TL' in url and url != self._last_url:
                    try:
                        cnt = _ob2_scrape_current_tl_page(self.driver, self.db_path)
                        if cnt:
                            self._log(f"[Watcher] Auto-scraped {cnt} item(s) from TL page")
                    except Exception:
                        pass
                    self._last_url = url
                time.sleep(self.interval)
            except Exception:
                time.sleep(self.interval)
    def start(self):
        if self._running: return
        self._running = True
        import threading
        self._th = threading.Thread(target=self._loop, daemon=True)
        self._th.start()
    def stop(self):
        self._running = False

_TL_WATCHER = None

def _start_tl_page_watcher(driver, db_path, status_cb=None):
    global _TL_WATCHER
    try:
        if _TL_WATCHER:
            _TL_WATCHER.stop()
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass
    _TL_WATCHER = _TLWatcher(driver, db_path, status_cb=status_cb)
    _TL_WATCHER.start()
    return True

# Start watcher after main shows
# [single-tab] TL page watcher hook disabled

# Multi-tab: after assign flow, best-effort scrape for current tab
# [single-tab] removed multi-tab patch wrapper for _stn_tl_assign_open_link

# Ensure PACK table constant exists
if '_PACK_TABLE' not in globals():
    _PACK_TABLE = 'pack_stn_items'

# ============================ Entry Point (after patches) =======================



# === GO PICK last-known S/D for stamping (globals) ===
_GP_LAST_STN = None
_GP_LAST_SRC = None
_GP_LAST_DEST = None

# ============================ GO PICK (runtime attach) =========================
# GO PICK attach (v5)
# - Guarantees GO PICK button in menu (show_menu patched)
# - Lazy init of view state
# - Parallel tab prepares Track Request page
# - Shelf scan -> fill TL, click Find, select shelf from multiselect
# - Box scan -> fill tote; shows suggested WID (DB)
# - WID scan -> fill WID; auto-click Pick -> Pick Items -> Close Tote/Box
# - Track flow thread: resolve STN (confirm page -> TL detail), fill Track, scrape S/D, persist to SQLite, open Box Create & print 1/1
# - Persists TL↔STN and S/D in new table tl_stn_map, and **also adds columns stn/source/dest** to stn_tl_scrape and updates rows for that TL
# - Added detailed print() logs at each step for traceability

try:
    CONFIRM_TL_URL
except NameError:
    CONFIRM_TL_URL = 'http://10.24.1.53/transfer_list/confirm_transfer_list'

# -------------------------- Selenium helpers --------------------------



# === ONE-TIME shelf keywords commit helpers (ENTER, TAB, TAB, ENTER) ===
from selenium.webdriver.common.keys import Keys as _GP_Keys

def _gp__focus_keywords_input(driver, timeout=4):
    """Ensure <input type='search' placeholder='Enter keywords'> exists and is focused; return element or None."""
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        el = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.XPATH, "//input[@type='search' and @placeholder='Enter keywords']"))
        )
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        except Exception:
            pass
        try:
            el.click()
        except Exception:
            pass
        try:
            driver.execute_script("arguments[0].focus();", el)
        except Exception:
            pass
        return el
    except Exception:
        return None

def _gp__menu_visible(driver):
    try:
        return driver.execute_script(
            """
            const el = document.getElementById('shelf_multiple_select');
            if(!el) return false;
            const menu = el.closest('.ui-multiselect-menu');
            if(!menu) return false;
            const cs = getComputedStyle(menu);
            return !(cs.display==='none' || cs.visibility==='hidden');
            """
        )
    except Exception:
        return False

def _gp__keyboard_select_verify(driver, shelf_text, wait_secs=1.2):
    """Return True if selection seems committed (menu closed OR radio checked OR aria-selected)."""
    import time as _t
    from selenium.webdriver.common.by import By
    t0 = _t.time()
    sel = (shelf_text or '').strip()
    while _t.time() - t0 < max(0.3, float(wait_secs)):
        # Menu closed => success
        if not _gp__menu_visible(driver):
            return True
        # Radio checked?
        try:
            if sel:
                q = f"#shelf_multiple_select input[type='radio'][value='{sel}']"
                for r in driver.find_elements(By.CSS_SELECTOR, q):
                    try:
                        if r.is_selected():
                            return True
                    except Exception:
                        pass
        except Exception:
            pass
        # aria-selected / active option
        try:
            if sel:
                node = driver.find_elements(
                    By.XPATH,
                    f"//ul[@id='shelf_multiple_select']//li//*[normalize-space(text())='{sel}' and (@aria-selected='true' or ancestor::li[contains(@class,'ui-state-active')])]"
                )
                if node:
                    return True
        except Exception:
            pass
        _t.sleep(0.08)
    return False

def _gp__windows_send_once_enter_tab_tab_enter(driver):
    """Windows-only: bring Chrome to front and send ENTER, TAB, TAB, ENTER once via SendInput."""
    import platform
    if platform.system() != 'Windows':
        return False
    try:
        try:
            driver.execute_script('window.focus();')
        except Exception:
            pass
        try:
            import ctypes, ctypes.wintypes as wt
            user32 = ctypes.windll.user32
            EnumWindows = user32.EnumWindows
            EnumWindowsProc = ctypes.WINFUNCTYPE(ctypes.c_bool, wt.HWND, wt.LPARAM)
            GetWindowText = user32.GetWindowTextW
            GetWindowTextLength = user32.GetWindowTextLengthW
            IsWindowVisible = user32.IsWindowVisible
            SetForegroundWindow = user32.SetForegroundWindow
            try:
                cur_title = driver.title or ''
            except Exception:
                cur_title = ''
            def foreach(hwnd, lParam):
                if IsWindowVisible(hwnd):
                    ln = GetWindowTextLength(hwnd)
                    if ln > 0:
                        buf = ctypes.create_unicode_buffer(ln + 1)
                        GetWindowText(hwnd, buf, ln + 1)
                        t = buf.value
                        if t and ((cur_title and cur_title in t) or 'Chrome' in t):
                            try:
                                SetForegroundWindow(hwnd)
                            except Exception:
                                pass
                            return False
                return True
            EnumWindows(EnumWindowsProc(foreach), 0)
        except Exception:
            pass
        import time as _t
        import ctypes
        user32 = ctypes.windll.user32
        KEYEVENTF_KEYUP = 0x0002
        VK_TAB = 0x09
        VK_RETURN = 0x0D
        def tap(vk):
            user32.keybd_event(vk, 0, 0, 0); _t.sleep(0.03)
            user32.keybd_event(vk, 0, KEYEVENTF_KEYUP, 0); _t.sleep(0.03)
        # ONE-TIME sequence: ENTER, TAB, TAB, ENTER
        tap(VK_RETURN); tap(VK_TAB); tap(VK_TAB); tap(VK_RETURN)
        print('[GO PICK] System keyboard (Windows) ENTER, TAB, TAB, ENTER sent (one shot)')
        return True
    except Exception:
        return False

def _gp_fire_once_confirm(driver, expected_text: str, verify_secs: float = 1.2):
    """ONE-TIME keyboard commit (ENTER, TAB, TAB, ENTER) then verify. No additional keyboard retries.
    If verification fails, falls back to DOM click on the matching option (no extra key presses)."""
    import platform
    import time as _t
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.webdriver.common.by import By

    # Ensure input focused and expected text visible (scanner latency)
    t0 = _t.time(); exp = (expected_text or '').strip()
    while _t.time() - t0 < 2.0:
        el = _gp__focus_keywords_input(driver, timeout=1)
        if not el:
            _t.sleep(0.1)
            continue
        try:
            val = el.get_attribute('value') or ''
        except Exception:
            val = ''
        if (not exp) or (exp.lower() in val.lower()):
            break
        _t.sleep(0.1)

    # ONE keyboard send only: prefer OS keys on Windows; else ActionChains
    sent = False
    if platform.system() == 'Windows':
        sent = _gp__windows_send_once_enter_tab_tab_enter(driver)
    else:
        try:
            ActionChains(driver).send_keys(_GP_Keys.ENTER).pause(0.06).send_keys(_GP_Keys.TAB).pause(0.06).send_keys(_GP_Keys.TAB).pause(0.06).send_keys(_GP_Keys.ENTER).perform()
            print('[GO PICK] ActionChains ENTER, TAB, TAB, ENTER sent (one shot)')
            sent = True
        except Exception as e:
            print(f'[GO PICK][WARN] ActionChains one-shot failed: {e}')
            sent = False

    # Verify effect
    ok = _gp__keyboard_select_verify(driver, exp, wait_secs=verify_secs)
    if ok:
        return True

    # No more keyboard sends; try DOM click to commit without keys
    try:
        if exp:
            # Try radio
            sel = driver.find_elements(By.CSS_SELECTOR, f"#shelf_multiple_select input[type='radio'][value='{exp}']")
            if sel:
                driver.execute_script('arguments[0].click();', sel[0])
                return True
            # Try span/label
            span = driver.find_elements(By.XPATH, f"//ul[@id='shelf_multiple_select']//span[normalize-space(text())='{exp}']")
            if span:
                driver.execute_script('arguments[0].click();', span[0])
                return True
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass
    return False

def _gp_set_input_value(driver, locator, value, timeout=15):
    print(f"[GO PICK] set_input {locator} := {value}")
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    try:
        el = WebDriverWait(driver, timeout).until(EC.presence_of_element_located(locator))
    except Exception as e:
        print(f"[GO PICK][WARN] element not found for {locator}: {e}")
        return False
    try:
        driver.execute_script(
            """
            const el = arguments[0], val = arguments[1];
            const proto = Object.getPrototypeOf(el);
            const desc = Object.getOwnPropertyDescriptor(proto, 'value');
            if (desc && desc.set) { desc.set.call(el, val); } else { el.value = val; }
            el.dispatchEvent(new Event('input', {bubbles:true}));
            el.dispatchEvent(new Event('change', {bubbles:true}));
            """,
            el, value
        )
        return True
    except Exception as e:
        print(f"[GO PICK][WARN] JS set failed for {locator}: {e} -> trying send_keys")
        try:
            el.clear(); el.send_keys(value)
            return True
        except Exception as e2:
            print(f"[GO PICK][ERROR] send_keys failed for {locator}: {e2}")
            return False

def _gp_click(driver, locator, timeout=15, label=''):
    if label:
        print(f"[GO PICK] click {label} -> {locator}")
    else:
        print(f"[GO PICK] click -> {locator}")
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    try:
        btn = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(locator))
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
        except Exception:
            pass
        try:
            btn.click()
        except Exception:
            driver.execute_script("arguments[0].click();", btn)
        return True
    except Exception as e:
        print(f"[GO PICK][WARN] click failed {locator}: {e}")
        return False

def _gp_select_shelf_refined(driver, shelf_name: str, wait: WebDriverWait) -> bool:
    """Refined logic to handle jQuery Multiselect shelf selection."""
    try:
        # 1. Open the dropdown
        trigger = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.ui-multiselect")))
        trigger.click()
        
        # 2. Type into the filter to make the hidden shelf visible
        filter_input = wait.until(EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'ui-multiselect-filter')]//input")))
        filter_input.clear()
        filter_input.send_keys(shelf_name)
        time.sleep(0.5) # Wait for jQuery animation
        
        # 3. Click the label specifically for that shelf
        shelf_xpath = f"//ul[@id='shelf_multiple_select']//label[contains(., '{shelf_name}')]"
        label = wait.until(EC.element_to_be_clickable((By.XPATH, shelf_xpath)))
        
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", label)
        label.click()
        
        print(f"[GO PICK] Selected shelf: {shelf_name}")
        return True
    except Exception as e:
        print(f"[GO PICK][ERROR] Shelf selection failed: {e}")
        return False

# Strong multiselect opener/selector for Shelf

def _gp_wait_menu_visible(driver, timeout=8):
    from selenium.webdriver.support.ui import WebDriverWait
    try:
        WebDriverWait(driver, timeout).until(lambda d: d.execute_script(
            """
            const el = document.getElementById('shelf_multiple_select');
            if(!el) return false;
            const menu = el.closest('.ui-multiselect-menu');
            if(!menu) return false;
            const style = window.getComputedStyle(menu);
            return style && style.display !== 'none' && style.visibility !== 'hidden';
            """
        ))
        return True
    except Exception as e:
        print(f"[GO PICK][WARN] shelf menu not visible yet: {e}")
        return False

def _gp_select_shelf_dropdown(driver, shelf_label, timeout=12):
    print(f"[GO PICK] select shelf from dropdown: {shelf_label}")
    from selenium.webdriver.common.by import By
    shelf = (shelf_label or '').strip()
    if not shelf:
        print("[GO PICK][WARN] shelf_label empty")
        return False
    opened = False
    # 1) Try the ACTIVE multiselect button first
    try:
        btns = driver.find_elements(By.XPATH, "//button[contains(@class,'ui-multiselect') and contains(@class,'ui-state-active') and @type='button']")
        print(f"[GO PICK] active multiselect buttons found: {len(btns)}")
        for b in btns:
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", b)
            except Exception:
                pass
            try:
                b.click()
            except Exception:
                driver.execute_script("arguments[0].click();", b)
            if _gp_wait_menu_visible(driver, timeout=6):
                opened = True
                break
    except Exception as e:
        print(f"[GO PICK][WARN] active button open fail: {e}")
    # 2) Fallback: click any multiselect button until menu is visible
    if not opened:
        try:
            any_btns = driver.find_elements(By.XPATH, "//button[contains(@class,'ui-multiselect') and @type='button']")
        except Exception:
            any_btns = []
        print(f"[GO PICK] fallback: multiselect buttons found: {len(any_btns)}")
        for b in any_btns[:8]:
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", b)
            except Exception:
                pass
            try:
                b.click()
            except Exception:
                driver.execute_script("arguments[0].click();", b)
            if _gp_wait_menu_visible(driver, timeout=6):
                opened = True
                break
    if not opened:
        print("[GO PICK][ERROR] could not open shelf multiselect menu")
        return False
    # 3) Filter and choose the item in the menu
    try:
        # filter box (optional)
        try:
            flt = driver.find_element(By.XPATH, "//div[contains(@class,'ui-multiselect-menu')]//div[contains(@class,'ui-multiselect-filter')]//input[@type='search']")
            flt.clear(); flt.send_keys(shelf)
        except Exception:
            pass
        # click the radio or the span
        try:
            radio = driver.find_element(By.CSS_SELECTOR, f"#shelf_multiple_select input[type='radio'][value='{shelf}']")
            driver.execute_script("arguments[0].click();", radio)
            print("[GO PICK] shelf selected via radio")
            return True
        except Exception:
            pass
        try:
            label = driver.find_element(By.XPATH, f"//ul[@id='shelf_multiple_select']//span[normalize-space(text())='{shelf}']")
            driver.execute_script("arguments[0].click();", label)
            print("[GO PICK] shelf selected via span text")
            return True
        except Exception as e:
            print(f"[GO PICK][ERROR] shelf option not found/selected: {e}")
            return False
    except Exception as e:
        print(f"[GO PICK][ERROR] selecting shelf failed: {e}")
        return False

# --------------------------- DB helpers & mapping ---------------------------

def _db_get_tl_for_shelf(shelf):
    try:
        if _ob2_sqlite3 is None:
            return None
        _ob2_ensure_table(_OB2_DB_PATH)
        with _ob2_sqlite3.connect(_OB2_DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute("SELECT tl FROM stn_tl_scrape WHERE TRIM(COALESCE(shelf,'')) = ? ORDER BY id DESC LIMIT 1", ((shelf or '').strip(),))
            row = cur.fetchone()
            val = row[0].strip() if row and row[0] else None
            print(f"[GO PICK][DB] TL for shelf {shelf}: {val}")
            return val
    except Exception as e:
        print(f"[GO PICK][DB][ERROR] read TL by shelf failed: {e}")
        return None

def _db_get_suggested_wid_for_tl(tl):
    try:
        if _ob2_sqlite3 is None or not tl:
            return None
        with _ob2_sqlite3.connect(_OB2_DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute("SELECT wid, COUNT(*) c, MAX(id) mid FROM stn_tl_scrape WHERE tl = ? AND TRIM(COALESCE(wid,''))<>'' GROUP BY wid ORDER BY c DESC, mid DESC LIMIT 1", (tl,))
            row = cur.fetchone()
            wid = row[0].strip() if row and row[0] else None
            print(f"[GO PICK][DB] suggested WID for TL {tl}: {wid}")
            return wid
    except Exception as e:
        print(f"[GO PICK][DB][ERROR] get suggested WID failed: {e}")
        return None

# Ensure extra columns in stn_tl_scrape and persist STN/S/D there as well

def _db_ensure_scrape_columns():
    try:
        if _ob2_sqlite3 is None:
            return
        with _ob2_sqlite3.connect(_OB2_DB_PATH) as conn:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(stn_tl_scrape)").fetchall()}
            for col in ('stn','source','dest'):
                if col not in cols:
                    print(f"[GO PICK][DB] adding column {col} to stn_tl_scrape")
                    conn.execute(f"ALTER TABLE stn_tl_scrape ADD COLUMN {col} TEXT")
            conn.commit()
    except Exception as e:
        print(f"[GO PICK][DB][WARN] ensure scrape columns failed: {e}")


def _db_update_scrape_with_stn_source_dest(*args, **kwargs):
    # disabled (single-table policy)
    return

def _db_ensure_tl_stn_map_columns():
    """Ensure tl_stn_map has stn/source/dest/updated_at columns (migrate older DBs)."""
    try:
        if _ob2_sqlite3 is None:
            return
        with _ob2_sqlite3.connect(_OB2_DB_PATH) as conn:
            try:
                cols = {r[1] for r in conn.execute(f"PRAGMA table_info({_TL_STN_TABLE})").fetchall()}
            except Exception:
                cols = set()
            if not cols:
                conn.execute(f"CREATE TABLE IF NOT EXISTS {_TL_STN_TABLE} (tl TEXT PRIMARY KEY, stn TEXT, source TEXT, dest TEXT, updated_at INTEGER)")
                conn.commit()
                return
            if 'stn' not in cols:
                try: conn.execute(f"ALTER TABLE {_TL_STN_TABLE} ADD COLUMN stn TEXT")
                except Exception: pass
            if 'source' not in cols:
                try: conn.execute(f"ALTER TABLE {_TL_STN_TABLE} ADD COLUMN source TEXT")
                except Exception: pass
            if 'dest' not in cols:
                try: conn.execute(f"ALTER TABLE {_TL_STN_TABLE} ADD COLUMN dest TEXT")
                except Exception: pass
            if 'updated_at' not in cols:
                try: conn.execute(f"ALTER TABLE {_TL_STN_TABLE} ADD COLUMN updated_at INTEGER")
                except Exception: pass
            conn.commit()
    except Exception as e:
        print(f"[GO PICK][DB][WARN] ensure tl_stn_map columns failed: {e}")
def _db_put_tl_map(*args, **kwargs):
    # disabled (single-table policy)
    return

def _db_get_stn_for_tl(tl):
    try:
        if _ob2_sqlite3 is None or not tl:
            return None
        with _ob2_sqlite3.connect(_OB2_DB_PATH) as conn:
            cur = conn.execute(f"SELECT stn FROM {_TL_STN_TABLE} WHERE tl = ?", (tl.strip(),))
            r = cur.fetchone()
            val = (r[0].strip() if r and r[0] else None)
            print(f"[GO PICK][DB] cached STN for TL {tl}: {val}")
            if val:
                return val
            # Fallback: derive STN from stn_tl_scrape if cache lacks it
            try:
                cur2 = conn.execute("SELECT stn FROM stn_tl_scrape WHERE tl = ? AND TRIM(COALESCE(stn,''))<>'' ORDER BY id DESC LIMIT 1", (tl.strip(),))
                r2 = cur2.fetchone()
                val2 = (r2[0].strip() if r2 and r2[0] else None)
                print(f"[GO PICK][DB] fallback STN from stn_tl_scrape for TL {tl}: {val2}")
                return val2
            except Exception:
                return None
    except Exception as e:
        print(f"[GO PICK][DB][ERROR] get stn for tl failed: {e}")
        return None

    try:
        if _ob2_sqlite3 is None or not tl:
            return None
        with _ob2_sqlite3.connect(_OB2_DB_PATH) as conn:
            cur = conn.execute(f"SELECT stn FROM {_TL_STN_TABLE} WHERE tl = ?", (tl.strip(),))
            r = cur.fetchone()
            val = (r[0].strip() if r and r[0] else None)
            print(f"[GO PICK][DB] cached STN for TL {tl}: {val}")
            return val
    except Exception as e:
        print(f"[GO PICK][DB][ERROR] get stn for tl failed: {e}")
        return None

# --------------------------- STN scraping ---------------------------

def _gp_try_scrape_stn_from_confirm_page(driver):
    print("[GO PICK] try scrape STN from Confirm TL page")
    try:
        html = driver.page_source or ''
    except Exception:
        html = ''
    import re as _re
    # Try common STN pattern, else look for External ID like field near labels
    m = _re.search(r"\bSTN[\w\-]+", html, flags=_re.I)
    if m:
        stn = m.group(0).upper()
        print(f"[GO PICK] STN found on confirm page: {stn}")
        return stn
    print("[GO PICK] STN not found on confirm page via regex. [ACTION REQUIRED] Consider adjusting regex/XPath.")
    return None


def _gp_scrape_stn_from_tl_page(driver, tl_id, timeout=12):
    print(f"[GO PICK] try scrape STN from TL detail page for TL={tl_id}")
    from selenium.webdriver.support.ui import WebDriverWait
    try:
        WebDriverWait(driver, timeout).until(lambda d: d.execute_script('return document.readyState')=='complete')
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass
    try:
        text = (driver.page_source or '')
    except Exception:
        text = ''
    import re as _re
    m = _re.search(r"\bSTN[\w\-]+", text, flags=_re.I)
    if m:
        stn = m.group(0).upper()
        print(f"[GO PICK] STN found on TL page: {stn}")
        return stn
    print("[GO PICK] STN not found on TL page. [ACTION REQUIRED] adjust regex or TL page path.")
    return None

# --------------------------- Track tab orchestration ---------------------------

def _gp_open_new_tab(driver):
    try:
        driver.execute_script("window.open('about:blank','_blank');")
        handles = driver.window_handles
        return handles[-1] if handles else None
    except Exception as e:
        print(f"[GO PICK][WARN] new tab open failed: {e}")
        return None


def _gp_scrape_source_dest_from_track_page(driver, timeout=15):
    """Scrape Source/Destination from Track page table anchored by <th>Dispatch by Date</th>.
    Fallback: try any table that contains 'Dispatch by Date' if strict fails.
    Returns (source, dest) or (None, None).
    """
    print('[GO PICK] scrape S/D from Track page (Dispatch by Date anchored)')
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    try:
        tbl = WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH,
            "//table[.//th[normalize-space(.)='Dispatch by Date']]")))
    except Exception as e:
        print(f'[GO PICK][WARN] Dispatch-by-Date table not found: {e}; trying contains fallback')
        try:
            tbl = driver.find_element(By.XPATH, "//table[contains(.,'Dispatch by Date')]")
        except Exception as e2:
            print(f'[GO PICK][ERROR] Track results table not found: {e2}')
            return None, None
    try:
        headers = tbl.find_elements(By.XPATH, ".//th")
        hmap = {(h.text or '').strip(): i+1 for i, h in enumerate(headers)}
        src_keys = ['Source','Source Warehouse','Source WH','Warehouse Id','Warehouse','From WH','Source Name','Source Id']
        dst_keys = ['Destination','Destination Warehouse','Destination WH','Destination Id','Destination Party','To WH','Destination Name']
        def _col_for(keys):
            for k in keys:
                for kk in list(hmap.keys()):
                    if kk.strip().lower() == k.lower():
                        return hmap.get(kk)
            return None
        s_col = _col_for(src_keys)
        d_col = _col_for(dst_keys)
        if not (s_col and d_col):
            print(f'[GO PICK][WARN] header indices missing; headers={list(hmap.keys())}')
            try:
                row = tbl.find_element(By.XPATH, ".//tbody/tr[1]")
                tds = row.find_elements(By.XPATH, ".//td")
                vals = [(td.text or '').strip() for td in tds]
                s_val = next((v for v in vals if v), None)
                d_val = next((v for v in reversed(vals) if v), None)
                return s_val, d_val
            except Exception:
                return None, None
        row = tbl.find_element(By.XPATH, ".//tbody/tr[1]")
        s_val = row.find_element(By.XPATH, f'.//td[{s_col}]').text.strip() if s_col else None
        d_val = row.find_element(By.XPATH, f'.//td[{d_col}]').text.strip() if d_col else None
        print(f"[GO PICK] scraped S={s_val} D={d_val}")
        return s_val, d_val
    except Exception as e:
        print(f"[GO PICK][ERROR] parse S/D failed: {e}")
        return None, None

def _gp_open_box_and_print(driver, wait):
    print("[GO PICK] open Box Creation and print 1/1")
    try:
        if not navigate_if_needed(driver, BOX_CREATION_URL, wait):
            print("[GO PICK][ERROR] cannot open Box Creation URL")
            return False
        from selenium.webdriver.common.by import By
        _gp_set_input_value(driver, (By.ID, 'quantity'), '1')
        _gp_click(driver, (By.XPATH, "//input[@name='commit' and @type='submit' and @value='Generate']"), label='Generate')
        _gp_set_input_value(driver, (By.ID, 'print_quantity'), '1')
        baseline_names = [p.name for p in _list_pdfs(DOWNLOAD_FOLDER)]
        import time as _t; start_ts = _t.time()
        _gp_click(driver, (By.XPATH, "//button[@name='button' and contains(@class,'print') and contains(normalize-space(.), 'Print All Box Labels')]"), label='Print All Box Labels')
        pdf = wait_for_new_pdf(DOWNLOAD_FOLDER, baseline_names=baseline_names, start_ts=start_ts)
        if not pdf:
            print('[GO PICK][ERROR] PDF not found after print')
            return False
        try:
            s_val = globals().get('_GP_LAST_SRC')
            d_val = globals().get('_GP_LAST_DEST')
            stn_val = globals().get('_GP_LAST_STN')
            stamped = pdf.with_name(pdf.stem + '_stamped.pdf')
            stamp_pdf_all_pages_split(pdf, stamped, s_val or '', d_val or '', stn_val or '')
            _open_file_in_default_app(stamped)
        except Exception as se:
            print(f"[GO PICK][WARN] stamping failed: {se}")
        return True
    except Exception as e:
        print(f"[GO PICK][ERROR] Box print flow failed: {e}")
        return False

# --- NEW: wait for Confirm TL page in current tab and paste Box-ID into #tote ---
def _gp_wait_confirm_page_and_paste_tote(driver, tl: str, box_id: str, timeout_sec: int = 90, shelf_label: str = None):
    import time as _t
    print(f"[GO PICK] [BOX-ID] waiting up to {timeout_sec}s for Confirm TL page to paste into #tote...")
    deadline = _t.time() + timeout_sec
    last_url = ''
    while _t.time() < deadline:
        try:
            url = driver.current_url or ''
        except Exception:
            url = ''
        if url != last_url:
            print('[GO PICK] [BOX-ID] observe URL:', url)
            last_url = url
        cond1 = '/transfer_list/confirm_transfer_list' in url
        cond2 = (tl or '') and ((tl in url) or True)  # tolerate UIs that hide params
        if cond1 and cond2:
            try:
                from selenium.webdriver.common.by import By
                from selenium.webdriver.support.ui import WebDriverWait
                from selenium.webdriver.support import expected_conditions as EC
                el = WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.ID, 'tote')))
                ok = _gp_set_input_value(driver, (By.ID, 'tote'), box_id)
                print(f"[GO PICK] [BOX-ID] pasted into #tote: {box_id} (ok={ok})")

                # --- Select Shelf multiselect and pick by shelf_label ---
                if shelf_label:
                    try:
                        from selenium.webdriver.common.by import By
                        from selenium.webdriver.support.ui import WebDriverWait
                        from selenium.webdriver.support import expected_conditions as EC
                        btn = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(@class,'ui-multiselect') and contains(@class,'ui-widget')]")))
                        btn.click()
                        print('[GO PICK] [SHELF] opened Select Shelf multiselect')
                        search = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//input[@type='search' and @placeholder='Enter keywords']")))
                        try:
                            search.clear()
                        except Exception:
                            pass
                        search.send_keys(shelf_label)
                        _gp_fire_once_confirm(driver, shelf_label, verify_secs=1.2)
                        import time as _t; _t.sleep(0.3)
                        first = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "(//ul[contains(@class,'ui-multiselect-checkboxes')]//li)[1]//label")))
                        driver.execute_script('arguments[0].click();', first)
                        print('[GO PICK] [SHELF] selected first shelf item')
                    except Exception as e:
                        print('[GO PICK] [SHELF][WARN] selection flow failed:', e)
                return True
            except Exception as e:
                print('[GO PICK] [BOX-ID][WARN] paste attempt failed on this page view:', e)
        _t.sleep(0.7)
    print('[GO PICK] [BOX-ID][WARN] timed out waiting for Confirm TL page to paste Box-ID')
    return False



def _gp_kickoff_track_flow(self):
    print("[GO PICK] >>> Track flow thread started")
    try:
        driver = GUI_ACTIVE_DRIVER
        if driver is None:
            self.set_status('Session closed for track tab.', is_error=True); return
        from selenium.webdriver.common.by import By
        wait = build_wait(driver, TIMEOUT_MED)
        tl = getattr(self, '_gp_current_tl', None)
        print(f"[GO PICK] current TL: {tl}")
        if not tl:
            self.set_status('TL not resolved yet; cannot track.', is_error=True); return
        # Try cache first
        stn = _db_get_stn_for_tl(tl)
        if not stn:
            print("[GO PICK] STN not in cache; attempting to detect from current Confirm TL page")
            try:
                stn = _gp_try_scrape_stn_from_confirm_page(driver)
            except Exception as _e:
                print(f"[GO PICK][WARN] confirm page scrape error: {_e}")
            if not stn:
                print("[GO PICK] Attempt TL detail page scrape")
                temp_tab = _gp_open_new_tab(driver)
                if temp_tab:
                    cur = driver.current_window_handle
                    try:
                        driver.switch_to.window(temp_tab)
                        base = 'http://10.24.1.53'
                        detail_url = f"{base}/transfer_list/{tl}"
                        print(f"[GO PICK] open TL detail URL: {detail_url}")
                        driver.get(detail_url)
                        stn = _gp_scrape_stn_from_tl_page(driver, tl)
                    finally:
                        try: driver.close()
                        except Exception: pass
                        try: driver.switch_to.window(cur)
                        except Exception: pass
        if not stn:
            print("[GO PICK][ERROR] Could not resolve STN for TL after attempts")
            self.set_status('Could not resolve STN for TL.', is_error=True); return
        print(f"[GO PICK] Resolved STN = {stn}")
        try:
            globals()['_GP_LAST_STN'] = stn
        except Exception: pass
        # Persist STN immediately in both tl_map and stn_tl_scrape
        _db_put_tl_map(tl, stn=stn)
        _db_update_scrape_with_stn_source_dest(tl, stn=stn)
        # Ensure Track tab exists and navigate
        track_tab = getattr(self, '_gp_track_tab', None)
        if not track_tab:
            track_tab = _gp_open_new_tab(driver)
            self._gp_track_tab = track_tab
        if not track_tab:
            print("[GO PICK][ERROR] could not open track tab")
            self.set_status('Could not open new tab for tracking.', is_error=True); return
        main_tab = getattr(self, '_gp_main_tab', None) or driver.current_window_handle
        print(f"[GO PICK] switch to Track tab: {track_tab}")
        driver.switch_to.window(track_tab)
        if not ensure_home_loaded(driver, wait, retries=4, sleep_between=0.7):
            navigate_if_needed(driver, HOME_URL, wait)
            ensure_home_loaded(driver, wait, retries=3, sleep_between=0.7)
        select_warehouse_by_name(driver, wait, SELECTED_WH_FULL)
        print("[GO PICK] navigate to Track Request page")
        navigate_if_needed(driver, OUTBOUND_TRACK_URL, wait)
        # Enter STN and track
        print("[GO PICK] Enter STN into filters_outbound_request_id")
        _gp_set_input_value(driver, (By.ID, 'filters_outbound_request_id'), stn)
        print("[GO PICK] Click Track Request button")
        if not _gp_click(driver, (By.NAME, 'searchbtn'), label='Track Request'):
            _gp_click(driver, (By.XPATH, "//input[@type='submit' and @name='searchbtn']"), label='Track Request (fallback)')
        try:
            import time as _t; _t.sleep(1.0)
        except Exception:
            pass
        # Scrape and persist S/D
        s_val, d_val = _gp_scrape_source_dest_from_track_page(driver)
        self._gp_src_s, self._gp_dst_d = s_val, d_val
        try:
            globals()['_GP_LAST_SRC'] = s_val
            globals()['_GP_LAST_DEST'] = d_val
        except Exception: pass
        _db_put_tl_map(tl, source=s_val, dest=d_val)
        _db_update_scrape_with_stn_source_dest(tl, stn=stn, source=s_val, dest=d_val)
        # Box print as requested
        _gp_open_box_and_print(driver, wait)
        # Return to main
        try:
            driver.switch_to.window(main_tab)
        except Exception:
            pass


        # --- NEW: After track flow, auto-paste Box-ID into #tote on Confirm TL page in original tab ---
        try:
            latest_box = _db_get_box_for_tl_preferring_shelf(tl, shelf_scan_var.get())
            if latest_box:
                print('[GO PICK] [BOX-ID] latest for TL', tl, '->', latest_box)
                _gp_wait_confirm_page_and_paste_tote(driver, tl, latest_box, timeout_sec=90, shelf_label=(getattr(self,'_gp_suggest_var',None).get() if hasattr(self,'_gp_suggest_var') else None))
            else:
                print('[GO PICK] [BOX-ID][WARN] no Box-ID found in DB yet for TL', tl)
        except Exception as _e:
            print('[GO PICK] [BOX-ID][WARN] post-track paste step failed:', _e)
        except Exception:
            pass
        if s_val or d_val:
            self.set_status(f"Track done. S={s_val or '—'}; D={d_val or '—'}. Box labels printed.")
        else:
            self.set_status("Track done. Box labels printed (S/D not found).")
        print("[GO PICK] >>> Track flow thread finished")
    except Exception as e:
        print(f"[GO PICK][FATAL] Track flow error: {e}")
        self.set_status(f"Track flow error: {e}", is_error=True)

# --------------------------- Attach into MainFrame ---------------------------

def _attach_gopick_to_mainframe():
    try:
        mf = MainFrame
    except Exception:
        return

    def _ensure_gopick_button(self):
        try:
            import tkinter as _tk
            kids = [w for w in self.menu_view.winfo_children() if isinstance(w, _tk.Button)]
            texts = [w.cget('text') for w in kids]
            if 'GO PICK' in texts:
                return
            by_text = {w.cget('text'): w for w in kids}
            stn_btn = by_text.get('STN upload & TL assign')
            print_btn = by_text.get('PRINT BOX-ID')
            tms_btn = by_text.get('TMS')
            tc_btn = by_text.get('TC')
            for w in (stn_btn, print_btn, tms_btn, tc_btn):
                if w: w.pack_forget()
            if stn_btn: stn_btn.pack(fill='x', pady=10)
            _gp = _tk.Button(self.menu_view, text='GO PICK', command=self.show_gopick_view,
                             font=('Arial', 14, 'bold'), bg=PACK_DEFAULT_BG, fg='white',
                             activebackground='#38BDF8', relief='flat', padx=18, pady=12)
            _gp.pack(fill='x', pady=10)
            if print_btn: print_btn.pack(fill='x', pady=10)
            if tms_btn: tms_btn.pack(fill='x', pady=10)
            if tc_btn: tc_btn.pack(fill='x', pady=10)
            print("[GO PICK] Button injected into menu")
        except Exception as _e:
            print('[GO PICK] ensure button error:', _e)

    mf._ensure_gopick_button = _ensure_gopick_button

    try:
        _orig_show_menu = mf.show_menu
        def _show_menu_with_gp(self):
            try:
                if hasattr(self, '_ensure_gopick_button'):
                    self._ensure_gopick_button()
            except Exception as e:
                print('[GO PICK][WARN] ensure button during show_menu:', e)
            return _orig_show_menu(self)
        mf.show_menu = _show_menu_with_gp
    except Exception as e:
        print('[GO PICK][WARN] could not patch show_menu:', e)

    try:
        _orig_build = mf._build
        def _build_with_gp(self):
            _orig_build(self)
            if hasattr(self, '_ensure_gopick_button'):
                self._ensure_gopick_button()
            if not hasattr(self, '_gp_track_tab'):
                self._gp_track_tab = None
            if not hasattr(self, '_gp_main_tab'):
                self._gp_main_tab = None
            if not hasattr(self, '_gp_src_s'):
                self._gp_src_s = None
            if not hasattr(self, '_gp_dst_d'):
                self._gp_dst_d = None
        mf._build = _build_with_gp
    except Exception as e:
        print('[GO PICK][WARN] could not patch _build:', e)

    def show_gopick_view(self):
        print("[GO PICK] show_gopick_view invoked")
        if not hasattr(self, 'gopick_view'):
            self.gopick_view = tk.Frame(self, padx=16, pady=10)
        if not hasattr(self, '_gp_suggest_var'): self._gp_suggest_var = tk.StringVar(value='—')
        if not hasattr(self, '_gp_scan_var'): self._gp_scan_var = tk.StringVar(value='')
        if not hasattr(self, '_gp_wid_scan_var'): self._gp_wid_scan_var = tk.StringVar(value='')
        if not hasattr(self, '_gp_wid_suggest_var'): self._gp_wid_suggest_var = tk.StringVar(value='—')
        if not hasattr(self, '_gp_track_tab'): self._gp_track_tab = None
        if not hasattr(self, '_gp_main_tab'): self._gp_main_tab = None

        for v in [getattr(self,'menu_view',None), getattr(self,'box_view',None), getattr(self,'tote_view',None), getattr(self,'tms_view',None), getattr(self,'tc_view',None), getattr(self,'stn_tl_view',None)]:
            try:
                if v is not None: v.pack_forget()
            except Exception: pass
        try:
            if not self.back_btn_top.winfo_ismapped():
                self.back_btn_top.pack(side='left')
        except Exception: pass
        try:
            self._gp_main_tab = GUI_ACTIVE_DRIVER.current_window_handle
        except Exception:
            self._gp_main_tab = None

        for ch in list(self.gopick_view.winfo_children()):
            try: ch.destroy()
            except Exception: pass
        tk.Label(self.gopick_view, text='GO PICK', font=('Arial', 14, 'bold')).pack(anchor='w', pady=(4,10))
        row1 = tk.Frame(self.gopick_view); row1.pack(fill='x', pady=(6,6))
        tk.Label(row1, text='Suggested Shelf:', font=('Arial', 11)).pack(side='left')
        tk.Label(row1, textvariable=self._gp_suggest_var, font=('Arial', 11, 'bold'), fg='#111').pack(side='left', padx=(8,0))
        row2 = tk.Frame(self.gopick_view); row2.pack(fill='x', pady=(4,2))
        tk.Label(row2, text='Shelf label scan:', font=('Arial', 11)).pack(side='left')
        self.gp_scan_entry = tk.Entry(row2, width=40, textvariable=self._gp_scan_var, font=('Arial', 12))
        self.gp_scan_entry.pack(side='left', padx=(8,0))
        self.gp_scan_entry.bind('<Return>', self._on_gopick_shelf_enter)
        tk.Label(self.gopick_view, text='Scan shelf barcode/QR and press Enter.', fg='#666').pack(anchor='w', pady=(2,8))
        self._gp_wid_row = tk.Frame(self.gopick_view)
        tk.Label(self._gp_wid_row, text='WID scan:', font=('Arial', 11)).pack(side='left')
        tk.Label(self._gp_wid_row, textvariable=self._gp_wid_suggest_var, font=('Arial', 11, 'bold'), fg='#0A5CC2').pack(side='left', padx=(8,8))
        self.gp_wid_entry = tk.Entry(self._gp_wid_row, width=40, textvariable=self._gp_wid_scan_var, font=('Arial', 12))
        self.gp_wid_entry.pack(side='left', padx=(8,0))
        self.gp_wid_entry.bind('<Return>', self._on_gopick_wid_enter)
        self.gopick_view.pack(fill='both', expand=True)

        driver = GUI_ACTIVE_DRIVER
        if driver is None:
            self.set_status('No active session. Please login first.', is_error=True); return
        wait = build_wait(driver, TIMEOUT_MED)
        self.set_status('Opening HOME…')
        if not ensure_home_loaded(driver, wait, retries=5, sleep_between=1.0):
            self.set_status('HOME not ready.', is_error=True); return
        if not select_warehouse_by_name(driver, wait, SELECTED_WH_FULL):
            self.set_status('Warehouse selection failed.', is_error=True); return
        self.set_status('Opening Confirm Transfer List…')
        if not navigate_if_needed(driver, CONFIRM_TL_URL, wait):
            self.set_status('Could not open Confirm Transfer List.', is_error=True); return
        try:
            latest = self._db_get_latest_shelf_label()
            self._gp_suggest_var.set(latest or '—')
        except Exception:
            self._gp_suggest_var.set('—')
        self.set_status('Ready. Scan the shelf label.')
        try:
            self.gp_scan_entry.focus_set(); self.gp_scan_entry.icursor('end')
        except Exception: pass
        # prepare track tab
        try:
            track_tab = _gp_open_new_tab(driver)
            if track_tab:
                self._gp_track_tab = track_tab
                cur = driver.current_window_handle
                driver.switch_to.window(track_tab)
                ensure_home_loaded(driver, wait, retries=4, sleep_between=0.7)
                select_warehouse_by_name(driver, wait, SELECTED_WH_FULL)
                navigate_if_needed(driver, OUTBOUND_TRACK_URL, wait)
                driver.switch_to.window(cur)
                self.set_status('Track tab ready.')
        except Exception as _e:
            self.set_status(f'Track tab prep skipped: {_e}', is_error=False)

    mf.show_gopick_view = show_gopick_view

    def _db_get_latest_shelf_label(self):
        latest = None
        try:
            if _ob2_sqlite3 is None:
                return None
            _ob2_ensure_table(_OB2_DB_PATH)
            with _ob2_sqlite3.connect(_OB2_DB_PATH) as conn:
                cur = conn.cursor()
                cur.execute("SELECT shelf FROM stn_tl_scrape WHERE TRIM(COALESCE(shelf,''))<>'' ORDER BY id DESC LIMIT 1")
                row = cur.fetchone()
                if row and (row[0] or '').strip():
                    latest = row[0].strip()
        except Exception:
            latest = None
        if latest:
            return latest
        try:
            with _ob2_sqlite3.connect(_OB2_DB_PATH) as conn:
                cur = conn.cursor()
                cur.execute("SELECT DISTINCT TRIM(COALESCE(shelf,'')) s FROM stn_tl_scrape WHERE s IS NOT NULL AND s<>''")
                rows = [r[0] for r in cur.fetchall() if r and (r[0] or '').strip()]
            try:
                rows = sorted(rows, key=_ob2_natural_key)
            except Exception:
                rows = sorted(rows)
            return rows[-1] if rows else None
        except Exception:
            return None

    mf._db_get_latest_shelf_label = _db_get_latest_shelf_label

    def _on_gopick_shelf_enter(self, _event=None):
        shelf_scanned = (self._gp_scan_var.get() or '').strip()
        shelf_suggested = (self._gp_suggest_var.get() or '').strip()
        print(f"[GO PICK] shelf scanned='{shelf_scanned}', suggested='{shelf_suggested}'")
        if not shelf_scanned:
            self.set_status('No shelf label scanned. Try again.', is_error=True); return
        driver = GUI_ACTIVE_DRIVER
        if driver is None:
            self.set_status('Session closed. Please re-login.', is_error=True); return
        from selenium.webdriver.common.by import By
        ok = True
        if shelf_suggested and shelf_scanned.lower() == shelf_suggested.lower():
            tl = _db_get_tl_for_shelf(shelf_scanned)
            self._gp_current_tl = tl
            print(f"[GO PICK] matched suggestion, TL={tl}")
            try:
                globals()['_GP_CURRENT_TL'] = (tl or '').strip().upper()
                globals()['_LAST_KNOWN_TL_ID'] = globals().get('_GP_CURRENT_TL')
            except Exception:
                pass
            if tl:
                if not _gp_set_input_value(driver, (By.ID, 'filters_transfer_list_id'), tl):
                    ok = False
                try:
                    _gp_select_shelf_dropdown(driver, shelf_scanned)
                except Exception:
                    pass
                if not _gp_click(driver, (By.XPATH, "//input[@name='commit' and @type='submit' and @value='Find']"), label='Find'):
                    ok = False
            else:
                ok = False
        else:
            print("[GO PICK] shelf != suggestion; selecting shelf only")
            try:
                _gp_select_shelf_dropdown(driver, shelf_scanned)
            except Exception:
                pass
            self.set_status('Shelf scanned (not equal to suggestion). TL not auto-filled.')
        if ok:
            self.set_status(f"Shelf OK. TL set: {getattr(self,'_gp_current_tl', None) or '—'}; Clicked Find. Now scan WID.")
            try:
                wid_sug = _db_get_suggested_wid_for_tl(getattr(self, '_gp_current_tl', None))
                self._gp_wid_suggest_var.set(wid_sug or '—')
            except Exception:
                self._gp_wid_suggest_var.set('—')

            try:
                self._gp_wid_row.pack(fill='x', pady=(10,4))
                self.gp_wid_entry.focus_set(); self.gp_wid_entry.icursor('end')
            except Exception: pass
            try:
                import threading
                threading.Thread(target=_gp_kickoff_track_flow, args=(self,), daemon=True).start()
            except Exception as e:
                print(f"[GO PICK][WARN] could not start thread: {e}")
        else:
            self.set_status('Could not set TL / click Find. You can retry scanning or fill manually.', is_error=True)


    def _on_gopick_wid_enter(self, _event=None):
        wid = (self._gp_wid_scan_var.get() or '').strip()
        print(f"[GO PICK] wid scanned='{wid}'")
        if not wid:
            self.set_status('No WID scanned.', is_error=True); return
        driver = GUI_ACTIVE_DRIVER
        if driver is None:
            self.set_status('Session closed.', is_error=True); return
        from selenium.webdriver.common.by import By
        ok = _gp_set_input_value(driver, (By.ID, 'input_box'), wid)
        if not ok:
            self.set_status('Could not paste WID to page input.', is_error=True); return
        self.set_status('WID captured. Picking…')
        _gp_click(driver, (By.ID, 'pick_button'), label='Pick')
        _gp_click(driver, (By.ID, 'complete_transfer_list'), label='Pick Items')
        _gp_click(driver, (By.ID, 'close_tote'), label='Close Tote/Box')
        self.set_status('Pick sequence triggered. Proceed as needed.')

    mf._on_gopick_shelf_enter = _on_gopick_shelf_enter
    mf._on_gopick_wid_enter = _on_gopick_wid_enter

# Attach on import
try:
    _attach_gopick_to_mainframe()
except Exception as _e:
    print('[GO PICK] Attach warning:', _e)
# ======================== END GO PICK (runtime attach) =========================

# ========================= BOX-ID SCRAPE & AUTOPASTE (inline) =========================
import threading as _bx_th, time as _bx_time, re as _bx_re
from typing import Optional as _bx_Optional

_BOX_URL_SIG = '/storage_locations/create_new_boxes'
_BOX_URL_LB1 = 'filters[packing_box_type]=LB1'
_CONFIRM_URL_SIG = '/transfer_list/confirm_transfer_list'

_BX_TL_PARAM_RE = _bx_re.compile(r"filters\[transfer_list_id\]=(TL\d+)", _bx_re.I)
_BX_ID_RE       = _bx_re.compile(r"^[A-Z0-9]{6,}$")

_BOXID_WATCHER = None

def _bx_get_current_tl() -> _bx_Optional[str]:
    try:
        tl = globals().get('_GP_CURRENT_TL')
        if tl: return (tl or '').strip().upper()
    except Exception: pass
    try:
        tl = globals().get('_LAST_KNOWN_TL_ID')
        if tl: return (tl or '').strip().upper()
    except Exception: pass
    return None

def _bx_get_stn_for_tl(tl: str) -> _bx_Optional[str]:
    try: return _db_get_stn_for_tl(tl)
    except Exception: return None

def _bx_record_box(stn: str, tl: str, box_id: str) -> bool:
    try:
        record_pack_stn(stn, tl, box_id)
        return True
    except Exception:
        try:
            table = globals().get('_PACK_TABLE', 'pack_stn_items')
            with get_conn() as conn:
                conn.execute(f"CREATE TABLE IF NOT EXISTS {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, stn TEXT NOT NULL, tl_id TEXT NOT NULL, box_id TEXT NOT NULL, created_at TEXT NOT NULL);")
                conn.execute(f"INSERT INTO {table} (stn, tl_id, box_id, created_at) VALUES (?, ?, ?, datetime('now'));", (stn, tl, box_id))
                conn.commit()
            return True
        except Exception:
            return False

def _bx_latest_box_for_tl(tl: str) -> _bx_Optional[str]:
    try: return _db_get_box_for_tl_preferring_shelf(tl, shelf_scan_var.get())
    except Exception:
        try:
            table = globals().get('_PACK_TABLE', 'pack_stn_items')
            with get_conn() as conn:
                cur = conn.execute(f"SELECT box_id FROM {table} WHERE UPPER(tl_id)=UPPER(?) AND TRIM(COALESCE(box_id,''))<>'' ORDER BY id DESC LIMIT 1;", (tl,))
                row = cur.fetchone()
                return (row[0].strip() if row and row[0] else None)
        except Exception:
            return None

def _bx_set_input_value(driver, by, sel, value, timeout=8) -> bool:
    try:
        return bool(_gp_set_input_value(driver, (by, sel), value))
    except Exception:
        from selenium.webdriver.support.ui import WebDriverWait as _W
        from selenium.webdriver.support import expected_conditions as _EC
        try:
            el = _W(driver, timeout).until(_EC.presence_of_element_located((by, sel)))
            try: el.clear()
            except Exception: pass
            el.send_keys(value)
            return True
        except Exception:
            return False

class _BoxIdWatcher:
    def __init__(self, interval=0.8):
        self.interval = interval
        self._running = False
        self._last_box_url = None
        self._last_confirm_url = None
    def start(self):
        if self._running: return
        self._running = True
        _bx_th.Thread(target=self._loop, daemon=True).start()
        print('[BOX-ID] watcher started')
    def stop(self): self._running = False
    def _loop(self):
        from selenium.webdriver.common.by import By as _By
        from selenium.webdriver.support.ui import WebDriverWait as _W
        from selenium.webdriver.support import expected_conditions as _EC
        while self._running:
            try:
                driver = globals().get('GUI_ACTIVE_DRIVER')
                if driver is None:
                    _bx_time.sleep(self.interval); continue
                try: url = driver.current_url or ''
                except Exception: url = ''
                if not url:
                    _bx_time.sleep(self.interval); continue
                if (_BOX_URL_SIG in url) and (_BOX_URL_LB1 in url):
                    if url != self._last_box_url:
                        self._last_box_url = url
                        try:
                            cell = _W(driver, 10).until(_EC.presence_of_element_located((_By.XPATH, "(//table[contains(@class,'fk-table')]//tbody//tr)[1]/td[2]")))
                            txt = (cell.text or cell.get_attribute('innerText') or '').strip().upper()
                        except Exception:
                            txt = ''
                        if txt and _BX_ID_RE.match(txt):
                            tl = _bx_get_current_tl()
                            if tl:
                                stn = _bx_get_stn_for_tl(tl)
                                if stn:
                                    if _bx_record_box(stn, tl, txt):
                                        print(f"[BOX-ID] Stored {txt} for TL {tl} (STN {stn})")
                                    else:
                                        print(f"[BOX-ID][WARN] Store failed for {txt} / {tl}")
                                else:
                                    print(f"[BOX-ID][WARN] STN not found for TL {tl}")
                            else:
                                print('[BOX-ID][WARN] Current TL unknown; skip store')
                        else:
                            print('[BOX-ID][WARN] Box cell not found or format not matched')
                elif _CONFIRM_URL_SIG in url:
                    if url != self._last_confirm_url:
                        self._last_confirm_url = url
                        m = _BX_TL_PARAM_RE.search(url)
                        tl = (m.group(1).upper() if m else _bx_get_current_tl())
                        if tl:
                            box = _bx_latest_box_for_tl(tl)
                            if box:
                                if _bx_set_input_value(driver, _By.ID, 'tote', box, timeout=8):
                                    print(f"[BOX-ID] Auto-pasted {box} into #tote for TL {tl}")
                                else:
                                    print(f"[BOX-ID][WARN] Could not paste into #tote for TL {tl}")
                            else:
                                print(f"[BOX-ID][WARN] No stored Box-ID for TL {tl}")
                _bx_time.sleep(self.interval)
            except Exception:
                _bx_time.sleep(self.interval)

def start_boxid_watcher_if_ready():
    global _BOXID_WATCHER
    try:
        if _BOXID_WATCHER is None:
            _BOXID_WATCHER = _BoxIdWatcher()
        _BOXID_WATCHER.start()
        return True
    except Exception:
        return False

try:
    start_boxid_watcher_if_ready()
except Exception:
    pass
# ======================= END BOX-ID SCRAPE & AUTOPASTE ==========================





# ====== Injected (header-driven S/D + robust header extraction + open_box) ======
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def _gp_scrape_sd_from_track_page_v2(driver, wait_timeout=20):
    """Header-driven S/D parser for Track Request page.
    * Targets the sticky table by class.
    * Reads <th> labels using innerText/textContent fallback (fixes empty .text).
    * Chooses header row that actually contains required labels; else the richest.
    Returns (src, dst) or (None, None).
    """
    def _label_of(th):
        for attr in ("innerText", "textContent"):
            try:
                v = th.get_attribute(attr)
                if v and v.strip():
                    return v.strip()
            except Exception:
                pass
        return (th.text or '').strip()

    try:
        table_xpath = (
            "//table[contains(@class,'sticky-header') and .//th[normalize-space()='Dispatch by Date'] "
            "and ((.//th[normalize-space()='Warehouse Id']) or (.//th[normalize-space()='Warehouse'])) "
            "and ((.//th[normalize-space()='Destination Id']) or (.//th[normalize-space()='Destination Party']))]"
        )
        table = WebDriverWait(driver, wait_timeout).until(
            EC.presence_of_element_located((By.XPATH, table_xpath))
        )
    except Exception:
        try:
            table = WebDriverWait(driver, wait_timeout).until(
                EC.presence_of_element_located((By.XPATH, "//table[.//th[normalize-space()='Dispatch by Date']]"))
            )
        except Exception:
            print("[GO PICK][ERROR] Track grid not found.")
            return None, None

    hdr_rows = table.find_elements(By.XPATH, ".//thead//tr[th]")
    if not hdr_rows:
        hdr_rows = table.find_elements(By.XPATH, ".//tr[th]")

    best_row = None
    best_score = (-1, -1)  # (non-empty count, contains both)
    best_labels = []

    for row in hdr_rows:
        ths = row.find_elements(By.XPATH, "./th")
        labels = [ _label_of(th) for th in ths ]
        norm = [ (lbl or '').replace('\xa0',' ').strip().lower() for lbl in labels ]
        non_empty = sum(1 for x in labels if x.strip())
        contains_src = any(x in norm for x in ("warehouse id","warehouse"))
        contains_dst = any(x in norm for x in ("destination id","destination party"))
        score = (non_empty, 1 if (contains_src and contains_dst) else 0)
        if score > best_score:
            best_score = score
            best_row = row
            best_labels = labels

    if not best_row:
        print("[GO PICK][ERROR] No header row found in grid.")
        return None, None

    headers = best_labels
    norm = [h.lower().replace("\xa0"," ").strip() for h in headers]

    def _first_index(cands):
        for name in cands:
            try:
                return norm.index(name.lower())
            except ValueError:
                continue
        return -1

    idx_src = _first_index(["warehouse id", "warehouse"])  # Source column
    idx_dst = _first_index(["destination id", "destination party"])  # Destination column

    if idx_src < 0 or idx_dst < 0:
        print(f"[GO PICK][ERROR] Header indices not found. Headers seen: {headers!r}")
        return None, None

    try:
        row = WebDriverWait(driver, 8).until(
            EC.presence_of_element_located((By.XPATH, ".//tbody/tr[1]"))
        )
    except Exception:
        print("[GO PICK][ERROR] No rows in grid.")
        return None, None

    tds = row.find_elements(By.XPATH, "./td")
    if not tds or max(idx_src, idx_dst) >= len(tds):
        print("[GO PICK][ERROR] Row has fewer columns than expected.")
        return None, None

    src_val = (tds[idx_src].text or '').strip()
    dst_val = (tds[idx_dst].text or '').strip()
    print(f"[GO PICK] Parsed Source='{src_val}' Destination='{dst_val}'")
    return src_val, dst_val


def _gp_scrape_source_dest_from_track_page(driver, timeout=15):
    """Return (source, dest) via header-driven parser; also update globals and DB."""
    print("[GO PICK] scrape S/D from Track page (header-driven v2)")
    try:
        src, dst = _gp_scrape_sd_from_track_page_v2(driver, wait_timeout=timeout)
        if src and dst:
            try:
                globals()['_GP_LAST_SRC'] = src
                globals()['_GP_LAST_DEST'] = dst
            except Exception:
                pass
            try:
                tl = _ob2_get_tl_id_from_url(driver) or globals().get('_LAST_KNOWN_TL_ID')
                if tl:
                    _db_put_tl_map(tl, source=src, dest=dst)
                    _db_update_scrape_with_stn_source_dest(tl, source=src, dest=dst)
            except Exception:
                pass
        else:
            print("[GO PICK][WARN] Could not parse Source/Dest; stamping may fall back to blanks.")
        return src, dst
    except Exception as e:
        print(f"[GO PICK][ERROR] S/D scrape failed: {e}")
        return None, None


def _gp_open_box_and_print(driver, wait):
    """Open Box Creation page, generate 1/1, print, stamp S/D & STN. Returns True on success."""
    print("[GO PICK] open Box Creation and print 1/1")
    try:
        if not navigate_if_needed(driver, BOX_CREATION_URL, wait):
            print("[GO PICK][ERROR] cannot open Box Creation URL"); return False
        _gp_set_input_value(driver, (By.ID, 'quantity'), '1')
        _gp_click(driver, (By.XPATH, "//input[@name='commit' and @type='submit' and @value='Generate']"), label='Generate')
        _gp_set_input_value(driver, (By.ID, 'print_quantity'), '1')
        baseline_names = [p.name for p in _list_pdfs(DOWNLOAD_FOLDER)]
        import time as _t; start_ts = _t.time()
        _gp_click(driver, (By.XPATH, "//button[@name='button' and contains(@class,'print') and contains(normalize-space(.), 'Print All Box Labels')]"), label='Print All Box Labels')
        pdf = wait_for_new_pdf(DOWNLOAD_FOLDER, baseline_names=baseline_names, start_ts=start_ts)
        if not pdf:
            print('[GO PICK][ERROR] PDF not found after print'); return False
        try:
            s_val = globals().get('_GP_LAST_SRC')
            d_val = globals().get('_GP_LAST_DEST')
            stn_val = globals().get('_GP_LAST_STN')
            stamped = pdf.with_name(pdf.stem + '_stamped.pdf')
            stamp_pdf_all_pages_split(pdf, stamped, s_val or '', d_val or '', stn_val or '')
            _open_file_in_default_app(stamped)
        except Exception as se:
            print(f"[GO PICK][WARN] stamp failed: {se}")
        return True
    except Exception as e:
        print(f"[GO PICK][ERROR] open_box_and_print failed: {e}")
        return False

# ====== End Injected ======
if __name__ == "__main__":
    start_application()


# === DB upgrade: ensure extended columns exist on older databases ===
def _ob2_upgrade_table_add_missing_columns(db_path: str):
    try:
        if _ob2_sqlite3 is None:
            return
    except NameError:
        return
    cols_needed = [
        ('stn', 'TEXT'),
        ('source', 'TEXT'),
        ('destination', 'TEXT'),
        ('box_id', 'TEXT'),
        ('pick', 'INTEGER DEFAULT 0'),
        ('tl_complete_status', 'TEXT'),
        ('pack', 'INTEGER DEFAULT 0'),
        ('dispatch', 'INTEGER DEFAULT 0'),
    ]
    try:
        with _ob2_sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute("PRAGMA table_info(stn_tl_scrape);")
            have = {row[1] for row in cur.fetchall()}
            for col, decl in cols_needed:
                if col not in have:
                    try:
                        cur.execute(f"ALTER TABLE stn_tl_scrape ADD COLUMN {col} {decl};")
                    except Exception:
                        pass
            # indexes
            try: cur.execute("CREATE INDEX IF NOT EXISTS idx_stn_tl_scrape_tl ON stn_tl_scrape(tl);")
            except Exception: pass
            try: cur.execute("CREATE INDEX IF NOT EXISTS idx_stn_tl_scrape_stn ON stn_tl_scrape(UPPER(stn));")
            except Exception: pass
            try: cur.execute("CREATE INDEX IF NOT EXISTS idx_stn_tl_scrape_shelf ON stn_tl_scrape(UPPER(shelf));")
            except Exception: pass
            try: cur.execute("CREATE INDEX IF NOT EXISTS idx_stn_tl_scrape_box ON stn_tl_scrape(UPPER(box_id));")
            except Exception: pass
            conn.commit()
    except Exception as e:
        print('[BOX-ID][ERROR] attach failed:', e)
        pass

# Ensure upgrade at import time
try:
    _ob2_upgrade_table_add_missing_columns(_OB2_DB_PATH)
except Exception:
    pass

# === Merge STN–TL maps into stn_tl_scrape rows ===
def _db_update_scrape_with_stn_source_dest(*args, **kwargs):
    # disabled (single-table policy)
    return

def _db_attach_box_id_to_scrape(tl: str, box_id: str, stn: str = None, shelf: str = None, db_path: str = None):
    db_path = db_path or _OB2_DB_PATH
    try:
        if _ob2_sqlite3 is None:
            return False
    except NameError:
        return False
    if not tl or not box_id:
        return False
    _ob2_upgrade_table_add_missing_columns(db_path)
    try:
        with _ob2_sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            where = ["UPPER(tl)=UPPER(?)", "(box_id IS NULL OR TRIM(box_id)='')"]
            args = [tl]
            if shelf:
                where.append("(shelf IS NULL OR UPPER(shelf)=UPPER(?))"); args.append(shelf)
            sql = f"UPDATE stn_tl_scrape SET box_id=? {{stn_set}} WHERE " + " AND ".join(where)
            if stn:
                sql = sql.replace('{stn_set}', ', stn=?')
                upd_args = [box_id, stn] + args
            else:
                sql = sql.replace('{stn_set}', '')
                upd_args = [box_id] + args
            cur.execute(sql, upd_args)
            if cur.rowcount:
                conn.commit(); return True
            cur.execute(
                """
                INSERT INTO stn_tl_scrape (tl, wid, fsn, title, category, qty, shelf, stn, source, destination, box_id, pick, tl_complete_status, pack, dispatch)
                VALUES (?, NULL, NULL, NULL, NULL, NULL, ?, ?, NULL, NULL, ?, 0, NULL, 0, 0)
                """,
                (tl, shelf, stn, box_id)
            )
            conn.commit(); return True
    except Exception:
        return False

def stn_final_distinct_shelves_sorted(limit: int = 1000):
    """Return distinct shelf labels, case-insensitive sorted ascending (stable, trimmed)."""
    try:
        with _gpf_sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            sql = (
                'SELECT s FROM ('
                '  SELECT TRIM(COALESCE("Shelf","")) AS s, MIN(ROWID) AS r'
                '  FROM ' + STN_FINAL_TABLE + ' WHERE TRIM(COALESCE("Shelf",""))<>""'
                '  GROUP BY UPPER(s)'
                ') ORDER BY UPPER(s) ASC LIMIT ?'
            )
            cur.execute(sql, (limit,))
            return [r[0] for r in cur.fetchall() if r and r[0]]
    except Exception as e:
        print('[GO PICK][DB] shelves(sorted) query failed:', e)
        return []

def stn_final_best_wid_for_tl_shelf(tl: str, shelf: str):
    """Pick a single, most-probable WID for given TL & Shelf.
    Strategy: frequency DESC; fall back to latest row if ties/no agg.
    """
    tl = (tl or '').strip(); shelf = (shelf or '').strip()
    if not (tl and shelf):
        return None
    try:
        with _gpf_sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            # Most frequent WID for this TL+Shelf
            sql1 = (
                'SELECT w FROM ('
                '  SELECT TRIM(COALESCE("WID",'')) AS w, COUNT(*) AS c'
                '  FROM ' + STN_FINAL_TABLE +
                '  WHERE UPPER("TL-Id")=UPPER(?) AND UPPER("Shelf")=UPPER(?) AND TRIM(COALESCE("WID",""))<>""'
                '  GROUP BY UPPER(w)'
                ') WHERE TRIM(COALESCE(w,''))<>'' ORDER BY c DESC LIMIT 1;'
            )
            cur.execute(sql1, (tl, shelf))
            row = cur.fetchone()
            if row and row[0]:
                return (row[0] or '').strip()
            # Fallback: latest non-empty WID row for this TL+Shelf
            sql2 = (
                'SELECT TRIM(COALESCE("WID",'')) FROM ' + STN_FINAL_TABLE +
                ' WHERE UPPER("TL-Id")=UPPER(?) AND UPPER("Shelf")=UPPER(?) AND TRIM(COALESCE("WID",""))<>""'
                ' ORDER BY ROWID DESC LIMIT 1;'
            )
            cur.execute(sql2, (tl, shelf))
            row = cur.fetchone()
            return (row[0] or '').strip() if row and row[0] else None
    except Exception as e:
        print('[GO PICK][DB] best-wid-for-tl-shelf failed:', e)
        return None


def stn_final_update_pick_message_for_box(tl: str, shelf: str, box_id: str, message: str) -> int:
    tl = (tl or '').strip(); shelf = (shelf or '').strip(); box_id = (box_id or '').strip()
    if not (tl and shelf and box_id):
        return 0
    try:
        import sqlite3
        with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            sql = (
                'UPDATE ' + STN_FINAL_TABLE +
                ' SET "Pick"=? WHERE UPPER("TL-Id")=UPPER(?) AND UPPER("Shelf")=UPPER(?) AND UPPER("Box-Id")=UPPER(?)'
            )
            cur.execute(sql, (message or '', tl, shelf, box_id))
            conn.commit()
            return cur.rowcount or 0
    except Exception as e:
        print('[GO PICK][DB] update Pick (by box) failed:', e)
        return 0


# === GO PICK watcher bootstrap on confirm TL pages ===
try:
    drv_ref, _ = _get_active_driver_wait()
    cur = getattr(drv_ref, 'current_url', '') or ''
    if '/transfer_list/confirm_transfer_list' in cur:
        try:
            _gp_start_close_message_poll_v2(GUI_ROOT or tk.Tk(), tk.StringVar(value=''), interval_ms=600)
            print('[GO PICK][Bootstrap] Watcher attached on confirm_transfer_list page')
        except Exception as _be:
            print('[GO PICK][Bootstrap] attach failed:', _be)
except Exception:
    pass


# ============================ V6 PATCH: PACK-only Consignment + Robust Multi-Box ==========
# Changes:
# 1) When user starts PACK, we open ONLY the Consignment URL (no HOME hop).
# 2) PACK sequence ensures weight=10 + ENTER (auto-pack), then Packing Slip -> qty=1 -> Security Slip.
# 3) Robust for second/next boxes: clears fields, polls banner for current box, strict DB update.
# 4) Processed counter increments ONLY when the DB update succeeded for that box.

# ---- Helpers reused/defined (idempotent) ----
_BOXID_PATTERNS = [
    r"\bTote/Box\s+([A-Z0-9]+)\s+is\s+closed\s+successfully\b",
    r"\bBox\s+packed\s+with\s+id\s*-\s*([A-Z0-9]+)\b",
    r"\bbox(?:\s*id)?\s*[:\-]?\s*([A-Z0-9]{6,})\b"
]

def _extract_box_ids_from_message(msg: str) -> set:
    try:
        import re as _re
        out = set()
        s = (msg or "")
        for pat in _BOXID_PATTERNS:
            for m in _re.finditer(pat, s, flags=_re.I):
                try: out.add(m.group(1).strip().upper())
                except Exception: pass
        return out
    except Exception:
        return set()

try:
    STN_FINAL_DB_PATH
    STN_FINAL_TABLE
except NameError:
    STN_FINAL_DB_PATH = 'stn_final.db'
    STN_FINAL_TABLE = 'stn_final'

# Strict updater (Box-Id exact)

def stn_final_update_pack_by_box(box_id: str, raw_message: str) -> int:
    import sqlite3 as _sqlite3
    target_from_msg = None
    parsed = _extract_box_ids_from_message(raw_message)
    arg_box = (box_id or '').strip().upper()
    if parsed:
        target_from_msg = arg_box if arg_box in parsed else next(iter(parsed))
    else:
        target_from_msg = arg_box
    target = (target_from_msg or '').strip().upper()
    if not target:
        print('[STN-Final][WARN] No Box-Id resolved to update Pack message.')
        return 0
    canonical_msg = f"Box packed with id - {target}"
    tl_status = None
    try:
        if 'tl complete' in (raw_message or '').lower():
            tl_status = 'TL Complete'
    except Exception:
        pass
    try:
        with _sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            sql = (
                f'UPDATE {STN_FINAL_TABLE} '
                f'SET "Pack" = ?, '
                f'    "TL-Id status" = COALESCE("TL-Id status", ?) '
                f'WHERE UPPER("Box-Id") = UPPER(?)'
            )
            cur.execute(sql, (canonical_msg, tl_status, target))
            affected = cur.rowcount or 0
            conn.commit()
            print(f"[STN-Final] Pack updated for Box-Id={target} → '{canonical_msg}' (rows={affected})")
            return affected
    except Exception as e:
        print('[STN-Final][ERROR] stn_final_update_pack_by_box failed:', e)
        return 0

# Selenium helpers from existing imports
from selenium.common.exceptions import TimeoutException as _PK_TO
from selenium.webdriver.common.by import By as _PK_BY
from selenium.webdriver.support.ui import WebDriverWait as _PK_Wait
from selenium.webdriver.support import expected_conditions as _PK_EC
from selenium.webdriver.common.keys import Keys as _PK_KEYS

# Existing IDs referenced earlier in script
# WEIGHT_INPUT_ID = "weight_hidden"
# BOX_ID_INPUT_ID = "tote_id"
# PACK_BOX_BUTTON_ID = "pack_box"
# PACKING_SLIP_BUTTON_ID = "print_label"
# PRINT_QUANTITY_INPUT_ID = "print_quantity"
# SECURITY_SLIP_BUTTON_ID = "print_security_label"
# PACK_BOX_MSG_ID = "pack_box_msg"

# Wait wrappers (reuse existing _pk_wait_* if available, else define light ones)

def _pk_wait_present(driver, by, sel, to=10):
    return _PK_Wait(driver, to).until(_PK_EC.presence_of_element_located((by, sel)))

def _pk_wait_click(driver, by, sel, to=10):
    return _PK_Wait(driver, to).until(_PK_EC.element_to_be_clickable((by, sel)))

# Read current banner

def _pk_read_pack_message(driver):
    try:
        el = _pk_wait_present(driver, _PK_BY.ID, PACK_BOX_MSG_ID, to=7)
        return (el.text or '').strip()
    except Exception:
        return ''

# ---- NEW: Open Consignment directly (no HOME hop) ----

def open_consign_page_direct(driver, wait) -> bool:
    """Open consignment page in the SAME tab without navigating to HOME.
    Robust: retries the same URL; optionally selects warehouse if selector exists on page.
    """
    try:
        print('[PACK] Opening Consignment URL…')
        ok = navigate_if_needed(driver, CONSIGNMENT_BOX_URL, wait)
        if not ok:
            return False
        # If a warehouse selector is present on this page, select the current warehouse
        try:
            dropdowns = driver.find_elements(_PK_BY.ID, 'select-warehouse')
            if dropdowns:
                # Use already-selected global SELECTED_WH_FULL if available
                try:
                    full_name = SELECTED_WH_FULL if 'SELECTED_WH_FULL' in globals() else None
                except Exception:
                    full_name = None
                if full_name:
                    select_warehouse_by_name(driver, wait, full_name)
        except Exception:
            pass
        # Ensure key controls appear
        try:
            _pk_wait_present(driver, _PK_BY.ID, BOX_ID_INPUT_ID, to=12)
            return True
        except Exception:
            return True  # some pages render lazily; we proceed
    except Exception as e:
        print('[PACK] open_consign_page_direct error:', e)
        return False

# ---- V6 single-box runner ----

def process_box_on_consign_page_v6(driver, wait, box_id: str, weight_default: str='10', print_qty: str='1') -> str:
    """V6: weight=10 + ENTER (auto-Pack) -> Packing Slip -> qty=1 -> Security Slip.
    Clears fields per-box, polls banner for current box, and returns the final message.
    """
    msg_text = ''
    try:
        # Clear any previous text/banner (best effort)
        try:
            box_input = _pk_wait_present(driver, _PK_BY.ID, BOX_ID_INPUT_ID, to=TIMEOUT_LONG)
            box_input.clear()
        except Exception:
            pass
        # Step1: Box-Id + ENTER
        print(f"[PACK][{box_id}] V6-Step1: Type Box-Id and press ENTER")
        box_input = _pk_wait_present(driver, _PK_BY.ID, BOX_ID_INPUT_ID, to=TIMEOUT_LONG)
        box_input.send_keys((box_id or '').strip())
        box_input.send_keys(_PK_KEYS.ENTER)
        handle_popups(driver)

        # Step2: weight=10 + ENTER
        print(f"[PACK][{box_id}] V6-Step2: Set weight=10 and press ENTER (auto-Pack)")
        try:
            wt_input = _pk_wait_present(driver, _PK_BY.ID, WEIGHT_INPUT_ID, to=TIMEOUT_MED)
            try:
                wt_input.clear()
            except Exception:
                pass
            _set_value_fast(driver, wt_input, str(weight_default))
            try:
                wt_input.send_keys(_PK_KEYS.ENTER)
            except Exception:
                driver.execute_script("arguments[0].dispatchEvent(new KeyboardEvent('keydown',{key:'Enter'}));", wt_input)
        except Exception as e:
            print(f"[PACK][{box_id}] WARN: weight set/enter failed: {e}")
        import time as _t
        _t.sleep(0.25)
        handle_popups(driver)

        # Step3: Packing Slip (with quick retries), fallback Pack if needed
        print(f"[PACK][{box_id}] V6-Step3: Click 'Packing Slip'")
        slip_clicked = False
        for _ in range(3):
            try:
                slip_btn = _pk_wait_click(driver, _PK_BY.ID, PACKING_SLIP_BUTTON_ID, to=TIMEOUT_SHORT)
                driver.execute_script("arguments[0].click();", slip_btn)
                slip_clicked = True
                handle_popups(driver)
                break
            except Exception:
                _t.sleep(0.25)
        if not slip_clicked:
            try:
                print(f"[PACK][{box_id}] V6-Fallback: click Pack then retry Slip")
                pack_btn = _pk_wait_click(driver, _PK_BY.ID, PACK_BOX_BUTTON_ID, to=TIMEOUT_SHORT)
                driver.execute_script("arguments[0].click();", pack_btn)
                handle_popups(driver)
                slip_btn = _pk_wait_click(driver, _PK_BY.ID, PACKING_SLIP_BUTTON_ID, to=TIMEOUT_MED)
                driver.execute_script("arguments[0].click();", slip_btn)
                slip_clicked = True
                handle_popups(driver)
            except Exception as e:
                print(f"[PACK][{box_id}] WARN: Slip click failed after fallback: {e}")

        # Step4: qty=1
        print(f"[PACK][{box_id}] V6-Step4: Set print quantity=1")
        try:
            pq_input = _pk_wait_present(driver, _PK_BY.ID, PRINT_QUANTITY_INPUT_ID, to=TIMEOUT_MED)
            try:
                pq_input.clear()
            except Exception:
                pass
            _set_value_fast(driver, pq_input, str(print_qty))
        except Exception as e:
            print(f"[PACK][{box_id}] WARN: print quantity set failed: {e}")

        # Step5: Security Slip
        print(f"[PACK][{box_id}] V6-Step5: Click 'Security Slip'")
        try:
            sec_btn = _pk_wait_click(driver, _PK_BY.ID, SECURITY_SLIP_BUTTON_ID, to=TIMEOUT_MED)
            driver.execute_script("arguments[0].click();", sec_btn)
            handle_popups(driver)
        except Exception as e:
            print(f"[PACK][{box_id}] WARN: Security Slip click failed: {e}")

        # Step6: Read message for THIS box with polling
        print(f"[PACK][{box_id}] V6-Step6: Read success message (polling)")
        msg_text = _pk_read_pack_message(driver)
        target = (box_id or '').strip().upper()
        tries = 6
        while tries > 0:
            ids = _extract_box_ids_from_message(msg_text)
            if target in ids and msg_text:
                break
            _t.sleep(0.4)
            msg_text = _pk_read_pack_message(driver)
            tries -= 1
        print(f"[PACK][{box_id}] Message: {msg_text}")
        return msg_text or ''
    except Exception as e:
        print(f"[PACK][{box_id}] ERROR in process_box_on_consign_page_v6: {e}")
        return msg_text or ''

# ---- Override the PACK entry to avoid HOME and use V6 runner ----

def start_pack_flow(self):
    """Open Consignment page in the original tab (NO HOME NAV) and process boxes.
    Uses V6 single-box runner and updates DB only if the message matches the current box.
    """
    global GUI_ACTIVE_DRIVER, SELECTED_WH_FULL
    drv = GUI_ACTIVE_DRIVER
    if drv is None:
        try: self.set_status('No active browser session. Please login first.', is_error=True)
        except Exception: pass
        print('[PACK] No active driver.')
        return
    wait = build_wait(drv, TIMEOUT_MED)
    try:
        self.set_status('PACK: preparing…')
    except Exception: pass

    # OPEN CONSIGNMENT ONLY
    if not open_consign_page_direct(drv, wait):
        try: self.set_status('PACK: Consignment page not reachable', is_error=True)
        except Exception: pass
        print('[PACK] Consignment page not reachable.')
        return

    # Run processing loop
    try:
        self.set_status('PACK: Checking TL status…')
    except Exception: pass

    try:
        if not stn_final_all_tls_complete():
            try: self.set_status('PACK: Not all TLs are complete', is_error=True)
            except Exception: pass
            print('[PACK] Not all TLs are TL Complete yet.')
            return
        box_ids = stn_final_box_ids_to_pack()
        print(f"[PACK] Box-Ids to process: {len(box_ids)}")
        if not box_ids: return

        processed = 0
        for i, bid in enumerate(box_ids, start=1):
            try:
                print(f"[PACK] ({i}/{len(box_ids)}) Processing Box: {bid}")
                msg = process_box_on_consign_page_v6(drv, wait, bid, weight_default='10', print_qty='1')
                ok_to_update = False
                if msg:
                    ids = _extract_box_ids_from_message(msg)
                    if (bid or '').strip().upper() in ids:
                        ok_to_update = True
                if ok_to_update:
                    updated = stn_final_update_pack_by_box(bid, msg)
                    if updated:
                        processed += 1
                else:
                    print(f"[PACK][{bid}] Skip DB update (message refers to different box or empty). msg= {msg!r}")
            except Exception as e:
                print(f"[PACK] ERROR on box {bid}: {e}")
        try:
            self.set_status(f'PACK: Completed. Boxes processed: {processed}', is_error=False)
        except Exception: pass
        print(f'[PACK] Flow finished. Boxes processed: {processed}')
    except Exception as e:
        print('[PACK] FATAL start_pack_flow:', e)
        try: self.set_status('PACK: fatal error', is_error=True)
        except Exception: pass

# ============================ END V6 PATCH =============================================
# ====== BEGIN: Box Label Auto-Generate on GO PICK scan (v6.1) ======
_LABEL_CACHE = {"box": None, "ts": 0.0}

def _stn_final_get_row_for_box(box_id: str):
    try:
        import sqlite3, time
        if not box_id:
            return None
        with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(f'''
                SELECT "Box-Id","STN","Source","Destination","TL-Id"
                FROM {STN_FINAL_TABLE}
                WHERE UPPER("Box-Id") = UPPER(?)
                ORDER BY ROWID DESC LIMIT 1
            ''', (box_id,))
            row = cur.fetchone()
            return row  # (box, stn, src, dst, tl)
    except Exception as e:
        print("[LABEL] DB lookup by Box-Id failed:", e)
        return None

def _stn_final_get_processing_box_for_tl(tl: str):
    try:
        import sqlite3
        if not tl:
            return None
        with sqlite3.connect(STN_FINAL_DB_PATH) as conn:
            cur = conn.cursor()
            # Prefer unmet Pick status if available
            cur.execute(f'''
                SELECT "Box-Id","STN","Source","Destination","TL-Id"
                FROM {STN_FINAL_TABLE}
                WHERE UPPER("TL-Id") = UPPER(?)
                  AND TRIM(COALESCE("Box-Id","")) <> ""
                  AND (TRIM(COALESCE("Pick","")) = "" OR "Pick" IS NULL)
                ORDER BY ROWID ASC
                LIMIT 1
            ''', (tl,))
            row = cur.fetchone()
            if row:
                return row
            # fallback to the latest Box-Id for TL
            cur.execute(f'''
                SELECT "Box-Id","STN","Source","Destination","TL-Id"
                FROM {STN_FINAL_TABLE}
                WHERE UPPER("TL-Id") = UPPER(?)
                  AND TRIM(COALESCE("Box-Id","")) <> ""
                ORDER BY ROWID DESC
                LIMIT 1
            ''', (tl,))
            return cur.fetchone()
    except Exception as e:
        print("[LABEL] DB lookup by TL failed:", e)
        return None

def _label_generate_box_label(box_id: str, stn: str, source: str, destination: str, serial_no: int = 1):
    """
    Create a single-page PDF label with:
    - Top: Code128 barcode (data = box_id)
    - Under barcode: S: {source}   D: {destination}
    - Big center: box_id
    - Bottom-left: QR (data = stn) + text STN
    - Bottom-center: timestamp
    Saved to DOWNLOAD_FOLDER / f"{box_id}_label.pdf" and auto-opened.
    """
    try:
        from reportlab.pdfgen import canvas
        from reportlab.lib.colors import black, white
        from reportlab.graphics.barcode import code128, qr
        from reportlab.graphics.shapes import Drawing
        from reportlab.graphics import renderPDF
        from datetime import datetime
        import os

        # Page size: landscape small label (approx 140mm x 100mm) -> ~400 x 280 pt
        page_w, page_h = 400.0, 280.0

        out_dir = DOWNLOAD_FOLDER
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        out_path = out_dir / f"{(box_id or 'BOX').strip()}_label.pdf"

        c = canvas.Canvas(str(out_path), pagesize=(page_w, page_h))
        c.setFillColor(black)
        c.setStrokeColor(black)

        # Serial number (top-right)
        c.setFont("Helvetica", 10)
        c.drawString(page_w - 10, page_h - 12, str(serial_no))

        # Barcode (top area)
        try:
            bc = code128.Code128((box_id or "").strip(), barHeight=page_h * 0.30, barWidth=1.0, humanReadable=False)
            bc_x = page_w * 0.05
            bc_y = page_h * 0.60
            bc.drawOn(c, bc_x, bc_y)
        except Exception as e:
            print("[LABEL] Code128 draw failed:", e)

        # S:/D: line under barcode
        c.setFont("Helvetica", 10)
        s_line = f"S: {(source or '').strip()}"
        d_line = f"D: {(destination or '').strip()}"
        c.drawString(page_w * 0.06, page_h * 0.56, s_line)
        c.drawRightString(page_w * 0.94, page_h * 0.56, d_line)

        # Big Box-Id in center
        box_text = (box_id or "").strip()
        font_size = 58
        while font_size >= 28:
            width_est = c.stringWidth(box_text, "Helvetica-Bold", font_size)
            if width_est <= page_w * 0.90:
                break
            font_size -= 2
        c.setFont("Helvetica-Bold", font_size)
        c.drawCentredString(page_w / 2.0, page_h * 0.40, box_text)

        # QR (bottom-left) for STN
        try:
            qr_data = (stn or "").strip()
            if qr_data:
                qr_code = qr.QrCodeWidget(qr_data)
                bounds = qr_code.getBounds()
                w = bounds[2] - bounds[0]
                h = bounds[3] - bounds[1]
                size = page_h * 0.22
                d = Drawing(size, size)
                d.add(qr_code)
                d.scale(size / float(w), size / float(h))
                renderPDF.draw(d, c, page_w * 0.06, page_h * 0.11)
        except Exception as e:
            print("[LABEL] QR draw failed:", e)

        # STN text next to QR
        c.setFont("Helvetica-Bold", 16)
        c.drawString(page_w * 0.06 + page_h * 0.22 + 10, page_h * 0.19, (stn or "").strip())

        # Timestamp at bottom-center
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.setFont("Helvetica", 12)
        c.drawCentredString(page_w / 2.0, page_h * 0.07, ts)

        c.showPage()
        c.save()

        _open_file_in_default_app(out_path)
        print(f"[LABEL] Generated: {out_path}")
        return True
    except Exception as e:
        print("[LABEL] Failed to generate label:", e)
        return False

def _autogen_box_label_for_current_pick():
    """
    Infer 'current' box context and generate label once per box
    when user scans/enters value in 'Scan FSN/EAN/Model-Id'.
    """
    try:
        import time
        tl = None
        box = None
        try:
            tl = (GO_PICK_STATE.get("tl") if isinstance(GO_PICK_STATE, dict) else None) or None
            box = (GO_PICK_STATE.get("box") if isinstance(GO_PICK_STATE, dict) else None) or None
        except Exception:
            pass

        row = None
        if box:
            row = _stn_final_get_row_for_box(box)
        if (not row) and tl:
            row = _stn_final_get_processing_box_for_tl(tl)
        if not row:
            return False

        box_id, stn, source, destination, _tl = row

        # Throttle: skip if same box generated within last 1.5s
        now = time.time()
        if _LABEL_CACHE.get("box") == (box_id or "") and (now - (_LABEL_CACHE.get("ts") or 0)) < 1.5:
            return False

        _LABEL_CACHE["box"] = (box_id or "")
        _LABEL_CACHE["ts"] = now

        # Generate asynchronously to keep UI snappy
        threading.Thread(target=_label_generate_box_label, args=(box_id, stn, source, destination, 1), daemon=True).start()
        return True
    except Exception as e:
        print("[LABEL] autogen error:", e)
        return False

# --- Hook into GO PICK scan handlers (best-effort monkey patches) ---
def _patch_after_scan_hooks():
    patched = 0
    # 1) Hook _gp_type_item_code (called when entering/scanning FSN/EAN/Model-Id)
    try:
        if '_gp_type_item_code' in globals():
            _orig = globals()['_gp_type_item_code']
            def _wrap(*a, **k):
                r = None
                try:
                    r = _orig(*a, **k)
                finally:
                    try: _autogen_box_label_for_current_pick()
                    except Exception: pass
                return r
            globals()['_gp_type_item_code'] = _wrap
            patched += 1
    except Exception as e:
        print("[LABEL] patch _gp_type_item_code failed:", e)

    # 2) Also hook _gp_after_wid_scanned as a fallback
    try:
        if '_gp_after_wid_scanned' in globals():
            _orig2 = globals()['_gp_after_wid_scanned']
            def _wrap2(*a, **k):
                r = None
                try:
                    r = _orig2(*a, **k)
                finally:
                    try: _autogen_box_label_for_current_pick()
                    except Exception: pass
                return r
            globals()['_gp_after_wid_scanned'] = _wrap2
            patched += 1
    except Exception as e:
        print("[LABEL] patch _gp_after_wid_scanned failed:", e)

    print(f"[LABEL] GO PICK scan hooks active: {patched}")

try:
    _patch_after_scan_hooks()
except Exception:
    pass
# ====== END: Box Label Auto-Generate on GO PICK scan ======
