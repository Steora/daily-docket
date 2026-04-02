"""
Ontario Court Dates scraper: Selenium (headless Chrome), Pandas, gspread.

Uses Application Default Credentials (google.auth.default()) — no service_account.json.

Open the workbook by ID (recommended): set ``GOOGLE_SHEET_ID`` to the id from the URL
(``.../spreadsheets/d/<ID>/edit...``) or paste the full ``docs.google.com`` link.

Otherwise opens by title: ``GOOGLE_SHEET_NAME`` or the default title below.

Each calendar day uses a **separate subsheet** titled ``YYYY-MM-DD`` (runner local date;
GitHub Actions uses UTC). The first run on a new day **creates** that tab; earlier days'
tabs stay in the workbook. A second run the **same** day reuses and clears that day's tab.
Override the tab title with env ``GOOGLE_SHEET_TAB``. Rows append after each successful extract.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import date
from io import StringIO
from typing import List, Optional

import google.auth
import gspread
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound
import pandas as pd
from google.auth.transport.requests import Request
from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
    UnexpectedAlertPresentException,
)
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    """
    Ensure logs go to stderr. basicConfig() is skipped if the root logger already
    has handlers (common in IDEs), which makes it look like 'no debug logs'.
    """
    level_name = os.environ.get("LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, level_name, logging.INFO)
    root = logging.getLogger()
    # Drop existing handlers so our StreamHandler always runs
    for h in root.handlers[:]:
        root.removeHandler(h)
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.addHandler(handler)
    root.setLevel(level)
    # Third-party noise (optional): keep urllib3 quieter unless DEBUG
    if level > logging.DEBUG:
        logging.getLogger("urllib3").setLevel(logging.WARNING)

BASE_URL = "https://www.ontariocourtdates.ca/Default.aspx"
DEFAULT_SPREADSHEET_TITLE = "Ontario-Daily-Docket"
_SPREADSHEET_ID_RAW = os.environ.get("GOOGLE_SHEET_ID", "").strip()
_SPREADSHEET_TITLE = os.environ.get("GOOGLE_SHEET_NAME", DEFAULT_SPREADSHEET_TITLE)


def _normalize_spreadsheet_id(value: str) -> str:
    """Accept raw id or a full Google Sheets URL and return the spreadsheet id."""
    value = value.strip()
    if "/spreadsheets/d/" in value:
        return value.split("/spreadsheets/d/", 1)[1].split("/")[0].split("?")[0]
    return value


def _open_spreadsheet(gc: gspread.Client):
    if _SPREADSHEET_ID_RAW:
        key = _normalize_spreadsheet_id(_SPREADSHEET_ID_RAW)
        logger.info("Opening spreadsheet by id (GOOGLE_SHEET_ID)")
        try:
            return gc.open_by_key(key)
        except SpreadsheetNotFound as exc:
            raise RuntimeError(
                f"Spreadsheet id {key!r} not found or the service account cannot access it. "
                "In Google Sheets: Share → add the workflow service account email with Editor."
            ) from exc
    logger.warning(
        "GOOGLE_SHEET_ID is unset — opening by title %r (often fails in CI). "
        "Set GitHub Variable or Secret GOOGLE_SHEET_ID to the id in the sheet URL.",
        _SPREADSHEET_TITLE,
    )
    try:
        return gc.open(_SPREADSHEET_TITLE)
    except SpreadsheetNotFound as exc:
        raise RuntimeError(
            f"No spreadsheet titled {_SPREADSHEET_TITLE!r} is visible to this Google identity. "
            "Fix: (1) Set GOOGLE_SHEET_ID to the spreadsheet id from the URL, or "
            "(2) rename the sheet to match GOOGLE_SHEET_NAME / default title, and "
            "(3) share the sheet with the workflow service account (Editor)."
        ) from exc

GOOGLE_SCOPES = (
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
)

# Element IDs (ASP.NET WebForms)
ID_COURT = "ctl00_MainContent_ddlCourt"
ID_CITY = "ctl00_MainContent_ddlCity"
ID_LOB = "ctl00_MainContent_ddlLob"
ID_LOCATION = "ctl00_MainContent_listBoxCourtOffice"
ID_AGREE = "ctl00_MainContent_chkAgree"
ID_ENTER = "ctl00_MainContent_btnEnter"
ID_SUBMIT = "ctl00_MainContent_btnSubmit"

POSTBACK_SLEEP = 1.2
BACK_SLEEP = 2.5
SUBMIT_SLEEP = 5.0
WAIT_SECONDS = 25


def _build_driver() -> tuple[webdriver.Chrome, WebDriverWait]:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options,
    )
    wait = WebDriverWait(driver, WAIT_SECONDS)
    return driver, wait


def _wait_form_ready(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    wait.until(EC.presence_of_element_located((By.ID, ID_COURT)))
    wait.until(EC.element_to_be_clickable((By.ID, ID_COURT)))


def _accept_terms(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    logger.info("Loading site and accepting terms")
    driver.get(BASE_URL)
    agree = wait.until(EC.element_to_be_clickable((By.ID, ID_AGREE)))
    agree.click()
    time.sleep(POSTBACK_SLEEP)
    wait.until(
        lambda d: d.execute_script(
            "var e=document.getElementById(arguments[0]);"
            "return !!(e && !e.disabled);",
            ID_ENTER,
        )
    )
    driver.execute_script(f"document.getElementById('{ID_ENTER}').click();")
    wait.until(EC.presence_of_element_located((By.ID, ID_CITY)))
    time.sleep(POSTBACK_SLEEP)


def _court_options(driver: webdriver.Chrome) -> List[str]:
    sel = Select(driver.find_element(By.ID, ID_COURT))
    out: List[str] = []
    for opt in sel.options:
        t = (opt.text or "").strip()
        if not t or t == "Both":
            continue
        out.append(t)
    return out


def _city_options(driver: webdriver.Chrome) -> List[str]:
    sel = Select(driver.find_element(By.ID, ID_CITY))
    out: List[str] = []
    for opt in sel.options:
        t = (opt.text or "").strip()
        if not t:
            continue
        out.append(t)
    return out


def _lob_options(driver: webdriver.Chrome) -> List[str]:
    sel = Select(driver.find_element(By.ID, ID_LOB))
    out: List[str] = []
    for opt in sel.options:
        t = (opt.text or "").strip()
        if not t or t == "All":
            continue
        out.append(t)
    return out


def _location_options(driver: webdriver.Chrome) -> List[str]:
    sel = Select(driver.find_element(By.ID, ID_LOCATION))
    out: List[str] = []
    for opt in sel.options:
        t = (opt.text or "").strip()
        if not t or "--- All Below ---" in t:
            continue
        out.append(t)
    return out


def _select_by_visible_text_safe(
    driver: webdriver.Chrome,
    element_id: str,
    visible_text: str,
    wait: WebDriverWait,
) -> None:
    last_err: Exception | None = None
    for attempt in range(3):
        try:
            el = wait.until(EC.element_to_be_clickable((By.ID, element_id)))
            Select(el).select_by_visible_text(visible_text)
            return
        except (StaleElementReferenceException, NoSuchElementException) as e:
            last_err = e
            time.sleep(0.4)
    if last_err:
        raise last_err


def _set_court_city(
    driver: webdriver.Chrome, wait: WebDriverWait, court: str, city: str
) -> None:
    _wait_form_ready(driver, wait)
    _select_by_visible_text_safe(driver, ID_COURT, court, wait)
    time.sleep(POSTBACK_SLEEP)
    wait.until(EC.element_to_be_clickable((By.ID, ID_CITY)))
    _select_by_visible_text_safe(driver, ID_CITY, city, wait)
    time.sleep(POSTBACK_SLEEP)


def _set_court_city_lob(
    driver: webdriver.Chrome, wait: WebDriverWait, court: str, city: str, lob: str
) -> None:
    _set_court_city(driver, wait, court, city)
    wait.until(EC.element_to_be_clickable((By.ID, ID_LOB)))
    _select_by_visible_text_safe(driver, ID_LOB, lob, wait)
    time.sleep(POSTBACK_SLEEP)


def _set_dropdowns(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    court: str,
    city: str,
    lob: str,
    location: str,
) -> None:
    """Rebuild full form state after navigation or postback."""
    _set_court_city_lob(driver, wait, court, city, lob)
    wait.until(EC.element_to_be_clickable((By.ID, ID_LOCATION)))
    _select_by_visible_text_safe(driver, ID_LOCATION, location, wait)
    time.sleep(POSTBACK_SLEEP)


def _extract_result_table(driver: webdriver.Chrome) -> pd.DataFrame | None:
    try:
        tables = pd.read_html(StringIO(driver.page_source), flavor="lxml")
    except ValueError:
        return None
    if not tables:
        return None
    return tables[0].copy()


def _append_row_context(
    df: pd.DataFrame,
    court: str,
    municipality: str,
    case_type: str,
    location: str,
) -> pd.DataFrame:
    out = df.copy()
    out["Court"] = court
    out["Municipality"] = municipality
    out["Case Type"] = case_type
    out["Location"] = location
    return out


def _google_sheets_client():
    credentials, _ = google.auth.default(scopes=GOOGLE_SCOPES)
    if credentials.expired and credentials.refresh_token:
        credentials.refresh(Request())
    return gspread.authorize(credentials)


def _dated_worksheet_name() -> str:
    """
    One subsheet name per calendar day: ``YYYY-MM-DD`` in the runner's local timezone
    (GitHub Actions: UTC). Override with env ``GOOGLE_SHEET_TAB`` to force a fixed tab name.
    """
    override = os.environ.get("GOOGLE_SHEET_TAB", "").strip()
    return override if override else date.today().isoformat()


def _get_or_create_dated_worksheet(sh: gspread.Spreadsheet) -> gspread.Worksheet:
    """
    Ensures a worksheet exists whose title is today's date (or ``GOOGLE_SHEET_TAB``).

    - **New calendar date** → that title does not exist yet → **creates a new subsheet**.
      Older date tabs are left unchanged.
    - **Same calendar date, another run** → tab already exists → **reuses** it, clears cells,
      then refills (so you do not accumulate duplicate sheets for the same day).
    """
    name = _dated_worksheet_name()
    try:
        ws = sh.worksheet(name)
        logger.info(
            "[SHEETS] Reusing subsheet %r (same calendar date); clearing for this run — other date tabs unchanged",
            name,
        )
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows=5000, cols=40)
        logger.info(
            "[SHEETS] New subsheet %r — first run for this calendar date (previous days' tabs kept)",
            name,
        )
    ws.clear()
    logger.info("[SHEETS] Tab %r cleared; incremental writes will follow", name)
    return ws


class _IncrementalSheetWriter:
    """Write header + rows on first chunk; append_rows for each later chunk (same columns as first)."""

    def __init__(self, ws: gspread.Worksheet):
        self.ws = ws
        self._header: Optional[List[str]] = None
        self.total_data_rows = 0

    def append_dataframe(self, df: pd.DataFrame) -> None:
        df2 = df.fillna("")
        if self._header is None:
            self._header = df2.columns.tolist()
            rows = df2.values.tolist()
            block = [self._header] + rows
            self.ws.update(block, "A1", value_input_option="USER_ENTERED")
            self.total_data_rows += len(rows)
            logger.info(
                "[SHEETS] First batch on tab %r: header + %s data rows",
                self.ws.title,
                len(rows),
            )
            return
        sub = df2.reindex(columns=self._header, fill_value="")
        rows = sub.values.tolist()
        if not rows:
            return
        self.ws.append_rows(rows, value_input_option="USER_ENTERED")
        self.total_data_rows += len(rows)
        logger.info(
            "[SHEETS] Appended %s rows on tab %r (total data rows so far: %s)",
            len(rows),
            self.ws.title,
            self.total_data_rows,
        )


def _prepare_workbook_and_dated_tab() -> Optional[gspread.Worksheet]:
    """
    Open spreadsheet, get/create today's tab, clear it. Returns worksheet or None on failure.
    """
    logger.info("[SHEETS] Connecting and preparing dated worksheet before web extraction…")
    try:
        gc = _google_sheets_client()
        sh = _open_spreadsheet(gc)
        ws = _get_or_create_dated_worksheet(sh)
        url = getattr(sh, "url", None) or (
            f"https://docs.google.com/spreadsheets/d/{sh.id}/edit"
        )
        logger.info(
            "[SHEETS] SUCCESS — workbook %r | data tab %r (open this tab for live updates) | %s",
            sh.title,
            ws.title,
            url,
        )
        return ws
    except Exception:
        logger.exception(
            "[SHEETS] FAILED — could not open spreadsheet or prepare tab "
            "(check GOOGLE_SHEET_ID / GOOGLE_SHEET_NAME, ADC/WIF, and Share with the service account)"
        )
        return None


def scrape_to_dataframes(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
    sheet_writer: Optional[_IncrementalSheetWriter] = None,
) -> List[pd.DataFrame]:
    master: List[pd.DataFrame] = []
    _accept_terms(driver, wait)
    _wait_form_ready(driver, wait)

    courts = _court_options(driver)
    logger.info("Courts to scrape (excl. Both): %s", len(courts))

    for court in courts:
        _wait_form_ready(driver, wait)
        _select_by_visible_text_safe(driver, ID_COURT, court, wait)
        time.sleep(POSTBACK_SLEEP)
        wait.until(EC.element_to_be_clickable((By.ID, ID_CITY)))
        cities = _city_options(driver)
        logger.info("Court %r: %s municipalities", court, len(cities))

        for city in cities:
            _set_court_city(driver, wait, court, city)
            wait.until(EC.element_to_be_clickable((By.ID, ID_LOB)))
            lobs = _lob_options(driver)
            for lob in lobs:
                _set_court_city_lob(driver, wait, court, city, lob)
                wait.until(EC.element_to_be_clickable((By.ID, ID_LOCATION)))
                locations = _location_options(driver)
                for loc in locations:
                    try:
                        _set_dropdowns(driver, wait, court, city, lob, loc)
                        submit = wait.until(
                            EC.element_to_be_clickable((By.ID, ID_SUBMIT))
                        )
                        submit.click()
                        time.sleep(SUBMIT_SLEEP)
                        tbl = _extract_result_table(driver)
                        if tbl is not None and not tbl.empty:
                            chunk = _append_row_context(tbl, court, city, lob, loc)
                            master.append(chunk)
                            if sheet_writer is not None:
                                try:
                                    sheet_writer.append_dataframe(chunk)
                                except Exception:
                                    logger.exception(
                                        "[SHEETS] Incremental write failed (scraping continues)"
                                    )
                            logger.info(
                                "Extracted %s rows for %s / %s / %s / %s",
                                len(tbl),
                                court,
                                city,
                                lob,
                                loc,
                            )
                        else:
                            logger.info(
                                "No table for %s / %s / %s / %s",
                                court,
                                city,
                                lob,
                                loc,
                            )
                    except UnexpectedAlertPresentException:
                        try:
                            driver.switch_to.alert.accept()
                        except Exception:
                            pass
                        logger.exception("Alert dismissed; skipping combination")
                    except TimeoutException:
                        logger.exception("Timeout; skipping combination")
                    except Exception:
                        logger.exception("Error on combination; continuing")

                    driver.back()
                    time.sleep(BACK_SLEEP)
                    _wait_form_ready(driver, wait)

    return master


def main() -> int:
    _configure_logging()
    ws = _prepare_workbook_and_dated_tab()
    if ws is None:
        return 1
    writer = _IncrementalSheetWriter(ws)

    driver, wait = _build_driver()
    try:
        chunks = scrape_to_dataframes(driver, wait, sheet_writer=writer)
    finally:
        driver.quit()

    if not chunks:
        logger.warning("No data collected")
        try:
            ws.update(
                "A1",
                [["No data collected this run"]],
                value_input_option="USER_ENTERED",
            )
        except Exception:
            logger.exception("Could not write placeholder to dated tab")
        return 1

    logger.info(
        "Done. Total data rows on tab %r: %s (header + rows written incrementally)",
        ws.title,
        writer.total_data_rows,
    )
    try:
        populated = len(ws.get_all_values())
        logger.info(
            "[SHEETS] Read-back: %s populated rows on tab %r (includes header)",
            populated,
            ws.title,
        )
    except Exception as exc:
        logger.warning("[SHEETS] Could not read back final row count: %s", exc)
    return 0


if __name__ == "__main__":
    sys.exit(main())
