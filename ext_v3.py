"""
Ontario Court Dates scraper: Selenium (headless Chrome), Pandas, gspread.

Uses Application Default Credentials (google.auth.default()) — no service_account.json.

Open the workbook by ID (recommended): set ``GOOGLE_SHEET_ID`` to the id from the URL
(``.../spreadsheets/d/<ID>/edit...``) or paste the full ``docs.google.com`` link.

Otherwise opens by title: ``GOOGLE_SHEET_NAME`` or the default title below.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from io import StringIO
from typing import List

import google.auth
import gspread
from gspread.exceptions import SpreadsheetNotFound
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


def _probe_sheet_connection_at_start() -> bool:
    """
    Open the target workbook and first worksheet once before Selenium runs.
    Logs clear SUCCESS / FAILED lines for CI and local debugging.
    """
    logger.info("[SHEETS] Checking spreadsheet access before web extraction…")
    try:
        gc = _google_sheets_client()
        sh = _open_spreadsheet(gc)
        ws = sh.get_worksheet(0)
        url = getattr(sh, "url", None) or (
            f"https://docs.google.com/spreadsheets/d/{sh.id}/edit"
        )
        logger.info(
            "[SHEETS] SUCCESS — connected to workbook %r | first worksheet %r | %s",
            sh.title,
            ws.title,
            url,
        )
        return True
    except Exception:
        logger.exception(
            "[SHEETS] FAILED — could not open spreadsheet before extraction "
            "(check GOOGLE_SHEET_ID / GOOGLE_SHEET_NAME, ADC/WIF, and Share with the service account)"
        )
        return False


def _log_sheets_verify(
    sh: gspread.Spreadsheet,
    ws: gspread.Worksheet,
    header: List[str],
    body: list[list],
    update_result: object,
) -> None:
    """Common debug lines: confirm which sheet was touched and that data round-trips."""
    url = getattr(sh, "url", None) or (
        f"https://docs.google.com/spreadsheets/d/{sh.id}/edit"
    )
    nrows = len(body)
    ncols = len(header) if header else 0
    logger.info(
        "[SHEETS VERIFY] Workbook: %s | worksheet: %r | wrote %s rows x %s cols (incl. header)",
        url,
        ws.title,
        nrows,
        ncols,
    )
    if isinstance(update_result, dict):
        logger.info(
            "[SHEETS VERIFY] API: updatedRows=%s updatedColumns=%s updatedCells=%s | range=%s",
            update_result.get("updatedRows"),
            update_result.get("updatedColumns"),
            update_result.get("updatedCells"),
            update_result.get("updatedRange"),
        )
    elif update_result is not None:
        logger.info("[SHEETS VERIFY] API raw response: %s", update_result)

    try:
        a1 = ws.acell("A1").value
        want_a1 = str(header[0]) if header else ""
        got_a1 = str(a1) if a1 is not None else ""
        if got_a1 == want_a1:
            logger.info("[SHEETS VERIFY] Read-back OK: A1 == header[0] (%r)", got_a1[:120])
        else:
            logger.warning(
                "[SHEETS VERIFY] Read-back mismatch: A1=%r vs expected header[0]=%r",
                got_a1[:120],
                want_a1[:120],
            )
        if nrows >= 1:
            tail = ws.acell(f"A{nrows}").value
            logger.info(
                "[SHEETS VERIFY] Read-back sample: row %s col A = %r",
                nrows,
                (str(tail) if tail is not None else "")[:120],
            )
        _max_full_scan = 2000
        if nrows <= _max_full_scan:
            populated = len(ws.get_all_values())
            if populated == nrows:
                logger.info(
                    "[SHEETS VERIFY] Row count OK: %s populated rows (matches upload)",
                    populated,
                )
            else:
                logger.warning(
                    "[SHEETS VERIFY] Row count mismatch: got %s populated rows, expected %s",
                    populated,
                    nrows,
                )
        else:
            logger.info(
                "[SHEETS VERIFY] Skipping full-sheet row count (nrows=%s > %s); A1/tail checks above suffice",
                nrows,
                _max_full_scan,
            )
    except Exception as exc:
        logger.warning(
            "[SHEETS VERIFY] Read-back / row-count check failed (upload may still be OK): %s",
            exc,
        )


def _upload_dataframe_to_sheet(df: pd.DataFrame) -> None:
    logger.info("Authenticating to Google Sheets")
    gc = _google_sheets_client()
    sh = _open_spreadsheet(gc)
    ws = sh.get_worksheet(0)
    url = getattr(sh, "url", None) or (
        f"https://docs.google.com/spreadsheets/d/{sh.id}/edit"
    )
    logger.info("[SHEETS VERIFY] Target workbook: %s | worksheet: %r", url, ws.title)

    ws.clear()
    df2 = df.fillna("")
    header = df2.columns.tolist()
    rows = df2.values.tolist()
    if not header and not rows:
        logger.warning("Empty dataframe; sheet cleared only")
        logger.info("[SHEETS VERIFY] Sheet cleared; no data rows to verify")
        return
    body = [header] + rows
    update_result = ws.update(body, "A1", value_input_option="USER_ENTERED")
    _log_sheets_verify(sh, ws, header, body, update_result)
    logger.info("Uploaded %s data rows (plus header) to first worksheet", len(rows))


def scrape_to_dataframes(driver: webdriver.Chrome, wait: WebDriverWait) -> List[pd.DataFrame]:
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
                            master.append(
                                _append_row_context(tbl, court, city, lob, loc)
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
    if not _probe_sheet_connection_at_start():
        return 1
    driver, wait = _build_driver()
    try:
        chunks = scrape_to_dataframes(driver, wait)
    finally:
        driver.quit()

    if not chunks:
        logger.warning("No data collected; clearing sheet and exiting")
        try:
            _upload_dataframe_to_sheet(pd.DataFrame())
        except Exception:
            logger.exception("Failed to update empty sheet")
        return 1

    final_df = pd.concat(chunks, ignore_index=True)
    final_df.dropna(how="all", inplace=True)
    try:
        _upload_dataframe_to_sheet(final_df)
    except Exception:
        logger.exception("Upload failed")
        return 1
    logger.info("Done. Total rows: %s", len(final_df))
    return 0


if __name__ == "__main__":
    sys.exit(main())
