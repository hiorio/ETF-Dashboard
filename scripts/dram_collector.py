"""
D램 현물가/고정거래가 수집기 (TrendForce)
실행: python3 scripts/dram_collector.py
"""

import json
import re
import sqlite3
import logging
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH  = DATA_DIR / "etf.db"
JSON_OUT = BASE_DIR / "docs" / "dram.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# ── DB 초기화 ──────────────────────────────────────────────────────────────

def init_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dram_prices (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            date           TEXT NOT NULL,
            type           TEXT NOT NULL,
            capacity       TEXT NOT NULL,
            spot_price     REAL,
            contract_price REAL,
            currency       TEXT DEFAULT 'USD',
            source         TEXT,
            UNIQUE(date, type, capacity)
        )
    """)
    conn.commit()


def save_prices(conn, records):
    """records: list of dict with date/type/capacity/spot_price/contract_price/currency/source"""
    saved = 0
    for r in records:
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO dram_prices
                    (date, type, capacity, spot_price, contract_price, currency, source)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    r["date"], r["type"], r["capacity"],
                    r.get("spot_price"), r.get("contract_price"),
                    r.get("currency", "USD"), r.get("source", "trendforce"),
                ),
            )
            if conn.execute("SELECT changes()").fetchone()[0]:
                saved += 1
        except Exception as e:
            log.warning(f"DB 저장 오류: {e} — {r}")
    conn.commit()
    return saved


# ── TrendForce 스크래핑 ────────────────────────────────────────────────────

def _to_float(s):
    if s is None:
        return None
    try:
        return float(re.sub(r"[^\d.]", "", str(s)))
    except (ValueError, TypeError):
        return None


def _scrape_with_requests():
    """requests + BS4로 TrendForce 공개 DRAM 데이터 수집"""
    results = []
    url = "https://www.trendforce.com/markets/dram"
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        log.info(f"TrendForce HTTP {res.status_code} ({len(res.content)} bytes)")
        if res.status_code != 200:
            return results

        soup = BeautifulSoup(res.text, "lxml")

        # 1) JSON-LD / window.__INITIAL_STATE__ / __NEXT_DATA__ 탐색
        for tag in soup.find_all("script"):
            text = tag.string or ""
            # Next.js __NEXT_DATA__
            if "__NEXT_DATA__" in text or "pageProps" in text:
                m = re.search(r'({.*"dram".*})', text, re.DOTALL)
                if m:
                    log.info("__NEXT_DATA__ JSON 발견")
                    try:
                        data = json.loads(m.group(1))
                        parsed = _parse_json_data(data)
                        results.extend(parsed)
                    except Exception:
                        pass

            # window.__data__ 또는 유사 패턴
            for pat in (r'window\.__data__\s*=\s*({.*?});', r'window\.data\s*=\s*({.*?});'):
                m = re.search(pat, text, re.DOTALL)
                if m:
                    try:
                        data = json.loads(m.group(1))
                        parsed = _parse_json_data(data)
                        results.extend(parsed)
                    except Exception:
                        pass

        if results:
            return results

        # 2) 일반 테이블 탐색
        results.extend(_parse_tables(soup))

        if results:
            return results

        log.warning("TrendForce: 파싱 가능한 구조 없음 (JS 렌더링 필요할 수 있음)")

    except requests.RequestException as e:
        log.warning(f"TrendForce requests 오류: {e}")

    return results


def _scrape_with_playwright():
    """Playwright로 JS 렌더링 후 스크래핑 (requests 실패 시 폴백)"""
    results = []
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        log.warning("Playwright 미설치 — pip install playwright 후 playwright install chromium")
        return results

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            page = browser.new_page(extra_http_headers={
                "User-Agent": HEADERS["User-Agent"]
            })

            # 네트워크 응답 인터셉트 — API JSON 캡처
            api_data = {}
            def on_response(response):
                if "dram" in response.url.lower() and response.status == 200:
                    try:
                        ct = response.headers.get("content-type", "")
                        if "json" in ct:
                            api_data[response.url] = response.json()
                            log.info(f"API JSON 캡처: {response.url}")
                    except Exception:
                        pass
            page.on("response", on_response)

            log.info("Playwright: TrendForce 페이지 로드 중...")
            try:
                page.goto("https://www.trendforce.com/markets/dram", timeout=30000)
                page.wait_for_load_state("networkidle", timeout=20000)
            except PWTimeout:
                log.warning("Playwright: 페이지 로드 타임아웃, 현재 상태로 파싱 시도")

            # 캡처된 API 응답 파싱
            for url, data in api_data.items():
                parsed = _parse_json_data(data)
                results.extend(parsed)
                log.info(f"API 데이터 파싱: {len(parsed)}건 (from {url})")

            if not results:
                # 페이지 HTML로 파싱 시도
                html = page.content()
                soup = BeautifulSoup(html, "lxml")
                results.extend(_parse_tables(soup))

            browser.close()

    except Exception as e:
        log.error(f"Playwright 오류: {type(e).__name__}: {e}")

    return results


def _parse_json_data(data):
    """JSON 응답에서 DRAM 가격 데이터 추출 (구조 불확실 → 재귀 탐색)"""
    results = []

    def search(obj, depth=0):
        if depth > 6:
            return
        if isinstance(obj, dict):
            # spot/contract 키 패턴 탐색
            keys_lower = {k.lower(): k for k in obj}
            spot = _to_float(obj.get(keys_lower.get("spot")) or obj.get(keys_lower.get("spot_price"))
                             or obj.get(keys_lower.get("spotprice")))
            contract = _to_float(obj.get(keys_lower.get("contract")) or obj.get(keys_lower.get("contract_price"))
                                  or obj.get(keys_lower.get("contractprice")))
            cap_key = keys_lower.get("capacity") or keys_lower.get("spec") or keys_lower.get("density")
            typ_key = keys_lower.get("type") or keys_lower.get("product")
            if spot is not None and cap_key:
                results.append({
                    "spot_price": spot,
                    "contract_price": contract,
                    "capacity": str(obj.get(cap_key, "")),
                    "type": str(obj.get(typ_key, "DDR4")) if typ_key else "DDR4",
                })
            for v in obj.values():
                search(v, depth + 1)
        elif isinstance(obj, list):
            for item in obj:
                search(item, depth + 1)

    search(data)
    return results


def _parse_tables(soup):
    """BeautifulSoup에서 테이블 기반 DRAM 가격 파싱"""
    results = []
    date_str = datetime.now().strftime("%Y-%m-%d")

    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        if not any(w in " ".join(headers) for w in ("ddr", "gb", "spot", "price", "dram")):
            continue

        log.info(f"테이블 발견: headers={headers}")
        col_map = {}
        for i, h in enumerate(headers):
            if any(w in h for w in ("spot",)):
                col_map["spot"] = i
            elif any(w in h for w in ("contract", "fixed")):
                col_map["contract"] = i
            elif any(w in h for w in ("capacity", "spec", "type", "product", "gb", "ddr")):
                col_map["product"] = i

        for row in table.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if not cells:
                continue
            product = cells[col_map.get("product", 0)] if col_map.get("product") is not None else cells[0]
            spot = _to_float(cells[col_map["spot"]]) if "spot" in col_map and len(cells) > col_map["spot"] else None
            contract = _to_float(cells[col_map["contract"]]) if "contract" in col_map and len(cells) > col_map["contract"] else None

            # DDR 타입 / 용량 파싱
            m_type = re.search(r'(DDR[45])', product, re.IGNORECASE)
            m_cap  = re.search(r'(\d+GB)', product, re.IGNORECASE)
            if (spot or contract):
                results.append({
                    "date": date_str,
                    "type": m_type.group(1).upper() if m_type else "DDR4",
                    "capacity": m_cap.group(1).upper() if m_cap else product,
                    "spot_price": spot,
                    "contract_price": contract,
                    "currency": "USD",
                    "source": "trendforce",
                })

    return results


# ── JSON export ────────────────────────────────────────────────────────────

def export_json(conn):
    """DB → docs/dram.json 내보내기"""
    rows = conn.execute("""
        SELECT date, type, capacity, spot_price, contract_price, currency
        FROM dram_prices
        ORDER BY type, capacity, date
    """).fetchall()

    data = {}
    for r in rows:
        key = f"{r[1]}_{r[2]}"  # e.g. "DDR4_8GB"
        data.setdefault(key, {
            "type": r[1], "capacity": r[2], "currency": r[5], "dates": [], "spot": [], "contract": []
        })
        data[key]["dates"].append(r[0])
        data[key]["spot"].append(r[3])
        data[key]["contract"].append(r[4])

    # 최신 가격 (현재가)
    latest = conn.execute("""
        SELECT type, capacity, spot_price, contract_price, currency, date
        FROM dram_prices
        WHERE date = (SELECT MAX(date) FROM dram_prices WHERE type=dram_prices.type AND capacity=dram_prices.capacity)
        ORDER BY type, capacity
    """).fetchall()
    latest_list = [{"type": r[0], "capacity": r[1], "spot": r[2], "contract": r[3],
                    "currency": r[4], "date": r[5]} for r in latest]

    # 이전 주 대비 변동
    prev_week = conn.execute("""
        SELECT p.type, p.capacity, p.spot_price, p.date
        FROM dram_prices p
        WHERE p.date = (
            SELECT MAX(p2.date) FROM dram_prices p2
            WHERE p2.type = p.type AND p2.capacity = p.capacity
              AND p2.date < (SELECT MAX(p3.date) FROM dram_prices p3
                             WHERE p3.type=p.type AND p3.capacity=p.capacity)
        )
    """).fetchall()
    prev_map = {(r[0], r[1]): r[2] for r in prev_week}

    for item in latest_list:
        prev = prev_map.get((item["type"], item["capacity"]))
        if prev and item["spot"]:
            item["spot_change_pct"] = round((item["spot"] - prev) / prev * 100, 2)
        else:
            item["spot_change_pct"] = None

    out = {"series": list(data.values()), "latest": latest_list,
           "updated": datetime.now().strftime("%Y-%m-%d %H:%M KST")}
    JSON_OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(f"JSON 내보내기 완료: {JSON_OUT} ({len(latest_list)}개 품목)")


# ── 메인 ──────────────────────────────────────────────────────────────────

def main():
    DATA_DIR.mkdir(exist_ok=True)
    JSON_OUT.parent.mkdir(exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    date_str = datetime.now().strftime("%Y-%m-%d")
    log.info(f"D램 가격 수집 시작: {date_str}")

    # 1. requests 시도
    records = _scrape_with_requests()

    # 2. 데이터 없으면 Playwright 시도
    if not records:
        log.info("requests로 데이터 없음 → Playwright 시도")
        records = _scrape_with_playwright()

    # 날짜 채우기 (테이블 파싱은 날짜 없음)
    for r in records:
        r.setdefault("date", date_str)
        r.setdefault("currency", "USD")
        r.setdefault("source", "trendforce")

    if records:
        saved = save_prices(conn, records)
        log.info(f"저장: {saved}건 신규 / {len(records)}건 수집")
    else:
        log.warning("수집된 데이터 없음 — 기존 데이터 유지")

    # 항상 JSON export (기존 데이터라도 최신 상태 유지)
    export_json(conn)
    conn.close()
    log.info("D램 수집 완료")


if __name__ == "__main__":
    main()
