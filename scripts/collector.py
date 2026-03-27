"""
ETF 데이터 수집기
실행: python3 scripts/collector.py
"""

import json
import sqlite3
import requests
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "etf.db"
ETF_LIST_PATH = DATA_DIR / "etf_list.json"

TODAY = datetime.now().strftime("%Y%m%d")
ONE_YEAR_AGO = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")

CYCLE_FREQ = {"일": 252, "주": 52, "월": 12, "분기": 4, "반기": 2, "연": 1}


# ── DB 초기화 ──────────────────────────────────────────────────────────────

_MIGRATIONS = [
    "ALTER TABLE etf_meta ADD COLUMN dividend_timing TEXT",
    "ALTER TABLE etf_weekly ADD COLUMN price_prev REAL",
    "ALTER TABLE etf_weekly ADD COLUMN price_change REAL",
    "ALTER TABLE etf_weekly ADD COLUMN price_change_pct REAL",
    "ALTER TABLE etf_weekly ADD COLUMN aum REAL",
    "ALTER TABLE etf_weekly ADD COLUMN dist_rate_monthly REAL",
    "ALTER TABLE etf_weekly ADD COLUMN return_1m REAL",
    "ALTER TABLE etf_weekly ADD COLUMN return_3m REAL",
    "ALTER TABLE etf_weekly ADD COLUMN return_6m REAL",
]


def init_db(conn):
    # 기존 DB 컬럼 추가 마이그레이션
    for sql in _MIGRATIONS:
        try:
            conn.execute(sql)
            conn.commit()
        except Exception:
            pass  # 이미 존재하면 무시

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS etf_meta (
            code             TEXT PRIMARY KEY,
            name             TEXT,
            country          TEXT,
            strategy         TEXT,
            dividend_cycle   TEXT,
            dividend_timing  TEXT,
            manager          TEXT,
            listed_date      TEXT
        );

        CREATE TABLE IF NOT EXISTS etf_weekly (
            id                       INTEGER PRIMARY KEY AUTOINCREMENT,
            code                     TEXT NOT NULL,
            collected_at             TEXT NOT NULL,
            nav_current              REAL,
            price_prev               REAL,
            price_change             REAL,
            price_change_pct         REAL,
            aum                      REAL,
            nav_change_1y            REAL,
            nav_change_since_listing REAL,
            return_1m                REAL,
            return_3m                REAL,
            return_6m                REAL,
            dist_rate_12m            REAL,
            dist_rate_monthly        REAL,
            dist_rate_annualized     REAL,
            real_return_1y           REAL,
            UNIQUE(code, collected_at)
        );

        CREATE TABLE IF NOT EXISTS etf_monthly_dist (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            code        TEXT NOT NULL,
            year_month  TEXT NOT NULL,
            amount      REAL,
            UNIQUE(code, year_month)
        );
    """)
    conn.commit()


def upsert_meta(conn, etf):
    code = etf.get("code") or etf.get("ticker")
    conn.execute(
        """
        INSERT OR REPLACE INTO etf_meta
            (code, name, country, strategy, dividend_cycle, dividend_timing, manager, listed_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            code,
            etf.get("name"),
            etf.get("country"),
            etf.get("strategy"),
            etf.get("dividend_cycle"),
            etf.get("dividend_timing"),
            etf.get("manager"),
            etf.get("listed_date"),
        ),
    )
    conn.commit()


def save_monthly_dists(conn, code, monthly_dists):
    """월별 분배금 dict {YYYY-MM: amount} → etf_monthly_dist 저장"""
    for ym, amount in monthly_dists.items():
        conn.execute(
            "INSERT OR REPLACE INTO etf_monthly_dist (code, year_month, amount) VALUES (?, ?, ?)",
            (code, ym, amount),
        )
    conn.commit()


# ── Yahoo Finance 세션 ─────────────────────────────────────────────────────

def _yf_session():
    """Yahoo Finance 쿠키 + crumb 세션 초기화"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://finance.yahoo.com",
    })
    try:
        session.get("https://finance.yahoo.com/", timeout=10)
    except Exception:
        pass
    crumb = None
    try:
        r = session.get("https://query1.finance.yahoo.com/v1/test/getcrumb", timeout=10)
        if r.status_code == 200 and r.text:
            crumb = r.text.strip()
    except Exception:
        pass
    log.info(f"Yahoo Finance 세션 초기화 (crumb: {'있음' if crumb else '없음'})")
    return session, crumb


def _get_aum(ticker, session, crumb):
    """Yahoo Finance 종목 페이지 HTML에서 AUM 파싱 (API 401 우회)"""
    import re as _re
    try:
        url = f"https://finance.yahoo.com/quote/{ticker}"
        res = session.get(url, timeout=15)
        log.info(f"[{ticker}] AUM HTML {res.status_code} ({len(res.content)} bytes)")
        if res.status_code != 200:
            log.warning(f"[{ticker}] AUM 수집 실패")
            return None
        text = res.text
        # HTML 내 JSON이 이중 인코딩되어 따옴표가 \" 로 이스케이프됨
        # → 필드명만 찾고, 그 뒤 80자 안에서 raw:숫자 패턴 매칭
        for field in ("totalAssets", "netAssets"):
            idx = text.find(field)
            if idx != -1:
                snippet = text[idx:idx + 80]
                m = _re.search(r'raw[^:]*:\s*(\d+)', snippet)
                if m:
                    val = float(m.group(1))
                    log.info(f"[{ticker}] AUM={val:,.0f} (from HTML {field})")
                    return val
        log.warning(f"[{ticker}] AUM 필드 없음 (HTML 파싱 실패)")
    except Exception as e:
        log.warning(f"[{ticker}] AUM 오류: {e}")
    return None


# ── Yahoo Finance Chart API ────────────────────────────────────────────────

def collect_via_yahoo_api(ticker, listed_date, dividend_cycle="월", session=None, crumb=None):
    """Yahoo Finance Chart API로 가격 이력 + 분배금 전체 수집 → dict"""
    empty = {
        "nav_current": None, "price_prev": None,
        "price_change": None, "price_change_pct": None,
        "nav_change_1y": None, "nav_change_since_listing": None,
        "return_1m": None, "return_3m": None, "return_6m": None,
        "dist_rate_12m": None, "dist_rate_monthly": None, "dist_rate_annualized": None,
        "monthly_dists": {},
    }
    try:
        import pandas as pd

        if session is None:
            session, crumb = _yf_session()

        listing_dt = datetime.strptime(listed_date, "%Y-%m-%d")
        now = datetime.now()
        start_ts = int(listing_dt.timestamp())
        end_ts   = int(now.timestamp())

        params = {
            "period1": start_ts,
            "period2": end_ts,
            "interval": "1d",          # 일별 데이터로 기간 수익률 정확도 향상
            "events": "dividends",
            "includePrePost": "false",
        }
        if crumb:
            params["crumb"] = crumb

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        res = session.get(url, params=params, timeout=15)
        log.info(f"[{ticker}] Yahoo API {res.status_code} ({len(res.content)} bytes)")

        data = res.json()
        chart_result = data.get("chart", {})
        if chart_result.get("error"):
            log.error(f"[{ticker}] Yahoo API 에러: {chart_result['error']}")
            return empty

        results = chart_result.get("result") or []
        if not results:
            log.warning(f"[{ticker}] Yahoo API result 없음")
            return empty

        chart  = results[0]
        closes = chart["indicators"]["quote"][0].get("close") or []
        tss    = chart.get("timestamp") or []

        valid = [(t, c) for t, c in zip(tss, closes) if c is not None]
        if not valid:
            log.warning(f"[{ticker}] 유효한 가격 데이터 없음")
            return empty

        prices = pd.Series(
            [v[1] for v in valid],
            index=pd.to_datetime([v[0] for v in valid], unit="s"),
        )

        price_now = float(prices.iloc[-1])
        result = dict(empty)
        result["nav_current"] = price_now

        # 전일가 / 등락
        if len(prices) >= 2:
            prev = float(prices.iloc[-2])
            result["price_prev"]        = prev
            result["price_change"]      = round(price_now - prev, 4)
            result["price_change_pct"]  = round((price_now / prev - 1) * 100, 2) if prev else None

        # 기간별 수익률
        def pct_return(dt):
            sub = prices[prices.index >= dt]
            if not sub.empty and float(sub.iloc[0]):
                return round((price_now / float(sub.iloc[0]) - 1) * 100, 2)
            return None

        result["return_1m"]              = pct_return(now - timedelta(days=30))
        result["return_3m"]              = pct_return(now - timedelta(days=91))
        result["return_6m"]              = pct_return(now - timedelta(days=182))
        result["nav_change_1y"]          = pct_return(now - timedelta(days=365))
        p_first = float(prices.iloc[0])
        result["nav_change_since_listing"] = round((price_now / p_first - 1) * 100, 2) if p_first else None

        # 분배금
        raw_divs = (chart.get("events") or {}).get("dividends") or {}
        if raw_divs:
            divs = pd.Series({
                pd.Timestamp.fromtimestamp(int(k)): float(v["amount"])
                for k, v in raw_divs.items()
            }).sort_index()

            # 월별 집계 (직전 1년)
            one_year_ago_dt = now - timedelta(days=365)
            monthly = divs.resample("ME").sum()
            monthly = monthly[monthly > 0]
            result["monthly_dists"] = {
                ts.strftime("%Y-%m"): round(float(v), 4)
                for ts, v in monthly.items()
            }

            divs_12m = divs[divs.index >= one_year_ago_dt]
            if not divs_12m.empty:
                dist_12m = float(divs_12m.sum())
                last_dist = float(divs_12m.iloc[-1])
                freq = CYCLE_FREQ.get(dividend_cycle, 12)

                result["dist_rate_12m"]        = round(dist_12m / price_now * 100, 2)
                result["dist_rate_monthly"]    = round(last_dist / price_now * 100, 2) if last_dist else None
                result["dist_rate_annualized"] = round(last_dist * freq / price_now * 100, 2) if last_dist else None

        log.info(
            f"[{ticker}] 현재가={price_now}  전일대비={result['price_change_pct']}%  "
            f"1M={result['return_1m']}%  1Y={result['nav_change_1y']}%  "
            f"분배율12M={result['dist_rate_12m']}%"
        )
        return result

    except Exception as e:
        log.error(f"[{ticker}] Yahoo API 오류: {type(e).__name__}: {e}")
        return empty


# ── 메인 ──────────────────────────────────────────────────────────────────

def main():
    etfs = json.loads(ETF_LIST_PATH.read_text(encoding="utf-8"))
    DATA_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    collected_at = datetime.now().strftime("%Y-%m-%d")
    log.info(f"수집 시작: {collected_at}  대상 {len(etfs)}개 ETF")

    # Yahoo Finance 세션은 한 번만 초기화
    yf_session, yf_crumb = _yf_session()

    for etf in etfs:
        code    = etf.get("code") or etf.get("ticker")
        country = etf.get("country")
        cycle   = etf.get("dividend_cycle", "월")
        log.info(f"── {code} ({etf.get('name')}) ──")

        upsert_meta(conn, etf)

        if country == "KR":
            d = collect_via_yahoo_api(f"{code}.KS", etf["listed_date"], cycle, yf_session, yf_crumb)
        else:
            d = collect_via_yahoo_api(code, etf["listed_date"], cycle, yf_session, yf_crumb)

        # AUM 수집
        # KR ETF는 Yahoo Finance quote 페이지 미지원(404) → US만 시도
        yf_ticker = None if country == "KR" else code
        aum = _get_aum(yf_ticker, yf_session, yf_crumb) if (yf_ticker and d.get("nav_current")) else None

        real_return_1y = None
        if d.get("dist_rate_12m") is not None and d.get("nav_change_1y") is not None:
            real_return_1y = round(d["dist_rate_12m"] + d["nav_change_1y"], 2)

        conn.execute(
            """
            INSERT OR REPLACE INTO etf_weekly
                (code, collected_at,
                 nav_current, price_prev, price_change, price_change_pct, aum,
                 nav_change_1y, nav_change_since_listing,
                 return_1m, return_3m, return_6m,
                 dist_rate_12m, dist_rate_monthly, dist_rate_annualized,
                 real_return_1y)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                code, collected_at,
                d.get("nav_current"),    d.get("price_prev"),
                d.get("price_change"),   d.get("price_change_pct"),
                aum,
                d.get("nav_change_1y"), d.get("nav_change_since_listing"),
                d.get("return_1m"),     d.get("return_3m"),     d.get("return_6m"),
                d.get("dist_rate_12m"), d.get("dist_rate_monthly"), d.get("dist_rate_annualized"),
                real_return_1y,
            ),
        )
        conn.commit()

        if d.get("monthly_dists"):
            save_monthly_dists(conn, code, d["monthly_dists"])

        time.sleep(1)

    conn.close()
    log.info("수집 완료")


if __name__ == "__main__":
    main()
