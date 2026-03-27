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


# ── KR ETF: pykrx NAV (GitHub Actions에서는 KRX IP 차단으로 실패할 수 있음) ──

def collect_kr_nav(code, listed_date):
    """pykrx로 NAV 이력 수집 → (nav_current, change_1y%, change_since%)"""
    try:
        from pykrx import stock

        listing_yyyymmdd = listed_date.replace("-", "")
        df = stock.get_etf_ohlcv_by_date(listing_yyyymmdd, TODAY, code)

        if df is None or df.empty:
            log.warning(f"[{code}] pykrx NAV 데이터 없음")
            return None, None, None

        nav_col = "NAV" if "NAV" in df.columns else df.columns[0]
        nav_series = df[nav_col].dropna()
        if nav_series.empty:
            return None, None, None

        nav_current = float(nav_series.iloc[-1])

        one_year_ago_dt = datetime.now() - timedelta(days=365)
        df_1y = df[df.index >= one_year_ago_dt]
        change_1y = None
        if not df_1y.empty:
            s = df_1y[nav_col].dropna()
            if not s.empty and float(s.iloc[0]):
                change_1y = round((nav_current / float(s.iloc[0]) - 1) * 100, 2)

        nav_first = float(nav_series.iloc[0])
        change_since = round((nav_current / nav_first - 1) * 100, 2) if nav_first else None

        log.info(f"[{code}] pykrx NAV={nav_current:,.0f}  1Y={change_1y}%")
        return nav_current, change_1y, change_since

    except Exception as e:
        log.error(f"[{code}] pykrx NAV 오류: {e}")
        return None, None, None


# ── KR ETF: KRX OTP 분배금 ────────────────────────────────────────────────

def collect_kr_distributions_krx(code):
    """KRX OTP 방식으로 최근 12M 분배금 수집 → [금액, ...] (최신순)"""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://data.krx.co.kr/",
        }
        otp_url = "https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
        otp_payload = {
            "bld": "dbms/MDC/STAT/standard/MDCSTAT04602",
            "isuCd": code,
            "strtDd": ONE_YEAR_AGO,
            "endDd": TODAY,
            "name": "fileDown",
            "url": "dbms/MDC/STAT/standard/MDCSTAT04602",
        }
        otp_res = requests.post(otp_url, data=otp_payload, headers=headers, timeout=15)
        otp = otp_res.text.strip()
        if not otp or len(otp) < 10:
            return []

        data_url = "https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
        data_res = requests.post(
            data_url,
            data={"code": otp},
            headers={**headers, "Referer": otp_url},
            timeout=15,
        )
        if len(data_res.content) < 50:
            return []

        import pandas as pd
        from io import StringIO

        try:
            text = data_res.content.decode("euc-kr")
        except UnicodeDecodeError:
            text = data_res.content.decode("utf-8", errors="replace")

        df = pd.read_csv(StringIO(text))
        amount_col = None
        for col in df.columns:
            if any(kw in col for kw in ["분배금", "주당분배", "지급금액", "분배"]):
                amount_col = col
                break
        if amount_col is None and len(df.columns) >= 2:
            amount_col = df.columns[1]

        if amount_col is None:
            return []

        amounts = []
        for val in df[amount_col]:
            try:
                amounts.append(float(str(val).replace(",", "")))
            except (ValueError, TypeError):
                pass
        return amounts

    except Exception as e:
        log.error(f"[{code}] KRX 분배금 오류: {e}")
        return []


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
    """Yahoo Finance quoteSummary로 AUM(순자산총액) 수집"""
    try:
        params = {"modules": "defaultKeyStatistics,summaryDetail"}
        if crumb:
            params["crumb"] = crumb
        url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
        res = session.get(url, params=params, timeout=10)
        log.info(f"[{ticker}] AUM API {res.status_code}")
        data = res.json()
        results = ((data.get("quoteSummary") or {}).get("result") or [])
        if not results:
            return None
        r = results[0]
        # defaultKeyStatistics.totalAssets → summaryDetail.totalAssets 순으로 시도
        for module in ("defaultKeyStatistics", "summaryDetail"):
            raw = ((r.get(module) or {}).get("totalAssets") or {}).get("raw")
            if raw:
                log.info(f"[{ticker}] AUM={raw:,.0f} (from {module})")
                return float(raw)
    except Exception as e:
        log.warning(f"[{ticker}] AUM 수집 오류: {e}")
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
            # pykrx 시도 (KRX IP 허용 환경에서만 성공)
            nav_kr, chg1y_kr, chgsince_kr = collect_kr_nav(code, etf["listed_date"])
            dists_kr = collect_kr_distributions_krx(code) if nav_kr else []

            if nav_kr is not None:
                # pykrx 성공: NAV 데이터는 pykrx, 나머지 기간수익률은 Yahoo에서 보완
                d = collect_via_yahoo_api(f"{code}.KS", etf["listed_date"], cycle, yf_session, yf_crumb)
                d["nav_current"]              = nav_kr
                d["nav_change_1y"]            = chg1y_kr
                d["nav_change_since_listing"] = chgsince_kr
                if dists_kr:
                    d12, dann = (sum(dists_kr) / nav_kr * 100, dists_kr[0] * CYCLE_FREQ.get(cycle, 12) / nav_kr * 100)
                    d["dist_rate_12m"]        = round(d12, 2)
                    d["dist_rate_annualized"] = round(dann, 2)
                    d["dist_rate_monthly"]    = round(dists_kr[0] / nav_kr * 100, 2)
            else:
                log.info(f"[{code}] pykrx 실패 → Yahoo Finance {code}.KS 폴백")
                d = collect_via_yahoo_api(f"{code}.KS", etf["listed_date"], cycle, yf_session, yf_crumb)
        else:
            d = collect_via_yahoo_api(code, etf["listed_date"], cycle, yf_session, yf_crumb)

        # AUM 수집
        yf_ticker = f"{code}.KS" if country == "KR" else code
        aum = _get_aum(yf_ticker, yf_session, yf_crumb) if d.get("nav_current") else None

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
