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


# ── DB 초기화 ──────────────────────────────────────────────────────────────

def init_db(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS etf_meta (
            code           TEXT PRIMARY KEY,
            name           TEXT,
            country        TEXT,
            strategy       TEXT,
            dividend_cycle TEXT,
            manager        TEXT,
            listed_date    TEXT
        );
        CREATE TABLE IF NOT EXISTS etf_weekly (
            id                       INTEGER PRIMARY KEY AUTOINCREMENT,
            code                     TEXT NOT NULL,
            collected_at             TEXT NOT NULL,
            nav_current              REAL,
            nav_change_1y            REAL,
            nav_change_since_listing REAL,
            dist_rate_12m            REAL,
            dist_rate_annualized     REAL,
            real_return_1y           REAL,
            UNIQUE(code, collected_at)
        );
    """)
    conn.commit()


def upsert_meta(conn, etf):
    code = etf.get("code") or etf.get("ticker")
    conn.execute(
        """
        INSERT OR REPLACE INTO etf_meta
            (code, name, country, strategy, dividend_cycle, manager, listed_date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            code,
            etf.get("name"),
            etf.get("country"),
            etf.get("strategy"),
            etf.get("dividend_cycle"),
            etf.get("manager"),
            etf.get("listed_date"),
        ),
    )
    conn.commit()


# ── KR ETF: pykrx NAV ──────────────────────────────────────────────────────

def collect_kr_nav(code, listed_date):
    """pykrx로 NAV 이력 수집 → (nav_current, change_1y%, change_since_listing%)"""
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

        # 1Y 변화율
        one_year_ago_dt = datetime.now() - timedelta(days=365)
        df_1y = df[df.index >= one_year_ago_dt]
        change_1y = None
        if not df_1y.empty:
            nav_1y_series = df_1y[nav_col].dropna()
            if not nav_1y_series.empty and float(nav_1y_series.iloc[0]):
                nav_1y_ago = float(nav_1y_series.iloc[0])
                change_1y = round((nav_current / nav_1y_ago - 1) * 100, 2)

        # 상장이후 변화율
        nav_first = float(nav_series.iloc[0])
        change_since = round((nav_current / nav_first - 1) * 100, 2) if nav_first else None

        log.info(f"[{code}] NAV={nav_current:,.0f}  1Y={change_1y}%  상장이후={change_since}%")
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

        # OTP 발급
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
            log.warning(f"[{code}] KRX OTP 발급 실패: '{otp[:50]}'")
            return []

        # CSV 다운로드
        data_url = "https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
        data_res = requests.post(
            data_url,
            data={"code": otp},
            headers={**headers, "Referer": otp_url},
            timeout=15,
        )

        if len(data_res.content) < 50:
            log.warning(f"[{code}] KRX 분배금 응답 없음 ({len(data_res.content)} bytes)")
            return []

        # 파싱 (KRX CSV는 EUC-KR)
        import pandas as pd
        from io import StringIO

        try:
            text = data_res.content.decode("euc-kr")
        except UnicodeDecodeError:
            text = data_res.content.decode("utf-8", errors="replace")

        df = pd.read_csv(StringIO(text))
        log.info(f"[{code}] KRX 분배금 컬럼: {list(df.columns)}")

        # 분배금 컬럼 탐색
        amount_col = None
        for col in df.columns:
            if any(kw in col for kw in ["분배금", "주당분배", "지급금액", "분배"]):
                amount_col = col
                break
        if amount_col is None and len(df.columns) >= 2:
            amount_col = df.columns[1]
            log.warning(f"[{code}] 분배금 컬럼 추정: '{amount_col}'")

        if amount_col is None:
            log.error(f"[{code}] 분배금 컬럼 찾기 실패: {list(df.columns)}")
            return []

        amounts = []
        for val in df[amount_col]:
            try:
                amounts.append(float(str(val).replace(",", "")))
            except (ValueError, TypeError):
                pass

        log.info(f"[{code}] 분배금 {len(amounts)}건: {amounts[:5]}")
        return amounts

    except Exception as e:
        log.error(f"[{code}] KRX 분배금 오류: {e}")
        return []


# ── 분배율 계산 ────────────────────────────────────────────────────────────

CYCLE_FREQ = {"일": 252, "주": 52, "월": 12, "분기": 4, "반기": 2, "연": 1}


def calc_dist_rates(distributions, nav_current, dividend_cycle):
    """(분배율12M%, 분배율연환산%)"""
    if not distributions or not nav_current:
        return None, None

    dist_12m_sum = sum(distributions)
    dist_rate_12m = round(dist_12m_sum / nav_current * 100, 2)

    freq = CYCLE_FREQ.get(dividend_cycle, 12)
    last_dist = distributions[0]
    dist_rate_ann = round(last_dist * freq / nav_current * 100, 2) if last_dist else None

    return dist_rate_12m, dist_rate_ann


# ── Yahoo Finance 직접 API ─────────────────────────────────────────────────

def _yf_session():
    """Yahoo Finance 쿠키 + crumb 세션 초기화"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://finance.yahoo.com",
    })
    # 쿠키 획득 (인증 필요 시)
    try:
        session.get("https://finance.yahoo.com/", timeout=10)
    except Exception:
        pass
    # crumb 획득
    crumb = None
    try:
        r = session.get("https://query1.finance.yahoo.com/v1/test/getcrumb", timeout=10)
        if r.status_code == 200 and r.text:
            crumb = r.text.strip()
    except Exception:
        pass
    return session, crumb


def collect_via_yahoo_api(ticker, listed_date, dividend_cycle="월"):
    """Yahoo Finance Chart API로 가격 이력 + 분배금 수집
    → (nav_current, change_1y%, change_since%, dist_rate_12m%, dist_rate_ann%)
    """
    try:
        import pandas as pd

        listing_dt = datetime.strptime(listed_date, "%Y-%m-%d")
        one_year_ago_dt = datetime.now() - timedelta(days=365)
        start_ts = int(listing_dt.timestamp())
        end_ts   = int(datetime.now().timestamp())

        session, crumb = _yf_session()

        params = {
            "period1": start_ts,
            "period2": end_ts,
            "interval": "1wk",
            "events": "dividends",
            "includePrePost": "false",
        }
        if crumb:
            params["crumb"] = crumb

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        res = session.get(url, params=params, timeout=15)
        log.info(f"[{ticker}] Yahoo API 응답 {res.status_code} ({len(res.content)} bytes)")

        data = res.json()
        chart_result = data.get("chart", {})
        if chart_result.get("error"):
            log.error(f"[{ticker}] Yahoo API 에러: {chart_result['error']}")
            return None, None, None, None, None

        results = chart_result.get("result") or []
        if not results:
            log.warning(f"[{ticker}] Yahoo API result 없음")
            return None, None, None, None, None

        chart    = results[0]
        closes   = chart["indicators"]["quote"][0].get("close") or []
        tss      = chart.get("timestamp") or []

        valid = [(t, c) for t, c in zip(tss, closes) if c is not None]
        if not valid:
            log.warning(f"[{ticker}] 유효한 가격 데이터 없음")
            return None, None, None, None, None

        prices = pd.Series(
            [v[1] for v in valid],
            index=pd.to_datetime([v[0] for v in valid], unit="s"),
        )

        price_now  = float(prices.iloc[-1])
        nav_current = price_now

        # 1Y 변화율
        p1y = prices[prices.index >= one_year_ago_dt]
        change_1y = None
        if not p1y.empty and float(p1y.iloc[0]):
            change_1y = round((price_now / float(p1y.iloc[0]) - 1) * 100, 2)

        # 상장이후 변화율
        p_first = float(prices.iloc[0])
        change_since = round((price_now / p_first - 1) * 100, 2) if p_first else None

        # 분배금
        dist_rate_12m = dist_rate_ann = None
        raw_divs = (chart.get("events") or {}).get("dividends") or {}
        if raw_divs:
            divs = pd.Series({
                pd.Timestamp.fromtimestamp(int(k)): float(v["amount"])
                for k, v in raw_divs.items()
            }).sort_index()
            divs_12m = divs[divs.index >= one_year_ago_dt]
            if not divs_12m.empty:
                dist_rate_12m = round(float(divs_12m.sum()) / nav_current * 100, 2)
                freq = CYCLE_FREQ.get(dividend_cycle, 12)
                dist_rate_ann = round(float(divs_12m.iloc[-1]) * freq / nav_current * 100, 2)

        log.info(
            f"[{ticker}] NAV={nav_current}  1Y={change_1y}%  "
            f"분배율12M={dist_rate_12m}%  배당건수={len(raw_divs)}"
        )
        return nav_current, change_1y, change_since, dist_rate_12m, dist_rate_ann

    except Exception as e:
        log.error(f"[{ticker}] Yahoo API 오류: {type(e).__name__}: {e}")
        return None, None, None, None, None


# ── 메인 ──────────────────────────────────────────────────────────────────

def main():
    etfs = json.loads(ETF_LIST_PATH.read_text(encoding="utf-8"))
    DATA_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    collected_at = datetime.now().strftime("%Y-%m-%d")
    log.info(f"수집 시작: {collected_at}  대상 {len(etfs)}개 ETF")

    for etf in etfs:
        code = etf.get("code") or etf.get("ticker")
        country = etf.get("country")
        log.info(f"── {code} ({etf.get('name')}) ──")

        upsert_meta(conn, etf)

        if country == "KR":
            nav_current, change_1y, change_since = collect_kr_nav(code, etf["listed_date"])
            distributions = collect_kr_distributions_krx(code)
            dist_rate_12m, dist_rate_ann = calc_dist_rates(
                distributions, nav_current, etf.get("dividend_cycle", "월")
            )
            # pykrx/KRX 실패 시 Yahoo Finance .KS 폴백 (숫자 6자리 코드만)
            if nav_current is None and code.isdigit():
                log.info(f"[{code}] pykrx 실패 → Yahoo Finance {code}.KS 폴백")
                nav_current, change_1y, change_since, dist_rate_12m, dist_rate_ann = \
                    collect_via_yahoo_api(f"{code}.KS", etf["listed_date"], etf.get("dividend_cycle", "월"))
        else:
            nav_current, change_1y, change_since, dist_rate_12m, dist_rate_ann = \
                collect_via_yahoo_api(code, etf["listed_date"], etf.get("dividend_cycle", "월"))

        real_return_1y = None
        if dist_rate_12m is not None and change_1y is not None:
            real_return_1y = round(dist_rate_12m + change_1y, 2)

        conn.execute(
            """
            INSERT OR REPLACE INTO etf_weekly
                (code, collected_at, nav_current, nav_change_1y, nav_change_since_listing,
                 dist_rate_12m, dist_rate_annualized, real_return_1y)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                code, collected_at,
                nav_current, change_1y, change_since,
                dist_rate_12m, dist_rate_ann, real_return_1y,
            ),
        )
        conn.commit()
        time.sleep(1)

    conn.close()
    log.info("수집 완료")


if __name__ == "__main__":
    main()
