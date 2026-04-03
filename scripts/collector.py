"""
ETF 데이터 수집기
실행: python3 scripts/collector.py

수집 전략:
  KR ETF:
    가격/수익률  - pykrx (기본) → yfinance {code}.KS (fallback)
    분배금       - yfinance {code}.KS
    AUM          - KRX API/ISIN (기본) → Naver API → pykrx 시가총액 (fallback)
    NAV/주       - pykrx NAV 컬럼 (기본) → KRX API → Naver API (fallback)
  US ETF:
    전체         - yfinance (가격 + 분배금 + AUM)
"""

import json
import sqlite3
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
ETF_LIST_PATH = DATA_DIR / "etf_list.json"
DB_PATH = DATA_DIR / "etf.db"

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
    "ALTER TABLE etf_weekly ADD COLUMN nav_per_share REAL",
    "ALTER TABLE etf_weekly ADD COLUMN nav_change_1m REAL",
    "ALTER TABLE etf_weekly ADD COLUMN nav_change_3m REAL",
    "ALTER TABLE etf_weekly ADD COLUMN nav_change_6m REAL",
    # 전략/과세 구분 확장
    "ALTER TABLE etf_meta ADD COLUMN underlying TEXT",
    "ALTER TABLE etf_meta ADD COLUMN tax_type TEXT",
    "ALTER TABLE etf_weekly ADD COLUMN tax_base_price REAL",
    "ALTER TABLE etf_weekly ADD COLUMN taxable_dist_amount REAL",
]


def init_db(conn):
    for sql in _MIGRATIONS:
        try:
            conn.execute(sql)
            conn.commit()
        except Exception:
            pass

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS etf_meta (
            code             TEXT PRIMARY KEY,
            name             TEXT,
            country          TEXT,
            strategy         TEXT,
            underlying       TEXT,
            tax_type         TEXT,
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
            nav_change_1m            REAL,
            nav_change_3m            REAL,
            nav_change_6m            REAL,
            dist_rate_12m            REAL,
            dist_rate_monthly        REAL,
            dist_rate_annualized     REAL,
            real_return_1y           REAL,
            nav_per_share            REAL,
            tax_base_price           REAL,
            taxable_dist_amount      REAL,
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
            (code, name, country, strategy, underlying, tax_type,
             dividend_cycle, dividend_timing, manager, listed_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            code,
            etf.get("name"),
            etf.get("country"),
            etf.get("strategy"),
            etf.get("underlying"),
            etf.get("tax_type"),
            etf.get("dividend_cycle"),
            etf.get("dividend_timing"),
            etf.get("manager"),
            etf.get("listed_date"),
        ),
    )
    conn.commit()


def save_monthly_dists(conn, code, monthly_dists):
    for ym, amount in monthly_dists.items():
        conn.execute(
            "INSERT OR REPLACE INTO etf_monthly_dist (code, year_month, amount) VALUES (?, ?, ?)",
            (code, ym, amount),
        )
    conn.commit()


# ── 공통 유틸 ─────────────────────────────────────────────────────────────

def _to_float(v):
    """쉼표·공백 제거 후 float 변환. 한국어 화폐 단위(억/조/만) 자동 처리. 실패하면 None."""
    if v in (None, "", "-", "N/A"):
        return None
    s = str(v).replace(",", "").strip()
    try:
        for suffix, mult in (
            ("조원", 1e12), ("조", 1e12),
            ("억원", 1e8),  ("억", 1e8),
            ("만원", 1e4),  ("만", 1e4),
            ("원", 1),
        ):
            if suffix in s:
                return float(s.replace(suffix, "").strip()) * mult
        return float(s.replace("%", "").strip())
    except (ValueError, TypeError):
        return None


def _empty_result():
    return {
        "nav_current": None, "price_prev": None,
        "price_change": None, "price_change_pct": None,
        "aum": None, "nav_per_share": None,
        "nav_change_1y": None, "nav_change_since_listing": None,
        "return_1m": None, "return_3m": None, "return_6m": None,
        "nav_change_1m": None, "nav_change_3m": None, "nav_change_6m": None,
        "dist_rate_12m": None, "dist_rate_monthly": None, "dist_rate_annualized": None,
        "monthly_dists": {},
        "tax_base_price": None, "taxable_dist_amount": None,
    }


# ── KRX 데이터 API (KR ETF AUM + NAV/주) ──────────────────────────────────

def _get_krx_etf_info(code, isin=None):
    """KRX 정보데이터시스템 API로 KR ETF 기준가격(NAV/주) + 순자산총액(AUM) 수집.

    주말·공휴일 대비 최근 5 영업일을 순서대로 시도.
    isin이 제공되면 ISIN을 우선 사용 (KRX API는 ISIN 기반이 더 안정적).
    반환: (nav_per_share: float|None, aum_won: float|None)
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer": "https://data.krx.co.kr/",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
    }
    url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    # ISIN이 있으면 ISIN 우선, 없으면 short code 사용
    isu_cd = isin if isin else code

    # 최근 5일 시도 (공휴일/주말 대응)
    for days_back in range(0, 6):
        date_str = (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d")
        for bld in (
            "dbms/MDC/STAT/standard/MDCSTAT04401",  # ETF 순자산가치현황
            "dbms/MDC/STAT/standard/MDCSTAT04301",  # ETF 기본정보
        ):
            try:
                payload = {
                    "bld": bld,
                    "isuCd": isu_cd,
                    "isuCd2": isu_cd,
                    "baseDd": date_str,
                    "share": "1",
                    "money": "1",
                    "csvxls_isNo": "false",
                }
                res = requests.post(url, data=payload, headers=headers, timeout=10)
                if res.status_code != 200:
                    continue
                data = res.json()
                items = data.get("output") or data.get("OutBlock_1") or []
                if not items:
                    continue

                item = items[0]
                # Actions 로그에서 실제 field 이름 확인 가능하도록 출력
                log.info(f"[{code}] KRX({bld.split('/')[-1]}, {date_str}) keys: {list(item.keys())}")

                # 기준가격(NAV/주) 후보 필드
                nav = None
                for f in ("BAS_PRC", "CLSPRC", "NAV", "NAV_PRC", "navPrice"):
                    nav = _to_float(item.get(f))
                    if nav and nav > 100:
                        log.info(f"[{code}] KRX NAV/주={nav:,.0f} ({f})")
                        break
                    nav = None

                # 순자산총액(AUM) 후보 필드
                # KRX 단위는 보통 백만원 또는 억원 — 값 크기로 판별
                aum = None
                for f in ("NETASST_TOTAMT", "NET_ASST_TOTAMT", "FUND_NETASST",
                          "MKTCAP", "TOT_NETASST", "netAssetTotAmt"):
                    raw = _to_float(item.get(f))
                    if raw and raw > 0:
                        # 백만원 단위 변환 (1억 이상이면 이미 원 단위로 처리)
                        if raw < 1e9:           # 백만원 단위로 추정
                            aum = raw * 1_000_000
                        else:                   # 이미 원 단위
                            aum = raw
                        log.info(f"[{code}] KRX AUM={aum/1e8:,.0f}억원 ({f}, raw={raw})")
                        break

                if nav is not None or aum is not None:
                    return nav, aum

            except Exception as e:
                log.warning(f"[{code}] KRX API 오류 ({date_str}): {type(e).__name__}: {e}")

    log.warning(f"[{code}] KRX API 수집 실패")
    return None, None


# ── KRX 과표기준가격 (해외주식형 ETF 매매차익 과세 기준가격) ────────────────

def _get_krx_tax_base_price(code, isin=None):
    """KRX에서 ETF 과표기준가격 수집.

    해외주식형 ETF: 매매차익 과세 시 취득·양도 과표기준가격 차액 기준 (배당소득세 15.4%)
    반환: tax_base_price (float|None)
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer": "https://data.krx.co.kr/",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
    }
    url = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    isu_cd = isin if isin else code

    for days_back in range(0, 6):
        date_str = (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d")
        for bld in (
            "dbms/MDC/STAT/standard/MDCSTAT04501",  # ETF 과표기준가격현황 (1순위)
            "dbms/MDC/STAT/standard/MDCSTAT04401",  # ETF 순자산가치현황 (fallback)
        ):
            try:
                payload = {
                    "bld": bld,
                    "isuCd": isu_cd,
                    "isuCd2": isu_cd,
                    "baseDd": date_str,
                    "csvxls_isNo": "false",
                }
                res = requests.post(url, data=payload, headers=headers, timeout=10)
                if res.status_code != 200:
                    continue
                data = res.json()
                items = data.get("output") or data.get("OutBlock_1") or []
                if not items:
                    continue

                item = items[0]
                log.info(f"[{code}] KRX 과표({bld.split('/')[-1]}, {date_str}) keys: {list(item.keys())}")

                for f in ("TAX_BAS_PRC", "TAXBASPRC", "ETX_PRC", "CLSPRC",
                          "BAS_PRC", "NAV", "NAV_PRC"):
                    val = _to_float(item.get(f))
                    if val and val > 100:
                        log.info(f"[{code}] KRX 과표기준가격={val:,.0f} ({f})")
                        return val

            except Exception as e:
                log.warning(f"[{code}] KRX 과표기준가격 API 오류 ({date_str}): {e}")

    log.warning(f"[{code}] 과표기준가격 수집 실패")
    return None


# ── 네이버 금융 API (KR ETF AUM + NAV/주 fallback) ────────────────────────

def _get_naver_etf(code):
    """네이버 금융 모바일 API로 KR ETF AUM + NAV/주 수집 (KRX API 실패 시 fallback).

    반환: (aum_won: float|None, nav_per_share: float|None)
    """
    aum, nav = None, None

    endpoints = [
        ("basic",       f"https://m.stock.naver.com/api/stock/{code}/basic"),
        ("etfAnalysis", f"https://m.stock.naver.com/api/stock/{code}/etfAnalysis"),
        ("etfSummary",  f"https://m.stock.naver.com/api/stock/{code}/etfSummary"),
        ("etfInfo",     f"https://m.stock.naver.com/api/stock/{code}/etfInfo"),
        ("summaryInfo", f"https://m.stock.naver.com/api/stock/{code}/summaryInfo"),
    ]

    for ep_name, url in endpoints:
        if aum is not None and nav is not None:
            break
        try:
            res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            log.info(f"[{code}] Naver {ep_name} → {res.status_code}")
            if res.status_code != 200:
                continue

            d = res.json()
            if isinstance(d, list):
                d = d[0] if d else {}

            # 전체 non-null 응답 로깅 (빈 dict 방지)
            non_null = {k: v for k, v in d.items() if v not in (None, "", "-", "0", 0)}
            if not non_null:
                continue
            log.info(f"[{code}] Naver {ep_name} non-null fields: {list(non_null.keys())[:20]}")

            # AUM 후보 필드 (원 단위 또는 억원 단위)
            if aum is None:
                for f in ("navTotalAsset", "totalNetAsset", "fundNetAsset",
                          "marketValue", "marketCap", "totalAsset",
                          "fundTotalAsset", "etfTotalAsset", "netAsset",
                          "fundSize", "netAssetValue", "etfNetAsset"):
                    raw = _to_float(d.get(f))
                    if raw and raw > 0:
                        # 단위 추정: 1조(1e12) 이하이면 원, 그 이상은 백만원 등
                        aum = raw if raw > 1e8 else raw * 1e8
                        if aum > 1e13:   # 비현실적으로 큰 값 → 단위 조정
                            aum = raw
                        log.info(f"[{code}] Naver AUM={aum/1e8:,.1f}억원 ({ep_name}/{f})")
                        break
                    aum = None

            # NAV/주 후보 필드
            if nav is None:
                for f in ("iNav", "nav", "navPrice", "navPerUnit", "iNavValue",
                          "estimatedNav", "netAssetValue", "etfNav", "basePrice",
                          "closingPrice", "currentPrice"):
                    raw = _to_float(d.get(f))
                    if raw and raw > 100:
                        nav = raw
                        log.info(f"[{code}] Naver NAV/주={nav:,.0f} ({ep_name}/{f})")
                        break
                    nav = None

        except Exception as e:
            log.warning(f"[{code}] Naver {ep_name} 오류: {e}")

    if aum is None:
        log.warning(f"[{code}] Naver AUM 수집 실패 (모든 endpoint 소진)")
    if nav is None:
        log.warning(f"[{code}] Naver NAV/주 수집 실패 (모든 endpoint 소진)")

    return aum, nav


# ── pykrx (KR ETF 가격/NAV 이력) ──────────────────────────────────────────

def collect_kr_via_pykrx(code, listed_date, dividend_cycle):
    """pykrx로 KR ETF 가격·NAV 이력 수집.
    신형 코드 등 미지원 시 None 반환 (yfinance fallback 트리거).
    """
    try:
        from pykrx import stock as pykrx_stock

        today = datetime.now().strftime("%Y%m%d")
        listing_dt = datetime.strptime(listed_date, "%Y-%m-%d")
        from_date = max(listing_dt, datetime.now() - timedelta(days=730)).strftime("%Y%m%d")

        df = pykrx_stock.get_etf_ohlcv_by_date(from_date, today, code)
        if df is None or df.empty:
            log.warning(f"[{code}] pykrx 데이터 없음")
            return None

        log.info(f"[{code}] pykrx 컬럼: {list(df.columns)}")

        # 종가 컬럼 탐색
        close_col = None
        for c in ("종가", "Close", "close"):
            if c in df.columns:
                close_col = c
                break
        if close_col is None:
            log.warning(f"[{code}] pykrx 종가 컬럼 없음")
            return None

        # NAV 컬럼 탐색
        nav_col = None
        for c in ("NAV", "순자산가치", "기준가격", "nav"):
            if c in df.columns:
                nav_col = c
                break

        price_now = float(df[close_col].iloc[-1])
        nav_now = None
        if nav_col:
            raw_nav = df[nav_col].iloc[-1]
            if not pd.isna(raw_nav) and float(raw_nav) > 100:
                nav_now = float(raw_nav)

        result = _empty_result()
        result["nav_current"] = price_now
        result["nav_per_share"] = nav_now

        # 전일가 / 등락
        if len(df) >= 2:
            prev = float(df[close_col].iloc[-2])
            result["price_prev"]       = prev
            result["price_change"]     = round(price_now - prev, 2)
            result["price_change_pct"] = round((price_now / prev - 1) * 100, 2) if prev else None

        # 기간별 수익률
        now = datetime.now()

        def pct_return(days_ago):
            cutoff = pd.Timestamp(now - timedelta(days=days_ago))
            sub = df[df.index >= cutoff]
            if not sub.empty:
                base = float(sub[close_col].iloc[0])
                if base:
                    return round((price_now / base - 1) * 100, 2)
            return None

        r1m = pct_return(30);  r3m = pct_return(91)
        r6m = pct_return(182); r1y = pct_return(365)

        result["return_1m"] = result["nav_change_1m"] = r1m
        result["return_3m"] = result["nav_change_3m"] = r3m
        result["return_6m"] = result["nav_change_6m"] = r6m
        result["nav_change_1y"] = r1y

        p_first = float(df[close_col].iloc[0])
        result["nav_change_since_listing"] = round((price_now / p_first - 1) * 100, 2) if p_first else None

        log.info(
            f"[{code}] pykrx 현재가={price_now:,.0f}  전일대비={result['price_change_pct']}%  "
            f"1M={r1m}%  1Y={r1y}%  NAV/주={nav_now}"
        )
        return result

    except Exception as e:
        log.warning(f"[{code}] pykrx 오류: {type(e).__name__}: {e}")
        return None


# ── yfinance (KR/US ETF 가격 + 분배금) ────────────────────────────────────

def collect_via_yfinance(ticker, listed_date, dividend_cycle):
    """yfinance로 가격 이력 + 분배금 수집.
    KR ETF: {code}.KS  |  US ETF: ticker 그대로
    """
    try:
        import yfinance as yf

        yf_ticker = yf.Ticker(ticker)
        hist = yf_ticker.history(start=listed_date, auto_adjust=True)
        if hist.empty:
            log.warning(f"[{ticker}] yfinance 가격 데이터 없음")
            return _empty_result()

        price_now = float(hist["Close"].iloc[-1])
        result = _empty_result()
        result["nav_current"] = price_now

        # 전일가 / 등락
        if len(hist) >= 2:
            prev = float(hist["Close"].iloc[-2])
            result["price_prev"]       = prev
            result["price_change"]     = round(price_now - prev, 4)
            result["price_change_pct"] = round((price_now / prev - 1) * 100, 2) if prev else None

        # 기간별 수익률 (tz-aware 처리)
        now = datetime.now()
        idx_tz = hist.index.tz

        def pct_return(days_ago):
            cutoff = now - timedelta(days=days_ago)
            cutoff_ts = pd.Timestamp(cutoff)
            if idx_tz:
                cutoff_ts = cutoff_ts.tz_localize(idx_tz)
            sub = hist[hist.index >= cutoff_ts]
            if not sub.empty:
                base = float(sub["Close"].iloc[0])
                if base:
                    return round((price_now / base - 1) * 100, 2)
            return None

        r1m = pct_return(30);  r3m = pct_return(91)
        r6m = pct_return(182); r1y = pct_return(365)

        result["return_1m"] = result["nav_change_1m"] = r1m
        result["return_3m"] = result["nav_change_3m"] = r3m
        result["return_6m"] = result["nav_change_6m"] = r6m
        result["nav_change_1y"] = r1y

        p_first = float(hist["Close"].iloc[0])
        result["nav_change_since_listing"] = round((price_now / p_first - 1) * 100, 2) if p_first else None

        # 분배금
        try:
            divs = yf_ticker.dividends
            if not divs.empty:
                # tz 제거
                try:
                    divs.index = divs.index.tz_convert(None)
                except TypeError:
                    try:
                        divs.index = divs.index.tz_localize(None)
                    except TypeError:
                        pass

                one_year_ago = now - timedelta(days=365)
                divs_12m = divs[divs.index >= one_year_ago]

                # 월별 집계 (전체 이력)
                monthly = divs.resample("ME").sum()
                monthly = monthly[monthly > 0]
                result["monthly_dists"] = {
                    ts.strftime("%Y-%m"): round(float(v), 4)
                    for ts, v in monthly.items()
                }

                if not divs_12m.empty:
                    dist_12m  = float(divs_12m.sum())
                    last_dist = float(divs_12m.iloc[-1])
                    freq = CYCLE_FREQ.get(dividend_cycle, 12)

                    result["dist_rate_12m"]        = round(dist_12m  / price_now * 100, 2)
                    result["dist_rate_monthly"]    = round(last_dist / price_now * 100, 2) if last_dist else None
                    result["dist_rate_annualized"] = round(last_dist * freq / price_now * 100, 2) if last_dist else None
        except Exception as e:
            log.warning(f"[{ticker}] yfinance 분배금 오류: {e}")

        # AUM (US ETF 우선, KR ETF는 보조)
        try:
            info = yf_ticker.info
            aum_val = _to_float(info.get("totalAssets"))
            if aum_val and aum_val > 0:
                result["aum"] = aum_val
                log.info(f"[{ticker}] yfinance AUM={aum_val:,.0f}")
            else:
                # fast_info 보조: shares * close
                try:
                    fi = yf_ticker.fast_info
                    shares = getattr(fi, "shares", None)
                    if shares and price_now:
                        result["aum"] = float(shares) * price_now
                        log.info(f"[{ticker}] yfinance AUM(shares×price)={result['aum']:,.0f}")
                except Exception:
                    pass
        except Exception as e:
            log.warning(f"[{ticker}] yfinance info 오류: {e}")

        log.info(
            f"[{ticker}] yfinance 현재가={price_now}  전일대비={result['price_change_pct']}%  "
            f"1M={r1m}%  1Y={r1y}%  분배율12M={result['dist_rate_12m']}%"
        )
        return result

    except Exception as e:
        log.error(f"[{ticker}] yfinance 오류: {type(e).__name__}: {e}")
        return _empty_result()


# ── 메인 ──────────────────────────────────────────────────────────────────

def main():
    etfs = json.loads(ETF_LIST_PATH.read_text(encoding="utf-8"))
    DATA_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    collected_at = datetime.now().strftime("%Y-%m-%d")
    log.info(f"수집 시작: {collected_at}  대상 {len(etfs)}개 ETF")

    success_count = 0

    for etf in etfs:
        code    = etf.get("code") or etf.get("ticker")
        isin    = etf.get("isin", "")
        country = etf.get("country")
        cycle   = etf.get("dividend_cycle", "월")
        log.info(f"══ {code} ({etf.get('name')}) ══")

        upsert_meta(conn, etf)

        aum          = None
        nav_per_share = None

        if country == "KR":
            # ── 1) 가격·수익률: pykrx 우선 ──────────────────────────────
            d = collect_kr_via_pykrx(code, etf["listed_date"], cycle)

            if d is not None:
                # pykrx 성공 → 분배금만 yfinance로 보완
                dist_data = collect_via_yfinance(f"{code}.KS", etf["listed_date"], cycle)
                if not d.get("dist_rate_12m") and dist_data.get("dist_rate_12m"):
                    d["dist_rate_12m"]        = dist_data["dist_rate_12m"]
                    d["dist_rate_monthly"]    = dist_data["dist_rate_monthly"]
                    d["dist_rate_annualized"] = dist_data["dist_rate_annualized"]
                    d["monthly_dists"]        = dist_data["monthly_dists"]
            else:
                # pykrx 실패 → yfinance 전체 fallback (신형 코드 등)
                log.info(f"[{code}] pykrx 실패 → yfinance fallback ({code}.KS)")
                d = collect_via_yfinance(f"{code}.KS", etf["listed_date"], cycle)

            # ── 2) AUM: KRX API(ISIN) → Naver API → pykrx 시가총액 → yfinance KS ─
            krx_nav, krx_aum = _get_krx_etf_info(code, isin=isin)
            if krx_aum:
                aum = krx_aum
                log.info(f"[{code}] AUM=KRX {aum/1e8:,.0f}억원")
            else:
                naver_aum, naver_nav = _get_naver_etf(code)
                if naver_aum:
                    aum = naver_aum
                    log.info(f"[{code}] AUM=Naver {aum/1e8:,.0f}억원")
                else:
                    # pykrx 시가총액 fallback (ETF 시총 = AUM에 근사)
                    try:
                        from pykrx import stock as pykrx_stock
                        today_str = datetime.now().strftime("%Y%m%d")
                        mc = pykrx_stock.get_market_cap_by_ticker(today_str, market="ETF")
                        if mc is not None and code in mc.index:
                            pykrx_aum = float(mc.loc[code, "시가총액"])
                            if pykrx_aum > 0:
                                aum = pykrx_aum
                                log.info(f"[{code}] AUM=pykrx 시가총액 {aum/1e8:,.0f}억원")
                    except Exception as e:
                        log.warning(f"[{code}] pykrx 시가총액 오류: {e}")

                    if not aum:
                        # 최후 fallback: yfinance KS (USD → 환율 미적용, 대략적 값)
                        ks_aum = d.get("aum")
                        if ks_aum:
                            aum = ks_aum
                            log.warning(f"[{code}] AUM=yfinance(KS) fallback (단위 불확실)")

            # ── 3) NAV/주: pykrx NAV → KRX API(ISIN) → Naver API 순 ────
            if d.get("nav_per_share"):
                nav_per_share = d["nav_per_share"]
                log.info(f"[{code}] NAV/주=pykrx {nav_per_share:,.0f}원")
            elif krx_nav:
                nav_per_share = krx_nav
                log.info(f"[{code}] NAV/주=KRX {nav_per_share:,.0f}원")
            else:
                _, naver_nav = _get_naver_etf(code)
                if naver_nav:
                    nav_per_share = naver_nav
                    log.info(f"[{code}] NAV/주=Naver {nav_per_share:,.0f}원")

            # ── 4) 과표기준가격: 해외주식형 ETF만 수집 ──────────────────
            if etf.get("tax_type") == "해외주식형":
                tax_bp = _get_krx_tax_base_price(code, isin=isin)
                if tax_bp:
                    d["tax_base_price"] = tax_bp

        else:
            # ── US ETF: yfinance 일괄 수집 ───────────────────────────────
            d = collect_via_yfinance(code, etf["listed_date"], cycle)
            aum          = d.get("aum")
            nav_per_share = None  # US ETF: 가격 ≈ NAV

        # 실질수익률 = 분배율 12M + NAV 변화율 1Y
        real_return_1y = None
        if d.get("dist_rate_12m") is not None and d.get("nav_change_1y") is not None:
            real_return_1y = round(d["dist_rate_12m"] + d["nav_change_1y"], 2)

        # 수집 결과 요약
        if d.get("nav_current") is not None:
            success_count += 1
            log.info(
                f"[{code}] ✓ 현재가={d['nav_current']}  AUM={'있음' if aum else '없음'}  "
                f"NAV/주={'있음' if nav_per_share else '없음'}  "
                f"분배율={d.get('dist_rate_12m')}%  1Y={d.get('nav_change_1y')}%"
            )
        else:
            log.error(f"[{code}] ✗ 현재가 수집 실패")

        conn.execute(
            """
            INSERT OR REPLACE INTO etf_weekly
                (code, collected_at,
                 nav_current, price_prev, price_change, price_change_pct, aum,
                 nav_change_1y, nav_change_since_listing,
                 return_1m, return_3m, return_6m,
                 nav_change_1m, nav_change_3m, nav_change_6m,
                 dist_rate_12m, dist_rate_monthly, dist_rate_annualized,
                 real_return_1y, nav_per_share,
                 tax_base_price, taxable_dist_amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                code, collected_at,
                d.get("nav_current"),    d.get("price_prev"),
                d.get("price_change"),   d.get("price_change_pct"),
                aum,
                d.get("nav_change_1y"), d.get("nav_change_since_listing"),
                d.get("return_1m"),     d.get("return_3m"),     d.get("return_6m"),
                d.get("nav_change_1m"), d.get("nav_change_3m"), d.get("nav_change_6m"),
                d.get("dist_rate_12m"), d.get("dist_rate_monthly"), d.get("dist_rate_annualized"),
                real_return_1y, nav_per_share,
                d.get("tax_base_price"), d.get("taxable_dist_amount"),
            ),
        )
        conn.commit()

        if d.get("monthly_dists"):
            save_monthly_dists(conn, code, d["monthly_dists"])

        time.sleep(1)

    conn.close()
    log.info(f"수집 완료: {success_count}/{len(etfs)}개 성공")
    if success_count < len(etfs):
        log.error(f"수집 실패: {len(etfs) - success_count}개 ETF 현재가 없음")


if __name__ == "__main__":
    main()
