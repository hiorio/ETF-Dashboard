"""
ETF 데이터 수집기
실행: python3 scripts/collector.py

수집 전략:
  KR ETF: pykrx (기본) → yfinance {code}.KS (fallback)
           분배금은 yfinance로 별도 수집
           AUM/NAV per share는 네이버 금융 API
  US ETF: yfinance (가격 + 분배금 + AUM 일괄)
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
    for ym, amount in monthly_dists.items():
        conn.execute(
            "INSERT OR REPLACE INTO etf_monthly_dist (code, year_month, amount) VALUES (?, ?, ?)",
            (code, ym, amount),
        )
    conn.commit()


# ── 결과 빈 dict ───────────────────────────────────────────────────────────

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
    }


# ── pykrx (KR ETF 가격/NAV) ────────────────────────────────────────────────

def collect_kr_via_pykrx(code, listed_date, dividend_cycle):
    """pykrx로 KR ETF 가격·NAV 이력 수집. 지원하지 않는 코드는 None 반환."""
    try:
        from pykrx import stock as pykrx_stock

        today = datetime.now().strftime("%Y%m%d")
        listing_dt = datetime.strptime(listed_date, "%Y-%m-%d")
        from_date = max(listing_dt, datetime.now() - timedelta(days=730)).strftime("%Y%m%d")

        df = pykrx_stock.get_etf_ohlcv_by_date(from_date, today, code)
        if df is None or df.empty:
            log.warning(f"[{code}] pykrx 데이터 없음 (신형 코드 미지원 가능)")
            return None

        # 컬럼명 호환 처리
        close_col = "종가" if "종가" in df.columns else "Close"
        nav_col   = "NAV"  if "NAV"  in df.columns else None

        if close_col not in df.columns:
            log.warning(f"[{code}] pykrx 종가 컬럼 없음: {list(df.columns)}")
            return None

        price_now = float(df[close_col].iloc[-1])
        nav_now = None
        if nav_col and not pd.isna(df[nav_col].iloc[-1]):
            nav_now = float(df[nav_col].iloc[-1])

        result = _empty_result()
        result["nav_current"]  = price_now
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
            # pykrx index는 tz-naive
            sub = df[df.index >= cutoff]
            if not sub.empty and float(sub[close_col].iloc[0]):
                return round((price_now / float(sub[close_col].iloc[0]) - 1) * 100, 2)
            return None

        result["return_1m"]   = pct_return(30)
        result["return_3m"]   = pct_return(91)
        result["return_6m"]   = pct_return(182)
        result["nav_change_1y"] = pct_return(365)
        result["nav_change_1m"] = result["return_1m"]
        result["nav_change_3m"] = result["return_3m"]
        result["nav_change_6m"] = result["return_6m"]

        p_first = float(df[close_col].iloc[0])
        result["nav_change_since_listing"] = round((price_now / p_first - 1) * 100, 2) if p_first else None

        log.info(
            f"[{code}] pykrx 현재가={price_now:,.0f}  전일대비={result['price_change_pct']}%  "
            f"1M={result['return_1m']}%  1Y={result['nav_change_1y']}%  NAV={nav_now}"
        )
        return result

    except Exception as e:
        log.warning(f"[{code}] pykrx 오류: {type(e).__name__}: {e}")
        return None


# ── yfinance (KR·US ETF 가격 + 분배금) ────────────────────────────────────

def collect_via_yfinance(ticker, listed_date, dividend_cycle):
    """yfinance로 가격 이력 + 분배금 수집. KR ETF는 {code}.KS 형식으로 전달."""
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

        # 기간별 수익률 (tz 처리 포함)
        now = datetime.now()
        idx_tz = hist.index.tz

        def pct_return(days_ago):
            cutoff = now - timedelta(days=days_ago)
            cutoff_ts = pd.Timestamp(cutoff)
            if idx_tz:
                cutoff_ts = cutoff_ts.tz_localize(idx_tz)
            sub = hist[hist.index >= cutoff_ts]
            if not sub.empty and float(sub["Close"].iloc[0]):
                return round((price_now / float(sub["Close"].iloc[0]) - 1) * 100, 2)
            return None

        result["return_1m"]   = pct_return(30)
        result["return_3m"]   = pct_return(91)
        result["return_6m"]   = pct_return(182)
        result["nav_change_1y"] = pct_return(365)
        result["nav_change_1m"] = result["return_1m"]
        result["nav_change_3m"] = result["return_3m"]
        result["nav_change_6m"] = result["return_6m"]

        p_first = float(hist["Close"].iloc[0])
        result["nav_change_since_listing"] = round((price_now / p_first - 1) * 100, 2) if p_first else None

        # 분배금
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
                dist_12m = float(divs_12m.sum())
                last_dist = float(divs_12m.iloc[-1])
                freq = CYCLE_FREQ.get(dividend_cycle, 12)

                result["dist_rate_12m"]        = round(dist_12m / price_now * 100, 2)
                result["dist_rate_monthly"]    = round(last_dist / price_now * 100, 2) if last_dist else None
                result["dist_rate_annualized"] = round(last_dist * freq / price_now * 100, 2) if last_dist else None

        # AUM (US ETF는 yfinance info에서 직접 조회)
        try:
            info = yf_ticker.info
            aum_val = info.get("totalAssets")
            if aum_val and float(aum_val) > 0:
                result["aum"] = float(aum_val)
        except Exception as e:
            log.warning(f"[{ticker}] yfinance info 오류: {e}")

        log.info(
            f"[{ticker}] yfinance 현재가={price_now}  전일대비={result['price_change_pct']}%  "
            f"1M={result['return_1m']}%  1Y={result['nav_change_1y']}%  "
            f"분배율12M={result['dist_rate_12m']}%"
        )
        return result

    except Exception as e:
        log.error(f"[{ticker}] yfinance 오류: {type(e).__name__}: {e}")
        return _empty_result()


# ── 네이버 금융 API (KR ETF AUM + 순자산가치) ──────────────────────────────

def _get_naver_etf(code):
    """네이버 금융 모바일 API로 KR ETF 시가총액(AUM) + 순자산가치/주 수집.
    여러 endpoint/필드를 순서대로 시도하며, 찾은 값 반환."""

    def to_float(v):
        try:
            return float(str(v).replace(",", "").strip()) if v not in (None, "", "-", "0") else None
        except (ValueError, TypeError):
            return None

    aum, nav = None, None

    # 1. /basic endpoint
    try:
        url = f"https://m.stock.naver.com/api/stock/{code}/basic"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        log.info(f"[{code}] Naver basic API {res.status_code}")
        if res.status_code == 200:
            data = res.json()
            for f in ("marketValue", "marketCap", "totalAsset", "fundTotalAsset",
                      "netAsset", "navTotalAsset", "etfTotalAsset"):
                aum = to_float(data.get(f))
                if aum and aum > 1000:
                    log.info(f"[{code}] AUM={aum:,.0f} (basic/{f})")
                    break
                aum = None
            for f in ("iNav", "nav", "navPrice", "netAssetValue",
                      "iNavValue", "estimatedNav", "navPerUnit"):
                nav = to_float(data.get(f))
                if nav and nav > 100:
                    log.info(f"[{code}] NAV/주={nav:,.2f} (basic/{f})")
                    break
                nav = None
    except Exception as e:
        log.warning(f"[{code}] Naver basic API 오류: {e}")

    # 2. ETF 전용 endpoints (basic에서 못 찾은 경우)
    if aum is None or nav is None:
        for ep in ("etfAnalysis", "etfSummary", "etfInfo"):
            try:
                url2 = f"https://m.stock.naver.com/api/stock/{code}/{ep}"
                res2 = requests.get(url2, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
                if res2.status_code != 200:
                    continue
                d2 = res2.json()
                if isinstance(d2, list):
                    d2 = d2[0] if d2 else {}

                if aum is None:
                    for f in ("navTotalAsset", "totalNetAsset", "fundNetAsset",
                              "marketValue", "totalAsset", "etfTotalAsset"):
                        aum = to_float(d2.get(f))
                        if aum and aum > 1000:
                            log.info(f"[{code}] AUM={aum:,.0f} ({ep}/{f})")
                            break
                        aum = None

                if nav is None:
                    for f in ("iNav", "nav", "navPrice", "navPerUnit",
                              "iNavValue", "estimatedNav", "netAssetValue"):
                        nav = to_float(d2.get(f))
                        if nav and nav > 100:
                            log.info(f"[{code}] NAV/주={nav:,.2f} ({ep}/{f})")
                            break
                        nav = None

                if aum is not None and nav is not None:
                    break
            except Exception as e:
                log.warning(f"[{code}] Naver {ep} API 오류: {e}")

    if aum is None:
        log.warning(f"[{code}] Naver AUM 수집 실패")
    if nav is None:
        log.warning(f"[{code}] Naver NAV/주 수집 실패")

    return aum, nav


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
        country = etf.get("country")
        cycle   = etf.get("dividend_cycle", "월")
        log.info(f"── {code} ({etf.get('name')}) ──")

        upsert_meta(conn, etf)

        aum = None
        nav_per_share = None

        if country == "KR":
            # 1차: pykrx (KRX 공식 데이터)
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
                # 2차: yfinance fallback (신형 코드 등 pykrx 미지원)
                log.info(f"[{code}] yfinance fallback ({code}.KS)")
                d = collect_via_yfinance(f"{code}.KS", etf["listed_date"], cycle)

            # 네이버 금융: AUM + NAV per share 보완
            naver_aum, naver_nav = _get_naver_etf(code)
            aum = naver_aum or d.get("aum")
            nav_per_share = naver_nav or d.get("nav_per_share")

        else:
            # US ETF: yfinance 일괄 수집
            d = collect_via_yfinance(code, etf["listed_date"], cycle)
            aum = d.get("aum")
            nav_per_share = None  # US ETF: 가격 ≈ NAV

        # 실질수익률 = 분배율 + NAV 변화율
        real_return_1y = None
        if d.get("dist_rate_12m") is not None and d.get("nav_change_1y") is not None:
            real_return_1y = round(d["dist_rate_12m"] + d["nav_change_1y"], 2)

        # 수집 성공 여부 확인 (핵심 데이터 존재 시 성공)
        if d.get("nav_current") is not None:
            success_count += 1
        else:
            log.error(f"[{code}] 핵심 데이터(현재가) 수집 실패 — DB에는 null 저장됨")

        conn.execute(
            """
            INSERT OR REPLACE INTO etf_weekly
                (code, collected_at,
                 nav_current, price_prev, price_change, price_change_pct, aum,
                 nav_change_1y, nav_change_since_listing,
                 return_1m, return_3m, return_6m,
                 nav_change_1m, nav_change_3m, nav_change_6m,
                 dist_rate_12m, dist_rate_monthly, dist_rate_annualized,
                 real_return_1y, nav_per_share)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            ),
        )
        conn.commit()

        if d.get("monthly_dists"):
            save_monthly_dists(conn, code, d["monthly_dists"])

        time.sleep(1)

    conn.close()
    log.info(f"수집 완료: {success_count}/{len(etfs)}개 성공")

    if success_count < len(etfs):
        log.error(f"수집 실패 ETF 존재: {len(etfs) - success_count}개")


if __name__ == "__main__":
    main()
