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
    "ALTER TABLE etf_weekly ADD COLUMN nav_per_share REAL",
    "ALTER TABLE etf_weekly ADD COLUMN nav_change_1m REAL",
    "ALTER TABLE etf_weekly ADD COLUMN nav_change_3m REAL",
    "ALTER TABLE etf_weekly ADD COLUMN nav_change_6m REAL",
    "ALTER TABLE etf_weekly ADD COLUMN ex_date TEXT",
    "ALTER TABLE etf_weekly ADD COLUMN pay_date TEXT",
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
            nav_change_1m            REAL,
            nav_change_3m            REAL,
            nav_change_6m            REAL,
            ex_date                  TEXT,
            pay_date                 TEXT,
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
    """월별 분배금 dict {YYYY-MM: amount} → etf_monthly_dist 저장"""
    for ym, amount in monthly_dists.items():
        conn.execute(
            "INSERT OR REPLACE INTO etf_monthly_dist (code, year_month, amount) VALUES (?, ?, ?)",
            (code, ym, amount),
        )
    conn.commit()


# ── 네이버 금융 API (KR ETF AUM + 순자산가치) ──────────────────────────────

def _get_naver_etf(code):
    """네이버 금융 API로 KR ETF 시가총액 + 순자산가치/주 수집"""

    def to_float(v):
        try:
            return float(str(v).replace(",", "").strip()) if v not in (None, "", "-", "0") else None
        except (ValueError, TypeError):
            return None

    # ── 1. 기본 정보 endpoint ──────────────────────────────────────────────
    aum, nav = None, None
    try:
        url = f"https://m.stock.naver.com/api/stock/{code}/basic"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        log.info(f"[{code}] Naver basic API {res.status_code}")
        if res.status_code == 200:
            data = res.json()

            # AUM: Naver는 marketValue (시가총액) 사용
            for f in ("marketValue", "marketCap", "totalAsset", "fundTotalAsset",
                      "netAsset", "navTotalAsset", "etfTotalAsset"):
                aum = to_float(data.get(f))
                if aum and aum > 1000:   # 최소 1천원 이상이어야 AUM
                    log.info(f"[{code}] AUM={aum:,.0f} (basic/{f})")
                    break
                aum = None

            # NAV per share
            for f in ("iNav", "nav", "navPrice", "netAssetValue",
                      "iNavValue", "estimatedNav", "navPerUnit"):
                nav = to_float(data.get(f))
                if nav and nav > 100:    # 최소 100원 이상이어야 NAV
                    log.info(f"[{code}] NAV={nav:,.2f} (basic/{f})")
                    break
                nav = None

            # 찾지 못하면 전체 응답 로깅 (디버그용)
            if aum is None or nav is None:
                non_null = {k: v for k, v in data.items()
                            if v not in (None, "", "-", "0", 0)}
                log.info(f"[{code}] Naver basic 응답 (non-null): {non_null}")
    except Exception as e:
        log.warning(f"[{code}] Naver basic API 오류: {e}")

    # ── 2. ETF 전용 endpoint (basic에서 못 찾은 경우) ───────────────────────
    if aum is None or nav is None:
        for ep in ("etfAnalysis", "etfSummary", "etfInfo"):
            try:
                url2 = f"https://m.stock.naver.com/api/stock/{code}/{ep}"
                res2 = requests.get(url2, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
                log.info(f"[{code}] Naver {ep} API {res2.status_code}")
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
                            log.info(f"[{code}] NAV={nav:,.2f} ({ep}/{f})")
                            break
                        nav = None

                if aum is not None and nav is not None:
                    break

                if aum is None and nav is None:
                    non_null2 = {k: v for k, v in d2.items()
                                 if v not in (None, "", "-", "0", 0)}
                    log.info(f"[{code}] Naver {ep} 응답 (non-null): {non_null2}")

            except Exception as e:
                log.warning(f"[{code}] Naver {ep} API 오류: {e}")

    return aum, nav


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


def _parse_krx_date(val):
    """KRX 날짜 문자열 정규화: YYYYMMDD → YYYY-MM-DD"""
    v = str(val or "").strip().replace("/", "-")
    if len(v) == 8 and v.isdigit():
        return f"{v[:4]}-{v[4:6]}-{v[6:]}"
    if len(v) == 10 and v[4] == "-":
        return v
    return None


def _get_kr_dist_dates(code):
    """KRX 데이터포털 API로 KR ETF 배당락일(ex_date) + 지급예정일(pay_date) 수집.

    KRX getJsonData.cmd 에 여러 bld 값을 순서대로 시도한다.
    성공 시 가장 최근/미래에 가까운 공시 레코드에서 날짜를 추출한다.
    Returns: (ex_date, pay_date)  — 못 찾으면 (None, None)
    """
    today = datetime.now()
    strt = (today - timedelta(days=90)).strftime("%Y%m%d")
    end  = (today + timedelta(days=90)).strftime("%Y%m%d")

    krx_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "http://data.krx.co.kr/",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
    }

    # bld 값 후보 — 각각 다른 파라미터 조합 시도
    configs = [
        # ETF 분배금 지급기준일 현황 (날짜범위)
        {"bld": "dbms/MDC/STAT/standard/MDCSTAT04401",
         "strtDd": strt, "endDd": end},
        # ETF 분배금 지급현황 (날짜범위)
        {"bld": "dbms/MDC/STAT/standard/MDCSTAT04501",
         "strtDd": strt, "endDd": end},
        # isuCd로 특정 종목 조회 시도
        {"bld": "dbms/MDC/STAT/standard/MDCSTAT04401",
         "isuCd": code, "strtDd": strt, "endDd": end},
        {"bld": "dbms/MDC/STAT/standard/MDCSTAT04501",
         "isuCd": code, "strtDd": strt, "endDd": end},
    ]

    # 날짜 필드 후보 (배당락일 / 지급예정일)
    EX_FIELDS  = ["EXRT_DT", "EX_DT", "exrtDt", "exDt", "BDIV_PAYMNT_RECORD_DT",
                  "RECORD_DT", "recordDt"]
    PAY_FIELDS = ["DIST_PAYMNT_DT", "PAYMNT_DT", "payDt", "distPaymntDt",
                  "BDIV_PAYMNT_DT", "bdivPaymntDt", "PAY_DT"]
    CODE_FIELDS = ["ISU_SRT_CD", "isuSrtCd", "shrtCd", "STD_CD", "stdCd"]

    for cfg in configs:
        try:
            resp = requests.post(
                "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
                data=cfg,
                headers=krx_headers,
                timeout=15,
            )
            log.info(f"[{code}] KRX bld={cfg['bld'].split('/')[-1]}: "
                     f"HTTP {resp.status_code} ({len(resp.content)} bytes)")
            if resp.status_code != 200:
                continue

            j = resp.json()

            # 응답 최상위 키 로깅 (디버그)
            top_keys = list(j.keys())
            log.info(f"[{code}] KRX 응답 최상위 키: {top_keys}")

            # 리스트 블록 탐색
            items = None
            for block_key in ("OutBlock_1", "output", "items", "list", "data"):
                candidate = j.get(block_key)
                if isinstance(candidate, list) and candidate:
                    items = candidate
                    log.info(f"[{code}] KRX block='{block_key}' len={len(items)} "
                             f"sample_keys={list(items[0].keys())[:8]}")
                    break

            if not items:
                # 단일 객체 응답이면 리스트로 감싸기
                if isinstance(j, dict) and any(f in j for f in EX_FIELDS + PAY_FIELDS):
                    items = [j]
                else:
                    log.info(f"[{code}] KRX 응답에 리스트 블록 없음")
                    continue

            # 매칭 레코드 찾기 (최신 순으로 정렬 후 필터)
            matched = []
            for item in items:
                # 종목 코드 매칭 (코드 필드가 없으면 전체 대상으로)
                item_code = None
                for cf in CODE_FIELDS:
                    v = str(item.get(cf) or "").strip()
                    if v:
                        item_code = v
                        break
                if item_code and item_code != code:
                    continue
                matched.append(item)

            if not matched:
                log.info(f"[{code}] KRX 매칭 레코드 없음 (전체 {len(items)}건)")
                continue

            # pay_date 기준 내림차순 → 가장 최신/미래 레코드 선택
            def sort_key(item):
                for f in PAY_FIELDS + EX_FIELDS:
                    v = _parse_krx_date(item.get(f))
                    if v:
                        return v
                return "0000-00-00"

            matched.sort(key=sort_key, reverse=True)
            best = matched[0]

            ex_date  = next((_parse_krx_date(best.get(f)) for f in EX_FIELDS  if best.get(f)), None)
            pay_date = next((_parse_krx_date(best.get(f)) for f in PAY_FIELDS if best.get(f)), None)

            log.info(f"[{code}] KRX 최종: ex_date={ex_date} pay_date={pay_date} "
                     f"(레코드={dict(list(best.items())[:6])})")

            if ex_date or pay_date:
                return ex_date, pay_date

        except Exception as e:
            log.warning(f"[{code}] KRX dist 오류 ({cfg.get('bld', '?').split('/')[-1]}): {e}")

    log.info(f"[{code}] KRX dist: 모든 시도 실패 → Yahoo Finance 폴백 사용")
    return None, None


def _get_aum(ticker, session, crumb):
    """Yahoo Finance 종목 페이지 HTML에서 AUM + 지급일(pay_date) 파싱 (API 401 우회)
    Returns: (aum, pay_date) tuple
    """
    import re as _re
    aum, pay_date = None, None
    try:
        url = f"https://finance.yahoo.com/quote/{ticker}"
        res = session.get(url, timeout=15)
        log.info(f"[{ticker}] YF HTML {res.status_code} ({len(res.content)} bytes)")
        if res.status_code != 200:
            log.warning(f"[{ticker}] YF HTML 수집 실패")
            return aum, pay_date
        text = res.text
        # HTML 내 JSON이 이중 인코딩되어 따옴표가 \" 로 이스케이프됨
        # → 필드명만 찾고, 그 뒤 80자 안에서 raw:숫자 패턴 매칭
        for field in ("totalAssets", "totalNetAssets", "netAssets"):
            idx = text.find(field)
            if idx != -1:
                snippet = text[idx:idx + 80]
                m = _re.search(r'raw[^:]*:\s*(\d+)', snippet)
                if m:
                    aum = float(m.group(1))
                    log.info(f"[{ticker}] AUM={aum:,.0f} (from HTML {field})")
                    break
        if aum is None:
            log.warning(f"[{ticker}] AUM 필드 없음 (HTML 파싱 실패)")

        # 지급일(pay_date): dividendDate 필드
        idx = text.find("dividendDate")
        if idx != -1:
            snippet = text[idx:idx + 120]
            # "fmt":"2024-01-15" 형식
            m = _re.search(r'"fmt"\s*:\s*"(\d{4}-\d{2}-\d{2})"', snippet)
            if m:
                pay_date = m.group(1)
                log.info(f"[{ticker}] pay_date={pay_date} (fmt)")
            else:
                # "raw":1234567890 형식 (unix timestamp)
                m = _re.search(r'"raw"\s*:\s*(\d{9,10})', snippet)
                if m:
                    pay_date = datetime.fromtimestamp(int(m.group(1))).strftime("%Y-%m-%d")
                    log.info(f"[{ticker}] pay_date={pay_date} (raw ts)")
    except Exception as e:
        log.warning(f"[{ticker}] YF HTML 오류: {e}")
    return aum, pay_date


# ── Yahoo Finance Chart API ────────────────────────────────────────────────

def collect_via_yahoo_api(ticker, listed_date, dividend_cycle="월", session=None, crumb=None):
    """Yahoo Finance Chart API로 가격 이력 + 분배금 전체 수집 → dict"""
    empty = {
        "nav_current": None, "price_prev": None,
        "price_change": None, "price_change_pct": None,
        "nav_change_1y": None, "nav_change_since_listing": None,
        "return_1m": None, "return_3m": None, "return_6m": None,
        "nav_change_1m": None, "nav_change_3m": None, "nav_change_6m": None,
        "ex_date": None,
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
        # NAV 변화율 (가격기반, ETF 특성상 가격 ≈ NAV)
        result["nav_change_1m"]          = result["return_1m"]
        result["nav_change_3m"]          = result["return_3m"]
        result["nav_change_6m"]          = result["return_6m"]
        p_first = float(prices.iloc[0])
        result["nav_change_since_listing"] = round((price_now / p_first - 1) * 100, 2) if p_first else None

        # 분배금
        raw_divs = (chart.get("events") or {}).get("dividends") or {}
        if raw_divs:
            # 가장 최근 배당락일 (dividend event timestamp = ex-dividend date)
            max_ts = max(int(k) for k in raw_divs.keys())
            result["ex_date"] = datetime.fromtimestamp(max_ts).strftime("%Y-%m-%d")
            log.info(f"[{ticker}] 최근 배당락일(ex_date)={result['ex_date']}")

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
            # 네이버 금융: KR ETF AUM + 순자산가치
            naver_aum, nav_per_share = _get_naver_etf(code)
            aum = naver_aum
            # KRX 공시: 배당락일 + 지급예정일 (우선), 실패 시 Yahoo Finance 폴백
            kr_ex_date, pay_date = _get_kr_dist_dates(code)
            if kr_ex_date:
                d["ex_date"] = kr_ex_date   # KRX 공시 배당락일로 override
            if pay_date is None:
                _, pay_date = _get_aum(f"{code}.KS", yf_session, yf_crumb)
        else:
            d = collect_via_yahoo_api(code, etf["listed_date"], cycle, yf_session, yf_crumb)
            # US ETF AUM + 지급일: Yahoo Finance HTML 파싱
            aum, pay_date = _get_aum(code, yf_session, yf_crumb) if d.get("nav_current") else (None, None)
            nav_per_share = None  # US ETF는 가격 ≈ NAV

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
                 nav_change_1m, nav_change_3m, nav_change_6m,
                 ex_date, pay_date,
                 dist_rate_12m, dist_rate_monthly, dist_rate_annualized,
                 real_return_1y, nav_per_share)
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
                d.get("ex_date"),       pay_date,
                d.get("dist_rate_12m"), d.get("dist_rate_monthly"), d.get("dist_rate_annualized"),
                real_return_1y, nav_per_share,
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
