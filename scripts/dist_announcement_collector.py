"""
ETF 분배금 공시 수집기
  KR ETF : KRX 정보데이터시스템 API (MDCSTAT04601 / MDCSTAT04602)
  US ETF : yfinance dividends (이력) + ticker.calendar (예정)

저장 테이블: etf_dist_announcement
실행: GitHub Actions daily 워크플로우 + weekly 워크플로우에서 호출
"""

import json
import sqlite3
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
ETF_LIST_PATH = DATA_DIR / "etf_list.json"
DB_PATH = DATA_DIR / "etf.db"


# ── DB 초기화 ──────────────────────────────────────────────────────────────

def init_dist_table(conn):
    """테이블 생성 + 구 스키마(UNIQUE payment_date) → 신 스키마(UNIQUE ex_div_date) 마이그레이션."""
    # 기존 테이블 스키마 확인
    old = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='etf_dist_announcement'"
    ).fetchone()

    if old and "UNIQUE(code, payment_date)" in old[0]:
        log.info("DB 마이그레이션: UNIQUE(code, payment_date) → UNIQUE(code, ex_div_date)")
        conn.execute("ALTER TABLE etf_dist_announcement RENAME TO _ann_old")
        conn.execute("""
            CREATE TABLE etf_dist_announcement (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                code              TEXT    NOT NULL,
                announcement_date TEXT,
                record_date       TEXT,
                ex_div_date       TEXT,
                payment_date      TEXT,
                amount            REAL    NOT NULL,
                dist_rate         REAL,
                is_upcoming       INTEGER DEFAULT 0,
                collected_at      TEXT    NOT NULL,
                UNIQUE(code, ex_div_date)
            )
        """)
        # 데이터 복사 — yfinance 데이터(record_date IS NULL)는 payment_date = NULL로 정리
        conn.execute("""
            INSERT OR IGNORE INTO etf_dist_announcement
                (code, record_date, ex_div_date, payment_date,
                 amount, dist_rate, is_upcoming, collected_at)
            SELECT
                code, record_date, ex_div_date,
                CASE WHEN record_date IS NULL THEN NULL ELSE payment_date END,
                amount, dist_rate, is_upcoming, collected_at
            FROM _ann_old
        """)
        conn.execute("DROP TABLE _ann_old")
        conn.commit()
        log.info("DB 마이그레이션 완료")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS etf_dist_announcement (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            code              TEXT    NOT NULL,
            announcement_date TEXT,               -- 공시일
            record_date       TEXT,               -- 기준일
            ex_div_date       TEXT,               -- 배당락일
            payment_date      TEXT,               -- 지급예정일 (추정 포함)
            amount            REAL    NOT NULL,   -- 1주당 분배금 (원 or USD)
            dist_rate         REAL,               -- 분배율 (%)
            is_upcoming       INTEGER DEFAULT 0,  -- 0: 완료, 1: 예정
            collected_at      TEXT    NOT NULL,
            UNIQUE(code, ex_div_date)
        )
    """)
    conn.commit()


# ── 공통 유틸 ─────────────────────────────────────────────────────────────

def _to_float(v):
    if v in (None, "", "-", "N/A"):
        return None
    try:
        return float(str(v).replace(",", "").replace("%", "").strip())
    except (ValueError, TypeError):
        return None


def _to_date_str(v):
    """Timestamp / str → 'YYYY-MM-DD' 변환. 실패 시 None."""
    if v is None:
        return None
    if hasattr(v, "strftime"):
        return v.strftime("%Y-%m-%d")
    s = str(v)[:10]
    return s if len(s) == 10 and s[4] == "-" else None


# ── KR ETF: KRX 데이터 API ────────────────────────────────────────────────

def collect_kr_via_krx_api(code):
    """KRX 정보데이터시스템 API로 ETF 분배금 현황 수집."""
    import requests

    url     = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer":    "https://data.krx.co.kr/",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
    }
    from_date = (datetime.now() - timedelta(days=210)).strftime("%Y%m%d")
    to_date   = (datetime.now() + timedelta(days=90)).strftime("%Y%m%d")
    today     = datetime.now().strftime("%Y-%m-%d")

    for bld in ("dbms/MDC/STAT/standard/MDCSTAT04601",
                "dbms/MDC/STAT/standard/MDCSTAT04602"):
        try:
            payload = {
                "bld":         bld,
                "isuCd":       code,
                "isuCd2":      code,
                "strtDd":      from_date,
                "endDd":       to_date,
                "csvxls_isNo": "false",
            }
            res = requests.post(url, data=payload, headers=headers, timeout=10)
            if res.status_code != 200:
                continue

            data  = res.json()
            items = data.get("output") or data.get("OutBlock_1") or []
            if not items:
                continue

            log.info(f"[{code}] KRX({bld.split('/')[-1]}) keys: {list(items[0].keys())}")

            records = []
            for item in items:
                payment_date = None
                for f in ("PYMNT_DT", "PAY_DT", "DIST_PAY_DT", "DIST_DT",
                          "지급예정일", "지급일"):
                    v = _to_date_str(item.get(f))
                    if v:
                        payment_date = v
                        break

                amount = None
                for f in ("DIST_AMT", "DIST_CASH", "CASH_DIST_AMT",
                          "현금분배금", "분배금액"):
                    v = _to_float(item.get(f))
                    if v and v > 0:
                        amount = v
                        break

                if not (payment_date and amount):
                    continue

                records.append({
                    "code":         code,
                    "record_date":  _to_date_str(
                        item.get("RCD_DT") or item.get("STND_DT") or item.get("기준일")),
                    "ex_div_date":  _to_date_str(
                        item.get("EX_DIV_DT") or item.get("배당락일")),
                    "payment_date": payment_date,
                    "amount":       amount,
                    "dist_rate":    _to_float(
                        item.get("DIST_RT") or item.get("DIST_RATE") or item.get("분배율")),
                    "is_upcoming":  1 if payment_date > today else 0,
                })

            if records:
                log.info(f"[{code}] KRX API 분배금 {len(records)}건")
                return records

        except Exception as e:
            log.warning(f"[{code}] KRX API ({bld}) 오류: {e}")

    return []


# ── US ETF: yfinance ──────────────────────────────────────────────────────

def collect_us_via_yfinance(ticker, pay_offset=None):
    """yfinance dividends(이력) + calendar(예정)으로 분배금 수집.

    배당락일(ex_div_date)만 저장. 지급일은 yfinance에서 제공하지 않으므로 표시 안 함.
    pay_offset 파라미터는 하위 호환성 유지 목적으로만 남아 있음 (미사용).
    """
    try:
        import yfinance as yf

        t     = yf.Ticker(ticker)
        today = datetime.now().strftime("%Y-%m-%d")
        records = []

        # 현재가 (분배율 계산용)
        price = None
        try:
            price = float(t.fast_info.last_price)
        except Exception:
            pass

        # ── 최근 7개월 이력 ─────────────────────────────────────────────
        divs = t.dividends
        if not divs.empty:
            try:
                divs.index = divs.index.tz_convert(None)
            except TypeError:
                try:
                    divs.index = divs.index.tz_localize(None)
                except TypeError:
                    pass

            cutoff = datetime.now() - timedelta(days=210)
            recent = divs[divs.index >= cutoff]

            for date, amount in recent.items():
                if float(amount) <= 0:
                    continue
                ex_div_str   = date.strftime("%Y-%m-%d")
                dist_rate    = round(float(amount) / price * 100, 4) if price else None

                records.append({
                    "code":         ticker,
                    "record_date":  None,
                    "ex_div_date":  ex_div_str,  # 배당락일 (yfinance 제공)
                    "payment_date": None,         # 지급일 미제공 — 표시 안 함
                    "amount":       round(float(amount), 5),
                    "dist_rate":    dist_rate,
                    "is_upcoming":  1 if ex_div_str > today else 0,
                })

        # ── 다음 예정 분배금 (calendar) ─────────────────────────────────
        try:
            cal = t.calendar
            if cal and isinstance(cal, dict):
                ex_date_raw = cal.get("Ex-Dividend Date")
                div_amount  = cal.get("Dividend")

                if ex_date_raw and div_amount and float(div_amount) > 0:
                    ex_date_str = _to_date_str(ex_date_raw)

                    if not any(r["ex_div_date"] == ex_date_str for r in records):
                        dist_rate = round(float(div_amount) / price * 100, 4) if price else None
                        records.append({
                            "code":         ticker,
                            "record_date":  None,
                            "ex_div_date":  ex_date_str,
                            "payment_date": None,  # 지급일 미제공 — 표시 안 함
                            "amount":       round(float(div_amount), 5),
                            "dist_rate":    dist_rate,
                            "is_upcoming":  1 if ex_date_str > today else 0,
                        })
                        log.info(f"[{ticker}] 예정 분배금: {div_amount} (ex-div {ex_date_str})")
        except Exception as e:
            log.warning(f"[{ticker}] calendar 수집 오류: {e}")

        log.info(f"[{ticker}] 분배금 {len(records)}건 수집")
        return records

    except Exception as e:
        log.error(f"[{ticker}] yfinance 분배금 수집 오류: {e}")
        return []


# ── DB 저장 ────────────────────────────────────────────────────────────────

def save_announcements(conn, records, collected_at):
    saved = 0
    for r in records:
        try:
            conn.execute("""
                INSERT OR REPLACE INTO etf_dist_announcement
                    (code, record_date, ex_div_date, payment_date,
                     amount, dist_rate, is_upcoming, collected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                r["code"],            r.get("record_date"),
                r.get("ex_div_date"), r["payment_date"],
                r["amount"],          r.get("dist_rate"),
                r.get("is_upcoming", 0), collected_at,
            ))
            saved += 1
        except Exception as e:
            log.warning(f"[{r['code']}] 저장 오류 ({r.get('payment_date')}): {e}")
    conn.commit()
    return saved


# ── 메인 ──────────────────────────────────────────────────────────────────

def main():
    etfs = json.loads(ETF_LIST_PATH.read_text(encoding="utf-8"))
    DATA_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    init_dist_table(conn)

    collected_at = datetime.now().strftime("%Y-%m-%d")
    log.info(f"분배금 공시 수집 시작: {collected_at}  대상 {len(etfs)}개")

    total_saved = 0

    for etf in etfs:
        code    = etf.get("code") or etf.get("ticker")
        country = etf.get("country")
        cycle   = etf.get("dividend_cycle", "월")
        log.info(f"══ {code} ({etf.get('name')}, {cycle}배당) ══")

        if country == "KR":
            # 배당주기별 지급일 추정 오프셋
            #   주배당: 배당락일 +5일 (같은 주 또는 다음 주 초 지급)
            #   월배당: 배당락일 +20일 (같은 달 말 또는 익월 초 지급)
            kr_pay_offset = 5 if cycle == "주" else 20

            records = collect_kr_via_krx_api(code)
            if not records:
                # KRX API 실패 → yfinance {code}.KS fallback (모든 KR ETF 코드 시도)
                log.info(f"[{code}] KRX API 실패 → yfinance {code}.KS fallback (offset={kr_pay_offset}일)")
                records = collect_us_via_yfinance(f"{code}.KS", pay_offset=kr_pay_offset)
                # code 필드를 원래 KR 코드로 덮어쓰기
                for r in records:
                    r["code"] = code
        else:
            records = collect_us_via_yfinance(code, pay_offset=14)

        if records:
            saved = save_announcements(conn, records, collected_at)
            total_saved += saved
            log.info(f"[{code}] {saved}건 저장")
        else:
            log.warning(f"[{code}] 분배금 데이터 수집 실패")

    conn.close()
    log.info(f"분배금 공시 수집 완료: 총 {total_saved}건 저장")


if __name__ == "__main__":
    main()
