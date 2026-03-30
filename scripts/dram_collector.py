"""
D램 현물가/고정거래가 수집기 (TrendForce)
실행: python3 scripts/dram_collector.py

수집 전략:
  1. cloudscraper로 TrendForce /price/dram/dram_spot 페이지 파싱
  2. 실패 시 최근 주간 spot price update 기사 파싱
  3. 모두 실패 시 기존 데이터 유지 (job은 성공 처리)
"""

import json
import re
import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH  = DATA_DIR / "etf.db"
JSON_OUT = BASE_DIR / "docs" / "dram.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


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
    saved = 0
    for r in records:
        try:
            conn.execute(
                """INSERT OR IGNORE INTO dram_prices
                   (date, type, capacity, spot_price, contract_price, currency, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (r["date"], r["type"], r["capacity"],
                 r.get("spot_price"), r.get("contract_price"),
                 r.get("currency", "USD"), r.get("source", "trendforce")),
            )
            if conn.execute("SELECT changes()").fetchone()[0]:
                saved += 1
        except Exception as e:
            log.warning(f"DB 저장 오류: {e} — {r}")
    conn.commit()
    return saved


# ── cloudscraper 세션 ──────────────────────────────────────────────────────

def _make_scraper():
    """cloudscraper 인스턴스 생성 (Cloudflare bypass)"""
    import cloudscraper
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "desktop": True}
    )
    scraper.headers.update({
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.trendforce.com/",
    })
    return scraper


def _to_float(s):
    if s is None:
        return None
    try:
        cleaned = re.sub(r"[^\d.]", "", str(s))
        return float(cleaned) if cleaned else None
    except (ValueError, TypeError):
        return None


# ── 전략 1: /price/dram/dram_spot 페이지 파싱 ─────────────────────────────

def _scrape_price_page(scraper):
    """TrendForce DRAM spot price 전용 페이지"""
    results = []
    url = "https://www.trendforce.com/price/dram/dram_spot"
    try:
        res = scraper.get(url, timeout=20)
        log.info(f"[price page] HTTP {res.status_code} ({len(res.content):,} bytes)")
        if res.status_code != 200:
            return results

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(res.text, "lxml")

        # 페이지 내 JSON 데이터 탐색
        for tag in soup.find_all("script"):
            text = tag.string or ""
            # Next.js / 임베디드 JSON
            for pat in (
                r'"spot[Pp]rice[s]?"\s*:\s*(\[.*?\])',
                r'"dram"\s*:\s*(\[.*?\])',
                r'"prices"\s*:\s*(\[.*?\])',
            ):
                m = re.search(pat, text, re.DOTALL)
                if m:
                    try:
                        items = json.loads(m.group(1))
                        parsed = _parse_price_json(items)
                        log.info(f"[price page] JSON 파싱 {len(parsed)}건")
                        results.extend(parsed)
                    except Exception:
                        pass

        if not results:
            # 테이블 파싱 시도
            results.extend(_parse_price_table(soup))

        log.info(f"[price page] 최종 {len(results)}건")
    except Exception as e:
        log.warning(f"[price page] 오류: {type(e).__name__}: {e}")
    return results


def _parse_price_json(items):
    results = []
    date_str = datetime.now().strftime("%Y-%m-%d")
    if not isinstance(items, list):
        items = [items]
    for item in items:
        if not isinstance(item, dict):
            continue
        keys = {k.lower(): k for k in item}
        cap_k = keys.get("capacity") or keys.get("spec") or keys.get("density")
        typ_k = keys.get("type") or keys.get("product") or keys.get("ddrtype")
        spot_k = keys.get("spot") or keys.get("spot_price") or keys.get("spotprice")
        con_k  = keys.get("contract") or keys.get("contract_price") or keys.get("contractprice")
        if cap_k and spot_k:
            raw = item[cap_k]
            m_type = re.search(r'(DDR[45])', str(raw), re.I)
            m_cap  = re.search(r'(\d+\s*GB)', str(raw), re.I)
            results.append({
                "date": date_str,
                "type": (m_type.group(1).upper() if m_type
                         else str(item.get(typ_k, "DDR4")).upper()),
                "capacity": (m_cap.group(1).replace(" ", "") if m_cap else str(raw)),
                "spot_price": _to_float(item.get(spot_k)),
                "contract_price": _to_float(item.get(con_k)) if con_k else None,
                "currency": "USD",
            })
    return results


def _parse_price_table(soup):
    results = []
    date_str = datetime.now().strftime("%Y-%m-%d")
    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        text_h = " ".join(headers)
        if not any(w in text_h for w in ("ddr", "spot", "gb", "price")):
            continue
        log.info(f"[price page] 테이블 발견: {headers}")
        for row in table.find_all("tr")[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if len(cells) < 2:
                continue
            product = cells[0]
            m_type = re.search(r'(DDR[45])', product, re.I)
            m_cap  = re.search(r'(\d+GB)', product, re.I)
            # 숫자처럼 보이는 셀에서 현물가 추출
            prices = [_to_float(c) for c in cells[1:] if _to_float(c) and _to_float(c) > 0.5]
            if prices:
                results.append({
                    "date": date_str,
                    "type": m_type.group(1).upper() if m_type else "DDR4",
                    "capacity": m_cap.group(1).upper() if m_cap else product,
                    "spot_price": prices[0],
                    "contract_price": prices[1] if len(prices) > 1 else None,
                    "currency": "USD",
                })
    return results


# ── 전략 2: 주간 spot price update 기사 파싱 ──────────────────────────────

# 검색 결과 확인된 가격 패턴 예시:
#   "DDR4 1Gx8 3200MT/s ... US$34.00"
#   "DDR5 16Gb 4800MT/s ... US$2.50"
#   "8GB DDR4 ... spot price of $X.XX"
PRICE_PATTERNS = [
    # "DDR4 1Gx8 ... US$34.00" 또는 "DDR4 4Gb ... $1.234"
    (r'(DDR[45])\s+(\d+G[bx]\d*[^,]*?)\s+[^\d]*?US?\$\s*([\d.]+)', 'spec'),
    # "8GB DDR4 ... $3.50" 또는 "16GB DDR5 ... US$5.00"
    (r'(\d+GB)\s+(DDR[45])[^\d]*?US?\$\s*([\d.]+)', 'cap_type'),
    # "DDR4 8GB spot price ... $X.XX"
    (r'(DDR[45])\s+(\d+GB)[^\d]*?US?\$\s*([\d.]+)', 'type_cap'),
    # generic "X.XX" near DDR mentions (fallback)
    (r'(DDR[45])[^.]*?([\d]+[Gx][\w]*)[^.]*?US?\$\s*([\d.]+)', 'generic'),
]

# 칩 스펙 → 용량 매핑
SPEC_TO_CAP = {
    "512mx8": "4GB", "512m": "4GB",
    "1gx8": "8GB",  "1g":  "8GB",
    "2gx8": "16GB", "2g":  "16GB",
    "4gx8": "32GB", "4g":  "32GB",
    "8g":   "64GB",
}


def _spec_to_capacity(spec_str):
    """'1Gx8', '2Gx8 4800' 등 칩 스펙 문자열 → 'XGB' 변환"""
    s = spec_str.lower().strip()
    for k, v in SPEC_TO_CAP.items():
        if s.startswith(k):
            return v
    # 숫자 + G 패턴 (예: 16Gb → 16GB)
    m = re.match(r'(\d+)\s*g', s)
    if m:
        n = int(m.group(1))
        # Gb(기가비트) vs GB(기가바이트) 구분 — 스펙 표기는 보통 Gb
        # 4Gb → 512MB (모듈 아님), 8Gb→1GB, 16Gb→2GB 이런 식 — 모듈 GB로 변환 불가
        # 그냥 숫자 그대로 사용 (기사 맥락에 따라)
        return f"{n}GB"
    return spec_str.upper()


def _find_latest_article_url(scraper):
    """TrendForce 뉴스 목록에서 최신 spot price update 기사 URL 탐색"""
    # 날짜 기반으로 최근 4주 시도
    today = datetime.now()
    candidates = []
    for i in range(0, 28, 7):
        d = today - timedelta(days=i)
        # 월요일로 정렬 (TrendForce는 보통 화~수 발행)
        for offset in range(-2, 4):
            dt = d + timedelta(days=offset)
            candidates.append(dt)

    tried_urls = set()
    # 뉴스 목록 페이지에서 기사 URL 검색
    list_urls = [
        "https://www.trendforce.com/news/tag/spot-price/",
        "https://www.trendforce.com/news/",
    ]
    for list_url in list_urls:
        try:
            res = scraper.get(list_url, timeout=15)
            log.info(f"[article search] {list_url} → HTTP {res.status_code}")
            if res.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(res.text, "lxml")
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if "spot-price-update" in href or "spot-price" in href.lower():
                        if href.startswith("/"):
                            href = "https://www.trendforce.com" + href
                        if href not in tried_urls:
                            log.info(f"[article search] 후보 URL: {href}")
                            return href
        except Exception as e:
            log.warning(f"[article search] {list_url} 오류: {e}")

    # 직접 날짜 기반 URL 구성 시도
    for dt in sorted(set(candidates), reverse=True)[:14]:
        slug = f"insights-memory-spot-price-update"
        url = f"https://www.trendforce.com/news/{dt.strftime('%Y/%m/%d')}/{slug}"
        if url not in tried_urls:
            tried_urls.add(url)
            try:
                res = scraper.head(url, timeout=8, allow_redirects=True)
                log.info(f"[article search] HEAD {url} → {res.status_code}")
                if res.status_code == 200:
                    return url
            except Exception:
                pass

    return None


def _scrape_article(scraper, url=None):
    """주간 spot price update 기사에서 가격 파싱"""
    results = []
    if url is None:
        url = _find_latest_article_url(scraper)
    if url is None:
        log.warning("[article] 기사 URL을 찾지 못함")
        return results

    try:
        res = scraper.get(url, timeout=20)
        log.info(f"[article] {url} → HTTP {res.status_code} ({len(res.content):,} bytes)")
        if res.status_code != 200:
            return results

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(res.text, "lxml")

        # 기사 날짜 추출
        date_str = datetime.now().strftime("%Y-%m-%d")
        m_date = re.search(r'/news/(\d{4}/\d{2}/\d{2})/', url)
        if m_date:
            date_str = m_date.group(1).replace("/", "-")
        else:
            for tag in soup.find_all(["time", "meta"]):
                dt_val = tag.get("datetime") or tag.get("content", "")
                m = re.search(r'(\d{4}-\d{2}-\d{2})', dt_val)
                if m:
                    date_str = m.group(1)
                    break

        # 기사 본문 텍스트
        article = soup.find("article") or soup.find(class_=re.compile(r'content|article|body'))
        text = article.get_text(" ", strip=True) if article else soup.get_text(" ", strip=True)
        log.info(f"[article] 본문 {len(text):,}자")

        seen = set()
        for pattern, mode in PRICE_PATTERNS:
            for m in re.finditer(pattern, text, re.IGNORECASE):
                groups = m.groups()
                if mode == 'spec':
                    ddr_type, spec, price_str = groups
                    capacity = _spec_to_capacity(spec)
                elif mode == 'cap_type':
                    capacity, ddr_type, price_str = groups
                elif mode == 'type_cap':
                    ddr_type, capacity, price_str = groups
                else:
                    ddr_type, spec, price_str = groups
                    capacity = _spec_to_capacity(spec)

                price = _to_float(price_str)
                if not price or price < 0.1 or price > 500:
                    continue

                key = (ddr_type.upper(), capacity.upper())
                if key in seen:
                    continue
                seen.add(key)

                log.info(f"[article] {ddr_type.upper()} {capacity} = ${price}")
                results.append({
                    "date": date_str,
                    "type": ddr_type.upper(),
                    "capacity": capacity.upper(),
                    "spot_price": price,
                    "contract_price": None,
                    "currency": "USD",
                    "source": "trendforce_article",
                })

        log.info(f"[article] 파싱 결과 {len(results)}건")
    except Exception as e:
        log.error(f"[article] 오류: {type(e).__name__}: {e}")

    return results


# ── JSON export ────────────────────────────────────────────────────────────

def export_json(conn):
    rows = conn.execute("""
        SELECT date, type, capacity, spot_price, contract_price, currency
        FROM dram_prices
        ORDER BY type, capacity, date
    """).fetchall()

    data = {}
    for r in rows:
        key = f"{r[1]}_{r[2]}"
        data.setdefault(key, {
            "type": r[1], "capacity": r[2], "currency": r[5],
            "dates": [], "spot": [], "contract": []
        })
        data[key]["dates"].append(r[0])
        data[key]["spot"].append(r[3])
        data[key]["contract"].append(r[4])

    latest_rows = conn.execute("""
        SELECT type, capacity, spot_price, contract_price, currency, date
        FROM dram_prices p1
        WHERE date = (
            SELECT MAX(date) FROM dram_prices p2
            WHERE p2.type = p1.type AND p2.capacity = p1.capacity
        )
        ORDER BY type, capacity
    """).fetchall()
    latest_list = [{"type": r[0], "capacity": r[1], "spot": r[2],
                    "contract": r[3], "currency": r[4], "date": r[5]}
                   for r in latest_rows]

    prev_rows = conn.execute("""
        SELECT p.type, p.capacity, p.spot_price FROM dram_prices p
        WHERE p.date = (
            SELECT MAX(p2.date) FROM dram_prices p2
            WHERE p2.type = p.type AND p2.capacity = p.capacity
              AND p2.date < (
                  SELECT MAX(p3.date) FROM dram_prices p3
                  WHERE p3.type=p.type AND p3.capacity=p.capacity
              )
        )
    """).fetchall()
    prev_map = {(r[0], r[1]): r[2] for r in prev_rows}

    for item in latest_list:
        prev = prev_map.get((item["type"], item["capacity"]))
        if prev and item["spot"]:
            item["spot_change_pct"] = round((item["spot"] - prev) / prev * 100, 2)
        else:
            item["spot_change_pct"] = None

    out = {
        "series": list(data.values()),
        "latest": latest_list,
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M KST"),
    }
    JSON_OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(f"JSON export 완료: {len(latest_list)}개 품목")


# ── 메인 ──────────────────────────────────────────────────────────────────

def main():
    DATA_DIR.mkdir(exist_ok=True)
    JSON_OUT.parent.mkdir(exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    date_str = datetime.now().strftime("%Y-%m-%d")
    log.info(f"D램 가격 수집 시작: {date_str}")

    try:
        scraper = _make_scraper()
    except ImportError:
        log.error("cloudscraper 미설치 — pip install cloudscraper")
        export_json(conn)
        conn.close()
        return

    records = []

    # 전략 1: price 전용 페이지
    records = _scrape_price_page(scraper)

    # 전략 2: 주간 기사 파싱 (전략 1 실패 시)
    if not records:
        log.info("price 페이지 데이터 없음 → 주간 기사 파싱 시도")
        records = _scrape_article(scraper)

    if records:
        saved = save_prices(conn, records)
        log.info(f"저장: {saved}건 신규 / {len(records)}건 파싱")
    else:
        log.warning("수집 데이터 없음 — 기존 데이터 유지 (cloudscraper도 차단된 것으로 보임)")

    export_json(conn)
    conn.close()
    log.info("D램 수집 완료")


if __name__ == "__main__":
    main()
