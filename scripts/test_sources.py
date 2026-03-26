# “””
ETF 데이터 소스 테스트 스크립트

실행: python3 scripts/test_sources.py
결과: 어떤 소스에서 어떤 데이터가 오는지 한눈에 확인
“””

import requests
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

# 테스트 대상

KR_CODE = “493810”          # TIGER 미국AI빅테크10타겟데일리커버드콜 (6자리)
KR_CODE_NEW = “0008S0”      # TIGER 미국배당다우존스타겟데일리커버드콜 (신형코드)
US_TICKER = “JEPI”

TODAY = datetime.now().strftime(”%Y%m%d”)
ONE_YEAR_AGO = (datetime.now() - timedelta(days=365)).strftime(”%Y%m%d”)

def section(title):
print(f”\n{’=’*50}”)
print(f”  {title}”)
print(f”{’=’*50}”)

def ok(msg): print(f”  ✅ {msg}”)
def fail(msg): print(f”  ❌ {msg}”)
def info(msg): print(f”  ℹ️  {msg}”)

# ── 1. pykrx 테스트 ────────────────────────────────────

section(“1. pykrx - NAV 이력 (6자리 코드)”)
try:
from pykrx import stock
df = stock.get_etf_ohlcv_by_date(ONE_YEAR_AGO, TODAY, KR_CODE)
if not df.empty:
ok(f”NAV 이력 {len(df)}행 수신”)
print(df[[“NAV”, “종가”]].tail(3).to_string())
else:
fail(“데이터 없음 (Empty DataFrame)”)
except Exception as e:
fail(f”오류: {e}”)

section(“1b. pykrx - NAV 이력 (신형 코드 0008S0)”)
try:
from pykrx import stock
df = stock.get_etf_ohlcv_by_date(ONE_YEAR_AGO, TODAY, KR_CODE_NEW)
if not df.empty:
ok(f”NAV 이력 {len(df)}행 수신”)
print(df[[“NAV”, “종가”]].tail(3).to_string())
else:
fail(“데이터 없음 — 신형 코드 미지원 가능성”)
except Exception as e:
fail(f”오류: {e}”)

section(“1c. pykrx - 괴리율 (6자리 코드)”)
try:
from pykrx import stock
df = stock.get_etf_price_deviation(ONE_YEAR_AGO, TODAY, KR_CODE)
if not df.empty:
ok(f”괴리율 {len(df)}행 수신”)
print(df.tail(3).to_string())
else:
fail(“데이터 없음”)
except Exception as e:
fail(f”오류: {e}”)

time.sleep(1)

# ── 2. 네이버 금융 테스트 ──────────────────────────────

section(“2. 네이버 금융 - 현재가 / NAV (6자리)”)
try:
headers = {“User-Agent”: “Mozilla/5.0”}
url = f”https://finance.naver.com/item/main.naver?code={KR_CODE}”
res = requests.get(url, headers=headers, timeout=10)
soup = BeautifulSoup(res.text, “lxml”)

```
price = soup.select_one(".today .blind")
name = soup.select_one(".wrap_company h2")
ok(f"종목명: {name.text.strip() if name else '없음'}")
ok(f"현재가: {price.text if price else '없음'}")
```

except Exception as e:
fail(f”오류: {e}”)

section(“2b. 네이버 금융 - 현재가 (신형 코드 0008S0)”)
try:
headers = {“User-Agent”: “Mozilla/5.0”}
url = f”https://finance.naver.com/item/main.naver?code={KR_CODE_NEW}”
res = requests.get(url, headers=headers, timeout=10)
soup = BeautifulSoup(res.text, “lxml”)

```
price = soup.select_one(".today .blind")
name = soup.select_one(".wrap_company h2")
ok(f"종목명: {name.text.strip() if name else '없음'}")
ok(f"현재가: {price.text if price else '없음'}")
```

except Exception as e:
fail(f”오류: {e}”)

section(“2c. 네이버 금융 - 분배금 이력”)
try:
import pandas as pd
from io import StringIO
headers = {“User-Agent”: “Mozilla/5.0”}

```
# 분배금 이력 페이지 시도
for page in [1, 2]:
    url = f"https://finance.naver.com/item/board.naver?code={KR_CODE}&page={page}"
    res = requests.get(url, headers=headers, timeout=10)
    tables = pd.read_html(StringIO(res.text))
    if tables:
        ok(f"테이블 {len(tables)}개 발견 (page {page})")
        for i, t in enumerate(tables[:2]):
            print(f"  테이블{i}: {list(t.columns)}")
        break
```

except Exception as e:
fail(f”오류: {e}”)

time.sleep(1)

# ── 3. KRX 분배금 이력 (OTP 방식) ─────────────────────

section(“3. KRX - 분배금 이력 (OTP 방식)”)
try:
headers = {
“User-Agent”: “Mozilla/5.0”,
“Referer”: “https://data.krx.co.kr/”
}

```
# OTP 발급
otp_url = "https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
otp_payload = {
    "bld": "dbms/MDC/STAT/standard/MDCSTAT04602",
    "isuCd": KR_CODE,
    "strtDd": ONE_YEAR_AGO,
    "endDd": TODAY,
    "name": "fileDown",
    "url": "dbms/MDC/STAT/standard/MDCSTAT04602"
}
otp_res = requests.post(otp_url, data=otp_payload, headers=headers, timeout=10)
otp = otp_res.text.strip()
info(f"OTP: {otp[:30]}...")

# 데이터 요청
data_url = "https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
data_res = requests.post(
    data_url,
    data={"code": otp},
    headers={**headers, "Referer": otp_url},
    timeout=10
)
info(f"응답 크기: {len(data_res.content)} bytes")
info(f"응답 미리보기: {data_res.text[:200]}")

if len(data_res.content) > 100:
    ok("데이터 수신 성공!")
else:
    fail("데이터 없음")
```

except Exception as e:
fail(f”오류: {e}”)

time.sleep(1)

# ── 4. yfinance 테스트 (미국 ETF) ─────────────────────

section(“4. yfinance - 미국 ETF NAV + 분배금”)
try:
import yfinance as yf
etf = yf.Ticker(US_TICKER)

```
# 현재가
info_data = etf.info
price = info_data.get("regularMarketPrice") or info_data.get("navPrice")
ok(f"현재가: {price}")
ok(f"AUM: {info_data.get('totalAssets')}")
ok(f"총보수: {info_data.get('annualReportExpenseRatio')}")

# NAV 이력
hist = etf.history(period="1y")
if not hist.empty:
    price_now = hist["Close"].iloc[-1]
    price_1y = hist["Close"].iloc[0]
    change = round((price_now / price_1y - 1) * 100, 2)
    ok(f"NAV 변화율 1Y: {change}%")
else:
    fail("NAV 이력 없음")

# 분배금
divs = etf.dividends
if not divs.empty:
    divs.index = divs.index.tz_localize(None)
    one_year_ago_dt = datetime.now() - timedelta(days=365)
    divs_12m = divs[divs.index >= one_year_ago_dt]
    ok(f"최근 12M 분배금 횟수: {len(divs_12m)}회")
    ok(f"최근 12M 분배금 합계: ${divs_12m.sum():.4f}")
    if price:
        dist_rate = round(divs_12m.sum() / price * 100, 2)
        ok(f"분배율 12M: {dist_rate}%")
else:
    fail("분배금 이력 없음")
```

except Exception as e:
fail(f”오류: {e}”)

# ── 최종 요약 ──────────────────────────────────────────

section(“테스트 완료 — 결과 요약 확인”)
print(”””
위 결과를 Claude에게 복붙해주세요.
✅ 표시된 소스를 기반으로 collector.py를 작성합니다.
“””)