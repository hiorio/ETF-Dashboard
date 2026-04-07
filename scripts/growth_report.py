"""
성장형 ETF 대시보드 HTML 리포트 생성기
실행: python3 scripts/growth_report.py
출력: docs/growth.html

대상: etf_meta.strategy = '성장' 인 ETF
표시: 가격·수익률 지표 중심 (배당 관련 컬럼 제외)
"""

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / "data" / "etf.db"
ETF_LIST_PATH = BASE_DIR / "data" / "etf_list.json"
DOCS_DIR = BASE_DIR / "docs"
OUT_PATH = DOCS_DIR / "growth.html"

STRATEGY = "성장"


# ── 포맷 헬퍼 ─────────────────────────────────────────────────────────────

def pct(val, digits=2):
    if val is None:
        return "-"
    return f"{val:+.{digits}f}%"


def price_fmt(val, country):
    if val is None:
        return "-"
    if country == "KR":
        return f"{val:,.0f}원"
    return f"${val:.2f}"


def aum_fmt(val, country):
    if val is None:
        return "-"
    if country == "KR":
        awk = val / 1e8
        if awk >= 10000:
            return f"{awk/10000:.1f}조원"
        return f"{awk:,.0f}억원"
    else:
        if val >= 1e9:
            return f"${val/1e9:.1f}B"
        return f"${val/1e6:.0f}M"


def color_class(val):
    if val is None:
        return "neutral"
    return "pos" if val >= 0 else "neg"


# ── DB 조회 ───────────────────────────────────────────────────────────────

def load_data():
    if not DB_PATH.exists():
        return [], [], {}

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT
            w.code, w.collected_at,
            w.nav_current, w.price_prev, w.price_change, w.price_change_pct,
            w.aum, w.nav_per_share, w.tax_base_price,
            w.nav_change_1y, w.nav_change_since_listing,
            w.return_1m, w.return_3m, w.return_6m,
            w.nav_change_1m, w.nav_change_3m, w.nav_change_6m,
            m.name, m.country, m.strategy, m.underlying, m.tax_type,
            m.dividend_cycle, m.manager, m.listed_date
        FROM etf_weekly w
        JOIN etf_meta m ON m.code = w.code
        WHERE m.strategy = ?
          AND w.collected_at = (
              SELECT MAX(collected_at) FROM etf_weekly WHERE code = w.code
          )
        ORDER BY
            CASE WHEN m.country = 'KR' THEN 0 ELSE 1 END,
            CASE WHEN w.nav_change_1y IS NULL THEN 1 ELSE 0 END,
            w.nav_change_1y DESC
    """, (STRATEGY,)).fetchall()

    # 수익률 이력 (최근 12주)
    codes = [r["code"] for r in rows]
    if not codes:
        conn.close()
        return [], [], {}

    placeholders = ",".join("?" * len(codes))
    history = conn.execute(f"""
        SELECT code, collected_at, nav_change_1y, return_1m, return_3m, return_6m
        FROM etf_weekly
        WHERE code IN ({placeholders})
          AND collected_at >= date('now', '-84 days')
        ORDER BY code, collected_at
    """, codes).fetchall()

    conn.close()
    return (
        [dict(r) for r in rows],
        [dict(r) for r in history],
    )


# ── HTML 생성 ─────────────────────────────────────────────────────────────

def build_html(rows, history):
    if not rows:
        updated = "데이터 없음"
    else:
        updated = rows[0]["collected_at"]
    now = datetime.now().strftime("%Y-%m-%d %H:%M KST")

    isin_map = {}
    link_map = {}
    try:
        etf_list = json.loads(ETF_LIST_PATH.read_text(encoding="utf-8"))
        for e in etf_list:
            key = e.get("code") or e.get("ticker", "")
            isin_map[key] = e.get("isin", "")
            if e.get("link"):
                link_map[key] = e["link"]
    except Exception:
        pass

    # 차트 데이터
    chart_labels = sorted({r["collected_at"] for r in history})
    names = {r["code"]: r["name"] for r in rows}
    PALETTE = ["#4f8ef7", "#f7874f", "#4fc98e", "#f74f7e", "#b04ff7", "#f7c94f",
               "#4ff7f0", "#f7f04f", "#f74fb0", "#7ef74f"]

    def series(code, field):
        by_date = {r["collected_at"]: r[field] for r in history if r["code"] == code}
        return [by_date.get(d) for d in chart_labels]

    chart_data = {
        "labels": chart_labels,
        "datasets": [
            {
                "code": code,
                "label": names.get(code, code),
                "nav_change_1y": series(code, "nav_change_1y"),
                "return_3m":     series(code, "return_3m"),
                "return_1m":     series(code, "return_1m"),
            }
            for code in [r["code"] for r in rows]
        ],
    }

    # ── 테이블 행 ─────────────────────────────────────────────────────────
    def row_html(r):
        code    = r["code"]
        country = r["country"]

        country_badge = (
            '<span class="badge kr">KR</span>'
            if country == "KR"
            else '<span class="badge us">US</span>'
        )
        tax_badge = ""
        tax_type = r.get("tax_type", "")
        if country == "US":
            tax_badge = '<span class="badge tax-overseas-etf">양도소득세</span>'
        elif tax_type == "해외주식형":
            tax_badge = '<span class="badge tax-foreign">해외주식형</span>'
        elif tax_type == "국내주식형":
            tax_badge = '<span class="badge tax-domestic">국내주식형</span>'

        price_disp  = price_fmt(r["nav_current"], country)
        prev_disp   = price_fmt(r["price_prev"], country)
        change_disp = price_fmt(r["price_change"], country) if r["price_change"] is not None else "-"
        change_pct      = pct(r.get("price_change_pct"))
        change_pct_cls  = color_class(r.get("price_change_pct"))
        aum_disp    = aum_fmt(r.get("aum"), country)
        listed      = r.get("listed_date") or "-"

        # NAV/주 + 프리미엄 (KR만)
        nav_ps   = r.get("nav_per_share")
        nav_ps_disp = price_fmt(nav_ps, "KR") if nav_ps else "-"
        price_now = r.get("nav_current")
        if nav_ps and price_now:
            pd_val  = round((price_now - nav_ps) / nav_ps * 100, 2)
            pd_disp = f'<span class="{color_class(pd_val)}">{pct(pd_val)}</span>'
        else:
            pd_disp = "-"

        # 과표기준가격
        tax_bp = r.get("tax_base_price")
        tax_bp_disp = f'{tax_bp:,.0f}원' if tax_bp else "-"

        r1m  = r.get("nav_change_1m") or r.get("return_1m")
        r3m  = r.get("nav_change_3m") or r.get("return_3m")
        r6m  = r.get("nav_change_6m") or r.get("return_6m")
        r1y  = r.get("nav_change_1y")
        rall = r.get("nav_change_since_listing")

        def td_pct(val):
            return f'<td class="{color_class(val)}">{pct(val)}</td>'

        # ETF 링크
        if code in link_map:
            etf_url = link_map[code]
        elif country == "KR":
            isin = isin_map.get(code, "")
            etf_url = (f"https://www.funetf.co.kr/product/etf/view/{isin}"
                       if isin else f"https://finance.naver.com/item/main.naver?code={code}")
        else:
            etf_url = f"https://finance.yahoo.com/quote/{code}"

        return f"""
        <tr>
          <td class="name-cell">
            <a href="{etf_url}" target="_blank" rel="noopener noreferrer" class="etf-name-link">
              <div class="etf-name">{r['name']}</div>
            </a>
            <div class="etf-sub">{country_badge} {tax_badge} {r.get('manager','')}</div>
          </td>
          <td class="muted-cell">{code}</td>
          <td>{price_disp}</td>
          <td>{prev_disp}</td>
          <td class="{change_pct_cls}">{change_disp}<br><span class="sub-val">{change_pct}</span></td>
          <td>{aum_disp}</td>
          <td class="muted-cell">{listed}</td>
          {td_pct(r1m)}
          {td_pct(r3m)}
          {td_pct(r6m)}
          {td_pct(r1y)}
          <td class="{color_class(rall)}">{pct(rall)}</td>
          <td>{nav_ps_disp}<br><span class="sub-val">{pd_disp}</span></td>
          <td class="muted-cell">{tax_bp_disp}</td>
        </tr>"""

    if rows:
        table_rows = "\n".join(row_html(r) for r in rows)
    else:
        table_rows = '<tr><td colspan="14" class="no-data">아직 수집된 데이터가 없습니다.</td></tr>'

    chart_json = json.dumps(chart_data, ensure_ascii=False)
    palette_json = json.dumps(PALETTE)

    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>성장형 ETF 대시보드</title>
<style>
  :root {{
    --bg: #0f1117;
    --card: #1a1d27;
    --border: #2a2d3a;
    --text: #e2e8f0;
    --muted: #8892a4;
    --pos: #4ade80;
    --neg: #f87171;
    --acc: #a78bfa;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    background: var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    font-size: 14px;
    line-height: 1.6;
  }}
  .header {{
    background: linear-gradient(135deg, #1a1d27 0%, #0f1117 100%);
    border-bottom: 1px solid var(--border);
    padding: 24px 20px 20px;
    text-align: center;
  }}
  .header h1 {{ font-size: 1.6rem; font-weight: 700; color: var(--acc); }}
  .header p  {{ color: var(--muted); margin-top: 4px; font-size: 0.85rem; }}
  .container {{ max-width: 1200px; margin: 0 auto; padding: 20px 16px; }}
  .section-title {{
    font-size: 0.9rem; font-weight: 600; color: var(--muted);
    text-transform: uppercase; letter-spacing: 0.08em; margin: 28px 0 12px;
  }}
  .table-wrap {{ overflow-x: auto; -webkit-overflow-scrolling: touch; }}
  table {{
    width: 100%; border-collapse: collapse;
    background: var(--card); border-radius: 10px; overflow: hidden;
  }}
  thead tr {{ background: #21253a; }}
  th {{
    padding: 10px 12px; text-align: right; font-weight: 600;
    color: var(--muted); font-size: 0.78rem; white-space: nowrap;
  }}
  th:first-child {{ text-align: left; }}
  td {{
    padding: 11px 12px; text-align: right;
    border-top: 1px solid var(--border); white-space: nowrap; font-size: 0.85rem;
  }}
  td:first-child {{ text-align: left; }}
  tr:hover td {{ background: rgba(167,139,250,0.04); }}
  .pos {{ color: var(--pos); }}
  .neg {{ color: var(--neg); }}
  .neutral {{ color: var(--text); }}
  .muted-cell {{ color: var(--muted); font-size: 0.8rem; }}
  .sub-val {{ font-size: 0.75rem; opacity: 0.85; }}
  .th-sub {{ font-size: 0.68rem; font-weight: 400; opacity: 0.65; }}
  .etf-name {{ font-weight: 500; font-size: 0.88rem; }}
  .etf-name-link {{ text-decoration: none; color: inherit; }}
  .etf-name-link:hover .etf-name {{ text-decoration: underline; color: var(--acc); }}
  .etf-sub {{ margin-top: 3px; display: flex; gap: 4px; align-items: center; flex-wrap: wrap; }}
  .badge {{
    display: inline-block; padding: 1px 6px;
    border-radius: 4px; font-size: 0.7rem; font-weight: 600;
  }}
  .badge.kr {{ background: #1e3a5f; color: #60a5fa; }}
  .badge.us {{ background: #3b1f1f; color: #f87171; }}
  .badge.tax-domestic  {{ background: #1a2a1a; color: #6ee7b7; }}
  .badge.tax-foreign   {{ background: #2a1f0a; color: #fcd34d; }}
  .badge.tax-overseas-etf {{ background: #2a0a0a; color: #fca5a5; }}
  .no-data {{
    text-align: center !important; padding: 40px !important;
    color: var(--muted); line-height: 2;
  }}
  /* 차트 */
  .charts {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
  @media (max-width: 680px) {{ .charts {{ grid-template-columns: 1fr; }} }}
  .chart-card {{
    background: var(--card); border: 1px solid var(--border);
    border-radius: 10px; padding: 16px;
  }}
  .chart-card h3 {{ font-size: 0.85rem; color: var(--muted); margin-bottom: 12px; }}
  canvas {{ width: 100% !important; }}
  .legend {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 10px; }}
  .legend-item {{ display: flex; align-items: center; gap: 5px; font-size: 0.75rem; color: var(--muted); }}
  .legend-dot {{ width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }}
  footer {{
    text-align: center; color: var(--muted); font-size: 0.75rem;
    padding: 24px 16px; border-top: 1px solid var(--border); margin-top: 32px;
  }}
</style>
</head>
<body>

<div class="header">
  <h1>🚀 성장형 ETF 대시보드</h1>
  <p>성장성 지표 중심 · 최종 수집: {updated} · 생성: {now}</p>
  <nav style="display:flex;justify-content:center;gap:12px;margin-top:14px;">
    <a href="index.html"  style="color:var(--muted);text-decoration:none;font-size:0.82rem;padding:4px 12px;border:1px solid var(--border);border-radius:6px;">📊 배당 ETF</a>
    <a href="growth.html" style="color:var(--acc);text-decoration:none;font-size:0.82rem;padding:4px 12px;border:1px solid var(--acc);border-radius:6px;">🚀 성장형 ETF</a>
    <a href="dram.html"   style="color:var(--muted);text-decoration:none;font-size:0.82rem;padding:4px 12px;border:1px solid var(--border);border-radius:6px;">🖥️ D램 가격</a>
  </nav>
</div>

<div class="container">
  <div class="section-title">📋 최신 지표 스냅샷 (1Y 수익률 기준 정렬)</div>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>ETF</th>
          <th>코드</th>
          <th>현재가</th>
          <th>전일가</th>
          <th>등락<br><span class="th-sub">전일대비</span></th>
          <th>시가총액<br><span class="th-sub">운용규모</span></th>
          <th>상장일</th>
          <th>수익률 1M<br><span class="th-sub">최근 1개월</span></th>
          <th>수익률 3M<br><span class="th-sub">최근 3개월</span></th>
          <th>수익률 6M<br><span class="th-sub">최근 6개월</span></th>
          <th>수익률 1Y ★<br><span class="th-sub">최근 1년</span></th>
          <th>상장이후<br><span class="th-sub">누적 수익률</span></th>
          <th>순자산가치<br><span class="th-sub">프리미엄/할인</span></th>
          <th>과표기준가격<br><span class="th-sub">해외주식형만</span></th>
        </tr>
      </thead>
      <tbody>
        {table_rows}
      </tbody>
    </table>
  </div>
  <p style="color:var(--muted);font-size:0.75rem;margin-top:8px;">
    ★ 수익률 1Y = 가격 수익률 (분배금 미포함) &nbsp;|&nbsp;
    과세: <span style="color:#fcd34d">■ 해외주식형</span> 매매차익·분배금 모두 15.4% &nbsp;|&nbsp;
    <span style="color:#fca5a5">■ 양도소득세</span> 해외 ETF 22% (연 250만원 공제)
  </p>

  <div class="section-title">📈 수익률 추이 차트 (최근 12주)</div>
  <div class="charts">
    <div class="chart-card">
      <h3>수익률 1Y (%)</h3>
      <canvas id="chart1y" height="200"></canvas>
    </div>
    <div class="chart-card">
      <h3>수익률 3M (%)</h3>
      <canvas id="chart3m" height="200"></canvas>
    </div>
    <div class="chart-card">
      <h3>수익률 1M (%)</h3>
      <canvas id="chart1m" height="200"></canvas>
    </div>
    <div class="chart-card" style="display:flex;flex-direction:column;justify-content:center;align-items:center;padding:24px;">
      <h3 style="margin-bottom:16px;">범례</h3>
      <div class="legend" id="legend"></div>
    </div>
  </div>
</div>

<footer>
  데이터 출처: KRX, pykrx, Yahoo Finance &nbsp;|&nbsp;
  매주 월요일 자동 업데이트 &nbsp;|&nbsp;
  <a href="https://github.com/hiorio/ETF-Dashboard" style="color:var(--acc)">GitHub</a>
</footer>

<script>
const DATA = {chart_json};
const PALETTE = {palette_json};

function drawChart(canvasId, field) {{
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const W = canvas.parentElement.clientWidth - 32;
  const H = 200;
  canvas.width  = W * dpr; canvas.height = H * dpr;
  canvas.style.width = W + 'px'; canvas.style.height = H + 'px';
  ctx.scale(dpr, dpr);

  const labels   = DATA.labels;
  const datasets = DATA.datasets;
  if (!labels.length) {{
    ctx.fillStyle = '#8892a4'; ctx.font = '13px sans-serif';
    ctx.textAlign = 'center'; ctx.fillText('데이터 없음', W/2, H/2); return;
  }}

  let allVals = [];
  datasets.forEach(ds => ds[field].forEach(v => {{ if (v !== null) allVals.push(v); }}));
  if (!allVals.length) {{
    ctx.fillStyle = '#8892a4'; ctx.font = '13px sans-serif';
    ctx.textAlign = 'center'; ctx.fillText('데이터 없음', W/2, H/2); return;
  }}
  const minV = Math.min(...allVals), maxV = Math.max(...allVals);
  const pad  = (maxV - minV) * 0.15 || 1;
  const yMin = minV - pad, yMax = maxV + pad;

  const padL = 42, padR = 12, padT = 12, padB = 36;
  const plotW = W - padL - padR, plotH = H - padT - padB;
  const xOf = i => padL + (labels.length > 1 ? i / (labels.length - 1) * plotW : plotW / 2);
  const yOf = v => padT + (1 - (v - yMin) / (yMax - yMin)) * plotH;

  ctx.fillStyle = '#1a1d27'; ctx.fillRect(0, 0, W, H);

  ctx.strokeStyle = '#2a2d3a'; ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {{
    const v = yMin + (yMax - yMin) * i / 4;
    const y = yOf(v);
    ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(W - padR, y); ctx.stroke();
    ctx.fillStyle = '#8892a4'; ctx.font = '10px sans-serif';
    ctx.textAlign = 'right'; ctx.fillText(v.toFixed(1), padL - 4, y + 3);
  }}
  if (yMin < 0 && yMax > 0) {{
    const y0 = yOf(0);
    ctx.strokeStyle = '#3a4050'; ctx.lineWidth = 1.5; ctx.setLineDash([4,4]);
    ctx.beginPath(); ctx.moveTo(padL, y0); ctx.lineTo(W - padR, y0); ctx.stroke();
    ctx.setLineDash([]);
  }}

  datasets.forEach((ds, idx) => {{
    const color = PALETTE[idx % PALETTE.length];
    const vals  = ds[field];
    ctx.strokeStyle = color; ctx.lineWidth = 2; ctx.beginPath();
    let started = false;
    vals.forEach((v, i) => {{
      if (v === null) {{ started = false; return; }}
      const x = xOf(i), y = yOf(v);
      if (!started) {{ ctx.moveTo(x, y); started = true; }} else ctx.lineTo(x, y);
    }});
    ctx.stroke();
    vals.forEach((v, i) => {{
      if (v === null) return;
      ctx.beginPath(); ctx.arc(xOf(i), yOf(v), 3, 0, Math.PI*2);
      ctx.fillStyle = color; ctx.fill();
    }});
  }});

  if (labels.length >= 2) {{
    ctx.fillStyle = '#8892a4'; ctx.font = '10px sans-serif';
    ctx.textAlign = 'left';  ctx.fillText(labels[0].slice(5), padL, H - 6);
    ctx.textAlign = 'right'; ctx.fillText(labels[labels.length-1].slice(5), W - padR, H - 6);
  }}
}}

drawChart('chart1y', 'nav_change_1y');
drawChart('chart3m', 'return_3m');
drawChart('chart1m', 'return_1m');

const legend = document.getElementById('legend');
DATA.datasets.forEach((ds, idx) => {{
  const color = PALETTE[idx % PALETTE.length];
  const item  = document.createElement('div');
  item.className = 'legend-item';
  item.innerHTML = `<span class="legend-dot" style="background:${{color}}"></span>${{ds.label}}`;
  legend.appendChild(item);
}});
</script>
</body>
</html>
"""


def main():
    DOCS_DIR.mkdir(exist_ok=True)
    rows, history = load_data()

    html = build_html(rows, history)
    OUT_PATH.write_text(html, encoding="utf-8")

    print(f"성장형 리포트 생성 완료: {OUT_PATH}")
    print(f"  ETF {len(rows)}개  이력 {len(history)}행")


if __name__ == "__main__":
    main()
