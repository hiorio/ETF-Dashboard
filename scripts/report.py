"""
ETF 대시보드 HTML 리포트 생성기
실행: python3 scripts/report.py
출력: docs/index.html
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / "data" / "etf.db"
DOCS_DIR = BASE_DIR / "docs"
OUT_PATH = DOCS_DIR / "index.html"


def pct(val, digits=2):
    if val is None:
        return "-"
    return f"{val:+.{digits}f}%"


def nav_fmt(val, country):
    if val is None:
        return "-"
    if country == "KR":
        return f"{val:,.0f}원"
    return f"${val:.2f}"


def color_class(val):
    if val is None:
        return "neutral"
    return "pos" if val >= 0 else "neg"


def load_data():
    if not DB_PATH.exists():
        return [], []

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # 최신 수집일 스냅샷
    rows = conn.execute("""
        SELECT
            w.code, w.collected_at,
            w.nav_current, w.nav_change_1y, w.nav_change_since_listing,
            w.dist_rate_12m, w.dist_rate_annualized, w.real_return_1y,
            m.name, m.country, m.strategy, m.dividend_cycle, m.dividend_timing, m.manager, m.listed_date
        FROM etf_weekly w
        JOIN etf_meta m ON m.code = w.code
        WHERE w.collected_at = (
            SELECT MAX(collected_at) FROM etf_weekly WHERE code = w.code
        )
        ORDER BY
            CASE WHEN w.real_return_1y IS NULL THEN 1 ELSE 0 END,
            w.real_return_1y DESC
    """).fetchall()

    # NAV 변화율 이력 (최근 12주)
    history = conn.execute("""
        SELECT code, collected_at, nav_change_1y, dist_rate_12m, real_return_1y
        FROM etf_weekly
        WHERE collected_at >= date('now', '-84 days')
        ORDER BY code, collected_at
    """).fetchall()

    conn.close()
    return [dict(r) for r in rows], [dict(r) for r in history]


def build_html(rows, history):
    updated = rows[0]["collected_at"] if rows else "데이터 없음"
    now = datetime.now().strftime("%Y-%m-%d %H:%M KST")

    # 차트용 JSON 데이터
    chart_labels = sorted({r["collected_at"] for r in history})
    codes = [r["code"] for r in rows]
    names = {r["code"]: r["name"] for r in rows}

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
                "dist_rate_12m": series(code, "dist_rate_12m"),
                "real_return_1y": series(code, "real_return_1y"),
            }
            for code in codes
        ],
    }

    # 팔레트 (6색)
    PALETTE = ["#4f8ef7", "#f7874f", "#4fc98e", "#f74f7e", "#b04ff7", "#f7c94f"]

    # ── 테이블 행 ──────────────────────────────────────
    def row_html(r):
        code = r["code"]
        country_badge = (
            '<span class="badge kr">KR</span>'
            if r["country"] == "KR"
            else '<span class="badge us">US</span>'
        )
        cycle = r.get("dividend_cycle", "-")
        timing = r.get("dividend_timing") or ""
        timing_str = f" {timing}" if timing else ""
        cycle_badge = f'<span class="badge cycle">{cycle}배당{timing_str}</span>'
        nav_disp = nav_fmt(r["nav_current"], r["country"])

        def td(val, cls_fn=color_class):
            v = pct(val)
            cls = cls_fn(val)
            return f'<td class="{cls}">{v}</td>'

        real_val = r["real_return_1y"]
        real_cls = color_class(real_val)
        real_disp = pct(real_val)

        return f"""
        <tr>
          <td class="name-cell">
            <div class="etf-name">{r['name']}</div>
            <div class="etf-sub">{country_badge} {cycle_badge} {r.get('manager','')}</div>
          </td>
          <td>{nav_disp}</td>
          {td(r['nav_change_1y'])}
          {td(r['nav_change_since_listing'])}
          {td(r['dist_rate_12m'], lambda v: 'neutral')}
          {td(r['dist_rate_annualized'], lambda v: 'neutral')}
          <td class="real-return {real_cls}"><strong>{real_disp}</strong></td>
        </tr>"""

    table_rows = "\n".join(row_html(r) for r in rows) if rows else (
        '<tr><td colspan="7" class="no-data">아직 수집된 데이터가 없습니다.<br>'
        'GitHub Actions에서 워크플로우를 실행해 주세요.</td></tr>'
    )

    chart_json = json.dumps(chart_data, ensure_ascii=False)

    # ── 완성 HTML ──────────────────────────────────────
    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>배당 ETF 대시보드</title>
<style>
  :root {{
    --bg: #0f1117;
    --card: #1a1d27;
    --border: #2a2d3a;
    --text: #e2e8f0;
    --muted: #8892a4;
    --pos: #4ade80;
    --neg: #f87171;
    --acc: #4f8ef7;
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
  .header p {{ color: var(--muted); margin-top: 4px; font-size: 0.85rem; }}
  .container {{ max-width: 1100px; margin: 0 auto; padding: 20px 16px; }}
  .section-title {{
    font-size: 0.9rem;
    font-weight: 600;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin: 28px 0 12px;
  }}
  /* ── 테이블 ── */
  .table-wrap {{ overflow-x: auto; -webkit-overflow-scrolling: touch; }}
  table {{
    width: 100%;
    border-collapse: collapse;
    background: var(--card);
    border-radius: 10px;
    overflow: hidden;
  }}
  thead tr {{ background: #21253a; }}
  th {{
    padding: 10px 14px;
    text-align: right;
    font-weight: 600;
    color: var(--muted);
    font-size: 0.8rem;
    white-space: nowrap;
  }}
  th:first-child {{ text-align: left; }}
  td {{
    padding: 12px 14px;
    text-align: right;
    border-top: 1px solid var(--border);
    white-space: nowrap;
  }}
  td:first-child {{ text-align: left; }}
  tr:hover td {{ background: rgba(79,142,247,0.04); }}
  .pos {{ color: var(--pos); }}
  .neg {{ color: var(--neg); }}
  .neutral {{ color: var(--text); }}
  .real-return {{ font-size: 1rem; }}
  .real-return.pos {{ color: #22c55e; }}
  .real-return.neg {{ color: #ef4444; }}
  .etf-name {{ font-weight: 500; font-size: 0.9rem; }}
  .etf-sub {{ margin-top: 3px; display: flex; gap: 4px; align-items: center; flex-wrap: wrap; }}
  .badge {{
    display: inline-block;
    padding: 1px 6px;
    border-radius: 4px;
    font-size: 0.7rem;
    font-weight: 600;
  }}
  .badge.kr {{ background: #1e3a5f; color: #60a5fa; }}
  .badge.us {{ background: #3b1f1f; color: #f87171; }}
  .badge.cycle {{ background: #1f2d1f; color: #86efac; }}
  .no-data {{
    text-align: center !important;
    padding: 40px !important;
    color: var(--muted);
    line-height: 2;
  }}
  /* ── 차트 ── */
  .charts {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
  @media (max-width: 680px) {{ .charts {{ grid-template-columns: 1fr; }} }}
  .chart-card {{
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 16px;
  }}
  .chart-card h3 {{ font-size: 0.85rem; color: var(--muted); margin-bottom: 12px; }}
  canvas {{ width: 100% !important; }}
  /* ── 범례 ── */
  .legend {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 10px; }}
  .legend-item {{ display: flex; align-items: center; gap: 5px; font-size: 0.75rem; color: var(--muted); }}
  .legend-dot {{ width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }}
  /* ── 푸터 ── */
  footer {{
    text-align: center;
    color: var(--muted);
    font-size: 0.75rem;
    padding: 24px 16px;
    border-top: 1px solid var(--border);
    margin-top: 32px;
  }}
</style>
</head>
<body>

<div class="header">
  <h1>📊 배당 ETF 대시보드</h1>
  <p>커버드콜 ETF 옥석 가리기 · 최종 수집: {updated} · 생성: {now}</p>
</div>

<div class="container">

  <div class="section-title">📋 최신 지표 스냅샷 (실질수익률 기준 정렬)</div>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>ETF</th>
          <th>NAV 현재</th>
          <th>NAV 변화율 1Y</th>
          <th>NAV 변화율 상장이후</th>
          <th>분배율 12M</th>
          <th>분배율 연환산</th>
          <th>실질수익률 1Y ★</th>
        </tr>
      </thead>
      <tbody>
        {table_rows}
      </tbody>
    </table>
  </div>
  <p style="color:var(--muted);font-size:0.75rem;margin-top:8px;">
    ★ 실질수익률 1Y = 분배율 12M + NAV 변화율 1Y
  </p>

  <div class="section-title">📈 추이 차트 (최근 12주)</div>
  <div class="charts">
    <div class="chart-card">
      <h3>실질수익률 1Y (%)</h3>
      <canvas id="chartReal" height="200"></canvas>
    </div>
    <div class="chart-card">
      <h3>분배율 12M (%)</h3>
      <canvas id="chartDist" height="200"></canvas>
    </div>
    <div class="chart-card">
      <h3>NAV 변화율 1Y (%)</h3>
      <canvas id="chartNav" height="200"></canvas>
    </div>
    <div class="chart-card" style="display:flex;flex-direction:column;justify-content:center;align-items:center;padding:24px;">
      <h3 style="margin-bottom:16px;">범례</h3>
      <div class="legend" id="legend"></div>
    </div>
  </div>

</div>

<footer>
  데이터 출처: KRX, pykrx, yfinance &nbsp;|&nbsp;
  매주 월요일 자동 업데이트 &nbsp;|&nbsp;
  <a href="https://github.com/hiorio/ETF-Dashboard" style="color:var(--acc)">GitHub</a>
</footer>

<script>
const DATA = {chart_json};

const PALETTE = {json.dumps(PALETTE)};

function drawChart(canvasId, field, yLabel) {{
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const W = canvas.parentElement.clientWidth - 32;
  const H = 200;
  canvas.width = W * dpr;
  canvas.height = H * dpr;
  canvas.style.width = W + 'px';
  canvas.style.height = H + 'px';
  ctx.scale(dpr, dpr);

  const labels = DATA.labels;
  const datasets = DATA.datasets;
  if (!labels.length) {{
    ctx.fillStyle = '#8892a4';
    ctx.font = '13px sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText('데이터 없음', W/2, H/2);
    return;
  }}

  // 값 범위 계산
  let allVals = [];
  datasets.forEach(ds => ds[field].forEach(v => {{ if (v !== null) allVals.push(v); }}));
  if (!allVals.length) {{
    ctx.fillStyle = '#8892a4';
    ctx.font = '13px sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText('데이터 없음', W/2, H/2);
    return;
  }}
  const minV = Math.min(...allVals);
  const maxV = Math.max(...allVals);
  const pad = (maxV - minV) * 0.15 || 1;
  const yMin = minV - pad;
  const yMax = maxV + pad;

  const padL = 42, padR = 12, padT = 12, padB = 36;
  const plotW = W - padL - padR;
  const plotH = H - padT - padB;

  const xOf = i => padL + (labels.length > 1 ? i / (labels.length - 1) * plotW : plotW / 2);
  const yOf = v => padT + (1 - (v - yMin) / (yMax - yMin)) * plotH;

  // 배경
  ctx.fillStyle = '#1a1d27';
  ctx.fillRect(0, 0, W, H);

  // 그리드 + y축 레이블
  ctx.strokeStyle = '#2a2d3a';
  ctx.lineWidth = 1;
  const ticks = 4;
  for (let i = 0; i <= ticks; i++) {{
    const v = yMin + (yMax - yMin) * i / ticks;
    const y = yOf(v);
    ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(W - padR, y); ctx.stroke();
    ctx.fillStyle = '#8892a4';
    ctx.font = '10px sans-serif';
    ctx.textAlign = 'right';
    ctx.fillText(v.toFixed(1), padL - 4, y + 3);
  }}

  // 제로선
  if (yMin < 0 && yMax > 0) {{
    const y0 = yOf(0);
    ctx.strokeStyle = '#3a4050';
    ctx.lineWidth = 1.5;
    ctx.setLineDash([4, 4]);
    ctx.beginPath(); ctx.moveTo(padL, y0); ctx.lineTo(W - padR, y0); ctx.stroke();
    ctx.setLineDash([]);
  }}

  // 데이터 선
  datasets.forEach((ds, idx) => {{
    const color = PALETTE[idx % PALETTE.length];
    const vals = ds[field];
    ctx.strokeStyle = color;
    ctx.lineWidth = 2;
    ctx.beginPath();
    let started = false;
    vals.forEach((v, i) => {{
      if (v === null) {{ started = false; return; }}
      const x = xOf(i), y = yOf(v);
      if (!started) {{ ctx.moveTo(x, y); started = true; }}
      else ctx.lineTo(x, y);
    }});
    ctx.stroke();
    // 점
    vals.forEach((v, i) => {{
      if (v === null) return;
      ctx.beginPath();
      ctx.arc(xOf(i), yOf(v), 3, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();
    }});
  }});

  // x축 레이블 (첫/마지막만)
  if (labels.length >= 2) {{
    ctx.fillStyle = '#8892a4';
    ctx.font = '10px sans-serif';
    ctx.textAlign = 'left';
    ctx.fillText(labels[0].slice(5), padL, H - 6);
    ctx.textAlign = 'right';
    ctx.fillText(labels[labels.length-1].slice(5), W - padR, H - 6);
  }}
}}

drawChart('chartReal', 'real_return_1y');
drawChart('chartDist', 'dist_rate_12m');
drawChart('chartNav', 'nav_change_1y');

// 범례
const legend = document.getElementById('legend');
DATA.datasets.forEach((ds, idx) => {{
  const color = PALETTE[idx % PALETTE.length];
  const item = document.createElement('div');
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

    print(f"리포트 생성 완료: {OUT_PATH}")
    print(f"  ETF {len(rows)}개  이력 {len(history)}행")


if __name__ == "__main__":
    main()
