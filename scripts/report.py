"""
ETF 대시보드 HTML 리포트 생성기
실행: python3 scripts/report.py
출력: docs/index.html
"""

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / "data" / "etf.db"
DOCS_DIR = BASE_DIR / "docs"
OUT_PATH = DOCS_DIR / "index.html"


def pct(val, digits=2):
    if val is None:
        return "-"
    return f"{val:+.{digits}f}%"


def pct_plain(val, digits=2):
    """부호 없는 퍼센트"""
    if val is None:
        return "-"
    return f"{val:.{digits}f}%"


def nav_fmt(val, country):
    if val is None:
        return "-"
    if country == "KR":
        return f"{val:,.0f}원"
    return f"${val:.2f}"


def aum_fmt(val, country):
    if val is None:
        return "-"
    if country == "KR":
        # 원 단위 → 억원
        awk = val / 1e8
        if awk >= 10000:
            return f"{awk/10000:.1f}조원"
        return f"{awk:,.0f}억원"
    else:
        # USD
        if val >= 1e9:
            return f"${val/1e9:.1f}B"
        return f"${val/1e6:.0f}M"


def color_class(val):
    if val is None:
        return "neutral"
    return "pos" if val >= 0 else "neg"


def load_data():
    if not DB_PATH.exists():
        return [], [], {}

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # 최신 수집일 스냅샷
    rows = conn.execute("""
        SELECT
            w.code, w.collected_at,
            w.nav_current, w.price_prev, w.price_change, w.price_change_pct, w.aum,
            w.nav_change_1y, w.nav_change_since_listing,
            w.return_1m, w.return_3m, w.return_6m,
            w.nav_change_1m, w.nav_change_3m, w.nav_change_6m,
            w.ex_date, w.pay_date,
            w.dist_rate_12m, w.dist_rate_monthly, w.dist_rate_annualized,
            w.real_return_1y, w.nav_per_share,
            m.name, m.country, m.strategy, m.dividend_cycle, m.dividend_timing,
            m.manager, m.listed_date
        FROM etf_weekly w
        JOIN etf_meta m ON m.code = w.code
        WHERE w.collected_at = (
            SELECT MAX(collected_at) FROM etf_weekly WHERE code = w.code
        )
        ORDER BY
            CASE WHEN m.country = 'KR' THEN 0 ELSE 1 END,
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

    # 월별 분배금 (직전 13개월)
    cutoff = (datetime.now() - timedelta(days=395)).strftime("%Y-%m")
    monthly_dist_rows = conn.execute("""
        SELECT code, year_month, amount
        FROM etf_monthly_dist
        WHERE year_month >= ?
        ORDER BY code, year_month
    """, (cutoff,)).fetchall()

    # {code: {year_month: amount}}
    monthly_dists = {}
    for r in monthly_dist_rows:
        monthly_dists.setdefault(r["code"], {})[r["year_month"]] = r["amount"]

    # 상장이후 누적 분배금 합계 {code: total_amount}
    all_dists = conn.execute("""
        SELECT code, SUM(amount) as total FROM etf_monthly_dist GROUP BY code
    """).fetchall()
    total_dist_since_listing = {r["code"]: r["total"] for r in all_dists}

    conn.close()
    return [dict(r) for r in rows], [dict(r) for r in history], monthly_dists, total_dist_since_listing


def build_html(rows, history, monthly_dists, total_dist_since_listing):
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
        country = r["country"]
        country_badge = (
            '<span class="badge kr">KR</span>'
            if country == "KR"
            else '<span class="badge us">US</span>'
        )
        cycle = r.get("dividend_cycle", "-")
        timing = r.get("dividend_timing") or ""
        timing_str = f" {timing}" if timing else ""
        cycle_badge = f'<span class="badge cycle">{cycle}배당{timing_str}</span>'

        nav_disp  = nav_fmt(r["nav_current"], country)
        prev_disp = nav_fmt(r["price_prev"], country)
        change_disp = nav_fmt(r["price_change"], country) if r["price_change"] is not None else "-"
        change_pct = pct(r.get("price_change_pct"))
        change_pct_cls = color_class(r.get("price_change_pct"))
        aum_disp  = aum_fmt(r.get("aum"), country)
        listed    = r.get("listed_date") or "-"

        # 순자산가치(NAV per share) — KR ETF만
        nav_ps = r.get("nav_per_share")
        nav_ps_disp = nav_fmt(nav_ps, country) if nav_ps else "-"
        # 프리미엄/할인: (가격 - NAV) / NAV * 100
        price_now = r.get("nav_current")
        if nav_ps and price_now:
            pd_val  = round((price_now - nav_ps) / nav_ps * 100, 2)
            pd_disp = f'<span class="{color_class(pd_val)}">{pct(pd_val)}</span>'
        else:
            pd_disp = "-"

        # 상장이후 NAV 변화
        chg_since = r.get("nav_change_since_listing")
        chg_since_cls = color_class(chg_since)

        # 상장이후 누적 분배금 비율
        total_dist = total_dist_since_listing.get(code)
        listed_price = None
        if price_now and chg_since is not None:
            try:
                listed_price = price_now / (1 + chg_since / 100)
            except ZeroDivisionError:
                pass
        total_dist_rate = round(total_dist / listed_price * 100, 1) if (total_dist and listed_price) else None

        # 상장이후 총수익률 = NAV변화 + 누적분배율
        total_ret = round(chg_since + total_dist_rate, 1) if (chg_since is not None and total_dist_rate is not None) else None
        total_ret_cls = color_class(total_ret)

        # 자본침식 경보: 상장이후 NAV -5% 이하이면서 분배금은 지급
        erosion_badge = ""
        if chg_since is not None and chg_since <= -5 and total_dist_rate:
            erosion_badge = '<span class="badge erosion">⚠ 자본침식</span>'

        def td_pct(val):
            cls = color_class(val)
            return f'<td class="{cls}">{pct(val)}</td>'

        def td_pct_neutral(val):
            return f'<td class="neutral">{pct_plain(val)}</td>'

        real_val = r["real_return_1y"]
        real_cls = color_class(real_val)

        return f"""
        <tr>
          <td class="name-cell">
            <div class="etf-name">{r['name']}</div>
            <div class="etf-sub">{country_badge} {cycle_badge} {r.get('manager','')} {erosion_badge}</div>
          </td>
          <td>{nav_disp}</td>
          <td>{prev_disp}</td>
          <td class="{change_pct_cls}">{change_disp}<br><span class="sub-val">{change_pct}</span></td>
          <td>{aum_disp}</td>
          <td class="muted-cell">{listed}</td>
          {td_pct(r.get('nav_change_1m') if r.get('nav_change_1m') is not None else r.get('return_1m'))}
          {td_pct(r.get('nav_change_3m') if r.get('nav_change_3m') is not None else r.get('return_3m'))}
          {td_pct(r.get('nav_change_6m') if r.get('nav_change_6m') is not None else r.get('return_6m'))}
          {td_pct(r.get('nav_change_1y'))}
          <td class="{chg_since_cls}">{pct(chg_since)}</td>
          <td class="{total_ret_cls}"><strong>{pct(total_ret)}</strong></td>
          <td>{nav_ps_disp}<br><span class="sub-val">{pd_disp}</span></td>
          {td_pct_neutral(r.get('dist_rate_monthly'))}
          {td_pct_neutral(r.get('dist_rate_12m'))}
          {td_pct_neutral(r.get('dist_rate_annualized'))}
          <td class="muted-cell" style="font-size:0.78rem;white-space:nowrap">
            {r.get('ex_date') or '-'}<br>
            <span class="sub-val">{r.get('pay_date') or '-'}</span>
          </td>
          <td class="real-return {real_cls}"><strong>{pct(real_val)}</strong></td>
        </tr>"""

    table_rows = "\n".join(row_html(r) for r in rows) if rows else (
        '<tr><td colspan="18" class="no-data">아직 수집된 데이터가 없습니다.<br>'
        'GitHub Actions에서 워크플로우를 실행해 주세요.</td></tr>'
    )

    # ── 월별 분배금 섹션 ────────────────────────────────
    def dist_section_html():
        if not monthly_dists:
            return '<p class="muted-note">월별 분배금 데이터 없음</p>'

        # 공통 연월 레이블 (최근 13개월)
        all_months = sorted({ym for code_data in monthly_dists.values() for ym in code_data})
        if not all_months:
            return '<p class="muted-note">월별 분배금 데이터 없음</p>'

        # 테이블 헤더
        month_headers = "".join(f'<th>{int(m[5:])}월</th>' for m in all_months)

        # 각 ETF 행
        country_map = {r["code"]: r["country"] for r in rows}
        name_map    = {r["code"]: r["name"]    for r in rows}

        dist_rows = []
        for r in rows:
            code = r["code"]
            country = r.get("country", "KR")
            dmap = monthly_dists.get(code, {})
            cells = []
            for ym in all_months:
                amt = dmap.get(ym)
                if amt is None:
                    cells.append('<td class="neutral">-</td>')
                else:
                    if country == "KR":
                        cells.append(f'<td class="dist-amt">{amt:,.0f}원</td>')
                    else:
                        cells.append(f'<td class="dist-amt">${amt:.4f}</td>')
            dist_rows.append(
                f'<tr><td class="name-cell-sm">{name_map.get(code, code)}</td>{"".join(cells)}</tr>'
            )

        dist_rows_html = "\n".join(dist_rows)
        return f"""
        <div class="table-wrap">
          <table class="dist-table">
            <thead>
              <tr>
                <th style="text-align:left">ETF</th>
                {month_headers}
              </tr>
            </thead>
            <tbody>
              {dist_rows_html}
            </tbody>
          </table>
        </div>
        """

    dist_section = dist_section_html()

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
  .container {{ max-width: 1200px; margin: 0 auto; padding: 20px 16px; }}
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
    padding: 10px 12px;
    text-align: right;
    font-weight: 600;
    color: var(--muted);
    font-size: 0.78rem;
    white-space: nowrap;
  }}
  th:first-child {{ text-align: left; }}
  td {{
    padding: 11px 12px;
    text-align: right;
    border-top: 1px solid var(--border);
    white-space: nowrap;
    font-size: 0.85rem;
  }}
  td:first-child {{ text-align: left; }}
  tr:hover td {{ background: rgba(79,142,247,0.04); }}
  .pos {{ color: var(--pos); }}
  .neg {{ color: var(--neg); }}
  .neutral {{ color: var(--text); }}
  .muted-cell {{ color: var(--muted); font-size: 0.8rem; }}
  .sub-val {{ font-size: 0.75rem; opacity: 0.85; }}
  .th-sub {{ font-size: 0.68rem; font-weight: 400; opacity: 0.65; }}
  .real-return {{ font-size: 0.95rem; }}
  .real-return.pos {{ color: #22c55e; }}
  .real-return.neg {{ color: #ef4444; }}
  .etf-name {{ font-weight: 500; font-size: 0.88rem; }}
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
  .badge.erosion {{ background: #3b1a1a; color: #fca5a5; }}
  .no-data {{
    text-align: center !important;
    padding: 40px !important;
    color: var(--muted);
    line-height: 2;
  }}
  /* ── 월별 분배금 테이블 ── */
  .dist-table th, .dist-table td {{ padding: 8px 10px; font-size: 0.8rem; }}
  .dist-amt {{ color: #fbbf24; }}
  .name-cell-sm {{ text-align: left; font-size: 0.8rem; color: var(--text); max-width: 220px; white-space: normal; }}
  .muted-note {{ color: var(--muted); font-size: 0.85rem; padding: 12px 0; }}
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
  <nav style="display:flex;justify-content:center;gap:12px;margin-top:14px;">
    <a href="index.html" style="color:var(--acc);text-decoration:none;font-size:0.82rem;padding:4px 12px;border:1px solid var(--acc);border-radius:6px;">📊 ETF 대시보드</a>
    <a href="dram.html" style="color:var(--muted);text-decoration:none;font-size:0.82rem;padding:4px 12px;border:1px solid var(--border);border-radius:6px;">🖥️ D램 가격</a>
  </nav>
</div>

<div class="container">

  <div class="section-title">📋 최신 지표 스냅샷 (실질수익률 기준 정렬)</div>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>ETF</th>
          <th>현재가<br><span class="th-sub">종가</span></th>
          <th>전일가<br><span class="th-sub">직전거래일</span></th>
          <th>등락<br><span class="th-sub">전일대비</span></th>
          <th>시가총액<br><span class="th-sub">운용규모</span></th>
          <th>상장일<br><span class="th-sub">설정일</span></th>
          <th>NAV 변화율 1M<br><span class="th-sub">최근 1개월</span></th>
          <th>NAV 변화율 3M<br><span class="th-sub">최근 3개월</span></th>
          <th>NAV 변화율 6M<br><span class="th-sub">최근 6개월</span></th>
          <th>NAV 변화율 1Y<br><span class="th-sub">최근 1년</span></th>
          <th>상장이후 NAV<br><span class="th-sub">상장가 기준</span></th>
          <th>상장이후 총수익률 ★★<br><span class="th-sub">NAV변화+누적분배</span></th>
          <th>순자산가치<br><span class="th-sub">프리미엄/할인</span></th>
          <th>월분배율<br><span class="th-sub">최근 1회</span></th>
          <th>분배율 12M<br><span class="th-sub">최근 12개월</span></th>
          <th>분배율 연환산<br><span class="th-sub">최근분배×횟수</span></th>
          <th>배당 일정<br><span class="th-sub">배당락일 / 지급일</span></th>
          <th>실질수익률 1Y ★<br><span class="th-sub">분배율+1Y수익</span></th>
        </tr>
      </thead>
      <tbody>
        {table_rows}
      </tbody>
    </table>
  </div>
  <p style="color:var(--muted);font-size:0.75rem;margin-top:8px;">
    ★ 실질수익률 1Y = 분배율 12M + 1Y 수익률 &nbsp;|&nbsp;
    ★★ 상장이후 총수익률 = 상장이후 NAV변화 + 상장이후 누적분배율 &nbsp;|&nbsp;
    ⚠ 자본침식 = 상장이후 NAV -5% 이하이면서 분배금 지급 중
  </p>

  <div class="section-title">💰 월별 분배금 이력 (직전 1년)</div>
  {dist_section}

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
  데이터 출처: KRX, pykrx, Yahoo Finance &nbsp;|&nbsp;
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
    rows, history, monthly_dists, total_dist_since_listing = load_data()

    html = build_html(rows, history, monthly_dists, total_dist_since_listing)
    OUT_PATH.write_text(html, encoding="utf-8")

    print(f"리포트 생성 완료: {OUT_PATH}")
    print(f"  ETF {len(rows)}개  이력 {len(history)}행  월별분배금 {sum(len(v) for v in monthly_dists.values())}건")


if __name__ == "__main__":
    main()
