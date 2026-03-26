-- ============================================================
--  superset/dashboard_queries.sql
--  Paste each query as a Chart in Apache Superset
--  Dataset: etf_kpis (Postgres)
-- ============================================================


-- ── 1. Latest close price per ETF ─────────────────────────────────────────
-- Chart type: Big Number / Table
SELECT
    ticker,
    close                   AS latest_close,
    daily_return_pct        AS day_change_pct,
    ma_signal
FROM etf_kpis
WHERE date = (SELECT MAX(date) FROM etf_kpis)
ORDER BY ticker;


-- ── 2. Price history (line chart — close + MA50 + MA200) ──────────────────
-- Chart type: Line Chart (multi-series)
-- Filters: ticker (dropdown), date range
SELECT
    date,
    ticker,
    close,
    ma_50,
    ma_200
FROM etf_kpis
WHERE date >= CURRENT_DATE - INTERVAL '1 year'
ORDER BY ticker, date;


-- ── 3. Daily return heatmap (all tickers × last 30 days) ──────────────────
-- Chart type: Heatmap  (x=date, y=ticker, color=daily_return_pct)
SELECT
    date,
    ticker,
    daily_return_pct
FROM etf_kpis
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY date, ticker;


-- ── 4. Volume spike alert table ───────────────────────────────────────────
-- Chart type: Table with conditional formatting
SELECT
    date,
    ticker,
    volume,
    avg_volume_30d,
    ROUND(volume::NUMERIC / NULLIF(avg_volume_30d, 0), 2) AS volume_ratio,
    volume_spike
FROM etf_kpis
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
  AND volume_spike = TRUE
ORDER BY date DESC, volume_ratio DESC;


-- ── 5. Monthly return % by ETF (bar chart) ────────────────────────────────
-- Chart type: Bar Chart (grouped or sorted)
SELECT
    ticker,
    monthly_return_pct
FROM etf_kpis
WHERE date = (SELECT MAX(date) FROM etf_kpis)
ORDER BY monthly_return_pct DESC;


-- ── 6. Intraday range % over time (volatility proxy) ─────────────────────
-- Chart type: Area Chart
SELECT
    date,
    ticker,
    high_low_range_pct
FROM etf_kpis
WHERE date >= CURRENT_DATE - INTERVAL '90 days'
ORDER BY ticker, date;


-- ── 7. MA signal summary (Golden / Death Cross) ───────────────────────────
-- Chart type: Pie or grouped bar
SELECT
    ticker,
    ma_signal,
    close,
    ma_50,
    ma_200
FROM etf_kpis
WHERE date = (SELECT MAX(date) FROM etf_kpis)
ORDER BY ticker;
