"""E1 — Phase D Diagnostic (Kill).

Answers two questions:
  1. Where does the alpha come from — which sectors, which time periods?
  2. Are there sub-strategies that would have cleared the gate?
     (All variations documented as POST-HOC observations.
     None are reformulations. Same discipline as FX lab S4/S6.)

Run: python diagnostic.py
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
LAB_DIR   = Path(__file__).parent
DATA_DIR  = LAB_DIR / 'data'
CACHE_DIR = DATA_DIR / 'cache'
RES_DIR   = DATA_DIR / 'results'
RES_DIR.mkdir(exist_ok=True)

MIN_PRICE  = 5.0
QUINTILE   = 0.20
SPREAD_OW  = 0.0002

STRESS_MONTHS = ['2011-08', '2015-08', '2018-02', '2020-03', '2022-01']

POSTHOC_DISCLAIMER = (
    "POST-HOC OBSERVATION — discovered after Phase C results. "
    "Not a reformulation. Documented for research only per FX lab discipline "
    "(same rule as S4 reversed direction +0.57, S6 short-leg +0.36)."
)


# ── Data helpers ──────────────────────────────────────────────────────────────

def load_prices() -> dict[str, pd.Series]:
    prices = {}
    for p in CACHE_DIR.glob('price_*.parquet'):
        tkr = p.stem.replace('price_', '')
        try:
            s = pd.read_parquet(p)['close'].dropna()
            if not s.empty:
                prices[tkr] = s
        except Exception:
            continue
    return prices


def load_spy() -> pd.Series:
    df = pd.read_parquet(CACHE_DIR / 'spy_monthly.parquet')
    ret = df['ret'].dropna()
    ret.index = pd.to_datetime(ret.index)
    return ret


def load_sectors() -> dict[str, str]:
    """Return {ticker: GICS sector} from cached Wikipedia table."""
    p = CACHE_DIR / 'sp500_constituents.parquet'
    if not p.exists():
        return {}
    df = pd.read_parquet(p)
    col = next((c for c in df.columns if 'Sector' in c or 'GICS' in c), None)
    sym_col = next((c for c in df.columns if 'Symbol' in c), None)
    if col is None or sym_col is None:
        return {}
    return dict(zip(
        df[sym_col].str.replace('.', '-', regex=False),
        df[col]
    ))


def load_phase_c() -> pd.DataFrame:
    """Load most recent Phase C monthly CSV."""
    files = sorted(RES_DIR.glob('e1_c_monthly_*.csv'))
    if not files:
        raise FileNotFoundError("No Phase C results found — run backtest.py first")
    df = pd.read_csv(files[-1], parse_dates=['rebal_date', 'hold_date'])
    return df


def _get_price(s: pd.Series, d: pd.Timestamp) -> float | None:
    try:
        return float(s.loc[d])
    except KeyError:
        return None


# ── Portfolio reconstruction ───────────────────────────────────────────────────

def reconstruct_portfolios(
    prices: dict[str, pd.Series],
    lookback_skip: int = 1,
    lookback_total: int = 13,
    quintile: float = QUINTILE,
    sector_neutral: bool = False,
    sectors: dict[str, str] | None = None,
    rebal_freq: str = 'monthly',   # 'monthly' or 'quarterly'
) -> dict[pd.Timestamp, set[str]]:
    """Reconstruct portfolio at each rebalance date.

    Returns dict[hold_date -> set of tickers in portfolio].
    Used for sector attribution and post-hoc variation analysis.
    """
    all_dates = sorted({d for s in prices.values() for d in s.index})
    start = pd.Timestamp('2010-02-28')

    # Quarterly: only rebalance in Feb, May, Aug, Nov (last month of each quarter)
    quarterly_months = {2, 5, 8, 11}

    portfolios: dict[pd.Timestamp, set[str]] = {}

    for rebal_date in all_dates:
        if rebal_date < start:
            continue
        m = rebal_date.to_period('M')
        if rebal_freq == 'quarterly' and m.month not in quarterly_months:
            continue

        d_recent = (m - lookback_skip).to_timestamp('M')
        d_far    = (m - lookback_total).to_timestamp('M')
        d_next   = (m + 1).to_timestamp('M')

        if d_next not in {d for s in prices.values() for d in s.index}:
            continue

        # Compute momentum
        mom_scores: dict[str, float] = {}
        for tkr, s in prices.items():
            p_r = _get_price(s, d_recent)
            p_f = _get_price(s, d_far)
            p_c = _get_price(s, rebal_date)
            if not (p_r and p_f and p_c) or p_f <= 0 or p_c < MIN_PRICE:
                continue
            mom_scores[tkr] = p_r / p_f - 1

        if len(mom_scores) < 20:
            continue

        # Select portfolio
        if sector_neutral and sectors:
            # Rank within each GICS sector, pick top quintile per sector
            by_sector: dict[str, list] = {}
            for tkr, mom in mom_scores.items():
                sec = sectors.get(tkr, 'Unknown')
                by_sector.setdefault(sec, []).append((tkr, mom))
            selected: set[str] = set()
            for sec_stocks in by_sector.values():
                ranked = sorted(sec_stocks, key=lambda x: x[1], reverse=True)
                n = max(1, int(len(ranked) * quintile))
                selected.update(t for t, _ in ranked[:n])
        else:
            ranked = sorted(mom_scores.items(), key=lambda x: x[1], reverse=True)
            n = max(1, int(len(ranked) * quintile))
            selected = set(t for t, _ in ranked[:n])

        portfolios[d_next] = selected

    return portfolios


# ── D1: Regime breakdown ──────────────────────────────────────────────────────

def d1_regime_breakdown(df: pd.DataFrame) -> dict:
    """Excess Sharpe by year and multi-year regime."""
    df = df.copy()
    df['year'] = df['hold_date'].dt.year

    regimes = [
        ('2010–2013', '2010-01', '2013-12'),
        ('2014–2016', '2014-01', '2016-12'),
        ('2017–2019', '2017-01', '2019-12'),
        ('2020–2022', '2020-01', '2022-12'),
        ('2023–2026', '2023-01', '2026-12'),
    ]

    # Year-by-year
    by_year = {}
    for yr, sub in df.groupby('year'):
        excess = sub['excess_ret_1x']
        net    = sub['net_ret_1x']
        spy    = sub['spy_ret']
        ir_    = excess.mean() * 12 / (excess.std() * np.sqrt(12)) \
                 if excess.std() > 0 else np.nan
        by_year[int(yr)] = {
            'n': len(sub),
            'strat_sharpe': round(net.mean() * 12 / (net.std() * np.sqrt(12)), 3)
                            if net.std() > 0 else np.nan,
            'spy_sharpe':   round(spy.mean() * 12 / (spy.std() * np.sqrt(12)), 3)
                            if spy.std() > 0 else np.nan,
            'excess_sharpe': round(ir_, 3),
            'cum_excess_pct': round(float((1 + excess).prod() - 1) * 100, 1),
        }

    # Multi-year regimes
    by_regime = {}
    df['ym'] = df['hold_date'].dt.to_period('M').astype(str)
    for label, start, end in regimes:
        sub = df[(df['ym'] >= start) & (df['ym'] <= end)]
        if sub.empty:
            continue
        excess = sub['excess_ret_1x']
        net    = sub['net_ret_1x']
        spy    = sub['spy_ret']
        ir_    = excess.mean() * 12 / (excess.std() * np.sqrt(12)) \
                 if excess.std() > 0 else np.nan
        by_regime[label] = {
            'n': len(sub),
            'strat_sharpe':  round(net.mean() * 12 / (net.std() * np.sqrt(12)), 3)
                             if net.std() > 0 else np.nan,
            'spy_sharpe':    round(spy.mean() * 12 / (spy.std() * np.sqrt(12)), 3)
                             if spy.std() > 0 else np.nan,
            'excess_sharpe': round(ir_, 3),
            'cum_excess_pct': round(float((1 + excess).prod() - 1) * 100, 1),
        }

    return {'by_year': by_year, 'by_regime': by_regime}


# ── D2: Sector attribution ────────────────────────────────────────────────────

def d2_sector_attribution(
    portfolios: dict[pd.Timestamp, set[str]],
    prices: dict[str, pd.Series],
    spy_ret: pd.Series,
    sectors: dict[str, str],
    df_c: pd.DataFrame,
) -> dict:
    """Attribute excess return to GICS sectors.

    For each month: compute per-sector contribution to excess return.
    Aggregate over full period and by regime.
    """
    sector_rows = []

    df_idx = df_c.set_index('hold_date')

    for hold_date, portfolio in portfolios.items():
        if hold_date not in df_idx.index:
            continue
        row = df_idx.loc[hold_date]
        spy_month = row['spy_ret'] if not pd.isna(row['spy_ret']) else 0.0
        m = hold_date.to_period('M')
        rebal_date = (m - 1).to_timestamp('M')

        # Compute per-stock returns
        stock_rets: dict[str, float] = {}
        for tkr in portfolio:
            s = prices.get(tkr)
            if s is None:
                continue
            p_e = _get_price(s, rebal_date)
            p_x = _get_price(s, hold_date)
            if p_e and p_x and p_e > 0:
                stock_rets[tkr] = p_x / p_e - 1

        if not stock_rets:
            continue

        n_stocks = len(stock_rets)

        # Group by sector
        sec_returns: dict[str, list] = {}
        for tkr, ret in stock_rets.items():
            sec = sectors.get(tkr, 'Unknown')
            sec_returns.setdefault(sec, []).append(ret)

        for sec, rets in sec_returns.items():
            sec_mean_ret = float(np.mean(rets))
            sec_weight   = len(rets) / n_stocks
            sec_contrib  = sec_weight * (sec_mean_ret - spy_month)
            sector_rows.append({
                'hold_date':  hold_date,
                'sector':     sec,
                'weight':     sec_weight,
                'sec_ret':    sec_mean_ret,
                'spy_ret':    spy_month,
                'contrib':    sec_contrib,
                'n_stocks':   len(rets),
            })

    if not sector_rows:
        return {}

    sec_df = pd.DataFrame(sector_rows)

    # Aggregate: average weight and total contribution by sector
    agg = (
        sec_df.groupby('sector')
        .agg(
            avg_weight    = ('weight', 'mean'),
            total_contrib = ('contrib', 'sum'),
            avg_sec_ret   = ('sec_ret', 'mean'),
            n_months      = ('hold_date', 'count'),
        )
        .sort_values('total_contrib', ascending=False)
        .reset_index()
    )

    # S&P 500 equal-weight sector distribution (benchmark)
    sp500_sector_counts = pd.Series(sectors).value_counts()
    sp500_sector_wt = (sp500_sector_counts / sp500_sector_counts.sum()).to_dict()

    result = {}
    for _, row in agg.iterrows():
        sec = row['sector']
        result[sec] = {
            'avg_portfolio_weight': round(float(row['avg_weight']), 4),
            'sp500_equal_weight':   round(sp500_sector_wt.get(sec, 0), 4),
            'overweight':           round(float(row['avg_weight'])
                                         - sp500_sector_wt.get(sec, 0), 4),
            'total_contrib_pct':    round(float(row['total_contrib']) * 100, 2),
            'avg_monthly_ret_pct':  round(float(row['avg_sec_ret']) * 100, 3),
            'months_represented':   int(row['n_months']),
        }

    return result


# ── D3: Post-hoc variation analysis ──────────────────────────────────────────

def _compute_excess_sharpe(
    portfolios: dict[pd.Timestamp, set[str]],
    prices: dict[str, pd.Series],
    spy_ret: pd.Series,
    df_c: pd.DataFrame,
) -> dict:
    """Compute IR and excess Sharpe for a given portfolio dict."""
    df_idx = df_c.set_index('hold_date')
    rows = []
    for hold_date, portfolio in portfolios.items():
        if hold_date not in df_idx.index:
            continue
        m = hold_date.to_period('M')
        rebal_date = (m - 1).to_timestamp('M')
        rets = []
        for tkr in portfolio:
            s = prices.get(tkr)
            if s is None:
                continue
            pe = _get_price(s, rebal_date)
            px = _get_price(s, hold_date)
            if pe and px and pe > 0:
                rets.append(px / pe - 1)
        if not rets:
            continue
        gross = float(np.mean(rets))
        try:
            spy_m = float(spy_ret.loc[hold_date])
        except KeyError:
            continue
        rows.append({'net_ret': gross, 'spy_ret': spy_m,
                     'excess': gross - spy_m})

    if not rows:
        return {'excess_sharpe': np.nan, 'ir': np.nan, 'n': 0}

    df = pd.DataFrame(rows)
    net = df['net_ret']
    spy = df['spy_ret']
    exc = df['excess']

    sharpe     = net.mean() * 12 / (net.std() * np.sqrt(12)) \
                 if net.std() > 0 else np.nan
    spy_sharpe = spy.mean() * 12 / (spy.std() * np.sqrt(12)) \
                 if spy.std() > 0 else np.nan
    ir         = exc.mean() * 12 / (exc.std() * np.sqrt(12)) \
                 if exc.std() > 0 else np.nan

    return {
        'sharpe':        round(sharpe, 3),
        'spy_sharpe':    round(spy_sharpe, 3),
        'excess_sharpe': round(sharpe - spy_sharpe, 3),
        'ir':            round(ir, 3),
        'n':             len(df),
    }


def d3_posthoc_variations(
    prices: dict[str, pd.Series],
    spy_ret: pd.Series,
    sectors: dict[str, str],
    df_c: pd.DataFrame,
) -> dict:
    """Post-hoc variation table. All results are research observations only.

    DISCIPLINE: None of these clear the gate in an independent out-of-sample
    test. They are observed on the same data that generated the Phase C result.
    Pursuing any of them would reset the validation cycle.
    """
    variations = {}
    baseline_ports = reconstruct_portfolios(prices)

    print("    Computing baseline...")
    baseline = _compute_excess_sharpe(baseline_ports, prices, spy_ret, df_c)
    variations['baseline (12-1, top 20%)'] = {**baseline, 'note': 'Phase C result'}

    print("    Computing sector-neutral...")
    sn_ports = reconstruct_portfolios(prices, sector_neutral=True, sectors=sectors)
    sn = _compute_excess_sharpe(sn_ports, prices, spy_ret, df_c)
    variations['sector-neutral (12-1, top 20% per sector)'] = {
        **sn, 'note': POSTHOC_DISCLAIMER
    }

    print("    Computing top 10% (decile)...")
    top10_ports = reconstruct_portfolios(prices, quintile=0.10)
    top10 = _compute_excess_sharpe(top10_ports, prices, spy_ret, df_c)
    variations['top 10% (12-1, decile)'] = {**top10, 'note': POSTHOC_DISCLAIMER}

    print("    Computing 6-1 month lookback...")
    mom6_ports = reconstruct_portfolios(
        prices, lookback_skip=1, lookback_total=7
    )
    mom6 = _compute_excess_sharpe(mom6_ports, prices, spy_ret, df_c)
    variations['6-1 month lookback (top 20%)'] = {
        **mom6, 'note': POSTHOC_DISCLAIMER
    }

    print("    Computing quarterly rebalancing...")
    q_ports = reconstruct_portfolios(prices, rebal_freq='quarterly')
    quarterly = _compute_excess_sharpe(q_ports, prices, spy_ret, df_c)
    variations['quarterly rebalancing (12-1, top 20%)'] = {
        **quarterly, 'note': POSTHOC_DISCLAIMER
    }

    print("    Computing sector-neutral top 10%...")
    sn10_ports = reconstruct_portfolios(
        prices, quintile=0.10, sector_neutral=True, sectors=sectors
    )
    sn10 = _compute_excess_sharpe(sn10_ports, prices, spy_ret, df_c)
    variations['sector-neutral top 10%'] = {**sn10, 'note': POSTHOC_DISCLAIMER}

    return variations


# ── D4: Monthly distribution ──────────────────────────────────────────────────

def d4_distribution(df: pd.DataFrame) -> dict:
    exc = df['excess_ret_1x']
    net = df['net_ret_1x']
    return {
        'excess_mean_monthly_pct':  round(float(exc.mean()) * 100, 3),
        'excess_std_monthly_pct':   round(float(exc.std())  * 100, 3),
        'excess_skew':              round(float(exc.skew()), 3),
        'excess_kurt':              round(float(exc.kurt()), 3),
        'pct_months_beat_spy':      round(float((exc > 0).mean()) * 100, 1),
        'worst_excess_month':       round(float(exc.min()) * 100, 2),
        'worst_excess_month_date':  str(df.loc[exc.idxmin(), 'hold_date'].date()),
        'best_excess_month':        round(float(exc.max()) * 100, 2),
        'best_excess_month_date':   str(df.loc[exc.idxmax(), 'hold_date'].date()),
        'avg_turnover_pct':         round(float(df['turnover'].mean()) * 100, 1),
        'avg_n_stocks':             round(float(df['n_stocks'].mean()), 1),
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 65)
    print("E1 — Phase D Diagnostic (Kill)")
    print(f"Run date: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 65)

    print("\n── Loading data...")
    prices  = load_prices()
    spy_ret = load_spy()
    sectors = load_sectors()
    df_c    = load_phase_c()
    print(f"  Tickers: {len(prices)} | Sectors mapped: {len(sectors)} | "
          f"Phase C months: {len(df_c)}")

    # ── D1: Regime breakdown ──────────────────────────────────────────────────
    print("\n── D1: Regime breakdown...")
    d1 = d1_regime_breakdown(df_c)

    # ── D2: Sector attribution ────────────────────────────────────────────────
    print("\n── D2: Sector attribution (reconstructing portfolios)...")
    base_portfolios = reconstruct_portfolios(prices)
    d2 = d2_sector_attribution(base_portfolios, prices, spy_ret, sectors, df_c)

    # ── D3: Post-hoc variations ───────────────────────────────────────────────
    print("\n── D3: Post-hoc variation analysis...")
    d3 = d3_posthoc_variations(prices, spy_ret, sectors, df_c)

    # ── D4: Distribution ──────────────────────────────────────────────────────
    print("\n── D4: Monthly return distribution...")
    d4 = d4_distribution(df_c)

    # ── Print report ──────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("PHASE D DIAGNOSTIC REPORT")
    print("=" * 65)

    # D1
    print("\nD1 — Regime Breakdown")
    print(f"  {'Period':<14}  {'N':>4}  {'Strat':>7}  {'SPY':>7}  "
          f"{'ExcShp':>7}  {'CumExc%':>8}")
    print("  " + "-" * 55)
    for label, r in d1['by_regime'].items():
        sign = '+' if r['excess_sharpe'] >= 0 else ''
        print(f"  {label:<14}  {r['n']:>4}  {r['strat_sharpe']:>7.3f}  "
              f"{r['spy_sharpe']:>7.3f}  "
              f"{sign}{r['excess_sharpe']:>6.3f}  "
              f"{r['cum_excess_pct']:>+8.1f}%")

    print()
    print("  Year-by-year excess Sharpe (positive = beats SPY):")
    for yr, r in sorted(d1['by_year'].items()):
        bar_len = int(abs(r['excess_sharpe']) * 20)
        bar = ('█' * bar_len) if r['excess_sharpe'] >= 0 else ('░' * bar_len)
        sign_chr = '+' if r['excess_sharpe'] >= 0 else '-'
        print(f"  {yr}  {sign_chr}{abs(r['excess_sharpe']):.3f}  {bar}")

    # D2
    print("\nD2 — Sector Attribution (average excess return contribution)")
    print(f"  {'Sector':<30}  {'PortWt':>7}  {'SpyWt':>7}  "
          f"{'OvrWt':>7}  {'ContribPct':>10}")
    print("  " + "-" * 65)
    for sec, sr in sorted(d2.items(),
                          key=lambda x: x[1]['total_contrib_pct'], reverse=True):
        ow_str = f"{sr['overweight']:+.3f}"
        print(f"  {sec:<30}  {sr['avg_portfolio_weight']:>7.3f}  "
              f"{sr['sp500_equal_weight']:>7.3f}  "
              f"{ow_str:>7}  {sr['total_contrib_pct']:>+10.2f}%")

    # D3
    print(f"\nD3 — Post-Hoc Variations  [{POSTHOC_DISCLAIMER[:60]}...]")
    print(f"  {'Variation':<45}  {'Sharpe':>7}  {'ExcShp':>7}  {'IR':>7}  {'Gate?':>6}")
    print("  " + "-" * 75)
    gate_es = 0.30
    gate_ir = 0.50
    for name, v in d3.items():
        es   = v.get('excess_sharpe', np.nan)
        ir_v = v.get('ir', np.nan)
        sh   = v.get('sharpe', np.nan)
        clears = (not np.isnan(es) and es >= gate_es and
                  not np.isnan(ir_v) and ir_v >= gate_ir)
        gate_str = '✓ YES' if clears else '✗ no'
        base_str = ' ← Phase C' if 'Phase C' in v.get('note', '') else ''
        print(f"  {name:<45}  {sh:>7.3f}  {es:>7.3f}  {ir_v:>7.3f}  "
              f"{gate_str:>6}{base_str}")

    # D4
    print("\nD4 — Monthly Return Distribution vs SPY")
    print(f"  Months beating SPY:       {d4['pct_months_beat_spy']:.1f}%")
    print(f"  Mean monthly excess:     {d4['excess_mean_monthly_pct']:+.3f}%")
    print(f"  Std monthly excess:       {d4['excess_std_monthly_pct']:.3f}%")
    print(f"  Skew / excess kurtosis:  {d4['excess_skew']:.3f} / {d4['excess_kurt']:.3f}")
    print(f"  Worst excess month:      {d4['worst_excess_month']:+.2f}%  "
          f"({d4['worst_excess_month_date']})")
    print(f"  Best excess month:       {d4['best_excess_month']:+.2f}%  "
          f"({d4['best_excess_month_date']})")
    print(f"  Avg turnover/month:       {d4['avg_turnover_pct']:.1f}%")
    print(f"  Avg stocks in portfolio:  {d4['avg_n_stocks']:.1f}")

    print("=" * 65)

    # ── Save ──────────────────────────────────────────────────────────────────
    ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    out = {
        'run_timestamp': datetime.utcnow().isoformat(),
        'd1_regime': d1,
        'd2_sector': d2,
        'd3_posthoc': d3,
        'd4_distribution': d4,
    }
    out_path = RES_DIR / f'e1_d_diagnostic_{ts}.json'
    with open(out_path, 'w') as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\n  [saved] {out_path.name}")


if __name__ == '__main__':
    main()
