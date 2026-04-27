"""E1 — Momentum-Only Backtest (Phase C).

Strategy:
  Long-only top quintile by 12-1 month momentum, equal-weight,
  monthly rebalance on last business day of month.

Momentum definition (pre-registered, Phase A):
  At rebalance date T (end of month M):
    mom = price[end of M-1] / price[end of M-13] - 1
  This skips the most recent month (reversal avoidance).
  Literature: Jegadeesh & Titman (1993).

Benchmark: SPY total return (auto-adjusted, dividends reinvested).

Cost model (pre-registered):
  $0 commission (IBKR Lite) + 0.02% one-way bid-ask on notional traded.
  Cost charged on entries, exits, and rebalancing drift of continuing positions.

Survivorship bias note:
  Universe = current S&P 500 constituents. Pre-committed haircut
  -0.10 to -0.20 excess Sharpe on all gross figures.

Gates (pre-registered, Phase A):
  IR >= 0.50
  Excess Sharpe >= 0.30
  Max active drawdown < 15%
  SPY Sharpe over backtest period ~0.974 → need strategy Sharpe >= ~1.27

Run: python backtest.py
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
RES_DIR.mkdir(parents=True, exist_ok=True)

# ── Config ────────────────────────────────────────────────────────────────────
BACKTEST_START = pd.Timestamp('2010-02-28')  # first rebalance (needs M-13 = Jan 2009)
INITIAL_EQUITY = 10_000.0
MIN_PRICE      = 5.0        # pre-registered filter
QUINTILE       = 0.20       # top 20%
SPREAD_OW      = 0.0002     # 0.02% one-way bid-ask

# Pre-registered SPY Sharpe from Phase B
SPY_SHARPE_PHASE_B = 0.974
EXCESS_SHARPE_GATE = 0.30
IR_GATE            = 0.50
MAX_ACTIVE_DD_GATE = 0.15


# ── Data loading ──────────────────────────────────────────────────────────────

def load_prices() -> dict[str, pd.Series]:
    """Load all cached monthly price series."""
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
    """Load SPY monthly returns from cache."""
    p = CACHE_DIR / 'spy_monthly.parquet'
    df = pd.read_parquet(p)
    ret = df['ret'].dropna()
    ret.index = pd.to_datetime(ret.index)
    return ret


# ── Backtest loop ─────────────────────────────────────────────────────────────

def _get_price(s: pd.Series, date: pd.Timestamp) -> float | None:
    """Return price at month-end date, or None if unavailable."""
    try:
        return float(s.loc[date])
    except KeyError:
        return None


def run_backtest(
    prices: dict[str, pd.Series],
    spy_ret: pd.Series,
    cost_mult: float = 1.0,
) -> pd.DataFrame:
    """Run the monthly momentum backtest.

    Returns DataFrame with one row per holding month:
      rebal_date, hold_date, n_stocks, n_entries, n_exits, turnover,
      gross_ret, cost_1x, spy_ret, excess_ret_1x
    (cost_mult applied analytically in compute_metrics for sensitivity table)
    """
    # All month-end dates from our price data
    all_dates = sorted({
        d for s in prices.values() for d in s.index
    })
    all_dates = [d for d in all_dates if d >= BACKTEST_START - pd.offsets.MonthEnd(14)]

    # Build rebalance schedule: all months where we have a valid M-13 lookback
    records = []
    prev_portfolio: set[str] = set()

    for i, rebal_date in enumerate(all_dates):
        if rebal_date < BACKTEST_START:
            continue

        m = rebal_date.to_period('M')
        d_recent = (m - 1).to_timestamp('M')   # end of M-1 (signal end)
        d_far    = (m - 13).to_timestamp('M')  # end of M-13 (signal start)
        d_next   = (m + 1).to_timestamp('M')   # end of M+1 (exit date)

        if d_next not in {d for s in prices.values() for d in s.index}:
            continue

        # ── Compute momentum for all stocks ──────────────────────────────────
        mom_scores: dict[str, float] = {}
        for tkr, s in prices.items():
            p_recent = _get_price(s, d_recent)
            p_far    = _get_price(s, d_far)
            p_cur    = _get_price(s, rebal_date)  # for price filter
            if p_recent is None or p_far is None or p_cur is None:
                continue
            if p_far <= 0 or p_cur < MIN_PRICE:
                continue
            mom_scores[tkr] = p_recent / p_far - 1

        if len(mom_scores) < 50:  # need enough stocks for a quintile
            continue

        # ── Select top quintile ───────────────────────────────────────────────
        ranked = sorted(mom_scores.items(), key=lambda x: x[1], reverse=True)
        n_select = max(1, int(len(ranked) * QUINTILE))
        curr_portfolio = set(t for t, _ in ranked[:n_select])

        # ── Compute holding-period return (earn month M+1) ────────────────────
        gross_rets = []
        for tkr in list(curr_portfolio):
            p_entry = _get_price(prices[tkr], rebal_date)
            p_exit  = _get_price(prices[tkr], d_next)
            if p_entry is None or p_exit is None or p_entry <= 0:
                curr_portfolio.discard(tkr)
                continue
            gross_rets.append(p_exit / p_entry - 1)

        if not gross_rets:
            continue

        gross_ret = float(np.mean(gross_rets))
        n_curr = len(curr_portfolio)

        # ── Cost: entries + exits at 0.02% one-way ───────────────────────────
        entries = curr_portfolio - prev_portfolio
        exits   = prev_portfolio - curr_portfolio

        # Continuing positions: rebalance drift cost
        n_prev = len(prev_portfolio) if prev_portfolio else n_curr
        drift_cost = 0.0
        if prev_portfolio:
            for tkr in curr_portfolio & prev_portfolio:
                prev_wt  = 1.0 / n_prev
                curr_wt  = 1.0 / n_curr
                p_prev   = _get_price(prices[tkr], rebal_date - pd.offsets.MonthEnd(1))
                p_now    = _get_price(prices[tkr], rebal_date)
                if p_prev and p_now and p_prev > 0:
                    actual_wt = prev_wt * (p_now / p_prev)
                    drift = abs(curr_wt - actual_wt)
                    drift_cost += drift * SPREAD_OW

        entry_cost = len(entries) * (1.0 / n_curr) * SPREAD_OW
        exit_cost  = len(exits)   * (1.0 / n_prev if n_prev else 1.0 / n_curr) * SPREAD_OW
        cost_1x    = entry_cost + exit_cost + drift_cost

        turnover = (len(entries) + len(exits)) / (2 * max(n_curr, 1))

        try:
            spy_month = float(spy_ret.loc[d_next])
        except KeyError:
            spy_month = np.nan

        records.append({
            'rebal_date':    rebal_date,
            'hold_date':     d_next,
            'n_stocks':      n_curr,
            'n_entries':     len(entries),
            'n_exits':       len(exits),
            'turnover':      round(turnover, 4),
            'gross_ret':     gross_ret,
            'cost_1x':       cost_1x,
            'net_ret_1x':    gross_ret - cost_1x,
            'spy_ret':       spy_month,
            'excess_ret_1x': gross_ret - cost_1x - spy_month,
        })

        prev_portfolio = curr_portfolio

    return pd.DataFrame(records)


# ── Metrics ───────────────────────────────────────────────────────────────────

def compute_metrics(df: pd.DataFrame) -> dict:
    """Compute all Phase C metrics across cost multipliers."""
    out = {'n_months': len(df)}

    for cmult, label in [(0.0, '0x'), (0.5, '0.5x'), (1.0, '1x'), (2.0, '2x')]:
        net_ret = df['gross_ret'] - cmult * df['cost_1x']
        spy_ret = df['spy_ret']
        excess  = net_ret - spy_ret

        ann_ret     = net_ret.mean() * 12
        ann_vol     = net_ret.std()  * np.sqrt(12)
        sharpe      = ann_ret / ann_vol if ann_vol > 0 else np.nan

        spy_ann_ret = spy_ret.mean() * 12
        spy_ann_vol = spy_ret.std()  * np.sqrt(12)
        spy_sharpe  = spy_ann_ret / spy_ann_vol if spy_ann_vol > 0 else np.nan

        excess_sharpe = sharpe - spy_sharpe

        ir_ann   = excess.mean() * 12
        ir_vol   = excess.std()  * np.sqrt(12)
        ir       = ir_ann / ir_vol if ir_vol > 0 else np.nan

        cum_excess   = (1 + excess).cumprod()
        rolling_peak = cum_excess.cummax()
        active_dd    = (cum_excess / rolling_peak - 1)
        max_active_dd = float(active_dd.min())

        # Absolute max drawdown (peak-to-trough in equity curve)
        equity = INITIAL_EQUITY * (1 + net_ret).cumprod()
        peak   = equity.cummax()
        abs_dd = float(((equity - peak) / peak).min())

        # Win rates
        win_rate_vs_spy = float((excess > 0).mean())
        win_rate_abs    = float((net_ret > 0).mean())

        out[label] = {
            'sharpe':          round(sharpe, 3),
            'spy_sharpe':      round(spy_sharpe, 3),
            'excess_sharpe':   round(excess_sharpe, 3),
            'ir':              round(ir, 3),
            'max_active_dd':   round(max_active_dd, 4),
            'max_abs_dd':      round(abs_dd, 4),
            'ann_ret_pct':     round(ann_ret * 100, 2),
            'ann_vol_pct':     round(ann_vol * 100, 2),
            'win_rate_vs_spy': round(win_rate_vs_spy, 3),
            'win_rate_abs':    round(win_rate_abs, 3),
            'total_ret_pct':   round(float((1 + net_ret).prod() - 1) * 100, 2),
        }

    # Gate evaluation at 1× cost
    m = out['1x']
    out['gate_results'] = {
        'ir':           {'value': m['ir'],             'gate': f'>= {IR_GATE}',
                         'pass': m['ir'] >= IR_GATE},
        'excess_sharpe':{'value': m['excess_sharpe'],  'gate': f'>= {EXCESS_SHARPE_GATE}',
                         'pass': m['excess_sharpe'] >= EXCESS_SHARPE_GATE},
        'max_active_dd':{'value': m['max_active_dd'],  'gate': f'> -{MAX_ACTIVE_DD_GATE}',
                         'pass': m['max_active_dd'] > -MAX_ACTIVE_DD_GATE},
    }
    out['gate_results']['all_pass'] = all(
        v['pass'] for v in out['gate_results'].values() if isinstance(v, dict)
    )

    out['survivorship_bias_note'] = (
        'Pre-committed haircut: -0.10 to -0.20 excess Sharpe. '
        f"Bias-adjusted excess Sharpe range: "
        f"[{m['excess_sharpe'] - 0.20:.3f}, {m['excess_sharpe'] - 0.10:.3f}]"
    )

    return out


# ── Stress months ─────────────────────────────────────────────────────────────

STRESS_MONTHS = {
    '2011-08': 'US credit downgrade / S&P 500 -6.3%',
    '2015-08': 'China devaluation shock / flash crash',
    '2018-02': 'VIX spike, short-vol unwind',
    '2020-03': 'COVID crash',
    '2022-01': 'Rate-hike repricing, growth selloff',
}

ADVERSARIAL_PERIODS = [
    ('2018-09', '2018-12', 'Q4 2018 momentum crash'),
    ('2020-05', '2020-05', 'Post-COVID momentum reversal'),
]


def stress_analysis(df: pd.DataFrame) -> dict:
    """Report strategy vs SPY for each pre-registered stress month."""
    results = {}
    df_idx = df.copy()
    df_idx['hold_ym'] = df_idx['hold_date'].dt.to_period('M').astype(str)

    for ym, label in STRESS_MONTHS.items():
        row = df_idx[df_idx['hold_ym'] == ym]
        if row.empty:
            results[ym] = {'label': label, 'note': 'not in backtest period'}
            continue
        r = row.iloc[0]
        results[ym] = {
            'label':       label,
            'strategy':    round(float(r['net_ret_1x']) * 100, 2),
            'spy':         round(float(r['spy_ret'])    * 100, 2),
            'excess':      round(float(r['excess_ret_1x']) * 100, 2),
            'n_stocks':    int(r['n_stocks']),
            'pass':        float(r['excess_ret_1x']) > 0,
        }

    # Adversarial periods
    adv = {}
    for start, end, label in ADVERSARIAL_PERIODS:
        mask = (df_idx['hold_ym'] >= start) & (df_idx['hold_ym'] <= end)
        sub = df_idx[mask]
        if sub.empty:
            adv[label] = {'note': 'not in backtest period'}
            continue
        strat_cum  = float((1 + sub['net_ret_1x']).prod() - 1)
        spy_cum    = float((1 + sub['spy_ret']).prod() - 1)
        adv[label] = {
            'period':      f"{start} → {end}",
            'strategy_cum': round(strat_cum * 100, 2),
            'spy_cum':      round(spy_cum   * 100, 2),
            'excess_cum':   round((strat_cum - spy_cum) * 100, 2),
            'n_months':     len(sub),
        }

    return {'stress_months': results, 'adversarial_periods': adv}


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 65)
    print("E1 — Momentum-Only Backtest (Phase C)")
    print(f"Run date: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 65)

    print("\n── Loading data...")
    prices  = load_prices()
    spy_ret = load_spy()
    print(f"  Tickers loaded: {len(prices)}")
    print(f"  SPY months:     {len(spy_ret)}")

    print("\n── Running backtest...")
    df = run_backtest(prices, spy_ret)
    print(f"  Months simulated: {len(df)}")
    if df.empty:
        print("  ✗ No data — check price cache")
        return

    period = (f"{df['hold_date'].min().strftime('%Y-%m')} → "
              f"{df['hold_date'].max().strftime('%Y-%m')}")
    print(f"  Period: {period}")
    print(f"  Avg stocks/month: {df['n_stocks'].mean():.1f}")
    print(f"  Avg turnover:     {df['turnover'].mean():.1%}/month")

    print("\n── Computing metrics...")
    metrics = compute_metrics(df)
    stress  = stress_analysis(df)

    # ── Print results ─────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("PHASE C RESULTS — MOMENTUM-ONLY STRATEGY")
    print("=" * 65)
    print(f"  Period: {period}  ({metrics['n_months']} months)")
    print(f"  Universe: current S&P 500 ({len(prices)} tickers with price data)")
    print()
    print(f"  {'Scenario':<12}  {'Sharpe':>8}  {'SPY Shrp':>8}  "
          f"{'Excess':>8}  {'IR':>8}  {'ActiveDD':>9}  {'TotRet%':>8}")
    print("  " + "-" * 65)
    for lbl in ['0x', '0.5x', '1x', '2x']:
        m = metrics[lbl]
        marker = ' ← baseline' if lbl == '1x' else ''
        print(f"  {lbl:<12}  {m['sharpe']:>8.3f}  {m['spy_sharpe']:>8.3f}  "
              f"{m['excess_sharpe']:>8.3f}  {m['ir']:>8.3f}  "
              f"{m['max_active_dd']:>9.3f}  {m['total_ret_pct']:>8.1f}%{marker}")

    print()
    m1 = metrics['1x']
    gr = metrics['gate_results']
    print("  Gate evaluation (1× cost, pre-registered):")
    for gname, gval in gr.items():
        if not isinstance(gval, dict):
            continue
        sym = '✓' if gval['pass'] else '✗'
        print(f"    {sym} {gname:<20} {gval['value']:>7.3f}  [gate {gval['gate']}]")
    print()
    overall = '✓ PASS' if gr['all_pass'] else '✗ FAIL'
    print(f"  Overall: {overall}")
    print()
    print(f"  {metrics['survivorship_bias_note']}")

    print("\n  ── Stress months (pre-registered):")
    for ym, sr in stress['stress_months'].items():
        if 'note' in sr:
            continue
        sym = '✓' if sr['pass'] else '✗'
        print(f"    {sym} {ym}  strat={sr['strategy']:+.1f}%  "
              f"spy={sr['spy']:+.1f}%  excess={sr['excess']:+.1f}%  "
              f"[{sr['label']}]")

    print("\n  ── Adversarial momentum periods (pre-registered):")
    for label, ap in stress['adversarial_periods'].items():
        if 'note' in ap:
            continue
        sym = '✓' if ap['excess_cum'] > 0 else '✗'
        print(f"    {sym} {ap['period']}  strat={ap['strategy_cum']:+.1f}%  "
              f"spy={ap['spy_cum']:+.1f}%  excess={ap['excess_cum']:+.1f}%  "
              f"[{label}]")

    # ── Save results ──────────────────────────────────────────────────────────
    ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    csv_path  = RES_DIR / f'e1_c_monthly_{ts}.csv'
    json_path = RES_DIR / f'e1_c_summary_{ts}.json'

    df.to_csv(csv_path, index=False)

    summary = {
        'run_timestamp': datetime.utcnow().isoformat(),
        'period': period,
        'metrics': metrics,
        'stress': stress,
    }
    with open(json_path, 'w') as f:
        json.dump(summary, f, indent=2, default=str)

    print(f"\n  [saved] {csv_path.name}")
    print(f"  [saved] {json_path.name}")
    print("=" * 65)


if __name__ == '__main__':
    main()
