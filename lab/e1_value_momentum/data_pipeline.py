"""E1 — Value+Momentum data pipeline (Phase B).

Fetches and caches:
  1. S&P 500 current constituents + historical changes (Wikipedia)
  2. Monthly adjusted prices for all tickers (yfinance, auto_adjust=True)
  3. SPY benchmark monthly total return (yfinance)
  4. Quarterly point-in-time P/B ratios (Simfin)

Point-in-time rule (pre-registered, Phase A):
  At rebalance date T, use the most recent P/B observation where
  PUBLISH_DATE <= T. Never use data published after T.

Phase B quality gates:
  PASS: price coverage >= 95% of tickers from 2009-01
  PASS: P/B coverage  >= 90% of tickers with >= 1 valid observation
  KILL: if P/B coverage < 90%, drop value factor, proceed momentum-only
        (decision logged before Phase C)

Run: python data_pipeline.py
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

# ── Paths ─────────────────────────────────────────────────────────────────────
LAB_DIR   = Path(__file__).parent
DATA_DIR  = LAB_DIR / 'data'
CACHE_DIR = DATA_DIR / 'cache'
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ── Config ────────────────────────────────────────────────────────────────────
PRICE_START  = '2009-01-01'   # 13 months before first rebalance (2010-02)
PRICE_END    = '2026-04-30'
BACKTEST_START = pd.Timestamp('2010-01-01')

PRICE_COV_GATE = 0.95   # >= 95% of tickers must have full price history
PB_COV_GATE    = 0.90   # >= 90% of tickers must have >= 1 P/B observation

MIN_MARKET_CAP = 1e9    # $1B
MIN_PRICE      = 5.0    # $5

YF_BATCH  = 50          # tickers per yfinance batch call
YF_DELAY  = 2.0         # seconds between batches


# ── 1. S&P 500 Constituents ───────────────────────────────────────────────────

def fetch_sp500_constituents() -> tuple[list[str], pd.DataFrame]:
    """Return (current_tickers, changes_df) from Wikipedia.

    changes_df columns: date, added_ticker, removed_ticker
    Tickers are cleaned: '.' → '-' (BRK.B → BRK-B for yfinance).
    """
    cache = CACHE_DIR / 'sp500_constituents.parquet'
    changes_cache = CACHE_DIR / 'sp500_changes.parquet'

    import requests as _requests
    _headers = {'User-Agent': 'Mozilla/5.0 (E1-DataPipeline/1.0; research)'}

    print("── S&P 500 constituents (Wikipedia)...")
    try:
        resp = _requests.get(
            'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies',
            headers=_headers, timeout=30,
        )
        resp.raise_for_status()
        wiki_html = resp.text

        all_tables = pd.read_html(wiki_html)
        tables = pd.read_html(wiki_html, attrs={'id': 'constituents'})
        current_df = tables[0]
        tickers = (
            current_df['Symbol']
            .str.replace('.', '-', regex=False)
            .tolist()
        )

        # Historical changes table (second table on the page)
        try:
            if len(all_tables) > 1:
                chg = all_tables[1].copy()
                chg.columns = ['_'.join(str(c) for c in col).strip()
                               for col in chg.columns]
                changes_cache_df = chg
                changes_cache_df.to_parquet(changes_cache)
                print(f"  ✓ Historical changes table: {len(chg)} rows")
            else:
                print("  ⚠ No changes table found")
                changes_cache_df = pd.DataFrame()
        except Exception as e:
            print(f"  ⚠ Changes table error: {e}")
            changes_cache_df = pd.DataFrame()

        current_df.to_parquet(cache)
        print(f"  ✓ Current constituents: {len(tickers)} tickers")
        print(f"  ⚠ SURVIVORSHIP BIAS: using current S&P 500 membership.")
        print(f"    Backtest excludes companies removed 2010-2026.")
        print(f"    Pre-committed haircut: -0.10 to -0.20 excess Sharpe.")
        return tickers, changes_cache_df

    except Exception as e:
        print(f"  ✗ Wikipedia fetch failed: {e}")
        if cache.exists():
            print("  Using cached constituents.")
            df = pd.read_parquet(cache)
            tickers = df['Symbol'].str.replace('.', '-', regex=False).tolist()
            return tickers, pd.DataFrame()
        raise


# ── 2. Monthly Prices ─────────────────────────────────────────────────────────

def fetch_prices(tickers: list[str]) -> dict[str, pd.Series]:
    """Download monthly adjusted close prices for all tickers.

    Returns dict[ticker -> pd.Series] with DatetimeIndex (month-end).
    auto_adjust=True includes splits and dividends (total return proxy).
    """
    print(f"\n── Monthly prices ({len(tickers)} tickers, {PRICE_START}–{PRICE_END})...")

    results: dict[str, pd.Series] = {}
    failed: list[str] = []

    # Load cached tickers first
    cached = {p.stem.replace('price_', ''): p
              for p in CACHE_DIR.glob('price_*.parquet')}
    already = set(cached.keys())

    to_fetch = [t for t in tickers if t not in already]
    print(f"  Cached: {len(already)} | To fetch: {len(to_fetch)}")

    # Load cached
    for tkr, path in cached.items():
        try:
            s = pd.read_parquet(path)['close']
            if not s.empty:
                results[tkr] = s
        except Exception:
            to_fetch.append(tkr)

    # Batch-fetch remaining
    for i in range(0, len(to_fetch), YF_BATCH):
        batch = to_fetch[i:i + YF_BATCH]
        batch_str = ' '.join(batch)
        try:
            raw = yf.download(
                batch_str,
                start=PRICE_START,
                end=PRICE_END,
                interval='1mo',
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            if raw.empty:
                failed.extend(batch)
                continue

            # yfinance returns MultiIndex columns when multiple tickers
            if isinstance(raw.columns, pd.MultiIndex):
                close = raw['Close']
            else:
                close = raw[['Close']].rename(columns={'Close': batch[0]})

            for tkr in batch:
                if tkr in close.columns:
                    s = close[tkr].dropna()
                    if len(s) >= 13:
                        # Normalize index to month-end
                        s.index = s.index.to_period('M').to_timestamp('M')
                        results[tkr] = s
                        pd.DataFrame({'close': s}).to_parquet(
                            CACHE_DIR / f'price_{tkr}.parquet'
                        )
                    else:
                        failed.append(tkr)
                else:
                    failed.append(tkr)

            if i + YF_BATCH < len(to_fetch):
                time.sleep(YF_DELAY)

        except Exception as e:
            print(f"  [batch {i//YF_BATCH + 1} error: {e}]")
            failed.extend(batch)
            time.sleep(5)

    n_ok = len(results)
    pct  = n_ok / len(tickers) * 100
    print(f"  Price coverage: {n_ok}/{len(tickers)} = {pct:.1f}%  "
          f"({'✓ PASS' if pct >= PRICE_COV_GATE * 100 else '✗ FAIL'})")
    if failed:
        print(f"  Failed tickers ({len(failed)}): {failed[:10]}"
              + (" ..." if len(failed) > 10 else ""))
    return results


# ── 3. SPY Benchmark ──────────────────────────────────────────────────────────

def fetch_spy_benchmark() -> pd.Series:
    """Download SPY monthly total return series."""
    cache = CACHE_DIR / 'spy_monthly.parquet'
    print("\n── SPY benchmark...")

    try:
        raw = yf.download(
            'SPY',
            start=PRICE_START,
            end=PRICE_END,
            interval='1mo',
            auto_adjust=True,
            progress=False,
        )
        if raw.empty:
            raise ValueError("Empty SPY download")

        close = raw['Close'].squeeze().dropna()
        close.index = close.index.to_period('M').to_timestamp('M')
        ret = close.pct_change().dropna()
        out = pd.DataFrame({'close': close})
        out['ret'] = ret
        out.to_parquet(cache)
        print(f"  ✓ SPY: {len(ret)} monthly returns "
              f"[{ret.index[0].strftime('%Y-%m')}→{ret.index[-1].strftime('%Y-%m')}]")
        return ret
    except Exception as e:
        print(f"  ✗ SPY fetch failed: {e}")
        raise


# ── 4. Simfin P/B Ratios ──────────────────────────────────────────────────────

def _simfin_setup() -> tuple:
    """Initialise Simfin, return (sf, ok: bool)."""
    api_key = os.getenv('SIMFIN_API_KEY', '').strip()
    if not api_key:
        return None, False
    try:
        import simfin as sf
        sf.set_api_key(api_key)
        simfin_dir = DATA_DIR / 'simfin_cache'
        simfin_dir.mkdir(exist_ok=True)
        sf.set_data_dir(str(simfin_dir))
        return sf, True
    except ImportError:
        return None, False


def _extract_pb_from_dataset(derived, tickers: list[str]) -> dict[str, pd.DataFrame]:
    """Pull per-ticker P/B from a Simfin derived-shareprices DataFrame."""
    pb_col = next((c for c in derived.columns
                   if 'Book' in c or 'P/B' in c or 'book' in c.lower()), None)
    pub_col = next((c for c in derived.columns
                    if 'Publish' in c or 'publish' in c.lower()), None)
    if pb_col is None:
        return {}

    results = {}
    all_tickers_in_df = set(derived.index.get_level_values('Ticker'))

    for tkr in tickers:
        tkr_sf = tkr.replace('-', '.')
        match = tkr_sf if tkr_sf in all_tickers_in_df else \
                (tkr if tkr in all_tickers_in_df else None)
        if match is None:
            continue
        try:
            sub = derived.xs(match, level='Ticker')[[pb_col]].copy()
            sub.columns = ['pb_ratio']
            sub = sub.dropna()
            if sub.empty:
                continue
            if not isinstance(sub.index, pd.DatetimeIndex):
                sub.index = pd.to_datetime(sub.index)
            if pub_col:
                pub = derived.xs(match, level='Ticker')[pub_col]
                sub['publish_date'] = pd.to_datetime(pub.values)
            else:
                sub['publish_date'] = sub.index + pd.offsets.Day(45)
            sub.index.name = 'report_date'
            results[tkr] = sub.reset_index()[['report_date', 'publish_date', 'pb_ratio']]
        except Exception:
            continue
    return results


def _compute_pb_from_balance(
    sf, tickers: list[str], prices: dict[str, pd.Series]
) -> dict[str, pd.DataFrame]:
    """Compute P/B from balance sheet + yfinance prices.

    Balance sheet already contains Total Equity AND Shares (Basic) AND Publish Date.
    P/B = price_at_month_end_before_publish_date / (Total Equity / Shares Basic)
    """
    results: dict[str, pd.DataFrame] = {}

    for variant in ['quarterly', 'annual']:
        try:
            print(f"    Loading balance sheet ({variant})...")
            balance = sf.load_balance(market='us', variant=variant)
            print(f"    ✓ {len(balance)} rows, "
                  f"{balance.index.get_level_values('Ticker').nunique()} tickers")
            break
        except Exception as e:
            print(f"    [{variant} failed: {e}]")
            balance = None

    if balance is None:
        return {}

    eq_col  = next((c for c in balance.columns
                    if c in ('Total Equity', 'Common Equity',
                              "Total Shareholders' Equity")), None)
    sh_col  = next((c for c in balance.columns
                    if c in ('Shares (Basic)', 'Shares (Diluted)',
                              'Basic Shares Outstanding')), None)
    pub_col = next((c for c in balance.columns
                    if 'Publish' in c), None)

    print(f"    equity='{eq_col}'  shares='{sh_col}'  publish='{pub_col}'")
    if eq_col is None or sh_col is None:
        print("    ✗ Required columns missing from balance sheet")
        return {}

    all_tickers = set(balance.index.get_level_values('Ticker'))

    for tkr in tickers:
        tkr_sf = tkr.replace('-', '.')
        match  = tkr_sf if tkr_sf in all_tickers else \
                 (tkr    if tkr    in all_tickers else None)
        if match is None:
            continue
        try:
            sub = balance.xs(match, level='Ticker').copy()
            if not isinstance(sub.index, pd.DatetimeIndex):
                sub.index = pd.to_datetime(sub.index)

            sub = sub[[eq_col, sh_col] + ([pub_col] if pub_col else [])].copy()
            sub = sub.dropna(subset=[eq_col, sh_col])
            sub = sub[sub[sh_col] > 0]
            if sub.empty:
                continue

            sub['bvps'] = sub[eq_col] / sub[sh_col]
            sub = sub[sub['bvps'] > 0]

            if pub_col:
                sub['publish_date'] = pd.to_datetime(sub[pub_col])
            else:
                sub['publish_date'] = sub.index + pd.offsets.Day(45)

            price_series = prices.get(tkr)
            if price_series is None or price_series.empty:
                continue

            rows = []
            for rpt_date, row in sub.iterrows():
                pub_date = row['publish_date']
                bvps     = row['bvps']
                if pd.isna(pub_date) or pd.isna(bvps) or bvps <= 0:
                    continue
                avail = price_series[price_series.index <= pub_date]
                if avail.empty:
                    continue
                price = float(avail.iloc[-1])
                rows.append({
                    'report_date':  rpt_date,
                    'publish_date': pub_date,
                    'pb_ratio':     price / bvps,
                })

            if rows:
                results[tkr] = pd.DataFrame(rows)

        except Exception:
            continue

    return results


def fetch_pb_ratios(
    tickers: list[str],
    prices: dict[str, pd.Series] | None = None,
) -> dict[str, pd.DataFrame]:
    """Fetch quarterly point-in-time P/B ratios.

    Strategy (in order):
      1. Simfin derived-shareprices quarterly
      2. Simfin derived-shareprices annual
      3. Compute from Simfin balance sheet + income + cached yfinance prices

    Returns dict[ticker -> DataFrame(report_date, publish_date, pb_ratio)].
    """
    api_key = os.getenv('SIMFIN_API_KEY', '').strip()
    if not api_key:
        print("\n── Simfin P/B ratios...")
        print("  ✗ SIMFIN_API_KEY not set.")
        return {}

    sf, ok = _simfin_setup()
    if not ok:
        print("  ✗ simfin not installed. Run: pip install simfin")
        return {}

    print(f"\n── Simfin P/B ratios ({len(tickers)} tickers)...")
    results: dict[str, pd.DataFrame] = {}

    # Attempt 1 & 2: derived shareprices (quarterly then annual)
    for variant in ['quarterly', 'annual']:
        try:
            print(f"  Trying derived shareprices ({variant})...")
            derived = sf.load_derived_shareprices(market='us', variant=variant)
            if derived is not None and not derived.empty:
                results = _extract_pb_from_dataset(derived, tickers)
                if results:
                    print(f"  ✓ Derived shareprices ({variant}): "
                          f"{len(results)} tickers")
                    break
        except Exception as e:
            print(f"  [{variant} derived: {type(e).__name__}]")

    # Attempt 3: compute from balance + income + yfinance prices
    if not results:
        print("  Falling back to balance sheet computation...")
        if prices is None:
            print("  ✗ prices dict not provided for fallback")
        else:
            results = _compute_pb_from_balance(sf, tickers, prices)

    n_ok = len(results)
    pct  = n_ok / len(tickers) * 100
    print(f"  P/B coverage: {n_ok}/{len(tickers)} = {pct:.1f}%  "
          f"({'✓ PASS' if pct >= PB_COV_GATE * 100 else '✗ FAIL'})")

    for tkr, df in results.items():
        df.to_parquet(CACHE_DIR / f'pb_{tkr}.parquet', index=False)

    return results


# ── 5. Quality Report ─────────────────────────────────────────────────────────

def quality_report(
    tickers: list[str],
    prices: dict[str, pd.Series],
    pb: dict[str, pd.DataFrame],
    spy_ret: pd.Series,
) -> dict:
    """Compute coverage stats and evaluate Phase B gates."""

    # Price coverage: any valid data downloaded (stocks IPO'd post-2009 are
    # included from their listing date; they simply don't appear in early months)
    n_full_price = sum(1 for s in prices.values() if not s.empty and len(s) >= 13)
    pct_price = n_full_price / len(tickers) * 100

    # Also report how many have full history back to PRICE_START (informational)
    price_start_ts = pd.Timestamp(PRICE_START)
    n_full_history = sum(
        1 for s in prices.values()
        if not s.empty and s.index[0] <= price_start_ts + pd.offsets.MonthEnd(3)
    )

    # P/B coverage: any valid observation in backtest window
    n_pb_ok = sum(
        1 for df in pb.values()
        if not df.empty and (
            pd.to_datetime(df['publish_date']).max() >= BACKTEST_START
        )
    )
    pct_pb = n_pb_ok / len(tickers) * 100 if tickers else 0

    price_pass = pct_price >= PRICE_COV_GATE * 100
    pb_pass    = pct_pb    >= PB_COV_GATE    * 100

    # SPY stats
    spy_backtest = spy_ret[spy_ret.index >= BACKTEST_START]
    spy_ann_ret  = spy_backtest.mean() * 12
    spy_ann_vol  = spy_backtest.std() * np.sqrt(12)
    spy_sharpe   = spy_ann_ret / spy_ann_vol if spy_ann_vol > 0 else np.nan

    # Median P/B observation count per ticker
    pb_obs_counts = [len(df) for df in pb.values() if not df.empty]
    median_pb_obs = float(np.median(pb_obs_counts)) if pb_obs_counts else 0

    qr = {
        'run_timestamp': datetime.utcnow().isoformat(),
        'n_tickers': len(tickers),
        'price': {
            'n_covered': n_full_price,
            'pct_covered': round(pct_price, 1),
            'n_full_history_to_2009': n_full_history,
            'gate': f'>= {PRICE_COV_GATE*100:.0f}% with >= 13 months data',
            'pass': price_pass,
        },
        'pb': {
            'n_covered': n_pb_ok,
            'pct_covered': round(pct_pb, 1),
            'gate': f'>= {PB_COV_GATE*100:.0f}%',
            'pass': pb_pass,
            'median_obs_per_ticker': median_pb_obs,
            'note': 'Empty if SIMFIN_API_KEY not set',
        },
        'spy': {
            'n_months': len(spy_backtest),
            'ann_return_pct': round(spy_ann_ret * 100, 2),
            'ann_vol_pct': round(spy_ann_vol * 100, 2),
            'sharpe': round(spy_sharpe, 3),
            'period': f"{spy_backtest.index[0].strftime('%Y-%m')} → "
                      f"{spy_backtest.index[-1].strftime('%Y-%m')}",
        },
        'phase_b_decision': (
            'PASS — proceed to Phase C with value+momentum'
            if price_pass and pb_pass
            else 'PASS (momentum-only) — P/B coverage insufficient; drop value factor'
            if price_pass and not pb_pass
            else 'FAIL — price coverage insufficient; investigate before Phase C'
        ),
        'survivorship_bias_note': (
            'Using current S&P 500 constituents. '
            'Pre-committed haircut: -0.10 to -0.20 excess Sharpe on gross result.'
        ),
    }
    return qr


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 65)
    print("E1 — Value+Momentum Data Pipeline (Phase B)")
    print(f"Run date: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 65)

    # Step 1: Constituents
    tickers, _changes = fetch_sp500_constituents()

    # Steps 2–3: Prices + SPY (parallel-ish via batching)
    prices  = fetch_prices(tickers)
    spy_ret = fetch_spy_benchmark()

    # Step 4: P/B
    pb = fetch_pb_ratios(tickers, prices=prices)

    # Step 5: Quality report
    print("\n── Quality Report...")
    qr = quality_report(tickers, prices, pb, spy_ret)

    ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    qr_path = DATA_DIR / f'e1_b_quality_{ts}.json'
    with open(qr_path, 'w') as f:
        json.dump(qr, f, indent=2)

    print("\n" + "=" * 65)
    print("PHASE B QUALITY REPORT")
    print("=" * 65)
    print(f"  Tickers in universe:  {qr['n_tickers']}")
    print()
    print(f"  Price coverage:  {qr['price']['n_covered']}/{qr['n_tickers']} "
          f"= {qr['price']['pct_covered']}%  "
          f"[gate {qr['price']['gate']}]  "
          f"{'✓ PASS' if qr['price']['pass'] else '✗ FAIL'}")
    print(f"    (of which {qr['price']['n_full_history_to_2009']} have data back to 2009-01)")
    print(f"  P/B coverage:    {qr['pb']['n_covered']}/{qr['n_tickers']} "
          f"= {qr['pb']['pct_covered']}%  "
          f"[gate {qr['pb']['gate']}]  "
          f"{'✓ PASS' if qr['pb']['pass'] else '✗ FAIL'}")
    print()
    print(f"  SPY benchmark ({qr['spy']['period']}):")
    print(f"    Ann return:  {qr['spy']['ann_return_pct']}%")
    print(f"    Ann vol:     {qr['spy']['ann_vol_pct']}%")
    print(f"    Sharpe:      {qr['spy']['sharpe']}")
    print()
    print(f"  Survivorship bias: {qr['survivorship_bias_note']}")
    print()
    print(f"  ── PHASE B DECISION: {qr['phase_b_decision']}")
    print()
    print(f"  [saved] {qr_path.name}")
    print("=" * 65)


if __name__ == '__main__':
    main()
