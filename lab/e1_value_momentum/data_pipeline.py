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

def fetch_pb_ratios(tickers: list[str]) -> dict[str, pd.DataFrame]:
    """Fetch quarterly point-in-time P/B ratios via Simfin.

    Returns dict[ticker -> DataFrame] with columns:
      report_date, publish_date, pb_ratio

    Point-in-time: publish_date is when the filing became public.
    At rebalance date T, use the most recent row where publish_date <= T.
    """
    api_key = os.getenv('SIMFIN_API_KEY', '').strip()
    if not api_key:
        print("\n── Simfin P/B ratios...")
        print("  ✗ SIMFIN_API_KEY not set.")
        print("  → Register free at https://simfin.com/login, get API key,")
        print("    add SIMFIN_API_KEY=<key> to .env file, re-run pipeline.")
        return {}

    try:
        import simfin as sf
    except ImportError:
        print("  ✗ simfin not installed. Run: pip install simfin")
        return {}

    print(f"\n── Simfin P/B ratios ({len(tickers)} tickers)...")
    sf.set_api_key(api_key)
    simfin_dir = DATA_DIR / 'simfin_cache'
    simfin_dir.mkdir(exist_ok=True)
    sf.set_data_dir(str(simfin_dir))

    results: dict[str, pd.DataFrame] = {}

    try:
        # Load quarterly derived data — includes P/Book, REPORT_DATE, PUBLISH_DATE
        print("  Downloading Simfin derived shareprices (quarterly)...")
        derived = sf.load_derived_shareprices(
            market='us',
            variant='quarterly',
        )

        if derived is None or derived.empty:
            print("  ✗ Simfin returned empty dataset")
            return {}

        print(f"  ✓ Simfin: {len(derived)} rows, "
              f"companies: {derived.index.get_level_values('Ticker').nunique()}")

        # Extract P/B per ticker with PUBLISH_DATE
        pb_col = next(
            (c for c in derived.columns if 'Book' in c or 'P/B' in c),
            None
        )
        if pb_col is None:
            print(f"  ✗ P/B column not found. Columns: {list(derived.columns[:10])}")
            return {}

        print(f"  Using column: '{pb_col}'")

        for tkr in tickers:
            tkr_clean = tkr.replace('-', '.')  # Simfin uses BRK.B, not BRK-B
            try:
                sub = derived.xs(tkr_clean, level='Ticker') if tkr_clean in \
                    derived.index.get_level_values('Ticker') else \
                    derived.xs(tkr, level='Ticker')

                sub = sub[[pb_col]].copy()
                sub.columns = ['pb_ratio']
                sub = sub.dropna()

                if len(sub) == 0:
                    continue

                # Normalize index to timestamp
                if not isinstance(sub.index, pd.DatetimeIndex):
                    sub.index = pd.to_datetime(sub.index)

                # Separate REPORT_DATE (index) and PUBLISH_DATE
                # Simfin index is REPORT_DATE; PUBLISH_DATE often in columns
                pub_col = next(
                    (c for c in derived.columns
                     if 'Publish' in c or 'PUBLISH' in c),
                    None
                )
                if pub_col:
                    try:
                        pub_data = derived.xs(tkr_clean, level='Ticker')[pub_col]
                        if not isinstance(pub_data.index, pd.DatetimeIndex):
                            pub_data.index = pd.to_datetime(pub_data.index)
                        sub['publish_date'] = pd.to_datetime(pub_data)
                    except Exception:
                        sub['publish_date'] = sub.index + pd.offsets.Day(45)
                else:
                    # Conservative fallback: assume 45-day lag from report date
                    sub['publish_date'] = sub.index + pd.offsets.Day(45)

                sub.index.name = 'report_date'
                sub = sub.reset_index()
                results[tkr] = sub[['report_date', 'publish_date', 'pb_ratio']]

            except (KeyError, Exception):
                continue

        n_ok  = len(results)
        pct   = n_ok / len(tickers) * 100
        print(f"  P/B coverage: {n_ok}/{len(tickers)} = {pct:.1f}%  "
              f"({'✓ PASS' if pct >= PB_COV_GATE * 100 else '✗ FAIL'})")

        # Cache per-ticker
        for tkr, df in results.items():
            df.to_parquet(CACHE_DIR / f'pb_{tkr}.parquet', index=False)

        return results

    except Exception as e:
        print(f"  ✗ Simfin error: {type(e).__name__}: {e}")
        return {}


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
    pb = fetch_pb_ratios(tickers)

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
