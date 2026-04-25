# Equity Factor Lab — Spec

**Date initialized**: 2026-04-25
**Status**: Phase B — data pipeline
**Repository**: `equity_factors`

---

## Goal

Test whether a simple long-only equity factor strategy on the S&P 500 universe
produces meaningful excess return over SPY on a risk-adjusted basis, using
$0-commission execution at Interactive Brokers.

This is a direct redirect from the FX Strategy Lab (all four FX strategies killed
2026-04-25). The structural change: IBKR equity cost regime (~$0 commission,
~0.02% bid-ask on large-caps) vs. G10 FX retail regime (~2 bps one-way) which
killed all FX strategies.

---

## Strategy Specification (pre-registered, Phase A)

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Universe | S&P 500 current constituents | Liquid, accessible at IBKR Lite |
| Factors | Value (P/B inverse) + Momentum (12−1 month return) | Fama-French (1992), Jegadeesh-Titman (1993) |
| Factor weighting | Equal-weight composite z-score | Literature default; no optimization |
| Selection | Top quintile by composite score (~100 stocks) | Standard factor quintile construction |
| Portfolio weighting | Equal-weight within selection | Simplest; avoids market-cap concentration |
| Rebalancing | Monthly, last business day | Consistent with FX lab cadence |
| Benchmark | SPY total return (dividends reinvested) | The passive alternative |
| Backtest period | Jan 2010 – present (~15 years) | Post-crisis; avoids GFC regime break |
| Momentum lookback | 12-month return, skip most recent month (12−1) | Avoids short-term reversal; literature standard |
| Value metric | Price-to-Book (P/B) ratio | Fama-French canonical value factor |
| Min liquidity filter | Market cap ≥ $1B, price ≥ $5 | Excludes stocks not actually tradeable |

**Direction lock**: Long top-quintile composite score = long cheap + high-momentum
stocks. This direction is fixed. Post-hoc reversal (e.g., bottom-quintile works
better) is prohibited by the same rule as S4/S6 in the FX lab.

**Point-in-time rule (pre-registered)**: At rebalance date T, the P/B value used
is the most recent quarterly filing with `PUBLISH_DATE ≤ T`. Never use data
published after T. This is the equity equivalent of S6's vintage-bias pre-commitment.

---

## Survivorship Bias Disclosure (analog to S6 vintage-bias)

**Problem**: Using current S&P 500 constituents for a 15-year backtest excludes
companies that went bankrupt, were acquired, or were removed from the index.
This inflates returns.

**Academic estimate**: +1–3% CAGR upward bias (Brown, Goetzmann & Ross 1992;
Elton et al. 1996).

**Pre-committed haircut**: Subtract 0.10–0.20 from gross excess Sharpe as
survivorship-bias correction. Gross excess Sharpe is an upper bound. The
bias-adjusted estimate is the primary reported metric.

**Mitigation**: Use Wikipedia's S&P 500 historical changes table (additions/
removals since 2000) to reconstruct approximate historical membership. Limitations
documented in Phase B quality report.

---

## Gate Thresholds (pre-registered, benchmark-relative)

Gates are evaluated **relative to SPY**, not relative to zero. Beta is free.
What matters is excess return over the passive benchmark.

| Gate | Threshold | Definition |
|------|-----------|------------|
| **Information Ratio (IR)** | ≥ 0.50 | `annualized(mean(monthly_excess_ret)) / annualized(std(monthly_excess_ret))` |
| **Excess Sharpe** | ≥ 0.30 | Strategy Sharpe − SPY Sharpe over same period |
| **Max active drawdown** | < 15% | Peak-to-trough underperformance vs SPY |

**All three gates must pass.** IR is primary. If all three fail, strategy is killed.

---

## Cost Model (pre-registered)

| Component | Assumption |
|-----------|-----------|
| Commission | $0 (IBKR Lite) |
| Bid-ask spread (one-way) | 0.02% of notional |
| Market impact | Negligible (large-cap S&P 500, small account) |
| Per-rebalance cost | `turnover_fraction × notional × 2 × 0.02%` |

Cost sensitivity at 0× / 0.5× / 1× / 2× required in Phase C. Given near-zero
commissions, costs are expected to be immaterial — this must be confirmed, not assumed.

---

## Pre-Registered Stress Months (locked 2026-04-25)

These are evaluated in Phase D regardless of overall strategy performance.
Selection locked before Phase C is run.

| Month | Event |
|-------|-------|
| 2011-08 | US credit downgrade / S&P 500 −6.3% |
| 2015-08 | China devaluation shock / flash crash |
| 2018-02 | VIX spike, short-vol unwind |
| 2020-03 | COVID crash, S&P 500 −12.5% |
| 2022-01 | Rate-hike repricing, growth selloff |

---

## Pre-Registered Adversarial Periods for Momentum (locked 2026-04-25)

Known momentum crash windows. Strategy must be evaluated on these sub-periods
explicitly in Phase D regime breakdown. These are not cherry-picked post-hoc.

| Period | Event |
|--------|-------|
| 2018-09 to 2018-12 | Q4 2018 momentum crash / factor rotation |
| 2020-05 | Post-COVID momentum reversal (deep value recovery) |

---

## Data Sources

| Data | Source | Notes |
|------|--------|-------|
| Monthly prices | yfinance (`auto_adjust=True`) | Total return (splits + dividends) |
| Quarterly P/B | Simfin API (free tier) | Point-in-time via `PUBLISH_DATE` |
| S&P 500 constituents | Wikipedia + historical changes table | Survivorship bias documented |
| SPY benchmark | yfinance (`SPY`, `auto_adjust=True`) | Total return |

**Simfin API key**: Required. Free registration at simfin.com. Set `SIMFIN_API_KEY`
in `.env`. Pipeline exits cleanly if key missing.

**Phase B kill condition**: If point-in-time P/B coverage < 90% after all fetching
attempts, the value factor is dropped and the strategy reduces to momentum-only.
This decision is made in Phase B before any backtest is run.

---

## Phase Structure

| Phase | Name | Gate | Status |
|-------|------|------|--------|
| A | Specification | Document approved | ✅ Complete (2026-04-25) |
| B | Data pipeline | Price coverage ≥ 95%; P/B coverage ≥ 90% | 🔄 Active |
| C | Backtest | All three gates pass | Not started |
| D | Diagnostic | Attribution, regime, constituent analysis | Not started |
| E | Decision | Live or kill | Not started |

**Stop between every phase. Explicit approval required to proceed.**

---

## Pre-Registration Checklist (must be locked before Phase C)

- [x] Factor definitions and composite weighting
- [x] Survivorship bias correction method (disclosure + haircut)
- [x] Point-in-time rule for P/B (PUBLISH_DATE ≤ rebalance date)
- [x] Rebalancing: last business day of month
- [x] Min filters: market cap ≥ $1B, price ≥ $5
- [x] Cost model: $0 commission + 0.02% one-way bid-ask
- [x] Benchmark: SPY total return
- [x] Stress months: 2011-08, 2015-08, 2018-02, 2020-03, 2022-01
- [x] Momentum adversarial periods: 2018-09 to 2018-12, 2020-05
- [x] No post-hoc direction reversal, factor substitution, or quintile adjustment
- [ ] Phase B data validation complete (gate: price ≥ 95%, P/B ≥ 90%)

---

## Execution Rules (inherited from FX lab)

1. ONE STRATEGY AT A TIME
2. PHASED — explicit approval between phases
3. NO PARAMETER OPTIMIZATION — parameters from literature only
4. COST MODEL IS FIXED — 0.02% one-way. Cannot change to improve results.
5. BENCHMARK-RELATIVE THROUGHOUT — all metrics vs SPY, never absolute Sharpe alone
6. SURVIVORSHIP BIAS PRE-COMMITTED — gross excess Sharpe = upper bound
7. REPRODUCIBILITY — every run saves inputs + config + results to `lab/e1_*/results/`
8. KILL FINDINGS required before synthesis

---

## Infrastructure Reuse from FX Lab

| Component | Reuse | Adaptation |
|-----------|-------|-----------|
| Risk management layer | 100% | Replace OANDA client with IBKR API when live |
| Backtest core loop | 80% | Replace FX cost model; equity total return |
| Lab framework (phases, gates, GitHub pattern) | 100% | None |
| Data pipeline patterns (caching, parquet, quality report) | 70% | Replace OECD/FRED with yfinance/Simfin |

---

## Decision Log

| Date | Decision | Approved by |
|------|----------|-------------|
| 2026-04-25 | Phase A spec approved; Simfin confirmed; stress months + adversarial periods locked | Victor Carranza |

---

## GitHub Issues

_(to be created per strategy as lab progresses)_

## Pull Requests

_(to be created per phase as lab progresses)_
