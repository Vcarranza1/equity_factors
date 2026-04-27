# E1 — Momentum-Only Equity Factor: Kill Findings

**Status**: KILLED — Phase C gate failure  
**Decision date**: 2026-04-27  
**Branch**: `main`  
**Period tested**: Mar 2010 – Apr 2026 (194 months)

---

## Executive Summary

E1 failed Phase C validation on the primary gate. Excess Sharpe of +0.208 is
below the pre-registered threshold of +0.30. The strategy generates real positive
alpha — IR 0.762, positive excess return in every multi-year regime — but not
enough to clear the gate after the pre-committed survivorship-bias haircut of
−0.10 to −0.20 Sharpe.

Once the bias correction is applied, the bias-adjusted excess Sharpe range is
**[+0.008, +0.108]** — essentially zero to marginal. This is not a deployable edge.

Three post-hoc variations clear the gate (sector-neutral momentum, quarterly
rebalancing, sector-neutral top 10%). Per pre-commitment discipline, none of
these are reformulations. They are documented here as research observations only.

---

## Pre-Registered Specification

- Universe: S&P 500 current constituents (~503 tickers, 502 with price data)
- Factor: 12-1 month momentum only (value factor dropped in Phase B, P/B coverage
  74.8% < 90% gate)
- Selection: top quintile (top 20%, ~92 stocks average)
- Weighting: equal-weight
- Rebalancing: monthly, last business day
- Benchmark: SPY total return (auto-adjusted)
- Cost: $0 commission + 0.02% one-way bid-ask

**Point-in-time pre-commitment**: All signals computed using data available at
rebalance date T. Momentum uses price[end of M-1] / price[end of M-13] − 1,
skipping the most recent month (reversal avoidance).

**Direction lock**: Long top-quintile = long high-momentum stocks. Fixed before
Phase C. Post-hoc reversals prohibited.

---

## Validation Gates

| Gate | Threshold | Result | Status |
|------|-----------|--------|--------|
| Excess Sharpe | ≥ 0.30 | **+0.208** | ✗ FAIL |
| Information Ratio | ≥ 0.50 | **0.762** | ✓ PASS |
| Max active drawdown | < 15% | **−13.4%** | ✓ PASS |
| **Overall** | all three | | **✗ FAIL** |

---

## Phase C Backtest Results

**Period**: Mar 2010 – Apr 2026 (194 months)  
**Universe**: ~502 tickers (current S&P 500)  
**Avg portfolio size**: 92.3 stocks  
**Avg monthly turnover**: 22.9%

### Headline metrics

| Scenario | Sharpe | SPY Sharpe | Excess Sharpe | IR | Max Active DD | Total Return |
|----------|--------|-----------|--------------|-----|--------------|-------------|
| Gross (0× cost) | 1.197 | 0.982 | 0.215 | 0.779 | −13.3% | +1,980% |
| 0.5× cost | 1.193 | 0.982 | 0.212 | 0.770 | −13.3% | +1,959% |
| **1× cost (baseline)** | **1.190** | **0.982** | **0.208** | **0.762** | **−13.4%** | **+1,939%** |
| 2× cost | 1.183 | 0.982 | 0.201 | 0.746 | −13.5% | +1,900% |

**Costs are not the cause of failure.** Excess Sharpe at zero cost (0.215) vs
baseline (0.208) differ by 0.007. The strategy is structurally different from the
FX lab failures where gross Sharpe also failed — here, positive alpha exists but
is insufficient.

### Survivorship bias correction (pre-committed)

Using current S&P 500 constituents for a 15-year backtest excludes companies
removed from the index (bankruptcies, acquisitions, demotions). Pre-committed
haircut: −0.10 to −0.20 excess Sharpe.

| Haircut | Bias-adjusted excess Sharpe | Assessment |
|---------|---------------------------|------------|
| −0.10 (low) | +0.108 | Below gate |
| −0.15 (mid) | +0.058 | Well below gate |
| −0.20 (high) | +0.008 | Essentially zero |

**The strategy does not survive the survivorship-bias correction.** The gross
excess Sharpe of +0.208 is an upper bound.

---

## Phase D Diagnostics

### D1 — Regime Breakdown

| Period | N | Strat Sharpe | SPY Sharpe | Excess Sharpe | Cum Excess |
|--------|---|-------------|-----------|--------------|-----------|
| 2010–2013 | 46 | 1.446 | 1.147 | **+1.466** | +39.1% |
| 2014–2016 | 36 | 0.884 | 0.826 | +0.122 | +1.9% |
| 2017–2019 | 36 | 1.344 | 1.202 | +0.520 | +11.3% |
| 2020–2022 | 36 | 0.774 | 0.447 | **+1.040** | +23.2% |
| 2023–2026 | 40 | 1.507 | 1.637 | +0.722 | +24.1% |

**Key finding**: Every multi-year regime has positive excess Sharpe. The strategy
is not a regime-dependent bet that happened to work in one period — the alpha
appears consistently, just too small to clear the gate in aggregate.

The weakest regime is 2014–2016 (+0.122 excess Sharpe, +1.9% cum excess).
2016 specifically was −0.535 excess Sharpe — the Trump election rotation into
energy/financials and away from tech/momentum stocks was the single worst year.

### D2 — Sector Attribution

Sector attribution of total cumulative excess return contribution (2010–2026):

| Sector | Portfolio Wt | S&P 500 Wt | Overweight | Contrib |
|--------|-------------|-----------|-----------|---------|
| Information Technology | 18.7% | 14.5% | **+4.2%** | **+41.3%** |
| Industrials | 15.6% | 15.7% | −0.1% | +19.3% |
| Consumer Discretionary | 13.2% | 9.5% | **+3.7%** | +19.2% |
| Communication Services | 5.2% | 4.6% | +0.7% | +6.3% |
| Health Care | 11.6% | 11.5% | +0.1% | +5.7% |
| Energy | 6.2% | 4.4% | +1.8% | +4.8% |
| Materials | 4.3% | 5.2% | −0.9% | +2.9% |
| Financials | 12.2% | 15.1% | −2.9% | +1.3% |
| Real Estate | 5.6% | 6.2% | −0.6% | +0.4% |
| Consumer Staples | 6.0% | 7.2% | −1.2% | −1.6% |
| Utilities | 5.0% | 6.2% | −1.1% | **−4.8%** |

**Key finding**: Over 60% of cumulative alpha comes from IT and Consumer
Discretionary. The strategy systematically overweights high-momentum tech and
consumer growth stocks and underweights low-momentum defensive sectors (Utilities,
Consumer Staples, Financials).

This alpha is real but has a structural character: the 16-year backtest period
(2010–2026) was an extended tech/growth bull market. Whether this sector tilt
persists in a different macro regime is not established by this backtest.

Utilities being −4.8% is notable: utilities are typically low-momentum and
persistently rank low. The strategy systematically shorts them implicitly by
underweighting — this is a permanent characteristic, not a variable signal.

### D3 — Post-hoc Variation Analysis

**DISCIPLINE NOTE**: All results below were discovered after Phase C results were
seen. Per pre-commitment (same rule as S4 reversed direction +0.57, S6 short-leg
+0.36, S6 CPI-only +0.35): none of these are reformulations. Pursuing any of them
would require a new independent lab cycle with pre-registered specification.

| Variation | Sharpe | Excess Sharpe | IR | Clears Gate? |
|-----------|--------|--------------|-----|-------------|
| Baseline (12-1, top 20%) | 1.197 | 0.215 | 0.779 | ✗ No |
| **Sector-neutral (12-1, top 20% per sector)** | **1.290** | **0.308** | **0.973** | **✓ Yes** |
| Top 10% (decile) | 1.243 | 0.261 | 0.963 | ✗ No |
| 6-1 month lookback (top 20%) | 1.224 | 0.242 | 0.752 | ✗ No |
| Quarterly rebalancing† | 0.387 | 0.389 | 1.001 | ✓ Yes (caution) |
| Sector-neutral top 10% | 1.282 | 0.300 | 0.962 | ✓ barely |

†**Quarterly rebalancing caution**: Strategy Sharpe drops to 0.387 (vs 1.197
baseline) while excess Sharpe is +0.389. This anomaly arises because the quarterly
strategy only holds positions in months Feb/May/Aug/Nov — SPY itself had lower
Sharpe in those specific months during this period. The apparent excess Sharpe
improvement is a calendar-selection artifact, not evidence of a better strategy.

**Sector-neutral momentum** is the most interesting post-hoc observation.
Excess Sharpe +0.308, IR 0.973 — both clearly above gate. The economic rationale
is sound: by ranking within sectors, the strategy removes sector timing exposure
(the IT/Utilities permanent tilt) and captures pure stock-level momentum alpha.
This is a well-established academic refinement (Grundy & Martin 2001; Moskowitz &
Grinblatt 1999). However: it was not pre-registered, and its sector-neutral excess
Sharpe was discovered by observing that the baseline failed and searching for
improvements. Pre-commitment applies. Not actionable within E1.

### D4 — Monthly Return Distribution

| Statistic | Value |
|-----------|-------|
| Months beating SPY | 59.3% |
| Mean monthly excess | +0.479% |
| Std monthly excess | 2.175% |
| Skew / excess kurtosis | +0.077 / +0.059 |
| Worst excess month | −5.13% (2023-01) |
| Best excess month | +7.48% (2024-02) |
| Avg turnover/month | 22.9% |
| Avg portfolio size | 92.3 stocks |

Near-symmetric distribution (slight positive skew). 59.3% win rate vs SPY is
consistent with a real but thin edge. Turnover at 22.9%/month is manageable at
IBKR $0 commissions — costs are genuinely negligible.

### Stress months (pre-registered)

| Month | Strategy | SPY | Excess | Event | Pass/Fail |
|-------|---------|-----|--------|-------|----------|
| 2011-08 | −8.1% | −5.5% | −2.6% | US credit downgrade | ✗ FAIL |
| 2015-08 | −5.3% | −6.1% | +0.8% | China devaluation | ✓ PASS |
| 2018-02 | −2.4% | −3.6% | +1.2% | VIX spike | ✓ PASS |
| 2020-03 | −14.5% | −13.0% | −1.5% | COVID crash | ✗ FAIL |
| 2022-01 | −5.9% | −4.9% | −0.9% | Rate-hike repricing | ✗ FAIL |

3/5 stress months lost to SPY. The pattern: momentum strategies trail SPY in
sharp market-wide drawdowns (2011-08, 2020-03) because high-momentum stocks have
above-average beta and fall more. They outperform in more localized volatility
events (2015-08 China, 2018-02 VIX spike) where the sector tilt is less relevant.

### Pre-registered adversarial periods

| Period | Strategy cum | SPY cum | Excess cum | Pass/Fail |
|--------|-------------|---------|-----------|----------|
| Q4 2018 momentum crash (4 months) | −16.9% | −13.5% | −3.4% | ✗ FAIL |
| Post-COVID reversal (2020-05) | +7.5% | +4.8% | +2.8% | ✓ PASS |

Q4 2018 confirms the known momentum crash risk: when factor crowding unwinds,
high-momentum stocks fall harder than the market. The −3.4% cumulative excess
over 4 months is consistent with momentum crash literature (Daniel & Moskowitz 2016).

---

## Comparison to FX Lab

| | FX lab (best strategy, S6) | E1 Equity Momentum |
|--|--------------------------|-------------------|
| Gross excess Sharpe | +0.153 (vs zero) | +0.215 (vs SPY) |
| After bias correction | −0.047 (mid) | +0.058 (mid) |
| Alpha real? | No — gross also fails | Yes — IR 0.762, positive every regime |
| Cause of failure | No edge, costs immaterial | Edge exists, insufficient vs gate |
| Post-hoc sub-strategy | S6 CPI-only +0.35 (insufficient) | Sector-neutral +0.308 (clears gate) |

This is a meaningfully different failure mode. The FX strategies had near-zero
gross alpha. E1 has real gross alpha (+0.215 excess Sharpe, IR 0.762), but:
1. Survivorship bias correction erases it (bias-adjusted mid: +0.058)
2. The alpha is structurally concentrated in IT/Consumer Discretionary, suggesting
   a regime-sensitive tech growth tilt rather than universal momentum edge

---

## Decision

**E1 KILLED — Phase C gate failure.**

Gates failed: Excess Sharpe +0.208 < 0.30 | Overall ✗

Survivorship bias mid-correction: +0.208 − 0.15 = +0.058 — no deployable edge.

No reformulation. No parameter adjustment. E1 is closed.

---

## What Would Need to Change for a Positive Result

For reference only — not an approved next step:

1. **Sector-neutral specification** (pre-registered from the start): addresses the
   IT/Utilities permanent tilt and captures purer stock-level momentum. Excess
   Sharpe +0.308 post-hoc, but requires independent out-of-sample validation.

2. **True point-in-time universe construction**: eliminating survivorship bias
   entirely (CRSP historical constituents, ~$30K/year institutional data) would
   make the bias correction unnecessary. At +0.308 excess Sharpe sector-neutral,
   even a −0.10 haircut would leave +0.208 — still below gate.

3. **Combined factor specification**: value + momentum together have stronger
   post-2015 evidence than momentum alone, particularly when value is defined by
   earnings yield rather than P/B. Requires re-running Phase B with a corrected
   data source.

These are research directions, not approved next steps. Each would require a new
Phase A spec and independent data validation.
