# Scalperkuy Project Context

This document is the handoff context for future chat sessions. Keep it updated when the project direction changes.

## Goal

Build an AI-assisted crypto scalping research bot for a 24/7 Ubuntu homeserver. Version `v0.1` is strictly for public market data collection and paper-trading research. No live trading is allowed.

Core philosophy:

> Collect truth first, simulate honestly second, use AI for summaries only, and keep live trading locked until the evidence is boringly convincing.

## Current Runtime

- Repository: `hrqstn/scalperkuy`
- Runtime: Docker Compose
- Server target: Ubuntu homeserver
- Database: PostgreSQL
- Dashboard: Streamlit
- Alerting: Discord webhook
- Exchange data source: Tokocrypto public REST API
- Symbols:
  - `BTC/USDT`
  - `ETH/USDT`
- Timezone: `Asia/Jakarta`
- Current mode: `paper`

Services:

- `postgres`: database
- `collector`: active market data collector
- `aggregator`: active 1m market feature aggregator
- `dashboard`: active Streamlit dashboard
- `paper_trader`: active conservative paper trader when `paper_trading.enabled: true`
- `paper_trader`: also acts as the experiment runner for parallel paper strategies on the same market data
- `reporter`: active deterministic journal reporter; Gemini summaries are still disabled

## Hard Rules

- No live trading in `v0.1`.
- No exchange API key with trading permission.
- No leverage.
- No top-up for at least 3 months.
- Collector must keep running even if paper trader hits emergency stop.
- Emergency stop only pauses new paper/live entries, never stops market data collection.
- LLM/Gemini is only for summaries and anomaly explanations.
- LLM/Gemini must not make buy/sell decisions, risk calculations, PnL calculations, or database truth.

## Data Being Collected

For `BTC/USDT` and `ETH/USDT`:

- 1m OHLCV candles
- best bid / best ask quotes
- spread and spread bps
- recent trades
- order book top 20 snapshots

Current intervals:

- candles: 1m timeframe, polled every 30 seconds
- quotes: every 5 seconds
- order book top 20: every 10 seconds
- recent trades: every 30 seconds
- dashboard refresh target: 10 seconds
- stale threshold: 120 seconds

## Implemented So Far

- Docker Compose scaffold.
- PostgreSQL schema.
- Tokocrypto public REST adapter layer.
- Tokocrypto request retry/backoff for temporary DNS/API failures such as 504 gateway timeout.
- Collector storing candles, quotes, recent trades, and order book snapshots.
- Aggregator materializing 1m market features into `market_features_1m`.
- Data quality scoring for 1m features via `quality_score`, `is_tradeable_minute`, and `quality_flags`.
- Conservative paper trader baseline using `micro_momentum_burst_v0`.
- Experiment framework backed by the `experiments` table for multi-strategy paper simulation.
- Trade excursion analysis for closed paper trades using quote-path labels and excursion metrics.
- Deterministic journal entries in `journal_entries`.
- Service health writes with throttled `ok` heartbeat.
- Discord alerts:
  - collector startup
  - collector task error
  - per-feed stale data
  - database/health write failure
  - disk usage warning
- Discord webhook test command:

```bash
docker compose run --rm collector python -m app.reporting.discord_test
```

- Streamlit dashboard:
  - service status
  - disk usage
  - database row counts
  - market data freshness per feed/symbol
  - latest quotes
  - latest candles
  - candle chart
  - paper trading metrics
  - deterministic journal summary

Aggregator currently materializes these 1m features:

- candle OHLCV
- quote count
- trade count
- order book snapshot count
- average mid price
- average/min/max spread
- average spread bps
- buy/sell trade counts
- buy/sell volume
- total trade volume
- trade notional
- trade flow imbalance
- average/min/max order book imbalance
- average top-20 bid depth
- average top-20 ask depth
- 1m volatility bps
- quality score
- tradeable minute flag
- quality flags

This aggregate table is long-term memory for research. It must exist before raw quotes/trades/order book snapshots are purged.

Current data quality flags:

- `missing_candle`
- `low_quote_samples`
- `low_trade_samples`
- `low_order_book_samples`
- `spread_too_wide`
- `volatility_too_low`

Dashboard `System` includes a data quality summary for the last 24 hours. This is the beginning of Phase 1 and will later become a gate for paper trading, experiment selection, labeling, and ML datasets.

## Risk Policy Update

Initial discussion had:

- daily profit target: `1.0%`
- daily max loss: `1.0%`
- risk per trade: `0.1%`

After discussion, daily max loss should be more conservative before paper trader activation:

```yaml
risk:
  daily_profit_target_percent: 1.0
  daily_max_loss_percent: 0.5
  risk_per_trade_percent: 0.1
  max_position_size_percent: 25
  max_trades_per_day: 10
  max_consecutive_losses: 3
  pause_after_consecutive_losses_minutes: 60
  max_spread_bps: 8
```

Reasoning:

- The 1% daily target is a stop target, not a forced daily expectation.
- Do not try to hit 1% in one all-in trade.
- Healthy paper-trading structure should use multiple small trades.
- Early baseline should use `0.1%` risk per trade.
- `0.5%` daily max loss makes survival more important than forcing trades.
- If using `0.2%` risk per trade later, 3 consecutive losses can already exceed `0.5%`, so `0.1%` is better for the first paper baseline.

Strategy direction update:

- Do not use one large trade to chase the full daily target.
- The daily `1.0%` target is a stop target, not a forced target.
- The first active paper trader should focus on small, repeatable scalps.
- EMA-only logic can remain as a simple comparison baseline, but the main hypothesis should use the market microstructure data being collected.

Primary paper-trading hypothesis:

> Micro momentum burst scalping.

This does not try to predict the broad direction 5 minutes into the future. It tries to capture a small, fast movement when short-term order book, trade flow, spread, and price action align.

Long candidate features:

- Spread is tight enough for the target to survive fees and slippage.
- Order book bid-side imbalance is positive, for example bid dominance above roughly `60%`.
- Imbalance should be persistent across recent snapshots, not just one snapshot.
- Aggressive buy flow is increasing in recent trades.
- Buy volume exceeds sell volume in a short recent window.
- Price breaks micro resistance or reclaims a short-term level.
- Short-term volatility is large enough to cover round-trip fee, spread, and slippage.
- Optional higher-level filter: EMA 9 above EMA 21 or similar light trend filter.

Exit policy is more important than entry:

- TP should be small but fee-aware.
- SL should be tight.
- Use a max holding time so stale scalps do not become accidental swing trades.
- Add dynamic exit when momentum fades.
- Dynamic exit examples:
  - spread widens sharply
  - order book imbalance flips against the position
  - aggressive buy flow fades
  - price fails to continue after entry
  - max holding time is reached

Initial TP/SL direction:

- If round-trip fee is around `0.2%`, a `0.2%` gross target is close to break-even before spread/slippage.
- Paper trading must calculate gross and net PnL separately.
- TP gross should likely start around `0.35% - 0.50%` if fees are high.
- SL gross should likely start around `0.15% - 0.25%`.
- If actual fees are lower, TP/SL can be revisited.
- Risk per trade should still start at `0.1%` of equity.
- Daily max loss should remain `0.5%`.
- Max trades/day should start at `10` or lower if overtrading appears.

Known scalping risks:

- Fees can erase small gross targets.
- Spread and slippage can turn a good-looking trade into a bad net trade.
- Order book imbalance can be spoofed.
- Order book features must not be used alone; require actual trade flow and price confirmation.
- Overtrading can destroy edge even if individual signals look reasonable.
- Market regime can change quickly.

Anti-overtrading guardrails:

- one open position per symbol max
- max trades/day
- max consecutive losses
- cooldown after any trade
- longer cooldown after a loss
- block entry if spread is too wide
- block entry if volatility is too low to cover costs
- block entry if TP/SL is missing

Simple EMA comparison baseline:

- long-only
- EMA 9 above EMA 21
- recent volume above rolling average
- spread below threshold
- price pulls back near EMA 9
- fixed or volatility-based TP/SL

The EMA baseline is useful as a benchmark, but it should not be treated as the main reason this project collects order book and trade-flow data.

All signals, decisions, features, entries, exits, fees, spread, slippage estimates, and skip reasons must be stored.

## Storage And Retention Direction

Do not purge raw data immediately. First run the collector and measure disk growth.

Rough estimate for current config:

- 100-250 MB/day realistic
- 3-8 GB/month
- 9-24 GB/90 days
- with PostgreSQL/index/WAL overhead, budget 15-35 GB for 90 days

Retention direction after aggregation exists:

- candles: keep long term
- paper signals/trades: keep long term
- daily performance: keep long term
- feature/label datasets: keep long term
- quote aggregates 1m/5m: keep long term
- trade aggregates 1m/5m: keep long term
- order book aggregates 1m/5m: keep long term
- raw quotes: keep 90 days
- raw trades: keep 90 days
- raw order book snapshots: start with 30 days
- service health: 14-30 days

Important: raw data can be deleted after 30-90 days only if derived aggregates/features/labels have already been materialized.

Paper trader honesty rules:

- When unsure whether an order would fill in real market conditions, assume it did not fill.
- For long entries, use ask price plus slippage, not mid/last.
- For long exits, use bid price minus slippage, not mid/last.
- If TP and SL are both touched in the same candle and trade-level ordering cannot prove TP came first, assume SL first.
- If spread is too wide, skip.
- If order book liquidity is insufficient, skip or simulate partial fill.
- If market data is stale, skip.
- Always calculate gross PnL and net PnL separately.
- Net PnL must include fees, spread cost, and slippage estimate.
- It is better to underestimate paper profit than to discover fake edge in live trading.

Current paper trader v0 baseline:

- Enabled through `paper_trading.enabled`.
- Uses `market_features_1m` and latest quote.
- Long-only.
- One open position per symbol.
- Entry price: ask plus slippage.
- Exit price: bid minus slippage.
- TP/SL configured in bps.
- Dynamic exits:
  - TP
  - SL
  - max holding time
  - trade flow turns negative
  - order book imbalance turns negative
- Daily risk manager blocks entries after:
  - daily max loss
  - daily profit target
  - max trades/day
  - max consecutive losses
  - spread too wide
- Signals are written to `paper_signals`.
- Trades are written to `paper_trades`.

Current experiment framework:

- `experiments` stores named experiment metadata plus the resolved paper/risk config snapshot.
- `paper_signals` records `experiment_id` and `experiment_name`.
- `paper_trades` records `experiment_id`, `experiment_name`, and `strategy_name`.
- Cooldowns, daily stats, and consecutive-loss checks are isolated per experiment.
- Multiple experiments can read the same market data while keeping separate paper histories.
- Dashboard `Paper Trading` supports experiment filtering and experiment comparison.
- Journal summaries now include experiment breakdown lines.
- Closed paper trades are now labeled with:
  - `gross_pnl_idr`
  - `gross_pnl_percent`
  - `hold_seconds`
  - `max_favorable_excursion_bps`
  - `max_adverse_excursion_bps`
  - `horizon_3m_label`
  - `horizon_5m_label`
  - `horizon_10m_label`
  - `label_source`
- Label source is currently quote-based best-bid path analysis. This is more honest than candle-only approximation for long exits, but still approximate because it depends on polling cadence.

Current default experiment set:

- `micro_burst_strict_v0`: current conservative micro-burst baseline.
- `micro_burst_loose_v0`: lower flow/imbalance thresholds with slightly larger TP.
- `micro_burst_no_dynamic_exit_v0`: disables momentum/orderbook dynamic exits and lets TP/SL plus max holding do more work.
- `ema_baseline_v0`: simple EMA comparison benchmark with a lighter daily trade budget.

Current journal v0:

- Reporter writes deterministic daily research summaries to `journal_entries`.
- Reporter updates the current day every `reporting.interval_seconds`.
- Gemini is not used in journal v0.
- Journal includes:
  - market data freshness
  - trade count
  - realized PnL
  - win rate
  - profit factor
  - fee/slippage estimate
  - signal outcome breakdown
  - exit reason breakdown
  - sample-size warning
- Dashboard `Journal` page displays the latest summary and recent entries.

## Live Observations

Keep this section updated with important observations from real paper runs.

### 2026-04-21 Initial Paper Trades

First observed paper trades after enabling `micro_momentum_burst_v0`:

- 2 closed trades.
- 0 wins, 2 losses.
- Realized PnL around `-Rp953`.
- Fees around `Rp1,000`.
- Slippage estimate around `Rp200`.
- Both trades exited via `momentum_faded`.
- BTC trade:
  - entered around `76,323.2516`
  - exited around `76,314.724`
  - net PnL around `-Rp627.93`
  - exit reason: `momentum_faded`
- ETH trade:
  - entered around `2,317.5034`
  - exited around `2,320.0559`
  - net PnL around `-Rp324.65`
  - exit reason: `momentum_faded`
  - gross price move was favorable, but net result was still negative because fee/slippage dominated.

Interpretation:

- Do not tune the strategy based on only 2 trades.
- Early evidence confirms why conservative paper simulation matters.
- Fee/slippage drag is material relative to small scalping targets.
- `momentum_faded` dynamic exit may be too sensitive, but wait for at least `10-20` closed trades before changing it.
- If many losses exit via `momentum_faded`, consider requiring momentum fade confirmation for 2-3 minutes or combining it with price weakness.
- If many trades are small gross winners but net losers, revisit fee assumptions, TP bps, minimum volatility, and expected edge filter.

### 2026-04-22 Morning Check

Observed from dashboard around morning Asia/Jakarta:

- All core services were `ok`: collector, aggregator, paper_trader, reporter.
- Market data freshness was `fresh` for candles, quotes, trades, and order book.
- Database rows were growing:
  - `market_candles`: around `8,675`
  - `market_features_1m`: around `7,879`
  - `market_quotes`: around `90,036`
  - `market_trades`: around `824,583`
  - `order_book_snapshots`: around `46,605`
  - `paper_signals`: around `4,858`
  - `paper_trades`: `4`
  - `journal_entries`: `2`
- Paper trading had `4` closed trades, all losses.
- Recent trades showed BTC/ETH long entries exited with net losses.
- Signal summary last 24h:
  - `trade flow not bullish enough`: `1,026`
  - `not enough quote samples`: `936`
  - `not enough order book samples`: `866`
  - `volatility too low to cover costs`: `560`
  - `order book imbalance not bullish enough`: `442`
  - `not enough trade samples`: `396`
  - `stale market data`: `292`
  - `max consecutive losses reached`: `212`
  - `cooldown active`: `120`
  - `TAKE`: `4`

Interpretation:

- Collector/aggregator health is good.
- Paper trader is conservative and mostly skipping.
- `max consecutive losses reached` is expected after early losses and protects the account.
- `not enough quote/order book/trade samples` was too frequent. This is likely because paper trader read the currently forming 1m feature bucket. Code was changed so paper trader reads only the latest completed 1m feature.
- `max consecutive losses reached` was later identified as too sticky because consecutive loss counting was global. It should reset by trading date in `Asia/Jakarta`. Code was changed so consecutive losses only count closed trades from the current trading day.
- Do not tune strategy edge yet; first reduce timing/noise skips and collect more closed trades.

### 2026-04-28 Experiment Review

Observed from dashboard after several days of experiment runs:

- `experiments`: `4`
- `paper_signals`: around `74,719`
- `paper_trades`: around `66`
- `market_features_1m`: around `20,785`
- `market_trades`: around `2,022,915`

Experiment comparison snapshot:

- `ema_baseline_v0`: `12` closed trades, realized PnL around `-Rp8,163.86`
- `micro_burst_strict_v0`: `12` closed trades, realized PnL around `-Rp8,382.80`
- `micro_burst_no_dynamic_exit_v0`: `13` closed trades, realized PnL around `-Rp8,613.57`
- `micro_burst_loose_v0`: `13` closed trades, realized PnL around `-Rp8,911.32`
- historical `legacy_single_strategy`: `16` closed trades, realized PnL around `-Rp11,638.76`

Interpretation:

- Pipeline stability is good enough.
- Experiment framework is working.
- Current strategy set still shows no edge.
- Dynamic exit is not the only issue because the no-dynamic-exit variant also loses.
- Entry quality is likely the main problem, not just exit timing.
- This review triggered the next implementation step:
  - excursion labeling
  - gross-vs-net move analysis
  - per-experiment hold/exit breakdown
  - retiring `legacy_single_strategy` from main comparison views

Bot evolution loop:

1. Collect raw market data.
2. Aggregate raw data into 1m/5m features.
3. Create transparent strategy hypotheses.
4. Simulate with conservative fills and costs.
5. Label outcomes such as TP-before-SL.
6. Evaluate win rate, profit factor, drawdown, fee drag, market regime, and weekly stability.
7. Improve or discard strategies based on evidence.
8. Repeat. ML can later become a filter, not a replacement for deterministic risk management.

## How To Explain The Project

This is not a bot that lets AI randomly create buy/sell signals. It is a data lab for crypto scalping research.

The system collects real market data, runs transparent paper-trading hypotheses, measures whether TP is hit before SL after fees/spread/slippage, and only later uses ML as a filter if the dataset proves there is something worth learning.

If asked who determines the strategy:

- Human-controlled research process determines the strategy.
- Rule-based baselines create trade candidates.
- Data validates or rejects the hypothesis.
- ML may later filter candidates, but does not replace deterministic risk management.
- If the data does not show edge, there is no live trading.

If asked whether the system will have its own strategy after 3 months:

- Yes, the goal is to build a strategy informed by our own collected data.
- No, it will not be AI freely inventing signals.
- Strategies must pass paper trading, walk-forward validation, and strict risk checks.

## Next Steps

Before enabling paper trader:

1. Let collector run overnight / 12-24 hours.
2. Check dashboard `System` page:
   - collector status is `ok`
   - freshness is `fresh` for all feeds/symbols
   - no stale/error events
3. Check DB table sizes:

```bash
docker compose exec postgres psql -U potatotan -d scalperkuy -c "
select
  relname as table_name,
  pg_size_pretty(pg_total_relation_size(relid)) as total_size
from pg_catalog.pg_statio_user_tables
order by pg_total_relation_size(relid) desc;
"
```

Then implement paper trader baseline:

1. Read latest candles, quotes, recent trades, and order book snapshots.
2. Generate rule-based micro momentum burst signal.
3. Enforce deterministic risk manager.
4. Simulate entries/exits.
5. Estimate fees, spread, and slippage.
6. Write `paper_signals` and `paper_trades`.
7. Daily stop/profit target/consecutive-loss stop blocks new entries only.
8. Collector must continue regardless of paper trader state.

Reporter should come after paper trader has useful data:

1. Start with deterministic daily summary.
2. Add Gemini only for narrative summaries later.
3. Never let Gemini calculate or decide trades.

## Calendar Roadmap

Roadmap starts from Wednesday, 2026-04-22. These dates are planning anchors, not hard promises. Move a phase later if data quality, reliability, or paper-trading honesty is not ready.

### 2026-04-22 to 2026-04-28: Phase 0, Pipeline Stability

Focus:

- collector, aggregator, paper trader, reporter stable
- dashboard and journal running
- freshness, errors, and disk growth monitored
- no aggressive strategy tuning yet

Milestone review: 2026-04-28.

### 2026-04-29 to 2026-05-05: Phase 1, Data Quality Layer

Focus:

- quality score per 1m feature
- missing sample detection
- spread outlier detection
- stale/gap flags
- dashboard data quality panel

Milestone review: 2026-05-05.

### 2026-05-06 to 2026-05-15: Phase 2, Paper Trader v0.2

Focus:

- better honest fill model
- dynamic exit confirmation
- conservative TP/SL same-candle handling
- fee/slippage review
- daily reset and risk validation

Milestone review: 2026-05-15.

### 2026-05-16 to 2026-05-29: Phase 3, Experiment Framework

Focus:

- `experiment_id`
- multi-strategy paper simulation
- compare strict, loose, no dynamic exit, and EMA baseline variants
- dashboard comparison

Milestone review: 2026-05-29.

Status update:

- The first experiment framework is already implemented ahead of the review date.
- Remaining work in this phase is to evaluate results, add or remove variants based on evidence, and improve honest-fill behavior for the experiments worth keeping.

### 2026-05-30 to 2026-06-12: Phase 4, Aggregation v2 And Retention

Focus:

- `market_features_5m`
- long-term aggregates
- retention jobs
- disk safety

Milestone review: 2026-06-12.

### 2026-06-13 to 2026-06-26: Phase 5, Labeling

Focus:

- trade candidates
- TP-before-SL labels
- conservative label logic
- candidate dataset

Milestone review: 2026-06-26.

### 2026-06-27 to 2026-07-10: Phase 6, Strategy Evaluation

Focus:

- profit factor
- expectancy
- drawdown
- fee drag
- performance by hour/regime
- pick promising strategies

Milestone review: 2026-07-10.

### 2026-07-11 to 2026-08-07: Phase 7, ML Baseline

Focus:

- Logistic Regression baseline
- LightGBM filter
- compare rule-only vs ML-filtered
- no live trading

Milestone review: 2026-08-07.

### 2026-08-08 to 2026-08-28: Phase 8, Walk-forward Validation

Focus:

- rolling train/validation
- calibration
- regime stability
- reject overfit models

Milestone review: 2026-08-28.

### 2026-08-29 to 2026-09-25: Phase 9, Paper Auto Serious Mode

Focus:

- best strategy/filter paper auto
- monitor 2-4 weeks
- compare journal vs dashboard vs DB
- no live trading

Milestone review: 2026-09-25.

### 2026-09-26 to 2026-10-23: Phase 10, Live Tiny Review Only

Focus:

- review 3-6 months evidence
- decide whether live tiny is even justified
- if metrics are not boringly convincing, continue paper

Milestone review: 2026-10-23.

Important review dates:

- 2026-04-28: pipeline stability review
- 2026-05-05: data quality review
- 2026-05-15: paper trader v0.2 review
- 2026-05-29: experiment framework review
- 2026-06-12: retention/aggregation review
- 2026-06-26: labeling review
- 2026-07-10: strategy evaluation review
- 2026-08-07: ML baseline review
- 2026-08-28: walk-forward review
- 2026-09-25: paper auto review
- 2026-10-23: live tiny eligibility review

Suggested calendar habit:

- Weekly Scalperkuy research review every Friday around 12:00 Asia/Jakarta.
- Bring dashboard screenshots, journal summary, Discord alerts, and the combined DB status query output.
