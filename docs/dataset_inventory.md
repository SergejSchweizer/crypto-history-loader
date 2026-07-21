# Dataset Inventory

Generated from local lake data on 2026-07-21 Europe/Amsterdam.
Builder commit: `ce8b481fa816ff857e169af943f3f2a49f6672b9`.

Missing days are counted per series between that series' first and last observed day. Option and instrument-heavy snapshot datasets use the path-level currency or aggregate coverage grain instead of volatile per-instrument membership.

| Layer | Dataset | Files | Series | Start | End | Expected days | Observed days | Missing days | Timestamp source | Per-series missing summary |
|---|---|---:|---:|---|---|---:|---:|---:|---|---|
| Bronze | `funding` | 6860 | 3 | 2019-04-30 | 2026-07-18 | 6860 | 6860 | 0 | `partition:date` | BTC-PERPETUAL=0; ETH-PERPETUAL=0; SOL-PERPETUAL=0 |
| Bronze | `futures_instrument_metadata_snapshot_daily` | 39 | 1 | 2026-06-13 | 2026-07-21 | 39 | 39 | 0 | `partition:date` | aggregate=0 |
| Bronze | `futures_summary_snapshot_1m` | 5608 | 3 | 2026-06-12 | 2026-07-21 | 120 | 120 | 0 | `partition:date` | BTC=0; ETH=0; SOL=0 |
| Bronze | `historical_volatility` | 51 | 3 | 2026-05-08 | 2026-05-24 | 51 | 51 | 0 | `partition:date` | BTC=0; ETH=0; SOL=0 |
| Bronze | `index_price_snapshot_1m` | 4188 | 3 | 2026-05-24 | 2026-07-21 | 177 | 177 | 0 | `partition:date` | btc_usd=0; eth_usd=0; sol_usdc=0 |
| Bronze | `instrument_metadata_snapshot_daily` | 58 | 1 | 2026-05-25 | 2026-07-21 | 58 | 58 | 0 | `partition:date` | aggregate=0 |
| Bronze | `open_interest` | 7164 | 3 | 2018-08-15 | 2026-07-18 | 7164 | 7164 | 0 | `partition:date` | BTC-PERPETUAL=0; ETH-PERPETUAL=0; SOL-PERPETUAL=0 |
| Bronze | `options_instrument_ticker_snapshot_1m` | 230435 | 3 | 2026-06-12 | 2026-07-21 | 120 | 120 | 0 | `partition:date` | BTC=0; ETH=0; SOL=0 |
| Bronze | `options_l2_snapshot_1m` | 1311 | 3 | 2026-07-03 | 2026-07-21 | 57 | 57 | 0 | `partition:date` | BTC=0; ETH=0; SOL=0 |
| Bronze | `options_ticker_snapshot_1m` | 4197 | 3 | 2026-05-24 | 2026-07-21 | 177 | 177 | 0 | `partition:date` | BTC=0; ETH=0; SOL=0 |
| Bronze | `options_trades` | 5814 | 3 | 2018-08-14 | 2026-07-18 | 5814 | 5814 | 0 | `partition:date` | BTC=0; ETH=0; SOL=0 |
| Bronze | `perps_l2_snapshot_1m` | 5541 | 3 | 2026-05-05 | 2026-07-21 | 234 | 234 | 0 | `partition:date` | BTC-PERPETUAL=0; ETH-PERPETUAL=0; SOL_USDC-PERPETUAL=0 |
| Bronze | `perps_ohlcv` | 7122 | 3 | 2018-08-14 | 2026-07-18 | 7122 | 7122 | 0 | `partition:date` | BTC-PERPETUAL=0; ETH-PERPETUAL=0; SOL-PERPETUAL=0 |
| Bronze | `perps_trades` | 5832 | 3 | 2018-08-14 | 2026-07-21 | 5832 | 5832 | 0 | `partition:date` | BTC-PERPETUAL=0; ETH-PERPETUAL=0; SOL-PERPETUAL=0 |
| Bronze | `recent_trade_snapshot_1m` | 7457 | 3 | 2026-06-12 | 2026-07-21 | 120 | 120 | 0 | `partition:date` | BTC=0; ETH=0; SOL=0 |
| Bronze | `spot_ohlcv` | 3237 | 3 | 2023-04-24 | 2026-07-18 | 3237 | 3237 | 0 | `partition:date` | BTC_USDC=0; ETH_USDC=0; SOL_USDC=0 |
| Bronze | `volatility_index_data` | 83 | 3 | 2022-11-07 | 2026-05-25 | 83 | 83 | 0 | `partition:date` | BTC=0; ETH=0; SOL=0 |
| Bronze | `volatility_index_snapshot_1m` | 1870 | 2 | 2026-06-12 | 2026-07-21 | 80 | 80 | 0 | `partition:date` | BTC=0; ETH=0 |
| Silver | `funding_1m_feature` | 226 | 3 | 2019-04-01 | 2026-06-24 | 6861 | 6861 | 0 | `timestamp` | BTC-PERPETUAL=0; ETH-PERPETUAL=0; SOL-PERPETUAL=0 |
| Silver | `funding_observed` | 226 | 3 | 2019-04-30 | 2026-06-24 | 6788 | 6788 | 0 | `funding_time` | BTC-PERPETUAL=0; ETH-PERPETUAL=0; SOL-PERPETUAL=0 |
| Silver | `iv_rv_1m_feature` | 5 | 3 | 2022-11-07 | 2026-05-25 | 83 | 83 | 0 | `timestamp_m1` | BTC=0; ETH=0; SOL=0 |
| Silver | `open_interest_1m_feature` | 235 | 3 | 2018-08-01 | 2026-06-24 | 7135 | 7135 | 0 | `timestamp_m1` | BTC-PERPETUAL=0; ETH-PERPETUAL=0; SOL-PERPETUAL=0 |
| Silver | `open_interest_observed` | 235 | 3 | 2018-08-15 | 2026-06-24 | 7092 | 7092 | 0 | `timestamp` | BTC-PERPETUAL=0; ETH-PERPETUAL=0; SOL-PERPETUAL=0 |
| Silver | `options_l2_1m_feature` | 2 | 2 | 2026-07-03 | 2026-07-14 | 24 | 24 | 0 | `timestamp_m1` | BTC=0; ETH=0 |
| Silver | `options_l2_snapshot_1m_observed` | 2 | 2 | 2026-07-03 | 2026-07-14 | 24 | 24 | 0 | `timestamp` | BTC=0; ETH=0 |
| Silver | `options_trades_1m_feature` | 186 | 3 | 2018-08-14 | 2026-06-25 | 5754 | 5570 | 184 | `timestamp_m1` | BTC=0; ETH=184; SOL=0 |
| Silver | `options_trades_observed` | 186 | 3 | 2018-08-14 | 2026-06-25 | 5754 | 5570 | 184 | `trade_time` | BTC=0; ETH=184; SOL=0 |
| Silver | `perps_l2_1m_feature` | 9 | 3 | 2026-05-05 | 2026-07-14 | 213 | 213 | 0 | `timestamp_m1` | BTC-PERPETUAL=0; ETH-PERPETUAL=0; SOL_USDC-PERPETUAL=0 |
| Silver | `perps_l2_snapshot_1m_observed` | 9 | 3 | 2026-05-05 | 2026-07-14 | 213 | 213 | 0 | `timestamp` | BTC-PERPETUAL=0; ETH-PERPETUAL=0; SOL_USDC-PERPETUAL=0 |
| Silver | `perps_ohlcv` | 234 | 3 | 2018-08-14 | 2026-06-25 | 7053 | 7053 | 0 | `open_time` | BTC-PERPETUAL=0; ETH-PERPETUAL=0; SOL-PERPETUAL=0 |
| Silver | `perps_trades_1m_feature` | 194 | 3 | 2018-08-14 | 2026-07-21 | 5832 | 5832 | 0 | `timestamp_m1` | BTC-PERPETUAL=0; ETH-PERPETUAL=0; SOL-PERPETUAL=0 |
| Silver | `perps_trades_observed` | 194 | 3 | 2018-08-14 | 2026-07-21 | 5832 | 5832 | 0 | `trade_time` | BTC-PERPETUAL=0; ETH-PERPETUAL=0; SOL-PERPETUAL=0 |
| Silver | `realized_volatility_1m_feature` | 234 | 3 | 2018-08-14 | 2026-06-25 | 7053 | 7053 | 0 | `timestamp_m1` | BTC=0; ETH=0; SOL=0 |
| Silver | `spot_ohlcv` | 107 | 3 | 2023-04-24 | 2026-06-25 | 3168 | 3168 | 0 | `open_time` | BTC_USDC=0; ETH_USDC=0; SOL_USDC=0 |
| Silver | `volatility_index_1m_feature` | 5 | 3 | 2022-11-07 | 2026-05-25 | 83 | 83 | 0 | `timestamp_m1` | BTC=0; ETH=0; SOL=0 |
| Silver | `volatility_index_data_observed` | 5 | 3 | 2022-11-07 | 2026-05-25 | 83 | 83 | 0 | `timestamp` | BTC=0; ETH=0; SOL=0 |
| Gold | `gold.live.full.m1` | 2 | 2 | 2026-05-01 | 2026-07-14 | 150 | 150 | 0 | `timestamp_m1` | BTC=0; ETH=0 |
| Gold | `gold.market.core.m1` | 33 | 3 | 2018-08-14 | 2026-06-25 | 7053 | 7053 | 0 | `timestamp_m1` | BTC=0; ETH=0; SOL=0 |
| Gold | `gold.market.core_funding.m1` | 33 | 3 | 2018-08-14 | 2026-06-25 | 7112 | 7112 | 0 | `timestamp_m1` | BTC=0; ETH=0; SOL=0 |
| Gold | `gold.market.full.m1` | 30 | 3 | 2018-08-01 | 2026-06-25 | 7138 | 7138 | 0 | `timestamp_m1` | BTC=0; ETH=0; SOL=0 |
| Gold | `gold.market.history_full.m1` | 3 | 3 | 2018-08-01 | 2026-06-25 | 7138 | 7138 | 0 | `timestamp_m1` | BTC=0; ETH=0; SOL=0 |
| Gold | `gold.market.options_trades.m1` | 9 | 3 | 2018-08-14 | 2026-06-25 | 5754 | 5754 | 0 | `timestamp_m1` | BTC=0; ETH=0; SOL=0 |
| Gold | `gold.market.perps_trades.m1` | 8 | 3 | 2018-08-14 | 2026-07-21 | 5832 | 5832 | 0 | `timestamp_m1` | BTC=0; ETH=0; SOL=0 |
| Gold | `index_price_m1_features` | 3 | 3 | 2026-05-24 | 2026-06-07 | 45 | 45 | 0 | `ts_minute` | btc_usd=0; eth_usd=0; sol_usdc=0 |
| Gold | `instrument_metadata_daily_summary` | 1 | 2 | 2026-05-25 | 2026-06-07 | 28 | 28 | 0 | `snapshot_date` | BTC=0; ETH=0 |
| Gold | `l2_m1_features` | 55 | 3 | 2026-05-05 | 2026-06-07 | 102 | 102 | 0 | `ts_minute` | BTC-PERPETUAL=0; ETH-PERPETUAL=0; SOL_USDC-PERPETUAL=0 |
| Gold | `option_surface_m1` | 3 | 3 | 2026-05-24 | 2026-05-24 | 3 | 3 | 0 | `ts_minute` | BTC=0; ETH=0; SOL=0 |
