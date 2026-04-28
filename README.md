# polymarketBot2

Standalone fine-time-return-z Polymarket bot. It does not import from the legacy bot or from the research scripts at runtime.

## Strategy

- Loads `fine_time_return_z` lookup tables from `polymarketBot2/model_artifacts/`.
- Uses RTDS Chainlink websocket to capture the first tick in each 5-minute bucket as `price_to_beat`.
- Uses **completed previous Binance 1m kline close** as `underlying_price_now`, matching the backtest `merge_asof(..., direction="backward")`口径.
- Computes `vol_60m` from completed 1m close log returns, requiring at least 20 returns.
- Paper mode is enabled by default and writes `runtime/paper_trades.jsonl`.

## Run

```bash
python -m polymarketBot2.fine_time_return_z_model --self-test
python -m polymarketBot2.main
```

Edit `polymarketBot2/config.py` for symbol, edge threshold, order size, and live trading credentials.
