# Market Snapshot Pipeline

This repository contains a small utility that mirrors the A-share heatmap data from [52etf.site](https://52etf.site/) and stores it locally for analysis.

## Local usage

```bash
python -m pip install -r requirements.txt
PYTHONPYCACHEPREFIX=/tmp python src/fetch_market_data.py
```

* Output parquet files are written to `data/<YYYYMMDD>/<HHMM>.parquet` using Asia/Shanghai timestamps (for example, `data/20250930/0930.parquet`).
* Each dataframe contains the following columns:
  * `stock_id` – security code in the form `000001.SZ`
  * `stock_name_cn` – security name
  * `change_pct` – latest percentage change
  * `price` – latest traded price (CNY)
  * `market_cap` – latest total market value (CNY)
  * `board` / `sub_board` – hierarchy derived from `board_tree.json`
* The run is skipped automatically on non-trading days; use `--force` to override for ad-hoc backfills.

## Automation

`.github/workflows/fetch-market-data.yml` schedules the pipeline at the Shanghai trading snapshots (09:30, 10:00, 10:30, 11:00, 11:30, 13:30, 14:00, 14:40 and 15:00 local time). Successful runs commit new parquet snapshots back to the repository using the built-in `GITHUB_TOKEN`.

Manual executions are also available through the **Run workflow** button in GitHub Actions.
