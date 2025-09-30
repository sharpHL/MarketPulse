# Market Snapshot Pipeline

This repository contains a small utility that mirrors the A-share heatmap data from [52etf.site](https://52etf.site/) and stores it locally for analysis.

## Local usage

```bash
python -m pip install -r requirements.txt
PYTHONPYCACHEPREFIX=/tmp python src/fetch_market_data.py --page-size 500 --throttle 0.3
```

* Output parquet files are written to `data/<YYYYMMDD>/<HHMM>.parquet` using Asia/Shanghai timestamps (for example, `data/20250930/0930.parquet`). The `HHMM` label snaps to the nearest scheduled slot (09:30…15:00) within a ±5 分钟窗口。
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

*可选参数*：`--page-size` 控制每页下载条数，`--throttle` 控制分页请求的间隔时间，`--max-retries` 控制单页失败后的最大重试次数（默认 3 次），`--min-page-size` 作为重试降级时的最小分页规模，`--max-total-retries` 设置全局失败上限，`--graceful` 允许遇到接口异常时跳过写入而不中断流程。
