# PR-54 Medallion Benchmark Baseline

Schema version: 1

Command:

```bash
python main.py --config config.yaml benchmark-build --fixture-only --output-report /tmp/pr54-benchmark.json --no-json-output
```

The command creates an isolated fixture outside `lake/` with one `BTC` symbol,
one `2026-01` partition, and two rows per Bronze, Silver, and Gold stage. It
only reads the fixture artifacts; `/tmp/pr54-benchmark.json` is the sole output.

Baseline run on 2026-08-02:

| Stage | Rows in/out | Bytes read | Workers | Polars threads |
| --- | ---: | ---: | ---: | ---: |
| Bronze | 2 / 2 | 892 | 1 | 4 |
| Silver | 2 / 2 | 892 | 1 | 4 |
| Gold | 2 / 2 | 892 | 1 | 4 |

Elapsed timings are intentionally reported by the generated JSON and are not
asserted as a fixed baseline because they depend on the local filesystem and
runtime scheduling.