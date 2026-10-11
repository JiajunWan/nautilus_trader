| Command | Mean Wall Time [s] | Change | Factor |
|:---|---:|---:|:---|
| `test-run-only --test-threads 4` | 50.626 ± 0.714 |  |  |
| `test-run-only --test-threads 8` | 29.019 ± 0.581 | -42.7% | (1.7x faster) |
| `test-run-only --test-threads 16` | 23.084 ± 0.365 | -54.4% | (2.2x faster) |
| `test-run-only --test-threads 32` | 20.955 ± 0.273 | -58.6% | (2.4x faster) |
