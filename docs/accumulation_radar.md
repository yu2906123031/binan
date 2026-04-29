# Accumulation Radar Integration

`scripts/accumulation_radar.py` is a distilled version of the public
`connectfarm1/accumulation-radar` idea, integrated as an external signal
generator for this project. It does not install or import the upstream repo.

## What It Does

- Scans Binance USDT perpetual contracts with public endpoints.
- Detects low-volume sideways accumulation windows from daily klines.
- Adds open-interest and negative-funding bonuses.
- Writes the existing external-signal protocol:
  - `binance_square_symbols.txt`
  - `binance_external_signal.json`

The main strategy can then consume the generated files with
`--square-symbols-file` and `--external-signal-json`.

## Generate Signals

```powershell
python scripts\accumulation_radar.py --mode external --top 30
```

By default this writes under `HERMES_HOME` or `~/.hermes`:

```text
~/.hermes/binance_square_symbols.txt
~/.hermes/binance_external_signal.json
```

For a small focused scan:

```powershell
python scripts\accumulation_radar.py --mode external --symbols BTCUSDT,SOLUSDT,DOGEUSDT --top 10 --print-json
```

Inspect only the accumulation pool without writing external files:

```powershell
python scripts\accumulation_radar.py --mode pool --symbols BTCUSDT,SOLUSDT --top 10
```

## Feed The Main Strategy

```powershell
python main.py --scan-only `
  --square-symbols-file "$env:USERPROFILE\.hermes\binance_square_symbols.txt" `
  --external-signal-json "$env:USERPROFILE\.hermes\binance_external_signal.json" `
  --output-format json
```

## Tunable Parameters

- `--min-sideways-days` default `45`
- `--max-range-pct` default `80`
- `--max-avg-vol-usd` default `20000000`
- `--min-oi-usd` default `2000000`
- `--top` default `30`

