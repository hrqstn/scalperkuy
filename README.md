# Scalperkuy

AI-assisted crypto scalping research bot untuk Ubuntu homeserver. Fase `v0.1` hanya mengumpulkan data publik dan menyiapkan paper-trading research loop. Tidak ada live order, tidak ada leverage, dan tidak ada exchange API key dengan permission trading.

## Hard rules

- No live trading in `v0.1`.
- No exchange API key with trading permission.
- No leverage.
- No top-up for at least 3 months.
- Collector harus tetap berjalan meskipun paper trader masuk emergency stop.
- Emergency stop hanya memblokir entry baru, tidak menghentikan market data collection.
- LLM/Gemini hanya untuk ringkasan, bukan sinyal buy/sell, risk, PnL, atau database truth.

## Milestone 1

Yang sudah disiapkan:

- Docker Compose dengan `postgres`, `collector`, `paper_trader`, `reporter`, dan `dashboard`.
- Schema PostgreSQL untuk market data, paper trading, event, dan service health.
- Adapter Tokocrypto public REST di `app/exchange/tokocrypto.py`.
- Collector untuk BTC/USDT dan ETH/USDT:
  - candle 1m,
  - best bid/ask quote,
  - recent trades,
  - order book top 20.
- Discord alert opsional untuk startup, error, dan stale data.
- Streamlit dashboard untuk system health, row counts, latest quotes, latest candles, dan candle chart.
- Paper-trading risk/strategy skeleton tanpa live execution.
- `paper_trader` dan `reporter` masih standby di milestone 1, hanya menulis service health.

## Quick start

```bash
cp .env.example .env
docker compose up --build
```

Dashboard:

```text
http://localhost:8501
```

Kalau tidak mau alert Discord, biarkan `DISCORD_WEBHOOK_URL` kosong. Collector tetap jalan tanpa webhook.

## Configuration

Edit `config.example.yaml` untuk konfigurasi awal. Default penting:

```yaml
exchange: Tokocrypto
mode: paper
symbols:
  - BTC/USDT
  - ETH/USDT
timezone: Asia/Jakarta
data:
  candle_timeframe: 1m
  quote_interval_seconds: 5
  order_book_depth: 20
  order_book_interval_seconds: 10
```

Untuk override database/port/Discord, edit `.env`:

```bash
POSTGRES_PASSWORD=change_me
DASHBOARD_PORT=8501
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
```

## Ubuntu homeserver notes

Install Docker Engine dan Compose plugin, lalu jalankan dari folder repo:

```bash
docker compose up -d --build
docker compose logs -f collector
```

Rekomendasi awal untuk Ryzen 5 3500U / 8GB RAM / 256GB SSD:

- Tetap 2 simbol dulu.
- Quote setiap 5 detik.
- Order book top 20 setiap 10 detik.
- Log rotation sudah dibatasi di Compose (`10m`, 5 file).
- Pantau disk usage dari page `System`.

## Database tables

Schema awal ada di `app/db/migrations/001_initial.sql`.

Tabel utama:

- `market_candles`
- `market_quotes`
- `market_trades`
- `order_book_snapshots`
- `paper_signals`
- `paper_trades`
- `daily_performance`
- `market_events`
- `service_health`

## Safety boundary

Kode saat ini tidak memiliki modul order execution dan tidak membaca exchange private API key. Paper-trading code harus tetap deterministik. Kalau nanti ada live tiny mode, itu harus menjadi fase terpisah setelah data 30-90 hari dan metriknya stabil.
