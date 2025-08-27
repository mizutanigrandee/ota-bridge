# ota-bridge

各OTA指標を収集→JSON化し、`vacancy-dashboard` の外部データ源として供給するリポジトリ。  
公開確認用 UI: `/daily_preview.html`（7日ウィンドウ）

---

## 1) 運用手順（通常）

- 日次：Rakuten 最安値取得（28日ウィンドウ）
  - Actions → **Fetch Rakuten Min Prices** → 自動（日次）／手動起動可
- 週次：Jalan 口コミ平均／件数
  - Actions → **Fetch Jalan Reviews** → 週1（推奨）／手動起動可
- 成果物（JSON）
  - `/data/competitor_min_prices.json`
  - `/data/ota_facility_meta.json`
  - `/data/hotel_master.json`（手動マスタ）
  - `/data/health_report.json`（ヘルスチェック）

## 2) JSONの見方（不変条件）

- 価格：整数（円）・税込、在庫なし／失敗は `null`
- `competitor_min_prices.json`
  - `meta: { currency:"JPY", source:"rakuten_travel", window_days:28, last_updated:<UTC ISO8601> }`
  - `days: { "YYYY-MM-DD": { "<hotel_id>": <int|null>, ... } }`（date は JST 相当の日付）
- `ota_facility_meta.json`
  - `hotels.<hotel_id>.rakuten.review_avg / review_count`
  - `hotels.<hotel_id>.jalan.review_avg / review_count`
- NaN/Infinity は出さない（厳守）

## 3) 手動リトライ（よく使うオペ）

- Actions → **Fetch Rakuten Min Prices** / **Fetch Jalan Reviews** → Run workflow
- ページ表示のキャッシュ回避：URL末尾に `?v=<timestamp>` を付与

## 4) ホテルを追加する

1. `data/hotel_master.json` に1行追加（`enabled:true`）
   - `rakuten_hotel_no`（数値）, `jalan_hotel_id`（**yad付き**：例 `yad340057`）
2. コミット後、日次/週次の取得で自然に反映

## 5) 失敗時の対処

- Rakuten: 429 等 → 自動リトライ／失敗日は `null` を書いて **前回値を壊さない**
- Jalan: DOM 変更で抽出失敗 → 3段フォールバック（JSON-LD → Microdata → 本文）  
  それでも失敗時は **前回値を保持**（`null` で上書きしない）
- まず `/data/health_report.json` を確認（null 比率／鮮度）

## 6) 依存とセットアップ

```bash
pip install -r requirements.txt
# Jalan 初回のみ
python -m playwright install --with-deps chromium
