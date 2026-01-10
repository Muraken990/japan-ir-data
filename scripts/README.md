# Japan IR Data Scripts

データ取得・更新スクリプトの説明

---

## 実行頻度

| スクリプト | 頻度 | GitHub Actions | 説明 |
|-----------|------|----------------|------|
| `fetch_wordpress_companies.py` | 月-金 + 日曜 | daily-update-complete, weekly-financials | WordPress登録企業リスト取得 |
| `fetch_stock_history.py` | 月-金 | daily-update-complete | 株価履歴データ取得 |
| `fetch_financials.py` | 日曜 | weekly-financials | 財務データ取得 |

---

## 1. fetch_wordpress_companies.py

### 概要
WordPress REST APIから登録済み企業のリストを取得し、CSVファイルに保存。

### 入力
- WordPress REST API: `https://japanir.jp/wp-json/wp/v2/company`

### 出力
- `data/wordpress_companies.csv`

### 取得フィールド

| WordPress フィールド | 出力カラム | 説明 |
|---------------------|-----------|------|
| `stock_code` | `code` | 証券コード (例: 7203) |
| `title.rendered` | `name` | 企業名 |
| - | `ticker` | yfinance形式 (例: 7203.T) |

---

## 2. fetch_stock_history.py

### 概要
yfinanceから過去5年分の日次株価データを取得し、JSON形式で保存。

### 入力
- `data/wordpress_companies.csv` (優先)
- `data/japan_companies_latest.csv` (フォールバック)

### 出力
- `data/stock_history/{code}.json`

### 取得フィールド

| yfinance フィールド | 出力キー | 説明 |
|-------------------|---------|------|
| `Open` | `open` | 始値 |
| `High` | `high` | 高値 |
| `Low` | `low` | 安値 |
| `Close` | `close` | 終値 |
| `Volume` | `volume` | 出来高 |

### JSON構造
```json
{
  "code": "7203",
  "ticker": "7203.T",
  "last_updated": "2026-01-09 12:00:00",
  "period": "5y",
  "data_points": 1234,
  "data": [
    {"date": "2021-01-04", "open": 1234.0, "high": 1250.0, "low": 1220.0, "close": 1245.0, "volume": 1000000},
    ...
  ]
}
```

---

## 3. fetch_financials.py

### 概要
yfinanceから5年分の財務諸表データを取得し、JSON形式で保存。

### 入力
- `data/wordpress_companies.csv` (優先)
- `data/japan_companies_latest.csv` (フォールバック)

### 出力
- `data/financials/{code}.json`

### 取得フィールド

#### company_info (企業情報)

| yfinance フィールド | 出力キー | 説明 |
|-------------------|---------|------|
| `shortName` | `name_en` | 英語社名（短縮） |
| `longName` | `long_name` | 英語社名（正式） |
| `sector` | `sector` | セクター |
| `industry` | `industry` | 業種 |
| `website` | `website` | 企業サイトURL |
| `fullTimeEmployees` | `employees` | 従業員数 |
| `country` | `country` | 国 |
| `city` | `city` | 都市 |
| `address1` | `address` | 住所 |
| `longBusinessSummary` | `description` | 事業概要 |

#### price_trend (株価トレンド - MA乖離率)

| 出力キー | 説明 |
|---------|------|
| `ma_5.ma_value` | 5日移動平均値 |
| `ma_5.deviation` | 5日MA乖離率 (%) |
| `ma_5.trend` | トレンド (up/down/neutral) |
| `ma_25.*` | 25日移動平均 |
| `ma_75.*` | 75日移動平均 |
| `ma_200.*` | 200日移動平均 |

#### financials (財務データ - 5年分)

| カテゴリ | 出力キー | 説明 |
|---------|---------|------|
| **損益計算書** | | |
| | `revenue` | 売上高 |
| | `gross_profit` | 売上総利益 |
| | `operating_income` | 営業利益 |
| | `ebit` | EBIT |
| | `net_income` | 純利益 |
| | `eps` | EPS |
| | `operating_margin` | 営業利益率 (%) |
| **貸借対照表** | | |
| | `total_assets` | 総資産 |
| | `total_equity` | 純資産 |
| | `total_debt` | 有利子負債 |
| | `total_cash` | 現金 |
| | `equity_ratio` | 自己資本比率 (%) |
| | `de_ratio` | D/E比率 |
| | `current_ratio` | 流動比率 |
| **キャッシュフロー** | | |
| | `operating_cf` | 営業CF |
| | `investing_cf` | 投資CF |
| | `financing_cf` | 財務CF |
| | `free_cf` | フリーCF |
| **効率性** | | |
| | `net_margin` | 純利益率 (%) |
| | `roe` | ROE (%) |
| | `roa` | ROA (%) |

#### dividends (配当情報)

| 出力キー | 説明 |
|---------|------|
| `history` | 年度別配当履歴 |
| `latest` | 最新配当額 |

### JSON構造
```json
{
  "success": true,
  "fetched_at": "2026-01-09T12:00:00",
  "ticker": "7203",
  "ticker_full": "7203.T",
  "company_name": "TOYOTA MOTOR CORP",
  "company_info": { ... },
  "price_trend": {
    "ma_5": {"ma_value": 3369.0, "deviation": 0.56, "trend": "up"},
    "ma_25": { ... },
    "ma_75": { ... },
    "ma_200": { ... }
  },
  "financials": {
    "years": [
      {"year": 2025, "revenue": 48036704000000, ...},
      {"year": 2024, ...},
      ...
    ],
    "has_data": true
  },
  "dividends": {
    "history": [{"year": 2025, "amount": 95.0}, ...],
    "latest": 45.0,
    "has_data": true
  }
}
```

---

## GitHub Actions ワークフロー

### daily-update-complete.yml
- **スケジュール**: 月-金 23:00 UTC (日本時間 08:00)
- **実行内容**:
  1. `fetch_wordpress_companies.py` - 企業リスト取得
  2. `fetch_stock_history.py` - 株価履歴取得
  3. `4_wordpress_smart_update.py` - WordPress更新

### weekly-financials.yml
- **スケジュール**: 日曜 00:00 UTC (日本時間 09:00)
- **実行内容**:
  1. `fetch_wordpress_companies.py` - 企業リスト取得
  2. `fetch_financials.py` - 財務データ取得
  3. `update_financials.py` - WordPress更新

---

## ローカル実行

```bash
cd japan-ir-data

# 企業リスト取得
python scripts/fetch_wordpress_companies.py

# 株価履歴取得（全企業）
python scripts/fetch_stock_history.py

# 株価履歴取得（特定銘柄）
python scripts/fetch_stock_history.py --ticker 7203

# 財務データ取得（全企業）
python scripts/fetch_financials.py

# 財務データ取得（特定銘柄）
python scripts/fetch_financials.py --ticker 7203

# ドライラン
python scripts/fetch_financials.py --dry-run --limit 5
```
