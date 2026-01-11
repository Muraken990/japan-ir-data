# Japan IR Data Scripts

データ取得・更新スクリプトの説明

---

## 実行頻度と取得データ

### fetch_wordpress_companies.py（月-金 + 日曜）

WordPress REST APIから企業リストを取得。後続スクリプトの対象企業を決定。

- **入力**: `https://japanir.jp/wp-json/wp/v2/company`
- **出力**: `data/wordpress_companies.csv`

| WordPress フィールド | 出力ID | 説明 |
|---------------------|--------|------|
| `stock_code` | `code` | 証券コード (例: 7203) |
| `title.rendered` | `name` | 企業名 |
| - | `ticker` | yfinance形式 (例: 7203.T) |

| 曜日 | 後続スクリプト | 取得データ |
|-----|---------------|-----------|
| 月-金 | `fetch_stock_history.py` | open, high, low, close, volume（5年分） |
| 日曜 | `fetch_financials.py` | revenue, net_income, ROE, ROA, 配当履歴 等（5年分） |

---

### fetch_stock_history.py（月-金）

yfinanceから株価履歴（5年分）を取得。

- **入力**: `data/wordpress_companies.csv`
- **出力**: `data/stock_history/{code}.json`

| yfinance フィールド | 出力ID | 説明 |
|--------------------|--------|------|
| `Open` | `open` | 始値 |
| `High` | `high` | 高値 |
| `Low` | `low` | 安値 |
| `Close` | `close` | 終値 |
| `Volume` | `volume` | 出来高 |

---

### fetch_financials.py（日曜）

yfinanceから財務データ（5年分）を取得。

- **入力**: `data/wordpress_companies.csv`
- **出力**: `data/financials/{code}.json`

#### company_info（企業情報）

| yfinance フィールド | 出力ID | 説明 |
|--------------------|--------|------|
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

#### price_trend（MA乖離率）

| 出力ID | 説明 |
|--------|------|
| `ma_5.ma_value` | 5日移動平均値 |
| `ma_5.deviation` | 5日MA乖離率 (%) |
| `ma_5.trend` | トレンド (up/down/neutral) |
| `ma_25.*` | 25日移動平均（同上） |
| `ma_75.*` | 75日移動平均（同上） |
| `ma_200.*` | 200日移動平均（同上） |

#### financials（財務データ - 年度別）

| yfinance フィールド | 出力ID | 説明 |
|--------------------|--------|------|
| `Total Revenue` | `revenue` | 売上高 |
| `Gross Profit` | `gross_profit` | 売上総利益 |
| `Operating Income` | `operating_income` | 営業利益 |
| `EBIT` | `ebit` | EBIT |
| `Net Income` | `net_income` | 純利益 |
| `Diluted EPS` | `eps` | EPS |
| (計算値) | `operating_margin` | 営業利益率 (%) |
| `Total Assets` | `total_assets` | 総資産 |
| `Stockholders Equity` | `total_equity` | 純資産 |
| `Total Debt` | `total_debt` | 有利子負債 |
| `Cash And Cash Equivalents` | `total_cash` | 現金 |
| (計算値) | `equity_ratio` | 自己資本比率 (%) |
| (計算値) | `de_ratio` | D/E比率 |
| (計算値) | `current_ratio` | 流動比率 |
| `Operating Cash Flow` | `operating_cf` | 営業CF |
| `Investing Cash Flow` | `investing_cf` | 投資CF |
| `Financing Cash Flow` | `financing_cf` | 財務CF |
| `Free Cash Flow` | `free_cf` | フリーCF |
| (計算値) | `net_margin` | 純利益率 (%) |
| (計算値) | `roe` | ROE (%) |
| (計算値) | `roa` | ROA (%) |

#### dividends（配当情報）

| 出力ID | 説明 |
|--------|------|
| `history[].year` | 配当年度 |
| `history[].amount` | 年間配当額 |
| `latest` | 最新配当額 |

---

### 2_fetch_yfinance_data.py（月-金）

yfinanceから株価・財務・アナリスト情報等を取得。

- **入力**: `data/japan_companies_latest.csv`
- **出力**: `output/yfinance_all_fields_*.csv`

#### 株価情報

| yfinance フィールド | 出力ID | 説明 |
|--------------------|--------|------|
| `currentPrice` | `currentPrice` | 現在株価 |
| `previousClose` | `previousClose` | 前日終値 |
| `open` | `open` | 始値 |
| `dayHigh` | `dayHigh` | 高値 |
| `dayLow` | `dayLow` | 安値 |
| `volume` | `volume` | 出来高 |
| `averageVolume` | `averageVolume` | 平均出来高 |
| `fiftyTwoWeekHigh` | `fiftyTwoWeekHigh` | 52週高値 |
| `fiftyTwoWeekLow` | `fiftyTwoWeekLow` | 52週安値 |
| `marketCap` | `marketCap` | 時価総額 |

#### 企業情報

| yfinance フィールド | 出力ID | 説明 |
|--------------------|--------|------|
| `shortName` | `shortName` | 英語社名（短縮） |
| `longName` | `longName` | 英語社名（正式） |
| `sector` | `sector` | セクター |
| `industry` | `industry` | 業種 |
| `website` | `website` | 企業サイトURL |
| `fullTimeEmployees` | `fullTimeEmployees` | 従業員数 |
| `country` | `country` | 国 |
| `city` | `city` | 都市 |

#### バリュエーション

| yfinance フィールド | 出力ID | 説明 |
|--------------------|--------|------|
| `trailingPE` | `trailingPE` | PER（実績） |
| `forwardPE` | `forwardPE` | PER（予想） |
| `priceToBook` | `priceToBook` | PBR |
| `enterpriseValue` | `enterpriseValue` | 企業価値 |

#### 収益性

| yfinance フィールド | 出力ID | 説明 |
|--------------------|--------|------|
| `profitMargins` | `profitMargins` | 利益率 |
| `operatingMargins` | `operatingMargins` | 営業利益率 |
| `returnOnEquity` | `returnOnEquity` | ROE |
| `returnOnAssets` | `returnOnAssets` | ROA |

#### 配当

| yfinance フィールド | 出力ID | 説明 |
|--------------------|--------|------|
| `dividendRate` | `dividendRate` | 配当額 |
| `dividendYield` | `dividendYield` | 配当利回り |
| `payoutRatio` | `payoutRatio` | 配当性向 |

#### アナリスト

| yfinance フィールド | 出力ID | 説明 |
|--------------------|--------|------|
| `targetMeanPrice` | `targetMeanPrice` | 目標株価（平均） |
| `recommendationMean` | `recommendationMean` | 推奨スコア |
| `recommendationKey` | `recommendationKey` | 推奨（buy/hold/sell） |
| `numberOfAnalystOpinions` | `numberOfAnalystOpinions` | アナリスト数 |

#### MA乖離率

| 出力ID | 説明 |
|--------|------|
| `ma_5_value` | 5日移動平均値 |
| `ma_5_deviation` | 5日MA乖離率 (%) |
| `ma_5_trend` | トレンド (up/down/neutral) |
| `ma_25_*` | 25日移動平均（同上） |
| `ma_75_*` | 75日移動平均（同上） |
| `ma_200_*` | 200日移動平均（同上） |

---

## GitHub Actions ワークフロー

### daily-update-complete.yml（月-金 23:00 UTC）

```
fetch_wordpress_companies.py → 2_fetch_yfinance_data.py → fetch_stock_history.py → 4_wordpress_smart_update.py
```

### weekly-financials.yml（日曜 00:00 UTC）

```
fetch_wordpress_companies.py → fetch_financials.py → update_financials.py
```

### monthly-analyst-earnings.yml（毎月1日 00:00 UTC）

```
fetch_wordpress_companies.py → fetch_analyst_earnings.py → update_analyst_earnings.py
```

---

## ローカル実行

```bash
cd japan-ir-data

# 企業リスト取得
python scripts/fetch_wordpress_companies.py

# yfinance全項目取得
python scripts/2_fetch_yfinance_data.py

# 株価履歴取得（全企業）
python scripts/fetch_stock_history.py

# 株価履歴取得（特定銘柄）
python scripts/fetch_stock_history.py --ticker 7203

# 財務データ取得（全企業）
python scripts/fetch_financials.py

# 財務データ取得（特定銘柄）
python scripts/fetch_financials.py --ticker 7203

# アナリスト・決算データ取得（全企業）
python scripts/fetch_analyst_earnings.py

# アナリスト・決算データ取得（特定銘柄）
python scripts/fetch_analyst_earnings.py --ticker 7203
```

---

## fetch_analyst_earnings.py（月次）

yfinanceからアナリスト予想・決算日程を取得。

- **入力**: `data/wordpress_companies.csv`
- **出力**: `data/analyst_earnings/{code}.json`

#### analyst_recommendations（アナリスト推奨）

| yfinance フィールド | 出力ID | 説明 |
|--------------------|--------|------|
| `recommendations.strongBuy` | `strong_buy` | Strong Buy 件数 |
| `recommendations.buy` | `buy` | Buy 件数 |
| `recommendations.hold` | `hold` | Hold 件数 |
| `recommendations.sell` | `sell` | Sell 件数 |
| `recommendations.strongSell` | `strong_sell` | Strong Sell 件数 |
| `info.recommendationKey` | `recommendation_key` | 推奨（buy/hold/sell） |
| `info.recommendationMean` | `recommendation_mean` | 推奨スコア（1-5） |
| (計算値) | `total_analysts` | アナリスト総数 |

#### target_prices（目標株価）

| yfinance フィールド | 出力ID | 説明 |
|--------------------|--------|------|
| `analyst_price_targets.current` | `current` | 現在株価 |
| `analyst_price_targets.high` | `high` | 目標株価（高値） |
| `analyst_price_targets.low` | `low` | 目標株価（安値） |
| `analyst_price_targets.mean` | `mean` | 目標株価（平均） |
| `analyst_price_targets.median` | `median` | 目標株価（中央値） |

#### earnings_dates（決算日程）

| yfinance フィールド | 出力ID | 説明 |
|--------------------|--------|------|
| `earnings_dates.index` | `date` | 決算日 |
| `earnings_dates['EPS Estimate']` | `eps_estimate` | EPS予想 |
| `earnings_dates['Reported EPS']` | `eps_actual` | EPS実績 |
| `earnings_dates['Surprise(%)']` | `surprise_pct` | サプライズ (%) |
| (計算値) | `next_earnings` | 次回決算情報 |
| (計算値) | `past_earnings` | 過去決算履歴（直近5件）|
