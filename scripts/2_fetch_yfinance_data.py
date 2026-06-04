"""
Japan IR - yfinance 全項目取得スクリプト（並列処理版）
- 20並列処理で高速化
- shortName バリデーション追加
- エラー時も取得できたデータを保存
- エラー理由の詳細化
- Price Trend (MA乖離率) 計算追加
"""

import argparse
import yfinance as yf
import pandas as pd
import time
from datetime import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

INPUT_CSV = "data/japan_companies_latest.csv"
OUTPUT_DIR = "output"
MAX_WORKERS = 3  # 並列数（yfinance API制限対策）
MAX_RETRIES = 2
RETRY_DELAY = 5
PROGRESS_INTERVAL = 20
BATCH_SIZE = 50  # バッチサイズ
BATCH_DELAY = 45  # バッチ間の待機秒数

# スレッドセーフなカウンター
lock = threading.Lock()
progress_counter = {"success": 0, "error": 0, "total": 0}

INFO_FIELDS = [
    "shortName", "longName", "symbol", "exchange", "currency",
    "country", "city", "address1", "website", "industry", "sector",
    "longBusinessSummary", "fullTimeEmployees",
    "currentPrice", "previousClose", "open", "dayHigh", "dayLow",
    "fiftyTwoWeekHigh", "fiftyTwoWeekLow", "fiftyDayAverage",
    "twoHundredDayAverage", "volume", "averageVolume",
    "averageVolume10days", "beta",
    "marketCap", "sharesOutstanding", "floatShares",
    "impliedSharesOutstanding",
    "trailingPE", "forwardPE", "priceToBook",
    "priceToSalesTrailing12Months", "enterpriseValue",
    "enterpriseToRevenue", "enterpriseToEbitda",
    "profitMargins", "operatingMargins", "grossMargins",
    "returnOnEquity", "returnOnAssets",
    "totalRevenue", "revenuePerShare", "revenueGrowth",
    "grossProfits", "ebitda", "netIncomeToCommon",
    "earningsGrowth", "earningsQuarterlyGrowth",
    "trailingEps", "forwardEps", "bookValue",
    "dividendRate", "dividendYield", "exDividendDate",
    "payoutRatio", "fiveYearAvgDividendYield",
    "lastDividendValue", "lastDividendDate",
    "totalCash", "totalCashPerShare", "totalDebt",
    "debtToEquity", "freeCashflow", "operatingCashflow",
    "targetHighPrice", "targetLowPrice", "targetMeanPrice",
    "targetMedianPrice", "recommendationMean", "recommendationKey",
    "numberOfAnalystOpinions",
    "heldPercentInsiders", "heldPercentInstitutions",
]

# Price Trend (MA乖離率) フィールド
MA_FIELDS = [
    "ma_5_value", "ma_5_deviation", "ma_5_trend",
    "ma_25_value", "ma_25_deviation", "ma_25_trend",
    "ma_75_value", "ma_75_deviation", "ma_75_trend",
    "ma_200_value", "ma_200_deviation", "ma_200_trend",
]


def calculate_ma_deviation(ticker, info):
    """移動平均乖離率を計算"""
    result = {}

    # デフォルト値（計算できない場合）
    for period in [5, 25, 75, 200]:
        result[f"ma_{period}_value"] = None
        result[f"ma_{period}_deviation"] = None
        result[f"ma_{period}_trend"] = "neutral"

    try:
        # 1年分の株価履歴を取得
        hist = ticker.history(period="1y", interval="1d")

        if hist is None or hist.empty:
            return result

        current_price = info.get("currentPrice") or info.get("regularMarketPrice", 0)
        if not current_price:
            return result

        close_prices = hist['Close']

        for period in [5, 25, 75, 200]:
            if len(close_prices) >= period:
                ma = close_prices.tail(period).mean()
                deviation = ((current_price - ma) / ma) * 100
                result[f"ma_{period}_value"] = round(float(ma), 2)
                result[f"ma_{period}_deviation"] = round(float(deviation), 2)
                result[f"ma_{period}_trend"] = "up" if deviation > 0 else "down"

    except Exception as e:
        # エラーが発生してもデフォルト値を返す
        pass

    return result


def fetch_stock_data(code):
    """単一企業のデータを取得"""
    ticker_symbol = f"{code}.T"

    for attempt in range(MAX_RETRIES):
        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info

            if not info or len(info) <= 1:
                raise Exception("Empty response")

            # データを取得
            data = {"code": code, "ticker": ticker_symbol}
            for field in INFO_FIELDS:
                data[field] = info.get(field)

            # Price Trend (MA乖離率) 計算
            ma_data = calculate_ma_deviation(ticker, info)
            data.update(ma_data)

            # バリデーション: 株価・時価総額チェック
            current_price = data.get("currentPrice")
            market_cap = data.get("marketCap")

            has_valid_price = current_price and current_price > 0
            has_valid_market_cap = market_cap and market_cap > 0

            if not has_valid_price and not has_valid_market_cap:
                data["status"] = "error: No valid price or market cap data"
                return data

            data["status"] = "success"
            return data

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                continue

            # エラー時も取得できたデータは保存
            data = {"code": code, "ticker": ticker_symbol}
            if 'info' in locals() and info:
                for field in INFO_FIELDS:
                    data[field] = info.get(field)
            else:
                for field in INFO_FIELDS:
                    data[field] = None

            # MAフィールドもNullで初期化
            for period in [5, 25, 75, 200]:
                data[f"ma_{period}_value"] = None
                data[f"ma_{period}_deviation"] = None
                data[f"ma_{period}_trend"] = "neutral"

            data["status"] = f"error: {str(e)}"
            return data

    # 最終エラー時もMAフィールドを含める
    error_data = {"code": code, "ticker": ticker_symbol, "status": "error: Max retries"}
    for field in INFO_FIELDS:
        error_data[field] = None
    for period in [5, 25, 75, 200]:
        error_data[f"ma_{period}_value"] = None
        error_data[f"ma_{period}_deviation"] = None
        error_data[f"ma_{period}_trend"] = "neutral"
    return error_data

def process_company(code):
    """並列処理用のラッパー関数"""
    result = fetch_stock_data(code)

    # スレッドセーフにカウンターを更新
    with lock:
        progress_counter["total"] += 1
        if result.get("status") == "success":
            progress_counter["success"] += 1
        else:
            progress_counter["error"] += 1

    return result

def main(skip=0, limit=None, suffix=""):
    print("=" * 60)
    print("Japan IR - yfinance 全項目取得（並列処理版）")
    print("=" * 60)
    start_time = datetime.now()
    print(f"開始: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"並列数: {MAX_WORKERS}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df_input = pd.read_csv(INPUT_CSV)
    stock_codes = df_input['code'].tolist()

    # skip/limitを適用
    if skip > 0:
        stock_codes = stock_codes[skip:]
    if limit is not None:
        stock_codes = stock_codes[:limit]

    total = len(stock_codes)

    num_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    batch_wait_time = (num_batches - 1) * BATCH_DELAY
    processing_time = total / MAX_WORKERS * 2
    estimated_time = (processing_time + batch_wait_time) / 60
    print(f"対象: {total}社")
    print(f"取得項目: {len(INFO_FIELDS)}項目")
    print(f"予想時間: 約{estimated_time:.0f}分（バッチ待機含む）")
    print(f"バッチ数: {num_batches}（{BATCH_SIZE}社/バッチ、{BATCH_DELAY}秒間隔）")
    print()

    results = []
    last_progress_print = 0

    # バッチ処理（API制限対策）
    batches = [stock_codes[i:i + BATCH_SIZE] for i in range(0, len(stock_codes), BATCH_SIZE)]
    total_batches = len(batches)

    for batch_idx, batch in enumerate(batches, 1):
        print(f"--- バッチ {batch_idx}/{total_batches} ({len(batch)}社) ---")

        # 並列処理
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_code = {executor.submit(process_company, code): code for code in batch}

            for future in as_completed(future_to_code):
                code = future_to_code[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    results.append({
                        "code": code,
                        "ticker": f"{code}.T",
                        "status": f"error: {str(e)}"
                    })

                # 進捗表示
                current_total = progress_counter["total"]
                if current_total - last_progress_print >= PROGRESS_INTERVAL or current_total == total:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    if current_total > 0:
                        eta = (elapsed / current_total) * (total - current_total) / 60
                    else:
                        eta = 0
                    print(f"[{current_total:4}/{total}] ✅ {progress_counter['success']} / ❌ {progress_counter['error']} | 経過: {elapsed/60:.1f}分 | ETA: {eta:.0f}分")
                    last_progress_print = current_total

        # バッチ間の待機（最後のバッチ以外）
        if batch_idx < total_batches:
            print(f"    💤 {BATCH_DELAY}秒待機...")
            time.sleep(BATCH_DELAY)

    scrape_date = datetime.now().strftime('%Y-%m-%d')
    for r in results:
        r["scrape_date"] = scrape_date

    df_results = pd.DataFrame(results)

    # 全データ
    output_file = f"{OUTPUT_DIR}/yfinance_all_fields_{scrape_date}{suffix}.csv"
    df_results.to_csv(output_file, index=False, encoding="utf-8-sig")

    # 成功データのみ
    df_success = df_results[df_results["status"] == "success"]
    success_file = f"{OUTPUT_DIR}/yfinance_success_{scrape_date}{suffix}.csv"
    df_success.to_csv(success_file, index=False, encoding="utf-8-sig")

    # エラーデータ
    df_errors = df_results[df_results["status"] != "success"]
    if len(df_errors) > 0:
        error_file = f"{OUTPUT_DIR}/yfinance_errors_{scrape_date}{suffix}.csv"
        df_errors.to_csv(error_file, index=False, encoding="utf-8-sig")

        print()
        print("=" * 60)
        print("エラー内訳:")
        print("=" * 60)
        error_types = df_errors['status'].value_counts()
        for error_type, count in error_types.items():
            print(f"  {error_type}: {count}社")

    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()

    success_count = progress_counter["success"]
    error_count = progress_counter["error"]

    print()
    print("=" * 60)
    print(f"完了: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"所要時間: {elapsed/60:.1f}分（{elapsed/3600:.2f}時間）")
    print(f"成功: {success_count}社 ({success_count/total*100:.1f}%)")
    print(f"失敗: {error_count}社")
    print(f"取得項目: {len(INFO_FIELDS)}項目")
    print(f"並列数: {MAX_WORKERS}")
    print(f"出力: {output_file}")
    print("=" * 60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip", type=int, default=0, help="スキップする企業数")
    parser.add_argument("--limit", type=int, default=None, help="処理する企業数")
    parser.add_argument("--suffix", type=str, default="", help="出力ファイルのサフィックス（例: _part1）")
    args = parser.parse_args()
    main(skip=args.skip, limit=args.limit, suffix=args.suffix)
