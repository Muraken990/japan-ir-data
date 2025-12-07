#!/usr/bin/env python3
"""
Japan IR - yfinance 全銘柄取得スクリプト
GitHub Actions用
"""

import yfinance as yf
import pandas as pd
import time
from datetime import datetime
import os

# ============================================================
# 設定
# ============================================================

# 入力ファイル
INPUT_CSV = "japan_ir_all_20251207.csv"

# 出力ディレクトリ
OUTPUT_DIR = "output"

# リクエスト間隔（秒）
REQUEST_DELAY = 2.0

# リトライ設定
MAX_RETRIES = 2
RETRY_DELAY = 10

# 進捗表示間隔
PROGRESS_INTERVAL = 100


def fetch_stock_data(code):
    """1銘柄のデータを取得"""
    ticker_symbol = f"{code}.T"
    
    for attempt in range(MAX_RETRIES):
        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info
            
            if not info or len(info) <= 1:
                raise Exception("Empty response")
            
            data = {
                "code": code,
                "ticker": ticker_symbol,
                "company_name_en": info.get("shortName", ""),
                "market_cap": info.get("marketCap"),
                "market_cap_million": None,
                "current_price": info.get("currentPrice"),
                "previous_close": info.get("previousClose"),
                "pe_ratio": info.get("trailingPE"),
                "pb_ratio": info.get("priceToBook"),
                "dividend_yield": info.get("dividendYield"),
                "sector": info.get("sector", ""),
                "industry": info.get("industry", ""),
                "status": "success"
            }
            
            if data["market_cap"]:
                data["market_cap_million"] = int(data["market_cap"] / 1_000_000)
            
            return data
            
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                continue
            
            return {
                "code": code,
                "ticker": ticker_symbol,
                "status": f"error: {str(e)}"
            }
    
    return {"code": code, "ticker": ticker_symbol, "status": "error: Max retries"}


def main():
    print("=" * 60)
    print("Japan IR - yfinance データ取得")
    print("=" * 60)
    start_time = datetime.now()
    print(f"開始: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 出力ディレクトリ作成
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # CSV読み込み
    df_input = pd.read_csv(INPUT_CSV)
    stock_codes = df_input['code'].tolist()
    total = len(stock_codes)
    
    print(f"対象: {total}社")
    print()
    
    # データ取得
    results = []
    success_count = 0
    error_count = 0
    
    for i, code in enumerate(stock_codes, 1):
        data = fetch_stock_data(code)
        results.append(data)
        
        if data.get("status") == "success":
            success_count += 1
        else:
            error_count += 1
        
        if i % PROGRESS_INTERVAL == 0 or i == total:
            print(f"[{i:4}/{total}] ✅ {success_count} / ❌ {error_count}")
        
        if i < total:
            time.sleep(REQUEST_DELAY)
    
    # 取得日追加
    scrape_date = datetime.now().strftime('%Y-%m-%d')
    for r in results:
        r["scrape_date"] = scrape_date
    
    # CSV出力
    df_results = pd.DataFrame(results)
    
    # 全データ
    output_file = f"{OUTPUT_DIR}/yfinance_all_{scrape_date}.csv"
    df_results.to_csv(output_file, index=False, encoding="utf-8-sig")
    
    # 成功データのみ（WordPress用）
    df_success = df_results[df_results["status"] == "success"]
    wp_file = f"{OUTPUT_DIR}/yfinance_wordpress_{scrape_date}.csv"
    df_success.to_csv(wp_file, index=False, encoding="utf-8-sig")
    
    # エラーデータ
    df_errors = df_results[df_results["status"] != "success"]
    if len(df_errors) > 0:
        error_file = f"{OUTPUT_DIR}/yfinance_errors_{scrape_date}.csv"
        df_errors.to_csv(error_file, index=False, encoding="utf-8-sig")
    
    # 結果表示
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    
    print()
    print("=" * 60)
    print(f"完了: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"所要時間: {elapsed/60:.1f}分")
    print(f"成功: {success_count}社 ({success_count/total*100:.1f}%)")
    print(f"失敗: {error_count}社")
    print(f"出力: {output_file}")
    print("=" * 60)


if __name__ == "__main__":
    main()
