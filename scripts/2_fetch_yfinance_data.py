"""
Japan IR - yfinance 全項目取得スクリプト（68項目版）
"""

import yfinance as yf
import pandas as pd
import time
from datetime import datetime
import os

INPUT_CSV = "japan_companies_latest.csv"
OUTPUT_DIR = "output"
REQUEST_DELAY = 2.0
MAX_RETRIES = 2
RETRY_DELAY = 10
PROGRESS_INTERVAL = 100

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

def fetch_stock_data(code):
    ticker_symbol = f"{code}.T"
    
    for attempt in range(MAX_RETRIES):
        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info
            
            if not info or len(info) <= 1:
                raise Exception("Empty response")
            
            data = {"code": code, "ticker": ticker_symbol}
            for field in INFO_FIELDS:
                data[field] = info.get(field)
            data["status"] = "success"
            return data
            
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                continue
            
            data = {"code": code, "ticker": ticker_symbol}
            for field in INFO_FIELDS:
                data[field] = None
            data["status"] = f"error: {str(e)}"
            return data
    
    return {"code": code, "ticker": ticker_symbol, "status": "error: Max retries"}

def main():
    print("=" * 60)
    print("Japan IR - yfinance 全項目取得（68項目）")
    print("=" * 60)
    start_time = datetime.now()
    print(f"開始: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    df_input = pd.read_csv(INPUT_CSV)
    stock_codes = df_input['code'].tolist()
    total = len(stock_codes)
    
    print(f"対象: {total}社")
    print(f"取得項目: {len(INFO_FIELDS)}項目")
    print()
    
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
            elapsed = (datetime.now() - start_time).total_seconds()
            eta = (elapsed / i) * (total - i) / 60
            print(f"[{i:4}/{total}] ✅ {success_count} / ❌ {error_count} | ETA: {eta:.0f}分")
        
        if i < total:
            time.sleep(REQUEST_DELAY)
    
    scrape_date = datetime.now().strftime('%Y-%m-%d')
    for r in results:
        r["scrape_date"] = scrape_date
    
    df_results = pd.DataFrame(results)
    
    # 全データ
    output_file = f"{OUTPUT_DIR}/yfinance_all_fields_{scrape_date}.csv"
    df_results.to_csv(output_file, index=False, encoding="utf-8-sig")
    
    # 成功データのみ
    df_success = df_results[df_results["status"] == "success"]
    success_file = f"{OUTPUT_DIR}/yfinance_success_{scrape_date}.csv"
    df_success.to_csv(success_file, index=False, encoding="utf-8-sig")
    
    # エラーデータ
    df_errors = df_results[df_results["status"] != "success"]
    if len(df_errors) > 0:
        error_file = f"{OUTPUT_DIR}/yfinance_errors_{scrape_date}.csv"
        df_errors.to_csv(error_file, index=False, encoding="utf-8-sig")
    
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    
    print()
    print("=" * 60)
    print(f"完了: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"所要時間: {elapsed/60:.1f}分（{elapsed/3600:.1f}時間）")
    print(f"成功: {success_count}社 ({success_count/total*100:.1f}%)")
    print(f"失敗: {error_count}社")
    print(f"取得項目: {len(INFO_FIELDS)}項目")
    print(f"出力: {output_file}")
    print("=" * 60)

if __name__ == "__main__":
    main()
