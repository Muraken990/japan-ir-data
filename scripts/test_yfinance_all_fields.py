"""
yfinance 全データ取得テスト
対象: 時価総額上位20社
目的: どのデータが取得可能か確認
"""

import yfinance as yf
import pandas as pd
import time
from datetime import datetime
import os

REQUEST_DELAY = 2.0

TEST_TICKERS = [
    "7203.T", "8306.T", "9984.T", "6758.T", "6501.T",
    "8316.T", "9983.T", "7974.T", "8035.T", "6857.T",
    "8058.T", "8001.T", "8411.T", "9432.T", "4519.T",
    "7011.T", "6861.T", "6098.T", "8031.T", "6902.T",
]

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
    "priceToSalesTrailing12Months", "pegRatio", "enterpriseValue",
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
    "shortRatio", "shortPercentOfFloat",
]

def main():
    os.makedirs("output", exist_ok=True)
    
    results = []
    field_availability = {field: 0 for field in INFO_FIELDS}
    
    print(f"テスト開始: {len(TEST_TICKERS)}社")
    
    for i, ticker in enumerate(TEST_TICKERS, 1):
        print(f"[{i}/{len(TEST_TICKERS)}] {ticker}")
        
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            
            row = {"ticker": ticker}
            for field in INFO_FIELDS:
                value = info.get(field)
                row[field] = value
                if value is not None and value != "":
                    field_availability[field] += 1
            
            row["status"] = "success"
            results.append(row)
            print(f"    → 成功")
        except Exception as e:
            results.append({"ticker": ticker, "status": f"error: {e}"})
            print(f"    → エラー: {e}")
        
        if i < len(TEST_TICKERS):
            time.sleep(REQUEST_DELAY)
    
    # CSV出力
    df = pd.DataFrame(results)
    df.to_csv("output/test_all_fields.csv", index=False, encoding="utf-8-sig")
    
    # サマリー出力
    total = len([r for r in results if r.get("status") == "success"])
    summary = [{"field": f, "count": c, "rate": f"{c/total*100:.0f}%"} 
               for f, c in sorted(field_availability.items(), key=lambda x: -x[1])]
    pd.DataFrame(summary).to_csv("output/field_summary.csv", index=False)
    
    # 結果表示
    print("\n" + "=" * 50)
    print(f"✅ 全社取得可能:")
    for f, c in field_availability.items():
        if c == total:
            print(f"   {f}")
    
    print(f"\n⚠️ 一部取得可能:")
    for f, c in field_availability.items():
        if 0 < c < total:
            print(f"   {f}: {c}/{total}")
    
    print(f"\n❌ 取得不可:")
    for f, c in field_availability.items():
        if c == 0:
            print(f"   {f}")

if __name__ == "__main__":
    main()
