#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Japan IR - 株価履歴データ取得スクリプト
過去5年分の日次株価データをyfinanceから取得してJSON形式で保存
"""

import yfinance as yf
import pandas as pd
import json
import os
import time
from datetime import datetime
from pathlib import Path

# 設定
INPUT_CSV = "data/japan_companies_latest.csv"
OUTPUT_DIR = "data/stock_history"
REQUEST_DELAY = 2.0  # レート制限対策
HISTORY_PERIOD = "5y"  # 5年分

def fetch_stock_history(code):
    """
    指定された証券コードの株価履歴を取得

    Args:
        code: 証券コード（例: 7203）

    Returns:
        dict: 株価履歴データ or None（エラー時）
    """
    ticker_symbol = f"{code}.T"

    try:
        print(f"  取得中: {code} ({ticker_symbol})")

        ticker = yf.Ticker(ticker_symbol)
        hist = ticker.history(period=HISTORY_PERIOD)

        if hist.empty:
            print(f"  ⚠️  データなし: {code}")
            return None

        # DataFrameをリスト形式に変換
        data_list = []
        for date, row in hist.iterrows():
            data_list.append({
                "date": date.strftime("%Y-%m-%d"),
                "open": round(float(row['Open']), 2) if pd.notna(row['Open']) else None,
                "high": round(float(row['High']), 2) if pd.notna(row['High']) else None,
                "low": round(float(row['Low']), 2) if pd.notna(row['Low']) else None,
                "close": round(float(row['Close']), 2) if pd.notna(row['Close']) else None,
                "volume": int(row['Volume']) if pd.notna(row['Volume']) else None
            })

        result = {
            "code": code,
            "ticker": ticker_symbol,
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "period": HISTORY_PERIOD,
            "data_points": len(data_list),
            "data": data_list
        }

        print(f"  ✅ 成功: {code} ({len(data_list)}件)")
        return result

    except Exception as e:
        print(f"  ❌ エラー: {code} - {str(e)}")
        return None

def save_to_json(data, code):
    """
    データをJSON形式で保存

    Args:
        data: 株価履歴データ
        code: 証券コード
    """
    if data is None:
        return False

    output_file = os.path.join(OUTPUT_DIR, f"{code}.json")

    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"  ❌ 保存エラー: {code} - {str(e)}")
        return False

def main():
    print("=" * 70)
    print("Japan IR - 株価履歴データ取得")
    print("=" * 70)
    print(f"期間: {HISTORY_PERIOD}")
    print(f"リクエスト間隔: {REQUEST_DELAY}秒")
    print()

    start_time = datetime.now()

    # 出力ディレクトリ作成
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    # 企業リスト読み込み
    if not os.path.exists(INPUT_CSV):
        print(f"❌ エラー: {INPUT_CSV} が見つかりません")
        return

    df = pd.read_csv(INPUT_CSV)
    stock_codes = df['code'].tolist()
    total = len(stock_codes)

    print(f"対象企業数: {total}社")
    print()

    success_count = 0
    error_count = 0

    for i, code in enumerate(stock_codes, 1):
        print(f"[{i}/{total}] {code}")

        # データ取得
        data = fetch_stock_history(code)

        # JSON保存
        if data and save_to_json(data, code):
            success_count += 1
        else:
            error_count += 1

        # 進捗表示
        if i % 10 == 0 or i == total:
            elapsed = (datetime.now() - start_time).total_seconds()
            eta = (elapsed / i) * (total - i) / 60
            print()
            print(f"進捗: {i}/{total} | 成功: {success_count} | 失敗: {error_count} | 残り時間: {eta:.1f}分")
            print()

        # レート制限対策（最後以外）
        if i < total:
            time.sleep(REQUEST_DELAY)

    # 完了サマリー
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()

    print()
    print("=" * 70)
    print(f"完了: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"所要時間: {elapsed/60:.1f}分 ({elapsed:.0f}秒)")
    print(f"成功: {success_count}社 ({success_count/total*100:.1f}%)")
    print(f"失敗: {error_count}社")
    print(f"出力先: {OUTPUT_DIR}/")
    print("=" * 70)

if __name__ == "__main__":
    main()
