#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Japan IR - 株価履歴データ取得スクリプト（並列処理版）
過去5年分の日次株価データをyfinanceから取得してJSON形式で保存
20並列処理で高速化
"""

import yfinance as yf
import pandas as pd
import json
import os
import time
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# 設定
INPUT_CSV_WORDPRESS = "data/wordpress_companies.csv"  # WordPress登録企業（優先）
INPUT_CSV_FALLBACK = "data/japan_companies_latest.csv"  # 全企業（フォールバック）
OUTPUT_DIR = "data/stock_history"
MAX_WORKERS = 20  # 並列数
RETRY_DELAY = 5  # リトライ時の待機秒数
MAX_RETRIES = 2
HISTORY_PERIOD = "5y"  # 5年分
PROGRESS_INTERVAL = 50

# スレッドセーフなカウンター
lock = threading.Lock()
progress_counter = {"success": 0, "error": 0, "total": 0}

def fetch_stock_history(code):
    """
    指定された証券コードの株価履歴を取得

    Args:
        code: 証券コード（例: 7203）

    Returns:
        dict: 株価履歴データ or None（エラー時）
    """
    ticker_symbol = f"{code}.T"

    for attempt in range(MAX_RETRIES):
        try:
            ticker = yf.Ticker(ticker_symbol)
            hist = ticker.history(period=HISTORY_PERIOD)

            if hist.empty:
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

            return result

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
                continue
            return None

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
        return False

def process_company(code):
    """並列処理用のラッパー関数"""
    data = fetch_stock_history(code)
    success = save_to_json(data, code)

    # スレッドセーフにカウンターを更新
    with lock:
        progress_counter["total"] += 1
        if success:
            progress_counter["success"] += 1
        else:
            progress_counter["error"] += 1

    return {"code": code, "success": success}

def main():
    print("=" * 70)
    print("Japan IR - 株価履歴データ取得（並列処理版）")
    print("=" * 70)
    print(f"期間: {HISTORY_PERIOD}")
    print(f"並列数: {MAX_WORKERS}")
    print()

    start_time = datetime.now()

    # 出力ディレクトリ作成
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    # 企業リスト読み込み（WordPress優先）
    input_csv = None
    source_type = None

    if os.path.exists(INPUT_CSV_WORDPRESS):
        input_csv = INPUT_CSV_WORDPRESS
        source_type = "WordPress登録企業"
        print(f"✅ WordPress登録企業リストを使用: {INPUT_CSV_WORDPRESS}")
    elif os.path.exists(INPUT_CSV_FALLBACK):
        input_csv = INPUT_CSV_FALLBACK
        source_type = "全企業（フォールバック）"
        print(f"⚠️  WordPress企業リストが見つからないため、全企業リストを使用: {INPUT_CSV_FALLBACK}")
    else:
        print(f"❌ エラー: 企業リストが見つかりません")
        print(f"  - {INPUT_CSV_WORDPRESS}")
        print(f"  - {INPUT_CSV_FALLBACK}")
        return

    df = pd.read_csv(input_csv)
    stock_codes = df['code'].tolist()
    total = len(stock_codes)

    print(f"データソース: {source_type}")
    print(f"対象企業数: {total}社")
    print(f"予想時間: 約{total / MAX_WORKERS * 2 / 60:.0f}分")
    print()

    last_progress_print = 0

    # 並列処理
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_code = {executor.submit(process_company, code): code for code in stock_codes}

        for future in as_completed(future_to_code):
            try:
                future.result()
            except Exception as e:
                with lock:
                    progress_counter["total"] += 1
                    progress_counter["error"] += 1

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

    # 完了サマリー
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()

    success_count = progress_counter["success"]
    error_count = progress_counter["error"]

    print()
    print("=" * 70)
    print(f"完了: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"所要時間: {elapsed/60:.1f}分 ({elapsed:.0f}秒)")
    print(f"成功: {success_count}社 ({success_count/total*100:.1f}%)")
    print(f"失敗: {error_count}社")
    print(f"並列数: {MAX_WORKERS}")
    print(f"出力先: {OUTPUT_DIR}/")
    print("=" * 70)

if __name__ == "__main__":
    main()
