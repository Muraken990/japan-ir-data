#!/usr/bin/env python3
"""
Japan IR - アナリスト予想・決算日程・株主情報取得スクリプト（並列処理版）
yfinanceからアナリスト推奨・目標株価・決算日程・株主構成を取得してJSON形式で保存

取得項目:
- アナリスト推奨: Strong Buy/Buy/Hold/Sell/Strong Sell 件数
- 目標株価: Current/High/Low/Mean/Median
- 決算日程: 次回・過去の決算日とEPS予想/実績
- 株主構成: インサイダー/機関投資家保有比率、主要株主リスト
"""

import yfinance as yf
import pandas as pd
import json
import os
import sys
import time
import argparse
import requests
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# 設定
OUTPUT_DIR = "data/analyst_earnings"
MAX_WORKERS = 3  # 並列数（yfinance API制限対策）
MAX_RETRIES = 3
RETRY_DELAY = 5
PROGRESS_INTERVAL = 20
BATCH_SIZE = 50  # バッチサイズ
BATCH_DELAY = 45  # バッチ間の待機秒数

# WordPress REST API設定
WP_SITE_URL = os.getenv('WP_SITE_URL', 'https://japanir.jp')
WP_API_URL = f"{WP_SITE_URL}/wp-json/wp/v2/company"
REQUEST_TIMEOUT = 30
MAX_PAGES = 50  # 最大50ページ（5000社）

# スレッドセーフなカウンター
lock = threading.Lock()
progress_counter = {"success": 0, "error": 0, "total": 0}


class AnalystEarningsFetcher:
    """アナリスト予想・決算日程取得クラス"""

    def __init__(self, ticker_code, verbose=False):
        self.ticker_code = str(ticker_code).replace('.T', '')
        self.ticker_full = f"{self.ticker_code}.T"
        self.ticker = yf.Ticker(self.ticker_full)
        self.info = {}
        self.verbose = verbose

    def fetch(self):
        """データを取得"""
        for attempt in range(MAX_RETRIES):
            try:
                self.info = self.ticker.info

                if not self.info or len(self.info) <= 1:
                    raise Exception("Empty response from yfinance")

                result = {
                    "success": True,
                    "fetched_at": datetime.now().isoformat(),
                    "ticker": self.ticker_code,
                    "ticker_full": self.ticker_full,
                    "company_name": self.info.get("shortName", ""),
                    "analyst_recommendations": self._get_recommendations(),
                    "target_prices": self._get_target_prices(),
                    "earnings_dates": self._get_earnings_dates(),
                    "shareholders": self._get_shareholders(),
                }

                return result

            except Exception as e:
                error_msg = str(e)
                if self.verbose:
                    print(f"    Attempt {attempt + 1}/{MAX_RETRIES} failed: {error_msg}")

                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    continue

                return {
                    "success": False,
                    "error": error_msg,
                    "ticker": self.ticker_code
                }

        return {
            "success": False,
            "error": "Max retries exceeded",
            "ticker": self.ticker_code
        }

    def _get_recommendations(self):
        """アナリスト推奨を取得"""
        try:
            recs = self.ticker.recommendations

            if recs is None or recs.empty:
                return self._get_recommendations_from_info()

            # 直近のレコードを取得
            latest = recs.iloc[-1] if len(recs) > 0 else None

            if latest is not None:
                return {
                    "has_data": True,
                    "period": str(latest.name) if hasattr(latest, 'name') else None,
                    "strong_buy": int(latest.get('strongBuy', 0) or 0),
                    "buy": int(latest.get('buy', 0) or 0),
                    "hold": int(latest.get('hold', 0) or 0),
                    "sell": int(latest.get('sell', 0) or 0),
                    "strong_sell": int(latest.get('strongSell', 0) or 0),
                    "total_analysts": int(
                        (latest.get('strongBuy', 0) or 0) +
                        (latest.get('buy', 0) or 0) +
                        (latest.get('hold', 0) or 0) +
                        (latest.get('sell', 0) or 0) +
                        (latest.get('strongSell', 0) or 0)
                    ),
                    # info からの補足データ
                    "recommendation_key": self.info.get("recommendationKey", ""),
                    "recommendation_mean": self.info.get("recommendationMean"),
                }

            return self._get_recommendations_from_info()

        except Exception as e:
            if self.verbose:
                print(f"    Recommendations error: {e}")
            return self._get_recommendations_from_info()

    def _get_recommendations_from_info(self):
        """info から推奨データを取得（フォールバック）"""
        rec_key = self.info.get("recommendationKey", "")
        rec_mean = self.info.get("recommendationMean")
        num_analysts = self.info.get("numberOfAnalystOpinions")

        if rec_key or rec_mean or num_analysts:
            return {
                "has_data": True,
                "recommendation_key": rec_key,
                "recommendation_mean": rec_mean,
                "total_analysts": num_analysts,
                "strong_buy": None,
                "buy": None,
                "hold": None,
                "sell": None,
                "strong_sell": None,
            }

        return {"has_data": False}

    def _get_target_prices(self):
        """アナリスト目標株価を取得"""
        try:
            targets = self.ticker.analyst_price_targets

            if targets is not None and hasattr(targets, 'get'):
                return {
                    "has_data": True,
                    "current": targets.get('current'),
                    "high": targets.get('high'),
                    "low": targets.get('low'),
                    "mean": targets.get('mean'),
                    "median": targets.get('median'),
                }
            elif targets is not None:
                # DataFrame形式の場合
                return {
                    "has_data": True,
                    "current": getattr(targets, 'current', None),
                    "high": getattr(targets, 'high', None),
                    "low": getattr(targets, 'low', None),
                    "mean": getattr(targets, 'mean', None),
                    "median": getattr(targets, 'median', None),
                }

            # info からフォールバック
            target_mean = self.info.get("targetMeanPrice")
            target_high = self.info.get("targetHighPrice")
            target_low = self.info.get("targetLowPrice")
            target_median = self.info.get("targetMedianPrice")

            if any([target_mean, target_high, target_low, target_median]):
                return {
                    "has_data": True,
                    "current": self.info.get("currentPrice"),
                    "high": target_high,
                    "low": target_low,
                    "mean": target_mean,
                    "median": target_median,
                }

            return {"has_data": False}

        except Exception as e:
            if self.verbose:
                print(f"    Target prices error: {e}")

            # info からフォールバック
            target_mean = self.info.get("targetMeanPrice")
            if target_mean:
                return {
                    "has_data": True,
                    "current": self.info.get("currentPrice"),
                    "high": self.info.get("targetHighPrice"),
                    "low": self.info.get("targetLowPrice"),
                    "mean": target_mean,
                    "median": self.info.get("targetMedianPrice"),
                }

            return {"has_data": False}

    def _get_earnings_dates(self):
        """決算日程を取得"""
        try:
            earnings = self.ticker.earnings_dates

            if earnings is None or earnings.empty:
                return {"has_data": False}

            # 今日の日付
            today = pd.Timestamp.now().tz_localize(None)

            # 将来の決算日（次回決算）
            future_earnings = []
            past_earnings = []

            for idx, row in earnings.iterrows():
                # インデックスが日付
                date = idx
                if hasattr(date, 'tz_localize'):
                    date = date.tz_localize(None) if date.tzinfo else date
                elif hasattr(date, 'replace'):
                    date = date.replace(tzinfo=None)

                entry = {
                    "date": str(date.date()) if hasattr(date, 'date') else str(date)[:10],
                    "eps_estimate": self._safe_float(row.get('EPS Estimate')),
                    "eps_actual": self._safe_float(row.get('Reported EPS')),
                    "surprise_pct": self._safe_float(row.get('Surprise(%)')),
                }

                try:
                    if pd.Timestamp(date) >= today:
                        future_earnings.append(entry)
                    else:
                        past_earnings.append(entry)
                except:
                    past_earnings.append(entry)

            # 次回決算（最も近い将来の日付）
            next_earnings = future_earnings[0] if future_earnings else None

            # 過去決算（直近5件）
            past_earnings = past_earnings[:5]

            return {
                "has_data": True,
                "next_earnings": next_earnings,
                "future_count": len(future_earnings),
                "past_earnings": past_earnings,
            }

        except Exception as e:
            if self.verbose:
                print(f"    Earnings dates error: {e}")
            return {"has_data": False, "error": str(e)}

    def _safe_float(self, value):
        """安全にfloatに変換"""
        if value is None:
            return None
        if pd.isna(value):
            return None
        try:
            return round(float(value), 2)
        except:
            return None

    def _get_shareholders(self):
        """株主構成を取得"""
        try:
            result = {
                "has_data": False,
                "insider_pct": None,
                "institution_pct": None,
                "major_holders": [],
                "institutional_holders": [],
                "mutualfund_holders": []
            }

            # 主要株主比率（インサイダー・機関投資家）
            try:
                major_holders = self.ticker.major_holders
                if major_holders is not None and not major_holders.empty:
                    # 標準的なyfinanceの順序: [0]=Insider, [1]=Institutions, [2]=Float held by Inst, [3]=Num of Inst
                    for idx, row in major_holders.iterrows():
                        label = str(row.iloc[1]).lower() if len(row) > 1 else ""
                        value = row.iloc[0]

                        # ラベルベースのマッチング
                        if "insider" in label:
                            result["insider_pct"] = self._safe_float(value * 100) if value < 1 else self._safe_float(value)
                        elif "institution" in label and "float" not in label:
                            result["institution_pct"] = self._safe_float(value * 100) if value < 1 else self._safe_float(value)

                        result["major_holders"].append({
                            "label": str(row.iloc[1]) if len(row) > 1 else "",
                            "value": self._safe_float(value * 100) if value and value < 1 else self._safe_float(value)
                        })

                    # ラベルが空の場合、位置ベースで取得（日本株対応）
                    # yfinanceの標準順序: [0]=Insider%, [1]=Institutions%, [2]=Float%, [3]=Num of Inst
                    if result["insider_pct"] is None and len(major_holders) >= 1:
                        val = major_holders.iloc[0, 0]
                        # 小数(0.15)ならx100、既にパーセント(15.0)ならそのまま
                        result["insider_pct"] = self._safe_float(val * 100) if val and val < 1 else self._safe_float(val)
                    if result["institution_pct"] is None and len(major_holders) >= 2:
                        val = major_holders.iloc[1, 0]
                        result["institution_pct"] = self._safe_float(val * 100) if val and val < 1 else self._safe_float(val)

                    result["has_data"] = True
            except Exception as e:
                if self.verbose:
                    print(f"    Major holders error: {e}")

            # 機関投資家リスト
            try:
                inst_holders = self.ticker.institutional_holders
                if inst_holders is not None and not inst_holders.empty:
                    for idx, row in inst_holders.head(10).iterrows():
                        # pctHeld または % Out カラムを取得（yfinanceバージョン差異対応）
                        pct_held = row.get("pctHeld") if pd.notna(row.get("pctHeld")) else row.get("% Out")
                        pct_change = row.get("pctChange") if pd.notna(row.get("pctChange")) else None
                        holder = {
                            "holder": str(row.get("Holder", "")),
                            "shares": int(row.get("Shares", 0)) if pd.notna(row.get("Shares")) else None,
                            "date_reported": str(row.get("Date Reported", ""))[:10] if pd.notna(row.get("Date Reported")) else None,
                            "pct_held": self._safe_float(pct_held * 100) if pd.notna(pct_held) and pct_held < 1 else self._safe_float(pct_held),
                            "pct_change": self._safe_float(pct_change * 100) if pd.notna(pct_change) and abs(pct_change) < 1 else self._safe_float(pct_change),
                            "value": int(row.get("Value", 0)) if pd.notna(row.get("Value")) else None
                        }
                        result["institutional_holders"].append(holder)
                    result["has_data"] = True
            except Exception as e:
                if self.verbose:
                    print(f"    Institutional holders error: {e}")

            # ミューチュアルファンドリスト
            try:
                mf_holders = self.ticker.mutualfund_holders
                if mf_holders is not None and not mf_holders.empty:
                    for idx, row in mf_holders.head(10).iterrows():
                        # pctHeld または % Out カラムを取得（yfinanceバージョン差異対応）
                        pct_held = row.get("pctHeld") if pd.notna(row.get("pctHeld")) else row.get("% Out")
                        pct_change = row.get("pctChange") if pd.notna(row.get("pctChange")) else None
                        holder = {
                            "holder": str(row.get("Holder", "")),
                            "shares": int(row.get("Shares", 0)) if pd.notna(row.get("Shares")) else None,
                            "date_reported": str(row.get("Date Reported", ""))[:10] if pd.notna(row.get("Date Reported")) else None,
                            "pct_held": self._safe_float(pct_held * 100) if pd.notna(pct_held) and pct_held < 1 else self._safe_float(pct_held),
                            "pct_change": self._safe_float(pct_change * 100) if pd.notna(pct_change) and abs(pct_change) < 1 else self._safe_float(pct_change),
                            "value": int(row.get("Value", 0)) if pd.notna(row.get("Value")) else None
                        }
                        result["mutualfund_holders"].append(holder)
                    result["has_data"] = True
            except Exception as e:
                if self.verbose:
                    print(f"    Mutualfund holders error: {e}")

            return result

        except Exception as e:
            if self.verbose:
                print(f"    Shareholders error: {e}")
            return {"has_data": False, "error": str(e)}


def save_to_json(data, code, output_dir):
    """JSONファイルに保存"""
    if data is None:
        return False

    output_file = os.path.join(output_dir, f"{code}.json")

    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        return False


def fetch_companies_from_wordpress():
    """WordPress REST APIから登録済み企業の証券コードを取得"""
    print(f"📥 WordPress REST APIから企業リスト取得中...")
    print(f"   API URL: {WP_API_URL}")

    stock_codes = []
    offset = 0
    per_page = 100
    max_companies = MAX_PAGES * per_page

    while offset < max_companies:
        params = {
            "per_page": per_page,
            "offset": offset,
            "_fields": "id,stock_code",
            "status": "publish"
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.get(
                    WP_API_URL,
                    params=params,
                    timeout=REQUEST_TIMEOUT
                )

                if response.status_code == 200:
                    companies = response.json()

                    if not companies:
                        print(f"   ✅ 取得完了: {len(stock_codes)}社")
                        return stock_codes

                    for company in companies:
                        code = company.get('stock_code', '')
                        if code and isinstance(code, str) and len(code) == 4 and code.isalnum():
                            stock_codes.append(code)

                    offset += per_page
                    time.sleep(0.3)
                    break

                elif response.status_code == 400:
                    print(f"   ✅ 取得完了: {len(stock_codes)}社")
                    return stock_codes

                else:
                    print(f"   ⚠️  HTTPエラー: {response.status_code}")
                    if attempt < MAX_RETRIES:
                        time.sleep(2)
                    else:
                        break

            except Exception as e:
                print(f"   ❌ 接続エラー: {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(2)
                else:
                    break

    print(f"   ✅ 取得完了: {len(stock_codes)}社")
    return stock_codes


def process_company(code):
    """並列処理用のラッパー関数"""
    fetcher = AnalystEarningsFetcher(code, verbose=False)
    data = fetcher.fetch()
    success = False

    if data.get("success"):
        success = save_to_json(data, code, OUTPUT_DIR)

    # スレッドセーフにカウンターを更新
    with lock:
        progress_counter["total"] += 1
        if success:
            progress_counter["success"] += 1
        else:
            progress_counter["error"] += 1

    return {"code": code, "success": success, "data": data}


def main():
    parser = argparse.ArgumentParser(description='Japan IR - アナリスト予想・決算日程取得スクリプト')
    parser.add_argument('--limit', type=int, help='処理する企業数を制限')
    parser.add_argument('--skip', type=int, default=0, help='スキップする企業数')
    parser.add_argument('--ticker', type=str, help='特定の銘柄のみ取得')
    parser.add_argument('--workers', type=int, default=MAX_WORKERS, help=f'並列数（デフォルト: {MAX_WORKERS}）')
    args = parser.parse_args()

    print("=" * 70)
    print("Japan IR - アナリスト予想・決算日程取得（並列処理版）")
    print("=" * 70)
    start_time = datetime.now()
    print(f"開始: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"並列数: {args.workers}")

    # 出力ディレクトリ作成
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    # 特定銘柄のみ取得（順次処理）
    if args.ticker:
        print(f"\n対象: {args.ticker}")
        fetcher = AnalystEarningsFetcher(args.ticker, verbose=True)
        data = fetcher.fetch()

        if data.get("success"):
            if save_to_json(data, args.ticker, OUTPUT_DIR):
                print(f"✅ 成功: {args.ticker}")
                print(f"出力: {OUTPUT_DIR}/{args.ticker}.json")
                # データ内容を表示
                print("\n--- 取得データ ---")
                print(json.dumps(data, ensure_ascii=False, indent=2))
            else:
                print(f"❌ 保存失敗: {args.ticker}")
        else:
            print(f"❌ 取得失敗: {args.ticker} - {data.get('error')}")
        return

    # WordPress REST APIから企業リスト取得
    stock_codes = fetch_companies_from_wordpress()

    if not stock_codes:
        print(f"❌ エラー: 企業リストを取得できませんでした")
        sys.exit(1)

    # 範囲指定
    if args.skip > 0:
        stock_codes = stock_codes[args.skip:]
        print(f"⏭️  最初の{args.skip}社をスキップ")

    if args.limit:
        stock_codes = stock_codes[:args.limit]
        print(f"📊 処理対象: {len(stock_codes)}社（limit: {args.limit}）")

    total = len(stock_codes)
    num_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    batch_wait_time = (num_batches - 1) * BATCH_DELAY
    processing_time = total / args.workers * 3
    estimated_time = (processing_time + batch_wait_time) / 60
    print(f"対象企業数: {total}社")
    print(f"予想時間: 約{estimated_time:.0f}分（バッチ待機含む）")
    print()

    last_progress_print = 0
    workers = args.workers

    # バッチ処理（API制限対策）
    batches = [stock_codes[i:i + BATCH_SIZE] for i in range(0, len(stock_codes), BATCH_SIZE)]
    total_batches = len(batches)
    print(f"バッチ数: {total_batches}（{BATCH_SIZE}社/バッチ、{BATCH_DELAY}秒間隔）")
    print()

    for batch_idx, batch in enumerate(batches, 1):
        print(f"--- バッチ {batch_idx}/{total_batches} ({len(batch)}社) ---")

        # 並列処理
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_code = {executor.submit(process_company, code): code for code in batch}

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

        # バッチ間の待機（最後のバッチ以外）
        if batch_idx < total_batches:
            print(f"    💤 {BATCH_DELAY}秒待機...")
            time.sleep(BATCH_DELAY)

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
    print(f"並列数: {workers}")
    print(f"出力先: {OUTPUT_DIR}/")
    print("=" * 70)


if __name__ == "__main__":
    main()
