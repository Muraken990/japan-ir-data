#!/usr/bin/env python3
"""
Japan IR - 財務データ取得スクリプト（並列処理版）
5年分の財務諸表データをyfinanceから取得してJSON形式で保存
20並列処理で高速化

取得項目:
- 損益計算書: 売上高、売上総利益、営業利益、経常利益、純利益、EPS、営業利益率
- 貸借対照表: 総資産、純資産、有利子負債、現金、自己資本比率、D/E比率、流動比率
- キャッシュフロー: 営業CF、投資CF、財務CF、フリーCF
- 効率性: 純利益率、ROE、ROA
- 株価トレンド: MA乖離率（5/25/75/200日）
"""

import yfinance as yf
import pandas as pd
import calendar
import json
import os
import sys
import time
import argparse
from datetime import date, datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# 設定
INPUT_CSV_WORDPRESS = "data/wordpress_companies.csv"
INPUT_CSV_FALLBACK = "data/japan_companies_latest.csv"
OUTPUT_DIR = "data/financials"
MAX_WORKERS = 3  # 並列数（yfinance API制限対策）
MAX_RETRIES = 3
RETRY_DELAY = 5
PROGRESS_INTERVAL = 20
BATCH_SIZE = 50  # バッチサイズ
BATCH_DELAY = 45  # バッチ間の待機秒数

# スレッドセーフなカウンター
lock = threading.Lock()
progress_counter = {"success": 0, "error": 0, "total": 0}


class FinancialDataFetcher:
    """財務データ取得クラス"""

    def __init__(self, ticker_code, verbose=False):
        self.ticker_code = str(ticker_code).replace('.T', '')
        self.ticker_full = f"{self.ticker_code}.T"
        self.ticker = yf.Ticker(self.ticker_full)
        self.info = {}
        self.verbose = verbose
        self.fiscal_year_end_month = None

    def fetch(self):
        """財務データを取得"""
        for attempt in range(MAX_RETRIES):
            try:
                self.info = self.ticker.info

                if not self.info or len(self.info) <= 1:
                    raise Exception("Empty response from yfinance")

                # 履歴データ取得（MA計算用・配当抽出用を1回のリクエストで兼用）
                # period=5y: MA計算（最大200日）にも、配当履歴（直近5年）・前期実績配当の
                # 算出にも十分な長さ。actions=TrueがデフォルトでDividends列を含むため、
                # 別途 ticker.dividends を呼ぶ（内部でperiod="max"の再リクエストが発生する）
                # 必要がなくなる。
                hist = self.ticker.history(period="5y", interval="1d")

                result = {
                    "success": True,
                    "fetched_at": datetime.now().isoformat(),
                    "ticker": self.ticker_code,
                    "ticker_full": self.ticker_full,
                    "company_name": self.info.get("shortName", ""),
                    "company_info": self._get_company_info(),
                    "price_trend": self._calculate_ma_deviation(hist),
                    "financials": self._get_financials(),
                    "dividends": self._get_dividends(hist),
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

    def _calculate_ma_deviation(self, hist):
        """移動平均乖離率を計算"""
        if hist is None or hist.empty:
            return self._empty_ma_deviation()

        current_price = self.info.get("currentPrice") or self.info.get("regularMarketPrice", 0)
        if not current_price:
            return self._empty_ma_deviation()

        close_prices = hist['Close']

        def calc(period):
            if len(close_prices) >= period:
                ma = close_prices.tail(period).mean()
                deviation = ((current_price - ma) / ma) * 100
                return {
                    "ma_value": round(float(ma), 2),
                    "deviation": round(float(deviation), 2),
                    "trend": "up" if deviation > 0 else "down"
                }
            return {"ma_value": 0, "deviation": 0, "trend": "neutral"}

        return {
            "ma_5": calc(5),
            "ma_25": calc(25),
            "ma_75": calc(75),
            "ma_200": calc(200),
        }

    def _empty_ma_deviation(self):
        """空のMA乖離率データ"""
        empty = {"ma_value": 0, "deviation": 0, "trend": "neutral"}
        return {
            "ma_5": empty.copy(),
            "ma_25": empty.copy(),
            "ma_75": empty.copy(),
            "ma_200": empty.copy(),
        }

    def _get_company_info(self):
        """企業基本情報を取得"""
        try:
            return {
                "name_en": self.info.get("shortName", ""),
                "long_name": self.info.get("longName", ""),
                "sector": self.info.get("sector", ""),
                "industry": self.info.get("industry", ""),
                "website": self.info.get("website", ""),
                "employees": self.info.get("fullTimeEmployees"),
                "country": self.info.get("country", ""),
                "city": self.info.get("city", ""),
                "address": self.info.get("address1", ""),
                "description": self.info.get("longBusinessSummary", ""),
            }
        except Exception as e:
            return {
                "name_en": "",
                "long_name": "",
                "sector": "",
                "industry": "",
                "website": "",
                "employees": None,
                "country": "",
                "city": "",
                "address": "",
                "description": "",
                "error": str(e)
            }

    def _get_financials(self):
        """財務データ（5年分）"""
        try:
            income_stmt = self.ticker.financials
            balance_sheet = self.ticker.balance_sheet
            cashflow = self.ticker.cashflow

            years_data = []

            if not income_stmt.empty:
                self._detect_fiscal_year_end_month(income_stmt)
                for col in income_stmt.columns[:5]:
                    year = col.year if hasattr(col, 'year') else str(col)[:4]

                    # 損益計算書
                    revenue = self._safe_get(income_stmt, 'Total Revenue', col)
                    gross_profit = self._safe_get(income_stmt, 'Gross Profit', col)
                    operating_income = self._safe_get(income_stmt, 'Operating Income', col)
                    ebit = self._safe_get(income_stmt, 'EBIT', col)
                    net_income = self._safe_get(income_stmt, 'Net Income', col)
                    eps = self._safe_get(income_stmt, 'Diluted EPS', col)

                    # 貸借対照表
                    total_assets = 0
                    total_equity = 0
                    total_debt = 0
                    total_cash = 0
                    current_assets = 0
                    current_liabilities = 0
                    if not balance_sheet.empty and col in balance_sheet.columns:
                        total_assets = self._safe_get(balance_sheet, 'Total Assets', col)
                        total_equity = (self._safe_get(balance_sheet, 'Stockholders Equity', col) or
                                       self._safe_get(balance_sheet, 'Total Stockholder Equity', col))
                        total_debt = self._safe_get(balance_sheet, 'Total Debt', col)
                        total_cash = (self._safe_get(balance_sheet, 'Cash And Cash Equivalents', col) or
                                     self._safe_get(balance_sheet, 'Cash', col))
                        current_assets = self._safe_get(balance_sheet, 'Current Assets', col)
                        current_liabilities = self._safe_get(balance_sheet, 'Current Liabilities', col)

                    # キャッシュフロー
                    operating_cf = 0
                    investing_cf = 0
                    financing_cf = 0
                    free_cf = 0
                    if not cashflow.empty and col in cashflow.columns:
                        operating_cf = (self._safe_get(cashflow, 'Operating Cash Flow', col) or
                                       self._safe_get(cashflow, 'Total Cash From Operating Activities', col))
                        investing_cf = (self._safe_get(cashflow, 'Investing Cash Flow', col) or
                                       self._safe_get(cashflow, 'Total Cashflows From Investing Activities', col))
                        financing_cf = (self._safe_get(cashflow, 'Financing Cash Flow', col) or
                                       self._safe_get(cashflow, 'Total Cash From Financing Activities', col))
                        free_cf = (self._safe_get(cashflow, 'Free Cash Flow', col) or
                                  (operating_cf + self._safe_get(cashflow, 'Capital Expenditure', col)))

                    # 比率計算
                    operating_margin = (operating_income / revenue * 100) if revenue else 0
                    net_margin = (net_income / revenue * 100) if revenue else 0
                    equity_ratio = (total_equity / total_assets * 100) if total_assets else 0
                    roe = (net_income / total_equity * 100) if total_equity else 0
                    roa = (net_income / total_assets * 100) if total_assets else 0
                    de_ratio = (total_debt / total_equity) if total_equity else 0
                    current_ratio = (current_assets / current_liabilities) if current_liabilities else 0

                    years_data.append({
                        "year": int(year),
                        "revenue": revenue,
                        "revenue_fmt": self._format_large_number(revenue),
                        "gross_profit": gross_profit,
                        "gross_profit_fmt": self._format_large_number(gross_profit),
                        "operating_income": operating_income,
                        "operating_income_fmt": self._format_large_number(operating_income),
                        "ebit": ebit,
                        "ebit_fmt": self._format_large_number(ebit),
                        "net_income": net_income,
                        "net_income_fmt": self._format_large_number(net_income),
                        "eps": round(eps, 2) if eps else 0,
                        "operating_margin": round(operating_margin, 2),
                        "total_assets": total_assets,
                        "total_assets_fmt": self._format_large_number(total_assets),
                        "total_equity": total_equity,
                        "total_equity_fmt": self._format_large_number(total_equity),
                        "total_debt": total_debt,
                        "total_debt_fmt": self._format_large_number(total_debt),
                        "total_cash": total_cash,
                        "total_cash_fmt": self._format_large_number(total_cash),
                        "equity_ratio": round(equity_ratio, 2),
                        "de_ratio": round(de_ratio, 2),
                        "current_ratio": round(current_ratio, 2),
                        "operating_cf": operating_cf,
                        "operating_cf_fmt": self._format_large_number(operating_cf),
                        "investing_cf": investing_cf,
                        "investing_cf_fmt": self._format_large_number(investing_cf),
                        "financing_cf": financing_cf,
                        "financing_cf_fmt": self._format_large_number(financing_cf),
                        "free_cf": free_cf,
                        "free_cf_fmt": self._format_large_number(free_cf),
                        "net_margin": round(net_margin, 2),
                        "roe": round(roe, 2),
                        "roa": round(roa, 2),
                    })

            return {
                "years": years_data,
                "has_data": len(years_data) > 0
            }

        except Exception as e:
            return {
                "years": [],
                "has_data": False,
                "error": str(e)
            }

    def _get_trailing_fiscal_dividend(self, dividends, hist):
        """前期（直近の決算期）1年分の配当合計（前期実績）を計算する。

        無配（0円、履歴はあるが前期に支払いがない/配当履歴が空）と
        取得不可（lastFiscalYearEndが取れない、取得時にエラー、
        価格履歴の取得範囲が対象期間の開始日より浅い）を区別する。
        amount: None=取得不可 / 0.0=無配 / >0=前期実績配当額
        """
        try:
            fye_ts = self.info.get('lastFiscalYearEnd')
            if not fye_ts:
                return {"amount": None, "fiscal_year_end": None, "status": "unavailable"}

            fye = pd.Timestamp.fromtimestamp(int(fye_ts), tz='Asia/Tokyo').normalize()
            fye_str = fye.strftime('%Y-%m-%d')
            window_start = fye - pd.DateOffset(years=1) + pd.Timedelta(days=1)

            # 価格履歴の取得範囲が対象期間の開始日をカバーしていない場合、
            # 「無配」と「データ不足」を区別できないため取得不可にする
            # （例: yfinance側の都合で該当銘柄の履歴が直近数ヶ月分しか返らないケース）
            if hist is None or hist.empty or hist.index.min() > window_start:
                return {"amount": None, "fiscal_year_end": fye_str, "status": "unavailable"}

            if dividends is None:
                return {"amount": None, "fiscal_year_end": fye_str, "status": "unavailable"}

            if dividends.empty:
                # 配当履歴が空＝一度も配当していない会社 → 無配（0円）。取得不可にはしない。
                return {"amount": 0.0, "fiscal_year_end": fye_str, "status": "no_dividend"}

            mask = (dividends.index >= window_start) & (dividends.index <= fye)
            total = float(dividends[mask].sum())
            status = "ok" if total > 0 else "no_dividend"
            return {"amount": round(total, 4), "fiscal_year_end": fye_str, "status": status}

        except Exception as e:
            return {"amount": None, "fiscal_year_end": None, "status": "unavailable", "error": str(e)}

    def _get_dividends(self, hist):
        """配当情報（history()のDividends列から抽出、追加リクエストなし）"""
        try:
            if hist is not None and not hist.empty and 'Dividends' in hist.columns:
                dividends = hist['Dividends']
                dividends = dividends[dividends != 0]
            else:
                dividends = pd.Series(dtype=float)
            fiscal_year_end_month = self._detect_fiscal_year_end_month()
            aggregation_basis = "fiscal_year" if fiscal_year_end_month else "calendar_year"
            trailing_fiscal = self._get_trailing_fiscal_dividend(dividends, hist)

            if dividends is None or dividends.empty:
                return {
                    "history": [],
                    "has_data": False,
                    "aggregation_basis": aggregation_basis,
                    "fiscal_year_end_month": fiscal_year_end_month,
                    "trailing_fiscal": trailing_fiscal,
                }

            dividends_df = dividends.to_frame(name='dividend')
            if fiscal_year_end_month:
                dividends_df['year'] = [
                    dividend_date.year
                    if dividend_date.month <= fiscal_year_end_month
                    else dividend_date.year + 1
                    for dividend_date in dividends_df.index
                ]
            else:
                dividends_df['year'] = dividends_df.index.year
            yearly = dividends_df.groupby('year')['dividend'].sum().tail(5)
            today = date.today()

            history = []
            for year, total in yearly.items():
                period_end_month = fiscal_year_end_month or 12
                period_end = date(
                    int(year),
                    period_end_month,
                    calendar.monthrange(int(year), period_end_month)[1],
                )
                history.append({
                    "year": int(year),
                    "amount": round(float(total), 2),
                    "in_progress": period_end > today,
                })

            return {
                "history": history,
                "latest": round(float(dividends.iloc[-1]), 2) if len(dividends) > 0 else 0,
                "has_data": True,
                "aggregation_basis": aggregation_basis,
                "fiscal_year_end_month": fiscal_year_end_month,
                "trailing_fiscal": trailing_fiscal,
            }

        except Exception as e:
            return {
                "history": [],
                "has_data": False,
                "error": str(e),
                "trailing_fiscal": {"amount": None, "fiscal_year_end": None, "status": "unavailable", "error": str(e)},
            }

    def _detect_fiscal_year_end_month(self, income_stmt=None):
        """最新の年次財務諸表から決算期末月を取得する。"""
        if self.fiscal_year_end_month is not None:
            return self.fiscal_year_end_month

        try:
            if income_stmt is None:
                income_stmt = self.ticker.financials
            if income_stmt is None or income_stmt.empty or len(income_stmt.columns) == 0:
                return None

            month = int(getattr(income_stmt.columns[0], 'month', 0))
            if 1 <= month <= 12:
                self.fiscal_year_end_month = month
        except (TypeError, ValueError, AttributeError):
            return None

        return self.fiscal_year_end_month

    def _safe_get(self, df, row_name, col):
        """DataFrameから安全に値を取得"""
        try:
            if row_name in df.index:
                val = df.loc[row_name, col]
                if pd.isna(val):
                    return 0
                return float(val)
        except:
            pass
        return 0

    def _format_large_number(self, value):
        """大きな数値をフォーマット"""
        if not value:
            return "N/A"

        abs_value = abs(value)
        sign = "-" if value < 0 else ""

        if abs_value >= 1e12:
            return f"{sign}¥{abs_value/1e12:.1f}T"
        elif abs_value >= 1e8:
            return f"{sign}¥{abs_value/1e8:.0f}億"
        elif abs_value >= 1e6:
            return f"{sign}¥{abs_value/1e6:.1f}M"
        else:
            return f"{sign}¥{abs_value:,.0f}"


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


def process_company(code):
    """並列処理用のラッパー関数"""
    fetcher = FinancialDataFetcher(code, verbose=False)
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
    parser = argparse.ArgumentParser(description='Japan IR - 財務データ取得スクリプト（並列処理版）')
    parser.add_argument('--limit', type=int, help='処理する企業数を制限')
    parser.add_argument('--skip', type=int, default=0, help='スキップする企業数')
    parser.add_argument('--ticker', type=str, help='特定の銘柄のみ取得')
    parser.add_argument('--workers', type=int, default=MAX_WORKERS, help=f'並列数（デフォルト: {MAX_WORKERS}）')
    args = parser.parse_args()

    print("=" * 70)
    print("Japan IR - 財務データ取得（並列処理版）")
    print("=" * 70)
    start_time = datetime.now()
    print(f"開始: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"並列数: {args.workers}")

    # 出力ディレクトリ作成
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

    # 特定銘柄のみ取得（順次処理）
    if args.ticker:
        print(f"\n対象: {args.ticker}")
        fetcher = FinancialDataFetcher(args.ticker, verbose=True)
        data = fetcher.fetch()

        if data.get("success"):
            if save_to_json(data, args.ticker, OUTPUT_DIR):
                print(f"✅ 成功: {args.ticker}")
                print(f"出力: {OUTPUT_DIR}/{args.ticker}.json")
            else:
                print(f"❌ 保存失敗: {args.ticker}")
        else:
            print(f"❌ 取得失敗: {args.ticker} - {data.get('error')}")
        return

    # 企業リスト読み込み
    input_csv = None
    if os.path.exists(INPUT_CSV_WORDPRESS):
        input_csv = INPUT_CSV_WORDPRESS
        print(f"✅ WordPress登録企業リストを使用: {INPUT_CSV_WORDPRESS}")
    elif os.path.exists(INPUT_CSV_FALLBACK):
        input_csv = INPUT_CSV_FALLBACK
        print(f"⚠️  フォールバック使用: {INPUT_CSV_FALLBACK}")
    else:
        print(f"❌ エラー: 企業リストが見つかりません")
        sys.exit(1)

    df = pd.read_csv(input_csv)
    stock_codes = df['code'].tolist()

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
