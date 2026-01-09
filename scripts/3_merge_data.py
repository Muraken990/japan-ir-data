#!/usr/bin/env python3
"""
Japan IR - データ統合スクリプト（がっちゃんこ）
Yahoo Japan Finance + yfinance → 統合データ生成
GitHub Actions対応版
"""

import pandas as pd
import sys
import os
from datetime import datetime

# ============================================================
# 設定
# ============================================================

# 入力ファイル（GitHub Actions環境で動的に指定可能）
YAHOO_JP_CSV = os.getenv('YAHOO_JP_CSV', 'data/japan_companies_latest.csv')
YFINANCE_CSV = os.getenv('YFINANCE_CSV', 'output/yfinance_all_fields_latest.csv')

# 出力ファイル
OUTPUT_DIR = os.getenv('OUTPUT_DIR', 'data')
OUTPUT_FILENAME = 'integrated_company_data.csv'

# ============================================================
# 関数定義
# ============================================================

def load_yahoo_jp_data(filepath):
    """Yahoo Japanのデータを読み込み"""
    print(f"📥 Yahoo JPデータ読み込み: {filepath}")
    
    try:
        df = pd.read_csv(filepath, encoding='utf-8-sig')
        
        # 必須カラムの確認
        required_columns = ['code', 'company_name']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"必須カラムが見つかりません: {missing_columns}")
        
        # データ型を文字列に統一
        df['code'] = df['code'].astype(str).str.strip()
        df['company_name'] = df['company_name'].astype(str).str.strip()
        
        print(f"   ✅ 読み込み成功: {len(df)}社")
        print(f"   カラム: {list(df.columns)}")
        
        return df
        
    except FileNotFoundError:
        print(f"   ❌ ファイルが見つかりません: {filepath}")
        sys.exit(1)
    except Exception as e:
        print(f"   ❌ エラー: {e}")
        sys.exit(1)


def load_yfinance_data(filepath):
    """yfinanceのデータを読み込み"""
    print(f"📥 yfinanceデータ読み込み: {filepath}")
    
    try:
        df = pd.read_csv(filepath, encoding='utf-8-sig')
        
        # codeカラムの確認
        if 'code' not in df.columns:
            raise ValueError("codeカラムが見つかりません")
        
        # データ型を文字列に統一
        df['code'] = df['code'].astype(str).str.strip()
        
        # 成功データのみフィルタリング
        if 'status' in df.columns:
            before_count = len(df)
            df = df[df['status'] == 'success'].copy()
            failed_count = before_count - len(df)
            if failed_count > 0:
                print(f"   ⚠️  失敗データを除外: {failed_count}社")
        
        print(f"   ✅ 読み込み成功: {len(df)}社")
        print(f"   カラム数: {len(df.columns)}")
        
        return df
        
    except FileNotFoundError:
        print(f"   ❌ ファイルが見つかりません: {filepath}")
        sys.exit(1)
    except Exception as e:
        print(f"   ❌ エラー: {e}")
        sys.exit(1)


def merge_data(yahoo_df, yfinance_df):
    """Yahoo JPとyfinanceデータを統合"""
    print("\n🔗 データ統合中...")
    
    # codeをキーに左結合（Yahoo JPをマスターに）
    merged = yahoo_df.merge(
        yfinance_df,
        on='code',
        how='left',
        suffixes=('_yahoo', '_yf')
    )
    
    print(f"   統合後: {len(merged)}行")
    
    # yfinanceデータがマッチした企業数
    matched_count = merged['longName'].notna().sum()
    unmatched_count = len(merged) - matched_count
    
    print(f"   ✅ マッチ成功: {matched_count}社 ({matched_count/len(merged)*100:.1f}%)")
    if unmatched_count > 0:
        print(f"   ⚠️  マッチ失敗: {unmatched_count}社")
    
    return merged


def create_integrated_dataframe(merged_df):
    """統合データから最終的なDataFrameを作成"""
    print("\n📊 最終データ構造を作成中...")
    
    # 統合データ構造
    integrated = pd.DataFrame({
        # 基本識別情報
        'code': merged_df['code'],
        'ticker': merged_df.get('ticker', merged_df['code'] + '.T'),
        
        # 企業名（多言語）
        'company_name_ja': merged_df['company_name'],  # Yahoo JP
        'company_name_en': merged_df.get('longName', ''),  # yfinance
        'short_name_en': merged_df.get('shortName', ''),  # yfinance
        
        # 株価データ
        'currentPrice': merged_df.get('currentPrice', None),
        'previousClose': merged_df.get('previousClose', None),
        'open': merged_df.get('open', None),
        'dayHigh': merged_df.get('dayHigh', None),
        'dayLow': merged_df.get('dayLow', None),
        
        # 時価総額・出来高
        'marketCap': merged_df.get('marketCap', None),
        'volume': merged_df.get('volume', None),
        'averageVolume': merged_df.get('averageVolume', None),
        
        # 52週高値・安値
        'fiftyTwoWeekHigh': merged_df.get('fiftyTwoWeekHigh', None),
        'fiftyTwoWeekLow': merged_df.get('fiftyTwoWeekLow', None),
        
        # 企業情報
        'sector': merged_df.get('sector', ''),
        'industry': merged_df.get('industry', ''),
        'country': merged_df.get('country', ''),
        'city': merged_df.get('city', ''),
        'website': merged_df.get('website', ''),
        'fullTimeEmployees': merged_df.get('fullTimeEmployees', None),
        
        # 財務指標
        'trailingPE': merged_df.get('trailingPE', None),
        'forwardPE': merged_df.get('forwardPE', None),
        'priceToBook': merged_df.get('priceToBook', None),
        'returnOnEquity': merged_df.get('returnOnEquity', None),
        'returnOnAssets': merged_df.get('returnOnAssets', None),
        'profitMargins': merged_df.get('profitMargins', None),
        
        # 配当
        'dividendRate': merged_df.get('dividendRate', None),
        'dividendYield': merged_df.get('dividendYield', None),

        # Price Trend (MA乖離率)
        'ma_5_value': merged_df.get('ma_5_value', None),
        'ma_5_deviation': merged_df.get('ma_5_deviation', None),
        'ma_5_trend': merged_df.get('ma_5_trend', ''),
        'ma_25_value': merged_df.get('ma_25_value', None),
        'ma_25_deviation': merged_df.get('ma_25_deviation', None),
        'ma_25_trend': merged_df.get('ma_25_trend', ''),
        'ma_75_value': merged_df.get('ma_75_value', None),
        'ma_75_deviation': merged_df.get('ma_75_deviation', None),
        'ma_75_trend': merged_df.get('ma_75_trend', ''),
        'ma_200_value': merged_df.get('ma_200_value', None),
        'ma_200_deviation': merged_df.get('ma_200_deviation', None),
        'ma_200_trend': merged_df.get('ma_200_trend', ''),

        # メタ情報
        'currency': merged_df.get('currency', 'JPY'),
        'exchange': merged_df.get('exchange', 'JPX'),
        'scrape_date': merged_df.get('scrape_date', datetime.now().strftime('%Y-%m-%d')),
    })
    
    # 統計情報
    total_rows = len(integrated)
    has_yfinance_data = integrated['company_name_en'].notna().sum()
    has_price_data = integrated['currentPrice'].notna().sum()
    
    print(f"   総企業数: {total_rows}社")
    print(f"   yfinanceデータあり: {has_yfinance_data}社 ({has_yfinance_data/total_rows*100:.1f}%)")
    print(f"   株価データあり: {has_price_data}社 ({has_price_data/total_rows*100:.1f}%)")
    
    return integrated


def save_integrated_data(df, output_dir, filename):
    """統合データをCSVに保存"""
    print(f"\n💾 データ保存中...")
    
    # 出力ディレクトリの作成
    os.makedirs(output_dir, exist_ok=True)
    
    # タイムスタンプ付きファイル名
    date_str = datetime.now().strftime('%Y%m%d')
    timestamped_filename = filename.replace('.csv', f'_{date_str}.csv')
    
    # ファイルパス
    output_path = os.path.join(output_dir, filename)
    timestamped_path = os.path.join(output_dir, timestamped_filename)
    
    # 保存（最新版）
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"   ✅ 保存完了: {output_path}")
    
    # 保存（タイムスタンプ版）
    df.to_csv(timestamped_path, index=False, encoding='utf-8-sig')
    print(f"   ✅ 保存完了: {timestamped_path}")
    
    # サンプルデータ表示
    print(f"\n📋 サンプルデータ（最初の3社）:")
    print(df[['code', 'company_name_ja', 'company_name_en', 'currentPrice', 'marketCap']].head(3).to_string(index=False))
    
    return output_path


def validate_data(df):
    """データ品質チェック"""
    print("\n🔍 データ品質チェック...")
    
    issues = []
    
    # チェック1: 企業名の存在
    missing_ja_name = df['company_name_ja'].isna().sum()
    if missing_ja_name > 0:
        issues.append(f"⚠️  日本語企業名が欠損: {missing_ja_name}社")
    
    missing_en_name = df['company_name_en'].isna().sum()
    if missing_en_name > 0:
        issues.append(f"⚠️  英語企業名が欠損: {missing_en_name}社")
    
    # チェック2: 株価データ
    missing_price = df['currentPrice'].isna().sum()
    if missing_price > 0:
        issues.append(f"⚠️  株価データが欠損: {missing_price}社")
    
    # チェック3: 時価総額
    missing_cap = df['marketCap'].isna().sum()
    if missing_cap > 0:
        issues.append(f"⚠️  時価総額が欠損: {missing_cap}社")
    
    # レポート
    if issues:
        print("\n   問題が見つかりました:")
        for issue in issues:
            print(f"   {issue}")
    else:
        print("   ✅ すべてのチェックに合格")
    
    return len(issues) == 0


def main():
    """メイン処理"""
    print("=" * 60)
    print("Japan IR - データ統合スクリプト（がっちゃんこ）")
    print("=" * 60)
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # ステップ1: Yahoo JPデータ読み込み
    yahoo_df = load_yahoo_jp_data(YAHOO_JP_CSV)
    
    # ステップ2: yfinanceデータ読み込み
    yfinance_df = load_yfinance_data(YFINANCE_CSV)
    
    # ステップ3: データ統合
    merged_df = merge_data(yahoo_df, yfinance_df)
    
    # ステップ4: 最終データ構造作成
    integrated_df = create_integrated_dataframe(merged_df)
    
    # ステップ5: データ品質チェック
    validate_data(integrated_df)
    
    # ステップ6: 保存
    output_path = save_integrated_data(integrated_df, OUTPUT_DIR, OUTPUT_FILENAME)
    
    # 完了
    print()
    print("=" * 60)
    print("✅ がっちゃんこ完了！")
    print("=" * 60)
    print(f"出力ファイル: {output_path}")
    print(f"総企業数: {len(integrated_df)}社")
    print()


if __name__ == "__main__":
    # コマンドライン引数のサポート
    if len(sys.argv) >= 3:
        YAHOO_JP_CSV = sys.argv[1]
        YFINANCE_CSV = sys.argv[2]
        print(f"カスタム入力ファイル:")
        print(f"  Yahoo JP: {YAHOO_JP_CSV}")
        print(f"  yfinance: {YFINANCE_CSV}")
        print()
    
    main()
