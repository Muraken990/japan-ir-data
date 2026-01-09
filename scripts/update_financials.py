#!/usr/bin/env python3
"""
Japan IR - 財務データ WordPress更新スクリプト
data/financials/{code}.json をWordPressに保存:
  - detailed_financial_data: JSON全体（チャート・テーブル表示用）
  - 個別フィールド: 最新年の値を抽出（スクリーニング用）
"""

import json
import os
import sys
import time
import base64
import argparse
import requests
from datetime import datetime
from pathlib import Path

# 設定
WP_SITE_URL = os.getenv('WP_SITE_URL', 'https://japanir.jp')
WP_USER = os.getenv('WP_USER')
WP_PASSWORD = os.getenv('WP_PASSWORD')

INPUT_DIR = "data/financials"
REQUEST_DELAY = 0.3
PROGRESS_INTERVAL = 10


def get_auth_headers():
    """WordPress REST API認証ヘッダー"""
    if not WP_USER or not WP_PASSWORD:
        raise ValueError("❌ エラー: WP_USER と WP_PASSWORD 環境変数を設定してください")

    credentials = f"{WP_USER}:{WP_PASSWORD}"
    token = base64.b64encode(credentials.encode()).decode('utf-8')
    return {
        'Authorization': f'Basic {token}',
        'Content-Type': 'application/json'
    }


def get_all_companies(lang='ja'):
    """WordPressから指定言語の全企業を取得"""
    headers = get_auth_headers()
    companies = {}
    offset = 0
    per_page = 100

    lang_name = "日本語" if lang == 'ja' else "英語"
    print(f"\n📥 WordPress {lang_name}版企業を取得中...")

    while True:
        params = {
            'per_page': per_page,
            'offset': offset,
            'context': 'edit',
            'lang': lang
        }

        try:
            response = requests.get(
                f"{WP_SITE_URL}/wp-json/wp/v2/company",
                params=params,
                headers=headers,
                timeout=30
            )

            if response.status_code != 200:
                print(f"   ⚠️  REST API エラー: ステータスコード {response.status_code}")
                break

            result = response.json()

            if not result or len(result) == 0:
                break

            for company in result:
                code = company.get('stock_code', '')
                if code:
                    clean_code = str(code).replace('.T', '')
                    companies[clean_code] = {
                        'id': company['id'],
                        'title': company.get('title', {}).get('rendered', ''),
                        'slug': company.get('slug', clean_code)
                    }

            print(f"   取得済み: {len(companies)}社（offset: {offset}）")

            if len(result) < per_page:
                break

            offset += per_page

            if offset >= 5000:
                print(f"   ⚠️  安全装置: 5,000社で停止")
                break

        except Exception as e:
            print(f"   ❌ エラー: {str(e)}")
            break

    print(f"   ✅ {lang_name}版企業取得完了: {len(companies)}社")
    return companies


def extract_individual_fields(financial_data):
    """JSONから個別フィールド用の値を抽出"""
    fields = {}

    # 財務データから最新年を取得
    financials = financial_data.get('financials', {})
    years_data = financials.get('years', [])

    if years_data:
        # 最新年のデータ（リストの先頭）
        latest = years_data[0]

        # 財務指標フィールド
        if latest.get('revenue') is not None:
            fields['totalRevenue'] = latest['revenue']
        if latest.get('operating_income') is not None:
            fields['OperatingIncome'] = latest['operating_income']
        if latest.get('net_income') is not None:
            fields['NetIncome'] = latest['net_income']
        if latest.get('operating_margin') is not None:
            fields['operatingMargins'] = latest['operating_margin']
        if latest.get('net_margin') is not None:
            fields['profitMargins'] = latest['net_margin']
        if latest.get('roe') is not None:
            fields['returnOnEquity'] = latest['roe']
        if latest.get('eps') is not None:
            fields['epsTrailingTwelveMonths'] = latest['eps']

    # 配当履歴
    dividends = financial_data.get('dividends', {})
    dividend_history = dividends.get('history', [])

    # 配当履歴を年度順にソート（新しい順）してフィールドに割り当て
    if dividend_history:
        sorted_dividends = sorted(dividend_history, key=lambda x: x.get('year', 0), reverse=True)
        for i, div in enumerate(sorted_dividends[:10], 1):
            field_name = f'get_dividend_history_year{i}'
            if div.get('amount') is not None:
                fields[field_name] = div['amount']

    # Company Information
    company_info = financial_data.get('company_info', {})
    if company_info:
        if company_info.get('name_en'):
            fields['company_name_en'] = company_info['name_en']
        if company_info.get('long_name'):
            fields['longName'] = company_info['long_name']
        if company_info.get('sector'):
            fields['sector'] = company_info['sector']
        if company_info.get('industry'):
            fields['industry'] = company_info['industry']
        if company_info.get('website'):
            fields['website'] = company_info['website']
        if company_info.get('employees') is not None:
            fields['fullTimeEmployees'] = company_info['employees']
        if company_info.get('country'):
            fields['country'] = company_info['country']
        if company_info.get('city'):
            fields['city'] = company_info['city']
        if company_info.get('address'):
            fields['address1'] = company_info['address']
        if company_info.get('description'):
            fields['longBusinessSummary'] = company_info['description']

    return fields


def update_financials(post_id, financial_data, dry_run=False):
    """財務データをWordPressに更新"""
    if dry_run:
        return True

    headers = get_auth_headers()
    url = f"{WP_SITE_URL}/wp-json/wp/v2/company/{post_id}"

    # 個別フィールドを抽出
    individual_fields = extract_individual_fields(financial_data)

    # メタデータ構築
    meta = {
        # JSON全体（チャート・テーブル表示用）
        'detailed_financial_data': json.dumps(financial_data, ensure_ascii=False),
    }

    # 個別フィールドを追加（スクリーニング用）
    meta.update(individual_fields)

    data = {'meta': meta}

    try:
        response = requests.post(url, headers=headers, json=data, timeout=30)
        return response.status_code == 200
    except Exception as e:
        print(f"      API エラー: {str(e)}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Japan IR - 財務データ WordPress更新スクリプト')
    parser.add_argument('--limit', type=int, help='処理する企業数を制限')
    parser.add_argument('--dry-run', action='store_true', help='実際には更新せず表示のみ')
    parser.add_argument('--ticker', type=str, help='特定の銘柄のみ更新')
    args = parser.parse_args()

    print("=" * 70)
    print("Japan IR - 財務データ WordPress更新")
    if args.dry_run:
        print("   🔍 Dry Run モード（実際には更新しません）")
    print("=" * 70)
    start_time = datetime.now()
    print(f"開始: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # 認証チェック
    if not WP_USER or not WP_PASSWORD:
        print("❌ エラー: WP_USER と WP_PASSWORD 環境変数を設定してください")
        sys.exit(1)

    # 入力ディレクトリ確認
    if not os.path.exists(INPUT_DIR):
        print(f"❌ エラー: 入力ディレクトリが見つかりません: {INPUT_DIR}")
        print("   先に fetch_financials.py を実行してください")
        sys.exit(1)

    # JSONファイル一覧取得
    json_files = list(Path(INPUT_DIR).glob("*.json"))

    if not json_files:
        print(f"❌ エラー: JSONファイルが見つかりません: {INPUT_DIR}")
        sys.exit(1)

    # 特定銘柄のみ更新
    if args.ticker:
        json_files = [f for f in json_files if f.stem == args.ticker]
        if not json_files:
            print(f"❌ エラー: {args.ticker}.json が見つかりません")
            sys.exit(1)

    # 制限
    if args.limit:
        json_files = json_files[:args.limit]

    total = len(json_files)
    print(f"対象ファイル数: {total}")

    # 日本語版・英語版の全企業を取得
    ja_companies = get_all_companies('ja')
    en_companies = get_all_companies('en')

    success_count = 0
    skipped_count = 0
    error_count = 0

    print("\n" + "=" * 70)
    if args.dry_run:
        print("🔍 処理内容プレビュー")
    else:
        print("🚀 WordPress更新開始")
    print("=" * 70)

    for i, json_file in enumerate(json_files, 1):
        code = json_file.stem
        print(f"\n[{i}/{total}] {code}")

        # JSONファイル読み込み
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"   ❌ JSON読み込みエラー: {str(e)}")
            error_count += 1
            continue

        # 取得成功データのみ処理
        if not data.get("success"):
            print(f"   ⏭️  スキップ（取得エラーデータ）")
            skipped_count += 1
            continue

        # WordPress登録済みか確認（日本語版）
        if code not in ja_companies:
            print(f"   ⏭️  スキップ（WordPress未登録）")
            skipped_count += 1
            continue

        ja_info = ja_companies[code]
        ja_post_id = ja_info['id']

        print(f"   ID: {ja_post_id} - {ja_info.get('title', code)}")

        if args.dry_run:
            print(f"   📋 財務データ年数: {len(data.get('financials', {}).get('years', []))}年分")
            if code in en_companies:
                print(f"   📋 英語版あり (ID: {en_companies[code]['id']})")
            success_count += 1
            continue

        # 日本語版を更新
        if update_financials(ja_post_id, data):
            print(f"   ✅ 日本語版更新成功")

            # 英語版も更新
            if code in en_companies:
                en_post_id = en_companies[code]['id']
                if update_financials(en_post_id, data):
                    print(f"   ✅ 英語版更新成功 (ID: {en_post_id})")
                else:
                    print(f"   ⚠️  英語版更新失敗 (ID: {en_post_id})")
            else:
                print(f"   ⚠️  英語版なし")

            success_count += 1
        else:
            print(f"   ❌ 更新失敗")
            error_count += 1

        # 進捗表示
        if i % PROGRESS_INTERVAL == 0 or i == total:
            print()
            print(f"進捗: {i}/{total} | 成功: {success_count} | スキップ: {skipped_count} | 失敗: {error_count}")

        # レート制限対策
        if i < total and not args.dry_run:
            time.sleep(REQUEST_DELAY)

    # 完了サマリー
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()

    print()
    print("=" * 70)
    if args.dry_run:
        print("✅ Dry Run 完了（実際には更新していません）")
    else:
        print("✅ 処理完了")
    print("=" * 70)
    print(f"所要時間: {elapsed:.1f}秒")
    print(f"成功: {success_count}社")
    print(f"スキップ: {skipped_count}社")
    print(f"失敗: {error_count}社")
    print(f"日本語版企業数: {len(ja_companies)}社")
    print(f"英語版企業数: {len(en_companies)}社")
    print("=" * 70)


if __name__ == "__main__":
    main()
