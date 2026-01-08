#!/usr/bin/env python3
"""
Japan IR - 財務データ WordPress更新スクリプト
data/financials/{code}.json をWordPressの yfinance_financials メタフィールドに保存
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
REQUEST_DELAY = 0.5
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


def get_existing_companies():
    """WordPressから既存の全企業を取得"""
    headers = get_auth_headers()
    existing_companies = {}
    offset = 0
    per_page = 100

    print("\n📥 WordPressから既存企業を取得中...")

    while True:
        params = {
            'per_page': per_page,
            'offset': offset,
            'context': 'edit'
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

            companies = response.json()

            if not companies or len(companies) == 0:
                break

            for company in companies:
                code = company.get('stock_code', '')
                if code:
                    clean_code = str(code).replace('.T', '')
                    existing_companies[clean_code] = {
                        'id': company['id'],
                        'title': company.get('title', {}).get('rendered', ''),
                        'slug': company.get('slug', clean_code)
                    }

            print(f"   取得済み: {len(existing_companies)}社（offset: {offset}）")

            if len(companies) < per_page:
                break

            offset += per_page

            if offset >= 5000:
                print(f"   ⚠️  安全装置: 5,000社で停止")
                break

        except Exception as e:
            print(f"   ❌ エラー: {str(e)}")
            break

    print(f"   ✅ 既存企業取得完了: {len(existing_companies)}社\n")
    return existing_companies


def get_translation_by_ticker(ticker, target_lang='en'):
    """証券コードから翻訳投稿を検索"""
    url = f"{WP_SITE_URL}/wp-json/wp/v2/company"
    params = {
        'lang': target_lang,
        'stock_code': ticker,
        'per_page': 100
    }

    try:
        response = requests.get(url, params=params, headers=get_auth_headers())
        if response.status_code != 200:
            return None

        companies = response.json()

        for company in companies:
            if company.get('stock_code') == ticker:
                return company['id']
    except:
        pass

    return None


def update_financials(post_id, financial_data, dry_run=False):
    """財務データをWordPressに更新"""
    if dry_run:
        return True

    headers = get_auth_headers()
    url = f"{WP_SITE_URL}/wp-json/wp/v2/company/{post_id}"

    # 財務データをJSON文字列として保存
    data = {
        'meta': {
            'yfinance_financials': json.dumps(financial_data, ensure_ascii=False)
        }
    }

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

    # 既存企業取得
    existing_companies = get_existing_companies()

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

        # WordPress登録済みか確認
        if code not in existing_companies:
            print(f"   ⏭️  スキップ（WordPress未登録）")
            skipped_count += 1
            continue

        company_info = existing_companies[code]
        post_id = company_info['id']

        print(f"   ID: {post_id} - {company_info.get('title', code)}")

        if args.dry_run:
            print(f"   📋 財務データ年数: {len(data.get('financials', {}).get('years', []))}年分")
            success_count += 1
            continue

        # 日本語版を更新
        if update_financials(post_id, data):
            print(f"   ✅ 日本語版更新成功")

            # 英語版も更新
            en_post_id = get_translation_by_ticker(code, 'en')
            if en_post_id:
                if update_financials(en_post_id, data):
                    print(f"   ✅ 英語版更新成功 (ID: {en_post_id})")
                else:
                    print(f"   ⚠️  英語版更新失敗 (ID: {en_post_id})")

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
    print("=" * 70)


if __name__ == "__main__":
    main()
