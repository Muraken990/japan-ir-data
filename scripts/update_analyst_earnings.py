#!/usr/bin/env python3
"""
Japan IR - アナリスト予想・決算日程 WordPress更新スクリプト
data/analyst_earnings/{code}.json をWordPressに保存:
  - analyst_earnings_data: JSON全体（カード表示用）
  - 個別フィールド: スクリーニング・表示用
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

INPUT_DIR = "data/analyst_earnings"
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


def extract_individual_fields(analyst_data):
    """JSONから個別フィールド用の値を抽出（既存フィールド名を使用）"""
    fields = {}

    # アナリスト推奨（既存フィールド名を使用）
    recommendations = analyst_data.get('analyst_recommendations', {})
    if recommendations.get('has_data'):
        # 既存フィールド
        if recommendations.get('recommendation_key'):
            fields['recommendationKey'] = recommendations['recommendation_key']
        if recommendations.get('recommendation_mean') is not None:
            fields['recommendationMean'] = recommendations['recommendation_mean']
        if recommendations.get('total_analysts') is not None:
            fields['numberOfAnalystOpinions'] = recommendations['total_analysts']

    # 目標株価（既存フィールド名を使用）
    target_prices = analyst_data.get('target_prices', {})
    if target_prices.get('has_data'):
        # 既存フィールド
        if target_prices.get('mean') is not None:
            fields['targetMeanPrice'] = target_prices['mean']

    # 注: 詳細データ（Strong Buy/Buy/Hold内訳、目標株価High/Low等）は
    #     analyst_earnings_data JSON に含まれるため、個別フィールドは不要

    return fields


def update_analyst_earnings(post_id, analyst_data, dry_run=False):
    """アナリスト・決算データをWordPressに更新"""
    if dry_run:
        return True

    headers = get_auth_headers()
    url = f"{WP_SITE_URL}/wp-json/wp/v2/company/{post_id}"

    # 個別フィールドを抽出
    individual_fields = extract_individual_fields(analyst_data)

    # メタデータ構築
    meta = {
        # JSON全体（カード表示用）
        'analyst_earnings_data': json.dumps(analyst_data, ensure_ascii=False),
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
    parser = argparse.ArgumentParser(description='Japan IR - アナリスト予想・決算日程 WordPress更新スクリプト')
    parser.add_argument('--limit', type=int, help='処理する企業数を制限')
    parser.add_argument('--dry-run', action='store_true', help='実際には更新せず表示のみ')
    parser.add_argument('--ticker', type=str, help='特定の銘柄のみ更新')
    args = parser.parse_args()

    print("=" * 70)
    print("Japan IR - アナリスト予想・決算日程 WordPress更新")
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
        print("   先に fetch_analyst_earnings.py を実行してください")
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
            # Dry Runの場合は取得データの概要を表示
            recommendations = data.get('analyst_recommendations', {})
            target_prices = data.get('target_prices', {})
            earnings_dates = data.get('earnings_dates', {})

            if recommendations.get('has_data'):
                rec_key = recommendations.get('recommendation_key', 'N/A')
                total_analysts = recommendations.get('total_analysts', 'N/A')
                print(f"   📋 アナリスト推奨: {rec_key} ({total_analysts}名)")

            if target_prices.get('has_data'):
                mean_price = target_prices.get('mean', 'N/A')
                print(f"   📋 目標株価: ¥{mean_price:,.0f}" if isinstance(mean_price, (int, float)) else f"   📋 目標株価: {mean_price}")

            if earnings_dates.get('has_data'):
                next_earnings = earnings_dates.get('next_earnings')
                if next_earnings:
                    print(f"   📋 次回決算: {next_earnings.get('date', 'N/A')}")

            if code in en_companies:
                print(f"   📋 英語版あり (ID: {en_companies[code]['id']})")

            success_count += 1
            continue

        # 日本語版を更新
        if update_analyst_earnings(ja_post_id, data):
            print(f"   ✅ 日本語版更新成功")

            # 英語版も更新
            if code in en_companies:
                en_post_id = en_companies[code]['id']
                if update_analyst_earnings(en_post_id, data):
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
