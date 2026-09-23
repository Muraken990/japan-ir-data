#!/usr/bin/env python3
"""
WordPress企業データ スマート更新スクリプト
条件分岐 + 段階的実行対応 + Dry Run機能 + Update Only機能

条件1: Yahoo○ + yfinance○ + WordPress× → 新規作成
条件2: Yahoo○ + yfinance○ + WordPress○ → 更新
条件3: Yahoo○ + yfinance× + WordPress× → スルー
条件4: Yahoo○ + yfinance× + WordPress○ → スキップ（手動確認推奨。yfinanceエラーだけでは
       上場廃止と確定できないため、ここでは自動下書き化しない）

下書き化（draft化）は --auto-unpublish 専用経路でのみ行う。根拠は公式確認済み上場廃止リスト
（--confirmed-delisted-csv）のみで、yfinance_errorsへのフォールバックは行わない。
通常の新規作成・更新処理（process_companies）とは完全に独立した処理経路になっている。
"""

import pandas as pd
import requests
import base64
import time
import os
import re
import sys
import argparse
import smtplib
from collections import defaultdict
from email.mime.text import MIMEText
from datetime import datetime, timezone, timedelta

# ============================================================
# 設定
# ============================================================

WP_SITE_URL = os.getenv('WP_SITE_URL', 'https://japanir.jp')
WP_URL = WP_SITE_URL
WP_USER = os.getenv('WP_USER')
WP_PASSWORD = os.getenv('WP_PASSWORD')

# セキュリティチェック
if not WP_USER or not WP_PASSWORD:
    raise ValueError("❌ エラー: WP_USER と WP_PASSWORD 環境変数を設定してください")

# デフォルトファイル
DEFAULT_CSV = 'data/integrated_company_data.csv'
DEFAULT_ERRORS_CSV = 'output/yfinance_errors_latest.csv'
# output/配下はgitignore対象でGitHub Actionsから参照できないため、確認済み上場廃止
# リストはdata/配下の固定パスに置く（通常更新・--auto-unpublishの両方で共有する）
DEFAULT_CONFIRMED_DELISTED_CSV = 'data/confirmed_delisted_companies.csv'

# 処理速度（秒）
REQUEST_DELAY = float(os.getenv('REQUEST_DELAY', '0.5'))

# WordPress REST API接続設定
WP_REQUEST_TIMEOUT = int(os.getenv('WP_REQUEST_TIMEOUT', '60'))
WP_MAX_RETRIES = int(os.getenv('WP_MAX_RETRIES', '5'))
WP_RETRY_BACKOFF = float(os.getenv('WP_RETRY_BACKOFF', '2'))
WP_RETRY_DELAYS = [
    float(value.strip())
    for value in os.getenv('WP_RETRY_DELAYS', '60,180,300,600').split(',')
    if value.strip()
]
WP_RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

# --auto-unpublish 専用: 公式確認済み上場廃止リストの必須列
REQUIRED_DELISTED_COLUMNS = ["code", "delisting_date", "market", "official_url", "confirmed_date"]
_DELISTED_CODE_PATTERN = re.compile(r"^[0-9A-Z]{4}$")
_DATE_FORMAT = "%Y-%m-%d"
JST = timezone(timedelta(hours=9))

# --auto-unpublish 専用: WPMLで運用している言語（CLAUDE.md準拠、日本語/英語のみ）。
# 上場廃止企業は、ここに列挙した全言語の投稿がdraftになるまで対応完了にしない。
WPML_LANGUAGES = ['ja', 'en']

# ============================================================
# WordPress認証
# ============================================================

def get_auth_headers():
    """WordPress REST API認証ヘッダー"""
    credentials = f"{WP_USER}:{WP_PASSWORD}"
    token = base64.b64encode(credentials.encode()).decode('utf-8')
    return {
        'Authorization': f'Basic {token}',
        'Content-Type': 'application/json'
    }


def wordpress_request(method, url, **kwargs):
    """WordPress REST APIリクエスト（リトライ付き）"""
    timeout = kwargs.pop('timeout', WP_REQUEST_TIMEOUT)
    method = method.upper()

    for attempt in range(1, WP_MAX_RETRIES + 1):
        try:
            response = requests.request(method, url, timeout=timeout, **kwargs)

            if response.status_code in WP_RETRY_STATUS_CODES and attempt < WP_MAX_RETRIES:
                wait_seconds = get_retry_delay(attempt)
                print(
                    f"   ⚠️  WordPress API {response.status_code} "
                    f"({method} {url}) → {wait_seconds:.1f}秒後にリトライ "
                    f"({attempt}/{WP_MAX_RETRIES})"
                )
                time.sleep(wait_seconds)
                continue

            return response

        except (
            requests.exceptions.ConnectTimeout,
            requests.exceptions.ReadTimeout,
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ) as e:
            if attempt >= WP_MAX_RETRIES:
                print(
                    f"   ❌ WordPress API接続失敗 "
                    f"({method} {url}, {attempt}/{WP_MAX_RETRIES}): {e}"
                )
                raise

            wait_seconds = get_retry_delay(attempt)
            print(
                f"   ⏱️  WordPress API接続エラー "
                f"({method} {url}) → {wait_seconds:.1f}秒後にリトライ "
                f"({attempt}/{WP_MAX_RETRIES}): {e}"
            )
            time.sleep(wait_seconds)


def get_retry_delay(attempt):
    """失敗回数に応じた待機秒数を返す。"""
    delay_index = attempt - 1
    if delay_index < len(WP_RETRY_DELAYS):
        return WP_RETRY_DELAYS[delay_index]
    return WP_RETRY_BACKOFF ** delay_index


def preflight_wordpress_api(wp_url):
    """更新開始前にWordPress REST APIの認証と応答を確認する。"""
    response = wordpress_request(
        'GET',
        f"{wp_url}/wp-json/wp/v2/company",
        params={
            'per_page': 1,
            'context': 'edit',
            '_fields': 'id'
        },
        headers=get_auth_headers()
    )
    response.raise_for_status()
    print(f"✅ WordPress API疎通確認成功: HTTP {response.status_code}")

# ============================================================
# WordPress企業取得
#
# WP REST APIの一覧（コレクション）エンドポイントは、context=editかつstatus
# フィルタを明示しても、レスポンスの各要素に'status'フィールドを含めない
# （本番REST APIで確認済み）。そのため全ステータスを一括取得することはできず、
# ステータス値ごとに個別クエリを発行し、「どのクエリでpost_idが返ってきたか」
# でstatusを判定する（fetch_company_posts_by_status）。
#
# また、statusパラメータを渡さない場合はpublishのみがデフォルトで返り、
# langパラメータを渡さない場合はサイトのデフォルト言語（日本語）のみが返る。
# この2つの暗黙のデフォルトが、上場廃止企業（draft）や英語投稿を「存在しない」
# と誤判定させ、重複投稿の自動作成を引き起こした（2026-09-14に本番で発覚）。
#
# get_existing_companies_all_langs()は、通常の新規作成・更新判定
# （process_companies）と--auto-unpublishの両方が共有する唯一の取得元。
# 経路ごとに別々の取得ロジックを持つと今回のような実装乖離が再発するため、
# 意図的に一本化している。
# ============================================================

ALL_STATUSES = ['publish', 'draft', 'pending', 'future', 'private', 'trash']


def fetch_company_posts_by_status(wp_url, lang, statuses=ALL_STATUSES):
    """指定言語の企業投稿を、ステータス値ごとに個別クエリを発行して取得する。

    同一コード・同一言語内に複数投稿があっても1件に上書きせず、全件をリストで返す。
    取得中にAPIエラー（非200レスポンスや例外、ページネーション不完全）が発生した場合、
    不完全なデータで「投稿なし」と誤判定するのを避けるため、即座にexit(1)する
    （fail-closed）。

    戻り値: [{'id', 'slug', 'stock_code', 'status', ...}, ...]（重複排除なし）
    """
    headers = get_auth_headers()
    posts = []

    for status in statuses:
        offset = 0
        per_page = 100
        status_count = 0
        while True:
            params = {
                'per_page': per_page,
                'offset': offset,
                'context': 'edit',
                'status': status,
                'lang': lang,
            }
            try:
                response = wordpress_request(
                    'GET', f"{wp_url}/wp-json/wp/v2/company",
                    params=params, headers=headers
                )
            except Exception as e:
                print(f"❌ WordPress API取得中に例外が発生しました"
                      f"（status={status}, lang={lang}, offset={offset}）: {e}")
                print("   不完全なデータで新規作成・下書き化の判定を行うのは危険なため中止します。")
                sys.exit(1)

            if response.status_code != 200:
                print(f"❌ WordPress APIエラー（status={status}, lang={lang}, offset={offset}）: "
                      f"HTTP {response.status_code}")
                print("   不完全なデータで新規作成・下書き化の判定を行うのは危険なため中止します。")
                sys.exit(1)

            batch = response.json()
            if not batch:
                break

            for c in batch:
                # レスポンスに'status'フィールドが含まれないため、クエリ元のstatusを明示的に付与する
                c['status'] = status
                posts.append(c)
            status_count += len(batch)

            if len(batch) < per_page:
                break
            offset += per_page
            if offset >= 5000:
                print(f"   ⚠️  安全装置: 5,000件で停止（status={status}, lang={lang}）")
                break

        print(f"   ✅ status={status}, lang={lang}: {status_count}件取得完了")

    return posts


def get_existing_companies_all_langs(wp_url, languages=None):
    """全言語・全ステータスの企業投稿を取得し、証券コード×言語単位のグループにまとめる。

    戻り値: {code: {lang: [{'id','slug','status'}, ...], ...}}
    同一コード・同一言語に複数投稿があっても1件に上書きせず、全件をリストで保持する
    （重複投稿を見落とさないため）。
    """
    if languages is None:
        languages = WPML_LANGUAGES

    groups = {}
    for lang in languages:
        print(f"\n📥 WordPressから既存企業を取得中（lang={lang}、全ステータス）...")
        for c in fetch_company_posts_by_status(wp_url, lang):
            code = c.get('stock_code', '')
            if not code:
                continue
            clean_code = str(code).replace('.T', '')
            entry = {'id': c['id'], 'slug': c.get('slug', clean_code), 'status': c.get('status', '')}
            groups.setdefault(clean_code, {}).setdefault(lang, []).append(entry)

    total = sum(len(entries) for langs in groups.values() for entries in langs.values())
    print(f"\n   ✅ 既存企業取得完了: {len(groups)}コード、延べ{total}投稿\n")
    return groups

def get_translation_by_ticker(ticker, target_lang='en'):
    """証券コードから翻訳投稿を検索"""
    url = f"{WP_SITE_URL}/wp-json/wp/v2/company"
    params = {
        'lang': target_lang,
        'stock_code': ticker,
        'per_page': 100
    }
    
    try:
        response = wordpress_request('GET', url, params=params, headers=get_auth_headers())
        if response.status_code != 200:
            return None
            
        companies = response.json()
        
        # stock_codeが完全一致するものを探す
        for company in companies:
            if company.get('stock_code') == ticker:
                return company['id']
    except:
        return None
    
    return None

# ============================================================
# WordPress企業作成
# ============================================================

def create_company(company_data, status='publish', dry_run=False):
    """新規企業ページ作成"""
    code = company_data.get('code', '')
    
    # Dry Run表示
    if dry_run:
        company_name_ja = company_data.get('company_name_ja', '')
        company_name_en = company_data.get('company_name_en', '')
        stock_price = company_data.get('currentPrice', 0)
        market_cap = company_data.get('marketCap', 0)
        
        if pd.notna(market_cap) and market_cap > 0:
            market_cap_million = int(market_cap / 1000000)
        else:
            market_cap_million = 0
        
        print(f"   スラッグ: company-{code}")
        print(f"   URL: {WP_SITE_URL}/company/company-{code}/")
        print(f"   企業名（日）: {company_name_ja}")
        print(f"   企業名（英）: {company_name_en}")
        print(f"   株価: {stock_price:,.0f}円" if pd.notna(stock_price) else "   株価: データなし")
        print(f"   時価総額: {market_cap_million:,}百万円")
        print(f"   ステータス: {status}")
        return True
    
    # 実際の作成処理
    headers = get_auth_headers()
    url = f"{WP_SITE_URL}/wp-json/wp/v2/company"
    
    # 時価総額（百万円単位に変換）
    market_cap = company_data.get('marketCap', 0)
    if pd.notna(market_cap) and market_cap > 0:
        market_cap_million = int(market_cap / 1000000)
    else:
        market_cap_million = 0
    
    # 株価
    stock_price = company_data.get('currentPrice', 0)
    if pd.isna(stock_price):
        stock_price = 0
    else:
        stock_price = float(stock_price)
    
    # 企業名
    company_name_ja = company_data.get('company_name_ja', '')
    company_name_en = company_data.get('company_name_en', '')
    
    # 日付
    date = company_data.get('scrape_date', datetime.now().strftime('%Y-%m-%d'))
    
    # セクター・業種
    sector = company_data.get('sector', '')
    industry = company_data.get('industry', '')

    # 財務指標
    trailing_pe = company_data.get('trailingPE', 0)
    price_to_book = company_data.get('priceToBook', 0)
    dividend_yield = company_data.get('dividendYield', 0)

    # 追加財務指標
    forward_pe = company_data.get('forwardPE', 0)
    return_on_equity = company_data.get('returnOnEquity', 0)
    return_on_assets = company_data.get('returnOnAssets', 0)
    profit_margins = company_data.get('profitMargins', 0)
    revenue_growth = company_data.get('revenueGrowth', 0)
    previous_close = company_data.get('previousClose', 0)
    open_price = company_data.get('open', 0)
    day_high = company_data.get('dayHigh', 0)
    day_low = company_data.get('dayLow', 0)
    volume = company_data.get('volume', 0)
    average_volume = company_data.get('averageVolume', 0)
    fifty_two_week_high = company_data.get('fiftyTwoWeekHigh', 0)
    fifty_two_week_low = company_data.get('fiftyTwoWeekLow', 0)

    # 会社基本情報
    website = company_data.get('website', '')
    city = company_data.get('city', '')
    full_time_employees = company_data.get('fullTimeEmployees', 0)

    # Price Trend (MA乖離率)
    ma_5_value = company_data.get('ma_5_value', 0)
    ma_5_deviation = company_data.get('ma_5_deviation', 0)
    ma_5_trend = company_data.get('ma_5_trend', 'neutral')
    ma_25_value = company_data.get('ma_25_value', 0)
    ma_25_deviation = company_data.get('ma_25_deviation', 0)
    ma_25_trend = company_data.get('ma_25_trend', 'neutral')
    ma_75_value = company_data.get('ma_75_value', 0)
    ma_75_deviation = company_data.get('ma_75_deviation', 0)
    ma_75_trend = company_data.get('ma_75_trend', 'neutral')
    ma_200_value = company_data.get('ma_200_value', 0)
    ma_200_deviation = company_data.get('ma_200_deviation', 0)
    ma_200_trend = company_data.get('ma_200_trend', 'neutral')

    # NaN対策
    if pd.isna(sector):
        sector = ''
    if pd.isna(industry):
        industry = ''
    if pd.isna(company_name_ja):
        company_name_ja = ''
    if pd.isna(company_name_en):
        company_name_en = ''
    if pd.isna(trailing_pe):
        trailing_pe = 0
    else:
        trailing_pe = float(trailing_pe)
    if pd.isna(price_to_book):
        price_to_book = 0
    else:
        price_to_book = float(price_to_book)
    if pd.isna(dividend_yield):
        dividend_yield = 0
    else:
        dividend_yield = float(dividend_yield)

    # 追加項目のNaN対策
    if pd.isna(forward_pe):
        forward_pe = 0
    else:
        forward_pe = float(forward_pe)
    if pd.isna(return_on_equity):
        return_on_equity = 0
    else:
        return_on_equity = float(return_on_equity)
    if pd.isna(return_on_assets):
        return_on_assets = 0
    else:
        return_on_assets = float(return_on_assets)
    if pd.isna(profit_margins):
        profit_margins = 0
    else:
        profit_margins = float(profit_margins)
    if pd.isna(revenue_growth):
        revenue_growth = 0
    else:
        revenue_growth = float(revenue_growth)
    if pd.isna(previous_close):
        previous_close = 0
    else:
        previous_close = float(previous_close)
    if pd.isna(open_price):
        open_price = 0
    else:
        open_price = float(open_price)
    if pd.isna(day_high):
        day_high = 0
    else:
        day_high = float(day_high)
    if pd.isna(day_low):
        day_low = 0
    else:
        day_low = float(day_low)
    if pd.isna(volume):
        volume = 0
    else:
        volume = int(volume)
    if pd.isna(average_volume):
        average_volume = 0
    else:
        average_volume = int(average_volume)
    if pd.isna(fifty_two_week_high):
        fifty_two_week_high = 0
    else:
        fifty_two_week_high = float(fifty_two_week_high)
    if pd.isna(fifty_two_week_low):
        fifty_two_week_low = 0
    else:
        fifty_two_week_low = float(fifty_two_week_low)
    if pd.isna(website):
        website = ''
    if pd.isna(city):
        city = ''
    if pd.isna(full_time_employees):
        full_time_employees = 0
    else:
        full_time_employees = int(full_time_employees)

    # Price TrendのNaN対策
    if pd.isna(ma_5_value):
        ma_5_value = 0
    else:
        ma_5_value = float(ma_5_value)
    if pd.isna(ma_5_deviation):
        ma_5_deviation = 0
    else:
        ma_5_deviation = float(ma_5_deviation)
    if pd.isna(ma_5_trend):
        ma_5_trend = 'neutral'
    if pd.isna(ma_25_value):
        ma_25_value = 0
    else:
        ma_25_value = float(ma_25_value)
    if pd.isna(ma_25_deviation):
        ma_25_deviation = 0
    else:
        ma_25_deviation = float(ma_25_deviation)
    if pd.isna(ma_25_trend):
        ma_25_trend = 'neutral'
    if pd.isna(ma_75_value):
        ma_75_value = 0
    else:
        ma_75_value = float(ma_75_value)
    if pd.isna(ma_75_deviation):
        ma_75_deviation = 0
    else:
        ma_75_deviation = float(ma_75_deviation)
    if pd.isna(ma_75_trend):
        ma_75_trend = 'neutral'
    if pd.isna(ma_200_value):
        ma_200_value = 0
    else:
        ma_200_value = float(ma_200_value)
    if pd.isna(ma_200_deviation):
        ma_200_deviation = 0
    else:
        ma_200_deviation = float(ma_200_deviation)
    if pd.isna(ma_200_trend):
        ma_200_trend = 'neutral'

    # 投稿データ
    data = {
        'title': str(company_name_ja),
        'slug': f'company-{code}',
        'status': status,
        'meta': {
            'Ticker': str(code),
            'marketCap': market_cap_million,
            'regularMarketPrice': stock_price,
            'DATE': str(date),
            'company_name_ja': str(company_name_ja),
            'longName': str(company_name_en),
            'sector': str(sector),
            'industry': str(industry),
            'trailingPE': trailing_pe,
            'priceToBook': price_to_book,
            'dividendYield': dividend_yield,
            # 追加項目
            'forwardPE': forward_pe,
            'returnOnEquity': return_on_equity,
            'returnOnAssets': return_on_assets,
            'profitMargins': profit_margins,
            'revenueGrowth': revenue_growth,
            'previousClose': previous_close,
            'open': open_price,
            'dayHigh': day_high,
            'dayLow': day_low,
            'volume': volume,
            'averageVolume': average_volume,
            'fiftyTwoWeekHigh': fifty_two_week_high,
            'fiftyTwoWeekLow': fifty_two_week_low,
            'website': str(website),
            'city': str(city),
            'fullTimeEmployees': full_time_employees,
            # Price Trend (MA乖離率)
            'ma_5_value': ma_5_value,
            'ma_5_deviation': ma_5_deviation,
            'ma_5_trend': str(ma_5_trend),
            'ma_25_value': ma_25_value,
            'ma_25_deviation': ma_25_deviation,
            'ma_25_trend': str(ma_25_trend),
            'ma_75_value': ma_75_value,
            'ma_75_deviation': ma_75_deviation,
            'ma_75_trend': str(ma_75_trend),
            'ma_200_value': ma_200_value,
            'ma_200_deviation': ma_200_deviation,
            'ma_200_trend': str(ma_200_trend),
        }
    }

    try:
        response = wordpress_request('POST', url, headers=headers, json=data)
        if response.status_code == 201:
            result = response.json()
            ja_post_id = result.get('id')

            en_post_id = create_english_post(ja_post_id, company_data, status, dry_run=False)
            if en_post_id:
                link_wpml_via_official_api(ja_post_id, en_post_id)

            return True
        else:
            return False
    except Exception as e:
        return False


def create_english_post(ja_post_id, company_data, status='publish', dry_run=False):
    """英語版企業ページ作成（日本語版作成の直後に呼び出す）"""
    code = company_data.get('code', '')

    if dry_run:
        return True

    headers = get_auth_headers()
    url = f"{WP_SITE_URL}/wp-json/wp/v2/company?lang=en"

    # 時価総額変換
    market_cap = company_data.get('marketCap', 0)
    if pd.notna(market_cap) and market_cap > 0:
        market_cap_million = int(market_cap / 1000000)
    else:
        market_cap_million = 0

    # 株価
    stock_price = company_data.get('currentPrice', 0)
    if pd.isna(stock_price):
        stock_price = 0
    else:
        stock_price = float(stock_price)

    # 企業名（英語）
    company_name_en = company_data.get('short_name_en', '')
    if pd.isna(company_name_en) or company_name_en == '':
        company_name_en = company_data.get('company_name_en', '')
    if pd.isna(company_name_en):
        company_name_en = ''

    # 日付
    date = company_data.get('scrape_date', datetime.now().strftime('%Y-%m-%d'))

    # セクター・業種
    sector = company_data.get('sector', '')
    industry = company_data.get('industry', '')
    if pd.isna(sector):
        sector = ''
    if pd.isna(industry):
        industry = ''

    data = {
        'title': str(company_name_en) if company_name_en else f"Company {code}",
        'slug': f'company-{code}',
        'status': status,
        'meta': {
            'Ticker': str(code),
            'marketCap': market_cap_million,
            'regularMarketPrice': stock_price,
            'DATE': str(date),
            'longName': str(company_name_en),
            'sector': str(sector),
            'industry': str(industry),
        }
    }

    try:
        response = wordpress_request('POST', url, headers=headers, json=data)
        if response.status_code == 201:
            result = response.json()
            en_post_id = result.get('id')
            print(f"   🌐 英語版作成成功 (ID: {en_post_id})")
            return en_post_id
        else:
            print(f"   ❌ 英語版作成失敗: {response.status_code}")
            print(f"   レスポンス: {response.text[:200]}")
            return None
    except Exception as e:
        print(f"   ❌ 英語版作成例外: {str(e)}")
        return None


def link_wpml_via_official_api(ja_post_id, en_post_id):
    """WPML正式APIで翻訳リンクを設定"""
    try:
        url = f"{WP_SITE_URL}/wp-json/custom/v1/wpml-link"
        payload = {
            "ja_post_id": ja_post_id,
            "en_post_id": en_post_id,
            "post_type": "company",
        }

        response = wordpress_request('POST', url, headers=get_auth_headers(), json=payload)

        if response.status_code == 200:
            result = response.json()
            if result.get("success"):
                print(f"   ✅ WPML リンク成功")
                return True
            else:
                print(f"   ❌ WPML リンク失敗: {result.get('message', '不明')}")
                return False
        else:
            print(f"   ❌ WPML API エラー: {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ WPML リンク例外: {str(e)}")
        return False


# ============================================================
# WordPress企業更新
# ============================================================

def update_single_post(post_id, company_data, lang='ja', dry_run=False):
    """単一投稿を更新（言語指定可能）"""
    headers = get_auth_headers()
    url = f"{WP_SITE_URL}/wp-json/wp/v2/company/{post_id}"

    # 時価総額（百万円単位に変換）
    market_cap = company_data.get('marketCap', 0)
    if pd.notna(market_cap) and market_cap > 0:
        market_cap_million = int(market_cap / 1000000)
    else:
        market_cap_million = 0

    # 株価
    stock_price = company_data.get('currentPrice', 0)
    if pd.isna(stock_price):
        stock_price = 0
    else:
        stock_price = float(stock_price)

    # 企業名
    company_name_ja = company_data.get('company_name_ja', '')
    company_name_en = company_data.get('company_name_en', '')

    # 日付
    date = company_data.get('scrape_date', datetime.now().strftime('%Y-%m-%d'))

    # セクター・業種
    sector = company_data.get('sector', '')
    industry = company_data.get('industry', '')

    # 財務指標
    trailing_pe = company_data.get('trailingPE', 0)
    price_to_book = company_data.get('priceToBook', 0)
    dividend_yield = company_data.get('dividendYield', 0)

    # 追加財務指標
    forward_pe = company_data.get('forwardPE', 0)
    return_on_equity = company_data.get('returnOnEquity', 0)
    return_on_assets = company_data.get('returnOnAssets', 0)
    profit_margins = company_data.get('profitMargins', 0)
    revenue_growth = company_data.get('revenueGrowth', 0)
    previous_close = company_data.get('previousClose', 0)
    open_price = company_data.get('open', 0)
    day_high = company_data.get('dayHigh', 0)
    day_low = company_data.get('dayLow', 0)
    volume = company_data.get('volume', 0)
    average_volume = company_data.get('averageVolume', 0)
    fifty_two_week_high = company_data.get('fiftyTwoWeekHigh', 0)
    fifty_two_week_low = company_data.get('fiftyTwoWeekLow', 0)

    # 会社基本情報
    website = company_data.get('website', '')
    city = company_data.get('city', '')
    full_time_employees = company_data.get('fullTimeEmployees', 0)

    # Price Trend (MA乖離率)
    ma_5_value = company_data.get('ma_5_value', 0)
    ma_5_deviation = company_data.get('ma_5_deviation', 0)
    ma_5_trend = company_data.get('ma_5_trend', 'neutral')
    ma_25_value = company_data.get('ma_25_value', 0)
    ma_25_deviation = company_data.get('ma_25_deviation', 0)
    ma_25_trend = company_data.get('ma_25_trend', 'neutral')
    ma_75_value = company_data.get('ma_75_value', 0)
    ma_75_deviation = company_data.get('ma_75_deviation', 0)
    ma_75_trend = company_data.get('ma_75_trend', 'neutral')
    ma_200_value = company_data.get('ma_200_value', 0)
    ma_200_deviation = company_data.get('ma_200_deviation', 0)
    ma_200_trend = company_data.get('ma_200_trend', 'neutral')

    # NaN対策
    if pd.isna(sector):
        sector = ''
    if pd.isna(industry):
        industry = ''
    if pd.isna(company_name_ja):
        company_name_ja = ''
    if pd.isna(company_name_en):
        company_name_en = ''
    if pd.isna(trailing_pe):
        trailing_pe = 0
    else:
        trailing_pe = float(trailing_pe)
    if pd.isna(price_to_book):
        price_to_book = 0
    else:
        price_to_book = float(price_to_book)
    if pd.isna(dividend_yield):
        dividend_yield = 0
    else:
        dividend_yield = float(dividend_yield)

    # 追加項目のNaN対策
    if pd.isna(forward_pe):
        forward_pe = 0
    else:
        forward_pe = float(forward_pe)
    if pd.isna(return_on_equity):
        return_on_equity = 0
    else:
        return_on_equity = float(return_on_equity)
    if pd.isna(return_on_assets):
        return_on_assets = 0
    else:
        return_on_assets = float(return_on_assets)
    if pd.isna(profit_margins):
        profit_margins = 0
    else:
        profit_margins = float(profit_margins)
    if pd.isna(revenue_growth):
        revenue_growth = 0
    else:
        revenue_growth = float(revenue_growth)
    if pd.isna(previous_close):
        previous_close = 0
    else:
        previous_close = float(previous_close)
    if pd.isna(open_price):
        open_price = 0
    else:
        open_price = float(open_price)
    if pd.isna(day_high):
        day_high = 0
    else:
        day_high = float(day_high)
    if pd.isna(day_low):
        day_low = 0
    else:
        day_low = float(day_low)
    if pd.isna(volume):
        volume = 0
    else:
        volume = int(volume)
    if pd.isna(average_volume):
        average_volume = 0
    else:
        average_volume = int(average_volume)
    if pd.isna(fifty_two_week_high):
        fifty_two_week_high = 0
    else:
        fifty_two_week_high = float(fifty_two_week_high)
    if pd.isna(fifty_two_week_low):
        fifty_two_week_low = 0
    else:
        fifty_two_week_low = float(fifty_two_week_low)
    if pd.isna(website):
        website = ''
    if pd.isna(city):
        city = ''
    if pd.isna(full_time_employees):
        full_time_employees = 0
    else:
        full_time_employees = int(full_time_employees)

    # Price TrendのNaN対策
    if pd.isna(ma_5_value):
        ma_5_value = 0
    else:
        ma_5_value = float(ma_5_value)
    if pd.isna(ma_5_deviation):
        ma_5_deviation = 0
    else:
        ma_5_deviation = float(ma_5_deviation)
    if pd.isna(ma_5_trend):
        ma_5_trend = 'neutral'
    if pd.isna(ma_25_value):
        ma_25_value = 0
    else:
        ma_25_value = float(ma_25_value)
    if pd.isna(ma_25_deviation):
        ma_25_deviation = 0
    else:
        ma_25_deviation = float(ma_25_deviation)
    if pd.isna(ma_25_trend):
        ma_25_trend = 'neutral'
    if pd.isna(ma_75_value):
        ma_75_value = 0
    else:
        ma_75_value = float(ma_75_value)
    if pd.isna(ma_75_deviation):
        ma_75_deviation = 0
    else:
        ma_75_deviation = float(ma_75_deviation)
    if pd.isna(ma_75_trend):
        ma_75_trend = 'neutral'
    if pd.isna(ma_200_value):
        ma_200_value = 0
    else:
        ma_200_value = float(ma_200_value)
    if pd.isna(ma_200_deviation):
        ma_200_deviation = 0
    else:
        ma_200_deviation = float(ma_200_deviation)
    if pd.isna(ma_200_trend):
        ma_200_trend = 'neutral'

    # 更新データ
    data = {
        'meta': {
            'marketCap': market_cap_million,
            'regularMarketPrice': stock_price,
            'DATE': str(date),
            'company_name_ja': str(company_name_ja),
            'longName': str(company_name_en),
            'sector': str(sector),
            'industry': str(industry),
            'trailingPE': trailing_pe,
            'priceToBook': price_to_book,
            'dividendYield': dividend_yield,
            # 追加項目
            'forwardPE': forward_pe,
            'returnOnEquity': return_on_equity,
            'returnOnAssets': return_on_assets,
            'profitMargins': profit_margins,
            'revenueGrowth': revenue_growth,
            'previousClose': previous_close,
            'open': open_price,
            'dayHigh': day_high,
            'dayLow': day_low,
            'volume': volume,
            'averageVolume': average_volume,
            'fiftyTwoWeekHigh': fifty_two_week_high,
            'fiftyTwoWeekLow': fifty_two_week_low,
            'website': str(website),
            'city': str(city),
            'fullTimeEmployees': full_time_employees,
            # Price Trend (MA乖離率)
            'ma_5_value': ma_5_value,
            'ma_5_deviation': ma_5_deviation,
            'ma_5_trend': str(ma_5_trend),
            'ma_25_value': ma_25_value,
            'ma_25_deviation': ma_25_deviation,
            'ma_25_trend': str(ma_25_trend),
            'ma_75_value': ma_75_value,
            'ma_75_deviation': ma_75_deviation,
            'ma_75_trend': str(ma_75_trend),
            'ma_200_value': ma_200_value,
            'ma_200_deviation': ma_200_deviation,
            'ma_200_trend': str(ma_200_trend),
        }
    }

    try:
        response = wordpress_request('POST', url, headers=headers, json=data)
        return response.status_code == 200
    except Exception as e:
        return False


def update_company(post_id, company_data, existing_slug='', dry_run=False):
    """既存企業ページ更新（多言語対応）"""
    code = company_data.get('code', '')

    # Dry Run表示
    if dry_run:
        company_name_ja = company_data.get('company_name_ja', '')
        company_name_en = company_data.get('company_name_en', '')
        stock_price = company_data.get('currentPrice', 0)
        market_cap = company_data.get('marketCap', 0)

        if pd.notna(market_cap) and market_cap > 0:
            market_cap_million = int(market_cap / 1000000)
        else:
            market_cap_million = 0

        print(f"   📍 日本語版:")
        print(f"      ID: {post_id}")
        print(f"      スラッグ: {existing_slug}")
        print(f"      URL: {WP_SITE_URL}/company/{existing_slug}/")

        # 英語版も確認
        en_post_id = get_translation_by_ticker(code, 'en')
        if en_post_id:
            print(f"   🌐 英語版:")
            print(f"      ID: {en_post_id}")
            print(f"      URL: {WP_SITE_URL}/en/company/{existing_slug}/")
        else:
            print(f"   ⚠️  英語版: 見つかりません")

        print(f"   企業名（日）: {company_name_ja}")
        print(f"   企業名（英）: {company_name_en}")
        print(f"   株価: {stock_price:,.0f}円 (更新)" if pd.notna(stock_price) else "   株価: データなし")
        print(f"   時価総額: {market_cap_million:,}百万円 (更新)")

        return True

    # 実際の更新処理
    # 1. 日本語版を更新
    success_ja = update_single_post(post_id, company_data, 'ja', dry_run)

    # 2. 英語版を更新
    en_post_id = get_translation_by_ticker(code, 'en')
    success_en = True

    if en_post_id:
        success_en = update_single_post(en_post_id, company_data, 'en', dry_run)

    return success_ja and success_en

# ============================================================
# WordPress企業下書き化
# ============================================================

def unpublish_company(post_id, dry_run=False):
    """企業ページを下書きに変更。

    HTTP 200だけでは成功とみなさない。レスポンスJSONを解析し、idが要求した
    post_idと一致すること、statusが実際にdraftになっていることまで確認する
    （2026-09-14の重複投稿インシデントの教訓：見かけ上のHTTP成功と実際の
    状態変化は別物として扱う）。
    """
    if dry_run:
        print(f"   既存ID: {post_id}")
        print(f"   アクション: 下書き化")
        return True

    headers = get_auth_headers()
    url = f"{WP_SITE_URL}/wp-json/wp/v2/company/{post_id}"

    data = {'status': 'draft'}

    try:
        response = wordpress_request('POST', url, headers=headers, json=data)
    except Exception as e:
        print(f"   ❌ 下書き化リクエストで例外: {e}")
        return False

    if response.status_code != 200:
        print(f"   ❌ 下書き化失敗: HTTP {response.status_code} {response.text[:200]}")
        return False

    try:
        body = response.json()
    except ValueError:
        print(f"   ❌ 下書き化のレスポンスがJSONとして解析できません: {response.text[:200]}")
        return False

    returned_id = body.get('id')
    returned_status = body.get('status')

    if returned_id != post_id:
        print(f"   ❌ 下書き化のレスポンスのid({returned_id})が要求したpost_id({post_id})と一致しません")
        return False

    if returned_status != 'draft':
        print(f"   ❌ 下書き化後もstatusがdraftになっていません（status={returned_status}）")
        return False

    return True

# ============================================================
# --auto-unpublish 専用: 公式確認済み上場廃止リストの検証・照合
#
# yfinance_errors（通信障害・レート制限・解析エラー等）は上場廃止を意味しないため、
# 下書き化の根拠には一切使わない。根拠は --confirmed-delisted-csv のみ。
# 4449→590Aのような別法人への移行や、企業名の類似性による自動統合は行わない
# （照合はcodeの完全一致のみで行う）。
# ============================================================

def get_jst_today():
    """JSTでの本日日付（date型）を返す。"""
    return datetime.now(JST).date()


def _parse_strict_date(value):
    """'YYYY-MM-DD'文字列をdateに変換。不正ならNoneを返す。"""
    try:
        return datetime.strptime(str(value).strip(), _DATE_FORMAT).date()
    except (ValueError, TypeError):
        return None


def load_confirmed_delisted_csv(path, today):
    """公式確認済み上場廃止リストを読み込み、厳格に検証する。

    1件でも問題があれば処理全体をexit(1)で中止する（fail-closed）。
    部分的に有効な行だけを使って処理を続行することはしない。

    必須列: code, delisting_date, market, official_url, confirmed_date
    戻り値: {code: {code, delisting_date(date), market, official_url, confirmed_date(date)}}
    """
    if not path:
        print("❌ --auto-unpublishには--confirmed-delisted-csvの指定が必須です"
              "（yfinance_errorsへのフォールバックは行いません）")
        sys.exit(1)

    if not os.path.exists(path):
        print(f"❌ 公式確認済み上場廃止リストが見つかりません: {path}")
        sys.exit(1)

    try:
        df = pd.read_csv(path, dtype=str, encoding='utf-8-sig')
    except Exception as e:
        print(f"❌ 公式確認済み上場廃止リストの読み込みに失敗しました: {path} ({e})")
        sys.exit(1)

    missing_columns = [c for c in REQUIRED_DELISTED_COLUMNS if c not in df.columns]
    if missing_columns:
        print(f"❌ 公式確認済み上場廃止リストに必須列がありません: {missing_columns}")
        sys.exit(1)

    errors = []
    codes_seen = {}
    records = {}

    for idx, row in df.iterrows():
        line = idx + 2  # ヘッダー行(1) + 0始まりのインデックス補正

        raw = {col: row[col] for col in REQUIRED_DELISTED_COLUMNS}
        blank_cols = [
            col for col, val in raw.items()
            if pd.isna(val) or str(val).strip() == ""
        ]
        if blank_cols:
            errors.append(f"行{line}: 必須列が空欄です: {blank_cols}")
            continue  # NaN起因の例外を避けるため、この行の残りの検証はスキップ

        code = str(raw["code"]).strip().upper()
        if not _DELISTED_CODE_PATTERN.match(code):
            errors.append(f"行{line}: codeの書式が不正です: '{code}'（英数字4文字である必要があります）")

        if code in codes_seen:
            errors.append(f"行{line}: codeが行{codes_seen[code]}と重複しています: '{code}'")
        else:
            codes_seen[code] = line

        delisting_date = _parse_strict_date(raw["delisting_date"])
        if delisting_date is None:
            errors.append(f"行{line}: delisting_dateがYYYY-MM-DD形式ではありません: '{raw['delisting_date']}'")

        confirmed_date = _parse_strict_date(raw["confirmed_date"])
        if confirmed_date is None:
            errors.append(f"行{line}: confirmed_dateがYYYY-MM-DD形式ではありません: '{raw['confirmed_date']}'")
        elif confirmed_date > today:
            errors.append(f"行{line}: confirmed_dateが未来日です: '{raw['confirmed_date']}'")

        url = str(raw["official_url"]).strip()
        if not (url.startswith("http://") or url.startswith("https://")):
            errors.append(f"行{line}: official_urlがhttp(s)://で始まっていません: '{url}'")

        records[code] = {
            "code": code,
            "delisting_date": delisting_date,
            "market": raw["market"],
            "official_url": url,
            "confirmed_date": confirmed_date,
        }

    if errors:
        print(f"❌ 公式確認済み上場廃止リストの検証に失敗しました（{len(errors)}件）:")
        for e in errors:
            print(f"   - {e}")
        sys.exit(1)

    return records


def split_confirmed_delisted(records, today):
    """delisting_dateが実行日(JST)以前か否かでeligible/deferredに分ける。"""
    eligible = {c: r for c, r in records.items() if r["delisting_date"] <= today}
    deferred = {c: r for c, r in records.items() if r["delisting_date"] > today}
    return eligible, deferred


def run_auto_unpublish(confirmed_delisted_csv, translation_groups,
                        execute, dry_run, today, languages, wp_url=None):
    """--auto-unpublish の専用処理経路（WPML全言語対応）。

    通常の新規作成・更新処理（process_companies）とは完全に分離しており、
    このパスからは create_company / update_company を一切呼ばない。

    translation_groupsはget_existing_companies_all_langs()の戻り値
    （{code: {lang: [{'id','slug','status'}, ...]}}）。証券コード×言語×投稿単位で
    対象を判定する。同一コード・同一言語に複数投稿（重複投稿）があっても1件目だけを
    見て判定するのではなく、全投稿を個別に評価する。draftでない投稿はすべて下書き化
    対象にする（「どちらが正規の投稿か」を推測して一方だけ処理することはしない。
    2026-09-14に本番で発覚した重複投稿インシデントの教訓）。

    languagesに列挙した全言語の全投稿がdraftになって初めて、そのコードは
    「対応完了」として扱う。一部の言語・一部の投稿だけdraft化できた場合はpartial
    （部分成功）として明示的に区別し、successには含めない。

    execute=False の場合は対象一覧の表示のみで、WordPressへの書き込みAPIは一切呼ばない
    （プレビューモード）。

    execute=True で実際に書き込みを行った場合、unpublish_company()が個々の
    レスポンスをid/statusまで検証していても、実行後のWordPress側の実際の状態を
    独立に再取得して確認する（wp_url必須）。「見かけ上は成功」と「実際に
    publishが残っていないこと」を別々に検証する。
    """
    records = load_confirmed_delisted_csv(confirmed_delisted_csv, today)
    eligible, deferred = split_confirmed_delisted(records, today)

    to_unpublish = []           # [(code, lang, post_id, status)]
    already_draft = []          # [(code, lang, post_id)]
    lang_not_found = []         # [(code, lang)]  その言語の投稿が1件も存在しない
    code_fully_not_found = []   # 全言語で投稿が見つからないコード

    for code in sorted(eligible):
        group = translation_groups.get(code)
        if not group:
            code_fully_not_found.append(code)
            continue
        for lang in languages:
            entries = group.get(lang, [])
            if not entries:
                lang_not_found.append((code, lang))
                continue
            for entry in entries:
                if entry["status"] == "draft":
                    already_draft.append((code, lang, entry["id"]))
                else:
                    to_unpublish.append((code, lang, entry["id"], entry["status"]))

    codes_with_target = sorted({c for c, _, _, _ in to_unpublish})

    print("\n" + "=" * 60)
    print("🗂  自動下書き化プレビュー（--auto-unpublish、WPML全言語対応）")
    print("=" * 60)
    print(f"公式確認済みリスト: {confirmed_delisted_csv}")
    print(f"実行日（JST）: {today.isoformat()}")
    print(f"対象言語: {', '.join(languages)}")
    print()
    print(f"実行対象（{len(to_unpublish)}件、{len(codes_with_target)}社）:")
    for code, lang, post_id, status in sorted(to_unpublish):
        print(f"   - {code} [{lang}] post_id={post_id}, 現在status={status}")
    print(f"\n廃止日前のため保留（{len(deferred)}社）:")
    for code in sorted(deferred):
        print(f"   - {code}（廃止日: {deferred[code]['delisting_date']}）")
    print(f"\nすでにdraftの言語（{len(already_draft)}件）:")
    for code, lang, post_id in sorted(already_draft):
        print(f"   - {code} [{lang}] post_id={post_id}（すでにdraft）")
    print(f"\n片言語のみ投稿が見つからない（{len(lang_not_found)}件）:")
    for code, lang in sorted(lang_not_found):
        print(f"   - {code} [{lang}]: 投稿が見つかりません")
    print(f"\n全言語で投稿が見つからないコード（{len(code_fully_not_found)}社）:")
    for code in sorted(code_fully_not_found):
        print(f"   - {code}")

    # 同一コード×同一言語に複数投稿がある場合を可視化する（重複投稿インシデントの再発検知）
    post_counts = defaultdict(int)
    for code, lang, _post_id, _status in to_unpublish:
        post_counts[(code, lang)] += 1
    for code, lang, _post_id in already_draft:
        post_counts[(code, lang)] += 1
    duplicated = sorted((code, lang, n) for (code, lang), n in post_counts.items() if n > 1)
    print(f"\n同一コード×同一言語に複数投稿あり（重複投稿、{len(duplicated)}件）:")
    for code, lang, n in duplicated:
        print(f"   - {code} [{lang}]: {n}件")

    print()
    print(
        f"合計: 対象{len(to_unpublish)}件({len(codes_with_target)}社) / "
        f"保留{len(deferred)}社 / 全言語未検出{len(code_fully_not_found)}社 / "
        f"既下書き{len(already_draft)}件 / 片言語未検出{len(lang_not_found)}件 / "
        f"重複投稿{len(duplicated)}件"
    )
    print("=" * 60)

    skipped = len(eligible) - len(codes_with_target)

    if not execute:
        print("\nℹ️  プレビューモードです。--auto-unpublish-executeを指定するまでWordPressへの書き込みは行いません。")
        return {"success": [], "partial": [], "failed": [], "skipped": skipped, "remaining_publish": []}

    if dry_run:
        print("\nℹ️  --dry-runが指定されているため、WordPressへの書き込みは行いません。")
        return {"success": [], "partial": [], "failed": [], "skipped": skipped, "remaining_publish": []}

    if not wp_url:
        print("❌ execute=Trueにはwp_urlの指定が必須です（実行後の残存publish確認に使用）")
        sys.exit(1)

    print(f"\n🚀 {len(to_unpublish)}件（{len(codes_with_target)}社）を下書き化します...")
    results_by_code = defaultdict(list)  # code -> [(lang, post_id, ok)]
    for code, lang, post_id, status in sorted(to_unpublish):
        print(f"\n[下書き化] {code} [{lang}] (post_id={post_id})")
        try:
            ok = unpublish_company(post_id, dry_run=False)
        except Exception as e:
            ok = False
            print(f"   ❌ 例外: {e}")
        results_by_code[code].append((lang, post_id, ok))
        if ok:
            print("   ✅ 下書き化成功")
        else:
            print("   ❌ 下書き化失敗")
        time.sleep(REQUEST_DELAY)

    success = []
    partial = []
    for code, results in results_by_code.items():
        if all(ok for _, _, ok in results):
            success.append(code)
        else:
            partial.append((code, results))

    print("\n" + "=" * 60)
    print("結果:")
    print(f"  全言語成功: {len(success)}社")
    print(f"  部分成功（一部言語のみ失敗、要手動対応）: {len(partial)}社")
    print(f"  スキップ（保留/未検出/既下書き）: {skipped}社")
    print("=" * 60)

    if partial:
        print("\n⚠️  一部言語のみ下書き化に失敗した銘柄（手動確認が必要）:")
        for code, results in partial:
            detail = ", ".join(f"{lang}(post_id={post_id}, {'成功' if ok else '失敗'})" for lang, post_id, ok in results)
            print(f"   - {code}: {detail}")

    # 実行後の独立検証: unpublish_company()の戻り値（見かけ上の成功）を信用せず、
    # WordPress側を再取得して対象投稿に実際にpublishが残っていないか確認する。
    print("\n" + "=" * 60)
    print("🔍 実行後の残存publish確認（JA/EN全ステータス再取得）")
    print("=" * 60)
    post_groups = get_existing_companies_all_langs(wp_url, languages)
    remaining_publish = []  # (code, lang, post_id, actual_status)
    for code, lang, post_id, _prev_status in sorted(to_unpublish):
        entries = post_groups.get(code, {}).get(lang, [])
        match = next((e for e in entries if e["id"] == post_id), None)
        if match is None:
            remaining_publish.append((code, lang, post_id, "再取得で見つからず"))
        elif match["status"] != "draft":
            remaining_publish.append((code, lang, post_id, match["status"]))

    if remaining_publish:
        print(f"❌ 実行後もdraftになっていない投稿が{len(remaining_publish)}件あります（手動確認が必要）:")
        for code, lang, post_id, actual_status in remaining_publish:
            print(f"   - {code} [{lang}] post_id={post_id}: 実際のstatus={actual_status}")
    else:
        print(f"✅ 対象{len(to_unpublish)}件すべて、実行後の再取得でdraftになっていることを確認しました")
    print("=" * 60)

    return {
        "success": success,
        "partial": partial,
        "failed": [c for c, _ in partial],
        "skipped": skipped,
        "remaining_publish": remaining_publish,
    }


# ============================================================
# メイン処理
# ============================================================

def process_companies(integrated_csv, errors_csv, existing_companies, confirmed_delisted_codes,
                     limit=None, skip=0, create_status='publish',
                     dry_run=False, update_only=False):
    """条件分岐処理（新規作成・更新のみ。下書き化は行わない）

    下書き化（draft化）は --auto-unpublish 専用経路（run_auto_unpublish）でのみ行う。
    ここでのerror_codes（yfinance_errors）は条件3（新規作成をスキップ）の判定にのみ使い、
    下書き化の根拠にはしない。

    existing_companiesはget_existing_companies_all_langs()の戻り値
    （{code: {lang: [{'id','slug','status'}, ...]}}）。全言語・全ステータスを対象に
    「存在するかどうか」を判定するため、下書き化された上場廃止企業や英語投稿しか
    存在しない企業を「未登録」と誤判定して再作成することはない。

    confirmed_delisted_codesは公式確認済み上場廃止リストのコード集合。このリストに
    含まれるコードは、既存投稿がどの言語にも見つからない場合でも新規作成しない
    （2026-09-14に本番で発覚した重複投稿インシデントの再発防止）。"""
    
    print("\n" + "=" * 60)
    if dry_run:
        print("🔍 Dry Run モード（実際には更新しません）")
        print("=" * 60)
    print("📊 データ処理開始")
    print("=" * 60)
    
    # 統合データ読み込み
    print(f"\n📥 統合データ読み込み: {integrated_csv}")
    df = pd.read_csv(integrated_csv, encoding='utf-8-sig')
    df['code'] = df['code'].astype(str)
    print(f"   ✅ 読み込み成功: {len(df)}社")
    
    # エラーデータ読み込み（存在する場合）
    error_codes = set()
    if os.path.exists(errors_csv):
        print(f"\n📥 エラーデータ読み込み: {errors_csv}")
        df_errors = pd.read_csv(errors_csv, encoding='utf-8-sig')
        df_errors['code'] = df_errors['code'].astype(str)
        error_codes = set(df_errors['code'].tolist())
        print(f"   ✅ エラー企業: {len(error_codes)}社")
    else:
        print(f"\n⚠️  エラーファイルなし: {errors_csv}")
    
    # 範囲指定
    if skip > 0:
        df = df.iloc[skip:]
        print(f"\n⏭️  最初の{skip}社をスキップ")
    
    if limit:
        df = df.iloc[:limit]
        print(f"📊 処理対象: {len(df)}社")
    
    # 統計カウンター
    stats = {
        'created': 0,
        'updated': 0,
        'skipped': 0,
        'failed': 0,
        'needs_repair': 0,
    }
    created_companies = []
    
    print("\n" + "=" * 60)
    if dry_run:
        print("🔍 処理内容プレビュー")
    else:
        print("🚀 WordPress処理開始")
    print("=" * 60)
    
    for index, row in df.iterrows():
        ticker = row['code']
        company_name = row.get('company_name_ja', ticker)
        
        # yfinanceデータの有無（株価または時価総額があればOK）
        has_yfinance_data = pd.notna(row.get('currentPrice')) or pd.notna(row.get('marketCap'))

        # WordPress登録状況（全言語・全ステータス対象）
        lang_entries = existing_companies.get(ticker, {})
        ja_posts = lang_entries.get('ja', [])
        en_posts = lang_entries.get('en', [])
        is_fully_new = not ja_posts and not en_posts

        # 条件分岐
        if ticker in confirmed_delisted_codes:
            # 確認済み上場廃止企業は、既存投稿の有無に関わらず新規作成しない。
            # draft化は --auto-unpublish 専用経路でのみ行う。
            stats['skipped'] += 1
            print(f"\n[スキップ] {company_name} ({ticker}) - 確認済み上場廃止リストに含まれるため新規作成しません")

        elif has_yfinance_data and is_fully_new:
            # 条件1: 新規作成

            # update-only モードなら新規作成をスキップ
            if update_only:
                stats['skipped'] += 1
                continue

            prefix = "[Dry Run] 新規作成予定" if dry_run else "[新規]"
            print(f"\n{prefix}: {company_name} ({ticker})")

            if create_company(row, status=create_status, dry_run=dry_run):
                stats['created'] += 1
                if not dry_run:
                    created_companies.append({'code': ticker, 'name': company_name})
                    print(f"   ✅ 作成成功")
            else:
                stats['failed'] += 1
                if not dry_run:
                    print(f"   ❌ 作成失敗")

        elif has_yfinance_data and not ja_posts and en_posts:
            # 英語投稿のみ存在し日本語投稿が見つからない。原因不明のまま自動でJAを
            # 作り直すと別の不整合を招きかねないため、自動作成せず要修復として報告する。
            stats['needs_repair'] += 1
            en_ids = [p['id'] for p in en_posts]
            print(f"\n[要修復] {company_name} ({ticker}) - EN投稿のみ存在しJA投稿がありません"
                  f"（自動作成しません、手動確認が必要）: en post_id={en_ids}")

        elif has_yfinance_data and ja_posts:
            # 条件2: 更新
            if len(ja_posts) > 1:
                stats['needs_repair'] += 1
                ja_ids = [p['id'] for p in ja_posts]
                print(f"\n[要修復] {company_name} ({ticker}) - JA投稿が{len(ja_posts)}件重複しています"
                      f"（自動更新をスキップ、手動確認が必要）: post_id={ja_ids}")
            else:
                post_id = ja_posts[0]['id']
                existing_slug = ja_posts[0].get('slug', '')
                prefix = "[Dry Run] 更新予定" if dry_run else "[更新]"
                print(f"\n{prefix}: {company_name} ({ticker})")

                if update_company(post_id, row, existing_slug=existing_slug, dry_run=dry_run):
                    stats['updated'] += 1
                    if not dry_run:
                        print(f"   ✅ 更新成功")
                else:
                    stats['failed'] += 1
                    if not dry_run:
                        print(f"   ❌ 更新失敗")

        elif ticker in error_codes and is_fully_new:
            # 条件3: スルー
            stats['skipped'] += 1
            # 静かにスキップ（ログ出力なし）

        elif ticker in error_codes and not is_fully_new:
            # 条件4: yfinanceエラーのみでは上場廃止と確定できないため、ここでは下書き化しない。
            # 下書き化は --auto-unpublish 専用経路（公式確認済みリストとの照合）でのみ行う。
            stats['skipped'] += 1
            print(f"\n[スキップ] {company_name} ({ticker}) - yfinanceエラー（手動確認推奨。"
                  f"下書き化は--auto-unpublishで公式確認済みリストと照合して行う）")

        # 待機（Dry Runでは待機しない）
        if not dry_run:
            time.sleep(REQUEST_DELAY)
    
    # 結果表示
    print("\n" + "=" * 60)
    if dry_run:
        print("✅ Dry Run 完了（実際には更新していません）")
    else:
        print("✅ 処理完了")
    print("=" * 60)
    print(f"新規作成: {stats['created']}社")
    print(f"更新: {stats['updated']}社")
    print(f"スキップ: {stats['skipped']}社")
    print(f"要修復（片言語欠落/重複投稿）: {stats['needs_repair']}社")
    print(f"失敗: {stats['failed']}社")
    print("=" * 60)
    
    return stats, created_companies

# ============================================================
# メール通知
# ============================================================

def send_new_companies_email(created_companies):
    gmail_user = "worldonetrading2015@gmail.com"
    gmail_password = os.getenv('GMAIL_APP_PASSWORD')
    if not gmail_password:
        print("⚠️  GMAIL_APP_PASSWORD が未設定のためメール送信をスキップ")
        return

    date_str = datetime.now().strftime('%Y-%m-%d')
    company_lines = "\n".join([f"  - {c['name']} ({c['code']})" for c in created_companies])
    body = f"JapanIR Daily Update: {date_str}\n\n新規上場企業 {len(created_companies)}社が登録されました：\n\n{company_lines}\n\nhttps://japanir.jp"

    msg = MIMEText(body, 'plain', 'utf-8')
    msg['Subject'] = f"[JapanIR] 新規上場企業 {len(created_companies)}社を登録しました ({date_str})"
    msg['From'] = gmail_user
    msg['To'] = gmail_user

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(gmail_user, gmail_password)
            server.send_message(msg)
        print(f"✅ メール送信完了 → {gmail_user}")
    except Exception as e:
        print(f"❌ メール送信失敗: {e}")

# ============================================================
# エントリーポイント
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description='WordPress企業データ スマート更新スクリプト'
    )
    
    parser.add_argument(
        '--csv',
        default=DEFAULT_CSV,
        help=f'統合CSVファイル（デフォルト: {DEFAULT_CSV}）'
    )
    
    parser.add_argument(
        '--errors',
        default=DEFAULT_ERRORS_CSV,
        help=f'エラーCSVファイル（デフォルト: {DEFAULT_ERRORS_CSV}）'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        help='処理する企業数（例: --limit 100）'
    )
    
    parser.add_argument(
        '--skip',
        type=int,
        default=0,
        help='スキップする企業数（例: --skip 100）'
    )
    
    parser.add_argument(
        '--status',
        default='publish',
        choices=['publish', 'draft'],
        help='新規作成時のステータス（デフォルト: publish）'
    )
    
    parser.add_argument(
        '--auto-unpublish',
        action='store_true',
        help='公式確認済み上場廃止リストと照合して自動で下書き化する専用モード（デフォルト: 無効）。'
             '--confirmed-delisted-csv の指定が必須。単独では対象一覧を表示するのみ（書き込みなし）'
    )

    parser.add_argument(
        '--confirmed-delisted-csv',
        default=DEFAULT_CONFIRMED_DELISTED_CSV,
        help='公式確認済み上場廃止リストCSV（列: code, delisting_date, market, official_url, '
             'confirmed_date）。--auto-unpublishでは下書き化対象の判定に使う。通常更新でも'
             'このリストに含まれるコードは新規作成の対象外にする'
             f'（デフォルト: {DEFAULT_CONFIRMED_DELISTED_CSV}）'
    )

    parser.add_argument(
        '--auto-unpublish-execute',
        action='store_true',
        help='--auto-unpublishで実際にWordPressへ下書き化を書き込む。これだけでは無効、'
             '--auto-unpublishと--confirmed-delisted-csvも同時に必要'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='実際には更新せず、処理内容を表示（Dry Runモード）'
    )

    parser.add_argument(
        '--update-only',
        action='store_true',
        help='既存企業のみ更新 (新規作成はスキップ)'
    )

    parser.add_argument(
        '--preflight-only',
        action='store_true',
        help='WordPress APIの疎通確認のみ実行'
    )

    args = parser.parse_args()

    # --auto-unpublish系フラグの組み合わせを厳格化（誤操作防止、fail-closed）
    if args.auto_unpublish_execute and not args.auto_unpublish:
        print("❌ --auto-unpublish-executeは単独指定できません。--auto-unpublishと"
              "--confirmed-delisted-csvも同時に指定してください")
        sys.exit(1)
    # --confirmed-delisted-csvはデフォルト値を持つため、--auto-unpublishの有無に
    # 関わらず常に指定されている（通常更新でも新規作成の除外判定に使う）

    if args.preflight_only:
        print("🔍 WordPress API疎通確認")
        preflight_wordpress_api(WP_URL)
        return

    # --auto-unpublishは通常の新規作成・更新処理（process_companies）とは
    # 完全に独立した専用経路。ここに入った場合、process_companiesは一切呼ばない。
    if args.auto_unpublish:
        today = get_jst_today()
        print("=" * 60)
        print("🚀 WordPress 自動下書き化（--auto-unpublish 専用経路）")
        print("=" * 60)
        print(f"実行日時: {datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')} JST")
        print(f"公式確認済みリスト: {args.confirmed_delisted_csv}")
        execute = args.auto_unpublish_execute
        print(f"実行モード: {'実行（下書き化を書き込む）' if execute else 'プレビューのみ（書き込みなし）'}")
        if args.dry_run:
            print("Dry Run: 有効（実行モードでも書き込みは行いません）")
        print()

        translation_groups = get_existing_companies_all_langs(WP_URL, WPML_LANGUAGES)
        result = run_auto_unpublish(
            confirmed_delisted_csv=args.confirmed_delisted_csv,
            translation_groups=translation_groups,
            execute=execute,
            dry_run=args.dry_run,
            today=today,
            languages=WPML_LANGUAGES,
            wp_url=WP_URL,
        )

        print("\n✅ スクリプト実行完了")
        if result["failed"] or result.get("remaining_publish"):
            sys.exit(1)
        return

    print("=" * 60)
    print("🚀 WordPress企業データ スマート更新")
    if args.dry_run:
        print("   🔍 Dry Run モード")
    if args.update_only:
        print("   📝 既存企業のみ更新モード")
    print("=" * 60)
    print(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"統合CSV: {args.csv}")
    print(f"エラーCSV: {args.errors}")
    if args.limit:
        print(f"処理制限: {args.limit}社")
    if args.skip:
        print(f"スキップ: {args.skip}社")
    print(f"新規作成ステータス: {args.status}")
    if args.update_only:
        print(f"既存のみ更新: 有効")
    if args.dry_run:
        print(f"Dry Run: 有効（実際には更新しません）")
    print(f"確認済み上場廃止リスト（新規作成除外用）: {args.confirmed_delisted_csv}")
    print()

    # 確認済み上場廃止リストの読み込み（新規作成の除外判定用）。
    # ファイル不備・欠損時はload_confirmed_delisted_csv()内でfail-closedにexit(1)する。
    today = get_jst_today()
    confirmed_delisted_records = load_confirmed_delisted_csv(args.confirmed_delisted_csv, today)
    confirmed_delisted_codes = set(confirmed_delisted_records.keys())
    print(f"確認済み上場廃止コード: {len(confirmed_delisted_codes)}件（新規作成対象から除外）\n")

    # 既存企業取得（全言語・全ステータス）
    existing_companies = get_existing_companies_all_langs(WP_URL, WPML_LANGUAGES)

    # 処理実行
    stats, created_companies = process_companies(
        integrated_csv=args.csv,
        errors_csv=args.errors,
        existing_companies=existing_companies,
        confirmed_delisted_codes=confirmed_delisted_codes,
        limit=args.limit,
        skip=args.skip,
        create_status=args.status,
        dry_run=args.dry_run,
        update_only=args.update_only
    )

    if created_companies and not args.dry_run:
        send_new_companies_email(created_companies)

    print("\n✅ スクリプト実行完了")


if __name__ == "__main__":
    main()
