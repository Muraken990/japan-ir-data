#!/usr/bin/env python3
"""
WordPress企業データ スマート更新スクリプト
条件分岐 + 段階的実行対応 + Dry Run機能 + Update Only機能

条件1: Yahoo○ + yfinance○ + WordPress× → 新規作成
条件2: Yahoo○ + yfinance○ + WordPress○ → 更新
条件3: Yahoo○ + yfinance× + WordPress× → スルー
条件4: Yahoo○ + yfinance× + WordPress○ → 下書き化（手動確認推奨）
"""

import pandas as pd
import requests
import base64
import time
import os
import argparse
from datetime import datetime

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

# 処理速度（秒）
REQUEST_DELAY = float(os.getenv('REQUEST_DELAY', '0.5'))

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

# ============================================================
# WordPress企業取得
# ============================================================

def get_all_existing_companies(wp_url):
    """WordPressから既存の全企業を取得（offsetベース）"""
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
        
        response = requests.get(
            f"{wp_url}/wp-json/wp/v2/company", 
            params=params,
            headers=headers,
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"   ⚠️  REST API エラー: ステータスコード {response.status_code}")
            break
            
        companies = response.json()
        
        # 空配列チェック
        if not companies or len(companies) == 0:
            break
        
        # デバッグ: 最初の1社だけ
        if offset == 0 and len(existing_companies) == 0:
            print(f"\n   🔍 デバッグ（最初の1社）:")
            print(f"      ID: {companies[0].get('id')}")
            print(f"      stock_code: '{companies[0].get('stock_code', '')}'")
            print()
        
        for company in companies:
            code = company.get('stock_code', '')

            if code:
                # .T を除去
                clean_code = str(code).replace('.T', '')
                existing_companies[clean_code] = {
                    'id': company['id'],
                    'slug': company.get('slug', '')
                }

        print(f"   取得済み: {len(existing_companies)}社（このバッチ: {len(companies)}社, offset: {offset}）")
        
        # 100未満で終了
        if len(companies) < per_page:
            break
        
        offset += per_page
        
        # 安全装置（最大5,000社）
        if offset >= 5000:
            print(f"   ⚠️  安全装置: 5,000社で停止")
            break
    
    print(f"   ✅ 既存企業取得完了: {len(existing_companies)}社\n")

    # デバッグ: 最初の10社を表示
    if existing_companies:
        print("   🔍 デバッグ: 既存企業の最初の10社:")
        for i, (code, info) in enumerate(list(existing_companies.items())[:10]):
            print(f"      {code}: ID={info['id']}, slug={info['slug']}")
        print()

    return existing_companies


def get_all_existing_companies_en(wp_url):
    """WordPressから既存の全英語版企業を取得（offsetベース）"""
    headers = get_auth_headers()
    existing_companies_en = {}
    offset = 0
    per_page = 100

    print("📥 WordPressから既存英語版企業を取得中...")

    while True:
        params = {
            'per_page': per_page,
            'offset': offset,
            'context': 'edit',
            'lang': 'en'
        }

        response = requests.get(
            f"{wp_url}/wp-json/wp/v2/company",
            params=params,
            headers=headers,
            timeout=30
        )

        if response.status_code != 200:
            print(f"   ⚠️  REST API エラー: ステータスコード {response.status_code}")
            break

        companies = response.json()

        # 空配列チェック
        if not companies or len(companies) == 0:
            break

        for company in companies:
            code = company.get('stock_code', '')

            if code:
                # .T を除去
                clean_code = str(code).replace('.T', '')
                existing_companies_en[clean_code] = {
                    'id': company['id'],
                    'slug': company.get('slug', '')
                }

        print(f"   取得済み: {len(existing_companies_en)}社（このバッチ: {len(companies)}社, offset: {offset}）")

        # 100未満で終了
        if len(companies) < per_page:
            break

        offset += per_page

        # 安全装置（最大5,000社）
        if offset >= 5000:
            print(f"   ⚠️  安全装置: 5,000社で停止")
            break

    print(f"   ✅ 既存英語版企業取得完了: {len(existing_companies_en)}社\n")

    return existing_companies_en


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


# ============================================================
# WPML翻訳リンク
# ============================================================

def link_wpml_via_official_api(ja_post_id, en_post_id):
    """WPML正式APIで翻訳リンクを設定"""
    try:
        url = f"{WP_SITE_URL}/wp-json/custom/v1/wpml-link"
        
        payload = {
            "ja_post_id": ja_post_id,
            "en_post_id": en_post_id,
            "post_type": "company",
        }
        
        response = requests.post(
            url,
            headers=get_auth_headers(),
            json=payload,
            timeout=30
        )
        
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


def create_english_post(ja_post_id, company_data, status='publish', dry_run=False):
    """英語投稿を作成"""
    code = company_data.get('code', '')
    
    if dry_run:
        company_name_en = company_data.get('short_name_en', '')
        if pd.isna(company_name_en):
            company_name_en = company_data.get('company_name_en', '')
        print(f"   🌐 英語版作成予定:")
        print(f"      URL: {WP_SITE_URL}/en/company/company-{code}/")
        print(f"      企業名（英）: {company_name_en}")
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
    if pd.isna(company_name_en):
        company_name_en = company_data.get('company_name_en', '')
    if pd.isna(company_name_en):
        company_name_en = ''
    
    # 日付
    date = company_data.get('scrape_date', datetime.now().strftime('%Y-%m-%d'))
    
    # セクター・業種
    sector = company_data.get('sector', '')
    industry = company_data.get('industry', '')
    
    # NaN対策
    if pd.isna(sector):
        sector = ''
    if pd.isna(industry):
        industry = ''
    
    # 投稿データ
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
        response = requests.post(url, headers=headers, json=data, timeout=30)
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


# ============================================================
# WPML翻訳リンク
# ============================================================

def link_wpml_via_official_api(ja_post_id, en_post_id):
    """WPML正式APIで翻訳リンクを設定"""
    try:
        url = f"{WP_SITE_URL}/wp-json/custom/v1/wpml-link"
        
        payload = {
            "ja_post_id": ja_post_id,
            "en_post_id": en_post_id,
            "post_type": "company",
        }
        
        response = requests.post(
            url,
            headers=get_auth_headers(),
            json=payload,
            timeout=30
        )
        
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


def create_english_post(ja_post_id, company_data, status='publish', dry_run=False):
    """英語投稿を作成"""
    code = company_data.get('code', '')
    
    if dry_run:
        company_name_en = company_data.get('short_name_en', '')
        if pd.isna(company_name_en):
            company_name_en = company_data.get('company_name_en', '')
        print(f"   🌐 英語版作成予定:")
        print(f"      URL: {WP_SITE_URL}/en/company/company-{code}/")
        print(f"      企業名（英）: {company_name_en}")
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
    if pd.isna(company_name_en):
        company_name_en = company_data.get('company_name_en', '')
    if pd.isna(company_name_en):
        company_name_en = ''
    
    # 日付
    date = company_data.get('scrape_date', datetime.now().strftime('%Y-%m-%d'))
    
    # セクター・業種
    sector = company_data.get('sector', '')
    industry = company_data.get('industry', '')
    
    # NaN対策
    if pd.isna(sector):
        sector = ''
    if pd.isna(industry):
        industry = ''
    
    # 投稿データ
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
        response = requests.post(url, headers=headers, json=data, timeout=30)
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

def create_company(company_data, status='publish', dry_run=False):
    """新規企業ページ作成"""
    code = company_data.get('code', '')
    
    # Dry Run表示
    if dry_run:
        company_name_ja = company_data.get('company_name_ja', '')
        company_name_en = company_data.get('short_name_en', '')
        if pd.isna(company_name_en) or company_name_en == '':
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
    
    # NaN対策
    if pd.isna(sector):
        sector = ''
    if pd.isna(industry):
        industry = ''
    if pd.isna(company_name_ja):
        company_name_ja = ''
    if pd.isna(company_name_en):
        company_name_en = ''
    
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
            'ir_tier': 'basic',
        }
    }
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=30)
        if response.status_code == 201:
            result = response.json()
            ja_post_id = result.get("id")
            print(f"   📍 日本語版作成成功 (ID: {ja_post_id})")
            
            # 英語版も作成
            en_post_id = create_english_post(ja_post_id, company_data, status, dry_run=False)
            if en_post_id:
                # WPMLリンク
                link_wpml_via_official_api(ja_post_id, en_post_id)
            
            return True
        else:
            print(f"   HTTPエラー: {response.status_code}")
            print(f"   レスポンス: {response.text[:500]}")
            return False
    except Exception as e:
        print(f"   エラー詳細: {str(e)}")
        import traceback
        traceback.print_exc()
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
    
    # NaN対策
    if pd.isna(sector):
        sector = ''
    if pd.isna(industry):
        industry = ''
    if pd.isna(company_name_ja):
        company_name_ja = ''
    if pd.isna(company_name_en):
        company_name_en = ''
    
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
        }
    }
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=30)
        return response.status_code == 200
    except Exception as e:
        print(f"   エラー詳細: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def update_company(post_id, company_data, existing_slug='', dry_run=False, existing_companies_en=None):
    """既存企業ページ更新（多言語対応）"""
    code = company_data.get('code', '')

    # 英語版IDを取得（事前取得したマッピングを優先、なければAPI検索）
    en_post_id = None
    if existing_companies_en and code in existing_companies_en:
        en_post_id = existing_companies_en[code]['id']
    else:
        # フォールバック: 従来のAPI検索
        en_post_id = get_translation_by_ticker(code, 'en')

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
    success_en = True

    if en_post_id:
        success_en = update_single_post(en_post_id, company_data, 'en', dry_run)

    return success_ja and success_en

# ============================================================
# WordPress企業下書き化
# ============================================================

def unpublish_company(post_id, dry_run=False):
    """企業ページを下書きに変更"""
    if dry_run:
        print(f"   既存ID: {post_id}")
        print(f"   アクション: 下書き化")
        return True
    
    headers = get_auth_headers()
    url = f"{WP_SITE_URL}/wp-json/wp/v2/company/{post_id}"
    
    data = {'status': 'draft'}
    
    try:
        response = requests.post(url, headers=headers, json=data, timeout=30)
        return response.status_code == 200
    except Exception as e:
        print(f"   エラー詳細: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

# ============================================================
# メイン処理
# ============================================================

def process_companies(integrated_csv, errors_csv, existing_companies,
                     limit=None, skip=0, create_status='publish',
                     auto_unpublish=False, dry_run=False, update_only=False,
                     existing_companies_en=None):
    """条件分岐処理"""
    
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
        'unpublished': 0,
        'failed': 0
    }
    
    print("\n" + "=" * 60)
    if dry_run:
        print("🔍 処理内容プレビュー")
    else:
        print("🚀 WordPress処理開始")
    print("=" * 60)
    
    for index, row in df.iterrows():
        ticker = row['code']
        company_name = row.get('company_name_ja', ticker)
        
        # yfinanceデータの有無
        # 英語名チェック
        has_name = pd.notna(row.get('company_name_en')) or pd.notna(row.get('short_name_en'))
        
        # 重要な財務データのチェック
        has_price = pd.notna(row.get('currentPrice')) and row.get('currentPrice') > 0
        has_market_cap = pd.notna(row.get('marketCap')) and row.get('marketCap') > 0
        
        # 英語名があり、かつ株価または時価総額がある
        has_yfinance_data = has_name and (has_price or has_market_cap)
        
        # WordPress登録済みか
        is_in_wordpress = ticker in existing_companies
        
        # 条件分岐
        if has_yfinance_data and not is_in_wordpress:
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
                    print(f"   ✅ 作成成功")
            else:
                stats['failed'] += 1
                if not dry_run:
                    print(f"   ❌ 作成失敗")
        
        elif has_yfinance_data and is_in_wordpress:
            # 条件2: 既存企業はスキップ（更新しない）
            stats['skipped'] += 1
            # 英語版リンク状況を表示
            post_id = existing_companies[ticker]['id']
            existing_slug = existing_companies[ticker].get('slug', ticker)
            en_info = existing_companies_en.get(ticker) if existing_companies_en else None

            print(f"\n[スキップ] {company_name} ({ticker})")
            print(f"   📍 日本語版: ID={post_id}, URL: {WP_SITE_URL}/company/{existing_slug}/")
            if en_info:
                print(f"   🌐 英語版: ID={en_info['id']}, URL: {WP_SITE_URL}/en/company/{existing_slug}/")
        
        elif ticker in error_codes and not is_in_wordpress:
            # 条件3: スルー
            stats['skipped'] += 1
            # 静かにスキップ（ログ出力なし）
        
        elif ticker in error_codes and is_in_wordpress:
            # 条件4: 下書き化（オプション）
            if auto_unpublish:
                post_id = existing_companies[ticker]['id']
                prefix = "[Dry Run] 下書き化予定" if dry_run else "[下書き]"
                print(f"\n{prefix}: {company_name} ({ticker})")
                
                if unpublish_company(post_id, dry_run=dry_run):
                    stats['unpublished'] += 1
                    if not dry_run:
                        print(f"   ✅ 下書き化成功")
                else:
                    stats['failed'] += 1
                    if not dry_run:
                        print(f"   ❌ 下書き化失敗")
            else:
                stats['skipped'] += 1
                print(f"\n[スキップ] {company_name} ({ticker}) - yfinanceエラー（手動確認推奨）")
        
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
    print(f"下書き化: {stats['unpublished']}社")
    print(f"スキップ: {stats['skipped']}社")
    print(f"失敗: {stats['failed']}社")
    print("=" * 60)
    
    return stats

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
        help='yfinanceエラー企業を自動で下書き化（デフォルト: 無効）'
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
    
    args = parser.parse_args()
    
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
    print(f"自動下書き化: {'有効' if args.auto_unpublish else '無効'}")
    if args.update_only:
        print(f"既存のみ更新: 有効")
    if args.dry_run:
        print(f"Dry Run: 有効（実際には更新しません）")
    print()
    
    # 既存企業取得（日本語版）
    existing_companies = get_all_existing_companies(WP_URL)

    # 既存企業取得（英語版）- 将来の更新機能用
    existing_companies_en = get_all_existing_companies_en(WP_URL)

    # 処理実行
    stats = process_companies(
        integrated_csv=args.csv,
        errors_csv=args.errors,
        existing_companies=existing_companies,
        limit=args.limit,
        skip=args.skip,
        create_status=args.status,
        auto_unpublish=args.auto_unpublish,
        dry_run=args.dry_run,
        update_only=args.update_only,
        existing_companies_en=existing_companies_en
    )
    
    print("\n✅ スクリプト実行完了")


if __name__ == "__main__":
    main()