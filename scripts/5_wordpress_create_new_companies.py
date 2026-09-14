#!/usr/bin/env python3
"""
WordPress企業データ スマート更新スクリプト
条件分岐 + 段階的実行対応 + Dry Run機能 + Update Only機能

条件1: Yahoo○ + yfinance○ + WordPress× → 新規作成
条件2: Yahoo○ + yfinance○ + WordPress○ → 更新
条件3: Yahoo○ + yfinance× + WordPress× → スルー
条件4: Yahoo○ + yfinance× + WordPress○ → スキップ（手動確認推奨）

通常の新規作成・更新経路では下書き化を行わない。--auto-unpublishは明示指定時のみの
旧来のオプションであり、workflowからは指定しない。
"""

import pandas as pd
import requests
import base64
import time
import os
import re
import sys
import argparse
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
# リストはdata/配下の固定パスに置く（4_wordpress_smart_update.pyと共有）
DEFAULT_CONFIRMED_DELISTED_CSV = 'data/confirmed_delisted_companies.csv'

# 処理速度（秒）
REQUEST_DELAY = float(os.getenv('REQUEST_DELAY', '0.5'))

# WPMLで運用している言語（CLAUDE.md準拠、日本語/英語のみ）
WPML_LANGUAGES = ['ja', 'en']

# 確認済み上場廃止リストの必須列（4_wordpress_smart_update.pyと同一仕様）
REQUIRED_DELISTED_COLUMNS = ["code", "delisting_date", "market", "official_url", "confirmed_date"]
_DELISTED_CODE_PATTERN = re.compile(r"^[0-9A-Z]{4}$")
_DATE_FORMAT = "%Y-%m-%d"
JST = timezone(timedelta(hours=9))

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
#
# WP REST APIの一覧エンドポイントはcontext=edit・statusフィルタ指定時でも
# レスポンスに'status'フィールドを含めない（本番REST APIで確認済み）。そのため
# ステータス値ごとに個別クエリを発行し、「どのクエリでpost_idが返ってきたか」で
# statusを判定する。また、statusを渡さない場合はpublishのみ、langを渡さない場合は
# 日本語のみがデフォルトで返る。この2つの暗黙のデフォルトが、下書き化済みの
# 上場廃止企業を「存在しない」と誤判定させ、重複投稿の自動作成を引き起こした
# （4_wordpress_smart_update.pyで2026-09-14に発覚。同じ取得ロジックの不備が
# このスクリプトにも存在したため、同じ設計で修正する）。
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
                response = requests.get(
                    f"{wp_url}/wp-json/wp/v2/company",
                    params=params,
                    headers=headers,
                    timeout=30
                )
            except Exception as e:
                print(f"❌ WordPress API取得中に例外が発生しました"
                      f"（status={status}, lang={lang}, offset={offset}）: {e}")
                print("   不完全なデータで新規作成の判定を行うのは危険なため中止します。")
                sys.exit(1)

            if response.status_code != 200:
                print(f"❌ WordPress APIエラー（status={status}, lang={lang}, offset={offset}）: "
                      f"HTTP {response.status_code}")
                print("   不完全なデータで新規作成の判定を行うのは危険なため中止します。")
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


# ============================================================
# 確認済み上場廃止リストの読み込み
#
# 新規作成の除外判定にのみ使う。yfinance_errors（通信障害・レート制限・解析エラー等）
# は上場廃止を意味しないため、この判定には一切使わない。
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
        print("❌ 確認済み上場廃止リストのパスが指定されていません")
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
        response = requests.post(url, headers=headers, json=data, timeout=30)
    except Exception as e:
        print(f"   エラー詳細: {str(e)}")
        import traceback
        traceback.print_exc()
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
# メイン処理
# ============================================================

def process_companies(integrated_csv, errors_csv, existing_companies,
                     confirmed_delisted_codes,
                     limit=None, skip=0, create_status='publish',
                     auto_unpublish=False, dry_run=False, update_only=False):
    """条件分岐処理

    existing_companies: get_existing_companies_all_langs()の戻り値
        {code: {lang: [{'id','slug','status'}, ...], ...}}
    confirmed_delisted_codes: 確認済み上場廃止コードの集合（新規作成対象から除外）
    """
    
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
        'failed': 0,
        'pending_no_english_name': 0,
        'unclassified': 0,
        'needs_repair': 0,
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
        has_price_or_market_cap = has_price or has_market_cap

        # 英語名があり、かつ株価または時価総額がある
        has_yfinance_data = has_name and has_price_or_market_cap

        # WordPress登録済みか（言語×ステータスを問わず全投稿を保持するリスト構造）
        lang_entries = existing_companies.get(ticker, {})
        ja_posts = lang_entries.get('ja', [])
        en_posts = lang_entries.get('en', [])
        is_fully_new = not ja_posts and not en_posts

        # 条件分岐
        if ticker in confirmed_delisted_codes:
            # 確認済み上場廃止リストに含まれる場合は、既存有無に関わらず新規作成しない
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
                    print(f"   ✅ 作成成功")
            else:
                stats['failed'] += 1
                if not dry_run:
                    print(f"   ❌ 作成失敗")

        elif has_yfinance_data and not ja_posts and en_posts:
            # EN投稿のみ存在しJA投稿がない状態。自動作成すると重複の原因になるため
            # 自動作成せず、要修復として報告する。
            stats['needs_repair'] += 1
            en_ids = [p['id'] for p in en_posts]
            print(f"\n[要修復] {company_name} ({ticker}) - EN投稿のみ存在しJA投稿がありません"
                  f"（自動作成しません、手動確認が必要）: en post_id={en_ids}")

        elif has_yfinance_data and ja_posts:
            if len(ja_posts) > 1:
                # 同一コード・同一言語に複数投稿（重複投稿）。誤ってどちらか一方を
                # 選んで処理すると重複が温存/悪化するため、自動処理せず報告する。
                stats['needs_repair'] += 1
                ja_ids = [p['id'] for p in ja_posts]
                print(f"\n[要修復] {company_name} ({ticker}) - JA投稿が{len(ja_posts)}件重複しています"
                      f"（手動確認が必要）: post_id={ja_ids}")
            else:
                # 条件2: 既存企業はスキップ（更新しない）
                stats['skipped'] += 1
                post_id = ja_posts[0]['id']
                existing_slug = ja_posts[0].get('slug', ticker)

                print(f"\n[スキップ] {company_name} ({ticker})")
                print(f"   📍 日本語版: ID={post_id}, URL: {WP_SITE_URL}/company/{existing_slug}/")
                if en_posts:
                    en_ids = [p['id'] for p in en_posts]
                    print(f"   🌐 英語版: ID={en_ids}, URL: {WP_SITE_URL}/en/company/{existing_slug}/")

        elif ticker in error_codes and is_fully_new:
            # 条件3: スルー
            stats['skipped'] += 1
            # 静かにスキップ（ログ出力なし）

        elif ticker in error_codes and not is_fully_new:
            # 条件4: 下書き化（オプション、yfinanceエラーに基づく既存の仕組み。
            # 確認済み上場廃止リストとは別軸のため変更しない）
            if auto_unpublish:
                if not ja_posts:
                    # JA投稿がない（EN投稿のみ）場合は自動下書き化の対象外とし、報告する
                    stats['needs_repair'] += 1
                    en_ids = [p['id'] for p in en_posts]
                    print(f"\n[要修復] {company_name} ({ticker}) - yfinanceエラーだがJA投稿がなく"
                          f"自動下書き化できません（手動確認が必要）: en post_id={en_ids}")
                else:
                    for post in ja_posts:
                        post_id = post['id']
                        prefix = "[Dry Run] 下書き化予定" if dry_run else "[下書き]"
                        print(f"\n{prefix}: {company_name} ({ticker}) post_id={post_id}")

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

        elif has_price_or_market_cap and not has_name and ticker not in error_codes:
            # 株価・時価総額は取得できているが英語社名（company_name_en/short_name_en）が
            # 未取得のケース。has_yfinance_dataがFalseになりどの条件にも合致しないため、
            # 以前はここで無言スキップになっていた（新規上場直後でyfinance側の英語名が
            # まだ反映されていない場合などに発生。例: 618A）。誤って自動作成せず、
            # 明示的に保留として報告する。
            stats['pending_no_english_name'] += 1
            if not is_fully_new:
                print(f"\n[保留] {company_name} ({ticker}) - 英語社名未取得のため更新を保留（既存WordPress登録あり）")
            else:
                print(f"\n[保留] {company_name} ({ticker}) - 英語社名未取得のため新規作成を保留（手動確認推奨）")

        else:
            # 上記いずれの条件にも該当しない未想定パターン。無言で処理から漏らさず必ず報告する。
            stats['unclassified'] += 1
            print(f"\n[未分類] {company_name} ({ticker}) - どの条件にも該当しませんでした（要調査）")

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
    print(f"保留（英語社名未取得）: {stats['pending_no_english_name']}社")
    print(f"要修復（片言語欠落/重複投稿）: {stats['needs_repair']}社")
    print(f"未分類（要調査）: {stats['unclassified']}社")
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

    parser.add_argument(
        '--confirmed-delisted-csv',
        default=DEFAULT_CONFIRMED_DELISTED_CSV,
        help=f'公式確認済み上場廃止リストCSV（新規作成対象から除外、デフォルト: {DEFAULT_CONFIRMED_DELISTED_CSV}）'
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
    
    # 確認済み上場廃止リスト読み込み（新規作成対象から除外。通常更新と共有する固定パス）
    today = get_jst_today()
    confirmed_delisted_records = load_confirmed_delisted_csv(args.confirmed_delisted_csv, today)
    confirmed_delisted_codes = set(confirmed_delisted_records.keys())
    print(f"確認済み上場廃止コード: {len(confirmed_delisted_codes)}件（新規作成対象から除外）\n")

    # 既存企業取得（全言語・全ステータス、重複投稿も保持）
    existing_companies = get_existing_companies_all_langs(WP_URL, WPML_LANGUAGES)

    # 処理実行
    stats = process_companies(
        integrated_csv=args.csv,
        errors_csv=args.errors,
        existing_companies=existing_companies,
        confirmed_delisted_codes=confirmed_delisted_codes,
        limit=args.limit,
        skip=args.skip,
        create_status=args.status,
        auto_unpublish=args.auto_unpublish,
        dry_run=args.dry_run,
        update_only=args.update_only,
    )
    
    print("\n✅ スクリプト実行完了")


if __name__ == "__main__":
    main()
