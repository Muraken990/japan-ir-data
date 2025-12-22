#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ir_summarizer.py

WordPress REST APIからIR情報を取得し、OpenAI APIで1文要約
IR-JsonToX.pyと同じ引数・処理フローに対応

使用方法:
    python ir_summarizer.py --date 20251215 --time-start 08:00 --time-end 12:00

必要なライブラリ:
    pip install requests openai python-dateutil
"""

import argparse
import sys
import os
import re
from datetime import datetime

try:
    import requests
    from openai import OpenAI
    from dateutil import parser as dateutil_parser
except ImportError as e:
    print(f"❌ 必要なライブラリがインストールされていません: {e}")
    print("以下のコマンドでインストールしてください:")
    print("pip install requests openai python-dateutil")
    sys.exit(1)


# ============================================================
# 定数定義
# ============================================================

# WordPress REST API設定
WORDPRESS_API_BASE = "https://japanir.jp/wp-json/wp/v2"
WORDPRESS_ENDPOINT = f"{WORDPRESS_API_BASE}/ir-release"

# カテゴリ優先順位マッピング（IR-JsonToX.pyと同じ）
CATEGORY_PRIORITY = {
    "tender_offer": 1,
    "m_and_a_alliance": 2,
    "financial_summary": 3,
    "business_update": 4,
    "earnings_guidance": 5,
    "dividend": 6,
    "share_buyback": 7,
    "capital_policy": 8,
    "share_cancellation": 9,
    "corporate_restructuring": 10,
    "product_announcement": 11,
    "executive_change": 12,
    "sales_update": 13,
    "esg_sustainability": 14,
    "stock_option": 15,
    "disclosure_update": 16,
    "general_ir": 17
}


# ============================================================
# ユーティリティ関数（IR-JsonToX.pyと同じ）
# ============================================================

def get_importance_stars(importance_str):
    if not importance_str:
        return 0
    match = re.search(r'[★☆â˜…](\d+)', str(importance_str))
    if match:
        return int(match.group(1))
    return 0


def get_category_priority(ir_type):
    return CATEGORY_PRIORITY.get(ir_type, 99)


def format_datetime_for_api(date_str, time_str):
    date_obj = datetime.strptime(date_str, '%Y%m%d')
    time_obj = datetime.strptime(time_str, '%H:%M').time()
    dt = datetime.combine(date_obj, time_obj)
    return dt.isoformat()


# ============================================================
# WordPress REST API取得（IR-JsonToX.pyと同じ）
# ============================================================

class WordPressIRFetcher:
    def __init__(self, base_url=WORDPRESS_ENDPOINT):
        self.base_url = base_url
    
    def fetch_irs(self, date_str, time_start, time_end, per_page=100):
        after = format_datetime_for_api(date_str, time_start)
        before = format_datetime_for_api(date_str, time_end)
        
        params = {
            'per_page': per_page,
            'after': after,
            'before': before,
            'status': 'publish',
            'orderby': 'date',
            'order': 'asc',
            'lang': 'en'
        }
        
        try:
            print(f"📡 WordPress APIからデータ取得中...")
            print(f"   時刻範囲: {time_start} - {time_end}")
            
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            print(f"✅ {len(data)}件のIR情報を取得しました")
            
            return data
        
        except requests.exceptions.RequestException as e:
            print(f"❌ WordPress API エラー: {e}")
            return []


class IRDataProcessor:
    def extract_ir_info(self, wp_post):
        meta = wp_post.get('meta', {})
        return {
            'id': wp_post.get('id'),
            'date': wp_post.get('date'),
            'stock_code': meta.get('jir_stock_code', ''),
            'company_name': meta.get('jir_company_name', ''),
            'ir_type': meta.get('jir_ir_type', ''),
            'importance': meta.get('jir_importance', ''),
            'short_summary': meta.get('jir_short_summary', ''),
            'link': wp_post.get('link', '')
        }
    
    def sort_by_priority(self, ir_list):
        return sorted(ir_list, key=lambda x: (
            get_category_priority(x['ir_type']),
            -get_importance_stars(x['importance'])
        ))
    
    def remove_duplicate_companies(self, ir_list):
        seen_companies = set()
        result = []
        for ir in ir_list:
            stock_code = ir['stock_code']
            if stock_code and stock_code not in seen_companies:
                seen_companies.add(stock_code)
                result.append(ir)
        if len(ir_list) != len(result):
            print(f"🔄 重複排除: {len(ir_list)}件 → {len(result)}件")
        return result
    
    def select_top_n(self, ir_list, n=5):
        if len(ir_list) >= n:
            print(f"🎯 Top {n}選定: {len(ir_list)}件から{n}件を選定")
            return ir_list[:n]
        print(f"🎯 Top {n}選定: {len(ir_list)}件（全件採用）")
        return ir_list


# ============================================================
# OpenAI要約クラス
# ============================================================

class IRSummarizer:
    def __init__(self, api_key=None):
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        if not self.api_key:
            raise ValueError("OpenAI API Keyが設定されていません")
        self.client = OpenAI(api_key=self.api_key)
    
    def summarize_to_one_sentence(self, ir_data):
        company_name = ir_data.get('company_name', '')
        ir_type = ir_data.get('ir_type', '').replace('_', ' ').title()
        summary = ir_data.get('short_summary', '')
        
        prompt = f"""Summarize this Japanese corporate IR announcement into ONE concise sentence.

Requirements:
- Maximum 20 words
- Focus on KEY NUMBERS and ACTIONS
- Use <span class="bold">...</span> for important terms and numbers
- Be specific and quantitative
- Remove company name (already displayed separately)

Company: {company_name}
Type: {ir_type}
Original Summary: {summary}

Format examples:
- "Completed <span class="bold">treasury stock tender offer</span> acquiring <span class="bold">24.9M shares</span> at <span class="bold">¥5,220</span>/share."
- "Acquiring <span class="bold">41%</span> of <span class="bold">Peanuts Holdings</span> from WildBrain, total stake to <span class="bold">80%</span>."

Guidelines for bold tags:
- Numbers with units (shares, yen, percentages)
- Key financial terms (buyback, tender offer, acquisition)
- Company/entity names being acquired
- Important positions (Chairman, President)

ONE SENTENCE SUMMARY:"""

        try:
            response = self.client.chat.completions.create(
                model="gpt-4.1-mini",
                messages=[
                    {"role": "system", "content": "You are a financial news expert specializing in concise, number-focused corporate announcements. Always output valid HTML with proper span tags."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=100,
                temperature=0.3
            )
            
            summary_text = response.choices[0].message.content.strip()
            
            # バリデーション
            if summary_text.count('<span') != summary_text.count('</span>'):
                print(f"⚠️  警告: spanタグが不正 - {company_name}")
                summary_text = summary_text.replace('<span class="bold">', '').replace('</span>', '')
            
            return summary_text
        
        except Exception as e:
            print(f"❌ OpenAI API エラー ({company_name}): {e}")
            return summary


# ============================================================
# メイン処理
# ============================================================

def main(date_str, time_start, time_end):
    print("=" * 60)
    print("🚀 IR要約処理開始")
    print("=" * 60)
    print(f"日付: {date_str}")
    print(f"時刻範囲: {time_start} - {time_end}")
    print("")
    
    # ステップ1: WordPress APIからデータ取得
    fetcher = WordPressIRFetcher()
    wp_posts = fetcher.fetch_irs(date_str, time_start, time_end)
    
    if len(wp_posts) == 0:
        print("ℹ️ 該当するIR情報がありませんでした")
        return []
    
    # ステップ2: IR情報抽出
    processor = IRDataProcessor()
    ir_list = [processor.extract_ir_info(post) for post in wp_posts]
    
    print(f"📊 全件対象: {len(ir_list)}件")
    
    # ステップ3: ソート・重複排除・Top 5選定
    ir_list = processor.sort_by_priority(ir_list)
    ir_list = processor.remove_duplicate_companies(ir_list)
    ir_list = processor.select_top_n(ir_list, 5)
    
    # ステップ4: OpenAI要約
    print("")
    print("=" * 60)
    print("🤖 OpenAI APIで要約生成中...")
    print("=" * 60)
    
    summarizer = IRSummarizer()
    
    for i, ir in enumerate(ir_list, 1):
        print(f"\n[{i}/5] {ir['company_name']} ({ir['stock_code']})")
        print(f"元の要約: {ir['short_summary'][:80]}...")
        
        one_sentence = summarizer.summarize_to_one_sentence(ir)
        ir['one_sentence_summary'] = one_sentence
        
        print(f"1文要約: {one_sentence}")
    
    print("")
    print("=" * 60)
    print("✅ 要約完了")
    print("=" * 60)
    
    return ir_list


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='WordPress APIからIR情報取得→OpenAI要約')
    parser.add_argument('--date', required=True, help='日付（YYYYMMDD形式）例: 20251215')
    parser.add_argument('--time-start', required=True, help='開始時刻（HH:MM形式）例: 08:00')
    parser.add_argument('--time-end', required=True, help='終了時刻（HH:MM形式）例: 12:00')
    
    args = parser.parse_args()
    
    # バリデーション
    try:
        datetime.strptime(args.date, '%Y%m%d')
        datetime.strptime(args.time_start, '%H:%M')
        datetime.strptime(args.time_end, '%H:%M')
    except ValueError as e:
        print(f"❌ 引数のフォーマットエラー: {e}")
        sys.exit(1)
    
    # OpenAI API Key チェック
    if not os.getenv('OPENAI_API_KEY'):
        print("❌ エラー: OPENAI_API_KEY 環境変数が設定されていません")
        print("export OPENAI_API_KEY='your-api-key-here'")
        sys.exit(1)
    
    # メイン処理実行
    ir_list = main(args.date, args.time_start, args.time_end)