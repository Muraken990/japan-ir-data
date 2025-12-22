#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
2_html_generator.py

1_ir_summarizer.pyの結果を受け取り、HTMLテンプレートに挿入
IR-JsonToX.pyと同じ引数形式

使用方法:
    python 2_html_generator.py --date 20251220 --time-start 08:00 --time-end 20:00

必要なライブラリ:
    pip install jinja2 requests openai python-dateutil
"""

import argparse
import sys
import os
from datetime import datetime
from jinja2 import Template

# 1_ir_summarizer.pyをインポート
try:
    # カレントディレクトリから1_ir_summarizer.pyをインポート
    import importlib.util
    spec = importlib.util.spec_from_file_location("ir_summarizer", "1_ir_summarizer.py")
    ir_summarizer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ir_summarizer)
except Exception as e:
    print(f"❌ 1_ir_summarizer.pyの読み込みに失敗: {e}")
    print("1_ir_summarizer.pyが同じディレクトリにあることを確認してください")
    sys.exit(1)


# ============================================================
# カテゴリ表示名マッピング
# ============================================================

CATEGORY_DISPLAY = {
    "tender_offer": "Tender Offer",
    "m_and_a_alliance": "M&A",
    "share_buyback": "Share Buyback",
    "dividend": "Dividend",
    "financial_summary": "Earnings",
    "business_update": "Business Update",
    "earnings_guidance": "Earnings Guidance",
    "capital_policy": "Capital Policy",
    "share_cancellation": "Share Cancellation",
    "corporate_restructuring": "Restructuring",
    "product_announcement": "Product",
    "executive_change": "Personnel",
    "sales_update": "Sales Update",
    "esg_sustainability": "ESG",
    "stock_option": "Stock Option",
    "disclosure_update": "Disclosure",
    "general_ir": "Other"
}


# ============================================================
# HTML生成クラス
# ============================================================

class HTMLGenerator:
    """
    IR情報をHTMLテンプレートに挿入するクラス
    """
    
    def __init__(self, template_path='japan_ir_highlights_template.html'):
        """
        初期化
        
        Args:
            template_path: HTMLテンプレートファイルのパス
        """
        self.template_path = template_path
        
        if not os.path.exists(template_path):
            raise FileNotFoundError(f"テンプレートファイルが見つかりません: {template_path}")
        
        # テンプレート読み込み
        with open(template_path, 'r', encoding='utf-8') as f:
            self.template_content = f.read()
        
        self.template = Template(self.template_content)
    
    def generate_html(self, ir_list, date_str, output_path=None):
        """
        IR情報リストからHTMLを生成
        
        Args:
            ir_list: IR情報のリスト（1_ir_summarizer.pyから取得）
            date_str: 日付（YYYYMMDD形式）
            output_path: 出力HTMLファイルパス（省略時は自動生成）
        
        Returns:
            str: 生成されたHTMLファイルのパス
        """
        
        # 日付フォーマット: December 19, 2025
        date_obj = datetime.strptime(date_str, '%Y%m%d')
        formatted_date = date_obj.strftime('%B %d, %Y')
        
        # IR情報を整形
        formatted_ir_list = []
        for ir in ir_list:
            # デバッグ: one_sentence_summaryの確認
            print(f"🔍 デバッグ: {ir['company_name']}")
            print(f"   one_sentence_summary: {ir.get('one_sentence_summary', 'なし')[:80]}...")
            
            formatted_ir = {
                'company_name': ir['company_name'],
                'stock_code': ir['stock_code'],
                'ir_type': ir['ir_type'],
                'category_display': CATEGORY_DISPLAY.get(ir['ir_type'], 'Other'),
                'summary': ir.get('one_sentence_summary', ir['short_summary'])
            }
            formatted_ir_list.append(formatted_ir)
        
        # HTMLレンダリング
        html_output = self.template.render(
            date=formatted_date,
            ir_list=formatted_ir_list
        )
        
        # 出力ファイルパス
        if output_path is None:
            output_path = f'japan_ir_highlights_{date_str}.html'
        
        # HTMLファイル保存
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_output)
        
        print(f"✅ HTML生成完了: {output_path}")
        
        return output_path


# ============================================================
# メイン処理
# ============================================================

def main(date_str, time_start, time_end, output_path=None):
    """
    メイン処理
    
    Args:
        date_str: 日付（YYYYMMDD）
        time_start: 開始時刻（HH:MM）
        time_end: 終了時刻（HH:MM）
        output_path: 出力HTMLファイルパス
    
    Returns:
        str: 生成されたHTMLファイルのパス
    """
    print("=" * 60)
    print("🚀 HTML生成処理開始")
    print("=" * 60)
    print(f"日付: {date_str}")
    print(f"時刻範囲: {time_start} - {time_end}")
    print("")
    
    # ステップ1: 1_ir_summarizer.pyでIR情報取得 + 要約
    print("📝 ステップ1: IR情報取得 + OpenAI要約")
    print("-" * 60)
    
    ir_list = ir_summarizer.main(date_str, time_start, time_end)
    
    if len(ir_list) == 0:
        print("ℹ️ 該当するIR情報がありませんでした")
        return None
    
    print("")
    
    # ステップ2: HTMLテンプレートに挿入
    print("📝 ステップ2: HTMLテンプレートに挿入")
    print("-" * 60)
    
    generator = HTMLGenerator()
    html_path = generator.generate_html(ir_list, date_str, output_path)
    
    print("")
    print("=" * 60)
    print("✅ HTML生成完了")
    print("=" * 60)
    print(f"ファイル: {html_path}")
    print("")
    print("次のステップ:")
    print(f"  python 3_image_generator.py {html_path}")
    
    return html_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='IR情報からHTML生成（1_ir_summarizer.py → HTMLテンプレート）'
    )
    parser.add_argument(
        '--date',
        required=True,
        help='日付（YYYYMMDD形式）例: 20251220'
    )
    parser.add_argument(
        '--time-start',
        required=True,
        help='開始時刻（HH:MM形式）例: 08:00'
    )
    parser.add_argument(
        '--time-end',
        required=True,
        help='終了時刻（HH:MM形式）例: 20:00'
    )
    parser.add_argument(
        '-o', '--output',
        help='出力HTMLファイルパス（省略時は自動生成）'
    )
    
    args = parser.parse_args()
    
    # 引数バリデーション
    try:
        datetime.strptime(args.date, '%Y%m%d')
        datetime.strptime(args.time_start, '%H:%M')
        datetime.strptime(args.time_end, '%H:%M')
    except ValueError as e:
        print(f"❌ 引数のフォーマットエラー: {e}")
        sys.exit(1)
    
    # メイン処理実行
    html_path = main(
        args.date,
        args.time_start,
        args.time_end,
        args.output
    )
    
    sys.exit(0 if html_path else 1)