#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
5_IR_ImageGenerator.py

WordPress APIからIR情報取得 → OpenAI要約 → HTML生成 → 画像生成
全自動で画像ファイルを生成

使用方法:
    python 5_IR_ImageGenerator.py --date 20251220 --time-start 08:00 --time-end 20:00

必要なライブラリ:
    pip install requests openai python-dateutil jinja2 playwright
    playwright install chromium
"""

import argparse
import sys
import os
from datetime import datetime

# 既存スクリプトをインポート
try:
    import importlib.util
    
    # 1_ir_summarizer.pyをインポート
    spec = importlib.util.spec_from_file_location("ir_summarizer", "1_ir_summarizer.py")
    ir_summarizer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ir_summarizer)
    
    # 2_html_generator.pyをインポート
    spec = importlib.util.spec_from_file_location("html_generator", "2_html_generator.py")
    html_generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(html_generator)
    
    # 3_image_generator.pyをインポート
    spec = importlib.util.spec_from_file_location("image_generator", "3_image_generator.py")
    image_generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(image_generator)
    
except Exception as e:
    print(f"❌ スクリプトの読み込みに失敗: {e}")
    print("1_ir_summarizer.py, 2_html_generator.py, 3_image_generator.py が同じディレクトリにあることを確認してください")
    sys.exit(1)


# ============================================================
# メイン処理
# ============================================================

def main(date_str, time_start, time_end, output_image_path=None, scale=2, keep_html=False):
    """
    全自動画像生成メイン処理
    
    Args:
        date_str: 日付（YYYYMMDD）
        time_start: 開始時刻（HH:MM）
        time_end: 終了時刻（HH:MM）
        output_image_path: 出力画像パス（省略時は自動生成）
        scale: デバイススケール（デフォルト: 2）
        keep_html: HTMLファイルを保持するか（デフォルト: False）
    
    Returns:
        str: 生成された画像のパス
    """
    print("=" * 70)
    print("🚀 IR画像自動生成処理開始")
    print("=" * 70)
    print(f"日付: {date_str}")
    print(f"時刻範囲: {time_start} - {time_end}")
    print(f"画像スケール: {scale}x")
    print("")
    
    # ============================================================
    # ステップ1: WordPress API + OpenAI要約
    # ============================================================
    print("📝 ステップ1/3: IR情報取得 + OpenAI要約")
    print("-" * 70)
    
    ir_list = ir_summarizer.main(date_str, time_start, time_end)
    
    if len(ir_list) == 0:
        print("ℹ️ 該当するIR情報がありませんでした")
        return None
    
    print("")
    
    # ============================================================
    # ステップ2: HTML生成
    # ============================================================
    print("📝 ステップ2/3: HTMLテンプレートに挿入")
    print("-" * 70)
    
    # 一時HTMLファイル
    temp_html_path = f"temp_japan_ir_{date_str}.html"
    
    generator = html_generator.HTMLGenerator()
    html_path = generator.generate_html(ir_list, date_str, temp_html_path)
    
    print("")
    
    # ============================================================
    # ステップ3: 画像生成
    # ============================================================
    print("📝 ステップ3/3: Playwrightで画像生成")
    print("-" * 70)
    
    # 出力画像パス
    if output_image_path is None:
        output_image_path = f"japan_ir_highlights_{date_str}.png"
    
    img_generator = image_generator.ImageGenerator(
        width=1200,
        height=675,
        scale=scale
    )
    
    image_path = img_generator.generate_image(html_path, output_image_path)
    
    # HTMLファイル削除（keep_html=Falseの場合）
    if not keep_html and os.path.exists(html_path):
        os.remove(html_path)
        print(f"🗑️  一時HTMLファイル削除: {html_path}")
    
    print("")
    print("=" * 70)
    print("✅ 全処理完了")
    print("=" * 70)
    print(f"生成画像: {image_path}")
    print("")
    
    if image_path and os.path.exists(image_path):
        file_size = os.path.getsize(image_path) / 1024
        print(f"📊 画像情報:")
        print(f"  ファイル名: {os.path.basename(image_path)}")
        print(f"  サイズ: 1200x{675}px")
        print(f"  スケール: {scale}x")
        print(f"  ファイルサイズ: {file_size:.1f} KB")
        print("")
        print("次のステップ:")
        print("  1. 画像を確認")
        print("  2. WordPressにアップロード（Phase 6）")
        print("  3. X投稿（IR-JsonToX.py）")
    
    return image_path


# ============================================================
# エントリーポイント
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='IR情報から画像を全自動生成（WordPress API → OpenAI → HTML → 画像）'
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
        help='出力画像のパス（省略時は自動生成）'
    )
    parser.add_argument(
        '-s', '--scale',
        type=int,
        default=2,
        help='デバイススケール（デフォルト: 2）'
    )
    parser.add_argument(
        '--keep-html',
        action='store_true',
        help='HTMLファイルを保持する（デバッグ用）'
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
    
    # OpenAI API Key チェック
    if not os.getenv('OPENAI_API_KEY'):
        print("❌ エラー: OPENAI_API_KEY 環境変数が設定されていません")
        print("export OPENAI_API_KEY='your-api-key-here'")
        sys.exit(1)
    
    # メイン処理実行
    image_path = main(
        args.date,
        args.time_start,
        args.time_end,
        args.output,
        args.scale,
        args.keep_html
    )
    
    sys.exit(0 if image_path else 1)