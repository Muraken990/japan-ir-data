#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
3_image_generator.py

HTMLファイルからPlaywrightで高品質PNG画像を生成

使用方法:
    python 3_image_generator.py test_takeda.html -o output.png

必要なライブラリ:
    pip install playwright
    playwright install chromium
"""

import argparse
import sys
import os
from datetime import datetime

try:
    from playwright.sync_api import sync_playwright
except ImportError as e:
    print(f"❌ 必要なライブラリがインストールされていません: {e}")
    print("以下のコマンドでインストールしてください:")
    print("pip install playwright")
    print("playwright install chromium")
    sys.exit(1)


# ============================================================
# 画像生成クラス
# ============================================================

class ImageGenerator:
    """
    PlaywrightでHTMLを画像に変換するクラス
    """
    
    def __init__(self, width=1200, height=675, scale=2):
        """
        初期化
        
        Args:
            width: 画像の幅（デフォルト: 1200px）
            height: 画像の高さ（デフォルト: 675px）
            scale: デバイススケール（デフォルト: 2 = Retina品質）
        """
        self.width = width
        self.height = height
        self.scale = scale
    
    def generate_image(self, html_file_path, output_image_path=None):
        """
        HTMLファイルから画像を生成
        
        Args:
            html_file_path: HTMLファイルのパス
            output_image_path: 出力画像のパス（省略時は自動生成）
        
        Returns:
            str: 生成された画像のパス
        """
        
        # 出力ファイルパス
        if output_image_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_image_path = f"japan_ir_image_{timestamp}.png"
        
        # HTMLファイルの絶対パス
        html_absolute_path = os.path.abspath(html_file_path)
        
        if not os.path.exists(html_absolute_path):
            raise FileNotFoundError(f"HTMLファイルが見つかりません: {html_absolute_path}")
        
        file_url = f"file://{html_absolute_path}"
        
        print(f"HTMLファイル: {html_file_path}")
        print(f"出力画像: {output_image_path}")
        print(f"サイズ: {self.width}x{self.height}px (Scale: {self.scale}x)")
        print("")
        print("Playwrightでレンダリング中...")
        
        try:
            with sync_playwright() as p:
                # Chromiumブラウザ起動
                browser = p.chromium.launch(headless=True)
                
                # ページ作成
                page = browser.new_page(
                    viewport={'width': self.width, 'height': self.height},
                    device_scale_factor=self.scale
                )
                
                # HTMLを読み込み
                page.goto(file_url, wait_until='networkidle')
                
                # フォント読み込み待機（Google Fonts）
                page.wait_for_timeout(2000)
                
                # .tweet-card 要素を正確に切り取り
                tweet_card = page.locator('.tweet-card')
                
                # 要素のバウンディングボックスを取得
                box = tweet_card.bounding_box()
                
                if box:
                    # 正確に1200x675pxで切り取り
                    page.screenshot(
                        path=output_image_path,
                        clip={
                            'x': box['x'],
                            'y': box['y'],
                            'width': self.width,
                            'height': self.height
                        }
                    )
                else:
                    # フォールバック: 要素全体
                    tweet_card.screenshot(path=output_image_path)
                
                browser.close()
            
            # ファイルサイズ確認
            if os.path.exists(output_image_path):
                file_size = os.path.getsize(output_image_path) / 1024
                print(f"✅ 画像生成完了: {output_image_path}")
                print(f"ファイルサイズ: {file_size:.1f} KB")
                return output_image_path
            else:
                print(f"❌ 画像生成失敗")
                return None
        
        except Exception as e:
            print(f"❌ エラーが発生しました: {e}")
            raise


# ============================================================
# メイン処理
# ============================================================

def main(html_file_path, output_image_path=None, width=1200, height=675, scale=2):
    """
    メイン処理
    
    Args:
        html_file_path: HTMLファイルのパス
        output_image_path: 出力画像のパス
        width: 画像の幅
        height: 画像の高さ
        scale: デバイススケール
    
    Returns:
        str: 生成された画像のパス
    """
    print("=" * 60)
    print("🚀 画像生成処理開始")
    print("=" * 60)
    print("")
    
    generator = ImageGenerator(width=width, height=height, scale=scale)
    image_path = generator.generate_image(html_file_path, output_image_path)
    
    if image_path:
        print("")
        print("=" * 60)
        print("✅ 画像生成完了")
        print("=" * 60)
        print(f"ファイル: {image_path}")
        print("")
        print("次のステップ:")
        print(f"  ブラウザで確認、またはWordPressにアップロード")
        
        return image_path
    else:
        print("")
        print("=" * 60)
        print("❌ 画像生成失敗")
        print("=" * 60)
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='HTMLファイルから高品質PNG画像を生成（Playwright使用）'
    )
    parser.add_argument(
        'html_file',
        help='HTMLファイルのパス'
    )
    parser.add_argument(
        '-o', '--output',
        help='出力画像のパス（省略時は自動生成）'
    )
    parser.add_argument(
        '-w', '--width',
        type=int,
        default=1200,
        help='画像の幅（デフォルト: 1200）'
    )
    parser.add_argument(
        '-H', '--height',
        type=int,
        default=675,
        help='画像の高さ（デフォルト: 675）'
    )
    parser.add_argument(
        '-s', '--scale',
        type=int,
        default=2,
        help='デバイススケール（デフォルト: 2 = Retina品質）'
    )
    
    args = parser.parse_args()
    
    # メイン処理実行
    image_path = main(
        args.html_file,
        args.output,
        args.width,
        args.height,
        args.scale
    )
    
    sys.exit(0 if image_path else 1)