"""
WordPress企業データ更新スクリプト
既存の100社のmarketCapとDATEを更新
"""

import pandas as pd
import requests
import base64
import time
import os

WP_SITE_URL = "https://japanir.jp"
WP_USER = os.environ.get("WP_USER")
WP_PASSWORD = os.environ.get("WP_PASSWORD")

def get_auth_headers():
    credentials = f"{WP_USER}:{WP_PASSWORD}"
    token = base64.b64encode(credentials.encode()).decode('utf-8')
    return {
        'Authorization': f'Basic {token}',
        'Content-Type': 'application/json'
    }

def get_existing_companies():
    """WordPressから既存の企業一覧を取得"""
    headers = get_auth_headers()
    companies = []
    page = 1
    
    while True:
        url = f"{WP_SITE_URL}/wp-json/wp/v2/company?per_page=100&page={page}"
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            break
            
        data = response.json()
        if not data:
            break
            
        for company in data:
            ticker = company.get('meta', {}).get('Ticker', '')
            if ticker:
                companies.append({
                    'post_id': company['id'],
                    'ticker': str(ticker).replace('.T', ''),
                    'title': company['title']['rendered']
                })
        
        page += 1
        time.sleep(0.5)
    
    return companies

def update_company(post_id, market_cap, date):
    """企業のmarketCapとDATEを更新"""
    headers = get_auth_headers()
    url = f"{WP_SITE_URL}/wp-json/wp/v2/company/{post_id}"
    
    data = {
        'meta': {
            'marketCap': market_cap,
            'DATE': date
        }
    }
    
    response = requests.post(url, headers=headers, json=data)
    return response.status_code == 200

def main():
    print("=" * 60)
    print("WordPress企業データ更新")
    print("=" * 60)
    
    # CSVファイルを探す
    csv_files = [f for f in os.listdir('output') if f.startswith('yfinance_all_fields_') and f.endswith('.csv')]
    if not csv_files:
        print("エラー: output/yfinance_all_fields_*.csv が見つかりません")
        return
    
    csv_file = f"output/{sorted(csv_files)[-1]}"
    print(f"CSVファイル: {csv_file}")
    
    # CSV読み込み
    df = pd.read_csv(csv_file)
    df['code'] = df['code'].astype(str)
    print(f"CSV総行数: {len(df)}")
    
    # 既存企業を取得
    print("\nWordPressから既存企業を取得中...")
    existing = get_existing_companies()
    print(f"既存企業数: {len(existing)}")
    
    # 更新処理
    updated = 0
    failed = 0
    not_found = 0
    
    for company in existing:
        ticker = company['ticker']
        post_id = company['post_id']
        title = company['title']
        
        # CSVから該当データを検索
        row = df[df['code'] == ticker]
        
        if row.empty:
            print(f"  スキップ: {title} ({ticker}) - CSVにデータなし")
            not_found += 1
            continue
        
        row = row.iloc[0]
        market_cap = row.get('marketCap')
        date = row.get('scrape_date', '')
        
        # None/NaNチェック
        if pd.isna(market_cap):
            market_cap = 0
        else:
            market_cap = int(market_cap)
        
        print(f"  更新中: {title} ({ticker}) - marketCap: {market_cap:,}")
        
        if update_company(post_id, market_cap, date):
            updated += 1
        else:
            print(f"    失敗")
            failed += 1
        
        time.sleep(0.5)
    
    # 結果表示
    print("\n" + "=" * 60)
    print("更新結果")
    print("=" * 60)
    print(f"更新成功: {updated}")
    print(f"更新失敗: {failed}")
    print(f"CSVにデータなし: {not_found}")
    print("=" * 60)

if __name__ == "__main__":
    main()
