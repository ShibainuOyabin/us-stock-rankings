# 堅牢版：統合株価ランキングシステム
# エラー対策強化 + フォールバック機能充実

import pandas as pd
import yfinance as yf
import numpy as np
import json
from datetime import datetime, timedelta
import os
from collections import OrderedDict
import time
import requests
import sys
import warnings

# 警告を抑制
warnings.filterwarnings('ignore')

# 環境変数からAPI設定を取得
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY', 'QTLKGZU9EXK5OI3Y')
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS', 'false').lower() == 'true'
MAX_SYMBOLS = int(os.getenv('MAX_SYMBOLS', '30'))  # さらに制限

def get_fallback_symbols():
    """フォールバック用の主要銘柄リスト"""
    nasdaq_fallback = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'NFLX', 'ADBE', 'CRM',
        'ORCL', 'CSCO', 'INTC', 'AMD', 'QCOM', 'AVGO', 'TXN', 'COST', 'TMUS', 'CMCSA'
    ]
    
    sp500_fallback = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'UNH', 'JNJ', 'V', 'XOM', 'PG', 'JPM',
        'HD', 'CVX', 'MA', 'BAC', 'ABBV', 'PFE', 'WMT', 'KO', 'DIS', 'ADBE'
    ]
    
    return nasdaq_fallback, sp500_fallback

def get_nasdaq100_symbols(limit=None):
    """NASDAQ100銘柄を取得（フォールバック強化）"""
    nasdaq_fallback, _ = get_fallback_symbols()
    
    try:
        print(f"🔍 NASDAQ100銘柄取得中...")
        
        # User-Agentを設定してアクセス
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        url = "https://en.wikipedia.org/wiki/Nasdaq-100"
        tables = pd.read_html(url, header=0)
        
        # テーブルを順番に確認
        for i, table in enumerate(tables):
            if 'Ticker' in table.columns or 'Symbol' in table.columns:
                print(f"📊 テーブル {i} を使用")
                symbol_col = 'Ticker' if 'Ticker' in table.columns else 'Symbol'
                symbols = table[symbol_col].dropna().tolist()
                
                # 無効なシンボルを除外
                symbols = [s for s in symbols if isinstance(s, str) and len(s) <= 5 and s.isalpha()]
                
                if len(symbols) > 50:  # 妥当な数の銘柄がある場合
                    if limit and len(symbols) > limit:
                        symbols = symbols[:limit]
                    print(f"✅ NASDAQ100取得成功: {len(symbols)}銘柄")
                    return symbols, "NASDAQ-100"
        
        # すべてのテーブルで失敗した場合
        raise Exception("適切なテーブルが見つかりません")
        
    except Exception as e:
        print(f"❌ NASDAQ100取得エラー: {e}")
        fallback = nasdaq_fallback[:limit] if limit else nasdaq_fallback
        print(f"📦 フォールバック使用: {len(fallback)}銘柄")
        return fallback, "NASDAQ-100"

def get_sp500_symbols(limit=None):
    """S&P500銘柄を取得（フォールバック強化）"""
    _, sp500_fallback = get_fallback_symbols()
    
    try:
        print(f"🔍 S&P500銘柄取得中...")
        
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(url, header=0)
        
        # 最初のテーブルを使用
        symbol_df = tables[0]
        
        # Symbol列を探す
        symbol_col = None
        for col in symbol_df.columns:
            if 'symbol' in col.lower():
                symbol_col = col
                break
        
        if symbol_col is None:
            raise Exception("Symbol列が見つかりません")
        
        symbols = symbol_df[symbol_col].dropna().tolist()
        
        # 問題の多い銘柄を除外
        problematic_tickers = ['BRK.B', 'BF.B']
        symbols = [s for s in symbols if s not in problematic_tickers and isinstance(s, str)]
        
        if limit and len(symbols) > limit:
            symbols = symbols[:limit]
        
        print(f"✅ S&P500取得成功: {len(symbols)}銘柄")
        return symbols, "S&P 500"
        
    except Exception as e:
        print(f"❌ S&P500取得エラー: {e}")
        fallback = sp500_fallback[:limit] if limit else sp500_fallback
        print(f"📦 フォールバック使用: {len(fallback)}銘柄")
        return fallback, "S&P 500"

def get_single_stock_data(symbol, start_date='2021-01-01', timeout=10):
    """単一銘柄のデータを取得"""
    try:
        ticker = yf.Ticker(symbol)
        data = ticker.history(
            start=start_date,
            timeout=timeout,
            auto_adjust=False,
            actions=False
        )
        
        if data.empty:
            return None
        
        close_data = data['Close'].dropna()
        if len(close_data) < 100:  # 最低限のデータ数
            return None
        
        return close_data
        
    except Exception as e:
        print(f"  ❌ {symbol}: {e}")
        return None

def get_stock_data_robust(symbols, start_date='2021-01-01'):
    """堅牢なデータ取得（個別取得方式）"""
    print(f"📡 堅牢データ取得開始: {len(symbols)}銘柄")
    
    all_data = {}
    success_count = 0
    
    for i, symbol in enumerate(symbols, 1):
        print(f"  📈 {i}/{len(symbols)}: {symbol}", end=" ")
        
        # 個別に取得
        data = get_single_stock_data(symbol, start_date)
        
        if data is not None:
            all_data[symbol] = data
            success_count += 1
            print("✅")
        else:
            print("❌")
        
        # レート制限対策
        if i % 5 == 0:
            time.sleep(1)
    
    print(f"📊 取得結果: {success_count}/{len(symbols)}銘柄成功")
    
    if all_data:
        result_df = pd.DataFrame(all_data)
        return result_df.sort_index()
    else:
        return pd.DataFrame()

def calculate_simple_ranking(df, periods=[12, 6, 3, 1]):
    """シンプルなランキング計算"""
    try:
        # 月末価格を取得
        monthly_prices = df.resample('ME').last()
        
        if len(monthly_prices) < max(periods):
            print(f"⚠️ データ不足: {len(monthly_prices)}ヶ月分のみ")
            # 利用可能な期間に調整
            periods = [p for p in periods if p <= len(monthly_prices)]
        
        if not periods:
            return [], []
        
        # 各期間のリターンを計算
        returns_dict = {}
        for period in periods:
            if len(monthly_prices) >= period:
                start_price = monthly_prices.iloc[-period]
                end_price = monthly_prices.iloc[-1]
                period_return = (end_price / start_price - 1) * 100
                returns_dict[f'{period}m'] = period_return.dropna()
        
        if not returns_dict:
            return [], []
        
        # スコア計算（利用可能な期間で）
        if '12m' in returns_dict and '6m' in returns_dict and '3m' in returns_dict:
            # 理想的なケース
            score = (
                returns_dict['12m'] * 0.4 + 
                returns_dict['6m'] * 0.3 + 
                returns_dict['3m'] * 0.3
            )
        elif '6m' in returns_dict and '3m' in returns_dict:
            # 6ヶ月と3ヶ月のみ
            score = (
                returns_dict['6m'] * 0.6 + 
                returns_dict['3m'] * 0.4
            )
        elif '3m' in returns_dict:
            # 3ヶ月のみ
            score = returns_dict['3m']
        else:
            # フォールバック
            score = list(returns_dict.values())[0]
        
        # ランキング作成
        top_10 = score.nlargest(10).index.tolist()
        
        # ULTRA TOP5（短期重視）
        if '3m' in returns_dict and '1m' in returns_dict:
            ultra_score = (
                returns_dict['3m'] * 0.7 + 
                returns_dict['1m'] * 0.3
            )
        elif '3m' in returns_dict:
            ultra_score = returns_dict['3m']
        else:
            ultra_score = score
        
        ultra_top_5 = ultra_score.nlargest(5).index.tolist()
        
        return top_10, ultra_top_5
        
    except Exception as e:
        print(f"❌ ランキング計算エラー: {e}")
        return [], []

def process_stock_data_simple(symbols, index_name):
    """シンプル版データ処理"""
    if not symbols:
        return None
    
    print(f"\n🎯 {index_name} シンプル処理開始...")
    start_time = time.time()
    
    try:
        # データ取得
        df = get_stock_data_robust(symbols, '2021-01-01')
        
        if df.empty:
            print(f"❌ {index_name}: データ取得完全失敗")
            return None
        
        print(f"📊 有効データ: {len(df.columns)}銘柄")
        
        if len(df.columns) < 5:
            print(f"❌ {index_name}: 有効銘柄数不足")
            return None
        
        # ランキング計算
        top_10, ultra_top_5 = calculate_simple_ranking(df)
        
        # 最新日付
        latest_date = df.index[-1]
        processing_time = time.time() - start_time
        
        result = {
            "index_name": index_name,
            "last_updated": latest_date.strftime("%Y-%m-%d"),
            "top_10": top_10,
            "ultra_top_5": ultra_top_5,
            "total_stocks_processed": len(df.columns),
            "processing_time_seconds": round(processing_time, 2)
        }
        
        print(f"✅ {index_name} 処理完了 ({processing_time:.1f}秒)")
        print(f"🏆 TOP5: {', '.join(ultra_top_5)}")
        
        return result
        
    except Exception as e:
        print(f"❌ {index_name} 処理エラー: {e}")
        return None

def ensure_output_directory():
    """出力ディレクトリの確保"""
    output_dir = "data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"📁 {output_dir}ディレクトリを作成")
    return output_dir

def main():
    """メイン処理（堅牢版）"""
    print("🛡️ 堅牢版 株価ランキングシステム開始")
    print("=" * 50)
    
    if IS_GITHUB_ACTIONS:
        print("🔧 GitHub Actions環境")
        print(f"📏 最大銘柄数: {MAX_SYMBOLS}")
    
    total_start_time = time.time()
    
    # 出力ディレクトリ確保
    output_dir = ensure_output_directory()
    
    results = {}
    
    # NASDAQ100処理
    print("\n🟢 NASDAQ100処理開始")
    try:
        nasdaq_symbols, _ = get_nasdaq100_symbols(MAX_SYMBOLS if IS_GITHUB_ACTIONS else 20)
        nasdaq_result = process_stock_data_simple(nasdaq_symbols, "NASDAQ-100")
        results['nasdaq100'] = nasdaq_result
    except Exception as e:
        print(f"❌ NASDAQ100処理エラー: {e}")
        results['nasdaq100'] = None
    
    # S&P500処理（オプション、時間があれば）
    process_sp500 = not IS_GITHUB_ACTIONS or os.getenv('PROCESS_SP500', 'false').lower() == 'true'
    
    if process_sp500:
        print("\n🔵 S&P500処理開始")
        try:
            sp500_symbols, _ = get_sp500_symbols(MAX_SYMBOLS if IS_GITHUB_ACTIONS else 20)
            sp500_result = process_stock_data_simple(sp500_symbols, "S&P 500")
            results['sp500'] = sp500_result
        except Exception as e:
            print(f"❌ S&P500処理エラー: {e}")
            results['sp500'] = None
    else:
        print("\n⏭️ S&P500処理をスキップ")
        results['sp500'] = None
    
    # 結果をまとめる
    final_result = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "environment": "github_actions" if IS_GITHUB_ACTIONS else "local",
        "version": "robust_v1.0",
        "nasdaq100": results['nasdaq100'],
        "sp500": results['sp500']
    }
    
    # JSON出力
    output_file = f"{output_dir}/stock_rankings.json"
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(final_result, f, indent=2, ensure_ascii=False)
        print(f"\n✅ 結果保存: {output_file}")
    except Exception as e:
        print(f"❌ ファイル保存エラー: {e}")
    
    # 実行時間
    total_time = time.time() - total_start_time
    print(f"\n⏱️ 総実行時間: {total_time:.1f}秒")
    
    # 結果サマリー表示
    print("\n" + "="*50)
    print("📊 実行結果サマリー")
    print("="*50)
    
    success_count = 0
    for index_name, result in results.items():
        if result:
            success_count += 1
            print(f"\n🎯 {result['index_name']}:")
            print(f"   ✅ 成功 - {result['total_stocks_processed']}銘柄処理")
            print(f"   📅 更新日: {result['last_updated']}")
            if result['ultra_top_5']:
                print(f"   🏆 TOP5: {', '.join(result['ultra_top_5'])}")
        else:
            print(f"\n❌ {index_name}: 処理失敗")
    
    # GitHub Actions用の出力
    if IS_GITHUB_ACTIONS:
        try:
            github_output = os.environ.get('GITHUB_OUTPUT')
            if github_output:
                with open(github_output, 'a') as f:
                    f.write(f"execution_time={total_time:.1f}\n")
                    f.write(f"success_count={success_count}\n")
                    if results['nasdaq100']:
                        f.write(f"nasdaq_success=true\n")
                    else:
                        f.write(f"nasdaq_success=false\n")
        except Exception as e:
            print(f"GitHub Output書き込みエラー: {e}")
    
    if success_count > 0:
        print(f"\n🎉 実行完了！（{success_count}個成功）")
        return final_result
    else:
        print(f"\n💥 全処理が失敗しました")
        return None

if __name__ == "__main__":
    try:
        result = main()
        sys.exit(0 if result else 1)
    except Exception as e:
        print(f"💥 致命的エラー: {e}")
        sys.exit(1)
