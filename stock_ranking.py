# GitHub Actions最適化版：統合株価ランキングシステム
# 実行時間短縮 + CI/CD環境対応

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

# 環境変数からAPI設定を取得（GitHub Secretsから）
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY', 'QTLKGZU9EXK5OI3Y')

# GitHub Actions用設定
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS', 'false').lower() == 'true'
MAX_SYMBOLS = int(os.getenv('MAX_SYMBOLS', '50'))  # GitHub Actions用に制限

def get_nasdaq100_symbols(limit=None):
    """NASDAQ100銘柄を取得（GitHub Actions用制限付き）"""
    try:
        print(f"🔍 NASDAQ100銘柄取得中...")
        Symbol_df = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")[4]
        symbols = Symbol_df.Ticker.to_list()
        
        # GitHub Actions用制限
        if limit and len(symbols) > limit:
            symbols = symbols[:limit]
            print(f"⚠️ GitHub Actions用に{limit}銘柄に制限")
        
        print(f"✅ NASDAQ100取得完了: {len(symbols)}銘柄")
        return symbols, "NASDAQ-100"
    except Exception as e:
        print(f"❌ NASDAQ100取得エラー: {e}")
        # フォールバック（主要銘柄）
        fallback = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'NFLX', 'ADBE', 'CRM']
        if limit:
            fallback = fallback[:min(limit, len(fallback))]
        print(f"📦 フォールバック使用: {len(fallback)}銘柄")
        return fallback, "NASDAQ-100"

def get_sp500_symbols(limit=None):
    """S&P500銘柄を取得（GitHub Actions用制限付き）"""
    try:
        print(f"🔍 S&P500銘柄取得中...")
        sp500_tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        Symbol_df = sp500_tables[0]
        
        # 問題の多い銘柄を事前に除外
        problematic_tickers = ['BRK.B', 'BF.B']
        symbols = [ticker for ticker in Symbol_df['Symbol'].tolist() if ticker not in problematic_tickers]
        
        # GitHub Actions用制限
        if limit and len(symbols) > limit:
            symbols = symbols[:limit]
            print(f"⚠️ GitHub Actions用に{limit}銘柄に制限")
        
        print(f"✅ S&P500取得完了: {len(symbols)}銘柄")
        return symbols, "S&P 500"
    except Exception as e:
        print(f"❌ S&P500取得エラー: {e}")
        # フォールバック
        fallback = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'UNH', 'JNJ', 'V', 'XOM', 'PG', 'JPM']
        if limit:
            fallback = fallback[:min(limit, len(fallback))]
        print(f"📦 フォールバック使用: {len(fallback)}銘柄")
        return fallback, "S&P 500"

def get_stock_data_batch(symbols, start_date='2020-01-01', max_retries=3):
    """GitHub Actions最適化版データ取得"""
    print(f"📡 データ取得開始: {len(symbols)}銘柄")
    
    for attempt in range(max_retries):
        try:
            print(f"🔄 試行 {attempt + 1}/{max_retries}")
            
            # より小さなバッチサイズでCI環境に配慮
            batch_size = 10 if IS_GITHUB_ACTIONS else 15
            all_data = {}
            
            for i in range(0, len(symbols), batch_size):
                batch_symbols = symbols[i:i+batch_size]
                batch_num = i//batch_size + 1
                total_batches = (len(symbols) + batch_size - 1) // batch_size
                
                print(f"  📦 バッチ {batch_num}/{total_batches}: {len(batch_symbols)}銘柄")
                
                try:
                    # yfinanceでバッチ取得
                    batch_df = yf.download(
                        batch_symbols,
                        start=start_date,
                        auto_adjust=False,
                        progress=False,
                        threads=False,
                        timeout=20,  # GitHub Actions用にタイムアウト短縮
                        show_errors=False
                    )
                    
                    if isinstance(batch_df, pd.DataFrame) and not batch_df.empty:
                        # Close価格を抽出
                        if 'Close' in batch_df.columns:
                            close_data = batch_df['Close']
                            
                            # 単一銘柄の場合の処理
                            if len(batch_symbols) == 1:
                                symbol = batch_symbols[0]
                                if not close_data.dropna().empty:
                                    all_data[symbol] = close_data.dropna()
                            else:
                                # 複数銘柄の場合
                                for symbol in batch_symbols:
                                    if symbol in close_data.columns:
                                        symbol_data = close_data[symbol].dropna()
                                        if len(symbol_data) > 100:  # 最低限のデータ数確保
                                            all_data[symbol] = symbol_data
                    
                    # GitHub Actions用短い待機時間
                    time.sleep(1 if IS_GITHUB_ACTIONS else 2)
                    
                except Exception as batch_error:
                    print(f"    ❌ バッチエラー: {batch_error}")
                    continue
            
            if all_data:
                print(f"✅ 取得成功: {len(all_data)}銘柄")
                result_df = pd.DataFrame(all_data)
                return result_df.sort_index()
            
        except Exception as e:
            print(f"❌ 試行 {attempt + 1} 失敗: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 3
                print(f"⏳ {wait_time}秒待機してリトライ...")
                time.sleep(wait_time)
    
    print("❌ すべての試行が失敗")
    return pd.DataFrame()

def process_stock_data_fast(symbols, index_name):
    """高速化版データ処理（GitHub Actions用）"""
    if not symbols:
        return None
    
    print(f"\n🚀 {index_name} 高速処理開始...")
    start_time = time.time()
    
    try:
        # データ取得
        df = get_stock_data_batch(symbols, '2021-01-01')  # 期間短縮で高速化
        
        if df.empty:
            print(f"❌ {index_name}: データ取得失敗")
            return None
        
        # データクリーニング
        min_data_points = 200  # 最低限のデータポイント数
        valid_columns = []
        
        for col in df.columns:
            if len(df[col].dropna()) >= min_data_points:
                valid_columns.append(col)
        
        if not valid_columns:
            print(f"❌ {index_name}: 有効なデータなし")
            return None
        
        df = df[valid_columns]
        print(f"📊 有効銘柄: {len(df.columns)}銘柄")
        
        # 月次リターン計算（高速化）
        monthly_returns = df.resample('ME').last().pct_change().dropna()
        
        if monthly_returns.empty:
            print(f"❌ {index_name}: 月次データ計算失敗")
            return None
        
        # 簡素化されたランキング計算
        # 過去12ヶ月、6ヶ月、3ヶ月のリターンを計算
        periods = [12, 6, 3, 1]
        returns_data = {}
        
        for period in periods:
            if len(monthly_returns) >= period:
                period_returns = (monthly_returns.tail(period) + 1).prod() - 1
                returns_data[f'{period}m'] = period_returns
        
        if not returns_data:
            print(f"❌ {index_name}: リターンデータなし")
            return None
        
        # ランキング計算（簡素化版）
        latest_date = monthly_returns.index[-1]
        
        # TOP10計算：12M, 6M, 3Mの加重平均
        if '12m' in returns_data and '6m' in returns_data and '3m' in returns_data:
            weighted_score = (
                returns_data['12m'] * 0.4 + 
                returns_data['6m'] * 0.3 + 
                returns_data['3m'] * 0.3
            ).dropna()
            
            top_10 = weighted_score.nlargest(10).index.tolist()
        else:
            top_10 = []
        
        # ULTRA TOP5計算：短期重視
        if '3m' in returns_data and '1m' in returns_data:
            ultra_score = (
                returns_data['3m'] * 0.6 + 
                returns_data['1m'] * 0.4
            ).dropna()
            
            ultra_top_5 = ultra_score.nlargest(5).index.tolist()
        else:
            ultra_top_5 = []
        
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
    """GitHub Actions最適化版メイン処理"""
    print("🚀 GitHub Actions最適化版 株価ランキングシステム開始")
    print("=" * 60)
    
    # GitHub Actions環境チェック
    if IS_GITHUB_ACTIONS:
        print("🔧 GitHub Actions環境を検出")
        print(f"📏 最大銘柄数制限: {MAX_SYMBOLS}")
    
    total_start_time = time.time()
    
    # 出力ディレクトリ確保
    output_dir = ensure_output_directory()
    
    # 処理対象の決定
    process_nasdaq = True
    process_sp500 = not IS_GITHUB_ACTIONS or os.getenv('PROCESS_SP500', 'false').lower() == 'true'
    
    results = {}
    
    # NASDAQ100処理
    if process_nasdaq:
        try:
            nasdaq_symbols, _ = get_nasdaq100_symbols(MAX_SYMBOLS if IS_GITHUB_ACTIONS else None)
            nasdaq_result = process_stock_data_fast(nasdaq_symbols, "NASDAQ-100")
            results['nasdaq100'] = nasdaq_result
        except Exception as e:
            print(f"❌ NASDAQ100処理エラー: {e}")
            results['nasdaq100'] = None
    
    # S&P500処理（オプション）
    if process_sp500:
        try:
            sp500_symbols, _ = get_sp500_symbols(MAX_SYMBOLS if IS_GITHUB_ACTIONS else None)
            sp500_result = process_stock_data_fast(sp500_symbols, "S&P 500")
            results['sp500'] = sp500_result
        except Exception as e:
            print(f"❌ S&P500処理エラー: {e}")
            results['sp500'] = None
    else:
        print("⏭️ S&P500処理をスキップ")
        results['sp500'] = None
    
    # 結果をまとめる
    final_result = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "environment": "github_actions" if IS_GITHUB_ACTIONS else "local",
        "nasdaq100": results['nasdaq100'],
        "sp500": results['sp500']
    }
    
    # JSON出力
    output_file = f"{output_dir}/stock_rankings.json"
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(final_result, f, indent=2, ensure_ascii=False)
        print(f"✅ 結果保存: {output_file}")
    except Exception as e:
        print(f"❌ ファイル保存エラー: {e}")
    
    # 実行時間
    total_time = time.time() - total_start_time
    print(f"\n⏱️ 総実行時間: {total_time:.1f}秒")
    
    # 結果サマリー表示
    print("\n" + "="*60)
    print("📊 実行結果サマリー")
    print("="*60)
    
    for index_name, result in results.items():
        if result:
            print(f"\n🎯 {result['index_name']}:")
            print(f"   更新日: {result['last_updated']}")
            print(f"   処理銘柄数: {result['total_stocks_processed']}")
            print(f"   TOP10: {', '.join(result['top_10'][:5])}...")
            print(f"   ULTRA TOP5: {', '.join(result['ultra_top_5'])}")
        else:
            print(f"\n❌ {index_name}: 処理失敗")
    
    # GitHub Actions用の出力
    if IS_GITHUB_ACTIONS:
        # GitHub Actions環境変数に結果を設定
        with open(os.environ.get('GITHUB_OUTPUT', '/dev/null'), 'a') as f:
            f.write(f"execution_time={total_time:.1f}\n")
            if results['nasdaq100']:
                f.write(f"nasdaq_success=true\n")
                f.write(f"nasdaq_top5={','.join(results['nasdaq100']['ultra_top_5'])}\n")
            else:
                f.write(f"nasdaq_success=false\n")
    
    print(f"\n🎉 実行完了！")
    return final_result

if __name__ == "__main__":
    try:
        result = main()
        sys.exit(0)
    except Exception as e:
        print(f"💥 致命的エラー: {e}")
        sys.exit(1)
