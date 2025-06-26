# 統合株価ランキングシステム（Webサービス用）
# NQ100 + S&P500両方対応、JSON出力対応版

import pandas as pd
import yfinance as yf
import numpy as np
import json
from datetime import datetime
import os

# テスト実行用フラグ
TEST_MODE = False  # 本格実行モード（全銘柄データ）

def get_nasdaq100_symbols():
    """NASDAQ100銘柄を取得"""
    if TEST_MODE:
        # テスト用の少数銘柄
        test_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'NFLX', 'ADBE', 'CRM']
        print(f"テストモード: NASDAQ100 {len(test_symbols)}銘柄")
        return test_symbols, "NASDAQ-100"
    
    try:
        Symbol_df = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100")[4]
        symbols = Symbol_df.Ticker.to_list()
        print(f"NASDAQ100取得完了: {len(symbols)}銘柄")
        return symbols, "NASDAQ-100"
    except Exception as e:
        print(f"NASDAQ100取得エラー: {e}")
        # フォールバック
        fallback = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'NFLX', 'ADBE', 'CRM']
        print(f"フォールバック使用: {len(fallback)}銘柄")
        return fallback, "NASDAQ-100"

def get_sp500_symbols():
    """S&P500銘柄を取得"""
    if TEST_MODE:
        # テスト用の少数銘柄（NASDAQ100と少し違う銘柄）
        test_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'BRK-B', 'UNH', 'JNJ', 'V', 'XOM', 'PG']
        print(f"テストモード: S&P500 {len(test_symbols)}銘柄")
        return test_symbols, "S&P 500"
    
    try:
        sp500_tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        Symbol_df = sp500_tables[0]
        
        # 問題の多い銘柄を事前に除外
        problematic_tickers = ['BRK.B', 'BF.B']
        symbols = [ticker for ticker in Symbol_df['Symbol'].tolist() if ticker not in problematic_tickers]
        print(f"S&P500取得完了: {len(symbols)}銘柄 (問題銘柄{len(problematic_tickers)}個除外)")
        return symbols, "S&P 500"
    except Exception as e:
        print(f"S&P500取得エラー: {e}")
        # フォールバック
        fallback = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'BRK-B', 'UNH', 'JNJ', 'V', 'XOM', 'PG']
        print(f"フォールバック使用: {len(fallback)}銘柄")
        return fallback, "S&P 500"

def process_stock_data(symbols, index_name):
    """株価データを処理してランキングを生成"""
    if not symbols:
        return None
    
    print(f"\n{index_name} データ処理開始...")
    
    try:
        # 株価データダウンロード
        df = yf.download(symbols, start='2020-01-01', auto_adjust=False)['Close']
        print(f"ダウンロード完了: {df.shape}")
        
        # データクリーニング
        original_count = len(df.columns) if len(df.shape) > 1 else 1
        df = df.dropna(axis=1)
        dropped_count = original_count - (len(df.columns) if len(df.shape) > 1 else 1)
        
        if dropped_count > 0:
            print(f"⚠️  {dropped_count}銘柄がデータ不足のため除外")
        
        # 月次リターン計算
        try:
            mtl = (df.pct_change()+1)[1:].resample('ME').prod()
        except:
            mtl = (df.pct_change()+1)[1:].resample('M').prod()
        
        # 各期間のリターン計算
        def get_rolling_ret(df, n):
            return df.rolling(n).apply(np.prod)
        
        ret_12 = get_rolling_ret(mtl, 12)
        ret_6 = get_rolling_ret(mtl, 6)
        ret_3 = get_rolling_ret(mtl, 3)
        ret_1 = get_rolling_ret(mtl, 1)
        
        # 最新のデータがある日付を取得
        latest_date = mtl.index[-1]
        
        # TOP10とTOP5を計算
        def get_top_stocks(date, ret_12, ret_6, ret_3, n_top=10):
            try:
                top_50 = ret_12.loc[date].nlargest(50).index
                top_30 = ret_6.loc[date, top_50].nlargest(30).index
                top_stocks = ret_3.loc[date, top_30].nlargest(n_top).index
                return top_stocks.tolist()
            except:
                return []
        
        def get_ultra_top_stocks(date, ret_12, ret_6, ret_3, ret_1, n_top=5):
            try:
                top_50 = ret_12.loc[date].nlargest(50).index
                top_30 = ret_6.loc[date, top_50].nlargest(30).index
                top_10 = ret_3.loc[date, top_30].nlargest(10).index
                ultra_top = ret_1.loc[date, top_10].nlargest(n_top).index
                return ultra_top.tolist()
            except:
                return []
        
        top_10 = get_top_stocks(latest_date, ret_12, ret_6, ret_3, 10)
        ultra_top_5 = get_ultra_top_stocks(latest_date, ret_12, ret_6, ret_3, ret_1, 5)
        
        result = {
            "index_name": index_name,
            "last_updated": latest_date.strftime("%Y-%m-%d"),
            "top_10": top_10,
            "ultra_top_5": ultra_top_5,
            "total_stocks_processed": len(df.columns) if len(df.shape) > 1 else 1
        }
        
        print(f"{index_name} 処理完了")
        return result
        
    except Exception as e:
        print(f"{index_name} 処理エラー: {e}")
        return None

def main():
    """メイン処理"""
    print("=== 統合株価ランキングシステム開始 ===")
    
    if TEST_MODE:
        print("🧪 テストモード実行中（少数銘柄で動作確認）")
    
    # 両指数のデータを取得・処理
    nasdaq_symbols, _ = get_nasdaq100_symbols()
    sp500_symbols, _ = get_sp500_symbols()
    
    nasdaq_result = process_stock_data(nasdaq_symbols, "NASDAQ-100")
    sp500_result = process_stock_data(sp500_symbols, "S&P 500")
    
    # 結果をまとめる
    final_result = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "test_mode": TEST_MODE,
        "nasdaq100": nasdaq_result,
        "sp500": sp500_result
    }
    
    # JSONファイルとして保存
    output_dir = "data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    filename = "stock_rankings_test.json" if TEST_MODE else "stock_rankings.json"
    with open(f"{output_dir}/{filename}", "w", encoding="utf-8") as f:
        json.dump(final_result, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ 結果を{output_dir}/{filename}に保存しました")
    
    # 結果をコンソールに表示
    print("\n" + "="*60)
    print("📊 最新ランキング結果")
    print("="*60)
    
    if nasdaq_result:
        print(f"\n🟢 NASDAQ-100 (更新日: {nasdaq_result['last_updated']})")
        print("🏆 TOP10:")
        for i, ticker in enumerate(nasdaq_result['top_10'], 1):
            print(f"  {i:2d}. {ticker}")
        print("⭐ ULTRA TOP5:")
        for i, ticker in enumerate(nasdaq_result['ultra_top_5'], 1):
            print(f"  {i}. {ticker}")
    
    if sp500_result:
        print(f"\n🔵 S&P 500 (更新日: {sp500_result['last_updated']})")
        print("🏆 TOP10:")
        for i, ticker in enumerate(sp500_result['top_10'], 1):
            print(f"  {i:2d}. {ticker}")
        print("⭐ ULTRA TOP5:")
        for i, ticker in enumerate(sp500_result['ultra_top_5'], 1):
            print(f"  {i}. {ticker}")
    
    print("\n" + "="*60)
    print("🎯 テスト完了！")
    if TEST_MODE:
        print("💡 本番実行時は TEST_MODE = False に変更してください")
    
    return final_result

if __name__ == "__main__":
    result = main()