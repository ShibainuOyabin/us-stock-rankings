# 統合株価ランキングシステム（Webサービス用）
# NQ100 + S&P500両方対応、JSON出力対応版 + 履歴管理機能

import pandas as pd
import yfinance as yf
import numpy as np
import json
from datetime import datetime, timedelta
import os
from collections import OrderedDict
import time

def get_nasdaq100_symbols():
    """NASDAQ100銘柄を取得"""
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
        # S&P500の場合は分割取得
        if len(symbols) > 200:
            print(f"大量データのため分割取得: {len(symbols)}銘柄")
            all_data = []
            batch_size = 100
            
            for i in range(0, len(symbols), batch_size):
                batch_symbols = symbols[i:i+batch_size]
                print(f"バッチ {i//batch_size + 1}: {len(batch_symbols)}銘柄取得中...")
                
                try:
                    batch_df = yf.download(batch_symbols, start='2020-01-01', auto_adjust=False, 
                                         progress=False, threads=True)['Close']
                    if not batch_df.empty:
                        all_data.append(batch_df)
                    time.sleep(1)  # レート制限対策
                except Exception as e:
                    print(f"バッチ {i//batch_size + 1} エラー: {e}")
                    continue
            
            if all_data:
                df = pd.concat(all_data, axis=1)
                print(f"分割取得完了: {df.shape}")
            else:
                print("すべてのバッチが失敗")
                return None
        else:
            # NASDAQ100など少数の場合は通常取得
            df = yf.download(symbols, start='2020-01-01', auto_adjust=False, progress=False)['Close']
            print(f"一括取得完了: {df.shape}")
        
        # データクリーニング
        original_count = len(df.columns) if len(df.shape) > 1 else 1
        df = df.dropna(axis=1)
        dropped_count = original_count - (len(df.columns) if len(df.shape) > 1 else 1)
        
        if dropped_count > 0:
            print(f"⚠️  {dropped_count}銘柄がデータ不足のため除外")
        
        # 残った銘柄数を確認
        remaining_count = len(df.columns) if len(df.shape) > 1 else 1
        print(f"📊 処理対象銘柄数: {remaining_count}")
        
        if remaining_count < 10:
            print("⚠️ 処理可能な銘柄が少なすぎます")
            return None
        
        print(f"📅 データ期間: {df.index[0]} ～ {df.index[-1]}")
        print(f"📈 データ形状: {df.shape}")
        
        # 月次リターン計算
        try:
            print("🔄 月次リターン計算中...")
            mtl = (df.pct_change()+1)[1:].resample('ME').prod()
            print(f"📊 月次データ形状: {mtl.shape}")
            print(f"📅 月次データ期間: {mtl.index[0]} ～ {mtl.index[-1]}")
        except Exception as e:
            print(f"月次リターン計算エラー: {e}")
            try:
                mtl = (df.pct_change()+1)[1:].resample('M').prod()
                print(f"📊 月次データ形状（代替）: {mtl.shape}")
            except Exception as e2:
                print(f"代替月次計算もエラー: {e2}")
                return None
        
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
                if ret_12.empty or ret_6.empty or ret_3.empty:
                    print(f"警告: データが空のためランキング計算をスキップ")
                    return []
                
                top_50_series = ret_12.loc[date].dropna()
                if len(top_50_series) == 0:
                    print(f"警告: {date}のデータが見つかりません")
                    return []
                
                top_50 = top_50_series.nlargest(min(50, len(top_50_series))).index
                if len(top_50) == 0:
                    return []
                
                top_30_series = ret_6.loc[date, top_50].dropna()
                top_30 = top_30_series.nlargest(min(30, len(top_30_series))).index
                if len(top_30) == 0:
                    return []
                
                top_stocks_series = ret_3.loc[date, top_30].dropna()
                top_stocks = top_stocks_series.nlargest(min(n_top, len(top_stocks_series))).index
                return top_stocks.tolist()
            except Exception as e:
                print(f"ランキング計算エラー: {e}")
                return []
        
        def get_ultra_top_stocks(date, ret_12, ret_6, ret_3, ret_1, n_top=5):
            try:
                if ret_12.empty or ret_6.empty or ret_3.empty or ret_1.empty:
                    print(f"警告: データが空のためULTRAランキング計算をスキップ")
                    return []
                
                top_50_series = ret_12.loc[date].dropna()
                if len(top_50_series) == 0:
                    return []
                
                top_50 = top_50_series.nlargest(min(50, len(top_50_series))).index
                if len(top_50) == 0:
                    return []
                
                top_30_series = ret_6.loc[date, top_50].dropna()
                top_30 = top_30_series.nlargest(min(30, len(top_30_series))).index
                if len(top_30) == 0:
                    return []
                
                top_10_series = ret_3.loc[date, top_30].dropna()
                top_10 = top_10_series.nlargest(min(10, len(top_10_series))).index
                if len(top_10) == 0:
                    return []
                
                ultra_top_series = ret_1.loc[date, top_10].dropna()
                ultra_top = ultra_top_series.nlargest(min(n_top, len(ultra_top_series))).index
                return ultra_top.tolist()
            except Exception as e:
                print(f"ULTRAランキング計算エラー: {e}")
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

def load_history():
    """既存の履歴データを読み込み"""
    history_file = "data/rankings_history.json"
    
    if os.path.exists(history_file):
        try:
            with open(history_file, "r", encoding="utf-8") as f:
                history = json.load(f)
            print(f"📚 既存履歴データ読み込み: {len(history)}日分")
            return history
        except Exception as e:
            print(f"履歴データ読み込みエラー: {e}")
            return {}
    else:
        print("📚 新規履歴データファイルを作成します")
        return {}

def update_history(history, nasdaq_result, sp500_result):
    """履歴データを更新（過去30日分を保持）"""
    today = datetime.now().strftime("%Y-%m-%d")
    
    # 今日のデータを追加
    history[today] = {
        "nasdaq100": {
            "ultra_top_5": nasdaq_result["ultra_top_5"] if nasdaq_result else [],
            "top_10": nasdaq_result["top_10"] if nasdaq_result else []
        },
        "sp500": {
            "ultra_top_5": sp500_result["ultra_top_5"] if sp500_result else [],
            "top_10": sp500_result["top_10"] if sp500_result else []
        }
    }
    
    # 日付でソートし、新しい順に並べる
    sorted_dates = sorted(history.keys(), reverse=True)
    
    # 過去30日分のみ保持
    if len(sorted_dates) > 30:
        dates_to_keep = sorted_dates[:30]
        history = {date: history[date] for date in dates_to_keep}
        print(f"📅 履歴データを30日分に制限: {len(dates_to_keep)}日分保持")
    
    return history

def save_history(history):
    """履歴データをファイルに保存"""
    history_file = "data/rankings_history.json"
    
    try:
        # 日付順にソート（OrderedDictを使用して順序を保持）
        sorted_history = OrderedDict(sorted(history.items(), reverse=True))
        
        with open(history_file, "w", encoding="utf-8") as f:
            json.dump(sorted_history, f, indent=2, ensure_ascii=False)
        
        print(f"📚 履歴データ保存完了: {len(sorted_history)}日分")
        
        # 履歴の統計情報を表示
        if sorted_history:
            oldest_date = min(sorted_history.keys())
            newest_date = max(sorted_history.keys())
            print(f"📊 履歴期間: {oldest_date} ～ {newest_date}")
        
    except Exception as e:
        print(f"履歴データ保存エラー: {e}")

def analyze_ranking_changes(history):
    """ランキング変動を分析（オプション機能）"""
    if len(history) < 2:
        return
    
    dates = sorted(history.keys(), reverse=True)
    today = dates[0]
    yesterday = dates[1] if len(dates) > 1 else None
    
    if not yesterday:
        return
    
    print(f"\n📈 ランキング変動分析 ({yesterday} → {today})")
    print("-" * 50)
    
    # NASDAQ100の変動
    if history[today]["nasdaq100"]["ultra_top_5"] and history[yesterday]["nasdaq100"]["ultra_top_5"]:
        today_nasdaq = history[today]["nasdaq100"]["ultra_top_5"]
        yesterday_nasdaq = history[yesterday]["nasdaq100"]["ultra_top_5"]
        
        print("🟢 NASDAQ100 ULTRA TOP5変動:")
        for i, (today_stock, yesterday_stock) in enumerate(zip(today_nasdaq, yesterday_nasdaq), 1):
            if today_stock != yesterday_stock:
                print(f"  {i}位: {yesterday_stock} → {today_stock} 🔄")
            else:
                print(f"  {i}位: {today_stock} (変動なし)")
    
    # S&P500の変動
    if history[today]["sp500"]["ultra_top_5"] and history[yesterday]["sp500"]["ultra_top_5"]:
        today_sp500 = history[today]["sp500"]["ultra_top_5"]
        yesterday_sp500 = history[yesterday]["sp500"]["ultra_top_5"]
        
        print("\n🔵 S&P500 ULTRA TOP5変動:")
        for i, (today_stock, yesterday_stock) in enumerate(zip(today_sp500, yesterday_sp500), 1):
            if today_stock != yesterday_stock:
                print(f"  {i}位: {yesterday_stock} → {today_stock} 🔄")
            else:
                print(f"  {i}位: {today_stock} (変動なし)")

def main():
    """メイン処理"""
    print("=== 統合株価ランキングシステム開始 ===")
    
    # dataディレクトリ作成
    output_dir = "data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"📁 {output_dir}ディレクトリを作成しました")
    
    # 履歴データを読み込み
    history = load_history()
    
    # デバッグ用：NASDAQ100のみ実行
    nasdaq_symbols, _ = get_nasdaq100_symbols()
    nasdaq_result = process_stock_data(nasdaq_symbols, "NASDAQ-100")
    
    print(f"\n🔍 NASDAQ100結果の詳細:")
    print(f"nasdaq_result = {nasdaq_result}")
    
    # S&P500は一時的に無効化
    print("🔧 S&P500処理を一時的にスキップ（問題調査中）")
    sp500_result = None
    
    # 履歴データを更新
    history = update_history(history, nasdaq_result, sp500_result)
    
    # 履歴データを保存
    save_history(history)
    
    # ランキング変動分析（オプション）
    analyze_ranking_changes(history)
    
    # 現在のランキングデータをまとめる
    final_result = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "nasdaq100": nasdaq_result,
        "sp500": sp500_result
    }
    
    # 現在のランキングJSONファイルとして保存
    with open(f"{output_dir}/stock_rankings.json", "w", encoding="utf-8") as f:
        json.dump(final_result, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ 結果を{output_dir}/stock_rankings.jsonに保存しました")
    
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
    print("🎯 実行完了！")
    print(f"📚 履歴データ: {len(history)}日分保存済み")
    
    return final_result

if __name__ == "__main__":
    result = main()
