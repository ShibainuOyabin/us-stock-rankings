# 軽量化株価ランキングシステム（NASDAQ-100のみ）
# S&P500処理をコメントアウトして負荷軽減版
#
# 【忘備録】S&P500再有効化方法:
# 将来的にS&P500も必要になった場合は、コメントアウト部分の `#` を削除し、
# メイン処理で以下の行のコメントを外してください：
# sp500_symbols, _ = get_sp500_symbols()
# sp500_result = process_stock_data(sp500_symbols, "S&P 500")

import pandas as pd
import yfinance as yf
import numpy as np
import json
import os
import time
import traceback
from datetime import datetime, timedelta
from collections import OrderedDict

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
        
        # GitHub Actions環境では銘柄数を制限
        if os.getenv('GITHUB_ACTIONS'):
            # 本番モードでもGitHub Actions環境では50銘柄に制限
            symbols = symbols[:50]
            print(f"GitHub Actions環境: NASDAQ100を{len(symbols)}銘柄に制限")
        else:
            print(f"NASDAQ100取得完了: {len(symbols)}銘柄")
            
        return symbols, "NASDAQ-100"
    except Exception as e:
        print(f"NASDAQ100取得エラー: {e}")
        # フォールバック
        fallback = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'NFLX', 'ADBE', 'CRM']
        print(f"フォールバック使用: {len(fallback)}銘柄")
        return fallback, "NASDAQ-100"

# S&P500関連の関数をコメントアウト
# def get_sp500_symbols():
#     """S&P500銘柄を取得"""
#     if TEST_MODE:
#         # テスト用の少数銘柄（NASDAQ100と少し違う銘柄）
#         test_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'BRK-B', 'UNH', 'JNJ', 'V', 'XOM', 'PG']
#         print(f"テストモード: S&P500 {len(test_symbols)}銘柄")
#         return test_symbols, "S&P 500"
#     
#     try:
#         sp500_tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
#         Symbol_df = sp500_tables[0]
#         
#         # 問題の多い銘柄を事前に除外
#         problematic_tickers = ['BRK.B', 'BF.B']
#         symbols = [ticker for ticker in Symbol_df['Symbol'].tolist() if ticker not in problematic_tickers]
#         print(f"S&P500取得完了: {len(symbols)}銘柄 (問題銘柄{len(problematic_tickers)}個除外)")
#         return symbols, "S&P 500"
#     except Exception as e:
#         print(f"S&P500取得エラー: {e}")
#         # フォールバック
#         fallback = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'BRK-B', 'UNH', 'JNJ', 'V', 'XOM', 'PG']
#         print(f"フォールバック使用: {len(fallback)}銘柄")
#         return fallback, "S&P 500"

def process_stock_data(symbols, index_name):
    """株価データを処理してランキングを生成"""
    if not symbols:
        return None
    
    print(f"\n{index_name} データ処理開始...")
    
    try:
        # GitHub Actions環境での実行時間を考慮して、リトライ機能付きでダウンロード
        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f"データダウンロード試行 {attempt + 1}/{max_retries}...")
                
                # より短い期間でデータ取得（GitHub Actions用）
                if os.getenv('GITHUB_ACTIONS'):
                    start_date = '2022-01-01'  # 期間を短縮
                    print(f"GitHub Actions環境: 期間を{start_date}からに短縮")
                else:
                    start_date = '2020-01-01'
                
                df = yf.download(symbols, start=start_date, auto_adjust=False, 
                               progress=True, threads=True)['Close']
                print(f"ダウンロード完了: {df.shape}")
                break
                
            except Exception as download_error:
                print(f"ダウンロード試行 {attempt + 1} 失敗: {download_error}")
                if attempt == max_retries - 1:
                    raise download_error
                print(f"5秒待機後、再試行...")
                time.sleep(5)
        
        # データクリーニング
        original_count = len(df.columns) if len(df.shape) > 1 else 1
        df = df.dropna(axis=1)
        dropped_count = original_count - (len(df.columns) if len(df.shape) > 1 else 1)
        
        if dropped_count > 0:
            print(f"⚠️  {dropped_count}銘柄がデータ不足のため除外")
        
        # 残った銘柄数を確認
        remaining_symbols = len(df.columns) if len(df.shape) > 1 else 1
        if remaining_symbols < 5:
            print(f"❌ 利用可能な銘柄数が少なすぎます: {remaining_symbols}銘柄")
            return None
            
        print(f"✅ 処理可能な銘柄数: {remaining_symbols}銘柄")
        
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
        
        # TOP10とTOP5を計算（利用可能な銘柄数に応じて調整）
        def get_top_stocks(date, ret_12, ret_6, ret_3, n_top=10):
            try:
                available_stocks = len(ret_12.loc[date].dropna())
                if available_stocks < n_top:
                    n_top = available_stocks
                    print(f"⚠️ 利用可能銘柄数が少ないため、TOP{n_top}に調整")
                
                top_50 = min(50, available_stocks)
                top_30 = min(30, available_stocks)
                
                top_50_stocks = ret_12.loc[date].nlargest(top_50).index
                top_30_stocks = ret_6.loc[date, top_50_stocks].nlargest(top_30).index
                top_stocks = ret_3.loc[date, top_30_stocks].nlargest(n_top).index
                return top_stocks.tolist()
            except Exception as e:
                print(f"TOP株選出エラー: {e}")
                return []
        
        def get_ultra_top_stocks(date, ret_12, ret_6, ret_3, ret_1, n_top=5):
            try:
                available_stocks = len(ret_12.loc[date].dropna())
                if available_stocks < n_top:
                    n_top = available_stocks
                    print(f"⚠️ 利用可能銘柄数が少ないため、ULTRA TOP{n_top}に調整")
                
                top_50 = min(50, available_stocks)
                top_30 = min(30, available_stocks)
                top_10 = min(10, available_stocks)
                
                top_50_stocks = ret_12.loc[date].nlargest(top_50).index
                top_30_stocks = ret_6.loc[date, top_50_stocks].nlargest(top_30).index
                top_10_stocks = ret_3.loc[date, top_30_stocks].nlargest(top_10).index
                ultra_top = ret_1.loc[date, top_10_stocks].nlargest(n_top).index
                return ultra_top.tolist()
            except Exception as e:
                print(f"ULTRA TOP株選出エラー: {e}")
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
        print(f"TOP10: {len(top_10)}銘柄, ULTRA TOP5: {len(ultra_top_5)}銘柄")
        return result
        
    except Exception as e:
        print(f"{index_name} 処理エラー: {e}")
        print(f"詳細エラー: {traceback.format_exc()}")
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

def update_history(history, nasdaq_result):
    """履歴データを更新（過去30日分を保持）- NASDAQ-100のみ"""
    today = datetime.now().strftime("%Y-%m-%d")
    
    # 今日のデータを追加（NASDAQ-100のみ）
    history[today] = {
        "nasdaq100": {
            "ultra_top_5": nasdaq_result["ultra_top_5"] if nasdaq_result else [],
            "top_10": nasdaq_result["top_10"] if nasdaq_result else []
        }
        # S&P500セクションをコメントアウト
        # "sp500": {
        #     "ultra_top_5": sp500_result["ultra_top_5"] if sp500_result else [],
        #     "top_10": sp500_result["top_10"] if sp500_result else []
        # }
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
    """ランキング変動を分析（NASDAQ-100のみ）"""
    if len(history) < 2:
        return
    
    dates = sorted(history.keys(), reverse=True)
    today = dates[0]
    yesterday = dates[1] if len(dates) > 1 else None
    
    if not yesterday:
        return
    
    print(f"\n📈 ランキング変動分析 ({yesterday} → {today})")
    print("-" * 50)
    
    # NASDAQ100の変動のみ
    if ("nasdaq100" in history[today] and "nasdaq100" in history[yesterday] and
        history[today]["nasdaq100"]["ultra_top_5"] and history[yesterday]["nasdaq100"]["ultra_top_5"]):
        today_nasdaq = history[today]["nasdaq100"]["ultra_top_5"]
        yesterday_nasdaq = history[yesterday]["nasdaq100"]["ultra_top_5"]
        
        print("🟢 NASDAQ100 ULTRA TOP5変動:")
        for i, (today_stock, yesterday_stock) in enumerate(zip(today_nasdaq, yesterday_nasdaq), 1):
            if today_stock != yesterday_stock:
                print(f"  {i}位: {yesterday_stock} → {today_stock} 🔄")
            else:
                print(f"  {i}位: {today_stock} (変動なし)")
    
    # S&P500の変動分析をコメントアウト
    # if history[today]["sp500"]["ultra_top_5"] and history[yesterday]["sp500"]["ultra_top_5"]:
    #     today_sp500 = history[today]["sp500"]["ultra_top_5"]
    #     yesterday_sp500 = history[yesterday]["sp500"]["ultra_top_5"]
    #     
    #     print("\n🔵 S&P500 ULTRA TOP5変動:")
    #     for i, (today_stock, yesterday_stock) in enumerate(zip(today_sp500, yesterday_sp500), 1):
    #         if today_stock != yesterday_stock:
    #             print(f"  {i}位: {yesterday_stock} → {today_stock} 🔄")
    #         else:
    #             print(f"  {i}位: {today_stock} (変動なし)")

def main():
    """メイン処理 - NASDAQ-100のみ"""
    print("=== 軽量化株価ランキングシステム開始（NASDAQ-100のみ）===")
    
    if TEST_MODE:
        print("🧪 テストモード実行中（少数銘柄で動作確認）")
    
    # dataディレクトリ作成
    output_dir = "data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"📁 {output_dir}ディレクトリを作成しました")
    else:
        print(f"📁 {output_dir}ディレクトリは既に存在します")
    
    # 履歴データを読み込み
    history = load_history()
    
    # NASDAQ-100のデータのみを取得・処理
    nasdaq_symbols, _ = get_nasdaq100_symbols()
    nasdaq_result = process_stock_data(nasdaq_symbols, "NASDAQ-100")
    
    # S&P500処理をコメントアウト
    # sp500_symbols, _ = get_sp500_symbols()
    # sp500_result = process_stock_data(sp500_symbols, "S&P 500")
    
    # 履歴データを更新（NASDAQ-100のみ）
    history = update_history(history, nasdaq_result)
    
    # 履歴データを保存
    save_history(history)
    
    # ランキング変動分析（NASDAQ-100のみ）
    analyze_ranking_changes(history)
    
    # 現在のランキングデータをまとめる（NASDAQ-100のみ）
    final_result = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "test_mode": TEST_MODE,
        "nasdaq100": nasdaq_result
        # S&P500結果をコメントアウト
        # "sp500": sp500_result
    }
    
    # 現在のランキングJSONファイルとして保存
    filename = "stock_rankings_nasdaq_only_test.json" if TEST_MODE else "stock_rankings_nasdaq_only.json"
    with open(f"{output_dir}/{filename}", "w", encoding="utf-8") as f:
        json.dump(final_result, f, indent=2, ensure_ascii=False)
    
    print(f"📁 ファイル保存完了: {output_dir}/{filename}")
    
    # ファイルが正常に作成されたか確認
    import os
    if os.path.exists(f"{output_dir}/{filename}"):
        file_size = os.path.getsize(f"{output_dir}/{filename}")
        print(f"✅ ファイル確認成功: {filename} ({file_size} bytes)")
    else:
        print(f"❌ ファイル作成失敗: {filename}")
        print(f"📂 現在のディレクトリ内容: {os.listdir(output_dir)}")
    
    print(f"\n✅ 結果を{output_dir}/{filename}に保存しました")
    
    # 結果をコンソールに表示（NASDAQ-100のみ）
    print("\n" + "="*60)
    print("📊 最新ランキング結果（NASDAQ-100のみ）")
    print("="*60)
    
    if nasdaq_result:
        print(f"\n🟢 NASDAQ-100 (更新日: {nasdaq_result['last_updated']})")
        print("🏆 TOP10:")
        for i, ticker in enumerate(nasdaq_result['top_10'], 1):
            print(f"  {i:2d}. {ticker}")
        print("⭐ ULTRA TOP5:")
        for i, ticker in enumerate(nasdaq_result['ultra_top_5'], 1):
            print(f"  {i}. {ticker}")
    
    # S&P500結果表示をコメントアウト
    # if sp500_result:
    #     print(f"\n🔵 S&P 500 (更新日: {sp500_result['last_updated']})")
    #     print("🏆 TOP10:")
    #     for i, ticker in enumerate(sp500_result['top_10'], 1):
    #         print(f"  {i:2d}. {ticker}")
    #     print("⭐ ULTRA TOP5:")
    #     for i, ticker in enumerate(sp500_result['ultra_top_5'], 1):
    #         print(f"  {i}. {ticker}")
    
    print("\n" + "="*60)
    print("🎯 実行完了！（軽量化版 - NASDAQ-100のみ）")
    print(f"📚 履歴データ: {len(history)}日分保存済み")
    print("⚡ 負荷軽減: S&P500処理を無効化済み")
    if TEST_MODE:
        print("💡 本番実行時は TEST_MODE = False に変更してください")
    
    return final_result

if __name__ == "__main__":
    result = main()
