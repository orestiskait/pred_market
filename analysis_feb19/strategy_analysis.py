import pandas as pd
import numpy as np

def run_analysis():
    # Load orderbook
    ob_df = pd.read_parquet('/home/kaitores/projects/pred_market/pred_market_src/collector/data/orderbook_snapshots/2026-02-19.parquet')
    ob_df = ob_df[ob_df['market_ticker'].str.contains('KXHIGHCHI-26FEB19')]
    ob_df['snapshot_ts'] = pd.to_datetime(ob_df['snapshot_ts'])
    ob_df = ob_df.sort_values('snapshot_ts')

    print(f"Loaded {len(ob_df)} orderbook entries for Chicago High.")

    # Fetch Weather
    import requests, os
    from dotenv import load_dotenv
    load_dotenv('/home/kaitores/projects/pred_market/pred_market_src/collector/.env')
    token = os.environ.get('SYNOPTIC_API_TOKEN').strip("'")
    url = "https://api.synopticdata.com/v2/stations/timeseries"
    params = {
        "token": token,
        "stid": "KMDW1M",
        "start": "202602190600",
        "end": "202602200600",
        "units": "english"
    }
    response = requests.get(url, params=params)
    obs = response.json()['STATION'][0]['OBSERVATIONS']
    wx_df = pd.DataFrame(obs)
    wx_df['date_time'] = pd.to_datetime(wx_df['date_time'])
    wx_df['air_temp_set_1'] = wx_df['air_temp_set_1'].astype(float)
    
    # 2-minute rolling min spike filter
    wx_df = wx_df.sort_values('date_time')
    wx_df['confirmed_temp'] = wx_df['air_temp_set_1'].rolling(window=2).min()
    wx_df['max_so_far'] = wx_df['confirmed_temp'].cummax().round()
    
    # Track busted states
    busted = set()
    total_profit_cents = 0
    trades = []
    
    # Iterate weather minutes
    for idx, row in wx_df.dropna(subset=['max_so_far']).iterrows():
        t = row['date_time']
        max_t = row['max_so_far']
        
        newly_busted = []
        if max_t >= 57:
            if 'T57' not in busted: newly_busted.append('T57')
        if max_t >= 59:
            if 'B57.5' not in busted: newly_busted.append('B57.5')
        if max_t >= 61:
            if 'B59.5' not in busted: newly_busted.append('B59.5')
        if max_t >= 63:
            if 'B61.5' not in busted: newly_busted.append('B61.5')
        if max_t >= 65:
            if 'B63.5' not in busted: newly_busted.append('B63.5')
            # If >= 65, T64 is CONFIRMED. We can short NO bids on T64.
            if 'T64_CONFIRMED' not in busted:
                newly_busted.append('T64_CONFIRMED')

        for b in newly_busted:
            busted.add(b)
            # Add ASOS latency
            latency_minutes = 3
            action_time = t + pd.Timedelta(minutes=latency_minutes)
            
            # Find closest snapshot before action_time
            past_snaps = ob_df[ob_df['snapshot_ts'] <= action_time]
            if len(past_snaps) == 0:
                continue
            latest_ts = past_snaps['snapshot_ts'].iloc[-1]
            current_book = ob_df[ob_df['snapshot_ts'] == latest_ts]
            
            if b == 'T64_CONFIRMED':
                # Can hit 'no' bids for T64
                book = current_book[(current_book['market_ticker'].str.contains('T64')) & (current_book['side'] == 'no')]
            else:
                book = current_book[(current_book['market_ticker'].str.contains(b)) & (current_book['side'] == 'yes')]
            
            # Sum profit
            for _, o in book.iterrows():
                profit = o['price_cents'] * o['quantity']
                total_profit_cents += profit
                trades.append({
                    'time': t,
                    'market_ticker': o['market_ticker'],
                    'qty': o['quantity'],
                    'price': o['price_cents'],
                    'profit': profit,
                    'trigger_temp': max_t
                })

    print(f"Total Profit Cents: {total_profit_cents}")
    print(f"Total Profit Dollars: ${total_profit_cents / 100:.2f}")
    if trades:
        pd.set_option('display.max_columns', None)
        trades_df = pd.DataFrame(trades)
        print("\nTrades Summary by bucket:")
        print(trades_df.groupby(['market_ticker', 'trigger_temp']).agg({'qty': 'sum', 'profit': 'sum', 'price': ['min', 'max']}))
        print("\nFirst few trades:")
        print(trades_df.head())
    else:
        print("No trades executed.")

run_analysis()
