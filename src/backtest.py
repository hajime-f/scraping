from datetime import datetime, timedelta, timezone
from tqdm import tqdm
import pandas as pd
import sqlite3

nikkei225 = [1332, 1333, 1605, 1721, 1801, 1802, 1803, 1808, 1812, 1925, 1928, 1963, 2002, 2269, 2282, 2501, 2502, 2503, 2531, 2801, 2802, 2871, 2914, 3101, 3103, 3401, 3402, 3861, 3863, 3405, 3407, 4004, 4005, 4021, 4042, 4043, 4061, 4063, 4183, 4188, 4208, 4452, 4631, 4901, 4911, 6988, 4151, 4502, 4503, 4506, 4523, 4503, 4506, 4568, 4519, 5020, 5019, 5108, 5101, 5332, 5214, 5333, 5233, 5301, 5202, 5201, 5232, 5401, 5406, 5411, 5541, 5801, 5802, 5703, 5803, 3436, 5711, 5714, 5713, 5706, 5707, 6367, 7011, 6103, 6302, 6301, 5631, 7013, 6305, 6361, 6113, 6473, 6326, 6471, 7004, 6472, 6861, 8035, 6954, 6758, 7735, 6504, 6902, 6702, 6857, 6501, 6981, 6479, 6971, 6701, 6762, 6724, 6976, 6674, 6770, 6506, 7752, 6503, 6752, 6753, 6841, 6703, 6952, 7751, 6645, 7012, 7003, 7269, 7267, 7270, 7272, 7203, 7261, 7202, 7211, 7201, 7205, 4543, 7731, 4902, 7733, 7762, 7951, 7911, 7912, 7832, 8058, 8015, 8001, 8053, 2768, 8002, 8031, 9983, 8267, 8252, 3099, 8233, 3086, 3382, 8309, 8316, 8304, 8354, 8331, 8355, 8411, 7186, 8308, 8306, 8601, 8604, 8628, 8766, 8630, 8725, 8795, 8750, 8591, 8697, 8253, 8801, 8830, 8802, 3289, 8804, 9022, 9009, 9001, 9020, 9005, 9007, 9021, 9008, 9147, 9064, 9104, 9101, 9107, 9202, 9301, 9433, 9432, 9984, 9434, 9613, 9503, 9501, 9502, 9532, 9531, 4704, 6098, 4324, 9735, 4689, 9602, 6178, 4751, 4755, 2432, 2413, 3659, 9766, 7974,]

def fetch_data(ref_date, code, column):

    query = f"SELECT {column} FROM DailyCandle WHERE code is {code} AND date > '{ref_date}' ORDER BY date;"
    conn = sqlite3.connect('stocks.db')
    with conn:
        df = pd.read_sql_query(query, conn)
    return df


def is_golden_position(m_ave75, m_ave25, m_ave05, m_flag, d_value):
    
    if (m_ave05 > m_ave25 > m_ave75) and not m_flag and (d_value == 0):
        return True, 1
    elif (m_ave05 > m_ave25 > m_ave75) and m_flag and (d_value == 1):
        return True, 0
    elif (m_ave05 > m_ave25 > m_ave75) and m_flag and (d_value == 0):
        return True, 0
    else:
        if m_flag and (d_value == 0):
            return False, 2
        else:
            return False, 0


def prepare_data(ref_date):
    
    data = []
    
    bar = tqdm(total = len(nikkei225), dynamic_ncols = True, iterable = True, leave = False)
    bar.set_description('データを取得しています')
    
    for code in nikkei225:

        # 各値をデータベースから取得する
        df_date = fetch_data(ref_date, code, 'date, open, high, low')
        df_close = fetch_data(ref_date, code, 'close')
        df_volume = fetch_data(ref_date, code, 'volume')
        
        # 移動平均をそれぞれ計算する
        m_ave75 = df_close.rolling(window = 75).mean().dropna()
        m_ave25 = df_close.rolling(window = 25).mean().dropna()
        m_ave05 = df_close.rolling(window =  5).mean().dropna()

        # データを結合する
        m_data = pd.concat([df_date, df_close, m_ave75, m_ave25, m_ave05, df_volume], axis='columns', join='inner')

        # リストに変換する
        l_data = m_data.values.tolist()

        # 変数 data に追加する
        data.append(l_data)
        
        bar.update(1)

    return data


if __name__ == '__main__':

    # データを用意する
    data = prepare_data('2019-01-01')
    
    for code, m_data in zip(nikkei225, data):
        
        m_flag = False
        d_value = 0
        
        for p_dt in m_data:

            m_flag, d_value = is_golden_position(p_dt[2], p_dt[3], p_dt[4], m_flag, d_value)
            
            if d_value == 1:
                print('+', code)
            elif d_value == 2:
                print('-', code)
            else:
                pass
            
            
            
            
