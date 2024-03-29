import pandas as pd
import sqlite3

if __name__ == '__main__':

    nikkei225 = [1332, 1333, 1605, 1721, 1801, 1802, 1803, 1808, 1812, 1925, 1928, 1963, 2002, 2269, 2282, 2501, 2502, 2503, 2531, 2801, 2802, 2871, 2914, 3101, 3103, 3401, 3402, 3861, 3863, 3405, 3407, 4004, 4005, 4021, 4042, 4043, 4061, 4063, 4183, 4188, 4208, 4452, 4631, 4901, 4911, 6988, 4151, 4502, 4503, 4506, 4523, 4503, 4506, 4568, 4519, 5020, 5019, 5108, 5101, 5332, 5214, 5333, 5233, 5301, 5202, 5201, 5232, 5401, 5406, 5411, 5541, 5801, 5802, 5703, 5803, 3436, 5711, 5714, 5713, 5706, 5707, 6367, 7011, 6103, 6302, 6301, 5631, 7013, 6305, 6361, 6113, 6473, 6326, 6471, 7004, 6472, 6861, 8035, 6954, 6758, 7735, 6504, 6902, 6702, 6857, 6501, 6981, 6479, 6971, 6701, 6762, 6724, 6976, 6674, 6770, 6506, 7752, 6503, 6752, 6753, 6841, 6703, 6952, 7751, 6645, 7012, 7003, 7269, 7267, 7270, 7272, 7203, 7261, 7202, 7211, 7201, 7205, 4543, 7731, 4902, 7733, 7762, 7951, 7911, 7912, 7832, 8058, 8015, 8001, 8053, 2768, 8002, 8031, 9983, 8267, 8252, 3099, 8233, 3086, 3382, 8309, 8316, 8304, 8354, 8331, 8355, 8411, 7186, 8308, 8306, 8601, 8604, 8628, 8766, 8630, 8725, 8795, 8750, 8591, 8697, 8253, 8801, 8830, 8802, 3289, 8804, 9022, 9009, 9001, 9020, 9005, 9007, 9021, 9008, 9147, 9064, 9104, 9101, 9107, 9202, 9301, 9433, 9432, 9984, 9434, 9613, 9503, 9501, 9502, 9532, 9531, 4704, 6098, 4324, 9735, 4689, 9602, 6178, 4751, 4755, 2432, 2413, 3659, 9766, 7974,]
    
    for nk in nikkei225:
        df = pd.read_csv(f'./csv/{nk}.T.csv', encoding="ms932")
        df = df.drop('終値', axis=1)
        df = df.rename(columns={'日付': 'date', '始値': 'open', '高値': 'high', '安値': 'low', '出来高': 'volume', '調整後終値': 'close'})
        df_date = pd.to_datetime(df['date'], format='%Y/%m/%d')
        df = df.drop('date', axis=1)
        df.insert(loc = 0, column = 'date', value = df_date)
        df['code'] = nk
        df = df.reindex(columns=['code', 'date', 'open', 'high', 'low', 'close', 'volume'])
        conn = sqlite3.connect('stocks.db')
        with conn:
            df.to_sql('DailyCandle', conn, if_exists='append', index=None)
        print(f'{nk} done.')
