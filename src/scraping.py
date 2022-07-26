from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
import pandas as pd
import urllib.request, sqlite3, time, jpholiday

def fetch_stock_data(code):
    
    url = 'https://www.nikkei.com/nkd/company/?scode=' + str(code)
    req = urllib.request.Request(url, method='GET')
    
    try:
        with urllib.request.urlopen(req) as res:
            content = res.read().decode('utf-8')
    except urllib.error.HTTPError as e:
        exit('\033[31m'+ str(e) + '\033[0m')
    except Exception as e:
        exit('\033[31m' + str(e) + '\033[0m')
    
    soup = BeautifulSoup(content, 'html.parser')
    values = soup.find_all("span", class_='m-stockInfo_detail_value')

    try:
        open_price = float(values[0].text.translate(str.maketrans({'円' : '', ',' : ''})))
        highest_price = float(values[1].text.translate(str.maketrans({'円' : '', ',' : ''})))
        lowest_price = float(values[2].text.translate(str.maketrans({'円' : '', ',' : ''})))
        close_price = float(soup.find("dd", class_='m-stockPriceElm_value now').text.translate(str.maketrans({'円' : '', ',' : ''})))
        volume = int(values[3].text.translate(str.maketrans({'株' : '', ',' : ''})))
    except IndexError:
        print(f'銘柄コード{code}のデータを取得できませんでした。')
    
    return open_price, highest_price, lowest_price, close_price, volume


if __name__ == '__main__':

    JST = timezone(timedelta(hours=+9), 'JST')
    d = datetime.now(JST)
    if d.weekday() >= 5 or jpholiday.is_holiday(d):
        exit('本日は土日祝のため市場は開いていません。')
    
    nikkei225 = [1332, 1333, 1605, 1721, 1801, 1802, 1803, 1808, 1812, 1925, 1928, 1963, 2002, 2269, 2282, 2501, 2502, 2503, 2531, 2801, 2802, 2871, 2914, 3101, 3103, 3401, 3402, 3861, 3863, 3405, 3407, 4004, 4005, 4021, 4042, 4043, 4061, 4063, 4183, 4188, 4208, 4452, 4631, 4901, 4911, 6988, 4151, 4502, 4503, 4506, 4523, 4503, 4506, 4568, 4519, 5020, 5019, 5108, 5101, 5332, 5214, 5333, 5233, 5301, 5202, 5201, 5232, 5401, 5406, 5411, 5541, 5801, 5802, 5703, 5803, 3436, 5711, 5714, 5713, 5706, 5707, 6367, 7011, 6103, 6302, 6301, 5631, 7013, 6305, 6361, 6113, 6473, 6326, 6471, 7004, 6472, 6861, 8035, 6954, 6758, 7735, 6504, 6902, 6702, 6857, 6501, 6981, 6479, 6971, 6701, 6762, 6724, 6976, 6674, 6770, 6506, 7752, 6503, 6752, 6753, 6841, 6703, 6952, 7751, 6645, 7012, 7003, 7269, 7267, 7270, 7272, 7203, 7261, 7202, 7211, 7201, 7205, 4543, 7731, 4902, 7733, 7762, 7951, 7911, 7912, 7832, 8058, 8015, 8001, 8053, 2768, 8002, 8031, 9983, 8267, 8252, 3099, 8233, 3086, 3382, 8309, 8316, 8304, 8354, 8331, 8355, 8411, 7186, 8308, 8306, 8601, 8604, 8628, 8766, 8630, 8725, 8795, 8750, 8591, 8697, 8253, 8801, 8830, 8802, 3289, 8804, 9022, 9009, 9001, 9020, 9005, 9007, 9021, 9008, 9147, 9064, 9104, 9101, 9107, 9202, 9301, 9433, 9432, 9984, 9434, 9613, 9503, 9501, 9502, 9532, 9531, 4704, 6098, 4324, 9735, 4689, 9602, 6178, 4751, 4755, 2432, 2413, 3659, 9766, 7974,]    
    
    index_date = datetime.now(JST).strftime('%Y-%m-%d 00:00:00')
    columns = ['code', 'date', 'open', 'high', 'low', 'close', 'volume']
    daily_candle_data = pd.DataFrame(index = [], columns = columns)
    
    for nk in nikkei225:
        open_price, highest_price, lowest_price, close_price, volume = fetch_stock_data(nk)
        daily_candle_data.loc[index_date] = {'code': nk,
                                             'date': index_date,
                                             'open': open_price,
                                             'high': highest_price,
                                             'low': lowest_price,
                                             'close': close_price,
                                             'volume': volume,}
        conn = sqlite3.connect('/Users/hajime-f/Development/private/scraping/stocks.db')
        with conn:
            daily_candle_data.to_sql('DailyCandle', conn, if_exists='append', index=None)
        print(f'{nk} done.')
        time.sleep(0.2)
        
        
        
