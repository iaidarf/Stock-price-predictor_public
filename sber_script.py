# Загрузка библиотек

# данные
import pandas as pd
pd.set_option('display.float_format', lambda x: '%.3f' % x)
import time, datetime

# загрузка модели
from joblib import load

# телеграм-бот
import telebot

# токен для телеграм-бота
token='...'
# chat_id
chat_id = '...'
# название бумаги
stock = 'SBER'
# названия колонок-предикторов
X_columns = ['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOL', 'DIF_O_C', 'DIF_H_L', 'MEAN_2', 
       'MEAN_3', 'MEAN_4', 'MEAN_5', 'HIGH_2', 'HIGH_3', 'HIGH_4', 'HIGH_5',
       'LOW_2', 'LOW_3', 'LOW_4', 'LOW_5', 'OPEN-1', 'HIGH-1', 'LOW-1',
       'CLOSE-1', 'VOL-1', 'OPEN-2', 'HIGH-2', 'LOW-2', 'CLOSE-2', 'VOL-2',
       'OPEN-3', 'HIGH-3', 'LOW-3', 'CLOSE-3', 'VOL-3', 'OPEN_TODAY',
       'DAY_OF_WEEK_0', 'DAY_OF_WEEK_1', 'DAY_OF_WEEK_2', 'DAY_OF_WEEK_3',
       'DAY_OF_WEEK_4', 'DAY_OF_WEEK_5', 'DAY_OF_WEEK_6', 'MONTH_1', 'MONTH_2',
       'MONTH_3', 'MONTH_4', 'MONTH_5', 'MONTH_6', 'MONTH_7', 'MONTH_8',
       'MONTH_9', 'MONTH_10', 'MONTH_11', 'MONTH_12']

# Feature engineering function

def features(data):

    # день недели
    data['DAY_OF_WEEK'] = data['DATE'].dt.dayofweek

    # месяц
    data['MONTH'] = data['DATE'].dt.month
    
    # разность между ценой открытия и закрытия
    data['DIF_O_C'] = data['OPEN'] - data['CLOSE']
    
    # разность между максимальной и минимальной ценой
    data['DIF_H_L'] = data['HIGH'] - data['LOW']

    # средние цены закрытия за ... дней
    data['MEAN_2'] = data['CLOSE'].rolling(window=2, center=False).mean()
    data['MEAN_3'] = data['CLOSE'].rolling(window=3, center=False).mean()
    data['MEAN_4'] = data['CLOSE'].rolling(window=4, center=False).mean()
    data['MEAN_5'] = data['CLOSE'].rolling(window=5, center=False).mean()

    # максимальные цены за ... дней
    data['HIGH_2'] = data['HIGH'].rolling(window=2, center=False).max()
    data['HIGH_3'] = data['HIGH'].rolling(window=3, center=False).max()
    data['HIGH_4'] = data['HIGH'].rolling(window=4, center=False).max()
    data['HIGH_5'] = data['HIGH'].rolling(window=5, center=False).max()

    # минимальные цены за ... дней
    data['LOW_2'] = data['LOW'].rolling(window=2, center=False).min()
    data['LOW_3'] = data['LOW'].rolling(window=3, center=False).min()
    data['LOW_4'] = data['LOW'].rolling(window=4, center=False).min()
    data['LOW_5'] = data['LOW'].rolling(window=5, center=False).min()
    
    # цены и объем за прошлые дни
    data[['OPEN-1', 'HIGH-1', 'LOW-1', 'CLOSE-1', 'VOL-1']] = data[['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOL']].shift(1)
    data[['OPEN-2', 'HIGH-2', 'LOW-2', 'CLOSE-2', 'VOL-2']] = data[['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOL']].shift(2)
    data[['OPEN-3', 'HIGH-3', 'LOW-3', 'CLOSE-3', 'VOL-3']] = data[['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOL']].shift(3)
    
    # цена открытия сегодня
    data['OPEN_TODAY'] = data['OPEN'].shift(-1)

    return pd.get_dummies(data, columns=['DAY_OF_WEEK', 'MONTH'], prefix=['DAY_OF_WEEK', 'MONTH'])

# Функция для предсказания цены закрытия
"""
Для предсказания сегодняшней цены закрытия нам нужны данные за предыдущие 5 дней плюс сегодняшняя цена открытия.\
Получим их с iss.moex.com
"""

def job():
    
    # вспомогательная переменная, для того чтобы сформировать запрос
    while True:
        try:
            total = pd.read_xml("https://iss.moex.com/iss/history/engines/stock/markets/shares/securities/" + stock, xpath="//row[@TOTAL]").TOTAL[0] 
        except:
            pass 
        else:
            break
    #print('total =', total)    

    # получение данных за прошедшие 5 дней
    while True:
        try:
            h = pd.read_xml("https://iss.moex.com/iss/history/engines/stock/markets/shares/securities/" + stock + "?start=" + str(total-10), xpath="//row")   
        except:
            pass
        else:
            break

    # обработка полученных данных
    h = h[h.BOARDID == 'TQBR'][['SECID', 'TRADEDATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']]
    h.rename(columns={'TRADEDATE':'DATE', 'VOLUME':'VOL'}, inplace=True)
    h.DATE = pd.to_datetime(h.DATE)
    #print('h=', h) 

    # Feature engineering
    h = features(h)
 
    # для предсказания сохраним одну строку, а также удалим ненужные колонки
    z = h.drop(columns=['SECID', 'DATE']).tail(1)
    #print('z=', z)  

    # Часть признаков (дни недели) могут не присутствовать в новых данных
    # Сравним признаки и сохраним набор недостающих
    missing_cols = set(X_columns) - set(z.columns )
    # Добавим их в новые данные и заполним нулями
    for c in missing_cols:
        z[c] = 0
    # Приведем порядок признаков в соотвествие
    z = z[X_columns] 

    # текущая информация по бумаге
    while True:
        try:
            t = pd.read_xml("https://iss.moex.com/iss/engines/stock/markets/shares/securities/" + stock + ".xml=", xpath="//row") 
            # цена сегодняшнего открытия
            open_today = t[t.BOARDID == 'TQBR']['OPEN'].dropna().values[0]
            z['OPEN_TODAY'] = open_today
        except:
            pass
        else:
            break
        time.sleep(5)
    #print('open_today =', open_today)

    # загрузка моделей
    model_close = load('./saved_models/sber_model_close.joblib')
    model_high = load('./saved_models/sber_model_high.joblib')
    model_low = load('./saved_models/sber_model_low.joblib')

    # предсказания по ценам
    close_pred = round(*model_close.predict(z), 2)
    high_pred = round(*model_high.predict(z), 2)
    low_pred = round(*model_low.predict(z), 2)
    #print(f'CLOSE = {close_pred}')
    #print(f'HIGH = {high_pred}')
    #print(f'LOW = {low_pred}')
    # покупать или продавать
    if close_pred > open_today:
        long_short = 'LONG'
    elif close_pred < open_today:
        long_short = 'SHORT'
    #print(long_short)

    # отправка сообщения
    bot.send_message(chat_id, 
                      f'Сегодня: {datetime.date.today()} \n'
                      f'OPEN TODAY = {open_today} \n'
                      f'PREDICT: \n'
                      f'CLOSE = {close_pred} \n'
                      f'HIGH = {high_pred} \n'
                      f'LOW = {low_pred} \n'
                      f'long_short = {long_short}')

def script():
    
    # сегодняшняя дата
    today_date = f'{datetime.datetime.now():%Y-%m-%d}'
    
    # рабочие дни на бирже пн - пт
    # также могут быть переносы рабочих дней, праздничные дни и пр.
    # поэтому перед запуском модели проверяем рабочий ли сегодня день на бирже
    while True:
        try:
            # смотрим переносы рабочих дней, праздничные выходные и пр.
            is_work_day = pd.read_xml("https://iss.moex.com/iss/engines/stock/" + stock + ".xml=", xpath="//row[@date]")
        except:
            pass
        else:
            break
        time.sleep(5)

    # если (день будний и нет исключений) или (день выходной, но есть исключения), то запускаем функцию    
    if datetime.datetime.today().weekday() in [0,1,2,3,4] and len(is_work_day[is_work_day.date == today_date].is_work_day)==0 \
        or datetime.datetime.today().weekday() in [5,6] and len(is_work_day[is_work_day.date == today_date].is_work_day)>0:
        #print('Сегодня на бирже рабочий день')  
        job()

# Телеграм-бот

bot = telebot.TeleBot(token)

# Запуск скрипта

script()