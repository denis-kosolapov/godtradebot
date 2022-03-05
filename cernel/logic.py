import asyncio
from datetime import datetime
from cernel.DataBase import DataBase
import json
import requests
import requests.exceptions


class DatabaseManagement:
    def __init__(self):
        super(DatabaseManagement, self).__init__()

        self.host = 'localhost'
        self.user = 'root'
        self.password = 'binancepassword'

        self.base_telegram_bot = DataBase(self.host, self.user, self.password, 'base_telegram_bot')


class LogicBot(DatabaseManagement):
    def __init__(self):
        super(LogicBot, self).__init__()

    async def get_signal(self):
        try:
            # url_list = [
            #     'http://127.0.0.1:8000/pump_pr',
            #     'http://127.0.0.1:8000/dump_pr',
            #     'http://127.0.0.1:8000/pump_rt',
            #     'http://127.0.0.1:8000/dump_rt'
            # ]
            url_list = [
                'http://godtrade.ru/pump_pr',
                'http://godtrade.ru/dump_pr',
                'http://godtrade.ru/pump_rt',
                'http://godtrade.ru/dump_rt'
            ]
            results = []
            for url in url_list:
                response = requests.get(url)
                json_string = response.json()
                values = json.loads(json_string)
                if values is not None:
                    for value in values:
                        results.append(value)
            return results

        except IOError:
            if requests.exceptions.ConnectionError():
                print('error connecting to the godtrade.ru server')

    def delta(self, time1: datetime, time2: datetime):
        delta = time1 - time2
        return int(delta.seconds)

    async def signals(self):
        api_dict = await self.get_signal()

        data_in_website = []
        if api_dict is not None:
            for signal in api_dict:
                data_in_website.append(list(signal.values()))

        data_in_database = self.base_telegram_bot.SelectRows('signals')

        symbols_database = []
        for i in range(len(data_in_database)):
            symbols_database.append(data_in_database[i][1])

        symbols_website = []
        for i in range(len(data_in_website)):
            symbols_website.append(data_in_website[i][0])

        symbol_validation = []
        time_validation = []
        for i in range(len(symbols_website)):
            if symbols_website[i] not in symbols_database:
                symbol_validation.append({
                    "symbol": data_in_website[i][0],
                    "percent_rate": data_in_website[i][1],
                    "time_of_add": data_in_website[i][2],
                    "lifetime": data_in_website[i][3],
                    "current_price": data_in_website[i][4],
                    "strategy": data_in_website[i][5],
                })
            if symbols_website[i] in symbols_database:
                symbol = await self.time_validation(data_in_website[i])
                time_validation.append(symbol)

        signals = []

        if symbol_validation is not None:
            dataset = []
            for data in symbol_validation:
                signals.append(data)
                result = list(data.values())
                dataset.append([result[0], result[1], datetime.strptime(result[2], "%Y-%m-%d %H:%M:%S"), result[3], result[4], result[5]])
            self.base_telegram_bot.WriteDataToDatabase(self.base_telegram_bot, dataset, 'signals')

        if None not in time_validation:
            dataset = []
            for data in time_validation:
                signals.append(data)
                result = list(data.values())
                dataset.append([result[0], result[1], result[2], result[3], result[4]])
            self.base_telegram_bot.WriteDataToDatabase(self.base_telegram_bot, dataset, 'signals')

        return signals

    async def time_validation(self, symbol: list):
        data_in_database = self.base_telegram_bot.SelectRows('signals')

        valid_data_database = []
        for data in data_in_database:
            valid_data_database.append([data[1], data[3]])

        valid_data_website = []
        datetime_website = datetime.strptime(symbol[2], "%Y-%m-%d %H:%M:%S")
        valid_data_website.append([symbol[0], symbol[1], datetime_website, symbol[3], symbol[4], symbol[5]])

        for i in range(len(valid_data_database)):
            if valid_data_website[0][0] in valid_data_database[i]:
                delta = self.delta(valid_data_website[0][2], valid_data_database[i][1])
                if delta > 300:
                    self.base_telegram_bot.DropRow('signals', "symbol", symbol[0])
                if delta > 300:
                    time_validation = {
                        "symbol": valid_data_website[0][0],
                        "percent_rate": valid_data_website[0][1],
                        "time_of_add": valid_data_website[0][2],
                        "lifetime": valid_data_website[0][3],
                        "current_price": valid_data_website[0][4],
                        "strategy": valid_data_website[0][5],
                    }
                    return time_validation

    # def async_trade_loop(self):
    #     loop = asyncio.get_event_loop()
    #     tasks = [
    #         # loop.create_task(self.get_signal()),
    #         # loop.create_task(self.time_validation()),
    #         # loop.create_task(self.symbol_validation()),
    #         loop.create_task(self.signals()),
    #     ]
    #     wait_tasks = asyncio.wait(tasks)
    #     loop.run_until_complete(wait_tasks)


# bot = LogicBot()
# bot.signals()
# while True:
#     bot = LogicBot()
#     bot.async_trade_loop()
#     time.sleep(1)