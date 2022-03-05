import logging
from cernel import config
from aiogram import Bot, Dispatcher, executor, types
from cernel.logic import LogicBot
import time

# log level
logging.basicConfig(level=logging.INFO)

# bot init
bot = Bot(token=config.TOKEN)
dp = Dispatcher(bot)


# remove new user joined messages
@dp.message_handler(commands=['start'])
async def on_user_joined(message: types.Message):
    await message.reply('hello, send command /startbot for start!', parse_mode='html')


@dp.message_handler(commands=['startbot'])
async def on_user_joined(message: types.Message):
    await message.reply('hello, bot started!', parse_mode='html')
    while True:
        logicbot = LogicBot()
        values = await logicbot.signals()
        if values is not None:
            for signal in values:
                symbol = signal['symbol']
                percent_rate = signal['percent_rate']
                time_of_add = signal['time_of_add']
                lifetime = signal['lifetime']
                current_price = signal['current_price']
                strategy = signal['strategy']
                url = str(f'https://www.binance.com/ru/futures/{symbol}_perpetual')
                text_message = f'{symbol} \n {percent_rate} \n {time_of_add} \n {lifetime} \n {current_price} \n {strategy} \n {url}'
                await message.reply(text_message, parse_mode='html')

        if values is None:
            pass

# tun long-polling
executor.start_polling(dp, skip_updates=False)
