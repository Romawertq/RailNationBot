import tracemalloc
from aiogram import Bot, Dispatcher, types, executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
import asyncio
from datetime import datetime
from db import (
    slave1,
    my_function,
    setting1,
    create_tables
)

bot = Bot(token="7457602333:AAFeT6Zm5lAg43no1xvd5zAlEFXcYuWIkU")
dp = Dispatcher(bot, storage=MemoryStorage())
tracemalloc.start()

async def check_time():
    minutes_to_run = [0, 45, 50, 54]
    print('Начало')
    while True:
        now = datetime.now()
        if now.minute in minutes_to_run:
            g = my_function()
            w, t, r = g
            await bot.send_message(chat_id=int(w), text=t, reply_to_message_id=int(r))
            print(f'Запуск функции, время: {now}')
            await asyncio.sleep(60)


@dp.message_handler(commands=['setting'])
async def setting(message: types.Message):
    await setting1(message)
    await message.answer("Я добавил ваши id")

@dp.message_handler(commands=['slave'])
async def slave(message: types.Message):
    g = slave1(message)
    re, fer, w = g
    await bot.send_message(chat_id=int(re), text=fer, reply_to_message_id=int(w))

async def on_startup(dp):
    asyncio.create_task(check_time())

def main():
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)

if __name__ == '__main__':
    asyncio.run(create_tables())
    main()