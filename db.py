import asyncpg
from datetime import datetime


HOST = "127.0.0.1"
PASSWORD = "postgresEQW1"
PORT = "5432"
USER = "tyr"
DB_NAME = "postgres"

async def fetch_messages(conn):
    return await conn.fetch("SELECT server_id, slave, money, time, sent FROM slave WHERE time = $1", datetime.now().strftime('%H:%M'))

async def my_function():
    await create_tables()
    conn = await asyncpg.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            database=DB_NAME,
            port=PORT
        )
    messages = await fetch_messages(conn)
    for record in messages:
        server_id = record['server']
        result = await conn.fetch(f"SELECT id_vivod, server_id FROM setting WHERE id_pros = '{server_id}'")
        if result:
            sent_value = result[0]['id_vivod']
            server = result[0]['server_id']
            t = record['sent']
            await conn.execute("DELETE FROM slave WHERE sent = $1", t)
            await conn.close()

            w = server
            g = sent_value
            print('Работа и конец функции по передаче')
            return  w, t, g

async def setting1(message):
    await create_tables()
    data = message.text.split()[1:]
    conn = await asyncpg.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DB_NAME,
        port=PORT
    )
    id_pros = data[0]
    id_vivod = data[1]
    server = data[2]
    print(id_pros, id_vivod, server)
    await conn.fetch(f"INSERT INTO setting VALUES ('{id_pros}', '{id_vivod}', '{server}')")
    await conn.close()

async def slave1(message):
    await create_tables()
    data = message.text.split()[1:]
    conn = await asyncpg.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DB_NAME,
        port=PORT
    )
    r = message.reply_to_message
    der = r = message.reply_to_message.message_id
    print(der)
    rows = await conn.fetch("SELECT id_pros, id_vivod, server_id FROM setting WHERE id_pros = $1", str(der))



    if rows:
        slave = data[0]
        money = data[1]
        time = 0
        now = datetime.now()

        hours, minutes = time.split(':')
        hour = int(hours)
        formatted_time = now.replace(hour=hour, minute=00, second=0, microsecond=0)
        formatted_time1 = now.replace(hour=hour, minute=45, second=0, microsecond=0)
        formatted_time2 = now.replace(hour=hour, minute=50, second=0, microsecond=0)
        formatted_time3 = now.replace(hour=hour, minute=54, second=0, microsecond=0)

        formatted_time34 = f"{hours}:54"
        formatted_time23 = f"{hours}:50"
        formatted_time12 = f"{hours}:45"
        formatted_time11 = f"{hours}:00"
        we = rows
        w = we[0].get('id_vivod')
        re = we[0].get('server_id')
        print(w)
        fer = f"@everyone Готовим денег от {money} на {slave}, ставки с {formatted_time11} до {time}"
        k = f'@everyone Начался аукцион на раба {slave}, ставка от {money}, НЕ СВЕТИМ!!!'
        k1 = f'@everyone 10 минут!!! {slave}, ставка от {money}, НЕ СВЕТИМ!!!'
        k2 = f'@everyone 5 минут!!! {slave}, ставка от {money}, НЕ СВЕТИМ!!!'
        k3 = f'@everyone 1 минута!!! {slave}, ставка от {money}, НЕ СВЕТИМ!!!'

        if formatted_time > now:
            await conn.execute(
                f"INSERT INTO slave VALUES ($1, $2, $3, $4, $5);",
                formatted_time11, slave, money, k, str(der)
            )


        if formatted_time1 > now:
            await conn.execute(
                f"INSERT INTO slave VALUES ($1, $2, $3, $4, $5);",
                formatted_time12, slave, money, k1, str(der)
            )

        if formatted_time2 > now:
            await conn.execute(
             f"INSERT INTO slave VALUES ($1, $2, $3, $4, $5);",
                formatted_time23, slave, money, k2, str(der)
            )

        if formatted_time3 > now:
            await conn.execute(
                f"INSERT INTO slave VALUES ($1, $2, $3, $4, $5);",
                formatted_time34, slave, money, k3, str(der)
            )
        return re, fer, w
    else:
        print(f"Не правильно {der}")


async def create_tables():
    conn = await asyncpg.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DB_NAME,
        port=PORT
    )

    exists = await conn.fetchval("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'setting'
        );
    """)

    if not exists:
        create_table_query = """
            CREATE TABLE setting (
                id_pros varchar(50),
                id_vivod varchar(50),
                server_id varchar(50)
            );
        """
        await conn.execute(create_table_query)

    exists = await conn.fetchval("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'slave'
        );
    """)

    if not exists:
        create_table_query = """
            CREATE TABLE slave (  -- ← тут была ошибка: вы создавали таблицу "setting" вместо "slave"
                server_id varchar(50),
                slave varchar(50),
                money varchar(50),
                time varchar(50),
                sent varchar(50)
            );
        """
        await conn.execute(create_table_query)

    await conn.close()
    print("✅ Таблицы проверены и созданы при необходимости.")