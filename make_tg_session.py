# -*- coding: utf-8 -*-
"""Запустіть ОДИН раз на своєму комп'ютері:  pip install telethon  &&  python make_tg_session.py
Скрипт попросить api_id / api_hash (https://my.telegram.org -> API development tools), номер телефону й код із Telegram.
Наприкінці він надрукує довгий рядок TG_SESSION — його разом з TG_API_ID і TG_API_HASH потрібно вписати
в Environment на Render. Рядок сесії = доступ до акаунта, нікому його не показуйте
(краще робити сесію на окремому, не основному акаунті)."""
import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession

async def main():
    api_id = int(input("api_id: ").strip())
    api_hash = input("api_hash: ").strip()
    async with TelegramClient(StringSession(), api_id, api_hash) as client:
        await client.start()   # спитає телефон і код
        print("\n===== TG_SESSION (скопіюйте весь рядок) =====\n")
        print(client.session.save())
        print("\n=============================================")

asyncio.run(main())
