import catch  
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, PhoneNumberInvalidError
import sqlite3
import json
import os
import asyncio

# КОНФИГУРАЦИЯ ===
CONFIG_FILE = 'config.json'
SESSION_DIR = 'sessions'
DB_FILE = 'hiro.db'
my_id = 0

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    return None

def save_config(api_id, api_hash):
    config = {'api_id': api_id, 'api_hash': api_hash}
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f)
    return config

def get_api_credentials():
    config = load_config()
    if config:
        return config['api_id'], config['api_hash']

    print("Первый запуск. Введите данные:")
    api_id = input("API ID: ")
    api_hash = input("API Hash: ")
    save_config(api_id, api_hash)
    return api_id, api_hash

# === БД ===
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS messages
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, chat_id INTEGER,
                  text TEXT, file_id TEXT, date TIMESTAMP, is_deleted BOOLEAN DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS favorites
                 (user_id INTEGER, fav_user_id INTEGER, UNIQUE(user_id, fav_user_id))''')
    c.execute('''CREATE TABLE IF NOT EXISTS ghost_mode
                 (user_id INTEGER PRIMARY KEY, enabled BOOLEAN DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS muted
                 (user_id INTEGER, muted_user_id INTEGER, UNIQUE(user_id, muted_user_id))''')
    conn.commit()
    conn.close()

def save_message(user_id, chat_id, text, file_id=None):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT INTO messages (user_id, chat_id, text, file_id, date) VALUES (?, ?, ?, ?, datetime('now'))",
              (user_id, chat_id, text, file_id))
    c.execute("""DELETE FROM messages WHERE id IN
                 (SELECT id FROM messages WHERE user_id = ? ORDER BY date DESC LIMIT -1 OFFSET 500)""",
              (user_id,))
    conn.commit()
    conn.close()

def is_favorite(user_id, fav_user_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT 1 FROM favorites WHERE user_id = ? AND fav_user_id = ?", (user_id, fav_user_id))
    result = c.fetchone()
    conn.close()
    return bool(result)

def add_favorite(user_id, fav_user_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO favorites (user_id, fav_user_id) VALUES (?, ?)", (user_id, fav_user_id))
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    finally:
        conn.close()

def remove_favorite(user_id, fav_user_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DELETE FROM favorites WHERE user_id = ? AND fav_user_id = ?", (user_id, fav_user_id))
    conn.commit()
    conn.close()

def is_ghost_mode(user_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT enabled FROM ghost_mode WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
    return bool(result and result[0])

def toggle_ghost_mode(user_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO ghost_mode (user_id, enabled) VALUES (?, CASE WHEN (SELECT enabled FROM ghost_mode WHERE user_id = ?) = 1 THEN 0 ELSE 1 END)",
              (user_id, user_id))
    conn.commit()
    conn.close()

def is_muted(user_id, muted_user_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT 1 FROM muted WHERE user_id = ? AND muted_user_id = ?", (user_id, muted_user_id))
    result = c.fetchone()
    conn.close()
    return bool(result)

def add_muted(user_id, muted_user_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    try:
        c.execute("INSERT INTO muted (user_id, muted_user_id) VALUES (?, ?)", (user_id, muted_user_id))
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    finally:
        conn.close()

# =telethon=
@catch.async_catch  # Правильный декоратор для async функций из твоего модуля
async def main():
    api_id, api_hash = get_api_credentials()
    os.makedirs(SESSION_DIR, exist_ok=True)

    client = TelegramClient(f'{SESSION_DIR}/hiro_session', api_id, api_hash)

    @client.on(events.NewMessage(outgoing=False))
    async def handler(event):
        sender = await event.get_sender()
        if not sender or not sender.id:
            return

        user_id = sender.id
        chat_id = event.chat_id

        if is_muted(my_id, user_id):
            return

        if is_ghost_mode(my_id) and not is_favorite(my_id, user_id):
            return

        text = event.text or ""
        file_id = None
        if event.media and hasattr(event.media, 'document'):
            if event.media.document.size <= 100 * 1024 * 1024:
                file_id = str(event.media.document.id)

        save_message(user_id, chat_id, text, file_id)

    @client.on(events.NewMessage(pattern='/start'))
    @client.on(events.NewMessage(pattern='/help'))
    async def help_handler(event):
        await event.respond("📜 Список команд:\n/start, /help, /mute, /ghost, /fav")
    async def start_handler(event):
        await event.respond("Hiro Userbot запущен. Используй /help для списка команд.")

    @client.on(events.NewMessage(pattern='/mute'))
    async def mute_handler(event):
        if not event.message.reply_to_msg_id and not event.message.entities:
            await event.respond("Используй: /mute @username или ответь на сообщение")
            return

        target_user = None
        if event.message.reply_to_msg_id:
            reply = await event.get_reply_message()
            target_user = reply.sender_id
        elif event.message.entities:
            for entity in event.message.entities:
                if hasattr(entity, 'user_id'):
                    target_user = entity.user_id
                    break

        if target_user:
            add_muted(my_id, target_user)
            await event.respond(f"Пользователь {target_user} замьючен.")
        else:
            await event.respond("Не удалось определить пользователя.")

    @client.on(events.NewMessage(pattern='/ghost'))
    async def ghost_handler(event):
        toggle_ghost_mode(my_id)
        status = "включён" if is_ghost_mode(my_id) else "выключен"
        await event.respond(f"Режим призрака {status}.")

    @client.on(events.NewMessage(pattern='/fav'))
    async def fav_handler(event):
        args = event.text.split()
        if len(args) < 3:
            await event.respond("Используй: /fav add @username или /fav remove @username")
            return

        action = args[1]
        username = args[2].replace('@', '')

        try:
            entity = await client.get_entity(username)
            user_id = entity.id
        except Exception as e:
            await event.respond(f"Ошибка: {str(e)}")
            return

        if action == 'add':
            add_favorite(my_id, user_id)
            await event.respond(f"{username} добавлен в избранное.")
        elif action == 'remove':
            remove_favorite(my_id, user_id)
            await event.respond(f"{username} удалён из избранного.")
        else:
            await event.respond("Неизвестное действие. Используй add или remove.")

    await client.start()
    global my_id
    me = await client.get_me()
    my_id = me.id
    print("Hiro Userbot запущен успешно!")
    await client.run_until_disconnected()

if __name__ == '__main__':
    init_db()
    # Используем catch.run_safe для безопасного запуска асинхронной main()
    catch.run_safe(main())
