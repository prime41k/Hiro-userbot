import re
import os
import sys                                                                         import io
import json                                                                        import time
import asyncio
import sqlite3                                                                     import hashlib
import logging
import catch
from collections import defaultdict
from telethon import TelegramClient, events                                        from telethon.errors import (
    FloodWaitError, MessageDeleteForbiddenError,
    UserIsBlockedError, ChatWriteForbiddenError
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("hiro")

CONFIG_FILE = 'config.json'
SESSION_DIR = 'sessions'                                                           DB_FILE = 'hiro.db'

class DB:
    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file, check_same_thread=False, timeout=10.0)
        self.lock = asyncio.Lock()
        self._init_sync()

    def _init_sync(self):                                                                  self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, chat_id INTEGER, text TEXT, file_hash TEXT, date TIMESTAMP);
            CREATE TABLE IF NOT EXISTS favorites (user_id INTEGER, fav_user_id INTEGER, UNIQUE(user_id, fav_user_id));
            CREATE TABLE IF NOT EXISTS ghost_mode (user_id INTEGER PRIMARY KEY, enabled BOOLEAN DEFAULT 0);
            CREATE TABLE IF NOT EXISTS muted (user_id INTEGER, muted_user_id INTEGER, UNIQUE(user_id, muted_user_id));
            CREATE TABLE IF NOT EXISTS safe_chats (chat_id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE IF NOT EXISTS banned_hashes (hash TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS blacklist (user_id INTEGER PRIMARY KEY);
        """)
        self.conn.commit()

    async def _exec(self, query, args=(), fetch=False):
        async with self.lock:
            cur = await asyncio.to_thread(self.conn.execute, query, args)
            if fetch:
                return await asyncio.to_thread(cur.fetchall)
            await asyncio.to_thread(self.conn.commit)
            return cur.lastrowid

    async def trim_history(self):
        await self._exec("DELETE FROM messages WHERE id NOT IN (SELECT id FROM messages ORDER BY date DESC LIMIT 500)")

    async def vacuum(self):
        await asyncio.to_thread(self.conn.execute, "VACUUM")
        await asyncio.to_thread(self.conn.commit)

    async def save_msg(self, uid, cid, text, fhash=None):
        await self._exec("INSERT INTO messages (user_id, chat_id, text, file_hash, date) VALUES (?, ?, ?, ?, datetime('now'))", (uid, cid, text, fhash))
        await self.trim_history()

    async def is_safe(self, cid):                                                          r = await self._exec("SELECT 1 FROM safe_chats WHERE chat_id = ?", (cid,), fetch=True)
        return bool(r)
                                                                                       async def add_safe(self, cid, name):
        await self._exec("INSERT OR IGNORE INTO safe_chats VALUES (?, ?)", (cid, name))

    async def is_muted(self, uid, target):
        r = await self._exec("SELECT 1 FROM muted WHERE user_id = ? AND muted_user_id = ?", (uid, target), fetch=True)
        return bool(r)
                                                                                       async def add_mute(self, uid, target):
        await self._exec("INSERT OR IGNORE INTO muted VALUES (?, ?)", (uid, target))

    async def remove_mute(self, uid, target):
        await self._exec("DELETE FROM muted WHERE user_id = ? AND muted_user_id = ?", (uid, target))

    async def is_ghost(self, uid):
        r = await self._exec("SELECT enabled FROM ghost_mode WHERE user_id = ?", (uid,), fetch=True)
        return bool(r and r[0][0])

    async def toggle_ghost(self, uid):
        await self._exec("INSERT OR REPLACE INTO ghost_mode VALUES (?, CASE WHEN (SELECT enabled FROM ghost_mode WHERE user_id = ?) = 1 THEN 0 ELSE 1 END)", (uid, uid))

    async def is_fav(self, uid, fid):
        r = await self._exec("SELECT 1 FROM favorites WHERE user_id = ? AND fav_user_id = ?", (uid, fid), fetch=True)                                                         return bool(r)

    async def add_fav(self, uid, fid):
        await self._exec("INSERT OR IGNORE INTO favorites VALUES (?, ?)", (uid, fid))

    async def remove_fav(self, uid, fid):
        await self._exec("DELETE FROM favorites WHERE user_id = ? AND fav_user_id = ?", (uid, fid))

    async def get_stats(self, uid):
        msgs = await self._exec("SELECT COUNT(*) FROM messages WHERE user_id = ?", (uid,), fetch=True)
        favs = await self._exec("SELECT COUNT(*) FROM favorites WHERE user_id = ?", (uid,), fetch=True)
        mutes = await self._exec("SELECT COUNT(*) FROM muted WHERE user_id = ?", (uid,), fetch=True)
        return {'messages': msgs[0][0], 'favorites': favs[0][0], 'muted': mutes[0][0]}

    async def export_data(self):
        msgs = await self._exec("SELECT * FROM messages", fetch=True)
        favs = await self._exec("SELECT * FROM favorites", fetch=True)
        mutes = await self._exec("SELECT * FROM muted", fetch=True)
        return {'messages': msgs, 'favorites': favs, 'muted': mutes}               
    async def is_hash_banned(self, fhash):
        r = await self._exec("SELECT 1 FROM banned_hashes WHERE hash = ?", (fhash,), fetch=True)
        return bool(r)

    async def ban_hash(self, fhash):
        await self._exec("INSERT OR IGNORE INTO banned_hashes VALUES (?)", (fhash,))
                                                                                       async def is_blacklisted(self, uid):
        r = await self._exec("SELECT 1 FROM blacklist WHERE user_id = ?", (uid,), fetch=True)
        return bool(r)

    async def add_blacklist(self, uid):
        await self._exec("INSERT OR IGNORE INTO blacklist VALUES (?)", (uid,))

    async def remove_blacklist(self, uid):
        await self._exec("DELETE FROM blacklist WHERE user_id = ?", (uid,))

def get_api_credentials():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            cfg = json.load(f)
            return int(cfg['api_id']), cfg['api_hash']

    if not sys.stdin.isatty():
        logger.error("TTY required for first run. Provide config.json manually.")
        sys.exit(1)

    while True:
        try:
            api_id = int(input("API ID: "))
            break
        except ValueError:
            print("API ID must be an integer.")

    while True:
        api_hash = input("API Hash: ").strip()
        if re.fullmatch(r'[0-9a-fA-F]{32}', api_hash):
            break
        print("API Hash must be exactly 32 hex characters.")

    with open(CONFIG_FILE, 'w') as f:
        json.dump({'api_id': api_id, 'api_hash': api_hash}, f)
    return api_id, api_hash

TRIGGERS = {
    r'(?<!\w)привет(?!\w)': "Здарова. Я Hiro. Не трать мое время.",
    r'(?<!\w)кто ты(?!\w)': "Я цифровой господь этого чата.",
    r'(?<!\w)статус(?!\w)': "Система стабильна. Режим доминирования активен."
}

SPAM_LIMIT = 5
SPAM_WINDOW = 3
spam_tracker = defaultdict(list)

async def safe_delete(client, chat_id, msg_ids):
    try:
        await client.delete_messages(chat_id, msg_ids)
    except MessageDeleteForbiddenError:
        pass
    except Exception as e:
        logger.warning(f"Delete failed: {e}")
                                                                                   async def auto_delete(client, chat_id, msg_id, delay=15):
    await asyncio.sleep(delay)
    await safe_delete(client, chat_id, msg_id)

async def safe_send(client, chat_id, text, auto_delete_delay=0, **kwargs):
    while True:
        try:
            msg = await client.send_message(chat_id, text, **kwargs)                           if auto_delete_delay > 0:
                asyncio.create_task(auto_delete(client, chat_id, msg.id, auto_delete_delay))
            return msg
        except FloodWaitError as e:
            if e.seconds > 300:
                logger.error(f"FloodWait too long: {e.seconds}s. Aborting.")
                return None
            logger.warning(f"FloodWait: sleeping {e.seconds}s")
            await asyncio.sleep(e.seconds + 1)
        except (UserIsBlockedError, ChatWriteForbiddenError):
            return None
        except Exception as e:
            logger.error(f"Send failed: {e}")                                                  return None

async def cleanup_spam_tracker():
    while True:
        await asyncio.sleep(3600)
        spam_tracker.clear()
        logger.info("Spam tracker cleared")

async def main():
    api_id, api_hash = get_api_credentials()
    os.makedirs(SESSION_DIR, exist_ok=True)

    client = TelegramClient(f'{SESSION_DIR}/hiro_session', api_id, api_hash)
    await client.start()
    me = await client.get_me()
    my_id = me.id
    db = DB(DB_FILE)

    logger.info(f"Hiro started. ID: {my_id}")
    asyncio.create_task(cleanup_spam_tracker())

    @client.on(events.NewMessage(outgoing=False))
    @catch.async_catch
    async def msg_handler(event):
        sender = await event.get_sender()
        if not sender or not sender.id:
            return

        cid = event.chat_id
        uid = sender.id                                                            
        if await db.is_safe(cid):
            return

        if await db.is_blacklisted(uid) or await db.is_muted(my_id, uid):
            await safe_delete(client, cid, event.id)
            return

        if await db.is_ghost(my_id) and not await db.is_fav(my_id, uid):
            return

        text = event.text or ""
        fhash = None

        if event.media and hasattr(event.media, 'document'):
            doc = event.media.document
            if doc.size < 5 * 1024 * 1024:
                try:
                    media_bytes = await client.download_media(doc, bytes)
                    fhash = hashlib.sha256(media_bytes).hexdigest()
                    if await db.is_hash_banned(fhash):
                        await safe_delete(client, cid, event.id)
                        await db.add_mute(my_id, uid)
                        logger.info(f"Auto-banned user {uid} via hash {fhash[:16]}")
                        return
                except Exception:
                    pass

        await db.save_msg(uid, cid, text, fhash)

        if await db.is_fav(my_id, uid):
            await client.forward_messages(me.id, event.message)

        now = time.time()
        spam_tracker[uid] = [t for t in spam_tracker[uid] if now - t < SPAM_WINDOW]        spam_tracker[uid].append(now)

        if len(spam_tracker[uid]) > SPAM_LIMIT:
            await db.add_mute(my_id, uid)
            await safe_send(client, cid, f"User {uid} muted for spam.", auto_delete_delay=15)
            logger.info(f"Auto-muted {uid} for spam")
            spam_tracker[uid] = []

        if not spam_tracker[uid]:
            del spam_tracker[uid]

        lower_text = text.lower()
        for pattern, response in TRIGGERS.items():
            if re.search(pattern, lower_text):
                await safe_send(client, cid, response, auto_delete_delay=15)
                break

    @client.on(events.NewMessage(pattern=r'^/(start|help)(?:@\w+)?(?:\s|$)'))
    @catch.async_catch
    async def help_handler(event):
        text = (
            "Hiro Userbot v7.0\n"
            "/start, /help - Справка\n"
            "/ping - Задержка\n"
            "/mute, /unmute - Мут (reply/@)\n"
            "/blacklist, /unblacklist - Глобальный бан\n"
            "/ghost - Режим призрака\n"
            "/fav add/remove - Избранное\n"                                                    "/safe - Безопасный чат\n"
            "/echo - Повтор\n"
            "/stats - Статистика\n"
            "/export - Выгрузка БД\n"                                                          "/banhash - Забанить хеш файла (reply)\n"
            "/eval - Выполнить Python код\n"
            "/vacuum - Сжать БД"
        )
        await safe_send(client, event.chat_id, text, auto_delete_delay=15)         
    @client.on(events.NewMessage(pattern=r'^/ping(?:@\w+)?(?:\s|$)'))
    @catch.async_catch
    async def ping_handler(event):
        start = time.time()
        msg = await safe_send(client, event.chat_id, "Вычисление...")
        if msg:
            latency = int((time.time() - start) * 1000)
            await msg.edit(f"Pong. {latency}ms")
            asyncio.create_task(auto_delete(client, event.chat_id, msg.id, 15))
                                                                                       @client.on(events.NewMessage(pattern=r'^/echo(?:@\w+)?\s+(.+)'))
    @catch.async_catch
    async def echo_handler(event):
        await safe_send(client, event.chat_id, event.pattern_match.group(1), auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/stats(?:@\w+)?(?:\s|$)'))
    @catch.async_catch
    async def stats_handler(event):
        if event.sender_id != my_id: return
        stats = await db.get_stats(my_id)
        await safe_send(client, event.chat_id, f"Msgs: {stats['messages']} | Fav: {stats['favorites']} | Muted: {stats['muted']}", auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/export(?:@\w+)?(?:\s|$)'))
    @catch.async_catch
    async def export_handler(event):
        if event.sender_id != my_id: return                                                data = await db.export_data()
        filename = f"hiro_export_{int(time.time())}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, default=str)
        await client.send_file(event.chat_id, filename, caption="DB Export")               os.remove(filename)

    @client.on(events.NewMessage(pattern=r'^/mute(?:@\w+)?(?:\s|$)'))
    @catch.async_catch
    async def mute_handler(event):
        if event.sender_id != my_id: return
        target = None
        if event.reply_to_msg_id:
            reply = await event.get_reply_message()
            target = reply.sender_id
        else:                                                                                  match = re.search(r'@(\w+)', event.text)
            if match:
                entity = await client.get_entity(match.group(1))
                target = entity.id
                                                                                           if target:
            await db.add_mute(my_id, target)
            await safe_send(client, event.chat_id, f"Muted {target}", auto_delete_delay=15)
            logger.info(f"Admin {my_id} muted {target}")

    @client.on(events.NewMessage(pattern=r'^/unmute(?:@\w+)?(?:\s|$)'))
    @catch.async_catch
    async def unmute_handler(event):
        if event.sender_id != my_id: return
        target = None
        if event.reply_to_msg_id:
            reply = await event.get_reply_message()
            target = reply.sender_id
        else:
            match = re.search(r'@(\w+)', event.text)
            if match:
                entity = await client.get_entity(match.group(1))
                target = entity.id

        if target:
            await db.remove_mute(my_id, target)
            await safe_send(client, event.chat_id, f"Unmuted {target}", auto_delete_delay=15)
            logger.info(f"Admin {my_id} unmuted {target}")

    @client.on(events.NewMessage(pattern=r'^/blacklist(?:@\w+)?(?:\s|$)'))
    @catch.async_catch
    async def blacklist_handler(event):
        if event.sender_id != my_id: return
        target = None
        if event.reply_to_msg_id:
            reply = await event.get_reply_message()
            target = reply.sender_id
        else:
            match = re.search(r'@(\w+)', event.text)
            if match:
                entity = await client.get_entity(match.group(1))
                target = entity.id

        if target:
            await db.add_blacklist(target)
            await safe_send(client, event.chat_id, f"Blacklisted {target}", auto_delete_delay=15)                                                                                 logger.info(f"Admin {my_id} blacklisted {target}")
                                                                                       @client.on(events.NewMessage(pattern=r'^/unblacklist(?:@\w+)?(?:\s|$)'))
    @catch.async_catch
    async def unblacklist_handler(event):                                                  if event.sender_id != my_id: return
        target = None
        if event.reply_to_msg_id:
            reply = await event.get_reply_message()
            target = reply.sender_id                                                       else:
            match = re.search(r'@(\w+)', event.text)
            if match:
                entity = await client.get_entity(match.group(1))
                target = entity.id

        if target:
            await db.remove_blacklist(target)
            await safe_send(client, event.chat_id, f"Unblacklisted {target}", auto_delete_delay=15)
            logger.info(f"Admin {my_id} unblacklisted {target}")                   
    @client.on(events.NewMessage(pattern=r'^/ghost(?:@\w+)?(?:\s|$)'))
    @catch.async_catch
    async def ghost_handler(event):
        if event.sender_id != my_id: return
        await db.toggle_ghost(my_id)
        status = "on" if await db.is_ghost(my_id) else "off"
        await safe_send(client, event.chat_id, f"Ghost {status}", auto_delete_delay=15)                                                                               
    @client.on(events.NewMessage(pattern=r'^/fav(?:@\w+)?\s+(add|remove)\s+@?(\w+)(?:\s|$)'))
    @catch.async_catch
    async def fav_handler(event):
        if event.sender_id != my_id: return
        action, username = event.pattern_match.group(1), event.pattern_match.group(2)
        entity = await client.get_entity(username)
        if action == 'add':
            await db.add_fav(my_id, entity.id)
        else:
            await db.remove_fav(my_id, entity.id)
        await safe_send(client, event.chat_id, f"Favorite {action}ed", auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/safe(?:@\w+)?(?:\s|$)'))
    @catch.async_catch
    async def safe_handler(event):
        if event.sender_id != my_id: return
        chat = await event.get_chat()
        name = getattr(chat, 'title', None) or getattr(chat, 'first_name', '') or str(event.chat_id)
        await db.add_safe(event.chat_id, name)
        await safe_send(client, event.chat_id, f"Safe chat added: {name}", auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/banhash(?:@\w+)?(?:\s|$)'))
    @catch.async_catch
    async def banhash_handler(event):
        if event.sender_id != my_id: return
        if not event.reply_to_msg_id: return
        reply = await event.get_reply_message()
        if not reply.media or not hasattr(reply.media, 'document'): return
        doc = reply.media.document
        if doc.size > 5 * 1024 * 1024: return
                                                                                           mime = doc.mime_type
        if not mime or not (mime.startswith('image/') or mime.startswith('video/') or mime.startswith('audio/') or mime == 'application/pdf'):
            await safe_send(client, event.chat_id, "Unsupported file type.", auto_delete_delay=15)
            return

        media_bytes = await client.download_media(doc, bytes)
        fhash = hashlib.sha256(media_bytes).hexdigest()
        await db.ban_hash(fhash)
        await safe_send(client, event.chat_id, f"Hash banned: {fhash[:16]}", auto_delete_delay=15)                                                                            logger.info(f"Admin {my_id} banned hash {fhash[:16]}")

    @client.on(events.NewMessage(pattern=r'^/eval(?:@\w+)?\s+(.+)'))
    @catch.async_catch
    async def eval_handler(event):
        if event.sender_id != my_id: return
        code = event.pattern_match.group(1)

        # Безопасная среда выполнения                                                      safe_globals = {"__builtins__": {}}
        old_stdout = sys.stdout
        sys.stdout = mystdout = io.StringIO()
        try:
            exec(code, safe_globals, locals())
            result = mystdout.getvalue()
            if not result:
                result = "Done (no output)"
        except Exception as e:
            result = f"Error: {e}"                                                         finally:
            sys.stdout = old_stdout

        result = result[:4000]
        await safe_send(client, event.chat_id, result, auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/vacuum(?:@\w+)?(?:\s|$)'))
    @catch.async_catch
    async def vacuum_handler(event):
        if event.sender_id != my_id: return
        await safe_send(client, event.chat_id, "Starting VACUUM...", auto_delete_delay=5)
        await db.vacuum()
        await safe_send(client, event.chat_id, "VACUUM completed.", auto_delete_delay=15)

    await client.run_until_disconnected()

if __name__ == '__main__':
    asyncio.run(main())