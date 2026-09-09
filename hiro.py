import os
import sys
import io
import json
import time
import asyncio
import sqlite3
import zlib
import logging
import math
import random
import string
import base64
import re
import traceback
from datetime import datetime, timedelta
from typing import Optional
from collections import defaultdict
from telethon import TelegramClient, events, functions, types
from telethon.errors import (FloodWaitError, MessageDeleteForbiddenError, 
                             PeerIdInvalidError, UsernameNotOccupiedError, 
                             ChatWriteForbiddenError, UserIsBlockedError)

# Настройка логирования для вывода только важных событий без лишнего шума
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("hiro")

CONFIG_FILE = 'config.json'
SESSION_DIR = 'sessions'
DB_FILE = 'hiro.db'
MAX_MSG_CACHE = 2000

# Словарь ошибок с понятными описаниями и конкретными решениями для быстрой отладки
ERROR_DICTIONARY = {
    "FloodWaitError": "Лимит запросов превышен. Решение: подождите указанное время перед следующим действием.",
    "PeerIdInvalidError": "Недействительный идентификатор чата или пользователя. Решение: убедитесь, что бот видит этого пользователя.",
    "MessageDeleteForbiddenError": "Отсутствуют права на удаление сообщений. Решение: проверьте права администратора или тип чата.",
    "UsernameNotOccupiedError": "Пользователь с таким именем не существует. Решение: проверьте орфографию юзернейма.",
    "ChatWriteForbiddenError": "Запись в этот чат запрещена. Решение: проверьте ограничения чата или свой бан.",
    "UserIsBlockedError": "Пользователь заблокировал бота. Решение: невозможно отправить сообщение, добавьте в исключения.",
    "TimeoutError": "Время ожидания истекло. Решение: проверьте стабильность интернет-соединения.",
    "OperationalError": "Ошибка базы данных. Решение: проверьте права на запись в файл базы данных или освободите место на диске.",
}

class ZSS:
    # Надежный обработчик ошибок с расшифровкой и предложениями по исправлению
    @staticmethod
    def catch(func):
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                error_type = type(e).__name__
                tb = traceback.extract_tb(e.__traceback__)
                line = tb[-1].lineno if tb else 0
                file = tb[-1].filename.split('/')[-1] if tb else 'unknown'
                
                base_msg = f"[{file}:{line}] {error_type}: {str(e)}"
                fix_suggestion = ERROR_DICTIONARY.get(error_type, "Решение: проверьте входные данные и системные логи.")
                
                logger.error(f"ZSS ERROR: {base_msg}\n-> {fix_suggestion}")
                return None
        return wrapper

class BatchSaver:
    # Простая и надежная реализация батчинга для стабильной записи без сторонних библиотек
    def __init__(self, db, size=100, delay=1.0):
        self.db = db
        self.size = size
        self.delay = delay
        self.queue = []
        self.lock = asyncio.Lock()
        self.task = None

    async def start(self):
        self.task = asyncio.create_task(self._worker())

    async def _worker(self):
        while True:
            await asyncio.sleep(self.delay)
            async with self.lock:
                if not self.queue:
                    continue
                items = self.queue[:]
                self.queue.clear()
            
            if items:
                try:
                    await self.db.save_msg_batch_sync(items)
                except Exception as e:
                    logger.error(f"Batch save failed: {e}")

    async def add(self, item):
        async with self.lock:
            self.queue.append(item)
            if len(self.queue) >= self.size:
                items = self.queue[:]
                self.queue.clear()
                asyncio.create_task(self.db.save_msg_batch_sync(items))

    async def flush(self):
        async with self.lock:
            if self.queue:
                await self.db.save_msg_batch_sync(self.queue[:])
                self.queue.clear()

class DB:
    # Схема базы данных создана для хранения всех необходимых данных без избыточности
    SCHEMA = """
    CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, chat_id INTEGER, text TEXT, file_hash INTEGER, date TIMESTAMP);
    CREATE TABLE IF NOT EXISTS favorites (user_id INTEGER, fav_user_id INTEGER, UNIQUE(user_id, fav_user_id));
    CREATE TABLE IF NOT EXISTS ghost_mode (user_id INTEGER PRIMARY KEY, enabled BOOLEAN DEFAULT 0);
    CREATE TABLE IF NOT EXISTS muted (user_id INTEGER, muted_user_id INTEGER, UNIQUE(user_id, muted_user_id));
    CREATE TABLE IF NOT EXISTS safe_chats (chat_id INTEGER PRIMARY KEY, name TEXT);
    CREATE TABLE IF NOT EXISTS banned_hashes (hash INTEGER PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS blacklist (user_id INTEGER PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS notes (user_id INTEGER, key TEXT, content TEXT, updated TIMESTAMP, PRIMARY KEY(user_id, key));
    CREATE TABLE IF NOT EXISTS afk (user_id INTEGER PRIMARY KEY, reason TEXT, since TIMESTAMP, missed INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS reminders (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, chat_id INTEGER, text TEXT, trigger_time TIMESTAMP, done BOOLEAN DEFAULT 0);
    CREATE TABLE IF NOT EXISTS aliases (user_id INTEGER, alias TEXT, target TEXT, PRIMARY KEY(user_id, alias));
    CREATE TABLE IF NOT EXISTS filters (chat_id INTEGER, pattern TEXT, response TEXT, PRIMARY KEY(chat_id, pattern));
    CREATE TABLE IF NOT EXISTS counters (user_id INTEGER, key TEXT, value INTEGER DEFAULT 0, PRIMARY KEY(user_id, key));
    CREATE TABLE IF NOT EXISTS todo (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, text TEXT, done BOOLEAN DEFAULT 0, created TIMESTAMP);
    """

    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file, check_same_thread=False, timeout=15.0)
        self.conn.row_factory = sqlite3.Row
        self.lock = asyncio.Lock()
        self._init_sync()

    def _init_sync(self):
        self.conn.executescript(self.SCHEMA)
        self.conn.commit()

    async def _exec(self, query, args=(), fetch=False, fetchone=False):
        async with self.lock:
            cur = await asyncio.to_thread(self.conn.execute, query, args)
            if fetchone:
                row = await asyncio.to_thread(cur.fetchone)
                return row
            if fetch:
                return await asyncio.to_thread(cur.fetchall)
            await asyncio.to_thread(self.conn.commit)
            return cur.lastrowid

    async def save_msg_batch_sync(self, items):
        self.conn.executemany(
            "INSERT INTO messages (user_id, chat_id, text, file_hash, date) VALUES (?, ?, ?, ?, datetime('now'))",
            items
        )
        self.conn.commit()

    _cleanup_counter = 0
    _CLEANUP_EVERY = 50

    async def save_msg(self, uid, cid, text, fhash=None):
        # Используем быстрый CRC32 вместо SHA256 для частых операций, как требуется для производительности
        await self.save_msg_batch_sync([(uid, cid, text[:4000] if text else None, fhash)])
        DB._cleanup_counter += 1
        if DB._cleanup_counter >= DB._CLEANUP_EVERY:
            DB._cleanup_counter = 0
            await self._exec(
                "DELETE FROM messages WHERE id < (SELECT MAX(id) - ? FROM messages)",
                (MAX_MSG_CACHE,)
            )

    async def set_note(self, uid, key, content):
        await self._exec("INSERT OR REPLACE INTO notes VALUES (?, ?, ?, datetime('now'))", (uid, key.lower().strip(), content))

    async def get_note(self, uid, key):
        return await self._exec("SELECT content FROM notes WHERE user_id=? AND key=?", (uid, key.lower().strip()), fetchone=True)

    async def del_note(self, uid, key):
        await self._exec("DELETE FROM notes WHERE user_id=? AND key=?", (uid, key.lower().strip()))

    async def list_notes(self, uid):
        rows = await self._exec("SELECT key, updated FROM notes WHERE user_id=? ORDER BY updated DESC", (uid,), fetch=True)
        return [(r['key'], r['updated']) for r in rows] if rows else []

    async def set_afk(self, uid, reason):
        await self._exec("INSERT OR REPLACE INTO afk VALUES (?, ?, datetime('now'), 0)", (uid, reason or "AFK"))

    async def unset_afk(self, uid):
        row = await self._exec("SELECT missed FROM afk WHERE user_id=?", (uid,), fetchone=True)
        missed = row['missed'] if row else 0
        await self._exec("DELETE FROM afk WHERE user_id=?", (uid,))
        return missed

    async def get_afk(self, uid):
        return await self._exec("SELECT reason, since FROM afk WHERE user_id=?", (uid,), fetchone=True)

    async def inc_afk_missed(self, uid):
        await self._exec("UPDATE afk SET missed=missed+1 WHERE user_id=?", (uid,))

    async def add_reminder(self, uid, cid, text, trigger_time):
        return await self._exec("INSERT INTO reminders (user_id, chat_id, text, trigger_time) VALUES (?,?,?,?)",
                                (uid, cid, text, trigger_time))

    async def get_pending_reminders(self):
        return await self._exec("SELECT * FROM reminders WHERE done=0 AND trigger_time<=datetime('now')", fetch=True)

    async def mark_reminder_done(self, rid):
        await self._exec("UPDATE reminders SET done=1 WHERE id=?", (rid,))

    async def list_reminders(self, uid):
        return await self._exec("SELECT id, text, trigger_time, done FROM reminders WHERE user_id=? ORDER BY trigger_time", (uid,), fetch=True)

    async def add_todo(self, uid, text):
        return await self._exec("INSERT INTO todo (user_id, text, created) VALUES (?, ?, datetime('now'))", (uid, text))

    async def toggle_todo(self, uid, tid):
        await self._exec("UPDATE todo SET done=NOT done WHERE id=? AND user_id=?", (tid, uid))

    async def del_todo(self, uid, tid):
        await self._exec("DELETE FROM todo WHERE id=? AND user_id=?", (tid, uid))

    async def list_todo(self, uid, show_done=False):
        if show_done:
            return await self._exec("SELECT * FROM todo WHERE user_id=? ORDER BY created DESC", (uid,), fetch=True)
        return await self._exec("SELECT * FROM todo WHERE user_id=? AND done=0 ORDER BY created DESC", (uid,), fetch=True)

    async def inc_counter(self, uid, key, delta=1):
        await self._exec("INSERT INTO counters VALUES (?,?,?) ON CONFLICT(user_id,key) DO UPDATE SET value=value+?",
                         (uid, key, delta, delta))

    async def get_counter(self, uid, key):
        row = await self._exec("SELECT value FROM counters WHERE user_id=? AND key=?", (uid, key), fetchone=True)
        return row['value'] if row else 0

    async def reset_counter(self, uid, key=None):
        if key:
            await self._exec("DELETE FROM counters WHERE user_id=? AND key=?", (uid, key))
        else:
            await self._exec("DELETE FROM counters WHERE user_id=?", (uid,))

    async def add_filter(self, cid, pattern, response):
        await self._exec("INSERT OR REPLACE INTO filters VALUES (?,?,?)", (cid, pattern.lower(), response))

    async def del_filter(self, cid, pattern):
        await self._exec("DELETE FROM filters WHERE chat_id=? AND pattern=?", (cid, pattern.lower()))

    async def get_filters(self, cid):
        return await self._exec("SELECT pattern, response FROM filters WHERE chat_id=?", (cid,), fetch=True)

    async def match_filter(self, cid, text):
        filters = await self.get_filters(cid)
        if not filters: return None
        lower = text.lower()
        for f in filters:
            if f['pattern'] in lower:
                return f['response']
        return None

    async def set_alias(self, uid, alias, target):
        await self._exec("INSERT OR REPLACE INTO aliases VALUES (?,?,?)", (uid, alias.lower(), target))

    async def get_alias(self, uid, alias):
        row = await self._exec("SELECT target FROM aliases WHERE user_id=? AND alias=?", (uid, alias.lower()), fetchone=True)
        return row['target'] if row else None

    async def del_alias(self, uid, alias):
        await self._exec("DELETE FROM aliases WHERE user_id=? AND alias=?", (uid, alias.lower()))

    async def list_aliases(self, uid):
        return await self._exec("SELECT alias, target FROM aliases WHERE user_id=?", (uid,), fetch=True)

    async def is_safe(self, cid): 
        row = await self._exec("SELECT 1 FROM safe_chats WHERE chat_id=?", (cid,), fetchone=True)
        return bool(row)

    async def add_safe(self, cid, name): 
        await self._exec("INSERT OR IGNORE INTO safe_chats VALUES (?,?)", (cid, name))

    async def remove_safe(self, cid): 
        await self._exec("DELETE FROM safe_chats WHERE chat_id=?", (cid,))

    async def is_muted(self, uid, target): 
        row = await self._exec("SELECT 1 FROM muted WHERE user_id=? AND muted_user_id=?", (uid, target), fetchone=True)
        return bool(row)

    async def add_mute(self, uid, target): 
        await self._exec("INSERT OR IGNORE INTO muted VALUES (?,?)", (uid, target))

    async def remove_mute(self, uid, target): 
        await self._exec("DELETE FROM muted WHERE user_id=? AND muted_user_id=?", (uid, target))

    async def is_ghost(self, uid):
        row = await self._exec("SELECT enabled FROM ghost_mode WHERE user_id=?", (uid,), fetchone=True)
        return bool(row['enabled']) if row else False

    async def toggle_ghost(self, uid):
        await self._exec("INSERT OR REPLACE INTO ghost_mode VALUES (?, CASE WHEN (SELECT enabled FROM ghost_mode WHERE user_id=?)=1 THEN 0 ELSE 1 END)", (uid, uid))

    async def is_fav(self, uid, fid): 
        row = await self._exec("SELECT 1 FROM favorites WHERE user_id=? AND fav_user_id=?", (uid, fid), fetchone=True)
        return bool(row)

    async def add_fav(self, uid, fid): 
        await self._exec("INSERT OR IGNORE INTO favorites VALUES (?,?)", (uid, fid))

    async def remove_fav(self, uid, fid): 
        await self._exec("DELETE FROM favorites WHERE user_id=? AND fav_user_id=?", (uid, fid))

    async def is_hash_banned(self, fhash): 
        row = await self._exec("SELECT 1 FROM banned_hashes WHERE hash=?", (fhash,), fetchone=True)
        return bool(row)

    async def ban_hash(self, fhash): 
        await self._exec("INSERT OR IGNORE INTO banned_hashes VALUES (?)", (fhash,))

    async def is_blacklisted(self, uid): 
        row = await self._exec("SELECT 1 FROM blacklist WHERE user_id=?", (uid,), fetchone=True)
        return bool(row)

    async def add_blacklist(self, uid): 
        await self._exec("INSERT OR IGNORE INTO blacklist VALUES (?)", (uid,))

    async def remove_blacklist(self, uid): 
        await self._exec("DELETE FROM blacklist WHERE user_id=?", (uid,))

    async def vacuum(self):
        await asyncio.to_thread(self.conn.execute, "VACUUM")
        await asyncio.to_thread(self.conn.commit)

    async def get_stats(self, uid):
        stats = {}
        for table, col in [('messages','user_id'),('favorites','user_id'),('muted','user_id'),('notes','user_id'),('todo','user_id'),('reminders','user_id')]:
            row = await self._exec(f"SELECT COUNT(*) as c FROM {table} WHERE {col}=?", (uid,), fetchone=True)
            stats[table] = row['c'] if row else 0
        return stats

    async def export_all(self):
        tables = ['messages','favorites','muted','notes','afk','reminders','aliases','filters','counters','todo','blacklist','banned_hashes','safe_chats','ghost_mode']
        data = {}
        for t in tables:
            rows = await self._exec(f"SELECT * FROM {t}", fetch=True)
            data[t] = [dict(r) for r in rows] if rows else []
        return data

def get_api_credentials():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            cfg = json.load(f)
        return int(cfg['api_id']), cfg['api_hash']
    
    logger.error("Файл config.json не найден. Создайте его вручную с полями api_id и api_hash.")
    sys.exit(1)

TRIGGERS = {
    r'(?<!\w)привет(?!\w)': "Здарова. Я Hiro.",
    r'(?<!\w)кто ты(?!\w)': "Цифровой господь этого чата.",
    r'(?<!\w)статус(?!\w)': "Система стабильна. Режим доминирования активен.",
    r'(?<!\w)версия(?!\w)': "Hiro v9.0 | Stable | SQLite+Batch | ZSS Error Handling",
}

class SpamTracker:
    def __init__(self, limit=6, window=4):
        self.limit = limit
        self.window = window
        self.history = defaultdict(list)

    def track(self, uid):
        now = time.time()
        self.history[uid] = [t for t in self.history[uid] if now - t < self.window]
        self.history[uid].append(now)
        return len(self.history[uid]) > self.limit

    def cleanup(self):
        self.history.clear()

spam_tracker = SpamTracker(limit=6, window=4)

async def safe_delete(client, chat_id, msg_ids):
    if not msg_ids: return
    try: 
        await client.delete_messages(chat_id, msg_ids)
    except (MessageDeleteForbiddenError, Exception): 
        pass

async def auto_delete(client, chat_id, msg_id, delay=15):
    await asyncio.sleep(delay)
    await safe_delete(client, chat_id, [msg_id])

async def safe_send(client, chat_id, text, auto_delete_delay=0, reply_to=None, **kwargs):
    try:
        msg = await client.send_message(chat_id, text, reply_to=reply_to, **kwargs)
        if auto_delete_delay > 0 and msg:
            asyncio.create_task(auto_delete(client, chat_id, msg.id, auto_delete_delay))
        return msg
    except Exception as e:
        logger.error(f"Send error: {e}")
        return None

async def resolve_target(event, client):
    if event.reply_to_msg_id:
        reply = await event.get_reply_message()
        return reply.sender_id if reply else None
    
    match = re.search(r'@(\w+)', event.text or "")
    if match:
        try:
            entity = await client.get_entity(match.group(1))
            return entity.id
        except Exception:
            return None
    return None

def parse_duration(s: str) -> Optional[timedelta]:
    total = timedelta()
    pattern = re.findall(r'(\d+)([dhms])', s.lower())
    if not pattern: return None
    for val, unit in pattern:
        v = int(val)
        if unit == 'd': total += timedelta(days=v)
        elif unit == 'h': total += timedelta(hours=v)
        elif unit == 'm': total += timedelta(minutes=v)
        elif unit == 's': total += timedelta(seconds=v)
    return total if total.total_seconds() > 0 else None

def generate_password(length=16):
    chars = string.ascii_letters + string.digits + "!@#$%^&*"
    return ''.join(random.SystemRandom().choice(chars) for _ in range(length))

async def main():
    api_id, api_hash = get_api_credentials()
    os.makedirs(SESSION_DIR, exist_ok=True)
    client = TelegramClient(f'{SESSION_DIR}/hiro_session', api_id, api_hash)
    await client.start()
    me = await client.get_me()
    my_id = me.id
    db = DB(DB_FILE)
    batch_saver = BatchSaver(db)
    await batch_saver.start()
    
    logger.info(f"Hiro v9.0 started | ID: {my_id} | Stability: MAX")

    async def reminder_checker():
        while True:
            try:
                pending = await db.get_pending_reminders()
                if pending:
                    for r in pending:
                        await safe_send(client, r['chat_id'], f"Напоминание: {r['text']}", auto_delete_delay=30)
                        await db.mark_reminder_done(r['id'])
            except Exception as e:
                logger.error(f"Reminder checker error: {e}")
            await asyncio.sleep(15)
    asyncio.create_task(reminder_checker())

    @client.on(events.NewMessage(outgoing=False))
    @ZSS.catch
    async def msg_handler(event):
        sender = await event.get_sender()
        uid = sender.id if sender else None
        if not uid: return
        cid = event.chat_id

        if await db.is_safe(cid): return

        if await db.is_blacklisted(uid) or await db.is_muted(my_id, uid):
            await safe_delete(client, cid, [event.id])
            return

        afk = await db.get_afk(my_id)
        if afk and uid != my_id:
            await db.inc_afk_missed(my_id)
            reason = afk['reason'] if afk else "AFK"
            await safe_send(client, cid, f"[AFK] {reason}", reply_to=event.id, auto_delete_delay=10)

        if await db.is_ghost(my_id) and not await db.is_fav(my_id, uid):
            return

        text = event.text or ""
        fhash = None

        doc = event.media if hasattr(event, 'media') and event.media and hasattr(event.media, 'document') else None
        if doc and getattr(doc, 'size', float('inf')) < 5*1024*1024:
            try:
                media_bytes = await client.download_media(doc, bytes)
                fhash = zlib.crc32(media_bytes)
                if await db.is_hash_banned(fhash):
                    await safe_delete(client, cid, [event.id])
                    await db.add_mute(my_id, uid)
                    logger.info(f"Auto-muted {uid} via fast hash {fhash}")
                    return
            except Exception: 
                pass

        await batch_saver.add((uid, cid, text, fhash))

        if await db.is_fav(my_id, uid):
            try:
                await client.forward_messages(me.id, event.message)
            except Exception:
                pass

        if spam_tracker.track(uid):
            await db.add_mute(my_id, uid)
            await safe_send(client, cid, f"Muted {uid} for spam", auto_delete_delay=15)
            logger.info(f"Auto-muted {uid} for spam")

        filter_resp = await db.match_filter(cid, text)
        if filter_resp:
            await safe_send(client, cid, filter_resp, reply_to=event.id, auto_delete_delay=20)
            return

        lower = text.lower()
        for pattern, response in TRIGGERS.items():
            if re.search(pattern, lower):
                await safe_send(client, cid, response, auto_delete_delay=15)
                break

    @client.on(events.NewMessage(pattern=r'^/(start|help)(?:@\w+)?(?:\s|$)'))
    @ZSS.catch
    async def help_h(event):
        txt = ("Hiro v9.0 | Stable Core\n\n"
               "Инфо: /ping /id /stats /info /osint\n"
               "Модерация: /mute /unmute /blacklist /unblacklist /banhash /purge /del\n"
               "Личное: /ghost /fav /safe /afk /note /todo /remind /counter\n"
               "Утилиты: /echo /calc /passgen /encode /decode /hash /alias /filter\n"
               "ЛС функции: /read /archive /unarchive /spamcheck\n"
               "Система: /eval /export /vacuum /backup")
        await safe_send(client, event.chat_id, txt, auto_delete_delay=30)

    @client.on(events.NewMessage(pattern=r'^/ping(?:@\w+)?(?:\s|$)'))
    @ZSS.catch
    async def ping_h(event):
        start = time.time()
        msg = await safe_send(client, event.chat_id, "Pinging...")
        if msg:
            ms = (time.time() - start) * 1000
            await msg.edit(f"Pong: {ms:.0f}ms")
            asyncio.create_task(auto_delete(client, event.chat_id, msg.id, 15))

    @client.on(events.NewMessage(pattern=r'^/id(?:@\w+)?(?:\s+@?(\w+))?'))
    @ZSS.catch
    async def id_h(event):
        username = event.pattern_match.group(1)
        if username:
            try:
                entity = await client.get_entity(username)
                info = f"ID: {entity.id}\nType: {type(entity).__name__}\nUsername: @{getattr(entity,'username','N/A')}"
            except Exception:
                info = "Not found"
        else:
            s = await event.get_sender()
            info = f"ID: {s.id}\nName: {getattr(s,'first_name','')} {getattr(s,'last_name','')}\nUser: @{getattr(s,'username','N/A')}" if s else "No sender"
        await safe_send(client, event.chat_id, info, auto_delete_delay=20)

    @client.on(events.NewMessage(pattern=r'^/stats(?:@\w+)?(?:\s|$)'))
    @ZSS.catch
    async def stats_h(event):
        if event.sender_id != my_id: return
        s = await db.get_stats(my_id)
        lines = ["Stats:"] + [f"- {k}: {v}" for k, v in s.items()]
        await safe_send(client, event.chat_id, "\n".join(lines), auto_delete_delay=20)

    @client.on(events.NewMessage(pattern=r'^/info(?:@\w+)?(?:\s|$)'))
    @ZSS.catch
    async def info_h(event):
        chat = await event.get_chat()
        title = getattr(chat, 'title', None) or getattr(chat, 'first_name', str(event.chat_id))
        participants = getattr(chat, 'participants_count', 'N/A')
        await safe_send(client, event.chat_id, f"Chat: {title}\nID: {event.chat_id}\nMembers: {participants}", auto_delete_delay=20)

    @client.on(events.NewMessage(pattern=r'^/osint(?:@\w+)?(?:\s+@?(\w+))?'))
    @ZSS.catch
    async def osint_h(event):
        if event.sender_id != my_id: return
        username = event.pattern_match.group(1)
        target = None
        if username:
            try: target = await client.get_entity(username)
            except Exception: pass
        else:
            target = await event.get_sender()
            
        if not target:
            await safe_send(client, event.chat_id, "Цель не найдена", auto_delete_delay=15)
            return
            
        info = [
            "OSINT Report:",
            f"ID: {target.id}",
            f"Name: {getattr(target, 'first_name', '')} {getattr(target, 'last_name', '')}",
            f"Username: @{getattr(target, 'username', 'None')}",
            f"Phone: {getattr(target, 'phone', 'Hidden')}",
            f"Is Bot: {getattr(target, 'bot', False)}",
            f"Is Verified: {getattr(target, 'verified', False)}",
            f"Status: {getattr(target, 'status', 'Unknown')}"
        ]
        await safe_send(client, event.chat_id, "\n".join(info), auto_delete_delay=30)

    @client.on(events.NewMessage(pattern=r'^/echo(?:@\w+)?\s+(.+)'))
    @ZSS.catch
    async def echo_h(event):
        await safe_send(client, event.chat_id, event.pattern_match.group(1), auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/mute(?:@\w+)?(?:\s|$)'))
    @ZSS.catch
    async def mute_h(event):
        if event.sender_id != my_id: return
        tid = await resolve_target(event, client)
        if tid:
            await db.add_mute(my_id, tid)
            await safe_send(client, event.chat_id, f"Muted {tid}", auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/unmute(?:@\w+)?(?:\s|$)'))
    @ZSS.catch
    async def unmute_h(event):
        if event.sender_id != my_id: return
        tid = await resolve_target(event, client)
        if tid:
            await db.remove_mute(my_id, tid)
            await safe_send(client, event.chat_id, f"Unmuted {tid}", auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/blacklist(?:@\w+)?(?:\s|$)'))
    @ZSS.catch
    async def bl_h(event):
        if event.sender_id != my_id: return
        tid = await resolve_target(event, client)
        if tid:
            await db.add_blacklist(tid)
            await safe_send(client, event.chat_id, f"Blacklisted {tid}", auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/unblacklist(?:@\w+)?(?:\s|$)'))
    @ZSS.catch
    async def ubl_h(event):
        if event.sender_id != my_id: return
        tid = await resolve_target(event, client)
        if tid:
            await db.remove_blacklist(tid)
            await safe_send(client, event.chat_id, f"Unblacklisted {tid}", auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/ghost(?:@\w+)?(?:\s|$)'))
    @ZSS.catch
    async def ghost_h(event):
        if event.sender_id != my_id: return
        await db.toggle_ghost(my_id)
        status = "ON" if await db.is_ghost(my_id) else "OFF"
        await safe_send(client, event.chat_id, f"Ghost mode: {status}", auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/fav(?:@\w+)?\s+(add|remove)\s+@?(\w+)'))
    @ZSS.catch
    async def fav_h(event):
        if event.sender_id != my_id: return
        action, username = event.pattern_match.group(1), event.pattern_match.group(2)
        try:
            entity = await client.get_entity(username)
            if action == 'add': await db.add_fav(my_id, entity.id)
            else: await db.remove_fav(my_id, entity.id)
            await safe_send(client, event.chat_id, f"Favorite {action}ed: {username}", auto_delete_delay=15)
        except Exception:
            await safe_send(client, event.chat_id, "User not found", auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/safe(?:@\w+)?(?:\s+(add|remove))?'))
    @ZSS.catch
    async def safe_h(event):
        if event.sender_id != my_id: return
        action = event.pattern_match.group(1) or "add"
        chat = await event.get_chat()
        name = getattr(chat, 'title', None) or str(event.chat_id)
        if action == "remove":
            await db.remove_safe(event.chat_id)
            await safe_send(client, event.chat_id, f"Safe removed: {name}", auto_delete_delay=15)
        else:
            await db.add_safe(event.chat_id, name)
            await safe_send(client, event.chat_id, f"Safe chat: {name}", auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/afk(?:@\w+)?(?:\s+(.+))?'))
    @ZSS.catch
    async def afk_h(event):
        if event.sender_id != my_id: return
        reason = event.pattern_match.group(1)
        if reason:
            await db.set_afk(my_id, reason)
            await safe_send(client, event.chat_id, f"AFK ON: {reason}", auto_delete_delay=10)
        else:
            missed = await db.unset_afk(my_id)
            await safe_send(client, event.chat_id, f"AFK OFF | Missed: {missed}", auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/note(?:@\w+)?(?:\s+(\w+)(?:\s+(.+))?)?'))
    @ZSS.catch
    async def note_h(event):
        if event.sender_id != my_id: return
        key = event.pattern_match.group(1)
        content = event.pattern_match.group(2)
        if not key:
            notes = await db.list_notes(my_id)
            txt = "\n".join([f"- {k} ({u})" for k, u in notes]) or "Empty"
            await safe_send(client, event.chat_id, f"Notes:\n{txt}", auto_delete_delay=20)
        elif not content:
            row = await db.get_note(my_id, key)
            val = row['content'] if row else f"Note '{key}' not found"
            await safe_send(client, event.chat_id, val, auto_delete_delay=20)
        else:
            await db.set_note(my_id, key, content)
            await safe_send(client, event.chat_id, f"Saved '{key}'", auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/todo(?:@\w+)?(?:\s+(add|done|del|list)\s*(.*))?'))
    @ZSS.catch
    async def todo_h(event):
        if event.sender_id != my_id: return
        action = event.pattern_match.group(1) or "list"
        arg = (event.pattern_match.group(2) or "").strip()
        if action == "add" and arg:
            await db.add_todo(my_id, arg)
            await safe_send(client, event.chat_id, f"Added: {arg}", auto_delete_delay=15)
        elif action == "done" and arg.isdigit():
            await db.toggle_todo(my_id, int(arg))
            await safe_send(client, event.chat_id, f"Toggled #{arg}", auto_delete_delay=15)
        elif action == "del" and arg.isdigit():
            await db.del_todo(my_id, int(arg))
            await safe_send(client, event.chat_id, f"Deleted #{arg}", auto_delete_delay=15)
        else:
            items = await db.list_todo(my_id)
            lines = [f"{'[x]' if i['done'] else '[ ]'} #{i['id']}: {i['text']}" for i in items] or ["Empty"]
            await safe_send(client, event.chat_id, "TODO:\n" + "\n".join(lines), auto_delete_delay=25)

    @client.on(events.NewMessage(pattern=r'^/remind(?:@\w+)?\s+(\S+)\s+(.+)'))
    @ZSS.catch
    async def remind_h(event):
        if event.sender_id != my_id: return
        dur_str = event.pattern_match.group(1)
        text = event.pattern_match.group(2)
        dur = parse_duration(dur_str)
        if not dur:
            await safe_send(client, event.chat_id, "Format: 1h, 30m, 2d, 1h30m", auto_delete_delay=15)
            return
        trigger = (datetime.utcnow() + dur).isoformat()
        await db.add_reminder(my_id, event.chat_id, text, trigger)
        await safe_send(client, event.chat_id, f"Reminder set: {dur_str} -> {text}", auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/counter(?:@\w+)?(?:\s+(\w+)(?:\s+(\d+))?)?'))
    @ZSS.catch
    async def counter_h(event):
        if event.sender_id != my_id: return
        key = event.pattern_match.group(1)
        delta = event.pattern_match.group(2)
        if not key:
            await safe_send(client, event.chat_id, "Usage: /counter <key> [delta]", auto_delete_delay=15)
        elif delta is not None:
            await db.inc_counter(my_id, key, int(delta))
            val = await db.get_counter(my_id, key)
            await safe_send(client, event.chat_id, f"{key}: {val}", auto_delete_delay=15)
        else:
            val = await db.get_counter(my_id, key)
            await safe_send(client, event.chat_id, f"{key}: {val}", auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/calc(?:@\w+)?\s+(.+)'))
    @ZSS.catch
    async def calc_h(event):
        expr = event.pattern_match.group(1)
        allowed = set('0123456789+-*/.() eE')
        if not all(c in allowed for c in expr):
            await safe_send(client, event.chat_id, "Invalid chars", auto_delete_delay=10)
            return
        try:
            result = eval(expr, {"__builtins__": {}, "math": math}, {})
            await safe_send(client, event.chat_id, f"= {result}", auto_delete_delay=15)
        except Exception:
            await safe_send(client, event.chat_id, "Error", auto_delete_delay=10)

    @client.on(events.NewMessage(pattern=r'^/passgen(?:@\w+)?(?:\s+(\d+))?'))
    @ZSS.catch
    async def passgen_h(event):
        length = min(int(event.pattern_match.group(1) or 16), 64)
        pwd = generate_password(length)
        await safe_send(client, event.chat_id, f"`{pwd}`", auto_delete_delay=30)

    @client.on(events.NewMessage(pattern=r'^/encode(?:@\w+)?\s+(.+)'))
    @ZSS.catch
    async def encode_h(event):
        data = event.pattern_match.group(1)
        encoded = base64.b64encode(data.encode()).decode()
        await safe_send(client, event.chat_id, f"`{encoded}`", auto_delete_delay=20)

    @client.on(events.NewMessage(pattern=r'^/decode(?:@\w+)?\s+(.+)'))
    @ZSS.catch
    async def decode_h(event):
        data = event.pattern_match.group(1)
        try:
            decoded = base64.b64decode(data).decode(errors='replace')
            await safe_send(client, event.chat_id, decoded, auto_delete_delay=20)
        except Exception:
            await safe_send(client, event.chat_id, "Invalid base64", auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/hash(?:@\w+)?(?:\s|$)'))
    @ZSS.catch
    async def hash_h(event):
        if not event.reply_to_msg_id:
            await safe_send(client, event.chat_id, "Reply to a file", auto_delete_delay=10)
            return
        reply = await event.get_reply_message()
        doc = reply.media if hasattr(reply, 'media') and reply.media and hasattr(reply.media, 'document') else None
        if not doc or getattr(doc, 'size', float('inf')) > 10*1024*1024:
            await safe_send(client, event.chat_id, "No file or too large", auto_delete_delay=10)
            return
        media_bytes = await client.download_media(doc, bytes)
        h = zlib.crc32(media_bytes)
        await safe_send(client, event.chat_id, f"CRC32 Fast Hash: `{h}`", auto_delete_delay=20)

    @client.on(events.NewMessage(pattern=r'^/banhash(?:@\w+)?(?:\s|$)'))
    @ZSS.catch
    async def banhash_h(event):
        if event.sender_id != my_id: return
        if not event.reply_to_msg_id: return
        reply = await event.get_reply_message()
        doc = reply.media if hasattr(reply, 'media') and reply.media and hasattr(reply.media, 'document') else None
        if not doc: return
        media_bytes = await client.download_media(doc, bytes)
        fhash = zlib.crc32(media_bytes)
        await db.ban_hash(fhash)
        await safe_send(client, event.chat_id, f"Hash banned: `{fhash}`", auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/purge(?:@\w+)?(?:\s+(\d+))?'))
    @ZSS.catch
    async def purge_h(event):
        if event.sender_id != my_id: return
        count = min(int(event.pattern_match.group(1) or 20), 200)
        ids = []
        async for msg in client.iter_messages(event.chat_id, from_user=my_id, limit=count):
            ids.append(msg.id)
        await safe_delete(client, event.chat_id, ids)
        await safe_send(client, event.chat_id, f"Purged {len(ids)} msgs", auto_delete_delay=10)

    @client.on(events.NewMessage(pattern=r'^/del(?:@\w+)?(?:\s|$)'))
    @ZSS.catch
    async def del_h(event):
        if event.sender_id != my_id: return
        if event.reply_to_msg_id:
            await safe_delete(client, event.chat_id, [event.reply_to_msg_id])
        await safe_delete(client, event.chat_id, [event.id])

    @client.on(events.NewMessage(pattern=r'^/filter(?:@\w+)?(?:\s+(add|del)\s+(\S+)(?:\s+(.+))?)?'))
    @ZSS.catch
    async def filter_h(event):
        if event.sender_id != my_id: return
        action = event.pattern_match.group(1)
        pattern = event.pattern_match.group(2)
        response = event.pattern_match.group(3)
        if not action:
            filters = await db.get_filters(event.chat_id)
            lines = [f"- `{f['pattern']}` -> {f['response'][:50]}" for f in filters] if filters else ["No filters"]
            await safe_send(client, event.chat_id, "Filters:\n" + "\n".join(lines), auto_delete_delay=20)
        elif action == "add" and pattern and response:
            await db.add_filter(event.chat_id, pattern, response)
            await safe_send(client, event.chat_id, f"Filter added: `{pattern}`", auto_delete_delay=15)
        elif action == "del" and pattern:
            await db.del_filter(event.chat_id, pattern)
            await safe_send(client, event.chat_id, f"Filter removed: `{pattern}`", auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/alias(?:@\w+)?(?:\s+(add|del)\s+(\S+)(?:\s+(.+))?)?'))
    @ZSS.catch
    async def alias_h(event):
        if event.sender_id != my_id: return
        action = event.pattern_match.group(1)
        name = event.pattern_match.group(2)
        target = event.pattern_match.group(3)
        if not action:
            aliases = await db.list_aliases(my_id)
            lines = [f"- `{a['alias']}` -> {a['target']}" for a in aliases] if aliases else ["No aliases"]
            await safe_send(client, event.chat_id, "Aliases:\n" + "\n".join(lines), auto_delete_delay=20)
        elif action == "add" and name and target:
            await db.set_alias(my_id, name, target)
            await safe_send(client, event.chat_id, f"Alias `{name}` -> `{target}`", auto_delete_delay=15)
        elif action == "del" and name:
            await db.del_alias(my_id, name)
            await safe_send(client, event.chat_id, f"Alias `{name}` removed", auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/eval(?:@\w+)?\s+(.+)'))
    @ZSS.catch
    async def eval_h(event):
        if event.sender_id != my_id: return
        code = event.pattern_match.group(1)
        safe_globals = {"__builtins__": {}, "asyncio": asyncio, "time": time, "math": math, "db": db, "client": client}
        old_stdout = sys.stdout
        sys.stdout = mystdout = io.StringIO()
        try:
            exec(code, safe_globals, locals())
            result = mystdout.getvalue() or "Done"
        except Exception as e:
            result = f"Error: {e}"
        finally:
            sys.stdout = old_stdout
        await safe_send(client, event.chat_id, str(result)[:4000], auto_delete_delay=20)

    @client.on(events.NewMessage(pattern=r'^/export(?:@\w+)?(?:\s|$)'))
    @ZSS.catch
    async def export_h(event):
        if event.sender_id != my_id: return
        data = await db.export_all()
        fname = f"hiro_export_{int(time.time())}.json"
        with open(fname, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, default=str)
        await client.send_file(event.chat_id, fname, caption="DB Export")
        os.remove(fname)

    @client.on(events.NewMessage(pattern=r'^/vacuum(?:@\w+)?(?:\s|$)'))
    @ZSS.catch
    async def vacuum_h(event):
        if event.sender_id != my_id: return
        await db.vacuum()
        await safe_send(client, event.chat_id, "VACUUM completed", auto_delete_delay=15)

    @client.on(events.NewMessage(pattern=r'^/backup(?:@\w+)?(?:\s|$)'))
    @ZSS.catch
    async def backup_h(event):
        if event.sender_id != my_id: return
        data = await db.export_all()
        raw = json.dumps(data, ensure_ascii=False, default=str).encode()
        compressed = zlib.compress(raw, level=9)
        fname = f"hiro_backup_{int(time.time())}.bin"
        with open(fname, 'wb') as f:
            f.write(compressed)
        await client.send_file(event.chat_id, fname, caption=f"Backup ({len(compressed)} bytes)")
        os.remove(fname)

    @client.on(events.NewMessage(pattern=r'^/read(?:@\w+)?(?:\s|$)'))
    @ZSS.catch
    async def read_h(event):
        if event.sender_id != my_id: return
        await client.send_read_acknowledge(event.chat_id)
        await safe_send(client, event.chat_id, "Сообщения отмечены как прочитанные", auto_delete_delay=10)

    @client.on(events.NewMessage(pattern=r'^/archive(?:@\w+)?(?:\s|$)'))
    @ZSS.catch
    async def archive_h(event):
        if event.sender_id != my_id: return
        await client(functions.messages.HidePeerSettingsFromRequestPeerRequest(peer=event.chat_id))
        await client(functions.folders.EditPeerFoldersRequest(folder_peers=[
            types.InputFolderPeer(event.chat_id, folder_id=1)
        ]))
        await safe_send(client, event.chat_id, "Чат архивирован", auto_delete_delay=10)

    @client.on(events.NewMessage(pattern=r'^/unarchive(?:@\w+)?(?:\s|$)'))
    @ZSS.catch
    async def unarchive_h(event):
        if event.sender_id != my_id: return
        await client(functions.folders.EditPeerFoldersRequest(folder_peers=[
            types.InputFolderPeer(event.chat_id, folder_id=0)
        ]))
        await safe_send(client, event.chat_id, "Чат разархивирован", auto_delete_delay=10)

    @client.on(events.NewMessage(pattern=r'^/spamcheck(?:@\w+)?(?:\s|$)'))
    @ZSS.catch
    async def spamcheck_h(event):
        if event.sender_id != my_id: return
        target = await resolve_target(event, client) or event.chat_id
        history_count = 0
        async for _ in client.iter_messages(target, limit=100):
            history_count += 1
        
        status = "Подозрительно" if history_count > 50 else "Норма"
        await safe_send(client, event.chat_id, f"Spam check:\nMessages in last 100: {history_count}\nStatus: {status}", auto_delete_delay=15)

    try:
        await client.run_until_disconnected()
    finally:
        await batch_saver.flush()
        spam_tracker.cleanup()
        logger.info("Shutdown complete: batches flushed, spam tracker cleaned")

if __name__ == '__main__':
    asyncio.run(main())
