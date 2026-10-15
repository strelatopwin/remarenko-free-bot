import asyncio
import html
import io
import re
import hashlib
import zipfile
from zoneinfo import ZoneInfo
from xml.sax.saxutils import escape as xml_escape
import json
import logging
import math
import os
import sqlite3
from contextlib import suppress
from dataclasses import asdict
from urllib.parse import urlparse
from datetime import datetime, timedelta, timezone

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ChatType, ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.base import BaseStorage, StorageKey
from aiogram.fsm.storage.memory import SimpleEventIsolation
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)


BOT_VERSION = "2.1.4"
BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "788167958"))
DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/remarenko_free.sqlite3")
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
CHANNEL_URL = os.getenv("CHANNEL_URL", "https://t.me/").strip()
LESSON_URL = os.getenv("LESSON_URL", "").strip()
SITE_URL = os.getenv("SITE_URL", "https://remarenkop2p.com/").strip()
CONTACT_URL = os.getenv("CONTACT_URL", "https://t.me/P2P_Marshal").strip()


def env_number(name: str, default: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
        return max(0.0, value) if math.isfinite(value) else default
    except ValueError:
        logging.warning("Invalid %s; using %s", name, default)
        return default


REMINDER_MINUTES = env_number("FUNNEL_REMINDER_MINUTES", 30)
VALUE_HOURS = env_number("FUNNEL_VALUE_HOURS", 4)
OFFER_HOURS = env_number("FUNNEL_OFFER_HOURS", 24)
FINAL_HOURS = env_number("FUNNEL_FINAL_HOURS", 48)
WORKER_INTERVAL_SECONDS = max(1, env_number("FUNNEL_WORKER_INTERVAL_SECONDS", 20))
FORM_REMINDER_HOURS = env_number("FUNNEL_FORM_REMINDER_HOURS", 2)

router = Router()
router.message.filter(F.chat.type == ChatType.PRIVATE)
router.callback_query.filter(F.message.chat.type == ChatType.PRIVATE)
notification_lock = asyncio.Lock()


class LeadForm(StatesGroup):
    experience = State()
    capital = State()
    goal = State()
    support = State()
    contact = State()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return utc_now().isoformat()


class ClosingConnection(sqlite3.Connection):
    def __exit__(self, *args):
        try:
            return super().__exit__(*args)
        finally:
            self.close()


def connect() -> sqlite3.Connection:
    folder = os.path.dirname(DATABASE_PATH)
    if folder:
        os.makedirs(folder, exist_ok=True)
    db = sqlite3.connect(DATABASE_PATH, timeout=20, factory=ClosingConnection)
    db.row_factory = sqlite3.Row
    db.create_function("casefold", 1, lambda value: str(value or "").casefold(), deterministic=True)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=20000")
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            telegram_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            source TEXT NOT NULL DEFAULT 'direct',
            stage TEXT NOT NULL DEFAULT 'started',
            subscribed INTEGER NOT NULL DEFAULT 0,
            experience TEXT,
            capital TEXT,
            goal TEXT,
            support TEXT,
            recommendation TEXT,
            recommendation_reason TEXT,
            contact TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    existing = {row[1] for row in db.execute("PRAGMA table_info(users)")}
    for column in ("support", "recommendation", "recommendation_reason",
                   "lesson_received_at", "lesson_completed_at", "questionnaire_started_at"):
        if column not in existing:
            db.execute(f"ALTER TABLE users ADD COLUMN {column} TEXT")
    if 'lead_status' not in existing:
        db.execute("ALTER TABLE users ADD COLUMN lead_status TEXT NOT NULL DEFAULT 'new'")
    if 'reminders_enabled' not in existing:
        db.execute("ALTER TABLE users ADD COLUMN reminders_enabled INTEGER NOT NULL DEFAULT 1")
    db.execute("CREATE TABLE IF NOT EXISTS form_state (key TEXT PRIMARY KEY, state TEXT, data TEXT NOT NULL DEFAULT '{}')")
    db.execute("""CREATE TABLE IF NOT EXISTS lead_notifications (
        user_id INTEGER PRIMARY KEY, text TEXT NOT NULL, due_at TEXT NOT NULL,
        sent_at TEXT, attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT)""")
    notice_columns = {row[1] for row in db.execute('PRAGMA table_info(lead_notifications)')}
    if 'message_id' not in notice_columns:
        db.execute('ALTER TABLE lead_notifications ADD COLUMN message_id INTEGER')
    if 'revision' not in notice_columns:
        db.execute('ALTER TABLE lead_notifications ADD COLUMN revision INTEGER NOT NULL DEFAULT 0')
    db.execute("""CREATE TABLE IF NOT EXISTS channel_joins (
        user_id INTEGER PRIMARY KEY, first_joined_at TEXT NOT NULL, last_joined_at TEXT NOT NULL,
        via_bot_link INTEGER NOT NULL DEFAULT 0)""")
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS scheduled_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            due_at TEXT NOT NULL,
            sent_at TEXT,
            canceled_at TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(user_id, kind)
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS funnel_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            event TEXT NOT NULL,
            details TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_scheduled_due "
        "ON scheduled_messages(due_at, sent_at, canceled_at)"
    )
    db.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_user_event "
        "ON funnel_events(user_id, event)"
    )
    for column, event in [('lesson_received_at', 'lesson_video_sent'),
                          ('lesson_completed_at', 'lesson_completed'),
                          ('questionnaire_started_at', 'questionnaire_started')]:
        if column not in existing:
            db.execute(f'''UPDATE users SET {column}=(SELECT MIN(created_at) FROM funnel_events
                WHERE user_id=users.telegram_id AND event=?)''', (event,))
    db.commit()
    return db


class SQLiteStorage(BaseStorage):
    """Keep questionnaire state on the same persistent volume as the funnel."""
    @staticmethod
    def key_string(key: StorageKey) -> str:
        return json.dumps(asdict(key), sort_keys=True)

    async def set_state(self, key, state=None):
        value = state.state if isinstance(state, State) else state
        with connect() as db:
            db.execute("INSERT INTO form_state(key,state) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET state=excluded.state",
                       (self.key_string(key), value))

    async def get_state(self, key):
        with connect() as db:
            row = db.execute('SELECT state FROM form_state WHERE key=?', (self.key_string(key),)).fetchone()
            return row['state'] if row else None

    async def set_data(self, key, data):
        with connect() as db:
            db.execute("INSERT INTO form_state(key,data) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET data=excluded.data",
                       (self.key_string(key), json.dumps(dict(data), ensure_ascii=False)))

    async def get_data(self, key):
        with connect() as db:
            row = db.execute('SELECT data FROM form_state WHERE key=?', (self.key_string(key),)).fetchone()
            return json.loads(row['data']) if row else {}

    async def close(self):
        pass


def record_event(user_id: int, event: str, details: str | None = None) -> None:
    with connect() as db:
        db.execute(
            "INSERT INTO funnel_events (user_id, event, details, created_at) VALUES (?, ?, ?, ?)",
            (user_id, event, details, now_iso()),
        )
        column = {'lesson_video_sent': 'lesson_received_at', 'lesson_completed': 'lesson_completed_at',
                  'questionnaire_started': 'questionnaire_started_at'}.get(event)
        if column:
            db.execute(f'UPDATE users SET {column}=COALESCE({column},?) WHERE telegram_id=?', (now_iso(), user_id))
        db.commit()


def get_user(user_id: int) -> sqlite3.Row | None:
    with connect() as db:
        return db.execute("SELECT * FROM users WHERE telegram_id=?", (user_id,)).fetchone()


def save_user(message: Message, source: str = "direct") -> bool:
    now = now_iso()
    with connect() as db:
        exists = db.execute(
            "SELECT 1 FROM users WHERE telegram_id=?", (message.from_user.id,)
        ).fetchone()
        db.execute(
            """
            INSERT INTO users (telegram_id, username, first_name, source, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(telegram_id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name,
                source=CASE WHEN users.source='direct' THEN excluded.source ELSE users.source END,
                updated_at=excluded.updated_at
            """,
            (
                message.from_user.id,
                message.from_user.username,
                message.from_user.first_name,
                source or "direct",
                now,
                now,
            ),
        )
        db.commit()
    if not exists:
        record_event(message.from_user.id, "started", source or "direct")
    return not bool(exists)


def update_user(user_id: int, **values: object) -> None:
    allowed = {
        "stage",
        "subscribed",
        "experience",
        "capital",
        "goal",
        "support",
        "recommendation",
        "recommendation_reason",
        "contact",
        "lead_status",
        "reminders_enabled",
    }
    clean = {key: value for key, value in values.items() if key in allowed}
    if not clean:
        return
    clean["updated_at"] = now_iso()
    fields = ", ".join(f"{key}=?" for key in clean)
    with connect() as db:
        if 'stage' in clean:
            row = db.execute('SELECT stage FROM users WHERE telegram_id=?', (user_id,)).fetchone()
            ranks = {'started': 0, 'subscription_required': 1, 'lesson_opened': 2,
                     'lesson_video_sent': 3, 'lesson_completed': 4, 'questionnaire_started': 5,
                     'recommendation_shown': 6, 'contact_requested': 7, 'application_completed': 8}
            if row and ranks.get(clean['stage'], 0) < ranks.get(row['stage'], 0):
                clean.pop('stage')
            fields = ', '.join(f'{key}=?' for key in clean)
        db.execute(
            f"UPDATE users SET {fields} WHERE telegram_id=?",
            (*clean.values(), user_id),
        )
        db.commit()


def set_setting(key: str, value: str) -> None:
    with connect() as db:
        db.execute(
            """
            INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """,
            (key, value, now_iso()),
        )
        db.commit()


def get_setting(key: str) -> str | None:
    with connect() as db:
        row = db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None


def schedule_funnel(user_id: int, test_mode: bool = False) -> None:
    start_at = utc_now()
    if test_mode:
        schedule = [
            ("lesson_reminder", timedelta(minutes=1)),
            ("value_case", timedelta(minutes=2)),
            ("tariff_offer", timedelta(minutes=3)),
            ("final_cta", timedelta(minutes=4)),
        ]
    else:
        schedule = [
            ("lesson_reminder", timedelta(minutes=REMINDER_MINUTES)),
            ("value_case", timedelta(hours=VALUE_HOURS)),
            ("tariff_offer", timedelta(hours=OFFER_HOURS)),
            ("final_cta", timedelta(hours=FINAL_HOURS)),
        ]
    with connect() as db:
        if test_mode:
            db.execute(
                "DELETE FROM scheduled_messages WHERE user_id=? AND sent_at IS NULL",
                (user_id,),
            )
        for kind, delay in schedule:
            db.execute(
                """
                INSERT INTO scheduled_messages
                    (user_id, kind, due_at, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id, kind) DO UPDATE SET
                    due_at=CASE WHEN ? THEN excluded.due_at ELSE scheduled_messages.due_at END,
                    sent_at=CASE WHEN ? THEN NULL ELSE scheduled_messages.sent_at END,
                    canceled_at=CASE WHEN ? THEN NULL ELSE scheduled_messages.canceled_at END,
                    attempts=CASE WHEN ? THEN 0 ELSE scheduled_messages.attempts END,
                    last_error=CASE WHEN ? THEN NULL ELSE scheduled_messages.last_error END
                """,
                (
                    user_id,
                    kind,
                    (start_at + delay).isoformat(),
                    now_iso(),
                    int(test_mode),
                    int(test_mode),
                    int(test_mode),
                    int(test_mode),
                    int(test_mode),
                ),
            )
        db.commit()


def cancel_pending_funnel(user_id: int) -> None:
    with connect() as db:
        db.execute(
            """
            UPDATE scheduled_messages SET canceled_at=?
            WHERE user_id=? AND sent_at IS NULL AND canceled_at IS NULL
            """,
            (now_iso(), user_id),
        )
        db.commit()


def schedule_form_reminder(user_id: int) -> None:
    now = utc_now()
    with connect() as db:
        previous = db.execute("SELECT created_at FROM scheduled_messages WHERE user_id=? AND kind='form_reminder'", (user_id,)).fetchone()
        anchor = datetime.fromisoformat(previous['created_at']) if previous else now
        for kind, hours in [('form_reminder', FORM_REMINDER_HOURS), ('form_reminder_24', max(24, FORM_REMINDER_HOURS+22)),
                            ('form_reminder_48', max(48, FORM_REMINDER_HOURS+46))]:
            db.execute('''INSERT INTO scheduled_messages(user_id,kind,due_at,created_at)
                VALUES(?,?,?,?) ON CONFLICT(user_id,kind) DO NOTHING''',
                (user_id, kind, (anchor+timedelta(hours=hours)).isoformat(), anchor.isoformat()))


def collapse_overdue_form_reminders() -> None:
    now = now_iso()
    with connect() as db:
        db.execute('''UPDATE scheduled_messages SET canceled_at=?
            WHERE kind IN ('form_reminder','form_reminder_24','form_reminder_48')
            AND sent_at IS NULL AND canceled_at IS NULL AND due_at<=?
            AND EXISTS(SELECT 1 FROM scheduled_messages AS later WHERE later.user_id=scheduled_messages.user_id
                AND later.kind IN ('form_reminder','form_reminder_24','form_reminder_48')
                AND later.sent_at IS NULL AND later.canceled_at IS NULL
                AND later.due_at>scheduled_messages.due_at AND later.due_at<=?)''', (now,now,now))


LEAD_STATUSES = {'new': '🆕 Нові', 'work': '🛠 В роботі', 'done': '✅ Опрацьовані', 'all': '📋 Усі заявки', 'people': '👥 Усі в боті'}
STAGE_LABELS = {'started': 'Запустив бота', 'subscription_required': 'Очікує підписки',
    'lesson_opened': 'Відкрив картку уроку', 'lesson_video_sent': 'Отримав урок',
    'lesson_completed': 'Позначив урок переглянутим', 'questionnaire_started': 'Заповнює анкету',
    'recommendation_shown': 'Отримав рекомендацію', 'contact_requested': 'Залишає контакт',
    'application_completed': 'Заявка збережена'}
TARIFF_NAMES = {'BASIC': 'Базовий', 'GROUP': 'Груповий', 'MASTER': 'P2P MASTER'}


def lead_buttons(user_id: int, origin: str = 'new', page: int = 0):
    if not resolve_lead_view(origin) or page < 0:
        origin, page = 'new', 0
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text='💬 Відкрити користувача', url=f'tg://user?id={user_id}')],
        [InlineKeyboardButton(text='🛠 В роботі', callback_data=f'adminlead:status:{user_id}:work:{origin}:{page}'),
         InlineKeyboardButton(text='✅ Опрацьовано', callback_data=f'adminlead:status:{user_id}:done:{origin}:{page}')],
        [InlineKeyboardButton(text='⬅️ Назад до списку', callback_data=f'adminlead:list:{origin}:{page}')]])


def store_application(user_id: int, contact: str, text: str) -> None:
    """Commit the accepted lead and its notification together before acknowledging it."""
    now = now_iso()
    with connect() as db:
        db.execute("UPDATE users SET stage='application_completed',contact=?,updated_at=? WHERE telegram_id=?",
                   (contact, now, user_id))
        db.execute('UPDATE scheduled_messages SET canceled_at=? WHERE user_id=? AND sent_at IS NULL AND canceled_at IS NULL', (now, user_id))
        db.execute('INSERT INTO funnel_events(user_id,event,created_at) VALUES(?,\'application_completed\',?)', (user_id, now))
        db.execute("""INSERT INTO lead_notifications(user_id,text,due_at) VALUES(?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET text=excluded.text,due_at=excluded.due_at,
            sent_at=NULL,attempts=0,last_error=NULL,revision=lead_notifications.revision+1""", (user_id, text, now))


def queue_questionnaire(user_id: int) -> None:
    user = get_user(user_id)
    if not user or not user['recommendation']:
        return
    text = '📝 <b>Анкета з безкоштовного уроку</b>\n\n'
    for key, label in [('first_name', 'Ім’я'), ('username', 'Нік'), ('source', 'Джерело'),
                       ('experience', 'Досвід'), ('capital', 'Капітал'), ('goal', 'Ціль'),
                       ('support', 'Підтримка'), ('recommendation', 'Рекомендація'), ('contact', 'Контакт')]:
        value = user[key] or ('Ще не залишив' if key == 'contact' else '—')
        if key == 'recommendation': value = TARIFF_NAMES.get(value, value)
        text += f'{label}: {html.escape(str(value))}\n'
    text += f'ID: <code>{user_id}</code>\n'
    with connect() as db:
        db.execute('''INSERT INTO lead_notifications(user_id,text,due_at) VALUES(?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET text=excluded.text,due_at=excluded.due_at,
            sent_at=NULL,attempts=0,last_error=NULL,revision=lead_notifications.revision+1''',
            (user_id, text, now_iso()))


async def deliver_lead_notifications(bot: Bot) -> None:
    async with notification_lock:
        with connect() as db:
            rows = db.execute('SELECT * FROM lead_notifications WHERE sent_at IS NULL AND due_at<=? ORDER BY due_at LIMIT 10', (now_iso(),)).fetchall()
        for row in rows:
            try:
                if row['message_id']:
                    try:
                        await bot.edit_message_text(chat_id=ADMIN_CHAT_ID, message_id=row['message_id'],
                                                    text=row['text'], reply_markup=lead_buttons(row['user_id']))
                    except TelegramBadRequest as error:
                        reason = str(error).lower()
                        if 'message is not modified' in reason:
                            pass
                        elif 'message to edit not found' in reason or "message can't be edited" in reason:
                            sent = await bot.send_message(ADMIN_CHAT_ID, row['text'], reply_markup=lead_buttons(row['user_id']))
                            with connect() as db:
                                db.execute('UPDATE lead_notifications SET message_id=? WHERE user_id=?', (sent.message_id, row['user_id']))
                        else:
                            raise
                else:
                    sent = await bot.send_message(ADMIN_CHAT_ID, row['text'], reply_markup=lead_buttons(row['user_id']))
                    with connect() as db:
                        db.execute('UPDATE lead_notifications SET message_id=? WHERE user_id=?', (sent.message_id, row['user_id']))
            except Exception as exc:
                logging.exception('Lead notification delayed for %s', row['user_id'])
                delay = min(60, 2 ** min(row['attempts'], 6))
                due = (utc_now() + timedelta(minutes=delay)).isoformat()
                with connect() as db:
                    db.execute('UPDATE lead_notifications SET attempts=attempts+1,due_at=?,last_error=? WHERE user_id=? AND revision=?',
                               (due, str(exc)[:500], row['user_id'], row['revision']))
            else:
                with connect() as db:
                    db.execute('UPDATE lead_notifications SET sent_at=?,last_error=NULL WHERE user_id=? AND revision=?', (now_iso(), row['user_id'], row['revision']))


# View tokens keep callbacks short while preserving search/filter context across restarts.
STAGE_FILTERS = {'any': 'Усі етапи', 'received': 'Отримали урок',
    'watched': 'Позначили переглянутим', 'started': 'Почали анкету',
    'completed': 'Завершили анкету', 'contact': 'Залишили контакт'}
DATE_FILTERS = {'any': 'Увесь час', 'today': 'Сьогодні', 'week': 'Останні 7 днів'}
KYIV = ZoneInfo('Europe/Kyiv')


def resolve_lead_view(origin: str):
    if origin in LEAD_STATUSES:
        return {'status': origin, 'stage': 'any', 'tariff': 'any', 'date': 'any', 'search': ''}
    if not re.fullmatch(r'v[0-9a-f]{16}', origin):
        return None
    raw = get_setting('lead_view:' + origin)
    return json.loads(raw) if raw else None


def save_lead_view(view: dict) -> str:
    raw = json.dumps(view, ensure_ascii=False, sort_keys=True)
    token = 'v' + hashlib.sha256(raw.encode()).hexdigest()[:16]
    if get_setting('lead_view:' + token) != raw:
        set_setting('lead_view:' + token, raw)
    return token


def lead_condition(view: dict, status=None):
    status = status or view['status']
    clauses = ['1=1' if status == 'people' else '(recommendation IS NOT NULL OR contact IS NOT NULL)']
    params = []
    if status in {'new', 'work', 'done'}:
        clauses.append('lead_status=?')
        params.append(status)
    stage = view.get('stage', 'any')
    milestones = {'received': 'lesson_received_at', 'watched': 'lesson_completed_at',
                  'started': 'questionnaire_started_at', 'completed': 'recommendation', 'contact': 'contact'}
    if stage in milestones:
        clauses.append(f"{milestones[stage]} IS NOT NULL AND {milestones[stage]}!=''")
    if view.get('tariff', 'any') in TARIFF_NAMES:
        clauses.append('recommendation=?')
        params.append(view['tariff'])
    # Date means first entry into the bot; a later status change cannot change this date.
    if view.get('date') in {'today', 'week'}:
        now = utc_now()
        lower = now.astimezone(KYIV).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc) if view['date']=='today' else now-timedelta(days=7)
        clauses.append('created_at>=? AND created_at<=?')
        params.extend([lower.isoformat(), now.isoformat()])
    query = view.get('search', '').strip().lstrip('@')
    if query:
        # instr treats %, _ and SQL punctuation literally. casefold supports Ukrainian names.
        clauses.append("(instr(casefold(COALESCE(first_name,'')),?)>0 OR instr(casefold(COALESCE(username,'')),?)>0 OR instr(CAST(telegram_id AS TEXT),?)>0 OR instr(casefold(COALESCE(contact,'')),?)>0)")
        params.extend([query.casefold()]*4)
    return ' AND '.join(clauses), params


def lead_rows(view: dict, page=None):
    paginated = page is not None
    condition, params = lead_condition(view)
    with connect() as db:
        count = db.execute(f'SELECT COUNT(*) FROM users WHERE {condition}', params).fetchone()[0]
        page = min(max(0, page or 0), max(0, (count-1)//8))
        suffix, extra = (' LIMIT 8 OFFSET ?', [page*8]) if paginated else ('', [])
        rows = db.execute(f'SELECT * FROM users WHERE {condition} ORDER BY updated_at DESC,telegram_id'+suffix, (*params,*extra)).fetchall()
    return count, page, rows


def view_description(view):
    lines = []
    if view.get('stage', 'any') != 'any': lines.append('Етап: '+STAGE_FILTERS[view['stage']])
    if view.get('tariff', 'any') != 'any': lines.append('Тариф: '+TARIFF_NAMES[view['tariff']])
    if view.get('date', 'any') != 'any': lines.append('Вхід у бот: '+DATE_FILTERS[view['date']])
    if view.get('search'): lines.append('Пошук: '+view['search'])
    return '\n'.join(lines)


async def edit_admin(message, text, markup):
    try:
        await message.edit_text(text, reply_markup=markup)
    except TelegramBadRequest as error:
        if 'message is not modified' not in str(error).lower(): raise


async def show_leads(message: Message, status: str = 'new', page: int = 0, edit: bool = False) -> None:
    view = resolve_lead_view(status)
    if not view or page < 0: return
    count, page, rows = lead_rows(view, page)
    set_setting('admin_current_view', status)
    text = f'<b>{LEAD_STATUSES[view["status"]]}</b> · {count}\nСторінка {page+1} із {max(1,(count+7)//8)}\n'
    description = view_description(view)
    if description: text += html.escape(description)+'\n'
    text += '\n'
    keyboard = []
    for row in rows:
        name = row['first_name'] or row['username'] or str(row['telegram_id'])
        label = TARIFF_NAMES.get(row['recommendation']) or STAGE_LABELS.get(row['stage'], 'У боті')
        text += f"• {html.escape(name[:60])} · {html.escape(label)}\n"
        keyboard.append([InlineKeyboardButton(text=f'{name[:40]} · {label}', callback_data=f'adminlead:detail:{row["telegram_id"]}:{status}:{page}')])
    if not rows: text += 'За цим відбором нікого не знайдено.'
    nav = []
    last = max(0,(count-1)//8)
    for label, target in [('⏮',0),('⬅️',page-1),('➡️',page+1),('⏭',last)]:
        if (label in {'⏮','⬅️'} and page>0) or (label in {'➡️','⏭'} and page<last):
            nav.append(InlineKeyboardButton(text=label, callback_data=f'adminlead:list:{status}:{target}'))
    if nav: keyboard.append(nav)
    keyboard.append([InlineKeyboardButton(text='🔎 Пошук',callback_data=f'av:search:{status}'),
                     InlineKeyboardButton(text='🎛 Фільтри',callback_data=f'av:filters:{status}')])
    keyboard.append([InlineKeyboardButton(text='🔢 До сторінки',callback_data=f'av:page:{status}'),
                     InlineKeyboardButton(text='📥 Excel',callback_data=f'av:export:{status}')])
    with connect() as db:
        for key in LEAD_STATUSES:
            condition, params = lead_condition(view, key)
            n = db.execute(f'SELECT COUNT(*) FROM users WHERE {condition}',params).fetchone()[0]
            target = key if not description else save_lead_view({**view,'status':key})
            keyboard.append([InlineKeyboardButton(text=f'{LEAD_STATUSES[key]} · {n}',callback_data=f'adminlead:list:{target}:0')])
    if description: keyboard.append([InlineKeyboardButton(text='✖️ Скинути пошук і фільтри',callback_data=f'adminlead:list:{view["status"]}:0')])
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    if edit: await edit_admin(message,text,markup)
    else: await message.answer(text,reply_markup=markup)


def excel_bytes(rows):
    """Minimal XLSX with explicit text cells: user input can never become an Excel formula."""
    fields = [('telegram_id','Telegram ID'),('first_name','Ім’я'),('username','Telegram'),
        ('created_at','Перший вхід (Київ)'),('updated_at','Оновлено (Київ)'),('source','Джерело'),
        ('stage','Етап'),('experience','Досвід'),('capital','Капітал'),('goal','Ціль'),
        ('support','Підтримка'),('recommendation','Рекомендований тариф'),('contact','Контакт'),
        ('lead_status','Статус роботи')]
    def clean(value):
        return xml_escape(''.join(c for c in str(value or '') if c in '\n\r\t' or 32<=ord(c)<=0xD7FF or 0xE000<=ord(c)<=0xFFFD or 0x10000<=ord(c)<=0x10FFFF)[:32767])
    def col(i):
        result=''
        while i:
            i,n=divmod(i-1,26); result=chr(65+n)+result
        return result
    data = [[label for _,label in fields]]
    for row in rows:
        values=[]
        for key,_ in fields:
            value=row[key]
            if key=='recommendation': value=TARIFF_NAMES.get(value,value)
            if key=='stage': value=STAGE_LABELS.get(value,value)
            if key=='lead_status': value=LEAD_STATUSES.get(value,value)
            if key in {'created_at','updated_at'} and value:
                value=datetime.fromisoformat(value).astimezone(KYIV).strftime('%d.%m.%Y %H:%M')
            values.append(value)
        data.append(values)
    xmlrows=[]
    for n,values in enumerate(data,1):
        cells=''.join(f'<c r="{col(i)}{n}" t="inlineStr" s="{1 if n==1 else 0}"><is><t xml:space="preserve">{clean(v)}</t></is></c>' for i,v in enumerate(values,1))
        xmlrows.append(f'<row r="{n}">{cells}</row>')
    end=f'N{len(data)}'
    sheet='<?xml version="1.0" encoding="UTF-8"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
    sheet+='<sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/></sheetView></sheetViews>'
    sheet+='<cols><col min="1" max="14" width="25" customWidth="1"/></cols><sheetData>'+''.join(xmlrows)+f'</sheetData><autoFilter ref="A1:{end}"/></worksheet>'
    output=io.BytesIO()
    with zipfile.ZipFile(output,'w',zipfile.ZIP_DEFLATED) as z:
        z.writestr('[Content_Types].xml','<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/></Types>')
        z.writestr('_rels/.rels','<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>')
        z.writestr('xl/workbook.xml','<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Заявки" sheetId="1" r:id="rId1"/></sheets></workbook>')
        z.writestr('xl/_rels/workbook.xml.rels','<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>')
        z.writestr('xl/styles.xml','<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><fonts count="2"><font><sz val="11"/><name val="Calibri"/></font><font><b/><sz val="11"/><name val="Calibri"/></font></fonts><fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills><borders count="1"><border/></borders><cellStyleXfs count="1"><xf/></cellStyleXfs><cellXfs count="2"><xf fontId="0" fillId="0" borderId="0" xfId="0"><alignment vertical="top" wrapText="1"/></xf><xf fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"><alignment vertical="top" wrapText="1"/></xf></cellXfs><cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles></styleSheet>')
        z.writestr('xl/worksheets/sheet1.xml',sheet)
    return output.getvalue()


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎁 Отримати безкоштовний урок", callback_data="lesson")],
            [InlineKeyboardButton(text="📚 Програма навчання", callback_data="program")],
            [InlineKeyboardButton(text="💎 Тарифи", callback_data="tariffs")],
            [InlineKeyboardButton(text="📝 Продовжити / пройти анкету", callback_data="resume_form")],
            [InlineKeyboardButton(text="🔕 Вимкнути нагадування", callback_data="stop_reminders")],
            [InlineKeyboardButton(text="👨‍💻 Написати Михайлу", url=CONTACT_URL)],
        ]
    )


def subscription_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📲 Підписатися на канал", url=CHANNEL_URL)],
            [InlineKeyboardButton(text="✅ Я підписався — перевірити", callback_data="check_subscription")],
        ]
    )


def lesson_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="▶️ Переглянути безкоштовний урок", callback_data="watch_lesson")],
            [InlineKeyboardButton(text="🏠 Головне меню", callback_data="menu")],
        ]
    )


def lesson_finished_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Я переглянув урок", callback_data="lesson_done")],
            [InlineKeyboardButton(text="🏠 Головне меню", callback_data="menu")],
        ]
    )


def back_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📝 Підібрати тариф", callback_data="apply")],
            [InlineKeyboardButton(text="👨‍💻 Написати Михайлу", url=CONTACT_URL)],
            [InlineKeyboardButton(text="🏠 Головне меню", callback_data="menu")],
        ]
    )


def recommendation_menu(tariff: str) -> InlineKeyboardMarkup:
    labels = {"BASIC": "БАЗОВИЙ", "GROUP": "ГРУПОВИЙ", "MASTER": "P2P MASTER"}
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"📚 Що входить у {labels[tariff]}",
                    callback_data=f"rec_details:{tariff}",
                )
            ],
            [InlineKeyboardButton(text="📲 Хочу консультацію", callback_data="contact_start")],
            [InlineKeyboardButton(text="🌐 Переглянути сайт", url=SITE_URL)],
            [InlineKeyboardButton(text="🏠 Головне меню", callback_data="menu")],
        ]
    )


def contact_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📱 Поділитися номером", request_contact=True)],
            [KeyboardButton(text="Використати мій Telegram")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
        input_field_placeholder="Номер телефону або @username",
    )


async def subscribed(bot: Bot, user_id: int) -> bool:
    if not CHANNEL_ID:
        logging.error('CHANNEL_ID is missing; subscription cannot be verified')
        return False
    try:
        member = await bot.get_chat_member(CHANNEL_ID, user_id)
        return member.status in {
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.CREATOR,
        } or (member.status == ChatMemberStatus.RESTRICTED and bool(getattr(member, 'is_member', False)))
    except Exception:
        logging.exception("Subscription check failed for %s", user_id)
        return False


async def show_menu(target: Message) -> None:
    await target.answer(
        "<b>REMARENKO P2P</b> 🚀\n\n"
        "Отримай безкоштовний практичний урок про P2P, подивись програму "
        "та підбери формат навчання під свій досвід і ціль.",
        reply_markup=main_menu(),
    )


async def send_lesson_card(message: Message, confirmed: bool = False) -> None:
    prefix = "✅ <b>Підписку підтверджено.</b>\n\n" if confirmed else ""
    caption = (
        prefix
        + "🎁 <b>Твій безкоштовний урок уже доступний!</b>\n\n"
        "У ньому ти побачиш практичну P2P-зв’язку, логіку розрахунку та ризики, "
        "які потрібно перевірити перед роботою. Результат залежить від ринку, "
        "комісій і правильності виконання.\n\n"
        "Після перегляду бот допоможе підібрати наступний крок."
    )
    cover_id = get_setting("lesson_cover_file_id")
    if cover_id:
        try:
            await message.answer_photo(photo=cover_id, caption=caption, reply_markup=lesson_menu())
            return
        except TelegramBadRequest:
            logging.exception("Stored lesson cover is invalid; falling back to text")
    await message.answer(caption, reply_markup=lesson_menu())


async def send_lesson_video(message: Message) -> None:
    video_id = get_setting("lesson_video_file_id")
    if video_id:
        try:
            await message.answer_video(
                video=video_id,
                caption=(
                    "▶️ <b>Безкоштовний практичний урок REMARENKO P2P</b>\n\n"
                    "Переглянь до кінця, а потім натисни «Я переглянув урок» — "
                    "бот підбере формат навчання під твою ситуацію."
                ),
                supports_streaming=True,
                reply_markup=lesson_finished_menu(),
            )
            update_user(message.chat.id, stage="lesson_video_sent")
            record_event(message.chat.id, "lesson_video_sent")
            return
        except TelegramBadRequest:
            logging.exception("Stored lesson video is invalid; using fallback")
    if LESSON_URL.startswith(("https://", "http://")):
        await message.answer(
            "Відео відкриється за кнопкою нижче:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="▶️ Відкрити урок", url=LESSON_URL)],
                    [InlineKeyboardButton(text="✅ Я переглянув урок", callback_data="lesson_done")],
                ]
            ),
        )
        update_user(message.chat.id, stage='lesson_video_sent')
        record_event(message.chat.id, 'lesson_video_sent', 'fallback_link_issued')
        return
    await message.answer(
        "🎬 Відео ще готується до публікації. Спробуй відкрити урок пізніше або напиши Михайлу.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="👨‍💻 Написати Михайлу", url=CONTACT_URL)],
                [InlineKeyboardButton(text="🏠 Головне меню", callback_data="menu")],
            ]
        ),
    )


async def lesson_gate(message: Message, confirmed: bool = False) -> None:
    user_id = message.chat.id
    if await subscribed(message.bot, user_id):
        update_user(user_id, subscribed=1, stage="lesson_opened")
        record_event(user_id, "lesson_opened")
        await send_lesson_card(message, confirmed=confirmed)
        return
    update_user(user_id, subscribed=0, stage="subscription_required")
    await message.answer(
        "Щоб відкрити безкоштовний урок, підпишися на канал "
        "<b>REMARENKO P2P</b>, а потім натисни кнопку перевірки.",
        reply_markup=subscription_menu(),
    )


async def send_program(message: Message) -> None:
    await message.answer(
        "<b>Програма REMARENKO P2P</b>\n\n"
        "🟢 <b>БАЗОВИЙ</b> — 12 модулів, відеоуроки, підтримка ментора, "
        "додаткові матеріали та 3 живі зідзвони.\n\n"
        "🔵 <b>ГРУПОВИЙ</b> — усе з Базового + 7 додаткових модулів, "
        "чат учнів і 5 живих зідзвонів.\n\n"
        "🟣 <b>P2P MASTER</b> — усе з Групового + 3 поглиблені модулі, "
        "розбір масштабування та 7 живих зідзвонів.\n\n"
        "Навчання дає систему розрахунку економіки, виконання угод і роботи "
        "з ризиками. Фінансовий результат не гарантується та залежить від практики й ринку.",
        reply_markup=back_menu(),
    )


async def send_tariffs(message: Message) -> None:
    await message.answer(
        "<b>Тарифи навчання</b> 💎\n\n"
        "🟢 <b>БАЗОВИЙ — $290</b>\nДля структурованого старту й опанування основ.\n\n"
        "🔵 <b>ГРУПОВИЙ — $450</b>\nДля практики в групі, додаткових модулів і більшої підтримки.\n\n"
        "🟣 <b>P2P MASTER — $740</b>\nДля поглибленої роботи, масштабування та максимального супроводу.\n\n"
        "Пройди коротке опитування — бот порекомендує формат під твою ситуацію.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📝 Підібрати тариф", callback_data="apply")],
                [InlineKeyboardButton(text="🌐 Детальніше на сайті", url=SITE_URL)],
                [InlineKeyboardButton(text="👨‍💻 Написати Михайлу", url=CONTACT_URL)],
                [InlineKeyboardButton(text="🏠 Головне меню", callback_data="menu")],
            ]
        ),
    )


def recommend_tariff(experience: str, capital: str, goal: str, support: str) -> tuple[str, str]:
    group_score = 0
    master_score = 0
    if experience == "first":
        group_score += 2
    elif experience == "active":
        master_score += 2
    if capital in {"500", "1500"}:
        group_score += 1
    elif capital == "more":
        master_score += 2
    if goal == "system":
        group_score += 2
    elif goal == "scale":
        master_score += 2
    if support == "group":
        group_score += 2
    elif support == "personal":
        master_score += 2

    if master_score >= 4 and (experience == "active" or goal == "scale" or support == "personal"):
        return "MASTER", "потрібні поглиблені інструменти, масштабування та максимальний супровід"
    if group_score >= 3 or experience == "first" or support == "group":
        return "GROUP", "потрібні практика, додаткові модулі, групове середовище та підтримка ментора"
    return "BASIC", "важливо послідовно пройти основу, безпечно підготуватися та зробити перші кроки"


def recommendation_text(tariff: str, reason: str) -> str:
    content = {
        "BASIC": (
            "🟢 <b>БАЗОВИЙ — $290</b>",
            "12 модулів, відеоуроки, матеріали, підтримка ментора та 3 живі зідзвони.",
        ),
        "GROUP": (
            "🔵 <b>ГРУПОВИЙ — $450</b>",
            "Усе з Базового + 7 модулів, чат учнів і 5 живих зідзвонів.",
        ),
        "MASTER": (
            "🟣 <b>P2P MASTER — $740</b>",
            "Усе з Групового + 3 поглиблені модулі та 7 живих зідзвонів.",
        ),
    }
    title, details = content[tariff]
    return (
        "🎯 <b>Твоя персональна рекомендація</b>\n\n"
        f"{title}\n\n"
        f"За твоїми відповідями тобі {html.escape(reason)}.\n\n"
        f"{details}\n\n"
        "Це рекомендація за відповідями, а не гарантія фінансового результату. "
        "Натисни нижче, щоб переглянути програму або обговорити деталі."
    )


async def send_recommendation(message: Message, data: dict[str, str]) -> None:
    tariff, reason = recommend_tariff(
        data.get("experience_code", "new"),
        data.get("capital_code", "200"),
        data.get("goal_code", "start"),
        data.get("support_code", "self"),
    )
    update_user(
        message.chat.id,
        stage="recommendation_shown",
        experience=data.get("experience"),
        capital=data.get("capital"),
        goal=data.get("goal"),
        support=data.get("support"),
        recommendation=tariff,
        recommendation_reason=reason,
    )
    record_event(message.chat.id, "recommendation_shown", tariff)
    queue_questionnaire(message.chat.id)
    await message.answer(recommendation_text(tariff, reason), reply_markup=recommendation_menu(tariff))
    await deliver_lead_notifications(message.bot)


async def show_lead_filters(message, origin):
    view = resolve_lead_view(origin)
    if not view: return
    keyboard=[]
    for label,field,choices in [('Етап','stage',STAGE_FILTERS),('Тариф','tariff',{'any':'Усі тарифи',**TARIFF_NAMES}),('Перший вхід у бот','date',DATE_FILTERS)]:
        for value,name in choices.items():
            mark='✅ ' if view.get(field,'any')==value else ''
            keyboard.append([InlineKeyboardButton(text=f'{mark}{label}: {name}',callback_data=f'av:set:{origin}:{field}:{value}')])
    keyboard.append([InlineKeyboardButton(text='📋 Показати список',callback_data=f'adminlead:list:{origin}:0')])
    await edit_admin(message,'<b>🎛 Фільтри</b>\nМожна поєднувати етап, тариф, дату та пошук.\nЕтап показує всіх, хто до нього дійшов. Дата — перший вхід у бот.\nЛічильники категорій враховують цей відбір.',InlineKeyboardMarkup(inline_keyboard=keyboard))


@router.callback_query(F.data.startswith('av:'))
async def admin_view_callback(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_CHAT_ID:
        await callback.answer('Доступ лише для адміністратора',show_alert=True)
        return
    parts=(callback.data or '').split(':')
    if len(parts)<3: return
    action,origin=parts[1:3]
    view=resolve_lead_view(origin)
    if not view: 
        await callback.answer('Відкрий /leads ще раз',show_alert=True)
        return
    await callback.answer()
    set_setting('admin_pending_input','')
    if action=='filters':
        await show_lead_filters(callback.message,origin)
    elif action=='set' and len(parts)==5:
        field,value=parts[3:5]
        valid={'stage':STAGE_FILTERS,'tariff':{'any':'',**TARIFF_NAMES},'date':DATE_FILTERS}
        if field not in valid or value not in valid[field]: return
        token=save_lead_view({**view,field:value})
        await show_lead_filters(callback.message,token)
    elif action in {'search','page'}:
        set_setting('admin_pending_input',json.dumps({'action':action,'origin':origin}))
        text=('🔎 Напиши ім’я, @нік, Telegram ID або частину контакту одним повідомленням. Пошук діє в поточній категорії та фільтрах.' if action=='search' else '🔢 Напиши номер сторінки одним повідомленням, наприклад: 12.')
        await edit_admin(callback.message,text,InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text='⬅️ Скасувати',callback_data=f'adminlead:list:{origin}:0')]]))
    elif action=='export':
        _,_,rows=lead_rows(view)
        if not rows:
            await callback.message.answer('За цим відбором немає записів для експорту.')
            return
        data=await asyncio.to_thread(excel_bytes,rows)
        stamp=utc_now().astimezone(KYIV).strftime('%Y-%m-%d_%H-%M')
        description=view_description(view)
        caption=f'📥 {LEAD_STATUSES[view["status"]]} · {len(rows)} записів\n{description}\nЗнімок даних на момент експорту. Зміни в Excel не змінюють бот.'
        await callback.message.answer_document(BufferedInputFile(data,filename=f'Remarenko_leads_{stamp}.xlsx'),caption=html.escape(caption[:1000]))


def is_admin_list_input(message):
    return (message.from_user.id==ADMIN_CHAT_ID and bool(message.text)
            and not message.text.startswith('/') and bool(get_setting('admin_pending_input')))


@router.message(is_admin_list_input)
async def admin_list_input(message: Message):
    pending=json.loads(get_setting('admin_pending_input') or '{}')
    origin=pending.get('origin','new')
    view=resolve_lead_view(origin)
    if not view: return
    value=(message.text or '').strip()
    if pending.get('action')=='page':
        count,_,_=lead_rows(view,0)
        total=max(1,(count+7)//8)
        if not value.isascii() or not value.isdigit() or not 1<=int(value)<=total:
            await message.answer(f'Введи номер від 1 до {total}, або /leads для виходу.')
            return
        page=int(value)-1
    else:
        if not value or len(value)>100:
            await message.answer('Введи від 1 до 100 символів, або /leads для виходу.')
            return
        origin=save_lead_view({**view,'search':value})
        page=0
    set_setting('admin_pending_input','')
    await show_leads(message,origin,page)


@router.message(CommandStart())
async def start(message: Message, state: FSMContext) -> None:
    payload = "direct"
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 2:
        payload = parts[1][:64]
    save_user(message, payload)
    schedule_funnel(message.from_user.id)
    await show_menu(message)


@router.message(Command("lesson"))
async def lesson_command(message: Message) -> None:
    save_user(message)
    await lesson_gate(message)


@router.message(Command("program"))
async def program_command(message: Message) -> None:
    save_user(message)
    await send_program(message)


@router.message(Command("tariffs"))
async def tariffs_command(message: Message) -> None:
    save_user(message)
    await send_tariffs(message)


@router.message(Command("support"))
async def support_command(message: Message) -> None:
    save_user(message)
    await message.answer(
        "Написати Михайлу щодо навчання:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="👨‍💻 @P2P_Marshal", url=CONTACT_URL)]]
        ),
    )


@router.callback_query(F.data == "menu")
async def menu_callback(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    await show_menu(callback.message)


@router.callback_query(F.data == "lesson")
async def lesson_callback(callback: CallbackQuery) -> None:
    await callback.answer()
    await lesson_gate(callback.message)


@router.callback_query(F.data == "check_subscription")
async def check_subscription(callback: CallbackQuery) -> None:
    if await subscribed(callback.bot, callback.from_user.id):
        update_user(callback.from_user.id, subscribed=1, stage="lesson_opened")
        record_event(callback.from_user.id, "subscription_confirmed")
        await callback.answer("Підписку підтверджено ✅")
        await send_lesson_card(callback.message, confirmed=True)
    else:
        await callback.answer("Підписку поки не знайдено", show_alert=True)


@router.callback_query(F.data == "watch_lesson")
async def watch_lesson(callback: CallbackQuery) -> None:
    if not await subscribed(callback.bot, callback.from_user.id):
        update_user(callback.from_user.id, subscribed=0, stage="subscription_required")
        await callback.answer("Спочатку підпишися на канал", show_alert=True)
        await callback.message.answer(
            "Підпишися на канал і натисни кнопку перевірки:", reply_markup=subscription_menu()
        )
        return
    await callback.answer("Завантажую урок…")
    await send_lesson_video(callback.message)


@router.callback_query(F.data == "lesson_done")
async def lesson_done(callback: CallbackQuery) -> None:
    user = get_user(callback.from_user.id)
    with connect() as db:
        issued = db.execute("SELECT 1 FROM funnel_events WHERE user_id=? AND event='lesson_video_sent' LIMIT 1", (callback.from_user.id,)).fetchone()
    if not user or not (user['lesson_received_at'] or issued):
        await callback.answer('Спочатку відкрий відеоурок кнопкою перегляду.', show_alert=True)
        return
    update_user(callback.from_user.id, stage="lesson_completed")
    record_event(callback.from_user.id, "lesson_completed")
    schedule_form_reminder(callback.from_user.id)
    await callback.answer("Урок відмічено як переглянутий ✅")
    await callback.message.answer(
        "🔥 <b>Чудово!</b> Відповідай на 4 короткі запитання кнопками — "
        "і бот одразу порекомендує формат навчання під твою ситуацію.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Підібрати формат →", callback_data="apply")]]
        ),
    )


@router.callback_query(F.data == "program")
async def program_callback(callback: CallbackQuery) -> None:
    await callback.answer()
    await send_program(callback.message)


@router.callback_query(F.data == "tariffs")
async def tariffs_callback(callback: CallbackQuery) -> None:
    await callback.answer()
    await send_tariffs(callback.message)


@router.callback_query(F.data == "apply")
async def apply_start(callback: CallbackQuery, state: FSMContext) -> None:
    if await state.get_state():
        await callback.answer()
        await ask_form_step(callback.message, await state.get_state())
        return
    await state.clear()
    await state.set_state(LeadForm.experience)
    update_user(callback.from_user.id, stage="questionnaire_started")
    record_event(callback.from_user.id, "questionnaire_started")
    schedule_form_reminder(callback.from_user.id)
    await callback.answer()
    await callback.message.answer(
        "<b>1/4. Який у тебе досвід у P2P?</b>",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Новачок", callback_data="exp:new")],
                [InlineKeyboardButton(text="Були перші угоди", callback_data="exp:first")],
                [InlineKeyboardButton(text="Уже працюю", callback_data="exp:active")],
            ]
        ),
    )


FORM_STEPS = {
    'LeadForm:experience': ('1/4. Який у тебе досвід у P2P?', [('Новачок', 'exp:new'), ('Були перші угоди', 'exp:first'), ('Уже працюю', 'exp:active')]),
    'LeadForm:capital': ('2/4. Який капітал плануєш використовувати для практики?', [('До $200', 'cap:200'), ('$200–500', 'cap:500'), ('$500–1 500', 'cap:1500'), ('Понад $1 500', 'cap:more')]),
    'LeadForm:goal': ('3/4. Яка твоя головна ціль?', [('Стартувати з нуля', 'goal:start'), ('Побудувати стабільну систему', 'goal:system'), ('Збільшити обороти', 'goal:scale')]),
    'LeadForm:support': ('4/4. Який рівень підтримки тобі потрібний?', [('Навчатися самостійно', 'sup:self'), ('Група + підтримка ментора', 'sup:group'), ('Максимальний супровід', 'sup:personal')]),
}


async def ask_form_step(message: Message, step: str) -> None:
    if step == LeadForm.contact.state:
        await message.answer('Залиш власний номер телефону або натисни «Використати мій Telegram».', reply_markup=contact_keyboard())
    elif step in FORM_STEPS:
        title, choices = FORM_STEPS[step]
        await message.answer(f'<b>{title}</b>', reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=label, callback_data=data)] for label, data in choices]))


@router.callback_query(F.data == 'resume_form')
async def resume_form(callback: CallbackQuery, state: FSMContext) -> None:
    step = await state.get_state()
    if step:
        await callback.answer('Продовжуємо з місця, де зупинились')
        await ask_form_step(callback.message, step)
    else:
        user = get_user(callback.from_user.id)
        if user and user['stage'] == 'application_completed':
            await callback.answer('Твоя заявка вже збережена ✅', show_alert=True)
        elif user and user['recommendation']:
            await contact_start(callback, state)
        else:
            await apply_start(callback, state)


@router.callback_query(F.data == 'stop_reminders')
async def stop_reminders(callback: CallbackQuery) -> None:
    update_user(callback.from_user.id, reminders_enabled=0)
    cancel_pending_funnel(callback.from_user.id)
    await callback.answer('Нагадування вимкнено')
    await callback.message.answer('🔕 Автоматичні нагадування вимкнено. Урок, анкета та меню залишаються доступними.')


@router.callback_query(F.data.startswith('adminlead:'))
async def admin_lead_callback(callback: CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        await callback.answer('Доступ лише для адміністратора', show_alert=True)
        return
    await callback.answer()
    parts = (callback.data or '').split(':')
    try:
        if len(parts) == 4 and parts[1] == 'list':
            set_setting('admin_pending_input','')
            await show_leads(callback.message, parts[2], int(parts[3]), edit=True)
        elif parts[1] in {'detail', 'status'}:
            offset = 3 if parts[1] == 'detail' else 4
            if len(parts) not in {offset, offset + 2}: return
            origin, page = (parts[offset], int(parts[offset+1])) if len(parts) == offset+2 else ('new', 0)
            if not resolve_lead_view(origin) or page < 0: return
            uid = int(parts[2])
            if parts[1] == 'status':
                if parts[3] not in {'new', 'work', 'done'}: return
                update_user(uid, lead_status=parts[3])
            user = get_user(uid)
            if not user: return
            status = LEAD_STATUSES.get(user['lead_status'], '🆕 Нові')
            text = f'<b>Клієнт №{uid}</b> · {status}\n\n'
            labels = [('first_name', 'Ім’я'), ('username', 'Нік'), ('source', 'Джерело'),
                      ('experience', 'Досвід'), ('capital', 'Капітал'), ('goal', 'Ціль'),
                      ('support', 'Підтримка'), ('recommendation', 'Рекомендація'), ('contact', 'Контакт'), ('stage', 'Етап')]
            for key, label in labels:
                value = user[key] or '—'
                if key == 'stage': value = STAGE_LABELS.get(value, value)
                if key == 'recommendation': value = TARIFF_NAMES.get(value, value)
                text += f'{label}: {html.escape(str(value))}\n'
            try:
                await callback.message.edit_text(text, reply_markup=lead_buttons(uid, origin, page))
            except TelegramBadRequest as error:
                if 'message is not modified' not in str(error).lower():
                    raise
    except (ValueError, IndexError):
        return


@router.message(Command('leads'))
async def leads(message: Message) -> None:
    if message.from_user.id == ADMIN_CHAT_ID:
        set_setting('admin_pending_input','')
        await show_leads(message)


@router.message(Command('people'))
async def people(message: Message) -> None:
    if message.from_user.id == ADMIN_CHAT_ID:
        set_setting('admin_pending_input','')
        await show_leads(message, 'people')


@router.message(Command('sync_leads'))
async def sync_leads(message: Message) -> None:
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    with connect() as db:
        rows = db.execute('''SELECT telegram_id FROM users WHERE recommendation IS NOT NULL
            AND NOT EXISTS(SELECT 1 FROM lead_notifications WHERE user_id=users.telegram_id)''').fetchall()
    for row in rows:
        queue_questionnaire(row['telegram_id'])
    await message.answer(f'✅ Додано збережених анкет до черги: {len(rows)}. Перелік: /leads')
    await deliver_lead_notifications(message.bot)


def is_channel_member(member) -> bool:
    return member.status in {ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR} or (
        member.status == ChatMemberStatus.RESTRICTED and bool(getattr(member, 'is_member', False)))


@router.chat_member()
async def track_channel_join(event: ChatMemberUpdated) -> None:
    if CHANNEL_ID not in {str(event.chat.id), '@' + (event.chat.username or '')}:
        return
    if is_channel_member(event.old_chat_member) or not is_channel_member(event.new_chat_member):
        return
    user = event.new_chat_member.user
    if user.is_bot: return
    invite = event.invite_link.invite_link if event.invite_link else ''
    via_link = int(bool(invite) and invite.rstrip('/') == CHANNEL_URL.rstrip('/'))
    stamp = event.date.isoformat()
    with connect() as db:
        db.execute('''INSERT INTO channel_joins(user_id,first_joined_at,last_joined_at,via_bot_link)
            VALUES(?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET last_joined_at=excluded.last_joined_at,
            via_bot_link=MAX(channel_joins.via_bot_link,excluded.via_bot_link)''', (user.id, stamp, stamp, via_link))


@router.callback_query(LeadForm.experience, F.data.startswith("exp:"))
async def apply_experience(callback: CallbackQuery, state: FSMContext) -> None:
    code = callback.data.split(":", 1)[1]
    values = {"new": "Новачок", "first": "Були перші угоди", "active": "Уже працюю"}
    await state.update_data(experience=values.get(code, "Новачок"), experience_code=code)
    update_user(callback.from_user.id, experience=values.get(code, 'Новачок'))
    await state.set_state(LeadForm.capital)
    await callback.answer()
    await callback.message.answer(
        "<b>2/4. Який капітал плануєш використовувати для практики?</b>",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="До $200", callback_data="cap:200")],
                [InlineKeyboardButton(text="$200–500", callback_data="cap:500")],
                [InlineKeyboardButton(text="$500–1 500", callback_data="cap:1500")],
                [InlineKeyboardButton(text="Понад $1 500", callback_data="cap:more")],
            ]
        ),
    )


@router.callback_query(LeadForm.capital, F.data.startswith("cap:"))
async def apply_capital(callback: CallbackQuery, state: FSMContext) -> None:
    code = callback.data.split(":", 1)[1]
    values = {
        "200": "До $200",
        "500": "$200–500",
        "1500": "$500–1 500",
        "more": "Понад $1 500",
    }
    await state.update_data(capital=values.get(code, "До $200"), capital_code=code)
    update_user(callback.from_user.id, capital=values.get(code, 'До $200'))
    await state.set_state(LeadForm.goal)
    await callback.answer()
    await callback.message.answer(
        "<b>3/4. Яка твоя головна ціль?</b>",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Стартувати з нуля", callback_data="goal:start")],
                [InlineKeyboardButton(text="Побудувати стабільну систему", callback_data="goal:system")],
                [InlineKeyboardButton(text="Збільшити обороти", callback_data="goal:scale")],
            ]
        ),
    )


@router.callback_query(LeadForm.goal, F.data.startswith("goal:"))
async def apply_goal(callback: CallbackQuery, state: FSMContext) -> None:
    code = callback.data.split(":", 1)[1]
    values = {
        "start": "Стартувати з нуля",
        "system": "Побудувати стабільну систему",
        "scale": "Збільшити обороти",
    }
    await state.update_data(goal=values.get(code, "Стартувати з нуля"), goal_code=code)
    update_user(callback.from_user.id, goal=values.get(code, 'Стартувати з нуля'))
    await state.set_state(LeadForm.support)
    await callback.answer()
    await callback.message.answer(
        "<b>4/4. Який рівень підтримки тобі потрібний?</b>",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Навчатися самостійно", callback_data="sup:self")],
                [InlineKeyboardButton(text="Група + підтримка ментора", callback_data="sup:group")],
                [InlineKeyboardButton(text="Максимальний супровід", callback_data="sup:personal")],
            ]
        ),
    )


@router.callback_query(LeadForm.support, F.data.startswith("sup:"))
async def apply_support(callback: CallbackQuery, state: FSMContext) -> None:
    code = callback.data.split(":", 1)[1]
    values = {
        "self": "Навчатися самостійно",
        "group": "Група + підтримка ментора",
        "personal": "Максимальний супровід",
    }
    await state.update_data(support=values.get(code, "Навчатися самостійно"), support_code=code)
    update_user(callback.from_user.id, support=values.get(code, 'Навчатися самостійно'))
    data = await state.get_data()
    await state.clear()
    await callback.answer("Рекомендацію сформовано ✅")
    await send_recommendation(callback.message, data)


@router.callback_query(F.data.startswith("rec_details:"))
async def recommendation_details(callback: CallbackQuery) -> None:
    tariff = callback.data.split(":", 1)[1]
    texts = {
        "BASIC": (
            "🟢 <b>БАЗОВИЙ</b>\n\n12 модулів, відеоуроки, алгоритми, шаблони, "
            "гайди, чек-листи, підтримка ментора та 3 живі зідзвони."
        ),
        "GROUP": (
            "🔵 <b>ГРУПОВИЙ</b>\n\nУсе з Базового + 7 додаткових модулів, "
            "закритий чат учнів і 5 живих зідзвонів."
        ),
        "MASTER": (
            "🟣 <b>P2P MASTER</b>\n\nУсе з Групового + 3 поглиблені модулі, "
            "розбір масштабування та 7 живих зідзвонів."
        ),
    }
    await callback.answer()
    await callback.message.answer(
        texts.get(tariff, "Деталі тарифу недоступні."),
        reply_markup=recommendation_menu(tariff if tariff in texts else "BASIC"),
    )


@router.callback_query(F.data == "contact_start")
async def contact_start(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(LeadForm.contact)
    update_user(callback.from_user.id, stage="contact_requested")
    await callback.answer()
    username_hint = html.escape(f"@{callback.from_user.username}") if callback.from_user.username else "Telegram-профіль"
    await callback.message.answer(
        "Залиш номер телефону або натисни «Використати мій Telegram».\n\n"
        f"Після цього Михайло перегляне відповіді та напише тобі. Поточний контакт: {username_hint}",
        reply_markup=contact_keyboard(),
    )


@router.message(LeadForm.contact, ~F.text.startswith('/'))
async def apply_contact(message: Message, state: FSMContext) -> None:
    if message.contact and message.contact.phone_number:
        if message.contact.user_id != message.from_user.id:
            await message.answer('Поділися власним номером кнопкою або використай свій Telegram.')
            return
        contact = message.contact.phone_number
    elif (message.text or "").strip() == "Використати мій Telegram":
        contact = f"@{message.from_user.username}" if message.from_user.username else f"Telegram ID {message.from_user.id}"
    else:
        contact = (message.text or "").strip()[:200]
    if not contact:
        await message.answer("Надішли номер телефону, @username або скористайся кнопкою нижче.")
        return

    user = get_user(message.from_user.id)
    username = f"@{message.from_user.username}" if message.from_user.username else "немає"
    admin_text = (
        "🔥 <b>Нова заявка з безкоштовного уроку</b>\n\n"
        f"Ім’я: {html.escape(message.from_user.full_name)}\n"
        f"Telegram: {html.escape(username)}\n"
        f"ID: <code>{message.from_user.id}</code>\n"
        f"Джерело: {html.escape((user['source'] if user else 'direct') or 'direct')}\n"
        f"Досвід: {html.escape((user['experience'] if user else None) or '—')}\n"
        f"Капітал: {html.escape((user['capital'] if user else None) or '—')}\n"
        f"Ціль: {html.escape((user['goal'] if user else None) or '—')}\n"
        f"Підтримка: {html.escape((user['support'] if user else None) or '—')}\n"
        f"Рекомендація: {html.escape((user['recommendation'] if user else None) or '—')}\n"
        f"Контакт: {html.escape(contact)}"
    )
    store_application(message.from_user.id, contact, admin_text)
    await state.clear()
    await message.answer(
        "✅ <b>Заявку прийнято.</b>\n\n"
        "Михайло перегляне твої відповіді та зв’яжеться з тобою особисто.",
        reply_markup=ReplyKeyboardRemove(),
    )
    await message.answer(
        "Поки очікуєш, можеш переглянути повну програму або повернутися в меню:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📚 Програма навчання", callback_data="program")],
                [InlineKeyboardButton(text="🌐 Переглянути сайт", url=SITE_URL)],
                [InlineKeyboardButton(text="🏠 Головне меню", callback_data="menu")],
            ]
        ),
    )
    await deliver_lead_notifications(message.bot)


@router.message(Command("set_lesson_video"))
async def set_lesson_video(message: Message) -> None:
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    replied = message.reply_to_message
    if not replied or not replied.video:
        await message.answer("Надішли відео боту, натисни на ньому «Відповісти» та введи /set_lesson_video")
        return
    set_setting("lesson_video_file_id", replied.video.file_id)
    await message.answer("✅ Відеоурок збережено. Тепер бот надсилатиме його клієнтам у Telegram.")


@router.message(Command("set_lesson_cover"))
async def set_lesson_cover(message: Message) -> None:
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    replied = message.reply_to_message
    if not replied or not replied.photo:
        await message.answer("Надішли картинку боту, натисни на ній «Відповісти» та введи /set_lesson_cover")
        return
    set_setting("lesson_cover_file_id", replied.photo[-1].file_id)
    await message.answer("✅ Обкладинку уроку збережено.")


@router.message(Command("media_status"))
async def media_status(message: Message) -> None:
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    await message.answer(
        "🎬 <b>Матеріали уроку</b>\n\n"
        f"Обкладинка: {'✅ завантажена' if get_setting('lesson_cover_file_id') else '❌ немає'}\n"
        f"Відео: {'✅ завантажене' if get_setting('lesson_video_file_id') else '❌ немає'}\n"
        f"Резервне посилання: {'✅ налаштоване' if LESSON_URL.startswith(('https://', 'http://')) else '❌ немає'}"
    )


@router.message(Command("test_timers"))
async def test_timers(message: Message) -> None:
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    save_user(message)
    schedule_funnel(message.from_user.id, test_mode=True)
    await message.answer(
        "🧪 Тестові таймери запущено лише для тебе:\n"
        "1 хв — нагадування; 2 хв — корисний блок; 3 хв — тариф; 4 хв — фінальний CTA."
    )


@router.message(Command("reset_funnel"))
async def reset_funnel(message: Message, state: FSMContext) -> None:
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    await state.clear()
    with connect() as db:
        db.execute(
            """
            UPDATE users SET stage='started', experience=NULL, capital=NULL, goal=NULL,
                support=NULL, recommendation=NULL, recommendation_reason=NULL, contact=NULL,
                lesson_received_at=NULL,lesson_completed_at=NULL,questionnaire_started_at=NULL,
                reminders_enabled=1,lead_status='new',
                updated_at=? WHERE telegram_id=?
            """,
            (now_iso(), message.from_user.id),
        )
        db.execute("DELETE FROM scheduled_messages WHERE user_id=?", (message.from_user.id,))
        db.execute("DELETE FROM lead_notifications WHERE user_id=?", (message.from_user.id,))
        db.execute("DELETE FROM funnel_events WHERE user_id=?", (message.from_user.id,))
        db.commit()
    await message.answer("✅ Твій тестовий шлях очищено. Тепер надішли /start")


@router.message(Command("version"))
async def version(message: Message) -> None:
    if message.from_user.id == ADMIN_CHAT_ID:
        await message.answer(f"REMARENKO FREE BOT v{BOT_VERSION}")


@router.message(Command("stats"))
async def stats(message: Message) -> None:
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    with connect() as db:
        total = db.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        subscribed_count = db.execute("SELECT COUNT(*) FROM users WHERE subscribed=1").fetchone()[0]
        videos = db.execute(
            "SELECT COUNT(DISTINCT user_id) FROM funnel_events WHERE event='lesson_video_sent'"
        ).fetchone()[0]
        lessons = db.execute(
            "SELECT COUNT(DISTINCT user_id) FROM funnel_events WHERE event='lesson_completed'"
        ).fetchone()[0]
        recommendations = db.execute(
            "SELECT COUNT(DISTINCT user_id) FROM funnel_events WHERE event='recommendation_shown'"
        ).fetchone()[0]
        leads = db.execute(
            "SELECT COUNT(DISTINCT user_id) FROM funnel_events WHERE event='application_completed'"
        ).fetchone()[0]
        pending = db.execute(
            "SELECT COUNT(*) FROM scheduled_messages WHERE sent_at IS NULL AND canceled_at IS NULL"
        ).fetchone()[0]
        incomplete = db.execute("SELECT COUNT(*) FROM users WHERE questionnaire_started_at IS NOT NULL AND contact IS NULL").fetchone()[0]
        new_leads = db.execute("SELECT COUNT(*) FROM users WHERE (recommendation IS NOT NULL OR contact IS NOT NULL) AND lead_status='new'").fetchone()[0]
        undelivered = db.execute('SELECT COUNT(*) FROM lead_notifications WHERE sent_at IS NULL').fetchone()[0]
        joins = db.execute('SELECT COUNT(*),COALESCE(SUM(via_bot_link),0) FROM channel_joins').fetchone()
        sources = db.execute('''SELECT source,COUNT(*) AS total,SUM(recommendation IS NOT NULL OR contact IS NOT NULL) AS leads
            FROM users GROUP BY source ORDER BY total DESC LIMIT 10''').fetchall()
    await message.answer(
        "📊 <b>Статистика воронки</b>\n\n"
        f"Користувачів: {total}\n"
        f"Підписку підтверджено: {subscribed_count}\n"
        f"Урок отримали (відео / посилання): {videos}\n"
        f"Урок позначили переглянутим: {lessons}\n"
        f"Отримали рекомендацію: {recommendations}\n"
        f"Залишили контакт: {leads}\n"
        f"Незавершених анкет без контакту: {incomplete}\n"
        f"Нових заявок до опрацювання: {new_leads}\n"
        f"Заявок очікують доставки тобі: {undelivered}\n"
        f"Нових вступів у канал від запуску обліку: {joins[0]}\n"
        f"Із них за посиланням із бота: {joins[1]}\n"
        f"Запланованих повідомлень: {pending}\n\n"
        '<b>Джерела: користувачі / анкети</b>\n' + '\n'.join(
            f"{html.escape(row['source'])}: {row['total']} / {row['leads']}" for row in sources) +
        '\n\n/leads — заявки зі статусами\n/people — усі користувачі бота\n'
        'Підтвердження підписки не означає новий вступ. Історичні вступи не враховані.'
    )


def should_skip_scheduled(kind: str, user: sqlite3.Row | None) -> bool:
    if not user:
        return True
    stage = user["stage"] or "started"
    if 'reminders_enabled' in user.keys() and not user['reminders_enabled']:
        return True
    if stage == "application_completed":
        return True
    if kind == "lesson_reminder" and stage in {
        "lesson_completed",
        "questionnaire_started",
        "recommendation_shown",
        "contact_requested",
    }:
        return True
    if kind in {"tariff_offer", "final_cta", "value_case"} and stage in {
        "lesson_completed",
        "questionnaire_started",
        "recommendation_shown",
        "contact_requested",
    }:
        return True
    return False


async def send_scheduled_message(bot: Bot, kind: str, user_id: int, user: sqlite3.Row) -> None:
    if kind in {'form_reminder', 'form_reminder_24', 'form_reminder_48'}:
        text = ('📝 <b>Залишився останній крок — твій контакт.</b>\n\n'
                'Рекомендація вже готова. Залиш контакт, щоб Михайло міг обговорити з тобою навчання.'
                if user['recommendation'] else
                '📝 <b>Ти почав підбирати формат навчання.</b>\n\n'
                'Продовж анкету з місця, де зупинився. Відповіді збережені — повторювати їх не потрібно.'
                if user['questionnaire_started_at'] else
                '📝 <b>Який формат навчання підійде тобі?</b>\n\n'
                'Ти позначив урок переглянутим. Дай відповіді на 4 короткі питання — '
                'отримаєш рекомендацію, а Михайло зможе побачити твою анкету.')
        await bot.send_message(user_id, text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text='📝 Підібрати / продовжити формат', callback_data='resume_form')],
            [InlineKeyboardButton(text='🔕 Вимкнути нагадування', callback_data='stop_reminders')]]))
    elif kind == "lesson_reminder":
        if not user["subscribed"]:
            await bot.send_message(
                user_id,
                "🎁 <b>Твій безкоштовний урок уже чекає.</b>\n\n"
                "Підпишися на REMARENKO P2P і підтвердь підписку — після цього урок відкриється автоматично.",
                reply_markup=subscription_menu(),
            )
        else:
            await bot.send_message(
                user_id,
                "⏰ <b>Нагадую про безкоштовний урок.</b>\n\n"
                "У ньому — практична логіка P2P-зв’язки, розрахунок і важливі ризики. "
                "Відкрий урок, коли матимеш кілька вільних хвилин.",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="▶️ Переглянути урок", callback_data="watch_lesson")]]
                ),
            )
    elif kind == "value_case":
        await bot.send_message(
            user_id,
            "💡 <b>Чому сама цифра прибутку нічого не гарантує?</b>\n\n"
            "У P2P потрібно враховувати курс входу й виходу, комісії, ліквідність, "
            "ліміти та ризики контрагента. Саме тому в уроці я показую не «магічну кнопку», "
            "а послідовність розрахунку й перевірки.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="▶️ Відкрити урок", callback_data="watch_lesson")]]
            ),
        )
    elif kind == "tariff_offer":
        await bot.send_message(
            user_id,
            "🎯 <b>Хочеш зрозуміти, який наступний крок підходить саме тобі?</b>\n\n"
            "Відповідай на 4 короткі запитання. Бот врахує твій досвід, капітал, "
            "ціль і потрібний рівень підтримки та покаже рекомендацію.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="📝 Підібрати формат", callback_data="apply")]]
            ),
        )
    elif kind == "final_cta":
        await bot.send_message(
            user_id,
            "👋 <b>Залишилися запитання щодо P2P або навчання?</b>\n\n"
            "Можеш переглянути програму, підібрати формат або написати мені особисто. "
            "Я допоможу зрозуміти, з чого логічно почати у твоїй ситуації.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="📝 Підібрати формат", callback_data="apply")],
                    [InlineKeyboardButton(text="👨‍💻 Написати Михайлу", url=CONTACT_URL)],
                ]
            ),
        )
    else:
        raise ValueError(f"Unknown scheduled message kind: {kind}")


async def funnel_worker(bot: Bot) -> None:
    while True:
        try:
            await deliver_lead_notifications(bot)
            collapse_overdue_form_reminders()
            with connect() as db:
                rows = db.execute(
                    """
                    SELECT * FROM scheduled_messages
                    WHERE sent_at IS NULL AND canceled_at IS NULL AND due_at<=?
                    ORDER BY due_at LIMIT 25
                    """,
                    (now_iso(),),
                ).fetchall()
            for row in rows:
                user = get_user(row["user_id"])
                if should_skip_scheduled(row["kind"], user):
                    with connect() as db:
                        db.execute(
                            "UPDATE scheduled_messages SET canceled_at=? WHERE id=?",
                            (now_iso(), row["id"]),
                        )
                        db.commit()
                    continue
                try:
                    await send_scheduled_message(bot, row["kind"], row["user_id"], user)
                    with connect() as db:
                        db.execute(
                            "UPDATE scheduled_messages SET sent_at=? WHERE id=?",
                            (now_iso(), row["id"]),
                        )
                        db.commit()
                    record_event(row["user_id"], f"timer_{row['kind']}")
                except TelegramForbiddenError:
                    with connect() as db:
                        db.execute(
                            "UPDATE scheduled_messages SET canceled_at=?, last_error=? WHERE id=?",
                            (now_iso(), "bot blocked by user", row["id"]),
                        )
                        db.commit()
                except Exception as exc:
                    logging.exception("Failed scheduled message %s", row["id"])
                    attempts = row["attempts"] + 1
                    with connect() as db:
                        if attempts >= 5:
                            db.execute(
                                "UPDATE scheduled_messages SET canceled_at=?, attempts=?, last_error=? WHERE id=?",
                                (now_iso(), attempts, str(exc)[:500], row["id"]),
                            )
                        else:
                            retry_at = (utc_now() + timedelta(minutes=10)).isoformat()
                            db.execute(
                                "UPDATE scheduled_messages SET due_at=?, attempts=?, last_error=? WHERE id=?",
                                (retry_at, attempts, str(exc)[:500], row["id"]),
                            )
                        db.commit()
        except Exception:
            logging.exception("Funnel worker iteration failed")
        await asyncio.sleep(WORKER_INTERVAL_SECONDS)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    for name, value in [('CHANNEL_URL', CHANNEL_URL), ('SITE_URL', SITE_URL),
                        ('CONTACT_URL', CONTACT_URL), ('LESSON_URL', LESSON_URL)]:
        if name == 'LESSON_URL' and not value:
            continue
        parsed = urlparse(value)
        if parsed.scheme not in {'http', 'https'} or not parsed.hostname or any(c.isspace() for c in value):
            raise RuntimeError(f'{name}: expected an http(s) URL without spaces')
    connect().close()
    with connect() as db:
        unfinished = db.execute('''SELECT telegram_id FROM users WHERE reminders_enabled=1 AND contact IS NULL
            AND stage IN ('lesson_completed','questionnaire_started','recommendation_shown','contact_requested')''').fetchall()
    for user in unfinished:
        schedule_form_reminder(user['telegram_id'])
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=SQLiteStorage(), events_isolation=SimpleEventIsolation())
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=False)
    worker = asyncio.create_task(funnel_worker(bot))
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        worker.cancel()
        with suppress(asyncio.CancelledError):
            await worker


if __name__ == "__main__":
    asyncio.run(main())
