import asyncio
import html
import logging
import os
import sqlite3
from contextlib import suppress
from datetime import datetime, timedelta, timezone

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)


BOT_VERSION = "2.0.0"
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
        return max(0.0, float(os.getenv(name, str(default))))
    except ValueError:
        logging.warning("Invalid %s; using %s", name, default)
        return default


REMINDER_MINUTES = env_number("FUNNEL_REMINDER_MINUTES", 30)
VALUE_HOURS = env_number("FUNNEL_VALUE_HOURS", 4)
OFFER_HOURS = env_number("FUNNEL_OFFER_HOURS", 24)
FINAL_HOURS = env_number("FUNNEL_FINAL_HOURS", 48)
WORKER_INTERVAL_SECONDS = env_number("FUNNEL_WORKER_INTERVAL_SECONDS", 20)

router = Router()


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


def connect() -> sqlite3.Connection:
    folder = os.path.dirname(DATABASE_PATH)
    if folder:
        os.makedirs(folder, exist_ok=True)
    db = sqlite3.connect(DATABASE_PATH, timeout=20)
    db.row_factory = sqlite3.Row
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
    for column in ("support", "recommendation", "recommendation_reason"):
        if column not in existing:
            db.execute(f"ALTER TABLE users ADD COLUMN {column} TEXT")
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
    db.commit()
    return db


def record_event(user_id: int, event: str, details: str | None = None) -> None:
    with connect() as db:
        db.execute(
            "INSERT INTO funnel_events (user_id, event, details, created_at) VALUES (?, ?, ?, ?)",
            (user_id, event, details, now_iso()),
        )
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
    }
    clean = {key: value for key, value in values.items() if key in allowed}
    if not clean:
        return
    clean["updated_at"] = now_iso()
    fields = ", ".join(f"{key}=?" for key in clean)
    with connect() as db:
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


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎁 Отримати безкоштовний урок", callback_data="lesson")],
            [InlineKeyboardButton(text="📚 Програма навчання", callback_data="program")],
            [InlineKeyboardButton(text="💎 Тарифи", callback_data="tariffs")],
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
            [InlineKeyboardButton(text="✅ Я переглянув урок", callback_data="lesson_done")],
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
        return True
    try:
        member = await bot.get_chat_member(CHANNEL_ID, user_id)
        return member.status in {
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.CREATOR,
        }
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
        return
    await message.answer(
        "🎬 Відео ще готується до публікації. Бот повідомить, щойно воно буде доступне.",
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
    await message.answer(recommendation_text(tariff, reason), reply_markup=recommendation_menu(tariff))


@router.message(CommandStart())
async def start(message: Message, state: FSMContext) -> None:
    await state.clear()
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
    await state.clear()
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
    update_user(callback.from_user.id, stage="lesson_completed")
    record_event(callback.from_user.id, "lesson_completed")
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
    await state.clear()
    await state.set_state(LeadForm.experience)
    update_user(callback.from_user.id, stage="questionnaire_started")
    record_event(callback.from_user.id, "questionnaire_started")
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


@router.callback_query(LeadForm.experience, F.data.startswith("exp:"))
async def apply_experience(callback: CallbackQuery, state: FSMContext) -> None:
    code = callback.data.split(":", 1)[1]
    values = {"new": "Новачок", "first": "Були перші угоди", "active": "Уже працюю"}
    await state.update_data(experience=values.get(code, "Новачок"), experience_code=code)
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
    username_hint = f"@{callback.from_user.username}" if callback.from_user.username else "Telegram-профіль"
    await callback.message.answer(
        "Залиш номер телефону або натисни «Використати мій Telegram».\n\n"
        f"Після цього Михайло перегляне відповіді та напише тобі. Поточний контакт: {username_hint}",
        reply_markup=contact_keyboard(),
    )


@router.message(LeadForm.contact)
async def apply_contact(message: Message, state: FSMContext) -> None:
    if message.contact and message.contact.phone_number:
        contact = message.contact.phone_number
    elif (message.text or "").strip() == "Використати мій Telegram":
        contact = f"@{message.from_user.username}" if message.from_user.username else f"Telegram ID {message.from_user.id}"
    else:
        contact = (message.text or "").strip()[:200]
    if not contact:
        await message.answer("Надішли номер телефону, @username або скористайся кнопкою нижче.")
        return

    user = get_user(message.from_user.id)
    update_user(message.from_user.id, stage="application_completed", contact=contact)
    cancel_pending_funnel(message.from_user.id)
    record_event(message.from_user.id, "application_completed")
    user = get_user(message.from_user.id) or user
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
    try:
        await message.bot.send_message(
            ADMIN_CHAT_ID,
            admin_text,
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="💬 Відкрити користувача", url=f"tg://user?id={message.from_user.id}")]
                ]
            ),
        )
    except Exception:
        logging.exception("Failed to notify admin")
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
                updated_at=? WHERE telegram_id=?
            """,
            (now_iso(), message.from_user.id),
        )
        db.execute("DELETE FROM scheduled_messages WHERE user_id=?", (message.from_user.id,))
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
    await message.answer(
        "📊 <b>Статистика воронки</b>\n\n"
        f"Користувачів: {total}\n"
        f"Підписку підтверджено: {subscribed_count}\n"
        f"Відео відкрили: {videos}\n"
        f"Урок позначили переглянутим: {lessons}\n"
        f"Отримали рекомендацію: {recommendations}\n"
        f"Залишили заявку: {leads}\n"
        f"Запланованих повідомлень: {pending}"
    )


def should_skip_scheduled(kind: str, user: sqlite3.Row | None) -> bool:
    if not user:
        return True
    stage = user["stage"] or "started"
    if stage == "application_completed":
        return True
    if kind == "lesson_reminder" and stage in {
        "lesson_completed",
        "questionnaire_started",
        "recommendation_shown",
        "contact_requested",
    }:
        return True
    if kind in {"tariff_offer", "final_cta"} and stage in {
        "recommendation_shown",
        "contact_requested",
    }:
        return True
    return False


async def send_scheduled_message(bot: Bot, kind: str, user_id: int, user: sqlite3.Row) -> None:
    if kind == "lesson_reminder":
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
    connect().close()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=False)
    worker = asyncio.create_task(funnel_worker(bot))
    try:
        await dp.start_polling(bot)
    finally:
        worker.cancel()
        with suppress(asyncio.CancelledError):
            await worker


if __name__ == "__main__":
    asyncio.run(main())
