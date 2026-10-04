import asyncio
import logging
import os
import sqlite3
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message


BOT_TOKEN = os.environ["BOT_TOKEN"].strip()
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "788167958"))
DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/remarenko_free.sqlite3")
CHANNEL_ID = os.getenv("CHANNEL_ID", "").strip()
CHANNEL_URL = os.getenv("CHANNEL_URL", "https://t.me/").strip()
LESSON_URL = os.getenv("LESSON_URL", "https://youtu.be/").strip()
SITE_URL = os.getenv("SITE_URL", "https://remarenkop2p.com/").strip()
CONTACT_URL = os.getenv("CONTACT_URL", "https://t.me/P2P_Marshal").strip()

router = Router()


class LeadForm(StatesGroup):
    experience = State()
    capital = State()
    goal = State()
    contact = State()


def connect() -> sqlite3.Connection:
    folder = os.path.dirname(DATABASE_PATH)
    if folder:
        os.makedirs(folder, exist_ok=True)
    db = sqlite3.connect(DATABASE_PATH)
    db.row_factory = sqlite3.Row
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
            contact TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    db.commit()
    return db


def save_user(message: Message, source: str = "direct") -> None:
    now = datetime.now(timezone.utc).isoformat()
    with connect() as db:
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


def update_user(user_id: int, **values: object) -> None:
    allowed = {"stage", "subscribed", "experience", "capital", "goal", "contact"}
    clean = {key: value for key, value in values.items() if key in allowed}
    if not clean:
        return
    clean["updated_at"] = datetime.now(timezone.utc).isoformat()
    fields = ", ".join(f"{key}=?" for key in clean)
    with connect() as db:
        db.execute(
            f"UPDATE users SET {fields} WHERE telegram_id=?",
            (*clean.values(), user_id),
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
            [InlineKeyboardButton(text="▶️ Переглянути урок", url=LESSON_URL)],
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
        logging.exception("Subscription check failed")
        return False


async def show_menu(target: Message) -> None:
    await target.answer(
        "<b>REMARENKO P2P</b> 🚀\n\n"
        "Тут ти отримаєш безкоштовний практичний урок про P2P, "
        "дізнаєшся про програму навчання та зможеш підібрати тариф.",
        reply_markup=main_menu(),
    )


@router.message(CommandStart())
async def start(message: Message) -> None:
    payload = "direct"
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 2:
        payload = parts[1][:64]
    save_user(message, payload)
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
async def menu_callback(callback: CallbackQuery) -> None:
    await callback.answer()
    await show_menu(callback.message)


async def lesson_gate(message: Message) -> None:
    if await subscribed(message.bot, message.chat.id):
        update_user(message.chat.id, subscribed=1, stage="lesson_opened")
        await message.answer(
            "🎁 <b>Твій безкоштовний урок уже доступний!</b>\n\n"
            "Переглянь його до кінця — після цього бот допоможе визначити "
            "наступний крок у навчанні.",
            reply_markup=lesson_menu(),
        )
        return
    update_user(message.chat.id, stage="subscription_required")
    await message.answer(
        "Щоб відкрити безкоштовний урок, підпишися на канал "
        "<b>REMARENKO P2P</b>, а потім натисни кнопку перевірки.",
        reply_markup=subscription_menu(),
    )


@router.callback_query(F.data == "lesson")
async def lesson_callback(callback: CallbackQuery) -> None:
    await callback.answer()
    await lesson_gate(callback.message)


@router.callback_query(F.data == "check_subscription")
async def check_subscription(callback: CallbackQuery) -> None:
    if await subscribed(callback.bot, callback.from_user.id):
        update_user(callback.from_user.id, subscribed=1, stage="lesson_opened")
        await callback.answer("Підписку підтверджено ✅")
        await callback.message.answer(
            "✅ <b>Підписку підтверджено.</b>\n\nБезкоштовний урок відкрито:",
            reply_markup=lesson_menu(),
        )
    else:
        await callback.answer("Підписку поки не знайдено", show_alert=True)


@router.callback_query(F.data == "lesson_done")
async def lesson_done(callback: CallbackQuery) -> None:
    update_user(callback.from_user.id, stage="lesson_completed")
    await callback.answer("Урок відмічено як переглянутий ✅")
    await callback.message.answer(
        "🔥 <b>Чудово!</b> Тепер дам тобі кілька коротких запитань і "
        "підкажу, який формат навчання може підійти найбільше.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Підібрати формат →", callback_data="apply")]]
        ),
    )


async def send_program(message: Message) -> None:
    await message.answer(
        "<b>Програма REMARENKO P2P</b>\n\n"
        "🟢 <b>БАЗОВИЙ</b> — 12 модулів, відеоуроки, підтримка ментора, 3 зідзвони.\n\n"
        "🔵 <b>ГРУПОВИЙ</b> — усе з Базового + 7 модулів, чат учнів і 5 зідзвонів.\n\n"
        "🟣 <b>P2P MASTER</b> — усе з Групового + 3 поглиблені модулі та 7 зідзвонів.\n\n"
        "Навчання пояснює систему, розрахунок економіки й безпеку. "
        "Результат залежить від рішень, практики та ринкових умов.",
        reply_markup=back_menu(),
    )


@router.callback_query(F.data == "program")
async def program_callback(callback: CallbackQuery) -> None:
    await callback.answer()
    await send_program(callback.message)


async def send_tariffs(message: Message) -> None:
    await message.answer(
        "<b>Тарифи навчання</b> 💎\n\n"
        "🟢 БАЗОВИЙ — $290\n"
        "🔵 ГРУПОВИЙ — $450\n"
        "🟣 P2P MASTER — $740\n\n"
        "Перед оплатою можна пройти коротке опитування — бот допоможе "
        "підібрати формат під твій досвід і ціль.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📝 Підібрати тариф", callback_data="apply")],
                [InlineKeyboardButton(text="🌐 Детальніше на сайті", url=SITE_URL)],
                [InlineKeyboardButton(text="👨‍💻 Написати Михайлу", url=CONTACT_URL)],
                [InlineKeyboardButton(text="🏠 Головне меню", callback_data="menu")],
            ]
        ),
    )


@router.callback_query(F.data == "tariffs")
async def tariffs_callback(callback: CallbackQuery) -> None:
    await callback.answer()
    await send_tariffs(callback.message)


@router.callback_query(F.data == "apply")
async def apply_start(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(LeadForm.experience)
    update_user(callback.from_user.id, stage="application_started")
    await callback.answer()
    await callback.message.answer(
        "Який у тебе досвід у P2P?",
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
    values = {"new": "Новачок", "first": "Були перші угоди", "active": "Уже працює"}
    value = values.get(callback.data.split(":", 1)[1], "Не вказано")
    await state.update_data(experience=value)
    await state.set_state(LeadForm.capital)
    await callback.answer()
    await callback.message.answer(
        "Який бюджет плануєш використовувати для практики?",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="До $500", callback_data="cap:500")],
                [InlineKeyboardButton(text="$500–2 000", callback_data="cap:2000")],
                [InlineKeyboardButton(text="Понад $2 000", callback_data="cap:more")],
            ]
        ),
    )


@router.callback_query(LeadForm.capital, F.data.startswith("cap:"))
async def apply_capital(callback: CallbackQuery, state: FSMContext) -> None:
    values = {"500": "До $500", "2000": "$500–2 000", "more": "Понад $2 000"}
    value = values.get(callback.data.split(":", 1)[1], "Не вказано")
    await state.update_data(capital=value)
    await state.set_state(LeadForm.goal)
    await callback.answer()
    await callback.message.answer("Яка твоя головна ціль у P2P? Напиши одним повідомленням.")


@router.message(LeadForm.goal)
async def apply_goal(message: Message, state: FSMContext) -> None:
    await state.update_data(goal=(message.text or "")[:500])
    await state.set_state(LeadForm.contact)
    await message.answer(
        "Залиш номер телефону або зручний контакт.\n"
        "Наприклад: <code>+380...</code> або <code>@username</code>."
    )


@router.message(LeadForm.contact)
async def apply_contact(message: Message, state: FSMContext) -> None:
    contact = (message.text or "")[:200]
    data = await state.get_data()
    update_user(
        message.from_user.id,
        stage="application_completed",
        experience=data.get("experience"),
        capital=data.get("capital"),
        goal=data.get("goal"),
        contact=contact,
    )
    username = f"@{message.from_user.username}" if message.from_user.username else "немає"
    admin_text = (
        "🔥 <b>Нова заявка з безкоштовного уроку</b>\n\n"
        f"Ім’я: {message.from_user.full_name}\n"
        f"Telegram: {username}\n"
        f"ID: <code>{message.from_user.id}</code>\n"
        f"Досвід: {data.get('experience', '—')}\n"
        f"Бюджет: {data.get('capital', '—')}\n"
        f"Ціль: {data.get('goal', '—')}\n"
        f"Контакт: {contact}"
    )
    try:
        await message.bot.send_message(ADMIN_CHAT_ID, admin_text)
    except Exception:
        logging.exception("Failed to notify admin")
    await state.clear()
    await message.answer(
        "✅ <b>Заявку прийнято.</b>\n\nМихайло перегляне відповіді та зв’яжеться з тобою.",
        reply_markup=back_menu(),
    )


@router.message(Command("stats"))
async def stats(message: Message) -> None:
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    with connect() as db:
        total = db.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        leads = db.execute("SELECT COUNT(*) FROM users WHERE stage='application_completed'").fetchone()[0]
        lessons = db.execute("SELECT COUNT(*) FROM users WHERE stage IN ('lesson_completed','application_started','application_completed')").fetchone()[0]
    await message.answer(
        "📊 <b>Статистика воронки</b>\n\n"
        f"Користувачів: {total}\n"
        f"Переглянули урок: {lessons}\n"
        f"Залишили заявку: {leads}"
    )


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    connect().close()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=False)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
