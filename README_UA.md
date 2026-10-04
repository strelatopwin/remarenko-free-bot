# REMARENKO P2P — бот безкоштовного уроку

Окремий Telegram-бот `@RemarenkoFreeBot`. Він не змінює та не використовує базу `@RemarenkoSupportBot`.

## Що вже працює

- `/start` і головне меню з кнопками;
- збереження Telegram ID, username, джерела переходу та етапу воронки;
- перевірка підписки на Telegram-канал;
- відкриття безкоштовного уроку;
- програма і тарифи;
- коротка анкета;
- надсилання заявки Михайлу;
- команда адміністратора `/stats`;
- SQLite-база на постійному Railway Volume.

## Як розгорнути в Railway

1. Створити **новий окремий Service** у Railway. Не замінювати сервіс бота підтримки.
2. Завантажити ці файли в окремий GitHub-репозиторій або під’єднати папку як новий сервіс.
3. У сервісі відкрити `Variables` і додати змінні з `.env.example`.
4. Створити Volume і встановити Mount Path:

   `/data`

5. Виконати Deploy. У логах має з’явитися запуск polling без помилок.

## Обов’язкові змінні

- `BOT_TOKEN` — токен лише нового `@RemarenkoFreeBot`.
- `ADMIN_CHAT_ID` — Telegram ID Михайла: `788167958`.
- `DATABASE_PATH` — `/data/remarenko_free.sqlite3`.
- `CHANNEL_ID` — username каналу з `@`, наприклад `@remarenko_p2p`.
- `CHANNEL_URL` — повне посилання на канал.
- `LESSON_URL` — посилання на готовий безкоштовний урок.
- `SITE_URL` — сайт із програмою навчання.
- `CONTACT_URL` — `https://t.me/P2P_Marshal`.

Щоб бот міг перевіряти підписку, додайте його адміністратором каналу. Достатньо права бачити учасників; публікувати від імені каналу йому не потрібно.

## Відстеження джерела

Використовуйте різні посилання:

- Instagram: `https://t.me/RemarenkoFreeBot?start=instagram`
- TikTok: `https://t.me/RemarenkoFreeBot?start=tiktok`
- YouTube: `https://t.me/RemarenkoFreeBot?start=youtube`
- сайт: `https://t.me/RemarenkoFreeBot?start=site`

Бот збереже джерело першого переходу користувача.

## Безпека

Не вставляйте токен у код, GitHub, скриншоти або цей архів. Зберігайте його лише в Railway Variables. Якщо токен десь видно, відкличте його через BotFather і відразу замініть значення `BOT_TOKEN` у Railway.
