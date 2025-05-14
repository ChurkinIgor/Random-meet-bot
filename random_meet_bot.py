import logging
import random
import sqlite3
import os
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta

# Включаем логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Темы для встреч
TOPICS_FILE = "topics.txt"
SUGGEST_LOG = "suggest_log.txt"

# Загрузка и запись тем
def load_topics():
    if not os.path.exists(TOPICS_FILE):
        with open(TOPICS_FILE, "w") as f:
            f.write("Что вдохновляет тебя в работе?\n")
            f.write("Какой проект ты бы хотел реализовать, но пока не начал?\n")
    with open(TOPICS_FILE, "r") as f:
        return [line.strip() for line in f if line.strip()]

def add_topic(new_topic: str):
    with open(TOPICS_FILE, "a") as f:
        f.write(f"{new_topic}\n")

MEETING_TOPICS = load_topics()

# Проверка времени последней отправки темы пользователем
def can_suggest(user_id):
    now = datetime.now()
    if not os.path.exists(SUGGEST_LOG):
        return True
    with open(SUGGEST_LOG, "r") as f:
        for line in f:
            uid, ts = line.strip().split("::")
            if int(uid) == user_id:
                last = datetime.fromisoformat(ts)
                return (now - last).days >= 7
    return True

def log_suggestion(user_id):
    now = datetime.now().isoformat()
    with open(SUGGEST_LOG, "a") as f:
        f.write(f"{user_id}::{now}\n")
    cursor.execute("INSERT INTO stats (user_id, topics) VALUES (?, 1) ON CONFLICT(user_id) DO UPDATE SET topics = topics + 1", (user_id,))
    conn.commit()

# База данных SQLite
conn = sqlite3.connect("users.db", check_same_thread=False)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    last_active TIMESTAMP
)
""")
cursor.execute("""
CREATE TABLE IF NOT EXISTS stats (
    user_id INTEGER PRIMARY KEY,
    meetings INTEGER DEFAULT 0,
    topics INTEGER DEFAULT 0,
    last_meeting TIMESTAMP
)
""")
conn.commit()

# ID администратора
ADMIN_ID = 217092555

# Регистрируем пользователя
def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "anonymous"
    now = datetime.now()

    cursor.execute("""
    INSERT INTO users (user_id, username, last_active)
    VALUES (?, ?, ?)
    ON CONFLICT(user_id) DO UPDATE SET
        username=excluded.username,
        last_active=excluded.last_active
    """, (user_id, username, now))
    cursor.execute("INSERT OR IGNORE INTO stats (user_id) VALUES (?)", (user_id,))
    conn.commit()

    return update.message.reply_text(
        "👋 Добро пожаловать! Это Telegram-бот, который поможет тебе познакомиться с другими участниками команды.\n"
        "Каждый четверг ты будешь получать приглашение на пятничную встречу для общения.\n\n"
        "Если хочешь узнать больше — напиши /help."
    )

# Отписка пользователя
def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    cursor.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
    conn.commit()
    return update.message.reply_text("Ты отписался от рандомных встреч. Возвращайся, когда захочешь!")

# Справка для участников
def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "🤖 *Как работает бот Random Meet:*\n\n"
        "• Каждый четверг ты получаешь сообщение с напарником и ссылкой на встречу.\n"
        "• Встреча проходит в 12:00 пятницу  — можно обсудить работу, идеи или просто пообщаться.\n"
        "• Команды:\n"
        "   /start — зарегистрироваться\n"
        "   /stop — отписаться\n"
        "   /help — эта справка\n"
        "   /suggest [тема] — предложить тему (раз в 7 дней)\n"
        "   /topics — посмотреть все доступные темы\n"
        "   /stats — посмотреть свою статистику\n"
        "\n*Бонус:* если кто-то не активен 30+ дней, бот его автоматически удаляет."
    )
    return update.message.reply_text(help_text, parse_mode="Markdown")

# Предложение новой темы пользователем
def suggest_topic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not context.args:
        return update.message.reply_text("✍️ Напиши тему после команды. Пример: /suggest Какой город тебе хочется посетить?")

    if not can_suggest(user_id):
        return update.message.reply_text("⚠️ Ты уже предлагал тему недавно. Попробуй снова через несколько дней.")

    topic = " ".join(context.args).strip()
    add_topic(topic)
    MEETING_TOPICS.append(topic)
    log_suggestion(user_id)
    return update.message.reply_text("✅ Спасибо! Твоя тема добавлена в список.")

# Просмотр всех тем
def list_topics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not MEETING_TOPICS:
        return update.message.reply_text("Пока нет доступных тем.")
    return update.message.reply_text("📋 Список тем:\n\n" + "\n".join(f"• {t}" for t in MEETING_TOPICS))

# Статистика участника
def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    cursor.execute("SELECT meetings, topics, last_meeting FROM stats WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    if not row:
        return update.message.reply_text("Ты ещё не участвовал во встречах и не предлагал тем.")
    meetings, topics, last = row
    last_str = last if last else "—"
    return update.message.reply_text(
        f"📊 Твоя статистика:\n\n"
        f"• Встреч: {meetings}\n"
        f"• Тем предложено: {topics}\n"
        f"• Последняя встреча: {last_str}"
    )

# Топ активных участников
def top_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cursor.execute("""
        SELECT u.username, s.meetings, s.topics
        FROM stats s
        JOIN users u ON u.user_id = s.user_id
        ORDER BY s.meetings DESC, s.topics DESC
        LIMIT 5
    """)
    rows = cursor.fetchall()
    if not rows:
        return update.message.reply_text("Пока нет статистики.")
    
    text = "🏆 Топ участников по активности:\n\n"
    for i, (username, meet, top) in enumerate(rows, start=1):
        name = f"@{username}" if username else f"ID {i}"
        text += f"{i}. {name} — {meet} встреч, {top} тем\n"
    return update.message.reply_text(text)
# Список всех участников (только для администратора)
def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        return update.message.reply_text("У тебя нет прав на выполнение этой команды.")

    cursor.execute("SELECT user_id, username, last_active FROM users")
    rows = cursor.fetchall()

    if not rows:
        return update.message.reply_text("Нет зарегистрированных участников.")

    message = "\n".join([f"ID: {uid}, Username: @{uname}, Last Active: {active}" for uid, uname, active in rows])
    return update.message.reply_text(f"👥 Список участников:\n{message}")

# Очистка неактивных участников (30+ дней)
def cleanup_inactive():
    threshold = datetime.now() - timedelta(days=30)
    cursor.execute("DELETE FROM users WHERE last_active < ?", (threshold,))
    conn.commit()
    logger.info("Очистка неактивных участников выполнена")

# Генерация персональной ссылки на Google Meet
def generate_google_meet_link(username1, username2):
    base1 = username1 or "anon"
    base2 = username2 or "anon"
    suffix = f"{base1}-{base2}-{random.randint(1000, 9999)}"
    return f"https://meet.google.com/lookup/{suffix}"

# Выбор случайной темы для встречи
def get_random_topic():
    return random.choice(MEETING_TOPICS)

# Создание пар и рассылка сообщений
async def send_meeting_pairs(application):
    threshold = datetime.now() - timedelta(days=14)
    cursor.execute("SELECT user_id, username FROM users WHERE last_active >= ?", (threshold,))
    user_data = cursor.fetchall()
    random.shuffle(user_data)

    if len(user_data) < 2:
        logger.warning("Недостаточно участников для встречи.")
        return

    pairs = [(user_data[i], user_data[i + 1]) for i in range(0, len(user_data) - 1, 2)]

    if len(user_data) % 2 == 1:
        pairs.append((user_data[-1], None))

    for u1, u2 in pairs:
        u1_id, u1_name = u1
        if u2:
            u2_id, u2_name = u2
            link = generate_google_meet_link(u1_name, u2_name)
            topic = get_random_topic()
            now = datetime.now()
            try:
                await application.bot.send_message(chat_id=u1_id, text=f"🤝 Твоя встреча на пятницу: @{u2_name}\nСсылка: {link}\n\n💡 Тема для разговора: *{topic}*", parse_mode="Markdown")
                await application.bot.send_message(chat_id=u2_id, text=f"🤝 Твоя встреча на пятницу: @{u1_name}\nСсылка: {link}\n\n💡 Тема для разговора: *{topic}*", parse_mode="Markdown")
                cursor.execute("UPDATE stats SET meetings = meetings + 1, last_meeting = ? WHERE user_id IN (?, ?)", (now, u1_id, u2_id))
                conn.commit()
            except Exception as e:
                logger.error(f"Ошибка при отправке: {e}")
        else:
            try:
                await application.bot.send_message(chat_id=u1_id, text="😔 К сожалению, на этой неделе не нашлось пары. Жди следующую пятницу!")
            except Exception as e:
                logger.error(f"Ошибка при отправке одиночному участнику: {e}")

# Планировщик задач
scheduler = BackgroundScheduler()
scheduler.add_job(lambda: cleanup_inactive(), 'cron', hour=3)  # каждый день в 03:00 по UTC

async def scheduled_job():
    await send_meeting_pairs(app)

scheduler.add_job(scheduled_job, 'cron', day_of_week='thu', hour=12, minute=0)
scheduler.start()

# Запуск бота
app = ApplicationBuilder().token(os.getenv("BOT_TOKEN")).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("stop", stop))
app.add_handler(CommandHandler("list", list_users))
app.add_handler(CommandHandler("help", help_command))
app.add_handler(CommandHandler("suggest", suggest_topic))
app.add_handler(CommandHandler("topics", list_topics))
app.add_handler(CommandHandler("stats", stats_command))
app.add_handler(CommandHandler("top", top_command))

logger.info("Бот запущен")
app.run_polling()
