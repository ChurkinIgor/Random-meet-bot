import logging
import random
import sqlite3
import os
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta

# Включаем логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    conn.commit()

    return update.message.reply_text("👋 Добро пожаловать! Это Telegram-бот, который поможет тебе познакомиться с другими участниками команды. Каждый четверг ты будешь получать приглашение на пятничную встречу для общения.")

# Отписка пользователя
def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    cursor.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
    conn.commit()
    return update.message.reply_text("Ты отписался от рандомных встреч. Возвращайся, когда захочешь!")

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

# Генерация персональной ссылки на Google Meet
def generate_google_meet_link(username1, username2):
    base1 = username1 or "anon"
    base2 = username2 or "anon"
    suffix = f"{base1}-{base2}-{random.randint(1000, 9999)}"
    return f"https://meet.google.com/lookup/{suffix}"

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
            try:
                await application.bot.send_message(chat_id=u1_id, text=f"🤝 Твоя встреча на пятницу: @{u2_name}\nСсылка: {link}")
                await application.bot.send_message(chat_id=u2_id, text=f"🤝 Твоя встреча на пятницу: @{u1_name}\nСсылка: {link}")
            except Exception as e:
                logger.error(f"Ошибка при отправке: {e}")
        else:
            try:
                await application.bot.send_message(chat_id=u1_id, text="😔 К сожалению, на этой неделе не нашлось пары. Жди следующую пятницу!")
            except Exception as e:
                logger.error(f"Ошибка при отправке одиночному участнику: {e}")

# Планировщик задач
scheduler = BackgroundScheduler()

async def scheduled_job():
    await send_meeting_pairs(app)

# Запуск бота
app = ApplicationBuilder().token(os.getenv("BOT_TOKEN")).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("stop", stop))
app.add_handler(CommandHandler("list", list_users))

scheduler.add_job(scheduled_job, 'cron', day_of_week='thu', hour=12, minute=0)
scheduler.start()

logger.info("Бот запущен")
app.run_polling()