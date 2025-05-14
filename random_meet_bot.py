import os
import random
import sqlite3
from fastapi import FastAPI, Request
from telegram import Update, Bot
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta
from fastapi.responses import FileResponse

TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
WEBHOOK_PATH = f"/webhook/{TOKEN}"
DB_PATH = "users.db"
app = FastAPI()

bot_app = ApplicationBuilder().token(TOKEN).build()
bot: Bot = bot_app.bot
scheduler = AsyncIOScheduler()

# --- DB SETUP ---
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
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

# --- THEMES ---
TOPICS_FILE = "topics.txt"
if not os.path.exists(TOPICS_FILE):
    with open(TOPICS_FILE, "w") as f:
        f.write("Что вдохновляет тебя в работе?\n")
        f.write("Какой навык хочешь развить?\n")

def load_topics():
    with open(TOPICS_FILE, "r") as f:
        return [line.strip() for line in f if line.strip()]

def save_topic(text):
    with open(TOPICS_FILE, "a") as f:
        f.write(text + "\n")

# --- COMMANDS ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or "anon"
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
    await update.message.reply_text("👋 Добро пожаловать! Встречи — по пятницам. Узнать больше: /help")

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    cursor.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
    conn.commit()
    await update.message.reply_text("🚪 Ты покинул встречный клуб. Возвращайся, когда захочешь!")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📌 Команды:\n/start — присоединиться\n/stop — выйти\n/suggest — предложить тему\n/topics — список тем\n/top — топ участников\n/stats — твоя статистика\n/export — выгрузка CSV (админ)"
    )

async def suggest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = " ".join(context.args)
    if not text:
        return await update.message.reply_text("✏️ Введите тему после /suggest ...")
    save_topic(text)
    cursor.execute("UPDATE stats SET topics = topics + 1 WHERE user_id = ?", (user_id,))
    conn.commit()
    await update.message.reply_text("✅ Тема добавлена!")

async def topics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    topics = load_topics()
    msg = "\n".join(f"• {t}" for t in topics[:20])
    await update.message.reply_text("📚 Темы для встреч:\n" + msg)

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    cursor.execute("SELECT meetings, topics, last_meeting FROM stats WHERE user_id = ?", (user_id,))
    r = cursor.fetchone()
    if r:
        await update.message.reply_text(f"📊 Статистика:\nВстреч: {r[0]}\nТем: {r[1]}\nПоследняя встреча: {r[2] or '-'}")
    else:
        await update.message.reply_text("Нет статистики.")

async def top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cursor.execute("""
    SELECT u.username, s.meetings, s.topics FROM stats s
    JOIN users u ON u.user_id = s.user_id
    ORDER BY s.meetings DESC, s.topics DESC LIMIT 5
    """)
    rows = cursor.fetchall()
    lines = [f"{i+1}. @{r[0] or 'anon'} — {r[1]} встреч, {r[2]} тем" for i, r in enumerate(rows)]
    await update.message.reply_text("🏆 Топ участников:\n\n" + "\n".join(lines))

async def export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin_id = 217092555
    if update.effective_user.id != admin_id:
        return await update.message.reply_text("⛔ Только для администратора.")
    df_path = "/tmp/export.csv"
    import pandas as pd
    df = pd.read_sql_query("SELECT * FROM users LEFT JOIN stats USING(user_id)", conn)
    df.to_csv(df_path, index=False)
    await bot.send_document(chat_id=admin_id, document=open(df_path, "rb"), filename="export.csv")

# --- MATCHING ---
async def match_users():
    cursor.execute("SELECT user_id, username FROM users")
    users = cursor.fetchall()
    random.shuffle(users)
    pairs = [(users[i], users[i+1]) for i in range(0, len(users)-1, 2)]
    if len(users) % 2:
        pairs.append((users[-1], None))
    topic = random.choice(load_topics())
    now = datetime.now()
    for u1, u2 in pairs:
        try:
            if u2:
                msg = f"🤝 Ваша пара на пятницу: @{u2[1]}\n💡 Тема: {topic}"
                await bot.send_message(chat_id=u1[0], text=msg)
                await bot.send_message(chat_id=u2[0], text=f"🤝 Ваша пара: @{u1[1]}\n💡 Тема: {topic}")
                cursor.execute("UPDATE stats SET meetings = meetings + 1, last_meeting = ? WHERE user_id IN (?, ?)", (now, u1[0], u2[0]))
            else:
                await bot.send_message(chat_id=u1[0], text="😔 На этой неделе нет пары. Жди следующей пятницы!")
        except:
            continue
    conn.commit()

# --- Регистрируем команды ---
bot_app.add_handler(CommandHandler("start", start))
bot_app.add_handler(CommandHandler("stop", stop))
bot_app.add_handler(CommandHandler("help", help_command))
bot_app.add_handler(CommandHandler("suggest", suggest))
bot_app.add_handler(CommandHandler("topics", topics))
bot_app.add_handler(CommandHandler("stats", stats))
bot_app.add_handler(CommandHandler("top", top))
bot_app.add_handler(CommandHandler("export", export))

# --- FastAPI ---
@app.on_event("startup")
async def startup():
    await bot.set_webhook(WEBHOOK_URL + WEBHOOK_PATH)
    scheduler.add_job(match_users, "cron", day_of_week="thu", hour=12)
    scheduler.start()
    print("✅ Бот запущен и Webhook установлен")

@app.post(WEBHOOK_PATH)
async def telegram_webhook(req: Request):
    data = await req.json()
    update = Update.de_json(data, bot)
    await bot_app.process_update(update)
    return {"ok": True}
