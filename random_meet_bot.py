import os
import random
import sqlite3
from fastapi import FastAPI, Request
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Bot
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime, timedelta

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
    last_active TIMESTAMP,
    profile TEXT
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
cursor.execute("CREATE TABLE IF NOT EXISTS skip (user_id INTEGER PRIMARY KEY)")

conn.commit()

TOPICS_FILE = "topics.txt"
if not os.path.exists(TOPICS_FILE):
    with open(TOPICS_FILE, "w") as f:
        f.write("Что вдохновляет тебя в работе?\n")

def load_topics():
    with open(TOPICS_FILE, "r") as f:
        return [line.strip() for line in f if line.strip()]

def save_topic(text):
    with open(TOPICS_FILE, "a") as f:
        f.write(text + "\n")

# --- Команды ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    uname = update.effective_user.username or "anon"
    now = datetime.now()
    cursor.execute("""INSERT INTO users (user_id, username, last_active) VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, last_active=excluded.last_active""", (uid, uname, now))
    cursor.execute("INSERT OR IGNORE INTO stats (user_id) VALUES (?)", (uid,))
    conn.commit()

    keyboard = ReplyKeyboardMarkup(
        [["/profile", "/topics"], ["/suggest", "/stats"]], resize_keyboard=True
    )
    inline = InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Пропустить эту неделю", callback_data="skip_week")]])
    await update.message.reply_text(
        "👋 *Добро пожаловать в Random Meet!*\n\nКаждую пятницу ты получаешь:\n• 🤝 Напарника из команды\n• 💬 Тему\n• 📎 Ссылку на Google Meet\n\n✍️ Заполни /profile, чтобы другим было проще тебя узнать.", 
        parse_mode="Markdown", reply_markup=keyboard)
    await update.message.reply_text("Хочешь пропустить ближайшую встречу?", reply_markup=inline)

async def skip_week_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.callback_query.from_user.id
    cursor.execute("INSERT OR IGNORE INTO skip (user_id) VALUES (?)", (uid,))
    conn.commit()
    await update.callback_query.answer("Ты пропустишь эту неделю ✌️")

async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if context.args:
        text = " ".join(context.args)
        cursor.execute("UPDATE users SET profile = ? WHERE user_id = ?", (text, uid))
        conn.commit()
        await update.message.reply_text("✅ Профиль обновлён!")
    else:
        cursor.execute("SELECT profile FROM users WHERE user_id = ?", (uid,))
        row = cursor.fetchone()
        if row and row[0]:
            await update.message.reply_text(f"👤 Твой профиль: {row[0]}")
        else:
            await update.message.reply_text("👤 Профиль пока не заполнен. Пример:\n/profile Я дизайнер, люблю авто и кофе")

async def suggest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = " ".join(context.args)
    if not text:
        return await update.message.reply_text("✏️ Напиши тему после /suggest ...")
    save_topic(text)
    cursor.execute("UPDATE stats SET topics = topics + 1 WHERE user_id = ?", (uid,))
    conn.commit()
    await update.message.reply_text("✅ Тема добавлена!")

async def topics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = "\n".join("• " + t for t in load_topics()[:20])
    await update.message.reply_text("📚 Темы:\n" + msg)

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    cursor.execute("SELECT meetings, topics, last_meeting FROM stats WHERE user_id = ?", (uid,))
    r = cursor.fetchone()
    if r:
        await update.message.reply_text(f"📊 Встреч: {r[0]}\nТем: {r[1]}\nПоследняя: {r[2]}")
    else:
        await update.message.reply_text("Нет статистики.")

async def top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cursor.execute("SELECT u.username, s.meetings, s.topics FROM stats s JOIN users u ON u.user_id = s.user_id ORDER BY s.meetings DESC LIMIT 5")
    rows = cursor.fetchall()
    lines = [f"{i+1}. @{r[0]} — {r[1]} встреч, {r[2]} тем" for i, r in enumerate(rows)]
    await update.message.reply_text("🏆 Топ участников:\n\n" + "\n".join(lines))

async def count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != 217092555:
        return await update.message.reply_text("⛔ Только для администратора.")
    cursor.execute("SELECT COUNT(*) FROM users")
    total = cursor.fetchone()[0]
    await update.message.reply_text(f"👥 Всего участников: {total}")

# --- Встречи ---
async def match_users():
    cursor.execute("SELECT user_id FROM skip")
    skipped = set(u[0] for u in cursor.fetchall())
    cursor.execute("SELECT user_id, username, profile FROM users")
    users = [u for u in cursor.fetchall() if u[0] not in skipped]
    random.shuffle(users)
    pairs = [(users[i], users[i+1]) for i in range(0, len(users)-1, 2)]
    if len(users) % 2: pairs.append((users[-1], None))
    topic = random.choice(load_topics())
    now = datetime.now()
    for u1, u2 in pairs:
        try:
            if u2:
                link = f"https://meet.google.com/lookup/{u1[1]}-{u2[1]}-{random.randint(1000,9999)}"
                msg1 = f"👥 Твоя пара: @{u2[1]}\n💬 Тема: {topic}\n📎 Ссылка: {link}"
                msg2 = f"👥 Твоя пара: @{u1[1]}\n💬 Тема: {topic}\n📎 Ссылка: {link}"
                if u2[2]: msg1 += f"\n👤 О себе: {u2[2]}"
                if u1[2]: msg2 += f"\n👤 О себе: {u1[2]}"
                await bot.send_message(chat_id=u1[0], text=msg1)
                await bot.send_message(chat_id=u2[0], text=msg2)
                cursor.execute("UPDATE stats SET meetings = meetings + 1, last_meeting = ? WHERE user_id IN (?, ?)", (now, u1[0], u2[0]))
            else:
                await bot.send_message(chat_id=u1[0], text="😔 На этой неделе ты без пары.")
        except: continue
    cursor.execute("DELETE FROM skip")
    conn.commit()

# --- Webhook FastAPI ---
@app.on_event("startup")
async def on_start():
    await bot_app.initialize()
    await bot.set_webhook(WEBHOOK_URL + WEBHOOK_PATH)
    scheduler.add_job(match_users, "cron", day_of_week="thu", hour=12)
    scheduler.start()
    print("✅ Webhook установлен и активен")

@app.post(WEBHOOK_PATH)
async def telegram_webhook(req: Request):
    data = await req.json()
    update = Update.de_json(data, bot)
    await bot_app.process_update(update)
    return {"ok": True}

# --- Регистрация команд ---
bot_app.add_handler(CommandHandler("start", start))
bot_app.add_handler(CommandHandler("profile", profile))
bot_app.add_handler(CommandHandler("suggest", suggest))
bot_app.add_handler(CommandHandler("topics", topics))
bot_app.add_handler(CommandHandler("stats", stats))
bot_app.add_handler(CommandHandler("top", top))
bot_app.add_handler(CallbackQueryHandler(skip_week_callback, pattern="^skip_week$"))
bot_app.add_handler(CommandHandler("count", count))
