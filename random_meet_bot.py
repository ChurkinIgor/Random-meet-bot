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
    uid = update.effective_user.id
    uname = update.effective_user.username or "anon"
    now = datetime.now()
    cursor.execute("""
        INSERT INTO users (user_id, username, last_active)
        VALUES (?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, last_active=excluded.last_active
    """, (uid, uname, now))
    cursor.execute("INSERT OR IGNORE INTO stats (user_id) VALUES (?)", (uid,))
    conn.commit()
    await update.message.reply_text(
        "👋 Привет! Ты подключён к *рандомным пятничным встречам*.

"
        "Каждый четверг в 12:00 (UTC) ты будешь получать сообщение с:
"
        "• твоим напарником из команды
"
        "• темой для разговора
"
        "• ссылкой на Google Meet для встречи

"
        "Это отличный способ познакомиться ближе с командой, обсудить идеи и просто пообщаться.

"
        "📌 Используй /help, чтобы узнать все доступные команды.",
        parse_mode="Markdown"
    )

async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    cursor.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
    conn.commit()
    await update.message.reply_text("🚪 Ты покинул встречный клуб. Возвращайся, когда захочешь!")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 *Random Meet Bot* — бот для рандомных встреч внутри команды.

"
        "🗓 *Что делает:*
"
        "• Каждую пятницу — новая пара участников
"
        "• Темы для разговора генерируются случайно
"
        "• Статистика по участникам и предложенным темам

"
        "📌 *Команды:*
"
        "/start — подключиться к встречам
"
        "/stop — отписаться
"
        "/suggest [тема] — предложить тему
"
        "/topics — посмотреть список тем
"
        "/top — топ участников
"
        "/stats — твоя статистика
"
        "/export — экспорт CSV (только для админа)

"
        "Желаем классных и полезных встреч!",
        parse_mode="Markdown"
    )"
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

async def faq(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "❓ *FAQ — часто задаваемые вопросы*

"
        "• *Что за встречи?*
Каждую пятницу бот рандомно объединяет участников в пары (или тройки), чтобы познакомиться, пообщаться и обменяться опытом.

"
        "• *Где проходит встреча?*
Через Google Meet — бот пришлёт персональную ссылку.

"
        "• *О чём говорить?*
Бот предлагает тему, но можно говорить на любую. Главное — живой диалог.

"
        "• *Что если я занят?*
Просто пропусти встречу. Или используй /stop, чтобы временно отключиться.

"
        "• *Зачем это всё?*
Чтобы команда лучше узнавала друг друга, делилась опытом и развивала доверие.",
        parse_mode="Markdown"
    )

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
            await update.message.reply_text(f"👤 Твой профиль: {row[0]}

Измени его так: /profile Я дизайнер, люблю велопоходы")
        else:
            await update.message.reply_text("👤 У тебя пока нет профиля. Добавь его так:
/profile Я дизайнер, люблю велопоходы")

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
                # Google Meet link
                link = f"https://meet.google.com/lookup/{u1[1]}-{u2[1]}-{random.randint(1000,9999)}"
                cursor.execute("SELECT profile FROM users WHERE user_id = ?", (u2[0],))
                p2 = cursor.fetchone()
                ptext = f"
👤 О себе: {p2[0]}" if p2 and p2[0] else ""
                await bot.send_message(chat_id=u1[0], text=f"👥 Твоя пара: @{u2[1]}
💬 Тема: {topic}
📎 Ссылка: {link}{ptext}")
                cursor.execute("SELECT profile FROM users WHERE user_id = ?", (u1[0],))
                p1 = cursor.fetchone()
                ptext2 = f"
👤 О себе: {p1[0]}" if p1 and p1[0] else ""
                await bot.send_message(chat_id=u2[0], text=f"👥 Твоя пара: @{u1[1]}
💬 Тема: {topic}
📎 Ссылка: {link}{ptext2}")
                cursor.execute("UPDATE stats SET meetings = meetings + 1, last_meeting = ? WHERE user_id IN (?, ?)", (now, u1[0], u2[0]))
            else:
                await bot.send_message(chat_id=u1[0], text="😔 На этой неделе ты без пары. Жди следующей пятницы!")
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
bot_app.add_handler(CommandHandler("faq", faq))
bot_app.add_handler(CommandHandler("profile", profile))
bot_app.add_handler(CommandHandler("faq", faq))

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
