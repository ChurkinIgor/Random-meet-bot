import os
from fastapi import FastAPI, Request
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler

TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_PATH = f"/webhook/{TOKEN}"
app = FastAPI()

bot_app = ApplicationBuilder().token(TOKEN).build()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Привет! Это бот Random Meet через webhook.")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📌 Команды: /start /help")

bot_app.add_handler(CommandHandler("start", start))
bot_app.add_handler(CommandHandler("help", help_command))

@app.on_event("startup")
async def on_startup():
    webhook_url = os.getenv("WEBHOOK_URL") + WEBHOOK_PATH
    await bot_app.bot.set_webhook(webhook_url)
    print(f"✅ Webhook установлен: {webhook_url}")

@app.post(WEBHOOK_PATH)
async def handle_webhook(request: Request):
    data = await request.json()
    update = Update.de_json(data, bot_app.bot)
    await bot_app.process_update(update)
    return {"ok": True}
