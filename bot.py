import os, json, logging, asyncio
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import ApplicationBuilder, CallbackQueryHandler, ContextTypes

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = "8116544730:AAE24c3b8VfCY1AlIEf8TeW60h8I9s4-ecM"
TELEGRAM_CHAT_ID   = "-5243518688"
SPREADSHEET_ID     = "1juQWx9PeSyZsTo6tOzCm61_W1X_8n6Wcj1XzUnNdvqM"
SHEET_NAME         = "20042026"
CREDS_FILE         = "credentials.json"

if os.getenv("GOOGLE_CREDS"):
    import base64
    with open("credentials.json","wb") as f:
        f.write(base64.b64decode(os.getenv("GOOGLE_CREDS")))
POLL_INTERVAL_SEC  = 120
SENT_DB            = "sent_leads.json"

EMPLOYEES = {
    "emp_1": {"name": "Фарангиз (РОП)",   "telegram_id": 1, "emoji": "👩‍💼"},
    "emp_2": {"name": "Камила (Менеджер)", "telegram_id": 2, "emoji": "👩‍💼"},
}

URGENCY_LABELS   = {"hozir": "🔥 Сейчас", "bir_necha_oy": "📅 Через месяц", "kelajakda": "⏳ В будущем"}
DIRECTION_LABELS = {"sut_fermasi": "🐄 Молочная ферма", "go'sht_fermasi": "🥩 Мясная ферма", "qorakol": "🐑 Каракуль"}
PLATFORM_LABELS  = {"ig": "Instagram", "fb": "Facebook"}
COL = {"id": "id", "created": "created_time", "ad": "ad_name", "platform": "platform",
       "direction": "sizni_qaysi_yo'nalish_qiziqtiradi?", "interest": "sizni_nima_qiziqtiradi?",
       "urgency": "bu_siz_uchun_qachon_dolzarb?", "name": "full_name",
       "phone": "phone_number", "email": "email", "status": "lead_status"}

def get_sheets_service():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds  = Credentials.from_service_account_file(CREDS_FILE, scopes=scopes)
    return build("sheets", "v4", credentials=creds).spreadsheets()

def fetch_all_rows(sheets):
    result = sheets.values().get(spreadsheetId=SPREADSHEET_ID, range=f"'{SHEET_NAME}'!A:S").execute()
    rows = result.get("values", [])
    if len(rows) < 2:
        return []
    headers = rows[0]
    return [dict(zip(headers, r + [""] * (len(headers) - len(r)))) for r in rows[1:]]

def update_status(sheets, row_index, status, responsible=""):
    sheet_row = row_index + 2
    sheets.values().update(spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!S{sheet_row}:T{sheet_row}",
        valueInputOption="RAW", body={"values": [[status, responsible]]}).execute()

def load_sent():
    if os.path.exists(SENT_DB):
        with open(SENT_DB) as f:
            return json.load(f)
    return {}

def save_sent(db):
    with open(SENT_DB, "w") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

def format_lead_card(lead):
    name      = lead.get(COL["name"], "-")
    phone     = lead.get(COL["phone"], "-").replace("p:", "")
    email     = lead.get(COL["email"], "-")
    direction = DIRECTION_LABELS.get(lead.get(COL["direction"], ""), lead.get(COL["direction"], "-"))
    interest  = lead.get(COL["interest"], "-").replace("_", " ")
    urgency   = URGENCY_LABELS.get(lead.get(COL["urgency"], ""), lead.get(COL["urgency"], "-"))
    platform  = PLATFORM_LABELS.get(lead.get(COL["platform"], ""), lead.get(COL["platform"], "-"))
    ad        = lead.get(COL["ad"], "-")
    created   = lead.get(COL["created"], "")[:19].replace("T", " ") if lead.get(COL["created"]) else "-"
    return (f"Новый лид\n---\nИмя: {name}\nТел: {phone}\nEmail: {email}\n---\n"
            f"Направление: {direction}\nИнтерес: {interest}\nСрочность: {urgency}\n---\n"
            f"Источник: {platform}\nОбъявление: {ad[:50]}\nСоздан: {created}")

def format_personal(lead, assigned_by):
    name    = lead.get(COL["name"], "-")
    phone   = lead.get(COL["phone"], "-").replace("p:", "")
    email   = lead.get(COL["email"], "-")
    direction = DIRECTION_LABELS.get(lead.get(COL["direction"], ""), "-")
    interest  = lead.get(COL["interest"], "-").replace("_", " ")
    urgency   = URGENCY_LABELS.get(lead.get(COL["urgency"], ""), "-")
    return (f"Вам назначен лид (от {assigned_by})\n---\n"
            f"Клиент: {name}\nТел: {phone}\nEmail: {email}\n---\n"
            f"Направление: {direction}\nИнтерес: {interest}\nСрочность: {urgency}\n---\n"
            f"Свяжитесь с клиентом как можно скорее!")

def main_keyboard(lead_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Беру в работу", callback_data=f"take:{lead_id}"),
         InlineKeyboardButton("Назначить", callback_data=f"show_staff:{lead_id}")],
        [InlineKeyboardButton("Просмотрено", callback_data=f"seen:{lead_id}")]
    ])

def staff_keyboard(lead_id):
    buttons = []
    for emp_id, emp in EMPLOYEES.items():
        buttons.append([InlineKeyboardButton(f"{emp['emoji']} {emp['name']}",
                        callback_data=f"assign:{lead_id}:{emp_id}")])
    buttons.append([InlineKeyboardButton("Назад", callback_data=f"back:{lead_id}")])
    return InlineKeyboardMarkup(buttons)

async def handle_callback(update, ctx):
    query    = update.callback_query
    await query.answer()
    data     = query.data
    user     = query.from_user
    username = f"@{user.username}" if user.username else user.full_name
    if data == "noop":
        return
    parts   = data.split(":")
    action  = parts[0]
    lead_id = parts[1] if len(parts) > 1 else ""
    sent_db  = load_sent()
    meta     = sent_db.get(lead_id, {})
    row_idx  = meta.get("row_index", -1)
    lead_data = meta.get("lead_data", {})
    sheets   = get_sheets_service()
    if action == "show_staff":
        await query.edit_message_reply_markup(reply_markup=staff_keyboard(lead_id))
    elif action == "back":
        await query.edit_message_reply_markup(reply_markup=main_keyboard(lead_id))
    elif action == "assign" and len(parts) == 3:
        emp_id = parts[2]
        emp    = EMPLOYEES.get(emp_id)
        if not emp:
            return
        emp_name  = emp["name"]
        emp_tg_id = emp["telegram_id"]
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton(f"Назначен: {emp_name} (РОП: {username})", callback_data="noop")
        ]]))
        if row_idx >= 0:
            update_status(sheets, row_idx, "ASSIGNED", f"{emp_name} (назначил {username})")
        try:
            await ctx.bot.send_message(chat_id=emp_tg_id,
                text=format_personal(lead_data, username))
        except Exception as e:
            log.error("Не удалось отправить %s: %s", emp_name, e)
            await ctx.bot.send_message(chat_id=TELEGRAM_CHAT_ID,
                text=f"Не удалось отправить уведомление {emp_name} — пусть напишет боту /start")
            return
        await ctx.bot.send_message(chat_id=TELEGRAM_CHAT_ID,
            text=f"{username} назначил лид на {emp_name}. Уведомление отправлено.")
        meta["status"] = "ASSIGNED"
        sent_db[lead_id] = meta
        save_sent(sent_db)
    elif action == "take":
        if row_idx >= 0:
            update_status(sheets, row_idx, "IN_PROGRESS", username)
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton(f"Взял в работу: {username}", callback_data="noop")
        ]]))
        await ctx.bot.send_message(chat_id=TELEGRAM_CHAT_ID,
            text=f"{username} взял лид в работу: {lead_data.get(COL['name'], '-')}")
        meta["status"] = "IN_PROGRESS"
        sent_db[lead_id] = meta
        save_sent(sent_db)
    elif action == "seen":
        if row_idx >= 0:
            update_status(sheets, row_idx, "SEEN", username)
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton(f"Просмотрено: {username}", callback_data="noop")
        ]]))

async def poll_loop(bot):
    log.info("Поллинг запущен, интервал %d сек.", POLL_INTERVAL_SEC)
    while True:
        try:
            sheets  = get_sheets_service()
            rows    = fetch_all_rows(sheets)
            sent_db = load_sent()
            changed = False
            for idx, lead in enumerate(rows):
                lead_id = lead.get(COL["id"], "").strip()
                if not lead_id or lead_id in sent_db:
                    continue
                msg = await bot.send_message(chat_id=TELEGRAM_CHAT_ID,
                    text=format_lead_card(lead), reply_markup=main_keyboard(lead_id))
                sent_db[lead_id] = {"row_index": idx, "message_id": msg.message_id,
                    "sent_at": datetime.now().isoformat(), "lead_data": lead}
                changed = True
                log.info("Отправлен лид %s", lead_id)
            if changed:
                save_sent(sent_db)
        except Exception as e:
            log.error("Ошибка поллинга: %s", e)
        await asyncio.sleep(POLL_INTERVAL_SEC)

async def main():
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CallbackQueryHandler(handle_callback))
    async with app:
        await app.start()
        await app.updater.start_polling()
        await poll_loop(app.bot)

if __name__ == "__main__":
    asyncio.run(main())
