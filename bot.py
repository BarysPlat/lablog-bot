import os, logging, asyncio, base64
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
from telegram.ext import ApplicationBuilder, CallbackQueryHandler, ContextTypes

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

google_creds_b64 = os.getenv("GOOGLE_CREDS")
if google_creds_b64:
    with open("credentials.json", "wb") as f:
        f.write(base64.b64decode(google_creds_b64))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
SPREADSHEET_ID     = os.getenv("SPREADSHEET_ID")
SHEET_NAME         = os.getenv("SHEET_NAME", "Leads")
CREDS_FILE         = os.getenv("CREDS_FILE", "credentials.json")
POLL_INTERVAL_SEC  = int(os.getenv("POLL_INTERVAL_SEC", "120"))
LEAD_CACHE = {}

EMPLOYEES = {
    "emp_1":  {"name": "Фарангиз (РОП)",            "telegram_id": 7279196775, "emoji": "👩"},
    "emp_2":  {"name": "Камила (Ст. менеджер)",      "telegram_id": 8194515580, "emoji": "👩"},
    "emp_3":  {"name": "Анвар (Менеджер)",           "telegram_id": 7340482923, "emoji": "👨"},
    "emp_4":  {"name": "Динора (Менеджер)",          "telegram_id": 6838703617, "emoji": "👩"},
    "emp_5":  {"name": "Азиз (Менеджер)",            "telegram_id": 6992638274, "emoji": "👨"},
    "emp_6":  {"name": "Шохиста (Менеджер)",         "telegram_id": 8098661552, "emoji": "👩"},
    "emp_7":  {"name": "Фаррух (Маркетолог)",        "telegram_id": 920437340,  "emoji": "👨"},
    "emp_8":  {"name": "Артемий (Ген. директор)",    "telegram_id": 7450966866, "emoji": "👨"},
    "emp_9":  {"name": "Алексей (Исп. директор)",    "telegram_id": 1127489602, "emoji": "👨"},
    "emp_10": {"name": "Анастасия (Куратор)",        "telegram_id": 6880815220, "emoji": "👩"},
    "emp_11": {"name": "Лорета (Куратор)",           "telegram_id": 1985871854, "emoji": "👩"},
    "emp_12": {"name": "Анвар М. (Комм. директор)", "telegram_id": 7687844277, "emoji": "👨"},
    "emp_13": {"name": "Борис (Директор)",           "telegram_id": 6695764184, "emoji": "👨"},
}

URGENCY_LABELS = {
    "hozir": "Сейчас", "bir_necha_oy": "Через месяц", "kelajakda": "В будущем",
    "yaqin_vaqt_ichida": "Скоро", "hozircha_variantlarni_o'rganmoqdaman": "Изучаю варианты",
}
DIRECTION_LABELS = {
    "sut_fermasi": "Молочная ферма", "go'sht_fermasi": "Мясная ферма",
    "go'sht_xo'jaligi": "Мясное хозяйство", "qorakol": "Каракуль",
    "sut_ishlab_chiqarish_va_qayta_ishlash": "Молочное производство",
}
PLATFORM_LABELS = {"ig": "Instagram", "fb": "Facebook"}
COL = {
    "id": "id", "created": "created_time", "ad": "ad_name", "platform": "platform",
    "direction": "sizni_qaysi_yo'nalish_qiziqtiradi?",
    "interest":  "sizni_nima_qiziqtiradi?",
    "urgency":   "bu_siz_uchun_qachon_dolzarb?",
    "name": "full_name", "phone": "phone_number", "email": "email", "status": "lead_status",
}

def get_sheets():
    creds = Credentials.from_service_account_file(
        CREDS_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds).spreadsheets()

def fetch_rows(sheets):
    result = sheets.values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"'{SHEET_NAME}'!A:U"
    ).execute()
    rows = result.get("values", [])
    if len(rows) < 2:
        return []
    headers = rows[0]
    data = []
    for i, r in enumerate(rows[1:]):
        padded = r + [""] * max(0, 21 - len(r))
        d = dict(zip(headers, r + [""] * max(0, len(headers) - len(r))))
        d["__tg_sent__"] = padded[20] if len(padded) > 20 else ""
        d["__row_idx__"] = i
        data.append(d)
    return data

def update_status_only(sheets, row_idx, status, responsible=""):
    sheet_row = row_idx + 2
    sheets.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!S{sheet_row}:T{sheet_row}",
        valueInputOption="RAW",
        body={"values": [[status, responsible]]}
    ).execute()

def mark_sent(sheets, row_idx):
    sheet_row = row_idx + 2
    sheets.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!U{sheet_row}",
        valueInputOption="RAW",
        body={"values": [["SENT_TO_TG"]]}
    ).execute()

def fmt_lead(lead):
    name      = lead.get(COL["name"], "-")
    phone     = lead.get(COL["phone"], "-").replace("p:", "")
    email     = lead.get(COL["email"], "-")
    direction = DIRECTION_LABELS.get(lead.get(COL["direction"], ""), lead.get(COL["direction"], "-"))
    interest  = lead.get(COL["interest"], "-").replace("_", " ")
    urgency   = URGENCY_LABELS.get(lead.get(COL["urgency"], ""), lead.get(COL["urgency"], "-"))
    platform  = PLATFORM_LABELS.get(lead.get(COL["platform"], ""), lead.get(COL["platform"], "-"))
    ad        = lead.get(COL["ad"], "-")[:50]
    created   = lead.get(COL["created"], "")[:19].replace("T", " ") if lead.get(COL["created"]) else "-"
    return (
        f"Новый лид\n---\nИмя: {name}\nТел: {phone}\nEmail: {email}\n---\n"
        f"Направление: {direction}\nИнтерес: {interest}\nСрочность: {urgency}\n---\n"
        f"Источник: {platform}\nОбъявление: {ad}\nСоздан: {created}"
    )

def fmt_personal(lead, by):
    name      = lead.get(COL["name"], "-")
    phone     = lead.get(COL["phone"], "-").replace("p:", "")
    email     = lead.get(COL["email"], "-")
    direction = DIRECTION_LABELS.get(lead.get(COL["direction"], ""), lead.get(COL["direction"], "-"))
    interest  = lead.get(COL["interest"], "-").replace("_", " ")
    urgency   = URGENCY_LABELS.get(lead.get(COL["urgency"], ""), lead.get(COL["urgency"], "-"))
    return (
        f"Вам назначен лид (от {by})\n---\n"
        f"Клиент: {name}\nТел: {phone}\nEmail: {email}\n---\n"
        f"Направление: {direction}\nИнтерес: {interest}\nСрочность: {urgency}\n---\n"
        f"Свяжитесь с клиентом как можно скорее!"
    )

def main_kb(lead_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Беру в работу", callback_data=f"take:{lead_id}"),
         InlineKeyboardButton("Назначить",     callback_data=f"show_staff:{lead_id}")],
        [InlineKeyboardButton("Просмотрено",   callback_data=f"seen:{lead_id}")]
    ])

def staff_kb(lead_id):
    btns = [
        [InlineKeyboardButton(f"{e['emoji']} {e['name']}", callback_data=f"assign:{lead_id}:{eid}")]
        for eid, e in EMPLOYEES.items()
    ]
    btns.append([InlineKeyboardButton("← Назад", callback_data=f"back:{lead_id}")])
    return InlineKeyboardMarkup(btns)

async def get_lead(lead_id):
    if LEAD_CACHE.get(lead_id):
        return LEAD_CACHE[lead_id]
    try:
        sheets = get_sheets()
        rows = fetch_rows(sheets)
        for r in rows:
            if r.get(COL["id"], "").strip() == lead_id:
                LEAD_CACHE[lead_id] = r
                return r
    except Exception as e:
        log.error("Ошибка чтения лида: %s", e)
    return {}

async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data  = query.data
    if data == "noop":
        return
    user  = query.from_user
    uname = f"@{user.username}" if user.username else user.full_name
    parts = data.split(":")
    action  = parts[0]
    lead_id = parts[1] if len(parts) > 1 else ""
    lead_data = await get_lead(lead_id)

    if action == "show_staff":
        await query.edit_message_reply_markup(reply_markup=staff_kb(lead_id))

    elif action == "back":
        await query.edit_message_reply_markup(reply_markup=main_kb(lead_id))

    elif action == "assign" and len(parts) == 3:
        emp = EMPLOYEES.get(parts[2])
        if not emp:
            return
        emp_name = emp["name"]
        emp_tid  = emp["telegram_id"]
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton(f"Назначен: {emp_name} | РОП: {uname}", callback_data="noop")
        ]]))
        try:
            sheets = get_sheets()
            rows = fetch_rows(sheets)
            row_idx = next((r["__row_idx__"] for r in rows if r.get(COL["id"], "").strip() == lead_id), -1)
            if row_idx >= 0:
                update_status_only(sheets, row_idx, "ASSIGNED", f"{emp_name} (назначил {uname})")
        except Exception as e:
            log.error("Ошибка таблицы при назначении: %s", e)
        try:
            await ctx.bot.send_message(chat_id=emp_tid, text=fmt_personal(lead_data, uname))
        except Exception as e:
            log.error("Не удалось отправить %s: %s", emp_name, e)
            await ctx.bot.send_message(chat_id=TELEGRAM_CHAT_ID,
                text=f"Не удалось отправить уведомление {emp_name} — пусть напишет боту /start")
            return
        await ctx.bot.send_message(chat_id=TELEGRAM_CHAT_ID,
            text=f"{uname} назначил лид на {emp_name}\nКлиент: {lead_data.get(COL['name'], '-')}")

    elif action == "take":
        try:
            sheets = get_sheets()
            rows = fetch_rows(sheets)
            row_idx = next((r["__row_idx__"] for r in rows if r.get(COL["id"], "").strip() == lead_id), -1)
            if row_idx >= 0:
                update_status_only(sheets, row_idx, "IN_PROGRESS", uname)
        except Exception as e:
            log.error("Ошибка таблицы: %s", e)
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton(f"Взял в работу: {uname}", callback_data="noop")
        ]]))
        await ctx.bot.send_message(chat_id=TELEGRAM_CHAT_ID,
            text=f"{uname} взял лид в работу\nКлиент: {lead_data.get(COL['name'], '-')}")

    elif action == "seen":
        try:
            sheets = get_sheets()
            rows = fetch_rows(sheets)
            row_idx = next((r["__row_idx__"] for r in rows if r.get(COL["id"], "").strip() == lead_id), -1)
            if row_idx >= 0:
                update_status_only(sheets, row_idx, "SEEN", uname)
        except Exception as e:
            log.error("Ошибка таблицы: %s", e)
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton(f"Просмотрено: {uname}", callback_data="noop")
        ]]))

async def poll_loop(bot: Bot):
    log.info("Поллинг запущен | Лист: %s | Интервал: %d сек.", SHEET_NAME, POLL_INTERVAL_SEC)
    while True:
        try:
            sheets = get_sheets()
            rows   = fetch_rows(sheets)
            for lead in rows:
                lead_id = lead.get(COL["id"], "").strip()
                tg_sent = lead.get("__tg_sent__", "").strip()
                if not lead_id or tg_sent == "SENT_TO_TG":
                    if lead_id:
                        LEAD_CACHE[lead_id] = lead
                    continue
                LEAD_CACHE[lead_id] = lead
                await bot.send_message(
                    chat_id=TELEGRAM_CHAT_ID,
                    text=fmt_lead(lead),
                    reply_markup=main_kb(lead_id)
                )
                mark_sent(sheets, lead["__row_idx__"])
                log.info("Отправлен лид: %s | %s", lead_id, lead.get(COL["name"], "-"))
        except Exception as e:
            log.error("Ошибка поллинга: %s", e)
        await asyncio.sleep(POLL_INTERVAL_SEC)

async def main():
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CallbackQueryHandler(handle_callback))
    async with app:
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        await poll_loop(app.bot)

if __name__ == "__main__":
    asyncio.run(main())
