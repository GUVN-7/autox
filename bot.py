import re
import time
import json
import asyncio
from datetime import time as dtime
import pytz
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    MessageHandler,
    filters,
)
import os

TOKEN = os.getenv("BOT_TOKEN")

# ================= CONFIG =================
OWNER_ID = 2006042636
MAX_USERS = 20
COLLECT_DURATION = 3600  # 1 giờ
STATE_FILE = "bot_state.json"
TIMEZONE = pytz.timezone("Asia/Ho_Chi_Minh")

TWEET_REGEX = re.compile(
    r"https?:\/\/(x|twitter)\.com\/\w+\/status\/\d+"
)
# ==========================================

# ================= STATE ===================
session = {
    "group_id": None,
    "active": False,
    "start_time": 0,
    "end_time": 0,
    "users": set(),
    "links": [],
    "auto_times": [],  # ["08:00", "20:00"]
    "jobs": [],        # job_queue objects (không lưu file)
    "pinned_message_id": None
}
# ==========================================

# ================= STORAGE =================
def save_state():
    tmp = {
        "group_id": session.get("group_id"),
        "auto_times": session.get("auto_times", [])
    }
    with open(STATE_FILE, "w") as f:
        json.dump(tmp, f)


def load_state():
    try:
        with open(STATE_FILE, "r") as f:
            data = json.load(f)
            session["group_id"] = data.get("group_id")
            session["auto_times"] = data.get("auto_times", [])
    except:
        pass
# ==========================================

# ================= HELPERS =================
def is_owner(update: Update) -> bool:
    return update.effective_user.id == OWNER_ID


def is_valid_group(update: Update) -> bool:
    return session["group_id"] is None or update.effective_chat.id == session["group_id"]
# ==========================================

# ================= /start ==================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "🤖 **Bot Collect Link Tweet**\n\n"
        "📌 Thu thập link tweet trong group\n"
        "⏱ Chạy thủ công hoặc tự động theo giờ\n\n"
        "📊 /status – xem trạng thái\n"
    )

    if is_owner(update):
        msg += (
            "\n👑 **Admin:**\n"
            "/startcollect\n"
            "/stopcollect\n"
            "/autocollect HH:MM\n"
            "/autocollect remove HH:MM\n"
            "/autocollect off\n"
        )

    await update.message.reply_text(msg)

# ================= CORE START ==============
async def start_collect_core(context: ContextTypes.DEFAULT_TYPE):
    if session["active"] or not session["group_id"]:
        return

    now = time.time()
    session["active"] = True
    session["start_time"] = now
    session["end_time"] = now + COLLECT_DURATION
    session["users"].clear()
    session["links"].clear()

    msg = await context.bot.send_message(
        chat_id=session["group_id"],
        text=(
            "🚀 **BẮT ĐẦU COLLECT LINK TWEET**\n\n"
            "⏱ 1 giờ | 👥 20 người\n"
            "📎 Gửi link tweet hợp lệ!"
        )
    )

    try:
        await context.bot.pin_chat_message(session["group_id"], msg.message_id)
        session["pinned_message_id"] = msg.message_id
    except:
        pass

    asyncio.create_task(auto_finish(context))

# ================= /startcollect ===========
async def startcollect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return

    if session["group_id"] is None:
        session["group_id"] = update.effective_chat.id
        save_state()

    if not is_valid_group(update):
        return

    await start_collect_core(context)

# ================= /stopcollect ============
async def stopcollect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return

    if not session["active"]:
        await update.message.reply_text("⚠️ Không có collect đang chạy.")
        return

    session["active"] = False

    if session["pinned_message_id"]:
        try:
            await context.bot.unpin_chat_message(
                session["group_id"],
                session["pinned_message_id"]
            )
        except:
            pass

    await context.bot.send_message(
        session["group_id"],
        "⛔ Collect đã bị dừng bởi admin"
    )

# ================= /autocollect ============
async def autocollect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print("DEBUG: /autocollect received", update.effective_user.id)
    if not is_owner(update):
        print("DEBUG: Not owner")
        return

    if session["group_id"] is None:
        session["group_id"] = update.effective_chat.id

    text = update.message.text or ""
    args = text.split()[1:]  # bỏ "/autocollect"

    if not args:
        await update.message.reply_text("❌ /autocollect HH:MM | remove HH:MM | off")
        return

    cmd = args[0]

    # ------------------ OFF ------------------
    if cmd == "off":
        for job in session["jobs"]:
            job.schedule_removal()
        session["jobs"].clear()
        session["auto_times"].clear()
        save_state()
        await update.message.reply_text("🛑 Đã tắt toàn bộ auto collect")
        return

    # ------------------ REMOVE ------------------
    if cmd == "remove" and len(args) == 2:
        time_str = args[1]
        if time_str not in session["auto_times"]:
            await update.message.reply_text("⚠️ Không tìm thấy giờ này")
            return

        index = session["auto_times"].index(time_str)
        session["jobs"][index].schedule_removal()
        session["jobs"].pop(index)
        session["auto_times"].pop(index)
        save_state()

        await update.message.reply_text(f"🗑 Đã xoá auto collect lúc {time_str}")
        return

    # ------------------ ADD ------------------
    try:
        hour, minute = map(int, cmd.split(":"))
        time_str = f"{hour:02d}:{minute:02d}"
    except:
        await update.message.reply_text("❌ Sai định dạng HH:MM")
        return

    if time_str in session["auto_times"]:
        await update.message.reply_text("⚠️ Giờ này đã tồn tại")
        return

    async def auto_collect_job(context: ContextTypes.DEFAULT_TYPE):
        await start_collect_core(context)

    job = context.application.job_queue.run_daily(
        auto_collect_job,
        time=dtime(hour=hour, minute=minute, tzinfo=TIMEZONE)
    )

    session["jobs"].append(job)
    session["auto_times"].append(time_str)
    save_state()

    await update.message.reply_text(f"✅ Đã thêm auto collect lúc {time_str}")

# ================= AUTO FINISH =============
async def auto_finish(context: ContextTypes.DEFAULT_TYPE):
    while session["active"]:
        if time.time() >= session["end_time"] or len(session["users"]) >= MAX_USERS:
            await finish_collect(context)
            break
        await asyncio.sleep(5)

# ================= COLLECT LINK ============
async def collect_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not session["active"]:
        return
    if update.effective_chat.id != session["group_id"]:
        return

    text = update.message.text or ""
    if not TWEET_REGEX.search(text):
        return

    user = update.effective_user
    if user.id in session["users"]:
        return

    session["users"].add(user.id)
    name = f"@{user.username}" if user.username else user.first_name

    session["links"].append(f"{len(session['links']) + 1}. {name}\n{text}")

    await update.message.reply_text(
        f"✅ Ghi nhận ({len(session['users'])}/{MAX_USERS})"
    )

# ================= FINISH ==================
async def finish_collect(context: ContextTypes.DEFAULT_TYPE):
    if not session["active"]:
        return

    session["active"] = False

    if session["pinned_message_id"]:
        try:
            await context.bot.unpin_chat_message(
                session["group_id"],
                session["pinned_message_id"]
            )
        except:
            pass

    msg = (
        "📊 **TỔNG HỢP LINK TWEET**\n\n"
        + ("\n\n".join(session["links"]) if session["links"] else "⛔ Không có link.")
    )

    await context.bot.send_message(
        session["group_id"],
        msg,
        disable_web_page_preview=True
    )

# ================= /status =================
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Chỉ cho phép lệnh trong group đã lưu
    if update.effective_chat.type not in ["group", "supergroup"]:
        return  # PM hoặc private chat => không phản hồi

    if session["group_id"] is None or update.effective_chat.id != session["group_id"]:
        return  # không phải group đang collect => không phản hồi

    if session["active"]:
        remain = int(session["end_time"] - time.time())
        await update.message.reply_text(
            f"📊 Đang collect\n"
            f"👥 {len(session['users'])}/{MAX_USERS}\n"
            f"⏱ {remain//60}m {remain%60}s"
        )
    elif session["auto_times"]:
        await update.message.reply_text(
            f"⏰ Auto collect mỗi ngày lúc: {', '.join(session['auto_times'])}"
        )
    else:
        await update.message.reply_text("📴 Không có collect.")

# ================= MAIN ====================
def main():
    load_state()
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("startcollect", startcollect))
    app.add_handler(CommandHandler("stopcollect", stopcollect))
    app.add_handler(CommandHandler("autocollect", autocollect))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, collect_link))

    # Restore auto jobs after restart
    for t in session.get("auto_times", []):
        h, m = map(int, t.split(":"))
        async def job_func(ctx, _h=h, _m=m):
            await start_collect_core(ctx)
        job = app.job_queue.run_daily(
            job_func,
            time=dtime(hour=h, minute=m, tzinfo=TIMEZONE)
        )
        session["jobs"].append(job)

    print("🤖 Bot running | multi auto collect + remove")
    app.run_polling()


if __name__ == "__main__":
    main()
