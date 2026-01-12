import re
import time
import json
import asyncio
import logging
from datetime import time as dtime, datetime, timedelta
from collections import defaultdict
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

# ================= LOGGING =================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('logs/bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ================= CONFIG =================
TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = 2006042636
MAX_USERS = 20
COLLECT_DURATION = 3600  # 1 giờ
STATE_FILE = "bot_state.json"
TIMEZONE = pytz.timezone("Asia/Ho_Chi_Minh")
USER_COOLDOWN = 30  # giây

TWEET_REGEX = re.compile(
    r"https?:\/\/(x|twitter)\.com\/\w+\/status\/\d+"
)
# ==========================================

# ================= STATE CLASS =============
class BotState:
    def __init__(self):
        self.group_id = None
        self.active = False
        self.start_time = 0
        self.end_time = 0
        self.users = set()
        self.links = []
        self.auto_times = []
        self.jobs = []
        self.pinned_message_id = None
        self.result_message_id = None  # Thêm ID tin nhắn kết quả
        self.last_collect_stats = {
            "timestamp": 0,
            "user_count": 0,
            "link_count": 0
        }
        self.bot_start_time = time.time()
    
    def to_dict(self):
        return {
            "group_id": self.group_id,
            "auto_times": self.auto_times,
            "last_collect_stats": self.last_collect_stats,
            "bot_start_time": self.bot_start_time
        }
    
    def from_dict(self, data):
        self.group_id = data.get("group_id")
        self.auto_times = data.get("auto_times", [])
        self.last_collect_stats = data.get("last_collect_stats", {
            "timestamp": 0,
            "user_count": 0,
            "link_count": 0
        })
        self.bot_start_time = data.get("bot_start_time", time.time())
    
    def reset_collect(self):
        self.users.clear()
        self.links.clear()
    
    def start_collect(self, duration=COLLECT_DURATION):
        self.active = True
        self.start_time = time.time()
        self.end_time = self.start_time + duration
        self.reset_collect()
        logger.info(f"Collect started. End time: {datetime.fromtimestamp(self.end_time).strftime('%H:%M:%S')}")
    
    def stop_collect(self):
        if self.active:
            self.last_collect_stats = {
                "timestamp": time.time(),
                "user_count": len(self.users),
                "link_count": len(self.links)
            }
        self.active = False
    
    def get_remaining_time(self):
        if not self.active:
            return 0
        remaining = max(0, int(self.end_time - time.time()))
        return remaining
    
    def get_progress_percentage(self):
        return min(100, (len(self.users) / MAX_USERS) * 100) if MAX_USERS > 0 else 0
    
    def get_bot_uptime(self):
        return int(time.time() - self.bot_start_time)
    
    def should_finish(self):
        """Check if collect should finish"""
        if not self.active:
            return False
        
        current_time = time.time()
        
        # Check time limit
        if current_time >= self.end_time:
            logger.info("Time limit reached - should finish")
            return True
        
        # Check user limit
        if len(self.users) >= MAX_USERS:
            logger.info(f"User limit reached ({len(self.users)}/{MAX_USERS}) - should finish")
            return True
        
        return False

# ================= GLOBALS =================
session = BotState()
user_cooldown = defaultdict(lambda: datetime.min)
# ==========================================

# ================= STORAGE =================
def save_state():
    try:
        with open(STATE_FILE, "w", encoding='utf-8') as f:
            json.dump(session.to_dict(), f, indent=2)
        logger.debug("State saved successfully")
    except Exception as e:
        logger.error(f"Failed to save state: {e}")

def load_state():
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding='utf-8') as f:
                data = json.load(f)
                session.from_dict(data)
            logger.info("State loaded successfully")
        else:
            logger.info("No state file found, starting fresh")
    except Exception as e:
        logger.error(f"Failed to load state: {e}")
# ==========================================

# ================= HELPERS =================
def is_owner(update: Update) -> bool:
    return update.effective_user.id == OWNER_ID

def is_valid_group(update: Update) -> bool:
    return session.group_id is None or update.effective_chat.id == session.group_id

def create_progress_bar(percentage, length=10):
    filled = int(percentage / 100 * length)
    return "█" * filled + "░" * (length - filled)

def format_time(seconds):
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    
    if hours > 0:
        return f"{hours}h {minutes}m {secs}s"
    elif minutes > 0:
        return f"{minutes}m {secs}s"
    else:
        return f"{secs}s"

def escape_markdown(text: str) -> str:
    """Escape special Markdown characters"""
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    for char in escape_chars:
        text = text.replace(char, f'\\{char}')
    return text
# ==========================================

# ================= BACKGROUND TASK =========
async def background_checker(context: ContextTypes.DEFAULT_TYPE):
    """Background task to check if collect should finish"""
    while True:
        try:
            if session.active and session.should_finish():
                logger.info("Background checker triggered finish_collect")
                await finish_collect(context)
                break
            
            # Check every 10 seconds
            await asyncio.sleep(10)
            
        except Exception as e:
            logger.error(f"Error in background checker: {e}")
            await asyncio.sleep(30)
# ==========================================

# ================= /start ==================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        msg = (
            "🤖 **Bot Collect Link Tweet**\n\n"
            "📌 Thu thập link tweet trong group\n"
            "⏱ Chạy thủ công hoặc tự động theo giờ\n\n"
            "📊 **Lệnh công khai:**\n"
            "/status – xem trạng thái\n"
            "/help – hướng dẫn sử dụng\n"
        )

        if is_owner(update):
            msg += (
                "\n👑 **Lệnh Admin:**\n"
                "/startcollect – bắt đầu collect\n"
                "/stopcollect – dừng collect\n"
                "/autocollect HH:MM – thêm auto collect\n"
                "/autocollect remove HH:MM – xóa auto collect\n"
                "/autocollect off – tắt tất cả auto\n"
                "/stats – thống kê\n"
                "/broadcast – gửi thông báo\n"
                "/export – xuất links\n"
            )

        await update.message.reply_text(msg, parse_mode='Markdown')
        logger.info(f"Start command from {update.effective_user.id}")
    except Exception as e:
        logger.error(f"Error in start command: {e}")

# ================= /help ===================
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = """
📖 **HƯỚNG DẪN SỬ DỤNG**

**1. Gửi link tweet:**
   - Chỉ gửi link tweet hợp lệ: https://twitter.com/user/status/123456789
   - Mỗi người chỉ được gửi 1 link
   - Chờ 30 giây giữa các lần gửi

**2. Lệnh công khai:**
   /status - Xem trạng thái collect hiện tại

**3. Admin commands:**
   Xem /start để biết đầy đủ lệnh admin

📌 **Lưu ý:**
- Bot chỉ hoạt động trong group được set
- Collect tự động kết thúc sau 1 giờ hoặc khi đủ 20 người
- Kết quả sẽ tự động được gửi và ghim sau khi kết thúc
"""
    await update.message.reply_text(help_text)

# ================= CORE START ==============
async def start_collect_core(context: ContextTypes.DEFAULT_TYPE):
    try:
        if session.active:
            logger.warning("Collect already active")
            return
        
        if not session.group_id:
            logger.warning("No group set")
            return
        
        session.start_collect()
        
        end_time_str = datetime.fromtimestamp(session.end_time).strftime('%H:%M:%S')
        
        msg = await context.bot.send_message(
            chat_id=session.group_id,
            text=(
                "🚀 **BẮT ĐẦU COLLECT LINK TWEET**\n\n"
                f"⏱ Thời gian: {COLLECT_DURATION//3600} giờ (kết thúc lúc {end_time_str})\n"
                f"👥 Số người tối đa: {MAX_USERS}\n"
                f"📎 Gửi link tweet hợp lệ!\n"
                f"📊 /status – Xem trạng thái\n"
                f"⏳ Cooldown: {USER_COOLDOWN}s giữa các lần gửi\n\n"
                f"✅ Tự động tổng hợp sau {COLLECT_DURATION//3600}h hoặc khi đủ {MAX_USERS} người"
            ),
            parse_mode='Markdown'
        )
        
        try:
            await context.bot.pin_chat_message(session.group_id, msg.message_id)
            session.pinned_message_id = msg.message_id
            logger.info(f"Message pinned: {msg.message_id}")
        except Exception as e:
            logger.error(f"Failed to pin message: {e}")
            # Notify owner about pin error
            try:
                await context.bot.send_message(
                    OWNER_ID,
                    f"⚠️ Không thể ghim tin nhắn trong group {session.group_id}\n"
                    f"Lỗi: {e}\n"
                    f"Có thể bot cần quyền 'Ghim tin nhắn'."
                )
            except:
                pass
        
        # Start background checker
        asyncio.create_task(background_checker(context))
        
        logger.info(f"Collect started in group {session.group_id}. Will finish at {end_time_str}")
        
    except Exception as e:
        logger.error(f"Error in start_collect_core: {e}")
        # Notify owner about error
        try:
            await context.bot.send_message(
                OWNER_ID,
                f"❌ Lỗi khi bắt đầu collect: {e}"
            )
        except:
            pass

# ================= /startcollect ===========
async def startcollect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return
    
    if session.group_id is None:
        session.group_id = update.effective_chat.id
        save_state()
        logger.info(f"Group set to: {session.group_id}")
    
    if not is_valid_group(update):
        await update.message.reply_text("❌ Bot chỉ hoạt động trong group đã được set")
        return
    
    if session.active:
        await update.message.reply_text("⚠️ Đã có collect đang chạy")
        return
    
    await start_collect_core(context)
    await update.message.reply_text("✅ Collect đã bắt đầu!")

# ================= /stopcollect ============
async def stopcollect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return
    
    if not session.active:
        await update.message.reply_text("⚠️ Không có collect đang chạy.")
        return
    
    # Unpin message trước khi dừng
    if session.pinned_message_id:
        try:
            await context.bot.unpin_chat_message(
                session.group_id,
                session.pinned_message_id
            )
            logger.info(f"Unpinned message: {session.pinned_message_id}")
        except Exception as e:
            logger.error(f"Failed to unpin message: {e}")
    
    session.stop_collect()
    
    await context.bot.send_message(
        session.group_id,
        "⛔ Collect đã bị dừng bởi admin"
    )
    await update.message.reply_text("✅ Collect đã dừng")
    logger.info("Collect stopped by admin")

# ================= /autocollect ============
async def autocollect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return
    
    if session.group_id is None:
        session.group_id = update.effective_chat.id
    
    text = update.message.text or ""
    args = text.split()[1:]
    
    if not args:
        await update.message.reply_text(
            "❌ **Sai cú pháp**\n\n"
            "✅ /autocollect HH:MM\n"
            "🗑 /autocollect remove HH:MM\n"
            "🛑 /autocollect off\n"
            "📋 /autocollect list",
            parse_mode='Markdown'
        )
        return
    
    cmd = args[0]
    
    # ------------------ LIST ------------------
    if cmd == "list":
        if not session.auto_times:
            await update.message.reply_text("📭 Chưa có lịch auto collect nào")
        else:
            times_list = "\n".join([f"• {t}" for t in session.auto_times])
            await update.message.reply_text(
                f"📅 **LỊCH AUTO COLLECT**\n\n{times_list}"
            )
        return
    
    # ------------------ OFF ------------------
    if cmd == "off":
        count = len(session.jobs)
        for job in session.jobs:
            job.schedule_removal()
        session.jobs.clear()
        session.auto_times.clear()
        save_state()
        
        await update.message.reply_text(f"🛑 Đã tắt {count} auto collect")
        logger.info(f"All auto collects disabled: {count} jobs removed")
        return
    
    # ------------------ REMOVE ------------------
    if cmd == "remove" and len(args) == 2:
        time_str = args[1]
        if time_str not in session.auto_times:
            await update.message.reply_text("⚠️ Không tìm thấy giờ này")
            return
        
        try:
            index = session.auto_times.index(time_str)
            if index < len(session.jobs):
                session.jobs[index].schedule_removal()
            session.jobs.pop(index)
            session.auto_times.pop(index)
            save_state()
            
            await update.message.reply_text(f"🗑 Đã xoá auto collect lúc {time_str}")
            logger.info(f"Auto collect removed: {time_str}")
        except Exception as e:
            logger.error(f"Error removing auto collect: {e}")
            await update.message.reply_text("❌ Lỗi khi xoá auto collect")
        return
    
    # ------------------ ADD ------------------
    try:
        hour, minute = map(int, cmd.split(":"))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError
        time_str = f"{hour:02d}:{minute:02d}"
    except:
        await update.message.reply_text("❌ Sai định dạng HH:MM (ví dụ: 08:30)")
        return
    
    if time_str in session.auto_times:
        await update.message.reply_text("⚠️ Giờ này đã tồn tại")
        return
    
    async def auto_collect_job(context: ContextTypes.DEFAULT_TYPE):
        logger.info(f"Auto collect triggered at {time_str}")
        await start_collect_core(context)
    
    try:
        job = context.application.job_queue.run_daily(
            auto_collect_job,
            time=dtime(hour=hour, minute=minute, tzinfo=TIMEZONE)
        )
        
        session.jobs.append(job)
        session.auto_times.append(time_str)
        save_state()
        
        await update.message.reply_text(f"✅ Đã thêm auto collect lúc {time_str}")
        logger.info(f"Auto collect added: {time_str}")
    except Exception as e:
        logger.error(f"Error adding auto collect: {e}")
        await update.message.reply_text("❌ Lỗi khi thêm auto collect")

# ================= COLLECT LINK ============
async def collect_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not session.active:
            return
        if update.effective_chat.id != session.group_id:
            return
        
        user = update.effective_user
        now = datetime.now()
        
        # Check cooldown
        if now - user_cooldown[user.id] < timedelta(seconds=USER_COOLDOWN):
            remaining = USER_COOLDOWN - (now - user_cooldown[user.id]).seconds
            await update.message.reply_text(
                f"⏳ Vui lòng đợi {remaining} giây trước khi gửi link tiếp theo"
            )
            return
        
        text = update.message.text or ""
        if not TWEET_REGEX.search(text):
            return
        
        # Check if user already submitted
        if user.id in session.users:
            await update.message.reply_text("⚠️ Bạn đã gửi link rồi!")
            return
        
        # Add user and link
        session.users.add(user.id)
        user_cooldown[user.id] = now
        
        name = f"@{user.username}" if user.username else user.first_name
        # Escape special characters in name
        escaped_name = escape_markdown(name)
        session.links.append(f"{len(session.links) + 1}. {escaped_name}\n{text}")
        
        # Send confirmation
        progress = session.get_progress_percentage()
        progress_bar = create_progress_bar(progress)
        
        await update.message.reply_text(
            f"✅ **Đã ghi nhận!**\n\n"
            f"👤 Bạn là người thứ {len(session.users)}\n"
            f"📊 Tiến độ: {len(session.users)}/{MAX_USERS}\n"
            f"{progress_bar} {progress:.0f}%",
            parse_mode='Markdown'
        )
        
        logger.info(f"Link collected from user {user.id} ({name}). Total: {len(session.users)}/{MAX_USERS}")
        
        # Check if we reached max users
        if len(session.users) >= MAX_USERS:
            logger.info(f"Max users reached! Triggering finish_collect")
            await finish_collect(context)
        
    except Exception as e:
        logger.error(f"Error in collect_link: {e}")

# ================= FINISH COLLECT ==========
async def finish_collect(context: ContextTypes.DEFAULT_TYPE):
    try:
        if not session.active:
            logger.warning("finish_collect called but collect is not active")
            return
        
        logger.info("=== FINISHING COLLECT ===")
        logger.info(f"Users: {len(session.users)}")
        logger.info(f"Links: {len(session.links)}")
        
        # Unpin message bắt đầu collect trước
        if session.pinned_message_id:
            try:
                await context.bot.unpin_chat_message(
                    session.group_id,
                    session.pinned_message_id
                )
                logger.info(f"Unpinned start message: {session.pinned_message_id}")
            except Exception as e:
                logger.error(f"Failed to unpin start message: {e}")
        
        # Stop collect
        session.stop_collect()
        
        # Prepare summary message
        if session.links:
            links_text = "\n\n".join(session.links)
            
            # Kiểm tra độ dài tin nhắn
            if len(links_text) > 4000:
                # Chia thành nhiều tin nhắn
                summary_part1 = (
                    f"📊 **KẾT QUẢ COLLECT**\n\n"
                    f"👥 Số người tham gia: {len(session.users)}\n"
                    f"📎 Số link thu được: {len(session.links)}\n\n"
                    f"**DANH SÁCH LINK:**\n\n"
                )
                
                # Chia links thành các phần nhỏ
                chunk_size = 10
                chunks = [session.links[i:i + chunk_size] for i in range(0, len(session.links), chunk_size)]
                
                # Gửi phần đầu tiên và pin nó
                first_chunk_text = "\n\n".join(chunks[0])
                result_msg = await send_message_safe(
                    context, 
                    session.group_id, 
                    summary_part1 + first_chunk_text
                )
                
                # Pin tin nhắn kết quả đầu tiên
                if result_msg:
                    try:
                        await context.bot.pin_chat_message(session.group_id, result_msg.message_id)
                        session.result_message_id = result_msg.message_id
                        logger.info(f"Pinned result message: {result_msg.message_id}")
                    except Exception as e:
                        logger.error(f"Failed to pin result message: {e}")
                
                # Gửi các phần tiếp theo
                for i in range(1, len(chunks)):
                    chunk_text = "\n\n".join(chunks[i])
                    await send_message_safe(
                        context,
                        session.group_id,
                        f"**TIẾP THEO...**\n\n{chunk_text}"
                    )
            else:
                # Tin nhắn ngắn, gửi một lần
                summary = (
                    f"📊 **KẾT QUẢ COLLECT**\n\n"
                    f"👥 Số người tham gia: {len(session.users)}\n"
                    f"📎 Số link thu được: {len(session.links)}\n\n"
                    f"**DANH SÁCH LINK:**\n\n"
                    + links_text
                )
                
                # Gửi và pin tin nhắn kết quả
                result_msg = await send_message_safe(context, session.group_id, summary)
                if result_msg:
                    try:
                        await context.bot.pin_chat_message(session.group_id, result_msg.message_id)
                        session.result_message_id = result_msg.message_id
                        logger.info(f"Pinned result message: {result_msg.message_id}")
                    except Exception as e:
                        logger.error(f"Failed to pin result message: {e}")
            
            logger.info(f"Sent summary with {len(session.links)} links")
        else:
            summary = (
                f"📊 **KẾT QUẢ COLLECT**\n\n"
                f"⛔ Không có link nào được gửi\n"
                f"Có thể do:\n"
                f"• Không có link hợp lệ\n"
                f"• Chưa đủ người tham gia\n"
                f"• Thời gian chưa kết thúc"
            )
            
            # Gửi và pin tin nhắn kết quả (kể cả khi không có link)
            result_msg = await send_message_safe(context, session.group_id, summary)
            if result_msg:
                try:
                    await context.bot.pin_chat_message(session.group_id, result_msg.message_id)
                    session.result_message_id = result_msg.message_id
                    logger.info(f"Pinned result message: {result_msg.message_id}")
                except Exception as e:
                    logger.error(f"Failed to pin result message: {e}")
            
            logger.info("No links to send")
        
        # Save stats
        save_state()
        logger.info("=== COLLECT FINISHED ===")
        
    except Exception as e:
        logger.error(f"Error in finish_collect: {e}")
        
        # Try to notify owner
        try:
            await context.bot.send_message(
                OWNER_ID,
                f"❌ Lỗi khi kết thúc collect: {str(e)}"
            )
        except:
            pass

async def send_message_safe(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str, max_retries: int = 3):
    """Send message safely with error handling and return message object"""
    for attempt in range(max_retries):
        try:
            # Try without parse_mode first if there are parse errors
            if attempt > 0:
                # On retry, send as plain text
                msg = await context.bot.send_message(
                    chat_id,
                    text,
                    disable_web_page_preview=True,
                    parse_mode=None  # Plain text
                )
            else:
                # First attempt with Markdown
                msg = await context.bot.send_message(
                    chat_id,
                    text,
                    disable_web_page_preview=True,
                    parse_mode='Markdown'
                )
            
            logger.info(f"Message sent successfully to {chat_id} (attempt {attempt + 1})")
            return msg
            
        except Exception as e:
            logger.warning(f"Failed to send message to {chat_id} (attempt {attempt + 1}): {e}")
            
            if "Can't parse entities" in str(e) and attempt == 0:
                # Markdown parse error, retry with plain text
                logger.info("Markdown parse error, retrying with plain text")
                continue
                
            if attempt < max_retries - 1:
                await asyncio.sleep(2 * (attempt + 1))
            else:
                logger.error(f"Failed to send message after {max_retries} attempts: {e}")
                return None
    
    return None

# ================= /status =================
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        # Only allow in groups
        if update.effective_chat.type not in ["group", "supergroup"]:
            return
        
        # Check if it's the correct group
        if session.group_id is None or update.effective_chat.id != session.group_id:
            return
        
        if session.active:
            remain = session.get_remaining_time()
            progress = session.get_progress_percentage()
            progress_bar = create_progress_bar(progress)
            
            # Calculate end time
            end_time = datetime.fromtimestamp(session.end_time).strftime('%H:%M:%S') if session.end_time > 0 else "N/A"
            
            status_text = (
                f"🚀 **ĐANG COLLECT**\n\n"
                f"⏳ Thời gian còn: {format_time(remain)}\n"
                f"⏰ Kết thúc lúc: {end_time}\n"
                f"👥 Người tham gia: {len(session.users)}/{MAX_USERS}\n"
                f"📎 Số link: {len(session.links)}\n"
                f"{progress_bar} {progress:.0f}%\n\n"
                f"⏰ Cooldown: {USER_COOLDOWN}s\n"
                f"📍 Gửi link tweet để tham gia!"
            )
            
            # Add next auto collect if available
            if session.auto_times:
                status_text += f"\n\n⏰ Auto tiếp theo: {session.auto_times[0]}"
            
        elif session.auto_times:
            times_list = "\n".join([f"• {t}" for t in session.auto_times])
            status_text = (
                f"⏰ **AUTO COLLECT**\n\n"
                f"Lịch hàng ngày:\n{times_list}\n\n"
                f"📊 Lần collect trước:\n"
                f"👥 {session.last_collect_stats.get('user_count', 0)} người\n"
                f"📎 {session.last_collect_stats.get('link_count', 0)} link"
            )
            
        else:
            status_text = (
                "📴 **KHÔNG CÓ COLLECT**\n\n"
                "Bot đang chờ lệnh từ admin\n"
                "Sử dụng /help để xem hướng dẫn"
            )
        
        await update.message.reply_text(status_text, parse_mode='Markdown')
        
    except Exception as e:
        logger.error(f"Error in status command: {e}")

# ================= /stats ==================
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return
    
    try:
        # Get bot uptime from session
        uptime = session.get_bot_uptime()
        
        stats_text = f"""
📊 **THỐNG KÊ BOT**

🆔 **Thông tin cơ bản:**
• Owner ID: {OWNER_ID}
• Group ID: {session.group_id or 'Chưa set'}
• Bot Uptime: {format_time(uptime)}

⚙️ **Cấu hình:**
• Max users: {MAX_USERS}
• Duration: {COLLECT_DURATION//3600} giờ
• Cooldown: {USER_COOLDOWN}s

⏰ **Auto Collect:**
• Số lịch: {len(session.auto_times)}
• Danh sách: {', '.join(session.auto_times) or 'Không có'}

📈 **Lần collect gần nhất:**
• Thời gian: {datetime.fromtimestamp(session.last_collect_stats.get('timestamp', 0)).strftime('%d/%m/%Y %H:%M') if session.last_collect_stats.get('timestamp') else 'N/A'}
• Số người: {session.last_collect_stats.get('user_count', 0)}
• Số link: {session.last_collect_stats.get('link_count', 0)}

🔄 **Trạng thái hiện tại:**
• Đang chạy: {'✅' if session.active else '❌'}
• Số user hiện tại: {len(session.users)}
• Số link hiện tại: {len(session.links)}
"""
        
        await update.message.reply_text(stats_text)
        logger.info(f"Stats requested by {update.effective_user.id}")
        
    except Exception as e:
        logger.error(f"Error in stats command: {e}")
        await update.message.reply_text(f"❌ Lỗi khi lấy thống kê: {e}")

# ================= /broadcast ==============
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return
    
    if session.group_id is None:
        await update.message.reply_text("❌ Chưa có group nào được set")
        return
    
    message = " ".join(context.args)
    if not message:
        await update.message.reply_text("❌ /broadcast <tin nhắn>")
        return
    
    try:
        escaped_message = escape_markdown(message)
        await context.bot.send_message(
            chat_id=session.group_id,
            text=f"📢 **THÔNG BÁO TỪ ADMIN**\n\n{escaped_message}",
            parse_mode='Markdown'
        )
        await update.message.reply_text("✅ Đã gửi broadcast đến group")
        logger.info(f"Broadcast sent: {message[:50]}...")
    except Exception as e:
        logger.error(f"Error in broadcast: {e}")
        await update.message.reply_text(f"❌ Lỗi khi gửi broadcast: {e}")

# ================= /export =================
async def export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_owner(update):
        return
    
    if not session.links:
        await update.message.reply_text("❌ Không có link để export")
        return
    
    try:
        # Create export content
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        content = f"Bot Export - {timestamp}\n"
        content += f"Total links: {len(session.links)}\n"
        content += f"Total users: {len(session.users)}\n"
        content += "=" * 50 + "\n\n"
        content += "\n\n".join(session.links)
        
        # Save to temporary file
        filename = f"export_links_{timestamp}.txt"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)
        
        # Send file
        await update.message.reply_document(
            document=open(filename, "rb"),
            filename=filename,
            caption=f"📁 Export {len(session.links)} links"
        )
        
        # Clean up
        os.remove(filename)
        logger.info(f"Export completed: {len(session.links)} links")
        
    except Exception as e:
        logger.error(f"Error in export: {e}")
        await update.message.reply_text(f"❌ Lỗi khi export: {e}")

# ================= MAIN ====================
def main():
    # Create logs directory if not exists
    if not os.path.exists("logs"):
        os.makedirs("logs")
    
    # Load state
    load_state()
    
    # Update bot start time
    session.bot_start_time = time.time()
    
    # Create application
    app = ApplicationBuilder().token(TOKEN).build()
    
    # Add handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("startcollect", startcollect))
    app.add_handler(CommandHandler("stopcollect", stopcollect))
    app.add_handler(CommandHandler("autocollect", autocollect))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("export", export))
    # Đã xóa lệnh checkperms
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, collect_link))
    
    # Restore auto jobs after restart
    session.jobs = []  # Clear existing jobs
    for time_str in session.auto_times:
        try:
            h, m = map(int, time_str.split(":"))
            
            # Sử dụng closure để giữ giá trị h, m
            def create_job_func(hour, minute):
                async def job_func(context: ContextTypes.DEFAULT_TYPE):
                    logger.info(f"Auto collect triggered (restored): {hour:02d}:{minute:02d}")
                    await start_collect_core(context)
                return job_func
            
            job = app.job_queue.run_daily(
                create_job_func(h, m),
                time=dtime(hour=h, minute=m, tzinfo=TIMEZONE)
            )
            session.jobs.append(job)
            logger.info(f"Restored auto collect: {time_str}")
        except Exception as e:
            logger.error(f"Failed to restore auto collect {time_str}: {e}")
    
    # Start bot
    logger.info("🤖 Bot is starting...")
    print("🤖 Bot is running with enhanced features!")
    print(f"📊 Owner ID: {OWNER_ID}")
    print(f"⏰ Auto times: {session.auto_times}")
    print(f"🏠 Group ID: {session.group_id}")
    print(f"⏱ Collect duration: {COLLECT_DURATION//3600} hours")
    print(f"👥 Max users: {MAX_USERS}")
    print("📝 Check logs/bot.log for details")
    print("\n✨ **Tính năng mới:**")
    print("• Bot tự động unpin tin nhắn bắt đầu collect")
    print("• Bot tự động pin tin nhắn kết quả collect")
    
    # Save initial state
    save_state()
    
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()