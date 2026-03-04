import json
import logging
import asyncio
import threading
import time
import os
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

# Third-party libraries
try:
    import requests
    import docker
    from telegram import Update
    from telegram.constants import ParseMode
    from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
except ImportError as e:
    print(f"Error importing dependencies: {e}")
    print("Please run: pip install -r requirements.txt")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger("StorjMonitor")

# Configuration
CONFIG_FILE = 'config.json'

def load_config():
    if not os.path.exists(CONFIG_FILE):
        logger.error(f"Config file {CONFIG_FILE} not found!")
        sys.exit(1)
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        sys.exit(1)

config = load_config()

# Authentication State
auth_attempts = defaultdict(int)
authenticated_users = set()
blocked_users = set()
ALLOWED_USERS_FILE = config['telegram'].get('allowed_users_file', 'allowed_users.json')

def load_allowed_users():
    global authenticated_users
    if os.path.exists(ALLOWED_USERS_FILE):
        try:
            with open(ALLOWED_USERS_FILE, 'r') as f:
                data = json.load(f)
                authenticated_users = set(data.get('users', []))
                logger.info(f"Loaded {len(authenticated_users)} allowed users.")
        except Exception as e:
            logger.error(f"Error loading allowed users: {e}")

def save_allowed_users():
    try:
        with open(ALLOWED_USERS_FILE, 'w') as f:
            json.dump({'users': list(authenticated_users)}, f)
    except Exception as e:
        logger.error(f"Error saving allowed users: {e}")

# Load users on startup
load_allowed_users()

# --- Storj API ---
class StorjAPI:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip('/')

    def get_dashboard(self):
        try:
            response = requests.get(f"{self.base_url}/api/sno/", timeout=5)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Dashboard API error: {e}")
            return None

    def get_satellites(self):
        try:
            # Using dashboard API to get satellites as requested by user
            # The user pointed out that satellites info is inside /api/sno/
            # but usually it's better to fetch from /api/sno/satellites if available.
            # However, user provided output from /api/sno/ showing 'satellites' list inside it.
            # Let's stick to /api/sno/ for the main data as shown in the example.
            response = requests.get(f"{self.base_url}/api/sno/", timeout=5)
            response.raise_for_status()
            data = response.json()
            return data.get('satellites', [])
        except Exception as e:
            logger.error(f"Satellites API error: {e}")
            return None

    def get_estimated_payout(self):
        try:
            response = requests.get(f"{self.base_url}/api/sno/estimated-payout", timeout=5)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Payout API error: {e}")
            return None

    def get_status_report(self):
        dashboard = self.get_dashboard()
        if not dashboard:
            return "❌ **Error**: Could not connect to Storj Node API."

        # Satellites are now part of dashboard response based on user input
        satellites = dashboard.get('satellites', [])
        payout = self.get_estimated_payout()

        node_id = dashboard.get('nodeID', 'Unknown')
        version = dashboard.get('version', 'Unknown')
        up_to_date = dashboard.get('upToDate', False)
        
        # Bandwidth
        bandwidth = dashboard.get('bandwidth', {})
        bw_used = bandwidth.get('used', 0) / 10**9
        
        # Disk
        disk = dashboard.get('diskSpace', {})
        disk_used = disk.get('used', 0) / 10**9
        disk_avail = disk.get('available', 0) / 10**9
        
        # Payout
        current_payout = 0.0
        if payout:
            current_payout = payout.get('currentMonth', {}).get('payout', 0.0)

        report = (
            f"📊 **Storj Node Report**\n\n"
            f"🆔 **Node ID**: `{node_id[:8]}...`\n"
            f"📦 **Version**: {version} {'✅' if up_to_date else '⚠️'}\n"
            f"💾 **Disk**: {disk_used:.2f} GB / {disk_used + disk_avail:.2f} GB\n"
            f"🌐 **Bandwidth**: {bw_used:.2f} GB\n"
            f"💰 **Est. Payout**: ${current_payout:.2f}\n"
        )
        
        if satellites:
            report += "\n🛰 **Satellites**:\n"
            for sat in satellites:
                try:
                    url = sat.get('url', 'Unknown')
                    # Safe parsing of name
                    name = url.split('@')[1].split(':')[0] if '@' in url else sat.get('id', 'Unknown')[:8]
                    
                    # Vetting logic based on vettedAt date
                    vetted_at = sat.get('vettedAt')
                    is_vetted = "✅" if vetted_at else "⏳ (Vetting)"
                    
                    # Note: Audit/Online scores are NOT in the /api/sno/ satellites list shown by user.
                    # They are typically in /api/sno/satellites. 
                    # If the user wants audit scores, we might still need the other endpoint.
                    # But based on the provided JSON, those fields are missing.
                    # We will show what we have: ID/Name and Vetted status.
                    
                    report += f"- **{name}**: {is_vetted}\n"
                except Exception as e:
                    logger.error(f"Error parsing satellite data: {e}")
                    continue
        else:
             report += "\n⚠️ **Satellites**: None found."

        return report

storj_api = StorjAPI(config['storj']['api_url'])

# --- Docker Monitor ---
class DockerLogMonitor(threading.Thread):
    def __init__(self, container_name, loop, app):
        super().__init__()
        self.container_name = container_name
        self.loop = loop
        self.app = app
        self.running = True
        self.daemon = True

    def run(self):
        logger.info(f"Starting Docker monitor for container: {self.container_name}")
        while self.running:
            try:
                client = docker.from_env()
                container = client.containers.get(self.container_name)
                
                # tail=0 to read only new logs
                logs = container.logs(stream=True, follow=True, tail=0)
                
                for line in logs:
                    if not self.running:
                        break
                    try:
                        line_str = line.decode('utf-8').strip()
                        if "ERROR" in line_str or "FATAL" in line_str:
                            logger.info(f"Error detected: {line_str[:50]}...")
                            asyncio.run_coroutine_threadsafe(
                                self.send_alert(line_str), self.loop
                            )
                    except Exception as e:
                        logger.error(f"Log parsing error: {e}")
                        
            except docker.errors.NotFound:
                logger.error(f"Container '{self.container_name}' not found. Retrying in 30s...")
                time.sleep(30)
            except Exception as e:
                logger.error(f"Docker monitor error: {e}. Retrying in 10s...")
                time.sleep(10)

    async def send_alert(self, log_line):
        if not authenticated_users:
            return
            
        # Format the alert
        msg = f"🚨 **Storj Node Error**\n\n```\n{log_line}\n```"
        
        for user_id in authenticated_users:
            try:
                await self.app.bot.send_message(
                    chat_id=user_id, 
                    text=msg, 
                    parse_mode=ParseMode.MARKDOWN
                )
            except Exception as e:
                logger.error(f"Failed to send alert to {user_id}: {e}")

# --- Telegram Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in blocked_users:
        return

    if user_id in authenticated_users:
        await update.message.reply_text("✅ You are logged in. Use /status for report.")
    else:
        await update.message.reply_text("🔒 **Authentication Required**\nPlease enter the password.")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in blocked_users:
        return

    # Check if user is already authenticated
    if user_id in authenticated_users:
        text = update.message.text.lower().strip()
        if text in ['status', '/status']:
            await send_status(update, context)
        else:
            await update.message.reply_text("Unknown command. Try /status.")
        return

    # Password Check
    password = update.message.text.strip()
    if password == config['telegram']['password']:
        authenticated_users.add(user_id)
        save_allowed_users()
        # Reset attempts on success? Usually yes.
        if user_id in auth_attempts:
            del auth_attempts[user_id]
        
        await update.message.reply_text("✅ **Access Granted**\nYou will now receive alerts.\nUse /status to check node.")
        logger.info(f"User {user_id} authenticated.")
    else:
        auth_attempts[user_id] += 1
        attempts = auth_attempts[user_id]
        
        if attempts >= 3:
            blocked_users.add(user_id)
            logger.warning(f"User {user_id} blocked after 3 failed attempts.")
            # Silent block or notify
            await update.message.reply_text("🚫 **Access Denied**\nToo many failed attempts.")
        else:
            remaining = 3 - attempts
            await update.message.reply_text(f"❌ **Wrong Password**\n{remaining} attempts remaining.")

async def send_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in authenticated_users:
        await update.message.reply_text("🔒 Authentication required.")
        return

    status_msg = await update.message.reply_text("🔄 Fetching data...")
    
    # Run API call in executor to avoid blocking asyncio loop
    loop = asyncio.get_running_loop()
    try:
        report = await loop.run_in_executor(None, storj_api.get_status_report)
        await context.bot.edit_message_text(
            chat_id=user_id,
            message_id=status_msg.message_id,
            text=report,
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"Error in status handler: {e}")
        await context.bot.edit_message_text(
            chat_id=user_id,
            message_id=status_msg.message_id,
            text="❌ An error occurred while fetching status."
        )

async def post_init(app: Application):
    """Callback to start background tasks"""
    loop = asyncio.get_running_loop()
    monitor = DockerLogMonitor(config['storj']['container_name'], loop, app)
    monitor.start()
    
    # Keep reference to avoid GC
    app.bot_data['monitor_thread'] = monitor
    logger.info("Bot initialized and monitor started.")

def main():
    token = config['telegram']['bot_token']
    if token == "YOUR_BOT_TOKEN_HERE":
        print("❌ Please configure 'bot_token' in config.json")
        sys.exit(1)

    # Build Application
    application = Application.builder().token(token).post_init(post_init).build()

    # Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("status", send_status))
    # Filter for text messages that are not commands
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Run
    print("🚀 Storj Monitor Bot started...")
    application.run_polling()

if __name__ == '__main__':
    main()
