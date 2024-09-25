# 1. Imports and Configuration
# ============================
# Standard library imports
import asyncio
import contextlib
import gzip
import html
import io
import json
import logging
import os
import re
import signal
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
import time
import traceback
import typing
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timezone, timedelta
from datetime import time as datetime_time
from urllib.parse import urlparse, parse_qs

# Third-party library imports
import aiofiles
import aiohttp
import asyncpraw
import asyncprawcore
import backoff
import discord
from discord.ext import commands, tasks
from discord import app_commands
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env_reddit')

# Create logs directory if it doesn't exist
if not os.path.exists('logs'):
    os.makedirs('logs')

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# 2. Global Variables and Constants
# =================================

# Configuration
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = 'Reddit2Discord Bot/v1.3 by Trai60'
DISCORD_BOT_TOKEN = os.getenv('REDDIT_BOT_TOKEN')
DEBUG_ROLE_ID = int(os.getenv('DEBUG_ROLE_ID'))
LOG_CHANNEL_ID = int(os.getenv('LOG_CHANNEL_ID'))
MAX_VIDEO_SIZE = 24 * 1024 * 1024  # 24MB in bytes
COMMAND_CACHE_FILE = 'command_cache.json'

processed_submissions = {}

# 3. Bot Initialization
# =====================
# Initialize Discord client
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)
debug_role = discord.Object(id=DEBUG_ROLE_ID)
bot.tree.default_permissions = discord.Permissions.none()

# Discord logging handler
class DiscordLogHandler(TimedRotatingFileHandler):
    last_rollover_time = 0

    def __init__(self, filename, bot, log_channel_id, when='H', interval=24, backupCount=7, encoding='utf-8', atTime=None):
        self.bot = bot
        self.log_channel_id = log_channel_id
        self.last_send_time = 0
        self.targetHour = 6
        self.targetMinute = 0  # Set to 06:00 as per your timezone and prefered time for log file to be sent to your Discord Log Channel.
        super().__init__(filename, when, interval, backupCount, encoding, atTime=atTime)
        self.rolloverAt = self.computeRollover(time.time())

    def computeRollover(self, currentTime):
        """
        Work out the rollover time based on the current time.
        """
        t = time.localtime(currentTime)
        currentHour = t.tm_hour
        currentMinute = t.tm_min
        
        # Calculate seconds until next rollover
        if (currentHour, currentMinute) < (self.targetHour, self.targetMinute):
            secondsUntilRollover = ((self.targetHour - currentHour) * 3600 +
                                    (self.targetMinute - currentMinute) * 60)
        else:
            secondsUntilRollover = ((24 + self.targetHour - currentHour) * 3600 +
                                    (self.targetMinute - currentMinute) * 60)
        
        return int(currentTime + secondsUntilRollover - (currentTime % 60))

    def doRollover(self):
        current_time = time.time()
        if current_time - DiscordLogHandler.last_rollover_time < 3600:  # Prevent rollovers more often than once per hour
            return

        print(f"Performing log rollover at {datetime.now()}")
        
        # Get the previous day's log file name before rollover
        previous_log = f"{self.baseFilename}.{time.strftime('%Y-%m-%d', time.localtime(current_time - 86400))}"
        
        # Perform the standard rollover
        super().doRollover()
        
        # Send the previous day's log file
        asyncio.create_task(self.send_log_to_discord(previous_log))
        
        DiscordLogHandler.last_rollover_time = current_time
        self.rolloverAt = self.computeRollover(current_time)
        print(f"Next rollover time: {datetime.fromtimestamp(self.rolloverAt)}")
        print("Log rollover completed")

    async def send_log_to_discord(self, log_file):
        current_time = time.time()
        if current_time - self.last_send_time < 86100:  # 24 hour cooldown (23hrs 55 mins this allows for the time it takes to create the file and still still send it to the log channel every day)
            print(f"Skipping log file send, cooldown not elapsed. Next send in {86400 - (current_time - self.last_send_time):.2f} seconds")
            return

        try:
            channel = self.bot.get_channel(self.log_channel_id)
            if channel and os.path.exists(log_file):
                print(f"Attempting to send log file: {log_file}")
                compressed_log = io.BytesIO()
                with gzip.open(compressed_log, 'wb') as f_out:
                    with open(log_file, 'rb') as f_in:
                        f_out.writelines(f_in)
                compressed_log.seek(0)
                
                compressed_size = compressed_log.getbuffer().nbytes
                if compressed_size <= 24 * 1024 * 1024:  # 24MB limit
                    print(f"Sending compressed log file. Size: {compressed_size / (1024 * 1024):.2f} MB")
                    await channel.send(f"Sending log file: {os.path.basename(log_file)}", file=discord.File(compressed_log, filename=f'{os.path.basename(log_file)}.gz'))
                    self.last_send_time = current_time
                    print(f"Compressed log file sent to Discord channel {self.log_channel_id}")
                else:
                    warning_message = (
                        f"⚠️ Warning: The compressed log file {os.path.basename(log_file)} "
                        f"exceeds the 24MB limit (size: {compressed_size / (1024 * 1024):.2f} MB). "
                        f"Please check the log file directory to retrieve the file manually. "
                        f"File path: {log_file}"
                    )
                    print(warning_message)
                    await channel.send(warning_message)
                    print("Warning message sent to Discord channel")
            else:
                error_message = f"Could not find channel with ID {self.log_channel_id} or log file {log_file}"
                print(error_message)
                if channel:
                    await channel.send(f"⚠️ Error: {error_message}")
        except Exception as e:
            error_details = f"Failed to send log to Discord: {type(e).__name__}: {e}\nError details: {traceback.format_exc()}"
            print(error_details)
            if channel:
                await channel.send(f"⚠️ Error: Failed to send log file. Check console for details.")

    def emit(self, record):
        try:
            if self.shouldRollover(record):
                self.doRollover()
            logging.FileHandler.emit(self, record)
            if record.levelno >= logging.WARNING:
                asyncio.create_task(self.send_immediate_log(record))
        except Exception:
            self.handleError(record)

    async def send_immediate_log(self, record):
        try:
            channel = self.bot.get_channel(self.log_channel_id)
            if channel:
                message = self.format(record)
                await channel.send(f"**{record.levelname}**\n```\n{message[:1900]}```")  # Truncate if necessary
            else:
                print(f"Could not find channel with ID {self.log_channel_id} for immediate log")
        except Exception as e:
            print(f"Failed to send immediate log to Discord: {type(e).__name__}: {e}")

    def check_rollover_status(self):
        now = time.time()  # Use time.time() instead of datetime.now()
        rollover_time = self.rolloverAt
        print(f"Current time: {datetime.fromtimestamp(now)}")
        print(f"Next rollover time: {datetime.fromtimestamp(rollover_time)}")
        time_until_rollover = rollover_time - now
        print(f"Time until next rollover: {timedelta(seconds=time_until_rollover)}")
        print(f"Current rollover at timestamp: {self.rolloverAt}")

# File handler for all logs
file_handler = DiscordLogHandler(
    filename='logs/bot.log',
    bot=bot,
    log_channel_id=LOG_CHANNEL_ID,
    when='D',  # Daily rotation
    interval=1,
    backupCount=7,
    encoding='utf-8'
)
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)

# Stream handler for console output
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(stream_formatter)

# FileHandler for discord_bot.log
discord_handler = logging.FileHandler('logs/discord_bot.log')
discord_handler.setLevel(logging.WARNING)
discord_handler.setFormatter(file_formatter)

# Add all handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(stream_handler)
logger.addHandler(discord_handler)

@tasks.loop(minutes=5)
async def periodic_log():
    logger.info("Periodic log message")
    for handler in logger.handlers:
        if isinstance(handler, DiscordLogHandler) and handler.baseFilename.endswith('bot.log'):
            handler.check_rollover_status()
            break  # Exit after checking the first DiscordLogHandler

# 4. Database Setup and Connection
# ================================

def ensure_column_exists(table_name, column_name, column_type):
    c.execute(f"PRAGMA table_info({table_name})")
    columns = [column[1] for column in c.fetchall()]
    if column_name not in columns:
        try:
            c.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
            logger.info(f"Added '{column_name}' column to {table_name} table")
            conn.commit()
        except sqlite3.OperationalError as e:
            if "duplicate column name" not in str(e).lower():
                logger.error(f"Error adding '{column_name}' column to {table_name}: {e}", exc_info=True)
            else:
                logger.info(f"'{column_name}' column already exists in {table_name} table")

try:
    conn = sqlite3.connect('subscriptions.db')
    c = conn.cursor()
    logger.info("Successfully connected to the database")
except sqlite3.Error as e:
    logger.error(f"Error connecting to database: {e}", exc_info=True)
    raise

# Ensure the tables exist with all required columns
try:
    c.execute('''CREATE TABLE IF NOT EXISTS subscriptions
                 (subreddit TEXT, channel_id INTEGER, last_check TEXT, last_submission_id TEXT, failed_attempts INTEGER DEFAULT 0, thread_id INTEGER)''')
    logger.debug("Ensured 'subscriptions' table exists")
except sqlite3.Error as e:
    logger.error(f"Error creating 'subscriptions' table: {e}", exc_info=True)    

try:    
    c.execute('''CREATE TABLE IF NOT EXISTS forum_subscriptions
                 (subreddit TEXT, channel_id INTEGER, thread_id INTEGER, last_check TEXT, last_submission_id TEXT)''')
    logger.debug("Ensured 'forum_subscriptions' table exists")
except sqlite3.Error as e:
    logger.error(f"Error creating 'forum_subscriptions' table: {e}", exc_info=True)

try:    
    c.execute('''CREATE TABLE IF NOT EXISTS button_visibility
                 (button_name TEXT PRIMARY KEY, is_visible INTEGER)''')
    logger.debug("Ensured 'button_visibility' table exists")
except sqlite3.Error as e:
    logger.error(f"Error creating 'button_visibility' table: {e}", exc_info=True)

try:    
    c.execute('''CREATE TABLE IF NOT EXISTS individual_forum_subscriptions
                 (subreddit TEXT, channel_id INTEGER, last_check TEXT)''')
    logger.debug("Ensured 'individual_forum_subscriptions' table exists")
except sqlite3.Error as e:
    logger.error(f"Error creating 'individual_forum_subscriptions' table: {e}", exc_info=True)

try:    
    c.execute('''CREATE TABLE IF NOT EXISTS submission_tracking
                 (subreddit TEXT, channel_id INTEGER, last_check TEXT, last_submission_id TEXT,
                 PRIMARY KEY (subreddit, channel_id))''')
    logger.debug("Ensured 'submission_tracking' table exists")
except sqlite3.Error as e:
    logger.error(f"Error creating 'submission_tracking' table: {e}", exc_info=True)

try:    
    c.execute('''CREATE TABLE IF NOT EXISTS forum_flair_settings
                 (subreddit TEXT, channel_id INTEGER, max_flairs INTEGER DEFAULT 20, flair_enabled INTEGER DEFAULT 1, blacklisted_flairs TEXT, PRIMARY KEY (subreddit, channel_id))''')
    logger.debug("Ensured 'forum_flair_settings' table exists")
except sqlite3.Error as e:
    logger.error(f"Error creating 'forum_flair_settings' table: {e}", exc_info=True)

try:    
    c.execute('''CREATE TABLE IF NOT EXISTS forum_tags
                 (channel_id INTEGER, tag_name TEXT, PRIMARY KEY (channel_id, tag_name))''')
    logger.debug("Ensured 'forum_tags' table exists")
except sqlite3.Error as e:
    logger.error(f"Error creating 'forum_tags' table: {e}", exc_info=True)

# Ensure all necessary columns exist in forum_flair_settings
ensure_column_exists("forum_flair_settings", "subreddit", "TEXT")
ensure_column_exists("forum_flair_settings", "channel_id", "INTEGER")
ensure_column_exists("forum_flair_settings", "max_flairs", "INTEGER")
ensure_column_exists("forum_flair_settings", "flair_enabled", "INTEGER")
ensure_column_exists("forum_flair_settings", "blacklisted_flairs", "TEXT")

# Log the current structure of forum_flair_settings table
c.execute("PRAGMA table_info(forum_flair_settings)")
columns = c.fetchall()
logger.debug(f"Current structure of forum_flair_settings table: {columns}")

try:
    conn.commit()
    logger.debug("Database changes committed successfully")
except sqlite3.Error as e:
    logger.error(f"Error committing changes to database: {e}", exc_info=True)

# Check if the last_submission_id column exists, if not, add it
c.execute("PRAGMA table_info(subscriptions)")
columns = [column[1] for column in c.fetchall()]
if 'last_submission_id' not in columns:
    c.execute("ALTER TABLE subscriptions ADD COLUMN last_submission_id TEXT")
    conn.commit()

# Initialize button visibility settings
button_list = ['Reddit Post', 'Watch Video', 'RedGIFs', 'YouTube Link', 'Image Gallery', 'Web Link']
for button in button_list:
    c.execute("INSERT OR IGNORE INTO button_visibility (button_name, is_visible) VALUES (?, 1)", (button,))
conn.commit()

# 5. Utility Functions
# ====================

def truncate_string(string, max_length):
    return (string[:max_length-3] + '...') if len(string) > max_length else string

def ensure_valid_url(url):
    if url.startswith('//'):
        return f'https:{url}'
    elif url.startswith('/'):
        return f'https://www.reddit.com{url}'
    elif not url.startswith(('http://', 'https://')):
        return f'https://{url}'
    return url

def get_button_visibility():
    try:
        c.execute("SELECT button_name, is_visible FROM button_visibility")
        result = dict(c.fetchall())
        logger.debug(f"Retrieved button visibility settings: {result}")
        return result
    except sqlite3.Error as e:
        logger.error(f"Error retrieving button visibility settings: {e}", exc_info=True)
        return {}

def get_flair_settings(channel_id):
    try:
        with sqlite3.connect('subscriptions.db', isolation_level=None) as conn:
            conn.execute("PRAGMA query_only = ON")
            conn.execute("PRAGMA cache_size = 0")
            c = conn.cursor()
            c.execute("SELECT max_flairs, flair_enabled, blacklisted_flairs FROM forum_flair_settings WHERE channel_id = ?", (channel_id,))
            result = c.fetchone()
            logger.debug(f"Raw database result for channel {channel_id}: {result}")
            if result:
                max_flairs, flair_enabled, blacklisted_flairs = result
                try:
                    blacklisted_flairs_list = json.loads(blacklisted_flairs or '[]')
                except json.JSONDecodeError:
                    logger.error(f"Error decoding blacklisted flairs for channel {channel_id}: {blacklisted_flairs}")
                    blacklisted_flairs_list = []
                logger.info(f"Flair settings retrieved for channel {channel_id}: max_flairs={max_flairs}, flair_enabled={bool(flair_enabled)}, blacklisted_flairs={blacklisted_flairs_list}")
                return max_flairs, bool(flair_enabled), blacklisted_flairs_list
            logger.info(f"No flair settings found for channel {channel_id}, using defaults")
            return 20, True, []  # Default values
    except sqlite3.Error as e:
        logger.error(f"Database error in get_flair_settings for channel {channel_id}: {e}", exc_info=True)
        return 20, True, []  # Default values in case of error

def is_database_locked():
    try:
        with sqlite3.connect('subscriptions.db', timeout=1) as conn:
            conn.execute('SELECT 1 FROM forum_flair_settings LIMIT 1')
        logger.debug("Database is not locked")
        return False
    except sqlite3.OperationalError as e:
        logger.warning(f"Database appears to be locked: {e}")
        return True
        
@contextlib.contextmanager
def get_db_connection():
    conn = None
    try:
        conn = sqlite3.connect('subscriptions.db', timeout=10)
        logger.debug("Database connection established")
        yield conn
    except sqlite3.Error as e:
        logger.error(f"Error in database connection: {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed")

def extract_all_images(text):
    # Pattern to match all Reddit image URLs
    image_pattern = r'(https?://(?:i\.redd\.it|preview\.redd\.it)/\S+?\.(?:jpg|png|gif))(?:\?[^\)\s]+)?'
    return re.findall(image_pattern, text)

def clean_selftext(selftext):
    # Remove URLs from preview.redd.it and i.redd.it
    cleaned_text = re.sub(r'https?://(?:preview|i)\.redd\.it/\S+', '', selftext)
    
    # Handle markdown links: if text and URL are different, keep both; if they're the same, keep only one
    def replace_link(match):
        text, url = match.groups()
        if text.strip() == url.strip():
            return url
        return f"{text} {url}"  # Remove parentheses around URL
    
    cleaned_text = re.sub(r'\[(.*?)\]\((.*?)\)', replace_link, cleaned_text)
    
    # Remove any remaining square brackets
    cleaned_text = re.sub(r'[\[\]]', '', cleaned_text)
    
    # Remove any remaining parentheses at the end of lines or strings
    cleaned_text = re.sub(r'\($', '', cleaned_text)
    cleaned_text = re.sub(r'\($', '', cleaned_text, flags=re.MULTILINE)
    
    # Replace &nbsp; with a space
    cleaned_text = re.sub(r'&nbsp;', ' ', cleaned_text)
    
    # Unescape HTML entities
    cleaned_text = html.unescape(cleaned_text)
    
    # Remove extra whitespace while preserving line breaks
    cleaned_text = re.sub(r' +', ' ', cleaned_text)
    cleaned_text = re.sub(r'\n\s*\n', '\n\n', cleaned_text)
    
    return cleaned_text.strip()

def extract_image_url(submission):
    parsed_url = urlparse(submission.url)
    if parsed_url.netloc == 'preview.redd.it':
        path = parsed_url.path
        if path.endswith(('.jpg', '.png', '.gif')):
            return f"https://i.redd.it{path}"
    elif parsed_url.netloc in ['i.redd.it', 'i.imgur.com']:
        return submission.url
    return None

def extract_video_id(url):
    parsed_url = urlparse(url)
    if parsed_url.netloc in ('youtu.be', 'www.youtu.be'):
        return parsed_url.path[1:]
    if parsed_url.netloc in ('youtube.com', 'www.youtube.com'):
        query = parse_qs(parsed_url.query)
        return query.get('v', [None])[0]
    return None

async def is_valid_image_url(url):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.head(url, allow_redirects=True, timeout=5) as response:
                if response.status == 200:
                    content_type = response.headers.get('Content-Type', '')
                    logger.debug(f"Checked image URL {url}: status={response.status}, content_type={content_type}")
                    return content_type.startswith('image/')
        except Exception as e:
            logger.warning(f"Error checking image URL {url}: {e}")
    logger.debug(f"Invalid image URL: {url}")
    return False

async def sync_forum_tags(forum_channel):
    try:
        c.execute("SELECT tag_name FROM forum_tags WHERE channel_id = ?", (forum_channel.id,))
        db_tags = set(row[0] for row in c.fetchall())
        discord_tags = set(tag.name for tag in forum_channel.available_tags)

        for tag_name in db_tags - discord_tags:
            c.execute("DELETE FROM forum_tags WHERE channel_id = ? AND tag_name = ?", (forum_channel.id, tag_name))
            logger.info(f"Removed tag '{tag_name}' from database for channel {forum_channel.id}")

        for tag_name in discord_tags - db_tags:
            c.execute("INSERT OR IGNORE INTO forum_tags (channel_id, tag_name) VALUES (?, ?)", (forum_channel.id, tag_name))
            logger.info(f"Added tag '{tag_name}' to database for channel {forum_channel.id}")

        conn.commit()
        logger.info(f"Forum tags synced for channel {forum_channel.id}")
    except sqlite3.Error as e:
        logger.error(f"Database error in sync_forum_tags for channel {forum_channel.id}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Error in sync_forum_tags for channel {forum_channel.id}: {e}", exc_info=True)

def has_debug_role():
    async def predicate(interaction: discord.Interaction):
        if interaction.user.id == interaction.guild.owner_id:
            logger.info(f"User {interaction.user.name} is the server owner")
            return True

        debug_role = discord.utils.get(interaction.guild.roles, name="Debug")
        if debug_role is None:
            logger.warning(f"Debug role not found in guild {interaction.guild.name}")
            await interaction.response.send_message("Debug role not found. Please create a role named 'Debug'.", ephemeral=True)
            return False
        logger.debug(f"User {interaction.user.name} roles: {[role.name for role in interaction.user.roles]}")
        if debug_role not in interaction.user.roles:
            logger.info(f"User {interaction.user.name} does not have Debug role")
            await interaction.response.send_message("You don't have permission to use this command.", ephemeral=True)
            return False
        logger.info(f"User {interaction.user.name} has Debug role")
        return True
    return app_commands.check(predicate)

def command_to_dict(cmd):
    try:
        command_dict = {
            'name': cmd.name,
            'description': cmd.description,
        }
        if hasattr(cmd, 'options'):
            command_dict['options'] = [{'name': opt.name, 'description': opt.description, 'type': opt.type.value} for opt in cmd.options]
        return command_dict
    except AttributeError as e:
        logging.warning(f"Error processing command {cmd}: {e}")
        return {'name': str(cmd), 'error': str(e)}

def commands_have_changed(bot):
    try:
        current_commands = {cmd.name: command_to_dict(cmd) for cmd in bot.tree.get_commands()}
        try:
            with open(COMMAND_CACHE_FILE, 'r') as f:
                cached_commands = json.load(f)
            return current_commands != cached_commands
        except FileNotFoundError:
            return True
    except Exception as e:
        logging.error(f"Error checking if commands have changed: {e}")
        return True

def update_command_cache(bot):
    try:
        current_commands = {cmd.name: command_to_dict(cmd) for cmd in bot.tree.get_commands()}
        with open(COMMAND_CACHE_FILE, 'w') as f:
            json.dump(current_commands, f)
    except Exception as e:
        logging.error(f"Error updating command cache: {e}")

# 6. Reddit API Functions
# =======================

@backoff.on_exception(backoff.expo, (asyncprawcore.exceptions.ServerError, asyncprawcore.exceptions.RequestException), max_tries=3)
async def fetch_new_submissions(subreddit, last_check, limit: int = 10) -> list:
    logger.debug(f"Fetching new submissions for r/{subreddit.display_name}, last_check: {last_check}, limit: {limit}")
    new_submissions = []
    try:
        async for submission in subreddit.new(limit=limit):
            submission_time = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)
            if submission_time <= last_check:
                break
            new_submissions.append(submission)
        logger.info(f"Fetched {len(new_submissions)} new submissions for r/{subreddit.display_name}")
    except asyncprawcore.exceptions.Forbidden:
        logger.warning(f"Access to r/{subreddit.display_name} is forbidden. The subreddit might be private or banned.")
    except asyncprawcore.exceptions.NotFound:
        logger.warning(f"Subreddit r/{subreddit.display_name} not found. It might have been deleted or doesn't exist.")
    except asyncprawcore.exceptions.TooManyRequests:
        logger.warning(f"Rate limit exceeded when fetching submissions for r/{subreddit.display_name}. Backing off.")
        raise  # Let backoff handle the retry
    except (asyncprawcore.exceptions.ServerError, asyncprawcore.exceptions.RequestException) as e:
        logger.warning(f"Temporary error when fetching submissions for r/{subreddit.display_name}: {str(e)}")
        raise  # Let backoff handle the retry
    except Exception as e:
        logger.info(f"Unexpected error fetching submissions for r/{subreddit.display_name}: {str(e)}", exc_info=True)
    return new_submissions

async def get_primary_image_url(submission):
    logger.debug(f"Getting primary image URL for submission {submission.id}")
    
    # Check for single image post
    if submission.url.endswith(('.jpg', '.jpeg', '.png', '.gif')):
        logger.debug(f"Single image post detected for submission {submission.id}")
        return submission.url
    
    # Check for gallery (return first image)
    if hasattr(submission, 'is_gallery') and submission.is_gallery:
        logger.debug(f"Gallery post detected for submission {submission.id}")
        if hasattr(submission, 'gallery_data') and submission.gallery_data:
            first_image_id = submission.gallery_data['items'][0]['media_id']
            if first_image_id in submission.media_metadata:
                try:
                    url = submission.media_metadata[first_image_id]['s']['u']
                    logger.debug(f"Gallery image URL found for submission {submission.id}: {url}")
                    return url
                except KeyError:
                    try:
                        url = submission.media_metadata[first_image_id]['p'][-1]['u']
                        logger.debug(f"Gallery image URL found for submission {submission.id}: {url}")
                        return url
                    except (KeyError, IndexError):
                        logger.debug(f"Unable to find image URL for gallery submission {submission.id}")
    
    # Check for image in media_metadata for non-gallery posts
    if hasattr(submission, 'media_metadata'):
        logger.debug(f"Checking media_metadata for submission {submission.id}")
        try:
            first_image_id = next(iter(submission.media_metadata))
            try:
                url = submission.media_metadata[first_image_id]['s']['u']
                logger.debug(f"Image URL found in media_metadata for submission {submission.id}: {url}")
                return url
            except KeyError:
                try:
                    url = submission.media_metadata[first_image_id]['p'][-1]['u']
                    logger.debug(f"Image URL found in media_metadata for submission {submission.id}: {url}")
                    return url
                except (KeyError, IndexError):
                    logger.debug(f"Unable to find image URL in media_metadata for submission {submission.id}")
        except (StopIteration, KeyError):
            pass
    
    # If no direct image found, check preview images
    if hasattr(submission, 'preview') and 'images' in submission.preview:
        logger.debug(f"Checking preview images for submission {submission.id}")
        try:
            url = submission.preview['images'][0]['source']['url']
            logger.debug(f"Preview image URL found for submission {submission.id}: {url}")
            return url
        except KeyError:
            logger.warning(f"Unable to find preview image URL for submission {submission.id}")
    
    # No suitable image found
    logger.info(f"No suitable image found for submission {submission.id}")
    return None

async def get_reddit_video_url(submission):
    logger.debug(f"Getting Reddit video URL for submission {submission.id}")
    if submission.media and 'reddit_video' in submission.media:
        video = submission.media['reddit_video']
        fallback_url = video['fallback_url'].split('?')[0]  # Remove anything after .mp4
        logger.debug(f"Reddit video URL found for submission {submission.id}: {fallback_url}")
        return fallback_url, video.get('scrubber_media_url')
    elif hasattr(submission, 'preview') and 'reddit_video_preview' in submission.preview:
        video = submission.preview['reddit_video_preview']
        fallback_url = video['fallback_url'].split('?')[0]  # Remove anything after .mp4
        logger.debug(f"Reddit video preview URL found for submission {submission.id}: {fallback_url}")
        return fallback_url, video.get('scrubber_media_url')
    elif hasattr(submission, 'secure_media') and submission.secure_media:
        if 'reddit_video' in submission.secure_media:
            video = submission.secure_media['reddit_video']
            fallback_url = video['fallback_url'].split('?')[0] # Remove anything after .mp4
            logger.debug(f"Secure Reddit video URL found for submission {submission.id}: {fallback_url}")
            return fallback_url, video.get('scrubber_media_url')
    logger.info(f"No Reddit video URL found for submission {submission.id}")
    return None, None

async def get_youtube_info(video_id):
    logger.debug(f"Getting YouTube info for video ID: {video_id}")
    url = f"https://www.youtube.com/oembed?url=http://www.youtube.com/watch?v={video_id}&format=json"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Successfully retrieved YouTube info for video ID: {video_id}")
                    return data.get('title', 'YouTube Video'), data.get('thumbnail_url')
                else:
                    logger.info(f"Failed to retrieve YouTube info for video ID: {video_id}. Status: {response.status}")
        except Exception as e:
            logger.error(f"Error retrieving YouTube info for video ID: {video_id}. Error: {str(e)}", exc_info=True)
    return 'YouTube Video', None
    
async def sync_tree_with_backoff(commands=None, guild=None, max_retries=5):
    for attempt in range(max_retries):
        try:
            sync_start = time.time()
            if commands:
                synced = await bot.tree.sync(guild=guild)
            else:
                synced = await bot.tree.sync()
            sync_end = time.time()
            logger.info(f"Synced {len(synced)} command(s) in {sync_end - sync_start:.2f} seconds")
            return
        except discord.HTTPException as e:
            wait_time = (2 ** attempt) + (random.randint(0, 1000) / 1000)
            logger.warning(f"Sync attempt {attempt + 1} failed. HTTP Status: {e.status}, Error: {e.text}")
            logger.info(f"Retrying in {wait_time:.2f} seconds.")
            await asyncio.sleep(wait_time)
        except Exception as e:
            logger.error(f"Unexpected error during sync: {type(e).__name__}: {str(e)}", exc_info=True)
            return
    logger.error("Failed to sync tree after maximum retries")
    

# 7. Discord Embed and Message Functions
# ======================================

# This function is specifically designed for use with process_individual_forum_subscriptions
async def create_simple_reddit_embed(submission):
    logger.debug(f"Creating simple Reddit embed for submission {submission.id}")
    embed = discord.Embed()
    
    image_set = False
    
    # Check if the submission is a crosspost
    if hasattr(submission, 'crosspost_parent_list') and submission.crosspost_parent_list:
        logger.info(f"Detected crosspost for submission {submission.id}")
        original_post_data = submission.crosspost_parent_list[0]
        # Use the original submission for image processing
        processing_submission = asyncpraw.models.Submission(reddit=submission._reddit, _data=original_post_data)
        # Keep the crosspost's title and subreddit
        processing_submission.title = submission.title
        if hasattr(submission, 'subreddit') and hasattr(submission.subreddit, 'display_name'):
            processing_submission.subreddit = submission.subreddit.display_name
        else:
            logger.warning(f"Could not get subreddit display name for submission {submission.id}")
            processing_submission.subreddit = "Unknown Subreddit"
    else:
        processing_submission = submission

    # Function to extract image URL
    def get_image_url(item):
        if isinstance(item, dict):
            if item.get('e') == 'AnimatedImage':
                return item.get('s', {}).get('gif')
            else:
                return item.get('s', {}).get('u')
        return None

    # Check for YouTube video
    if 'youtu.be' in processing_submission.url or 'youtube.com' in processing_submission.url:
        youtube_id = extract_video_id(processing_submission.url)
        if youtube_id:
            thumbnail_url = f"https://img.youtube.com/vi/{youtube_id}/maxresdefault.jpg"
            embed.set_image(url=thumbnail_url)
            image_set = True
            logger.debug(f"Set YouTube thumbnail for submission {processing_submission.id}")

    # Check for RedGIFs thumbnail
    if 'redgifs.com' in processing_submission.url:
        if hasattr(processing_submission, 'preview') and 'images' in processing_submission.preview:
            try:
                image_url = processing_submission.preview['images'][0]['source']['url']
                embed.set_image(url=image_url)
                image_set = True
                logger.debug(f"Set RedGIFs thumbnail for submission {processing_submission.id}")
            except KeyError:
                logger.warning(f"Failed to set RedGIFs thumbnail for submission {processing_submission.id}")

    # Check for Reddit video thumbnail
    if not image_set and hasattr(processing_submission, 'is_video') and processing_submission.is_video:
        if hasattr(processing_submission, 'preview') and 'images' in processing_submission.preview:
            try:
                image_url = processing_submission.preview['images'][0]['source']['url']
                embed.set_image(url=image_url)
                image_set = True
                logger.debug(f"Set Reddit video thumbnail for submission {processing_submission.id}")
            except KeyError:
                logger.warning(f"Failed to set Reddit video thumbnail for submission {processing_submission.id}")
        if not image_set and hasattr(processing_submission, 'thumbnail') and processing_submission.thumbnail != 'default':
            embed.set_image(url=processing_submission.thumbnail)
            image_set = True
            logger.debug(f"Set Reddit video thumbnail URL for submission {processing_submission.id}: {processing_submission.thumbnail}")

    # Check for gallery
    if not image_set and hasattr(processing_submission, 'is_gallery') and processing_submission.is_gallery:
        if hasattr(processing_submission, 'gallery_data') and processing_submission.gallery_data:
            first_image_id = processing_submission.gallery_data['items'][0]['media_id']
            if first_image_id in processing_submission.media_metadata:
                image_url = get_image_url(processing_submission.media_metadata[first_image_id])
                if image_url:
                    embed.set_image(url=image_url)
                    image_set = True
                    logger.debug(f"Set gallery image for submission {processing_submission.id}")

    # Check for single image post or multiple images
    if not image_set and hasattr(processing_submission, 'url'):
        if processing_submission.url.endswith(('.jpg', '.jpeg', '.png', '.gif')):
            if await is_valid_image_url(processing_submission.url):
                embed.set_image(url=processing_submission.url)
                image_set = True
                logger.debug(f"Set single image for submission {processing_submission.id}")
        elif hasattr(processing_submission, 'preview') and 'images' in processing_submission.preview:
            try:
                image_data = processing_submission.preview['images'][0]
                if 'variants' in image_data and 'gif' in image_data['variants']:
                    image_url = image_data['variants']['gif']['source']['url']
                else:
                    image_url = image_data['source']['url']
                # Replace preview.redd.it with i.redd.it
                image_url = image_url.replace('preview.redd.it', 'i.redd.it')
                if await is_valid_image_url(image_url):
                    embed.set_image(url=image_url)
                    image_set = True
                    logger.debug(f"Set preview image for submission {processing_submission.id}")
            except KeyError:
                logger.warning(f"Failed to set preview image for processing_submission {processing_submission.id}")

    # Check for image in media_metadata for non-gallery posts
    if not image_set and hasattr(processing_submission, 'media_metadata'):
        try:
            first_image_id = next(iter(processing_submission.media_metadata))
            image_url = get_image_url(processing_submission.media_metadata[first_image_id])
            if image_url:
                embed.set_image(url=image_url)
                image_set = True
                logger.debug(f"Set media_metadata image for submission {processing_submission.id}")
        except (StopIteration, KeyError):
            logger.warning(f"Failed to set media_metadata image for submission {processing_submission.id}")

    # Use fallback image if no other image was set
    if not image_set:
        fallback_image_url = "https://www.redditstatic.com/desktop2x/img/favicon/android-icon-512x512.png"
        embed.set_image(url=fallback_image_url)
        logger.debug(f"Set fallback image for submission {processing_submission.id}")

    logger.info(f"Created simple Reddit embed for submission {processing_submission.id}")
    
    # Check for poll
    if hasattr(processing_submission, 'poll_data') and processing_submission.poll_data:
        poll_data = processing_submission.poll_data
        poll_options = "\n".join([f"{i+1}. {option.text}" for i, option in enumerate(poll_data.options)])
        embed.add_field(name="Poll Options", value=poll_options, inline=False)
        
        if poll_data.voting_end_timestamp:
            try:
                # Always convert to seconds if the timestamp is too large
                if poll_data.voting_end_timestamp > 253402300799:  # Max valid Unix timestamp (year 9999)
                    adjusted_timestamp = poll_data.voting_end_timestamp / 1000
                    logger.info(f"Converted large timestamp from {poll_data.voting_end_timestamp} to {adjusted_timestamp}")
                else:
                    adjusted_timestamp = poll_data.voting_end_timestamp

                end_time = datetime.fromtimestamp(adjusted_timestamp, tz=timezone.utc)
                current_time = datetime.now(timezone.utc)
                duration = end_time - current_time
                
                if duration.total_seconds() > 0:
                    days = duration.days
                    hours, remainder = divmod(duration.seconds, 3600)
                    minutes, _ = divmod(remainder, 60)
                    duration_str = f"{days} days, {hours} hours, {minutes} minutes"
                    embed.add_field(name="Poll Ends", value=f"In {duration_str}", inline=False)
                else:
                    embed.add_field(name="Poll Status", value="Poll has ended", inline=False)
            except (ValueError, OverflowError, OSError) as e:
                logger.error(f"Error processing poll end time: {e}")
                embed.add_field(name="Poll End Time", value="End time not available", inline=False)
        
        embed.set_footer(text=f"r/{processing_submission.subreddit.display_name} | Poll")
        logger.debug(f"Added poll data to embed for submission {processing_submission.id}")

    logger.info(f"Created simple Reddit embed for submission {processing_submission.id}")
    logger.debug(f"Image set: {image_set}")
    logger.debug(f"Embed image URL: {embed.image.url if embed.image else 'No image set'}")
    return embed

async def send_image_carousel(channel, image_urls, view=None):
    print(f"Debug: send_image_carousel received {len(image_urls)} images")
    files = []
    oversized_images = []
    async with aiohttp.ClientSession() as session:
        for url in image_urls:
            url = url.replace('preview.redd.it', 'i.redd.it')
            print(f"Debug: Processing image URL: {url}")
            async with session.get(url) as resp:
                if resp.status == 200:
                    content_length = int(resp.headers.get('Content-Length', 0))
                    if content_length <= MAX_VIDEO_SIZE:
                        data = await resp.read()
                        file_extension = url.split('.')[-1].split('?')[0].lower()
                        filename = f"image.{file_extension}"
                        files.append(discord.File(io.BytesIO(data), filename=filename))
                        print(f"Debug: Successfully added image {filename} to files list")
                    else:
                        oversized_images.append(url)
                        print(f"Debug: Image {url} exceeds size limit, added to oversized images list")
                else:
                    print(f"Debug: Failed to fetch image from {url}. Status code: {resp.status}")
    
    if files:
        print(f"Debug: Sending {len(files)} images to Discord")
        # Send up to 10 images in a single message
        for i in range(0, len(files), 10):
            is_last_chunk = (i + 10 >= len(files))
            try:
                if hasattr(channel, 'thread'):
                    if is_last_chunk and view:
                        await channel.thread.send(files=files[i:i+10], view=view)
                    else:
                        await channel.thread.send(files=files[i:i+10])
                else:
                    if is_last_chunk and view:
                        await channel.send(files=files[i:i+10], view=view)
                    else:
                        await channel.send(files=files[i:i+10])
            except discord.HTTPException as e:
                print(f"Error sending images: {e}")
                # If sending fails, add these images to the oversized list
                oversized_images.extend([f.filename for f in files[i:i+10]])
    else:
        print("Debug: No files to send")

    return files, oversized_images

async def create_reddit_embed(submission):
    embed = discord.Embed()
    
    image_set = False
    
    # Function to extract image URL
    def get_image_url(item):
        if isinstance(item, dict):
            if item.get('e') == 'AnimatedImage':
                return item.get('s', {}).get('gif')
            else:
                return item.get('s', {}).get('u')
        return None

    # Check for YouTube video
    if 'youtu.be' in submission.url or 'youtube.com' in submission.url:
        youtube_id = extract_video_id(submission.url)
        if youtube_id:
            thumbnail_url = f"https://img.youtube.com/vi/{youtube_id}/maxresdefault.jpg"
            embed.set_image(url=thumbnail_url)
            image_set = True

    # Check for RedGIFs thumbnail
    if 'redgifs.com' in submission.url:
        if hasattr(submission, 'preview') and 'images' in submission.preview:
            try:
                image_url = submission.preview['images'][0]['source']['url']
                embed.set_image(url=image_url)
                image_set = True
            except KeyError:
                pass

    # Check for Reddit video thumbnail
    if not image_set and hasattr(submission, 'is_video') and submission.is_video:
        if hasattr(submission, 'preview') and 'images' in submission.preview:
            try:
                image_url = submission.preview['images'][0]['source']['url']
                embed.set_image(url=image_url)
                image_set = True
            except KeyError:
                pass
        if not image_set and hasattr(submission, 'thumbnail') and submission.thumbnail != 'default':
            embed.set_image(url=submission.thumbnail)
            image_set = True
            print(f"Reddit Video thumbnail URL: {submission.thumbnail}")  # Debug logging

    # Check for gallery
    if not image_set and hasattr(submission, 'is_gallery') and submission.is_gallery:
        if hasattr(submission, 'gallery_data') and submission.gallery_data:
            first_image_id = submission.gallery_data['items'][0]['media_id']
            if first_image_id in submission.media_metadata:
                image_url = get_image_url(submission.media_metadata[first_image_id])
                if image_url:
                    embed.set_image(url=image_url)
                    image_set = True

    # Check for single image post or multiple images
    if not image_set and hasattr(submission, 'url'):
        if submission.url.endswith(('.jpg', '.jpeg', '.png', '.gif')):
            if await is_valid_image_url(submission.url):
                embed.set_image(url=submission.url)
                image_set = True
        elif hasattr(submission, 'preview') and 'images' in submission.preview:
            try:
                image_data = submission.preview['images'][0]
                if 'variants' in image_data and 'gif' in image_data['variants']:
                    image_url = image_data['variants']['gif']['source']['url']
                else:
                    image_url = image_data['source']['url']
                # Replace preview.redd.it with i.redd.it
                image_url = image_url.replace('preview.redd.it', 'i.redd.it')
                if await is_valid_image_url(image_url):
                    embed.set_image(url=image_url)
                    image_set = True
            except KeyError:
                pass

    # Check for image in media_metadata for non-gallery posts
    if not image_set and hasattr(submission, 'media_metadata'):
        try:
            first_image_id = next(iter(submission.media_metadata))
            image_url = get_image_url(submission.media_metadata[first_image_id])
            if image_url:
                embed.set_image(url=image_url)
                image_set = True
        except (StopIteration, KeyError):
            pass

    # Use fallback image if no other image was set
    if not image_set:
        fallback_image_url = "https://www.redditstatic.com/desktop2x/img/favicon/android-icon-512x512.png"
        embed.set_image(url=fallback_image_url)
    
    print(f"Reddit Embed data for submission {submission.id}: {embed.to_dict()}")
    return embed

def create_button(label, url, button_visibility):
    if button_visibility.get(label, True):
        return discord.ui.Button(label=label, url=ensure_valid_url(url))
    return None

async def send_suppressed_message(channel, content=None, embed=None, view=None, file=None):
    return await channel.send(
        content=content, 
        embed=embed, 
        view=view, 
        file=file, 
        allowed_mentions=discord.AllowedMentions.none()
    )

# 8. Forum and Tag Management Functions
# =====================================

async def get_flair_as_tag(submission, forum_channel):
    if not submission.link_flair_text:
        logger.info(f"No flair for submission {submission.id}")
        return None

    max_flairs, flair_enabled, blacklisted_flairs = get_flair_settings(forum_channel.id)
    logger.info(f"Flair settings for channel {forum_channel.id}: max_flairs={max_flairs}, flair_enabled={flair_enabled}, blacklisted_flairs={blacklisted_flairs}")

    if not flair_enabled:
        logger.info(f"Flair-to-tag conversion is disabled for channel {forum_channel.id}")
        return None

    if submission.link_flair_text in blacklisted_flairs:
        logger.info(f"Flair '{submission.link_flair_text}' is blacklisted for channel {forum_channel.id}")
        return None

    # Truncate flair name to 20 characters
    truncated_flair_name = submission.link_flair_text[:20]

    # Check if the tag already exists
    tag = discord.utils.get(forum_channel.available_tags, name=truncated_flair_name)
    if tag:
        logger.info(f"Existing tag found for flair '{truncated_flair_name}' in channel {forum_channel.id}")
        return tag

    # Create a new tag if possible
    if len(forum_channel.available_tags) < min(max_flairs, 20):
        try:
            new_tags = list(forum_channel.available_tags) + [discord.ForumTag(name=truncated_flair_name)]
            await forum_channel.edit(available_tags=new_tags)
            new_tag = discord.utils.get(forum_channel.available_tags, name=truncated_flair_name)
            logger.info(f"Created new tag for flair '{truncated_flair_name}' in channel {forum_channel.id}")
            return new_tag
        except discord.Forbidden:
            logger.error(f"Error creating tag: Missing permissions for channel {forum_channel.id}")
        except discord.HTTPException as e:
            logger.error(f"Error creating tag: {e}")

    logger.info(f"Could not create or find tag for flair: {submission.link_flair_text}")
    return None

async def sync_forum_tags_function(forum: discord.ForumChannel):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT blacklisted_flairs FROM forum_flair_settings WHERE channel_id = ?", (forum.id,))
            result = c.fetchone()
        
        if result and result[0]:
            blacklist = json.loads(result[0])
            removed_tags = []
            tags_to_keep = [tag for tag in forum.available_tags if tag.name not in blacklist]
            
            if len(tags_to_keep) < len(forum.available_tags):
                try:
                    await forum.edit(available_tags=tags_to_keep)
                    removed_tags = [tag.name for tag in forum.available_tags if tag.name in blacklist]
                    logger.info(f"Removed tags {removed_tags} from forum {forum.name}")
                except discord.HTTPException as e:
                    logger.error(f"Failed to remove tags from forum {forum.name}: {e}")
            
            return removed_tags
        else:
            logger.info(f"No blacklist settings found for forum {forum.name}")
            return []
    except sqlite3.Error as e:
        logger.error(f"An error occurred while accessing the database for forum {forum.name}: {e}")
        return []
    except discord.HTTPException as e:
        logger.error(f"An error occurred while syncing forum tags for {forum.name}: {e}")
        return []

async def sync_forum_tags(forum_channel):
    # Remove tags from our database that aren't in Discord
    c.execute("SELECT tag_name FROM forum_tags WHERE channel_id = ?", (forum_channel.id,))
    db_tags = set(row[0] for row in c.fetchall())
    discord_tags = set(tag.name for tag in forum_channel.available_tags)

    for tag_name in db_tags - discord_tags:
        c.execute("DELETE FROM forum_tags WHERE channel_id = ? AND tag_name = ?", (forum_channel.id, tag_name))

    # Add tags from Discord that aren't in our database
    for tag_name in discord_tags - db_tags:
        c.execute("INSERT OR IGNORE INTO forum_tags (channel_id, tag_name) VALUES (?, ?)", (forum_channel.id, tag_name))

    conn.commit()

# 9. Submission Processing Functions
# ==================================

def clean_video_post_text(selftext, video_urls):
    # Remove video URLs from the text
    for url in video_urls:
        selftext = selftext.replace(url, '')
    
    # Split into lines, remove empty lines, and rejoin
    lines = [line.strip() for line in selftext.split('\n') if line.strip() and line.strip() != '&#x200B;']
    cleaned_text = '\n\n'.join(lines)  # Use double newline for paragraph separation
    
    return cleaned_text if cleaned_text.strip() and cleaned_text.strip() != '&#x200B;' else None

# Poll Post Handeling
async def process_reddit_poll(submission, channel, button_visibility):
    logger.info(f"Processing Reddit Poll: {submission.id}")
    
    embed = discord.Embed(
        title=truncate_string(submission.title, 256),
        url=f"https://www.reddit.com{submission.permalink}",
        color=discord.Color.green()
    )
    
    # Add author information
    author_name = submission.author.name if submission.author else "[deleted]"
    author_icon = None
    if submission.author:
        try:
            await submission.author.load()
            author_icon = submission.author.icon_img
        except Exception as e:
            logger.warning(f"Failed to load author icon for {author_name}: {e}")

    embed.set_author(name=author_name, icon_url=author_icon)
    embed.set_footer(text=f"r/{submission.subreddit.display_name} | Poll")
    embed.timestamp = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)
    
    # Add cleaned selftext if available
    if submission.selftext:
        cleaned_text = clean_selftext(submission.selftext)
        if cleaned_text:
            # Remove video URLs from the cleaned text
            video_url_pattern = r'https://reddit\.com/link/[^/]+/video/[^/]+/player'
            cleaned_text = re.sub(video_url_pattern, '', cleaned_text).strip()
            if cleaned_text:
                embed.description = truncate_string(cleaned_text, 4096)  # Discord embed description limit

    # Add poll information
    poll_data = submission.poll_data
    options = [f"{i+1}. {option.text}" for i, option in enumerate(poll_data.options)]
    embed.add_field(name="Poll Options", value="\n".join(options), inline=False)
    
    if poll_data.voting_end_timestamp:
        try:
            if poll_data.voting_end_timestamp > 253402300799:
                adjusted_timestamp = poll_data.voting_end_timestamp / 1000
            else:
                adjusted_timestamp = poll_data.voting_end_timestamp

            end_time = datetime.fromtimestamp(adjusted_timestamp, tz=timezone.utc)
            current_time = datetime.now(timezone.utc)
            duration = end_time - current_time
            
            if duration.total_seconds() > 0:
                days = duration.days
                hours, remainder = divmod(duration.seconds, 3600)
                minutes, _ = divmod(remainder, 60)
                duration_str = f"{days} days, {hours} hours, {minutes} minutes"
                embed.add_field(name="Poll Ends", value=f"In {duration_str}", inline=False)
            else:
                embed.add_field(name="Poll Status", value="Poll has ended", inline=False)
        except (ValueError, OverflowError, OSError) as e:
            logger.error(f"Error processing poll end time: {e}")
            embed.add_field(name="Poll End Time", value="End time not available", inline=False)

    # Add total votes if available
    if hasattr(poll_data, 'total_vote_count'):
        embed.add_field(name="Total Votes", value=str(poll_data.total_vote_count), inline=False)

    # Handle different media types
    image_urls = []
    video_urls = set()
    if hasattr(submission, 'media_metadata') and submission.media_metadata:
        for item in submission.media_metadata.values():
            if item['e'] == 'Image':
                image_url = item['s']['u'].split('?')[0]  # Remove query parameters
                image_urls.append(image_url)
            elif item['e'] == 'AnimatedImage':
                image_url = item['s']['gif'].split('?')[0]  # Remove query parameters
                image_urls.append(image_url)
            elif item['e'] == 'RedditVideo':
                video_url_matches = re.findall(r'https://reddit\.com/link/[^/]+/video/[^/]+/player', submission.selftext)
                video_urls.update(video_url_matches)

    if video_urls:
        embed.add_field(name="Reddit Video", value="This type of Reddit video(s) can only be viewed online or via the Reddit App.", inline=False)
        video_links = "\n".join(video_urls)
        embed.add_field(name="Video Link(s)", value=video_links, inline=False)
        
        view = discord.ui.View()
        reddit_post_button = create_button("Reddit Post", f"https://www.reddit.com{submission.permalink}", button_visibility)
        watch_video_button = create_button("Watch Video", next(iter(video_urls)), button_visibility)
        if reddit_post_button:
            view.add_item(reddit_post_button)
        if watch_video_button:
            view.add_item(watch_video_button)
        
        await channel.send(embed=embed, view=view)
    elif image_urls:
        # Send poll information first
        await channel.send(embed=embed)

        # Process images if any (no changes to this part)
        oversized_gifs = []
        remaining_urls = []
        async with aiohttp.ClientSession() as session:
            for url in image_urls:
                if url.lower().endswith('.gif'):
                    async with session.head(url) as resp:
                        if resp.status == 200:
                            content_length = int(resp.headers.get('Content-Length', 0))
                            if content_length > MAX_VIDEO_SIZE:
                                oversized_gifs.append(url)
                            else:
                                remaining_urls.append(url)
                else:
                    remaining_urls.append(url)

        # Embed oversized GIFs (no changes to this part)
        for i, gif_url in enumerate(oversized_gifs):
            if i == len(oversized_gifs) - 1 and not remaining_urls:
                # This is the last oversized GIF and there are no remaining images
                view = discord.ui.View()
                reddit_post_button = create_button("Reddit Post", f"https://www.reddit.com{submission.permalink}", button_visibility)
                if reddit_post_button:
                    view.add_item(reddit_post_button)
                await embed_oversized_gif(channel, None, view, gif_url)
            else:
                await embed_oversized_gif(channel, None, None, gif_url)

        # Send remaining images in carousel with the Reddit Post button (no changes to this part)
        if remaining_urls:
            image_view = discord.ui.View()
            image_reddit_post_button = create_button("Reddit Post", f"https://www.reddit.com{submission.permalink}", button_visibility)
            if image_reddit_post_button:
                image_view.add_item(image_reddit_post_button)
            await send_image_carousel(channel, remaining_urls, image_view)
    else:
        # For text-only polls, send everything in one message
        view = discord.ui.View()
        reddit_post_button = create_button("Reddit Post", f"https://www.reddit.com{submission.permalink}", button_visibility)
        if reddit_post_button:
            view.add_item(reddit_post_button)
        await channel.send(embed=embed, view=view)

    logger.info(f"Processed Reddit Poll: {submission.id}")

def add_footer_and_crosspost_info(embed, processing_submission, submission, original_post_data=None):
    # Add crosspost information if it's a crosspost
    if processing_submission != submission and original_post_data:
        subreddit_url = f"https://www.reddit.com/r/{original_post_data['subreddit']}"
        crosspost_info = f"[r/{original_post_data['subreddit']}]({subreddit_url})"
        embed.add_field(name="Crosspost", value=crosspost_info, inline=False)
    
    # Always set footer and timestamp
    embed.set_footer(text=f"r/{processing_submission.subreddit.display_name}")
    embed.timestamp = datetime.fromtimestamp(processing_submission.created_utc, tz=timezone.utc)

async def process_submission(submission, channel, button_visibility):
    # If we received a ThreadWithMessage, use its thread attribute
    if hasattr(channel, 'thread'):
        channel = channel.thread

    try:
        # Ensure the submission is fully loaded
        await submission.load()
    except asyncprawcore.exceptions.RequestException as e:
        logger.error(f"Error loading submission: {e}")
        return  # Exit if we can't load the submission

    # Check if the submission is a crosspost
    if hasattr(submission, 'crosspost_parent_list') and submission.crosspost_parent_list:
        logger.info(f"Detected crosspost for submission {submission.id}")
        original_post_data = submission.crosspost_parent_list[0]
        # Use the original submission for content processing
        processing_submission = asyncpraw.models.Submission(reddit=submission._reddit, _data=original_post_data)
        # Keep the crosspost's title and subreddit
        processing_submission.title = submission.title
        processing_submission.subreddit = submission.subreddit.display_name if hasattr(submission.subreddit, 'display_name') else str(submission.subreddit)
        processing_submission.author = submission.author
        processing_submission.created_utc = submission.created_utc
        processing_submission.permalink = submission.permalink
    else:
        processing_submission = submission

    # First, check if it's a poll post
    if hasattr(processing_submission, 'poll_data') and processing_submission.poll_data:
        await process_reddit_poll(processing_submission, channel, button_visibility)
        return

    # Then, try to process as a Reddit Video
    if await process_reddit_video(processing_submission, channel, button_visibility):
        return  # If it was a Reddit Video, we're done

    author_name = processing_submission.author.name if processing_submission.author else "[deleted]"
    author_profile_url = f"https://www.reddit.com/user/{author_name}" if processing_submission.author else None
    
    try:
        if processing_submission.author:
            await processing_submission.author.load()
            author_icon_url = processing_submission.author.icon_img if hasattr(processing_submission.author, 'icon_img') else None
        else:
            author_icon_url = None
    except Exception as e:
        logger.info(f"Error fetching author details: {e}")
        author_icon_url = None
    
    embed = discord.Embed(
        title=truncate_string(processing_submission.title, 256),
        url=f"https://www.reddit.com{processing_submission.permalink}",
        color=discord.Color.green()
    )
    
    embed.set_author(name=author_name, url=author_profile_url, icon_url=author_icon_url)

    view = discord.ui.View()
    reddit_post_button = create_button("Reddit Post", f"https://www.reddit.com{processing_submission.permalink}", button_visibility)
    if reddit_post_button:
        view.add_item(reddit_post_button)

    message_sent = False
    image_urls = []  # Initialize image_urls at the beginning

    # Process text content (if exists)
    if processing_submission.selftext:
        cleaned_text = clean_selftext(processing_submission.selftext)
        if cleaned_text:
            embed.description = cleaned_text[:4000] if len(cleaned_text) > 4000 else cleaned_text

    # Now check if it's a gallery
    if hasattr(processing_submission, 'is_gallery') and processing_submission.is_gallery:
        gallery_items = processing_submission.gallery_data['items']
        image_urls = []
        oversized_gifs = []
        async with aiohttp.ClientSession() as session:
            for item in gallery_items:
                media_id = item['media_id']
                media_info = processing_submission.media_metadata.get(media_id, {})
                
                # Check if 'm' key exists in media_info
                if 'm' in media_info:
                    image_url = f"https://i.redd.it/{media_id}.{media_info['m'].split('/')[-1]}"
                    
                    # Check image size and separate GIFs
                    async with session.head(image_url) as resp:
                        if resp.status == 200:
                            content_length = int(resp.headers.get('Content-Length', 0))
                            if image_url.lower().endswith('.gif') and content_length > MAX_VIDEO_SIZE:
                                oversized_gifs.append(image_url)
                            else:
                                image_urls.append(image_url)
                else:
                    logger.warning(f"'m' key not found in media_info for media_id: {media_id}. Skipping this item.")

        image_count = len(gallery_items)
        embed.add_field(name="Image Gallery", value=f"This Reddit Post contains {image_count} image{'s' if image_count != 1 else ''}")
        embed.add_field(name="Gallery Link", value=processing_submission.url, inline=False)

        # Send the initial message with embed and without buttons
        add_footer_and_crosspost_info(embed, processing_submission, submission, original_post_data if processing_submission != submission else None)
        await channel.send(embed=embed)
        message_sent = True

        # Create a new view for the image message
        image_view = discord.ui.View()
        reddit_post_button = create_button("Reddit Post", f"https://www.reddit.com{processing_submission.permalink}", button_visibility)
        gallery_button = create_button("Image Gallery", processing_submission.url, button_visibility)

        # Handle oversized GIFs
        for gif_url in oversized_gifs:
            await embed_oversized_gif(channel, None, None, gif_url)

        # Send remaining images in carousel
        if image_urls:
            for i in range(0, len(image_urls), 10):
                chunk = image_urls[i:i+10]
                is_last_chunk = (i + 10 >= len(image_urls))
                
                if is_last_chunk:
                    if reddit_post_button:
                        image_view.add_item(reddit_post_button)
                    if gallery_button:
                        image_view.add_item(gallery_button)
                    await send_image_carousel(channel, chunk, view=image_view)
                else:
                    await send_image_carousel(channel, chunk)
        elif reddit_post_button and gallery_button:  # If there are no remaining URLs but we have oversized GIFs
            image_view.add_item(reddit_post_button)
            image_view.add_item(gallery_button)
            await send_suppressed_message(channel, view=image_view)

        return  # Exit the function after handling the gallery

    elif 'redgifs.com' in processing_submission.url:
        fallback_url = None
        if hasattr(processing_submission, 'preview') and 'reddit_video_preview' in processing_submission.preview:
            fallback_url = processing_submission.preview['reddit_video_preview'].get('fallback_url')
    
        if fallback_url:
            fallback_url = fallback_url.split('?')[0]  # Remove anything after .mp4
            video_content = await download_video(fallback_url, MAX_VIDEO_SIZE)
            if video_content:
                with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as temp_file:
                    temp_file.write(video_content)
                    temp_file_path = temp_file.name

                file = discord.File(temp_file_path, filename="redgifs_video.mp4")
                embed.add_field(name="RedGIFs Video", value=processing_submission.url)
            
                # Create a new view for the video message
                video_view = discord.ui.View()
                reddit_post_button = create_button("Reddit Post", f"https://www.reddit.com{processing_submission.permalink}", button_visibility)
                if reddit_post_button:
                    video_view.add_item(reddit_post_button)
                redgifs_button = create_button("RedGIFs", processing_submission.url, button_visibility)
                if redgifs_button:
                    video_view.add_item(redgifs_button)
            
                # Send the embed message without buttons
                add_footer_and_crosspost_info(embed, processing_submission, submission, original_post_data if processing_submission != submission else None)
                await channel.send(embed=embed)
            
                # Send the video file with buttons in a separate message
                await channel.send(file=file, view=video_view)
            
                os.unlink(temp_file_path)
                return  # Exit the function after sending both messages
            else:
                embed.add_field(name="RedGIFs Link", value=processing_submission.url)
                redgifs_button = create_button("RedGIFs", processing_submission.url, button_visibility)
                if redgifs_button:
                    view.add_item(redgifs_button)

        else:
            embed.add_field(name="RedGIFs Link", value=processing_submission.url)
            redgifs_button = create_button("RedGIFs", processing_submission.url, button_visibility)
            if redgifs_button:
                view.add_item(redgifs_button)
    elif processing_submission.is_self:
        print(f"Processing self post: {processing_submission.id}")
        
        if processing_submission.selftext:
            #print(f"Original selftext: {processing_submission.selftext}")
            
            # Clean the text while preserving URLs
            cleaned_text = clean_selftext(processing_submission.selftext)
            
            #print(f"Cleaned text: {cleaned_text}")
            
            # Set description with the full cleaned text, including URLs
            if cleaned_text:
                embed.description = cleaned_text[:4000]
            
            # Process images
            image_urls = extract_all_images(processing_submission.selftext)
            
            if image_urls:
                if len(image_urls) == 1:
                    embed.add_field(name="Reddit Image", value="This post contains 1 image.", inline=False)
                else:
                    embed.add_field(name="Reddit Images", value=f"This post contains {len(image_urls)} images.", inline=False)
        
        print(f"Final embed description: {embed.description}")
        
        if not image_urls:
			# Only add the Reddit Post button for text-only posts
            view = discord.ui.View()
            reddit_post_button = create_button("Reddit Post", f"https://www.reddit.com{processing_submission.permalink}", button_visibility)
            if reddit_post_button:
                view.add_item(reddit_post_button)
            add_footer_and_crosspost_info(embed, processing_submission, submission, original_post_data if processing_submission != submission else None)
            if hasattr(channel, 'thread'):
                await channel.thread.send(embed=embed, view=view)
            else:
                await channel.send(embed=embed, view=view)
        else:
            # For posts with images, send the text content without the button
            add_footer_and_crosspost_info(embed, processing_submission, submission, original_post_data if processing_submission != submission else None)
            if hasattr(channel, 'thread'):
                await channel.thread.send(embed=embed)
            else:
                await channel.send(embed=embed)
        
        # Process images if any
        if image_urls:
            # Process oversized GIFs and remaining images
            oversized_gifs = []
            remaining_urls = []
            async with aiohttp.ClientSession() as session:
                for url in image_urls:
                    if url.lower().endswith('.gif'):
                        async with session.head(url) as resp:
                            if resp.status == 200:
                                content_length = int(resp.headers.get('Content-Length', 0))
                                if content_length > MAX_VIDEO_SIZE:
                                    oversized_gifs.append(url)
                                else:
                                    remaining_urls.append(url)
                    else:
                        remaining_urls.append(url)

            # Embed oversized GIFs
            for i, gif_url in enumerate(oversized_gifs):
                if i == len(oversized_gifs) - 1 and not remaining_urls:
                    # This is the last oversized GIF and there are no remaining images
                    view = discord.ui.View()
                    reddit_post_button = create_button("Reddit Post", f"https://www.reddit.com{processing_submission.permalink}", button_visibility)
                    if reddit_post_button:
                        view.add_item(reddit_post_button)
                    await embed_oversized_gif(channel, None, view, gif_url)
                else:
                    await embed_oversized_gif(channel, None, None, gif_url)

            # Send remaining images in carousel with the Reddit Post button
            if remaining_urls:
                image_view = discord.ui.View()
                image_reddit_post_button = create_button("Reddit Post", f"https://www.reddit.com{processing_submission.permalink}", button_visibility)
                if image_reddit_post_button:
                    image_view.add_item(image_reddit_post_button)
                await send_image_carousel(channel, remaining_urls, image_view)

        return  # Exit the function after handling the post
    else:
        image_url = extract_image_url(processing_submission)
        if image_url:
            embed.set_image(url=image_url)
        else:
            reddit_video_url, thumbnail_url = await get_reddit_video_url(processing_submission)
            if reddit_video_url:
                video_content = await download_video(reddit_video_url, MAX_VIDEO_SIZE)
                if video_content:
                    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as temp_file:
                        temp_file.write(video_content)
                        temp_file_path = temp_file.name

                    file = discord.File(temp_file_path, filename="reddit_video.mp4")
                    embed.add_field(name="Reddit Video", value=reddit_video_url)
                    
                    # Create a new view for the video message
                    video_view = discord.ui.View()
                    reddit_post_button = create_button("Reddit Post", f"https://www.reddit.com{processing_submission.permalink}", button_visibility)
                    if reddit_post_button:
                        video_view.add_item(reddit_post_button)
                    watch_video_button = create_button("Watch Video", reddit_video_url, button_visibility)
                    if watch_video_button:
                        video_view.add_item(watch_video_button)
                    
                    # Send the embed message without buttons
                    add_footer_and_crosspost_info(embed, processing_submission, submission, original_post_data if processing_submission != submission else None)
                    await channel.send(embed=embed)
                    
                    # Send the video file with buttons in a separate message
                    await channel.send(file=file, view=video_view)
                    
                    os.unlink(temp_file_path)
                    return  # Exit the function after sending both messages
                else:
                    embed.add_field(name="Reddit Video", value=reddit_video_url)
                    embed.set_image(url=thumbnail_url if thumbnail_url else reddit_video_url)
                    watch_video_button = create_button("Watch Video", reddit_video_url, button_visibility)
                    if watch_video_button:
                        view.add_item(watch_video_button)            
                    
                    # Add the new message for large files
                    embed.add_field(name="Note", value="Due to Discord upload limits, you'll need to view this video on Reddit or via the Reddit App using the link provided.", inline=False)
            else:
                youtube_id = extract_video_id(processing_submission.url)
                if youtube_id:
                    youtube_title, thumbnail_url = await get_youtube_info(youtube_id)
                    embed.add_field(name=youtube_title, value=f"https://www.youtube.com/watch?v={youtube_id}")
                    if thumbnail_url:
                        embed.set_image(url=thumbnail_url)
                    youtube_button = create_button("YouTube Link", f"https://www.youtube.com/watch?v={youtube_id}", button_visibility)
                    if youtube_button:
                        view.add_item(youtube_button)
                else:
                    if '/gallery/' in processing_submission.url:
                        embed.add_field(name="Image Gallery Link", value=processing_submission.url)
                        gallery_button = create_button("Image Gallery", processing_submission.url, button_visibility)
                        if gallery_button:
                            view.add_item(gallery_button)
                    else:
                        embed.add_field(name="Link", value=processing_submission.url)
                        web_link_button = create_button("Web Link", processing_submission.url, button_visibility)
                        if web_link_button:
                            view.add_item(web_link_button)

    # Send the message only if it hasn't been sent already
    if not message_sent:
        # Add footer and crosspost information
        add_footer_and_crosspost_info(embed, processing_submission, submission, original_post_data if processing_submission != submission else None)

        # If it's a crosspost, ensure the crosspost info is the last field
        if processing_submission != submission:
            crosspost_field = None
            for field in embed.fields:
                if field.name == "Crosspost Info":
                    crosspost_field = field
                    embed.remove_field(embed.fields.index(field))
                    break
            if crosspost_field:
                embed.add_field(name=crosspost_field.name, value=crosspost_field.value, inline=False)

        print(f"Final Embed data before sending for submission {processing_submission.id}: {embed.to_dict()}")
        if hasattr(channel, 'thread'):
            await channel.thread.send(embed=embed, view=view)
        else:
            await channel.send(embed=embed, view=view)

    logger.debug(f"Image set: {bool(embed.image)}")
    logger.debug(f"Embed image URL: {embed.image.url if embed.image else 'No image set'}")

async def process_forum_subscription(reddit, subreddit, channel_id, thread_id, last_check, last_submission_id):
    forum_channel = bot.get_channel(channel_id)
    if forum_channel:
        try:
            await sync_forum_tags_function(forum_channel)
            subreddit_obj = await reddit.subreddit(subreddit)
            last_check_dt = datetime.fromisoformat(last_check).replace(tzinfo=timezone.utc)
            new_submissions = await fetch_new_submissions(subreddit_obj, last_check_dt, limit=10)
            
            if channel_id not in processed_submissions:
                processed_submissions[channel_id] = set()
            
            if thread_id:  # Single thread for all posts
                thread = bot.get_channel(thread_id)
                if thread:
                    for submission in reversed(new_submissions):
                        if submission.id not in processed_submissions[channel_id]:
                            processed_submissions[channel_id].add(submission.id)
                            tag = await get_flair_as_tag(submission, forum_channel)
                            if tag and tag not in thread.applied_tags:
                                new_tags = list(thread.applied_tags) + [tag]
                                if len(new_tags) > 5:
                                    new_tags = new_tags[-5:]  # Keep only the 5 most recent tags
                                await thread.edit(applied_tags=new_tags)
                            await process_submission(submission, thread, get_button_visibility())
                        else:
                            logger.info(f"Skipping already processed submission {submission.id} for channel {channel_id}")
            else:  # New thread for each post
                for submission in reversed(new_submissions):
                    if submission.id not in processed_submissions[channel_id]:
                        processed_submissions[channel_id].add(submission.id)
                        tag = await get_flair_as_tag(submission, forum_channel)
                        thread_name = truncate_string(submission.title, 100)
                        thread = await forum_channel.create_thread(
                            name=thread_name, 
                            content=f"New post from r/{subreddit}", 
                            applied_tags=[tag] if tag else None
                        )
                        await process_submission(submission, thread, get_button_visibility())
                    else:
                        logger.info(f"Skipping already processed submission {submission.id} for channel {channel_id}")
            
            if new_submissions:
                new_last_check = datetime.now(timezone.utc).isoformat()
                c.execute("UPDATE forum_subscriptions SET last_check = ?, last_submission_id = ? WHERE subreddit = ? AND channel_id = ?",
                          (new_last_check, new_submissions[0].id, subreddit, channel_id))
                conn.commit()
        except Exception as e:
            # Check if the error is a known issue (like a 500 HTTP response)
            if "500" in str(e):
                logger.error(f"Error processing forum subscription for r/{subreddit}: Received a 500 HTTP response.")
            else:
                logger.error(f"Error processing forum subscription for r/{subreddit}: {str(e)}", exc_info=False)

async def process_individual_forum_subscription(reddit, subreddit, channel_id, last_check):
    forum_channel = bot.get_channel(channel_id)
    if forum_channel is None:
        logger.warning(f"Forum not found: {channel_id}")
        return

    try:
        await sync_forum_tags_function(forum_channel)
        subreddit_obj = await reddit.subreddit(subreddit)
        last_check_dt = datetime.fromisoformat(last_check).replace(tzinfo=timezone.utc)
        new_submissions = await fetch_new_submissions(subreddit_obj, last_check_dt, limit=10)
        
        if channel_id not in processed_submissions:
            processed_submissions[channel_id] = set()
        
        for submission in reversed(new_submissions):
            if submission.id not in processed_submissions[channel_id]:
                processed_submissions[channel_id].add(submission.id)
                thread_name = truncate_string(submission.title, 100)
                
                # Get the primary image URL
                image_url = await get_primary_image_url(submission)
                
                tag = await get_flair_as_tag(submission, forum_channel)
                applied_tags = [tag] if tag else []
                
                if image_url:
                    # If we have a direct image URL, use it in the thread creation
                    thread = await forum_channel.create_thread(
                        name=thread_name,
                        content=image_url,
                        applied_tags=applied_tags[:5]  # Limit to 5 tags
                    )
                else:
                    # If no direct image, fall back to the embed method
                    simple_embed = await create_simple_reddit_embed(submission)
                    thread = await forum_channel.create_thread(
                        name=thread_name,
                        content=f"New post from r/{subreddit}",
                        embed=simple_embed,
                        applied_tags=applied_tags[:5]  # Limit to 5 tags
                    )
                
                await process_submission(submission, thread, get_button_visibility())
                
                logger.info(f"Created new thread for r/{subreddit} post: {submission.title}")
            else:
                logger.info(f"Skipping already processed submission {submission.id} for channel {channel_id}")
        
        if new_submissions:
            new_last_check = datetime.now(timezone.utc).isoformat()
            c.execute("UPDATE individual_forum_subscriptions SET last_check = ? WHERE subreddit = ? AND channel_id = ?",
                      (new_last_check, subreddit, channel_id))
            conn.commit()
        else:
            logger.info(f"No new submissions found for r/{subreddit}")

    except asyncprawcore.exceptions.NotFound:
        logger.error(f"Subreddit r/{subreddit} not found. Consider removing this subscription.")
    except asyncprawcore.exceptions.Forbidden:
        logger.error(f"Access to r/{subreddit} is forbidden. Consider removing this subscription.")
    except asyncprawcore.exceptions.TooManyRequests:
        logger.warning(f"Rate limit hit while processing r/{subreddit}. Waiting before next request.")
        await asyncio.sleep(60)  # Wait for 60 seconds before next request
    except Exception as e:
        logger.exception(f"Error processing individual forum subscription for {subreddit}: {str(e)}")

async def update_tracking(subreddit, channel_id, last_check, last_submission_id):
    c.execute('''INSERT OR REPLACE INTO submission_tracking 
                 (subreddit, channel_id, last_check, last_submission_id) 
                 VALUES (?, ?, ?, ?)''', 
              (subreddit, channel_id, last_check.strftime("%Y-%m-%d %H:%M:%S.%f"), last_submission_id))
    conn.commit()

async def get_tracking(subreddit, channel_id):
    c.execute('''SELECT last_check, last_submission_id FROM submission_tracking 
                 WHERE subreddit = ? AND channel_id = ?''', 
              (subreddit, channel_id))
    result = c.fetchone()
    if result:
        return datetime.strptime(result[0], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc), result[1]
    return datetime.now(timezone.utc), None

async def download_video(url, max_size):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                content = await response.read()
                if len(content) <= max_size:
                    return content
    return None

async def process_reddit_video(submission, channel, button_visibility):
    if (hasattr(submission, 'media_metadata') and 
        any(item.get('e') == 'RedditVideo' for item in submission.media_metadata.values())):
        
        video_url_matches = re.findall(r'https://reddit\.com/link/[^/]+/video/[^/]+/player', submission.selftext)
        
        if video_url_matches:
            primary_video_url = video_url_matches[0]
            
            embed = discord.Embed(
                title=truncate_string(submission.title, 256),
                url=f"https://www.reddit.com{submission.permalink}",
                color=discord.Color.green()
            )
            
            author_name = submission.author.name if submission.author else "[deleted]"
            author_profile_url = f"https://www.reddit.com/user/{author_name}" if submission.author else None
            author_icon_url = None
            if submission.author:
                try:
                    await submission.author.load()
                    author_icon_url = submission.author.icon_img
                except Exception as e:
                    print(f"Error fetching author details: {e}")
            embed.set_author(name=author_name, url=author_profile_url, icon_url=author_icon_url)
            
            if submission.selftext:
                cleaned_text = clean_video_post_text(submission.selftext, video_url_matches)
                if cleaned_text and cleaned_text.strip() != '&#x200B;':
                    embed.description = cleaned_text[:4000]
            
            embed.add_field(name="Reddit Video", value="This type of Reddit video(s) can only be viewed online or via the Reddit App.", inline=False)
            
            video_links = "\n".join(video_url_matches)
            embed.add_field(name="Video Link(s)", value=video_links, inline=False)
            
            embed.set_footer(text=f"r/{submission.subreddit.display_name}")
            embed.timestamp = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)
            
            view = discord.ui.View()
            
            reddit_post_button = create_button("Reddit Post", f"https://www.reddit.com{submission.permalink}", button_visibility)
            if reddit_post_button:
                view.add_item(reddit_post_button)
            
            watch_video_button = create_button("Watch Video", primary_video_url, button_visibility)
            if watch_video_button:
                view.add_item(watch_video_button)
            
            # Final check before sending
            if embed.description == '&#x200B;':
                embed.description = None
            
            if hasattr(channel, 'send'):
                await channel.send(embed=embed, view=view)
            elif hasattr(channel, 'thread'):
                await channel.thread.send(embed=embed, view=view)
            else:
                print(f"Unexpected channel type: {type(channel)}")
                return False
            
            return True
    
    return False  # Indicate that this submission wasn't handled by this function

async def embed_oversized_gif(channel, embed, view, gif_url):
    print(f"Embedding oversized GIF: {gif_url}")
    new_embed = discord.Embed(color=discord.Color.green())
    new_embed.set_image(url=gif_url)
    try:
        await channel.send(embed=new_embed, view=view)
        print("Successfully sent oversized GIF embed")
        return True
    except discord.HTTPException:
        print("Failed to embed GIF, adding link to embed")
        new_embed.add_field(name="Oversized GIF", value=f"This GIF may have exceeded the upload size limit, but should be viewable via this link if the direct embed does not work:\n{gif_url}", inline=False)
        await channel.send(embed=new_embed, view=view)
        return True
    return False

async def process_subscription(reddit, subreddit, channel_id, last_check, last_submission_id):
    channel = bot.get_channel(channel_id)
    processed_ids = set()
    if channel:
        try:
            subreddit_obj = await reddit.subreddit(subreddit)
            last_check_dt = datetime.fromisoformat(last_check).replace(tzinfo=timezone.utc)
            new_submissions = await fetch_new_submissions(subreddit_obj, last_check_dt, limit=10)
            
            if channel_id not in processed_submissions:
                processed_submissions[channel_id] = set()
            
            for submission in reversed(new_submissions):
                if submission.id not in processed_submissions[channel_id]:
                    processed_submissions[channel_id].add(submission.id)
                    processed_ids.add(submission.id)
                    logger.info(f"Processing submission {submission.id} with flair '{submission.link_flair_text}' for subreddit {subreddit}")
                    
                    # Log flair settings before processing
                    max_flairs, flair_enabled, blacklisted_flairs = get_flair_settings(channel_id)
                    logger.info(f"Current flair settings for channel {channel_id}: max_flairs={max_flairs}, flair_enabled={flair_enabled}, blacklisted_flairs={blacklisted_flairs}")
                    
                    await process_submission(submission, channel, get_button_visibility())
                else:
                    logger.info(f"Skipping already processed submission {submission.id} for subreddit {subreddit}")
            
            if new_submissions:
                new_last_check = datetime.now(timezone.utc).isoformat()
                c.execute("UPDATE subscriptions SET last_check = ?, last_submission_id = ? WHERE subreddit = ? AND channel_id = ?",
                          (new_last_check, new_submissions[0].id, subreddit, channel_id))
                conn.commit()
        except Exception as e:
            print(f"Error processing subreddit {subreddit}: {e}")
    return processed_ids

# 10. Database Management Functions
# ================================

def kill_sqlite_connections():
    try:
        # This command works on Linux systems
        os.system("fuser -k subscriptions.db")
        return True
    except Exception as e:
        logger.error(f"Error killing SQLite connections: {e}")
        return False

# 11. Bot Commands
# ===============

@bot.tree.command(name="check_flair_settings", description="Check current flair settings for a forum")
async def check_flair_settings(interaction: discord.Interaction, forum: discord.ForumChannel):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) checking flair settings for forum {forum.name} ({forum.id})")
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        max_flairs, flair_enabled, blacklisted_flairs = get_flair_settings(forum.id)
        
        response = f"Current flair settings for {forum.name}:\n"
        response += f"Flair-to-tag conversion: {'Enabled' if flair_enabled else 'Disabled'}\n"
        response += f"Max flairs: {max_flairs}\n"
        response += f"Blacklisted flairs: {', '.join(blacklisted_flairs) if blacklisted_flairs else 'None'}"
        
        logger.info(f"Flair settings for forum {forum.name} ({forum.id}): max_flairs={max_flairs}, flair_enabled={flair_enabled}, blacklisted_flairs={blacklisted_flairs}")
        await interaction.followup.send(response, ephemeral=True)
    except Exception as e:
        logger.error(f"Error checking flair settings for forum {forum.name} ({forum.id}): {str(e)}", exc_info=True)
        await interaction.followup.send("An error occurred while checking flair settings. Please try again later.", ephemeral=True)

@bot.tree.command(name="list_forum_tags", description="List all tags in a forum channel")
async def list_forum_tags(interaction: discord.Interaction, forum: discord.ForumChannel):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) listing tags for forum {forum.name} ({forum.id})")
    
    await interaction.response.defer(ephemeral=True)
    try:
        tags = forum.available_tags
        if tags:
            tag_list = "\n".join([f"- {tag.name}" for tag in tags])
            logger.info(f"Listed {len(tags)} tags for forum {forum.name} ({forum.id})")
            await interaction.followup.send(f"Tags in {forum.name}:\n{tag_list}", ephemeral=True)
        else:
            logger.info(f"No tags found in forum {forum.name} ({forum.id})")
            await interaction.followup.send(f"No tags found in {forum.name}", ephemeral=True)
    except Exception as e:
        logger.error(f"Error listing tags for forum {forum.name} ({forum.id}): {str(e)}", exc_info=True)
        await interaction.followup.send("An error occurred while listing forum tags. Please try again later.", ephemeral=True)

@bot.tree.command(name="remove_forum_tag", description="Remove a tag from a forum channel")
@has_debug_role()
async def remove_forum_tag(interaction: discord.Interaction, forum: discord.ForumChannel, tag_name: str):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) attempting to remove tag '{tag_name}' from forum {forum.name} ({forum.id})")
    
    await interaction.response.defer()

    # Check if the tag exists
    tag_to_remove = discord.utils.get(forum.available_tags, name=tag_name)
    if not tag_to_remove:
        logger.warning(f"Tag '{tag_name}' not found in forum {forum.name} ({forum.id})")
        await interaction.followup.send(f"Tag '{tag_name}' not found in the forum.", ephemeral=True)
        return

    # Remove the tag
    new_tags = [tag for tag in forum.available_tags if tag.name != tag_name]
    try:
        await forum.edit(available_tags=new_tags)
        logger.info(f"Successfully removed tag '{tag_name}' from forum {forum.name} ({forum.id})")
        await interaction.followup.send(f"Successfully removed tag '{tag_name}' from the forum.", ephemeral=True)
    except discord.Forbidden:
        logger.error(f"Forbidden: Bot lacks permission to remove tag '{tag_name}' from forum {forum.name} ({forum.id})")
        await interaction.followup.send("I don't have permission to edit forum tags.", ephemeral=True)
    except discord.HTTPException as e:
        logger.error(f"HTTP error occurred while removing tag '{tag_name}' from forum {forum.name} ({forum.id}): {str(e)}", exc_info=True)
        await interaction.followup.send(f"An error occurred while removing the tag: {str(e)}", ephemeral=True)

    # Remove the tag from the database
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("DELETE FROM forum_tags WHERE channel_id = ? AND tag_name = ?", (forum.id, tag_name))
            conn.commit()
        logger.info(f"Removed tag '{tag_name}' from database for forum {forum.name} ({forum.id})")
    except Exception as e:
        logger.error(f"Database error while removing tag '{tag_name}' for forum {forum.name} ({forum.id}): {str(e)}", exc_info=True)
        await interaction.followup.send("An error occurred while updating the database. The tag may not have been fully removed.", ephemeral=True)

@bot.tree.command(name="sync_forum_tags", description="Sync forum tags with the current blacklist")
async def sync_forum_tags(interaction: discord.Interaction, forum: discord.ForumChannel):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) initiating forum tag sync for {forum.name} ({forum.id})")
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        removed_tags = await sync_forum_tags_function(forum)
        
        if removed_tags:
            logger.info(f"Removed tags {', '.join(removed_tags)} from forum {forum.name} ({forum.id}) to match the blacklist")
            await interaction.followup.send(f"Removed tags {', '.join(removed_tags)} from the forum to match the blacklist.", ephemeral=True)
        else:
            logger.info(f"No tags needed to be removed from forum {forum.name} ({forum.id}). Tags are in sync with the blacklist")
            await interaction.followup.send("No tags needed to be removed. Forum tags are in sync with the blacklist.", ephemeral=True)
    except Exception as e:
        logger.error(f"Error syncing forum tags for {forum.name} ({forum.id}): {str(e)}", exc_info=True)
        await interaction.followup.send("An error occurred while syncing forum tags. Please try again later.", ephemeral=True)

@bot.tree.command(name="check_database", description="Check the raw database content for a forum")
@has_debug_role()
async def check_database(interaction: discord.Interaction, forum: discord.ForumChannel):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) checking database for forum {forum.name} ({forum.id})")
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        with sqlite3.connect('subscriptions.db') as conn:
            c = conn.cursor()
            c.execute("PRAGMA table_info(forum_flair_settings)")
            columns = [column[1] for column in c.fetchall()]
            
            c.execute("SELECT * FROM forum_flair_settings WHERE channel_id = ?", (forum.id,))
            result = c.fetchone()
        
        if result:
            logger.info(f"Database entry found for forum {forum.name} ({forum.id})")
            response = f"Raw database content for {forum.name}:\n"
            for col, val in zip(columns, result):
                response += f"{col}: {val}\n"
        else:
            logger.warning(f"No database entry found for forum {forum.name} ({forum.id})")
            response = f"No database entry found for {forum.name}"
        
        await interaction.followup.send(response, ephemeral=True)
    except sqlite3.Error as e:
        logger.error(f"SQLite error while checking database for forum {forum.name} ({forum.id}): {str(e)}", exc_info=True)
        await interaction.followup.send("An error occurred while checking the database. Please try again later.", ephemeral=True)
    except Exception as e:
        logger.error(f"Unexpected error while checking database for forum {forum.name} ({forum.id}): {str(e)}", exc_info=True)
        await interaction.followup.send("An unexpected error occurred. Please try again later.", ephemeral=True)

@bot.tree.command(name="vacuum_database", description="Perform VACUUM on the database")
@has_debug_role()
async def vacuum_database(interaction: discord.Interaction):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) initiating database VACUUM")
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        start_time = time.time()
        with sqlite3.connect('subscriptions.db') as conn:
            conn.execute("VACUUM")
        end_time = time.time()
        
        duration = round(end_time - start_time, 2)
        await interaction.followup.send(f"Database VACUUM completed successfully in {duration} seconds.", ephemeral=True)
        logger.info(f"Database VACUUM completed successfully in {duration} seconds")
    except sqlite3.Error as e:
        error_message = f"An error occurred while performing VACUUM: {str(e)}"
        await interaction.followup.send(error_message, ephemeral=True)
        logger.error(f"SQLite error during VACUUM: {str(e)}", exc_info=True)
    except Exception as e:
        error_message = f"An unexpected error occurred while performing VACUUM: {str(e)}"
        await interaction.followup.send(error_message, ephemeral=True)
        logger.error(f"Unexpected error during VACUUM: {str(e)}", exc_info=True)

@bot.tree.command(name="check_database_integrity", description="Check the integrity of the database")
@has_debug_role()
async def check_database_integrity(interaction: discord.Interaction):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) initiating database integrity check")
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        start_time = time.time()
        with sqlite3.connect('subscriptions.db') as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchone()
        end_time = time.time()
        
        duration = round(end_time - start_time, 2)
        
        if result[0] == "ok":
            message = f"Database integrity check passed in {duration} seconds."
            await interaction.followup.send(message, ephemeral=True)
            logger.info(message)
        else:
            message = f"Database integrity check failed in {duration} seconds. Result: {result[0]}"
            await interaction.followup.send(message, ephemeral=True)
            logger.error(message)
    except sqlite3.Error as e:
        error_message = f"SQLite error occurred while checking database integrity: {str(e)}"
        await interaction.followup.send(error_message, ephemeral=True)
        logger.error(error_message, exc_info=True)
    except Exception as e:
        error_message = f"Unexpected error occurred while checking database integrity: {str(e)}"
        await interaction.followup.send(error_message, ephemeral=True)
        logger.error(error_message, exc_info=True)

@bot.tree.command(name="check_database_lock", description="Check if the database is locked")
@has_debug_role()
async def check_database_lock(interaction: discord.Interaction):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) checking database lock status")
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        is_locked = is_database_locked()
        status = 'locked' if is_locked else 'not locked'
        message = f"Database is {status}"
        
        await interaction.followup.send(message, ephemeral=True)
        logger.info(f"Database lock check completed. Result: {message}")
    except Exception as e:
        error_message = f"An error occurred while checking database lock status: {str(e)}"
        await interaction.followup.send(error_message, ephemeral=True)
        logger.error(f"Error during database lock check: {str(e)}", exc_info=True)

@bot.tree.command(name="check_database_permissions", description="Check if the database file is writable")
@has_debug_role()
async def check_database_permissions(interaction: discord.Interaction):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) checking database permissions")
    
    await interaction.response.defer(ephemeral=True)
    
    db_path = 'subscriptions.db'
    try:
        # Check if file exists
        if not os.path.exists(db_path):
            message = f"Database file {db_path} does not exist."
            logger.warning(message)
            await interaction.followup.send(message, ephemeral=True)
            return

        # Check if file is writable
        if os.access(db_path, os.W_OK):
            message = f"Database file {db_path} is writable."
            logger.info(message)
            await interaction.followup.send(message, ephemeral=True)
        else:
            message = f"Database file {db_path} is not writable."
            logger.warning(message)
            await interaction.followup.send(message, ephemeral=True)

        # Get file permissions
        permissions = oct(os.stat(db_path).st_mode)[-3:]
        message = f"Database file permissions: {permissions}"
        logger.info(message)
        await interaction.followup.send(message, ephemeral=True)

    except Exception as e:
        error_message = f"An error occurred while checking database permissions: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(error_message, ephemeral=True)

@bot.tree.command(name="force_database_write", description="Force a write to the database")
@has_debug_role()
async def force_database_write(interaction: discord.Interaction):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) initiating forced database write")
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        start_time = time.time()
        with sqlite3.connect('subscriptions.db') as conn:
            c = conn.cursor()
            c.execute("BEGIN")
            c.execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, value TEXT)")
            test_value = f"Test value at {time.time()}"
            c.execute("INSERT INTO test_table (value) VALUES (?)", (test_value,))
            conn.commit()
        end_time = time.time()
        
        duration = round(end_time - start_time, 2)
        message = f"Forced database write completed successfully in {duration} seconds."
        await interaction.followup.send(message, ephemeral=True)
        logger.info(f"{message} Inserted value: {test_value}")
    except sqlite3.Error as e:
        error_message = f"SQLite error occurred while forcing database write: {str(e)}"
        await interaction.followup.send(error_message, ephemeral=True)
        logger.error(error_message, exc_info=True)
    except Exception as e:
        error_message = f"Unexpected error occurred while forcing database write: {str(e)}"
        await interaction.followup.send(error_message, ephemeral=True)
        logger.error(error_message, exc_info=True)

@bot.tree.command(name="query_database", description="Directly query the database for flair settings")
@has_debug_role()
async def query_database(interaction: discord.Interaction, forum: discord.ForumChannel):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) querying database for forum {forum.name} ({forum.id})")
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        start_time = time.time()
        with sqlite3.connect('subscriptions.db', isolation_level=None) as conn:
            conn.execute("PRAGMA query_only = ON")
            conn.execute("PRAGMA cache_size = 0")
            c = conn.cursor()
            c.execute("SELECT * FROM forum_flair_settings WHERE channel_id = ?", (forum.id,))
            result = c.fetchone()
        end_time = time.time()
        
        duration = round(end_time - start_time, 2)
        
        if result:
            response = f"Direct database query result for {forum.name}:\n"
            response += f"Channel ID: {result[1]}\n"
            response += f"Max flairs: {result[2]}\n"
            response += f"Flair enabled: {bool(result[3])}\n"
            response += f"Blacklisted flairs: {result[4]}"
            logger.info(f"Database query for forum {forum.name} ({forum.id}) completed in {duration} seconds. Result found.")
        else:
            response = f"No database entry found for {forum.name}"
            logger.warning(f"Database query for forum {forum.name} ({forum.id}) completed in {duration} seconds. No result found.")
        
        await interaction.followup.send(response, ephemeral=True)
        logger.debug(f"Full query result: {result}")
    except sqlite3.Error as e:
        error_message = f"SQLite error occurred while querying the database: {str(e)}"
        await interaction.followup.send(error_message, ephemeral=True)
        logger.error(f"Error during direct database query for forum {forum.name} ({forum.id}): {str(e)}", exc_info=True)
    except Exception as e:
        error_message = f"Unexpected error occurred while querying the database: {str(e)}"
        await interaction.followup.send(error_message, ephemeral=True)
        logger.error(f"Unexpected error during direct database query for forum {forum.name} ({forum.id}): {str(e)}", exc_info=True)

@bot.tree.command(name="check_wal_mode", description="Check and enable WAL mode for the database")
@has_debug_role()
async def check_wal_mode(interaction: discord.Interaction):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) checking and potentially enabling WAL mode")
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        with sqlite3.connect('subscriptions.db') as conn:
            c = conn.cursor()
            c.execute("PRAGMA journal_mode")
            current_mode = c.fetchone()[0]
            
            logger.info(f"Current database journal mode: {current_mode}")
            
            if current_mode.upper() != 'WAL':
                c.execute("PRAGMA journal_mode=WAL")
                new_mode = c.fetchone()[0]
                message = f"Database mode changed from {current_mode} to {new_mode}"
                logger.info(message)
                await interaction.followup.send(message, ephemeral=True)
            else:
                message = "Database is already in WAL mode"
                logger.info(message)
                await interaction.followup.send(message, ephemeral=True)
    except sqlite3.Error as e:
        error_message = f"SQLite error occurred while checking/enabling WAL mode: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(error_message, ephemeral=True)
    except Exception as e:
        error_message = f"Unexpected error occurred while checking/enabling WAL mode: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An unexpected error occurred: {str(e)}", ephemeral=True)

@bot.tree.command(name="force_checkpoint", description="Force a checkpoint and vacuum the database")
@has_debug_role()
async def force_checkpoint(interaction: discord.Interaction):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) initiating forced checkpoint and vacuum")
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        start_time = time.time()
        with sqlite3.connect('subscriptions.db') as conn:
            c = conn.cursor()
            logger.info("Executing WAL checkpoint")
            c.execute("PRAGMA wal_checkpoint(FULL)")
            logger.info("Executing VACUUM")
            c.execute("VACUUM")
        end_time = time.time()
        
        duration = round(end_time - start_time, 2)
        message = f"Database checkpoint and vacuum completed successfully in {duration} seconds"
        logger.info(message)
        await interaction.followup.send(message, ephemeral=True)
    except sqlite3.Error as e:
        error_message = f"SQLite error occurred during checkpoint/vacuum: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An error occurred: {str(e)}", ephemeral=True)
    except Exception as e:
        error_message = f"Unexpected error occurred during checkpoint/vacuum: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An unexpected error occurred: {str(e)}", ephemeral=True)

@bot.tree.command(name="check_db_lock_status", description="Check if the database is locked")
@has_debug_role()
async def check_db_lock_status(interaction: discord.Interaction):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) checking database lock status")
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        start_time = time.time()
        is_locked = is_database_locked()
        end_time = time.time()
        
        duration = round(end_time - start_time, 2)
        status = 'locked' if is_locked else 'not locked'
        message = f"Database is {status} (check completed in {duration} seconds)"
        
        logger.info(f"Database lock check result: {status}. Check duration: {duration} seconds")
        await interaction.followup.send(message, ephemeral=True)
    except Exception as e:
        error_message = f"Error occurred while checking database lock status: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An error occurred: {str(e)}", ephemeral=True)

@bot.tree.command(name="force_close_connections", description="Force close all database connections")
@has_debug_role()
async def force_close_connections(interaction: discord.Interaction):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) initiating force close of all database connections")
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        start_time = time.time()
        sqlite3.connect(':memory:').close()
        end_time = time.time()
        
        duration = round(end_time - start_time, 2)
        message = f"All database connections have been forcibly closed. Operation took {duration} seconds."
        
        logger.info(message)
        await interaction.followup.send(message, ephemeral=True)
    except sqlite3.Error as e:
        error_message = f"SQLite error occurred while closing connections: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An error occurred while closing connections: {str(e)}", ephemeral=True)
    except Exception as e:
        error_message = f"Unexpected error occurred while closing connections: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An unexpected error occurred while closing connections: {str(e)}", ephemeral=True)

@bot.tree.command(name="kill_db_connections", description="Kill all database connections")
@has_debug_role()
async def kill_db_connections(interaction: discord.Interaction):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) initiating kill of all database connections")
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        start_time = time.time()
        success = kill_sqlite_connections()
        end_time = time.time()
        
        duration = round(end_time - start_time, 2)
        
        if success:
            message = f"All database connections have been forcibly terminated. Operation took {duration} seconds."
            logger.info(message)
            await interaction.followup.send(message, ephemeral=True)
        else:
            message = f"Failed to terminate database connections. Operation took {duration} seconds."
            logger.warning(message)
            await interaction.followup.send(message, ephemeral=True)
    except Exception as e:
        error_message = f"Unexpected error occurred while killing database connections: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An unexpected error occurred: {str(e)}", ephemeral=True)

@bot.tree.command(name="recreate_database", description="Recreate the database (USE WITH EXTREME CAUTION)")
@has_debug_role()
async def recreate_database(interaction: discord.Interaction):
    logger.warning(f"User {interaction.user.name} ({interaction.user.id}) initiating database recreation")
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        start_time = time.time()

        # Close all connections
        logger.info("Attempting to kill all SQLite connections")
        kill_sqlite_connections()

        # Rename the old database
        logger.info("Renaming old database to 'subscriptions_backup.db'")
        os.rename('subscriptions.db', 'subscriptions_backup.db')

        # Create a new database
        logger.info("Creating new 'subscriptions.db' database")
        conn = sqlite3.connect('subscriptions.db')
        c = conn.cursor()

        # Recreate your tables here
        logger.info("Recreating tables in the new database")
        c.execute('''CREATE TABLE IF NOT EXISTS forum_flair_settings
                     (channel_id INTEGER PRIMARY KEY, max_flairs INTEGER, flair_enabled INTEGER, blacklisted_flairs TEXT)''')

        # Add more table creations as needed
        # logger.info("Creating additional table: table_name")
        # c.execute('''CREATE TABLE IF NOT EXISTS table_name ...''')

        conn.commit()
        conn.close()

        end_time = time.time()
        duration = round(end_time - start_time, 2)

        success_message = f"Database has been recreated in {duration} seconds. Old database backed up as 'subscriptions_backup.db'."
        logger.info(success_message)
        await interaction.followup.send(success_message, ephemeral=True)

    except Exception as e:
        error_message = f"Error occurred while recreating database: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An error occurred while recreating the database: {str(e)}", ephemeral=True)

        # Attempt to restore the old database if an error occurred
        try:
            logger.info("Attempting to restore the old database due to error")
            os.rename('subscriptions_backup.db', 'subscriptions.db')
            logger.info("Old database restored")
        except Exception as restore_error:
            logger.error(f"Failed to restore old database: {str(restore_error)}", exc_info=True)

@bot.tree.command(name="force_update_blacklist", description="Force update the blacklist for a forum")
@has_debug_role()
async def force_update_blacklist(interaction: discord.Interaction, forum: discord.ForumChannel, new_blacklist: str):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) initiating forced blacklist update for forum {forum.name} ({forum.id})")
    
    await interaction.response.defer(ephemeral=True)
    
    start_time = time.time()
    
    with sqlite3.connect('subscriptions.db') as conn:
        c = conn.cursor()
        try:
            new_blacklist_list = new_blacklist.split(',')
            logger.info(f"Attempting to update blacklist for forum {forum.name} ({forum.id}) with: {new_blacklist_list}")
            
            c.execute("UPDATE forum_flair_settings SET blacklisted_flairs = ? WHERE channel_id = ?", 
                      (json.dumps(new_blacklist_list), forum.id))
            conn.commit()
            
            # Verify the update
            c.execute("SELECT blacklisted_flairs FROM forum_flair_settings WHERE channel_id = ?", (forum.id,))
            result = c.fetchone()
            if result:
                updated_blacklist = json.loads(result[0])
                end_time = time.time()
                duration = round(end_time - start_time, 2)
                
                logger.info(f"Forced update successful for forum {forum.name} ({forum.id}). New blacklist: {updated_blacklist}. Duration: {duration} seconds")
                await interaction.followup.send(f"Blacklist forcefully updated for {forum.name}: {', '.join(updated_blacklist)}", ephemeral=True)
            else:
                logger.warning(f"No settings found for forum {forum.name} ({forum.id}) after update attempt")
                await interaction.followup.send(f"No settings found for {forum.name} after update", ephemeral=True)
        except sqlite3.Error as e:
            error_message = f"SQLite error in force_update_blacklist for forum {forum.name} ({forum.id}): {str(e)}"
            logger.error(error_message, exc_info=True)
            await interaction.followup.send(f"An error occurred: {str(e)}", ephemeral=True)
        except Exception as e:
            error_message = f"Unexpected error in force_update_blacklist for forum {forum.name} ({forum.id}): {str(e)}"
            logger.error(error_message, exc_info=True)
            await interaction.followup.send(f"An unexpected error occurred: {str(e)}", ephemeral=True)

    end_time = time.time()
    logger.info(f"force_update_blacklist command completed in {round(end_time - start_time, 2)} seconds")

@bot.tree.command(name="check_active_transactions", description="Check for active transactions in the database")
@has_debug_role()
async def check_active_transactions(interaction: discord.Interaction):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) initiating check for active database transactions")
    
    await interaction.response.defer(ephemeral=True)
    
    start_time = time.time()
    
    try:
        with sqlite3.connect('subscriptions.db') as conn:
            c = conn.cursor()
            c.execute("PRAGMA query_only = ON")
            logger.debug("Executing query to check for active transactions")
            c.execute("SELECT * FROM sqlite_master WHERE type = 'table' AND name = 'sqlite_master'")
            result = c.fetchone()
            
            if result:
                message = "There are active transactions in the database."
                logger.warning(message)
                await interaction.followup.send(message, ephemeral=True)
            else:
                message = "No active transactions found in the database."
                logger.info(message)
                await interaction.followup.send(message, ephemeral=True)
    
    except sqlite3.Error as e:
        error_message = f"SQLite error occurred while checking for active transactions: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An error occurred while checking for active transactions: {str(e)}", ephemeral=True)
    except Exception as e:
        error_message = f"Unexpected error occurred while checking for active transactions: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An unexpected error occurred: {str(e)}", ephemeral=True)
    
    end_time = time.time()
    duration = round(end_time - start_time, 2)
    logger.info(f"check_active_transactions command completed in {duration} seconds")

@bot.tree.command(name="check_db_processes", description="Check processes using the database")
@has_debug_role()
async def check_db_processes(interaction: discord.Interaction):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) initiating check for database processes")
    
    await interaction.response.defer(ephemeral=True)
    
    start_time = time.time()
    
    try:
        logger.debug("Executing 'fuser' command to check database processes")
        result = subprocess.run(["fuser", "-v", "subscriptions.db"], capture_output=True, text=True)
        
        if result.stderr:
            message = f"Processes using the database:\n{result.stderr}"
            logger.info(f"Found processes using the database: {result.stderr.strip()}")
            await interaction.followup.send(message, ephemeral=True)
        else:
            message = "No processes found using the database."
            logger.info(message)
            await interaction.followup.send(message, ephemeral=True)
    
    except subprocess.CalledProcessError as e:
        error_message = f"Subprocess error occurred while checking database processes: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"Error checking database processes: {str(e)}", ephemeral=True)
    except Exception as e:
        error_message = f"Unexpected error occurred while checking database processes: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"Error checking database processes: {str(e)}", ephemeral=True)
    
    end_time = time.time()
    duration = round(end_time - start_time, 2)
    logger.info(f"check_db_processes command completed in {duration} seconds")

@bot.tree.command(name="check_db_integrity", description="Check database integrity")
@has_debug_role()
async def check_db_integrity(interaction: discord.Interaction):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) initiating database integrity check")
    
    await interaction.response.defer(ephemeral=True)
    
    start_time = time.time()
    
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            logger.debug("Executing PRAGMA integrity_check")
            c.execute("PRAGMA integrity_check")
            result = c.fetchone()
            
            if result[0] == "ok":
                message = "Database integrity check passed."
                logger.info(message)
                await interaction.followup.send(message, ephemeral=True)
            else:
                message = f"Database integrity check failed: {result[0]}"
                logger.warning(message)
                await interaction.followup.send(message, ephemeral=True)
    
    except sqlite3.Error as e:
        error_message = f"SQLite error occurred during database integrity check: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"Error checking database integrity: {str(e)}", ephemeral=True)
    except Exception as e:
        error_message = f"Unexpected error occurred during database integrity check: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"Unexpected error checking database integrity: {str(e)}", ephemeral=True)
    
    end_time = time.time()
    duration = round(end_time - start_time, 2)
    logger.info(f"check_db_integrity command completed in {duration} seconds")

@bot.tree.command(name="compact_database", description="Compact the database")
@has_debug_role()
async def compact_database(interaction: discord.Interaction):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) initiating database compaction")
    
    await interaction.response.defer(ephemeral=True)
    
    start_time = time.time()
    
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            logger.debug("Executing VACUUM command")
            c.execute("VACUUM")
            
        end_time = time.time()
        duration = round(end_time - start_time, 2)
        
        message = f"Database compacted successfully in {duration} seconds."
        logger.info(message)
        await interaction.followup.send(message, ephemeral=True)
    
    except sqlite3.Error as e:
        error_message = f"SQLite error occurred during database compaction: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"Error compacting database: {str(e)}", ephemeral=True)
    except Exception as e:
        error_message = f"Unexpected error occurred during database compaction: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"Unexpected error compacting database: {str(e)}", ephemeral=True)
    
    end_time = time.time()
    total_duration = round(end_time - start_time, 2)
    logger.info(f"compact_database command completed in {total_duration} seconds")

@bot.tree.command(name="show_db_contents", description="Show the contents of the forum_flair_settings table")
@has_debug_role()
async def show_db_contents(interaction: discord.Interaction):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) requesting to show database contents")
    
    await interaction.response.defer(ephemeral=True)
    
    start_time = time.time()
    
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            logger.debug("Executing SELECT query on forum_flair_settings table")
            c.execute("SELECT * FROM forum_flair_settings")
            rows = c.fetchall()
            
        if rows:
            logger.info(f"Retrieved {len(rows)} rows from forum_flair_settings table")
            content = "Database contents:\n"
            for row in rows:
                content += f"Channel ID: {row[0]}, Max Flairs: {row[1]}, Enabled: {bool(row[2])}, Blacklist: {row[3]}\n"
        else:
            logger.info("The forum_flair_settings table is empty")
            content = "The forum_flair_settings table is empty."
        
        # Split content into chunks of 1900 characters (leaving room for formatting)
        chunks = textwrap.wrap(content, width=1900, replace_whitespace=False, break_long_words=False)
        logger.debug(f"Content split into {len(chunks)} chunks for sending")
        
        # Send the first chunk
        await interaction.followup.send(chunks[0], ephemeral=True)
        
        # Send additional chunks as separate messages
        for i, chunk in enumerate(chunks[1:], start=2):
            await interaction.followup.send(chunk, ephemeral=True)
            logger.debug(f"Sent chunk {i} of {len(chunks)}")
        
        end_time = time.time()
        duration = round(end_time - start_time, 2)
        logger.info(f"Database contents displayed successfully in {duration} seconds")
    
    except sqlite3.Error as e:
        error_message = f"SQLite error occurred while querying database: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"Error querying database: {str(e)}", ephemeral=True)
    except Exception as e:
        error_message = f"Unexpected error occurred while showing database contents: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"Unexpected error occurred: {str(e)}", ephemeral=True)
    
    end_time = time.time()
    total_duration = round(end_time - start_time, 2)
    logger.info(f"show_db_contents command completed in {total_duration} seconds")

@bot.tree.command(name="cleanup_database", description="Clean up duplicate entries and add unique constraint")
@has_debug_role()
async def cleanup_database(interaction: discord.Interaction):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) initiating database cleanup")
    
    await interaction.response.defer(ephemeral=True)
    
    start_time = time.time()
    
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            
            logger.debug("Creating temporary table")
            c.execute('''CREATE TABLE temp_forum_flair_settings
                         (channel_id INTEGER PRIMARY KEY,
                          max_flairs INTEGER,
                          flair_enabled INTEGER,
                          blacklisted_flairs TEXT)''')
            
            logger.debug("Inserting unique rows into temporary table")
            c.execute('''INSERT OR REPLACE INTO temp_forum_flair_settings
                         SELECT channel_id, max_flairs, flair_enabled, blacklisted_flairs
                         FROM forum_flair_settings
                         GROUP BY channel_id''')
            
            # Get the number of rows before and after cleanup
            c.execute("SELECT COUNT(*) FROM forum_flair_settings")
            rows_before = c.fetchone()[0]
            c.execute("SELECT COUNT(*) FROM temp_forum_flair_settings")
            rows_after = c.fetchone()[0]
            
            logger.debug("Dropping original table")
            c.execute('DROP TABLE forum_flair_settings')
            
            logger.debug("Renaming temporary table")
            c.execute('ALTER TABLE temp_forum_flair_settings RENAME TO forum_flair_settings')
            
            conn.commit()
        
        end_time = time.time()
        duration = round(end_time - start_time, 2)
        
        rows_removed = rows_before - rows_after
        message = f"Database cleaned up and unique constraint added. Removed {rows_removed} duplicate entries. Operation took {duration} seconds."
        logger.info(message)
        await interaction.followup.send(message, ephemeral=True)
    
    except sqlite3.Error as e:
        error_message = f"SQLite error occurred during database cleanup: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"Error cleaning up database: {str(e)}", ephemeral=True)
    except Exception as e:
        error_message = f"Unexpected error occurred during database cleanup: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"Unexpected error cleaning up database: {str(e)}", ephemeral=True)
    
    end_time = time.time()
    total_duration = round(end_time - start_time, 2)
    logger.info(f"cleanup_database command completed in {total_duration} seconds")

@bot.tree.command(name="subscribe", description="Subscribe to a subreddit for a specific channel")
@app_commands.describe(
    subreddit="The name of the subreddit to subscribe to",
    channel="The channel to post updates in"
)
async def subscribe(interaction: discord.Interaction, subreddit: str, channel: discord.TextChannel):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) attempting to subscribe to r/{subreddit} in channel {channel.name} ({channel.id})")
    
    # Defer the response immediately
    await interaction.response.defer(ephemeral=True)

    start_time = time.time()

    try:
        c.execute("SELECT * FROM subscriptions WHERE subreddit = ? AND channel_id = ?", (subreddit, channel.id))
        existing_subscription = c.fetchone()

        if existing_subscription:
            logger.info(f"Subscription to r/{subreddit} in channel {channel.id} already exists")
            await interaction.followup.send(f"Already subscribed to r/{subreddit} in {channel.mention}")
        else:
            current_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
            logger.debug(f"Inserting new subscription: r/{subreddit}, channel {channel.id}, last_check {current_time}")
            c.execute("INSERT INTO subscriptions (subreddit, channel_id, last_check, last_submission_id) VALUES (?, ?, ?, ?)",
                      (subreddit, channel.id, current_time, None))
            conn.commit()
            logger.info(f"Successfully subscribed to r/{subreddit} in channel {channel.id}")
            await interaction.followup.send(f"Subscribed to r/{subreddit} in {channel.mention}")

    except sqlite3.Error as e:
        error_message = f"SQLite error occurred while subscribing to r/{subreddit}: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An error occurred while subscribing: {str(e)}", ephemeral=True)
    except Exception as e:
        error_message = f"Unexpected error occurred while subscribing to r/{subreddit}: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An unexpected error occurred: {str(e)}", ephemeral=True)

    end_time = time.time()
    duration = round(end_time - start_time, 2)
    logger.info(f"subscribe command completed in {duration} seconds")

@bot.tree.command(name="unsubscribe", description="Unsubscribe from a subreddit for a specific channel")
@app_commands.describe(
    subreddit="The name of the subreddit to unsubscribe from",
    channel="The channel to unsubscribe from"
)
async def unsubscribe(interaction: discord.Interaction, subreddit: str, channel: discord.TextChannel):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) attempting to unsubscribe from r/{subreddit} in channel {channel.name} ({channel.id})")
    
    # Defer the response immediately
    await interaction.response.defer(ephemeral=True)
    
    start_time = time.time()
    
    try:
        logger.debug(f"Executing DELETE query for r/{subreddit} in channel {channel.id}")
        c.execute("DELETE FROM subscriptions WHERE subreddit = ? AND channel_id = ?", (subreddit, channel.id))
        
        if c.rowcount > 0:
            conn.commit()
            logger.info(f"Successfully unsubscribed from r/{subreddit} in channel {channel.id}")
            await interaction.followup.send(f"Unsubscribed from r/{subreddit} in {channel.mention}")
        else:
            logger.warning(f"No subscription found for r/{subreddit} in channel {channel.id}")
            await interaction.followup.send(f"No subscription found for r/{subreddit} in {channel.mention}")
    
    except sqlite3.Error as e:
        error_message = f"SQLite error occurred while unsubscribing from r/{subreddit}: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An error occurred while unsubscribing: {str(e)}", ephemeral=True)
    except Exception as e:
        error_message = f"Unexpected error occurred while unsubscribing from r/{subreddit}: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An unexpected error occurred: {str(e)}", ephemeral=True)
    
    end_time = time.time()
    duration = round(end_time - start_time, 2)
    logger.info(f"unsubscribe command completed in {duration} seconds")

@bot.tree.command(name="list_subscriptions", description="List all subreddit subscriptions")
async def list_subscriptions(interaction: discord.Interaction):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) requesting list of subscriptions")
    
    # Defer the response immediately
    await interaction.response.defer(ephemeral=True)

    start_time = time.time()

    try:
        logger.debug("Executing SELECT query to fetch all subscriptions")
        c.execute("SELECT subreddit, channel_id FROM subscriptions ORDER BY channel_id, subreddit")
        subscriptions = c.fetchall()
        
        if not subscriptions:
            logger.info("No subscriptions found")
            await interaction.followup.send("No subscriptions found.")
            return

        logger.info(f"Found {len(subscriptions)} subscriptions")
        response = "Subreddit subscriptions:\n\n"
        current_channel = None
        subscription_count = 0

        for subreddit, channel_id in subscriptions:
            channel = bot.get_channel(channel_id)
            if channel:
                if channel != current_channel:
                    response += f"#{channel.name}:\n"
                    current_channel = channel
                response += f"- r/{subreddit}\n"
                subscription_count += 1

        logger.debug(f"Generated response with {subscription_count} valid subscriptions")

        # If the response is too long, split it into multiple messages
        if len(response) > 2000:
            chunks = [response[i:i+2000] for i in range(0, len(response), 2000)]
            logger.debug(f"Response split into {len(chunks)} chunks")
            for i, chunk in enumerate(chunks, 1):
                await interaction.followup.send(chunk)
                logger.debug(f"Sent chunk {i} of {len(chunks)}")
        else:
            await interaction.followup.send(response)
            logger.debug("Sent single response message")

    except sqlite3.Error as e:
        error_message = f"SQLite error occurred while listing subscriptions: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An error occurred while listing subscriptions: {str(e)}", ephemeral=True)
    except Exception as e:
        error_message = f"Unexpected error occurred while listing subscriptions: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An unexpected error occurred: {str(e)}", ephemeral=True)

    end_time = time.time()
    duration = round(end_time - start_time, 2)
    logger.info(f"list_subscriptions command completed in {duration} seconds")

@bot.tree.command(name="subscribe_forum", description="Subscribe to a subreddit and post updates to a forum thread")
@app_commands.describe(
    subreddit="The subreddit to subscribe to",
    forum="The forum channel to post updates in",
    thread="The thread to post updates in (optional)",
    enable_flairs="Enable or disable flair creation (default: True)",
    max_flairs="Maximum number of flairs to create (default: 20)",
    blacklisted_flairs="Comma-separated list of flairs to blacklist (optional)"
)
async def subscribe_forum(
    interaction: discord.Interaction, 
    subreddit: str, 
    forum: discord.ForumChannel, 
    thread: typing.Optional[discord.Thread] = None,
    enable_flairs: bool = True,
    max_flairs: int = 20, 
    blacklisted_flairs: str = ""
):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) attempting to subscribe forum to r/{subreddit}")
    
    await interaction.response.defer(ephemeral=True)
    
    start_time = time.time()
    
    try:
        if not isinstance(forum, discord.ForumChannel):
            logger.warning(f"User specified a non-forum channel: {forum.name} ({forum.id})")
            await interaction.followup.send("The specified channel is not a forum channel.", ephemeral=True)
            return

        if thread and thread.parent_id != forum.id:
            logger.warning(f"Specified thread {thread.id} does not belong to the selected forum {forum.id}")
            await interaction.followup.send("The specified thread does not belong to the selected forum.", ephemeral=True)
            return

        if not thread:
            logger.info(f"Creating new thread in forum {forum.name} ({forum.id}) for r/{subreddit}")
            thread = await forum.create_thread(name=f"Updates for r/{subreddit}", content=f"This thread will contain updates from r/{subreddit}")
            thread_id = thread.id
        else:
            thread_id = thread.id
        
        logger.debug(f"Thread ID for subscription: {thread_id}")

        blacklisted_flairs_list = [flair.strip() for flair in blacklisted_flairs.split(',') if flair.strip()]
        logger.debug(f"Blacklisted flairs: {blacklisted_flairs_list}")

        logger.debug(f"Inserting forum subscription for r/{subreddit} in channel {forum.id}, thread {thread_id}")
        c.execute("INSERT INTO forum_subscriptions (subreddit, channel_id, thread_id, last_check) VALUES (?, ?, ?, ?)",
                  (subreddit, forum.id, thread_id, datetime.now(timezone.utc).isoformat()))
        
        logger.debug(f"Inserting/updating forum flair settings for r/{subreddit} in channel {forum.id}")
        c.execute("INSERT OR REPLACE INTO forum_flair_settings (subreddit, channel_id, max_flairs, flair_enabled, blacklisted_flairs) VALUES (?, ?, ?, ?, ?)",
                  (subreddit, forum.id, max_flairs, int(enable_flairs), json.dumps(blacklisted_flairs_list)))
        
        conn.commit()
        logger.info(f"Successfully subscribed forum to r/{subreddit} in channel {forum.id}, thread {thread_id}")
        await interaction.followup.send(f"Successfully subscribed to r/{subreddit} in the specified forum thread.", ephemeral=True)
    
    except sqlite3.IntegrityError as e:
        logger.warning(f"Attempted to create duplicate subscription for r/{subreddit} in forum {forum.id}: {str(e)}")
        await interaction.followup.send(f"You are already subscribed to r/{subreddit} in this forum.", ephemeral=True)
    except sqlite3.Error as e:
        error_message = f"SQLite error occurred while subscribing forum to r/{subreddit}: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An error occurred: {str(e)}", ephemeral=True)
    except Exception as e:
        error_message = f"Unexpected error occurred while subscribing forum to r/{subreddit}: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An unexpected error occurred: {str(e)}", ephemeral=True)

    end_time = time.time()
    duration = round(end_time - start_time, 2)
    logger.info(f"subscribe_forum command completed in {duration} seconds")

@bot.tree.command(name="unsubscribe_forum", description="Unsubscribe from a subreddit for a specific forum thread")
@app_commands.describe(
    subreddit="The name of the subreddit to unsubscribe from",
    forum="The forum channel to unsubscribe from",
    thread="The thread to unsubscribe from"
)
async def unsubscribe_forum(interaction: discord.Interaction, subreddit: str, forum: discord.ForumChannel, thread: discord.Thread):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) attempting to unsubscribe forum from r/{subreddit}")
    
    # Defer the response immediately
    await interaction.response.defer(ephemeral=True)
    
    start_time = time.time()
    
    try:
        logger.debug(f"Executing DELETE query for r/{subreddit} in forum {forum.id}, thread {thread.id}")
        c.execute("DELETE FROM forum_subscriptions WHERE subreddit = ? AND channel_id = ? AND thread_id = ?", 
                  (subreddit, forum.id, thread.id))
        
        if c.rowcount > 0:
            conn.commit()
            logger.info(f"Successfully unsubscribed forum from r/{subreddit} in forum {forum.id}, thread {thread.id}")
            await interaction.followup.send(f"Unsubscribed from r/{subreddit} in thread {thread.mention}")
        else:
            logger.warning(f"No subscription found for r/{subreddit} in forum {forum.id}, thread {thread.id}")
            await interaction.followup.send(f"No subscription found for r/{subreddit} in thread {thread.mention}")
        
        # Optionally, clean up forum_flair_settings if no more subscriptions exist for this subreddit in this forum
        c.execute("SELECT COUNT(*) FROM forum_subscriptions WHERE subreddit = ? AND channel_id = ?", (subreddit, forum.id))
        if c.fetchone()[0] == 0:
            logger.debug(f"Cleaning up forum_flair_settings for r/{subreddit} in forum {forum.id}")
            c.execute("DELETE FROM forum_flair_settings WHERE subreddit = ? AND channel_id = ?", (subreddit, forum.id))
            conn.commit()
    
    except sqlite3.Error as e:
        error_message = f"SQLite error occurred while unsubscribing forum from r/{subreddit}: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An error occurred while unsubscribing: {str(e)}", ephemeral=True)
    except Exception as e:
        error_message = f"Unexpected error occurred while unsubscribing forum from r/{subreddit}: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An unexpected error occurred: {str(e)}", ephemeral=True)
    
    end_time = time.time()
    duration = round(end_time - start_time, 2)
    logger.info(f"unsubscribe_forum command completed in {duration} seconds")

@bot.tree.command(name="unsubscribe_forum_individual", description="Unsubscribe from a subreddit that creates individual threads for each new post")
@app_commands.describe(
    subreddit="The name of the subreddit to unsubscribe from",
    forum="The forum channel to unsubscribe from"
)
async def unsubscribe_forum_individual(interaction: discord.Interaction, subreddit: str, forum: discord.ForumChannel):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) attempting to unsubscribe individual forum posts from r/{subreddit}")
    
    await interaction.response.defer(ephemeral=True)
    
    start_time = time.time()
    
    try:
        # Check if a subscription exists
        logger.debug(f"Checking for existing subscription for r/{subreddit} in forum {forum.id}")
        c.execute("SELECT * FROM individual_forum_subscriptions WHERE subreddit = ? AND channel_id = ?", (subreddit, forum.id))
        existing_subscription = c.fetchone()
        
        if not existing_subscription:
            logger.warning(f"No individual post subscription found for r/{subreddit} in forum {forum.id}")
            await interaction.followup.send(f"No subscription found for r/{subreddit} in {forum.mention} for individual posts")
            return

        # Remove the subscription from the database
        logger.debug(f"Executing DELETE query for r/{subreddit} in forum {forum.id}")
        c.execute("DELETE FROM individual_forum_subscriptions WHERE subreddit = ? AND channel_id = ?", (subreddit, forum.id))
        conn.commit()
        
        logger.info(f"Successfully unsubscribed individual forum posts from r/{subreddit} in forum {forum.id}")
        await interaction.followup.send(f"Unsubscribed from r/{subreddit} in {forum.mention} for individual posts.")
        
        # Optionally, clean up forum_flair_settings if no more subscriptions exist for this subreddit in this forum
        c.execute("SELECT COUNT(*) FROM forum_subscriptions WHERE subreddit = ? AND channel_id = ?", (subreddit, forum.id))
        if c.fetchone()[0] == 0:
            logger.debug(f"Cleaning up forum_flair_settings for r/{subreddit} in forum {forum.id}")
            c.execute("DELETE FROM forum_flair_settings WHERE subreddit = ? AND channel_id = ?", (subreddit, forum.id))
            conn.commit()
    
    except sqlite3.Error as e:
        error_message = f"SQLite error occurred while unsubscribing individual forum posts from r/{subreddit}: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An error occurred while unsubscribing: {str(e)}", ephemeral=True)
    except Exception as e:
        error_message = f"Unexpected error occurred while unsubscribing individual forum posts from r/{subreddit}: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An unexpected error occurred: {str(e)}", ephemeral=True)
    
    end_time = time.time()
    duration = round(end_time - start_time, 2)
    logger.info(f"unsubscribe_forum_individual command completed in {duration} seconds")

@bot.tree.command(name="list_forum_subscriptions", description="List all forum subreddit subscriptions")
async def list_forum_subscriptions(interaction: discord.Interaction):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) requesting list of forum subscriptions")
    
    # Defer the response immediately
    await interaction.response.defer(ephemeral=True)

    start_time = time.time()

    try:
        # Query regular forum subscriptions
        logger.debug("Executing SELECT query for regular forum subscriptions")
        c.execute("SELECT subreddit, channel_id, thread_id FROM forum_subscriptions ORDER BY channel_id, thread_id, subreddit")
        regular_subscriptions = c.fetchall()
        
        # Query individual forum subscriptions
        logger.debug("Executing SELECT query for individual forum subscriptions")
        c.execute("SELECT subreddit, channel_id FROM individual_forum_subscriptions ORDER BY channel_id, subreddit")
        individual_subscriptions = c.fetchall()
        
        logger.info(f"Found {len(regular_subscriptions)} regular forum subscriptions and {len(individual_subscriptions)} individual forum subscriptions")
        
        if not regular_subscriptions and not individual_subscriptions:
            logger.info("No forum subscriptions found")
            await interaction.followup.send("No forum subscriptions found.")
            return

        response = "Forum subreddit subscriptions:\n\n"
        
        # Process regular forum subscriptions
        if regular_subscriptions:
            logger.debug("Processing regular forum subscriptions")
            response += "Regular forum thread subscriptions:\n"
            current_forum = None
            current_thread = None
            for subreddit, channel_id, thread_id in regular_subscriptions:
                forum = bot.get_channel(channel_id)
                thread = bot.get_channel(thread_id)
                if forum and thread:
                    if forum != current_forum:
                        response += f"Forum: {forum.name}\n"
                        current_forum = forum
                    if thread != current_thread:
                        response += f"  Thread: {thread.name}\n"
                        current_thread = thread
                    response += f"    - r/{subreddit}\n"
                else:
                    logger.warning(f"Unable to find forum {channel_id} or thread {thread_id} for subscription to r/{subreddit}")
            response += "\n"
        
        # Process individual forum subscriptions
        if individual_subscriptions:
            logger.debug("Processing individual forum subscriptions")
            response += "Individual post forum subscriptions:\n"
            current_forum = None
            for subreddit, channel_id in individual_subscriptions:
                forum = bot.get_channel(channel_id)
                if forum:
                    if forum != current_forum:
                        response += f"Forum: {forum.name}\n"
                        current_forum = forum
                    response += f"  - r/{subreddit} (individual posts)\n"
                else:
                    logger.warning(f"Unable to find forum {channel_id} for individual subscription to r/{subreddit}")

        # If the response is too long, split it into multiple messages
        if len(response) > 2000:
            chunks = [response[i:i+2000] for i in range(0, len(response), 2000)]
            logger.debug(f"Response split into {len(chunks)} chunks")
            for i, chunk in enumerate(chunks, 1):
                await interaction.followup.send(chunk)
                logger.debug(f"Sent chunk {i} of {len(chunks)}")
        else:
            await interaction.followup.send(response)
            logger.debug("Sent single response message")

    except sqlite3.Error as e:
        error_message = f"SQLite error occurred while listing forum subscriptions: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An error occurred while listing forum subscriptions: {str(e)}", ephemeral=True)
    except Exception as e:
        error_message = f"Unexpected error occurred while listing forum subscriptions: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.followup.send(f"An unexpected error occurred: {str(e)}", ephemeral=True)

    end_time = time.time()
    duration = round(end_time - start_time, 2)
    logger.info(f"list_forum_subscriptions command completed in {duration} seconds")

@bot.tree.command(name="subscribe_forum_create", description="Subscribe to a subreddit and create a new forum thread")
@app_commands.describe(
    subreddit="The name of the subreddit to subscribe to",
    forum="The forum channel to create the thread in",
    thread_name="The name for the new thread",
    enable_flairs="Enable or disable flair creation (default: True)",
    max_flairs="Maximum number of flairs to create (default: 20)",
    blacklisted_flairs="Comma-separated list of flairs to blacklist (optional)"
)
async def subscribe_forum_create(
    interaction: discord.Interaction, 
    subreddit: str, 
    forum: discord.ForumChannel, 
    thread_name: str,
    enable_flairs: bool = True,
    max_flairs: int = 20, 
    blacklisted_flairs: str = ""
):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) attempting to create forum subscription for r/{subreddit}")
    
    await interaction.response.defer(ephemeral=True)
    
    start_time = time.time()
    
    try:
        # Check if a subscription already exists for this subreddit in this forum
        logger.debug(f"Checking for existing subscription for r/{subreddit} in forum {forum.id}")
        c.execute("SELECT thread_id FROM forum_subscriptions WHERE subreddit = ? AND channel_id = ?", (subreddit, forum.id))
        existing_subscription = c.fetchone()

        if existing_subscription:
            thread_id = existing_subscription[0]
            existing_thread = bot.get_channel(thread_id)
            
            if existing_thread is None:
                logger.warning(f"Previous thread for r/{subreddit} in forum {forum.id} was deleted. Removing old subscription.")
                c.execute("DELETE FROM forum_subscriptions WHERE subreddit = ? AND channel_id = ?", (subreddit, forum.id))
                conn.commit()
                await interaction.followup.send(f"The previous thread for r/{subreddit} was deleted. Creating a new one.")
            else:
                logger.info(f"Subscription already exists for r/{subreddit} in thread {existing_thread.id}")
                await interaction.followup.send(f"A subscription for r/{subreddit} already exists in thread {existing_thread.mention}. Use that or unsubscribe first.")
                return

        # Fetch the latest post from the subreddit
        logger.debug(f"Fetching latest post from r/{subreddit}")
        async with aiohttp.ClientSession() as session:
            reddit = asyncpraw.Reddit(client_id=REDDIT_CLIENT_ID,
                                      client_secret=REDDIT_CLIENT_SECRET,
                                      user_agent=REDDIT_USER_AGENT,
                                      requestor_kwargs={'session': session})
            
            try:
                subreddit_instance = await reddit.subreddit(subreddit)
                async for submission in subreddit_instance.new(limit=1):
                    latest_post = submission
                    break
            except Exception as e:
                logger.error(f"Error fetching posts from r/{subreddit}: {str(e)}", exc_info=True)
                await interaction.followup.send(f"Error fetching posts from r/{subreddit}. Please check if the subreddit exists and is accessible.")
                return

        # Create the thread
        logger.debug(f"Creating new thread '{thread_name}' in forum {forum.id}")
        try:
            thread_with_message = await forum.create_thread(
                name=thread_name,
                content=f"This thread will track new posts from this r/{subreddit} and any other subreddits you may add later.",
                embed=await create_reddit_embed(latest_post)
            )
            thread = thread_with_message.thread
            logger.info(f"Created new thread {thread.id} for r/{subreddit} in forum {forum.id}")
        except discord.errors.Forbidden:
            logger.error(f"Insufficient permissions to create thread in forum {forum.id}")
            await interaction.followup.send("I don't have permission to create threads in this forum. Please check my permissions.")
            return
        except Exception as e:
            logger.error(f"Error creating thread for r/{subreddit}: {str(e)}", exc_info=True)
            await interaction.followup.send(f"An error occurred while creating the thread: {str(e)}")
            return

        # Convert blacklisted_flairs to a list and remove any leading/trailing whitespace
        blacklisted_flairs_list = [flair.strip() for flair in blacklisted_flairs.split(',') if flair.strip()]
        logger.debug(f"Blacklisted flairs for r/{subreddit}: {blacklisted_flairs_list}")

        # Add the subscription to the database
        logger.debug(f"Adding subscription for r/{subreddit} to database")
        try:
            c.execute("INSERT INTO forum_subscriptions (subreddit, channel_id, thread_id, last_check, last_submission_id) VALUES (?, ?, ?, ?, ?)",
                      (subreddit, forum.id, thread.id, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"), latest_post.id))
            
            c.execute("INSERT OR REPLACE INTO forum_flair_settings (subreddit, channel_id, max_flairs, flair_enabled, blacklisted_flairs) VALUES (?, ?, ?, ?, ?)",
                      (subreddit, forum.id, max_flairs, int(enable_flairs), json.dumps(blacklisted_flairs_list)))
            
            conn.commit()
            logger.info(f"Successfully added subscription for r/{subreddit} in thread {thread.id}")
        except sqlite3.Error as e:
            logger.error(f"SQLite error while saving subscription for r/{subreddit}: {str(e)}", exc_info=True)
            await interaction.followup.send(f"An error occurred while saving the subscription: {str(e)}")
            return

        await interaction.followup.send(f"Created and subscribed to thread {thread.mention} for r/{subreddit}")

    except Exception as e:
        logger.error(f"Unexpected error in subscribe_forum_create for r/{subreddit}: {str(e)}", exc_info=True)
        await interaction.followup.send(f"An unexpected error occurred: {str(e)}")

    end_time = time.time()
    duration = round(end_time - start_time, 2)
    logger.info(f"subscribe_forum_create command for r/{subreddit} completed in {duration} seconds")

@bot.tree.command(name="subscribe_forum_individual", description="Subscribe to a subreddit and create individual threads for each new post")
@app_commands.describe(
    subreddit="The name of the subreddit to subscribe to",
    forum="The forum channel to create threads in",
    enable_flairs="Enable flair-to-tag conversion (default: True)",
    max_flairs="Maximum number of flairs to convert to tags (1-20, default: 20)",
    blacklisted_flairs="Comma-separated list of flairs to blacklist (optional)"
)
async def subscribe_forum_individual(
    interaction: discord.Interaction,
    subreddit: str,
    forum: discord.ForumChannel,
    enable_flairs: bool = True,
    max_flairs: int = 20,
    blacklisted_flairs: str = ""
):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) attempting to subscribe to individual posts from r/{subreddit} in forum {forum.id}")
    
    await interaction.response.defer(ephemeral=True)
    
    start_time = time.time()
    
    try:
        # Check if a subscription already exists
        logger.debug(f"Checking for existing subscription for r/{subreddit} in forum {forum.id}")
        c.execute("SELECT * FROM individual_forum_subscriptions WHERE subreddit = ? AND channel_id = ?", (subreddit, forum.id))
        if c.fetchone():
            logger.info(f"Subscription already exists for r/{subreddit} in forum {forum.id}")
            await interaction.followup.send(f"Already subscribed to r/{subreddit} in {forum.mention} for individual posts")
            return

        # Convert blacklisted_flairs to a list and remove any leading/trailing whitespace
        blacklisted_flairs_list = [flair.strip() for flair in blacklisted_flairs.split(',') if flair.strip()]
        logger.debug(f"Blacklisted flairs for r/{subreddit}: {blacklisted_flairs_list}")

        # Add the subscription to the database
        logger.debug(f"Adding subscription for r/{subreddit} to database")
        try:
            c.execute("INSERT INTO individual_forum_subscriptions (subreddit, channel_id, last_check) VALUES (?, ?, ?)",
                      (subreddit, forum.id, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")))
            
            # Add flair settings to the database
            logger.debug(f"Attempting to insert into forum_flair_settings: subreddit={subreddit}, channel_id={forum.id}, max_flairs={max_flairs}, flair_enabled={int(enable_flairs)}, blacklisted_flairs={json.dumps(blacklisted_flairs_list)}")
            c.execute("INSERT OR REPLACE INTO forum_flair_settings (subreddit, channel_id, max_flairs, flair_enabled, blacklisted_flairs) VALUES (?, ?, ?, ?, ?)",
                      (subreddit, forum.id, max_flairs, int(enable_flairs), json.dumps(blacklisted_flairs_list)))
            
            conn.commit()
            logger.info(f"Successfully added subscription for r/{subreddit} in forum {forum.id}")
            await interaction.followup.send(f"Subscribed to r/{subreddit} in {forum.mention}. Each new post will create a separate thread.")
        except sqlite3.Error as e:
            logger.error(f"SQLite error while saving subscription for r/{subreddit}: {str(e)}", exc_info=True)
            await interaction.followup.send(f"An error occurred while saving the subscription: {str(e)}")
            return

        # Fetch the latest post from the subreddit
        logger.debug(f"Fetching latest post from r/{subreddit}")
        async with aiohttp.ClientSession() as session:
            reddit = asyncpraw.Reddit(client_id=REDDIT_CLIENT_ID,
                                      client_secret=REDDIT_CLIENT_SECRET,
                                      user_agent=REDDIT_USER_AGENT,
                                      requestor_kwargs={'session': session})
            
            try:
                subreddit_instance = await reddit.subreddit(subreddit)
                async for submission in subreddit_instance.new(limit=1):
                    latest_post = submission
                    break
                
                # Create a thread for the latest post
                thread_name = truncate_string(latest_post.title, 100)
                image_url = await get_primary_image_url(latest_post)
                
                logger.debug(f"Creating new thread '{thread_name}' in forum {forum.id}")
                if image_url:
                    thread = await forum.create_thread(name=thread_name, content=image_url)
                else:
                    embed = await create_simple_reddit_embed(latest_post)
                    thread = await forum.create_thread(name=thread_name, embed=embed)
                
                logger.info(f"Created new thread {thread.thread.id} for latest post from r/{subreddit}")
                
                # Process the submission in the new thread
                logger.debug(f"Processing submission in thread {thread.thread.id}")
                await process_submission(latest_post, thread.thread, get_button_visibility())
                logger.info(f"Successfully processed latest post from r/{subreddit} in thread {thread.thread.id}")
            except Exception as e:
                logger.error(f"Error fetching or processing the latest post from r/{subreddit}: {str(e)}", exc_info=True)

    except Exception as e:
        logger.error(f"Unexpected error in subscribe_forum_individual for r/{subreddit}: {str(e)}", exc_info=True)
        await interaction.followup.send(f"An unexpected error occurred: {str(e)}")

    end_time = time.time()
    duration = round(end_time - start_time, 2)
    logger.info(f"subscribe_forum_individual command for r/{subreddit} completed in {duration} seconds")

@bot.tree.command(name="set_button_visibility", description="Set visibility for message buttons")
@app_commands.describe(
    button="The button to set visibility for",
    visible="Whether the button should be visible or not"
)
@app_commands.choices(button=[
    app_commands.Choice(name="All Buttons", value="all"),
    app_commands.Choice(name="Reddit Post", value="Reddit Post"),
    app_commands.Choice(name="Watch Video", value="Watch Video"),
    app_commands.Choice(name="RedGIFs", value="RedGIFs"),
    app_commands.Choice(name="YouTube Link", value="YouTube Link"),
    app_commands.Choice(name="Image Gallery", value="Image Gallery"),
    app_commands.Choice(name="Web Link", value="Web Link"),
])
async def set_button_visibility(interaction: discord.Interaction, button: app_commands.Choice[str], visible: bool):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) setting button visibility: {button.value} to {'visible' if visible else 'hidden'}")
    
    start_time = time.time()
    
    try:
        if button.value == "all":
            logger.debug("Updating visibility for all buttons")
            for btn in button_list:
                c.execute("UPDATE button_visibility SET is_visible = ? WHERE button_name = ?", (int(visible), btn))
                logger.debug(f"Updated visibility for button '{btn}' to {visible}")
            message = f"All buttons are now {'visible' if visible else 'hidden'}."
        else:
            logger.debug(f"Updating visibility for button '{button.value}'")
            c.execute("UPDATE button_visibility SET is_visible = ? WHERE button_name = ?", (int(visible), button.value))
            message = f"The '{button.value}' button is now {'visible' if visible else 'hidden'}."
        
        conn.commit()
        logger.info("Database updated successfully")
        
        await interaction.response.send_message(message)
        logger.info(f"Response sent to user: {message}")
    
    except sqlite3.Error as e:
        error_message = f"SQLite error occurred while setting button visibility: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.response.send_message(f"An error occurred while updating button visibility: {str(e)}", ephemeral=True)
    except Exception as e:
        error_message = f"Unexpected error occurred while setting button visibility: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.response.send_message(f"An unexpected error occurred: {str(e)}", ephemeral=True)
    
    end_time = time.time()
    duration = round(end_time - start_time, 2)
    logger.info(f"set_button_visibility command completed in {duration} seconds")

@bot.tree.command(name="get_button_visibility", description="Get current visibility settings for message buttons")
async def get_button_visibility_command(interaction: discord.Interaction):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) requesting button visibility settings")
    
    start_time = time.time()
    
    try:
        logger.debug("Fetching button visibility settings from database")
        visibility = get_button_visibility()
        
        logger.debug(f"Retrieved visibility settings: {visibility}")
        
        response = "Current button visibility settings:\n\n"
        for button, is_visible in visibility.items():
            response += f"{button}: {'Visible' if is_visible else 'Hidden'}\n"
        
        logger.debug(f"Prepared response message: {response}")
        
        await interaction.response.send_message(response)
        logger.info("Button visibility settings sent to user")
    
    except sqlite3.Error as e:
        error_message = f"SQLite error occurred while fetching button visibility settings: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.response.send_message(f"An error occurred while fetching button visibility settings: {str(e)}", ephemeral=True)
    except Exception as e:
        error_message = f"Unexpected error occurred while fetching button visibility settings: {str(e)}"
        logger.error(error_message, exc_info=True)
        await interaction.response.send_message(f"An unexpected error occurred: {str(e)}", ephemeral=True)
    
    end_time = time.time()
    duration = round(end_time - start_time, 2)
    logger.info(f"get_button_visibility command completed in {duration} seconds")

@bot.tree.command(name="manage_flairs", description="Manage flair settings for a forum")
@app_commands.describe(
    forum="The forum channel to manage flair settings for",
    enable_flairs="Enable or disable flair-to-tag conversion",
    max_flairs="Maximum number of flairs to convert to tags (1-20)",
    add_blacklist="Add a flair to the blacklist (comma-separated for multiple)",
    remove_blacklist="Remove a flair from the blacklist (comma-separated for multiple)"
)
async def manage_flairs(
    interaction: discord.Interaction, 
    forum: discord.ForumChannel, 
    enable_flairs: typing.Optional[bool] = None,
    max_flairs: typing.Optional[int] = None,
    add_blacklist: typing.Optional[str] = None,
    remove_blacklist: typing.Optional[str] = None
):
    logger.info(f"User {interaction.user.name} ({interaction.user.id}) managing flairs for forum {forum.name} ({forum.id})")
    
    await interaction.response.defer(ephemeral=True)
    
    start_time = time.time()

    try:
        # Fetch current settings
        logger.debug(f"Fetching current flair settings for forum {forum.id}")
        with sqlite3.connect('subscriptions.db') as conn:
            c = conn.cursor()
            c.execute("SELECT max_flairs, flair_enabled, blacklisted_flairs FROM forum_flair_settings WHERE channel_id = ?", (forum.id,))
            result = c.fetchone()
            if result:
                current_max_flairs, current_flair_enabled, current_blacklist = result
                current_blacklist = json.loads(current_blacklist or '[]')
            else:
                current_max_flairs, current_flair_enabled, current_blacklist = 20, True, []

        logger.info(f"Current settings for forum {forum.id}: max_flairs={current_max_flairs}, flair_enabled={current_flair_enabled}, blacklisted_flairs={current_blacklist}")

        # Update settings
        settings_changed = False
        if enable_flairs is not None and enable_flairs != current_flair_enabled:
            logger.info(f"Updating flair_enabled for forum {forum.id}: {current_flair_enabled} -> {enable_flairs}")
            current_flair_enabled = enable_flairs
            settings_changed = True
        if max_flairs is not None and max_flairs != current_max_flairs:
            new_max_flairs = max(1, min(max_flairs, 20))
            logger.info(f"Updating max_flairs for forum {forum.id}: {current_max_flairs} -> {new_max_flairs}")
            current_max_flairs = new_max_flairs
            settings_changed = True
        if add_blacklist:
            added_items = [item.strip() for item in add_blacklist.split(',') if item.strip() not in current_blacklist]
            if added_items:
                logger.info(f"Adding items {added_items} to blacklist for forum {forum.id}")
                current_blacklist.extend(added_items)
                settings_changed = True
                
                # Remove corresponding tags from the forum
                tags_to_keep = [tag for tag in forum.available_tags if tag.name not in added_items]
                if len(tags_to_keep) < len(forum.available_tags):
                    try:
                        await forum.edit(available_tags=tags_to_keep)
                        removed_tags = [tag.name for tag in forum.available_tags if tag.name in added_items]
                        logger.info(f"Removed tags {removed_tags} from forum {forum.id}")
                        await interaction.followup.send(f"Added {', '.join(added_items)} to the blacklist and removed corresponding tags from the forum.", ephemeral=True)
                    except discord.HTTPException as e:
                        logger.error(f"Failed to remove tags from forum {forum.id}: {e}", exc_info=True)
                        await interaction.followup.send(f"Added {', '.join(added_items)} to the blacklist, but failed to remove corresponding tags from the forum.", ephemeral=True)
                else:
                    await interaction.followup.send(f"Added {', '.join(added_items)} to the blacklist. No corresponding tags were found in the forum to remove.", ephemeral=True)

        if remove_blacklist:
            removed_items = [item.strip() for item in remove_blacklist.split(',') if item.strip() in current_blacklist]
            if removed_items:
                logger.info(f"Removing items {removed_items} from blacklist for forum {forum.id}")
                for item in removed_items:
                    current_blacklist.remove(item)
                settings_changed = True
                await interaction.followup.send(f"Removed {', '.join(removed_items)} from the blacklist.", ephemeral=True)

        logger.info(f"Updated blacklist for forum {forum.id}: {current_blacklist}")

        if not settings_changed:
            logger.info(f"No changes made to flair settings for forum {forum.id}")
            await interaction.followup.send("No changes were made to the flair settings.", ephemeral=True)
            return

        # Update database with a different approach
        try:
            with get_db_connection() as conn:
                c = conn.cursor()
                c.execute("BEGIN EXCLUSIVE TRANSACTION")
                
                logger.debug(f"Deleting existing row for forum {forum.id}")
                c.execute("DELETE FROM forum_flair_settings WHERE channel_id = ?", (forum.id,))
                
                sql = '''INSERT INTO forum_flair_settings 
                         (channel_id, max_flairs, flair_enabled, blacklisted_flairs) 
                         VALUES (?, ?, ?, ?)'''
                params = (forum.id, current_max_flairs, current_flair_enabled, json.dumps(current_blacklist))
                logger.debug(f"Executing SQL: {sql} with params: {params}")
                
                c.execute(sql, params)
                logger.debug(f"Rows affected: {c.rowcount}")
                
                c.execute("COMMIT")
                logger.info(f"Transaction committed successfully for forum {forum.id}")

                # Verify immediately after commit
                c.execute("SELECT * FROM forum_flair_settings WHERE channel_id = ?", (forum.id,))
                verified_row = c.fetchone()
                logger.debug(f"Verified row in database immediately after commit: {verified_row}")

        except sqlite3.Error as e:
            logger.error(f"Error updating database for forum {forum.id}: {e}", exc_info=True)
            await interaction.followup.send(f"An error occurred while updating the database. Please try again later.", ephemeral=True)
            return

        # Verify with a separate connection
        try:
            verify_conn = sqlite3.connect('subscriptions.db', timeout=30)
            try:
                vc = verify_conn.cursor()
                vc.execute("SELECT * FROM forum_flair_settings WHERE channel_id = ?", (forum.id,))
                verified_row = vc.fetchone()
                logger.debug(f"Verified row in database after commit: {verified_row}")
            finally:
                verify_conn.close()
        except sqlite3.Error as e:
            logger.error(f"Error verifying database update for forum {forum.id}: {e}", exc_info=True)

        # Prepare response message
        response = f"Flair settings updated for {forum.name}:\n"
        response += f"Flair-to-tag conversion: {'Enabled' if current_flair_enabled else 'Disabled'}\n"
        response += f"Max flairs: {current_max_flairs}\n"
        response += f"Blacklisted flairs: {', '.join(current_blacklist) if current_blacklist else 'None'}"

        await interaction.followup.send(response, ephemeral=True)
        logger.info(f"Flair settings update response sent for forum {forum.id}")

        # Fetch updated settings after a short delay
        await asyncio.sleep(1)
        updated_max_flairs, updated_flair_enabled, updated_blacklist = get_flair_settings(forum.id)
        logger.info(f"Fetched updated settings after delay for forum {forum.id}: max_flairs={updated_max_flairs}, flair_enabled={updated_flair_enabled}, blacklisted_flairs={updated_blacklist}")

    except Exception as e:
        logger.error(f"Unexpected error in manage_flairs for forum {forum.id}: {str(e)}", exc_info=True)
        await interaction.followup.send(f"An unexpected error occurred: {str(e)}", ephemeral=True)

    end_time = time.time()
    duration = round(end_time - start_time, 2)
    logger.info(f"manage_flairs command for forum {forum.id} completed in {duration} seconds")

@bot.tree.command(name="test_warning", description="Test the warning log system")
async def test_warning(interaction: discord.Interaction):
    logger.warning("This is a test warning message from a slash command")
    print("Test warning logged to file")
    
    discord_handler = next((handler for handler in logger.handlers if isinstance(handler, DiscordLogHandler)), None)
    if discord_handler:
        print(f"DiscordLogHandler found with channel ID: {discord_handler.log_channel_id}")
    else:
        print("DiscordLogHandler not found in logger handlers")
    
    await interaction.response.send_message("Test warning message logged. Check console output and Discord log channel.")

@bot.tree.command(name="rotate_logs", description="Manually trigger log rotation")
async def rotate_logs(interaction: discord.Interaction):
    logger.info("Manually triggering log rotation")
    print("Manually triggering log rotation")  # Add this line
    
    rotated_files = []
    for handler in logger.handlers:
        if isinstance(handler, DiscordLogHandler):
            print(f"Found DiscordLogHandler with file: {handler.baseFilename}")
            file_size = os.path.getsize(handler.baseFilename)
            print(f"Current log file size: {file_size} bytes")
            handler.doRollover()
            rotated_files.append(handler.baseFilename)
    
    if rotated_files:
        file_sizes = [os.path.getsize(file) for file in rotated_files]
        await interaction.response.send_message(f"Log rotation triggered. Rotated files: {rotated_files}, Sizes: {file_sizes} bytes")
    else:
        await interaction.response.send_message("No DiscordLogHandler found to rotate logs")

# 12. Help Command Section
# =========================

class HelpView(discord.ui.View):
    def __init__(self, embeds: list[discord.Embed], user: discord.User, is_debug: bool):
        super().__init__(timeout=300)
        self.embeds = embeds
        self.user = user
        self.is_debug = is_debug
        self.index = 0
        self.update_buttons()
        logger.debug(f"HelpView initialized for user {user.name} (ID: {user.id}), is_debug: {is_debug}")

    def update_buttons(self):
        self.previous.disabled = self.index == 0
        self.next.disabled = self.index == len(self.embeds) - 1
        logger.debug(f"Buttons updated: previous={not self.previous.disabled}, next={not self.next.disabled}")

    @discord.ui.button(label="<", style=discord.ButtonStyle.red)
    async def previous(self, interaction: discord.Interaction, button: discord.ui.Button):
        logger.info(f"User {interaction.user.name} (ID: {interaction.user.id}) clicked previous button")
        if self.index > 0:
            self.index -= 1
            self.update_buttons()
            await interaction.response.edit_message(embed=self.embeds[self.index], view=self)
            logger.debug(f"Moved to previous embed, new index: {self.index}")
        else:
            await interaction.response.defer()
            logger.debug("Previous button clicked but already at first embed")

    @discord.ui.button(label=">", style=discord.ButtonStyle.green)
    async def next(self, interaction: discord.Interaction, button: discord.ui.Button):
        logger.info(f"User {interaction.user.name} (ID: {interaction.user.id}) clicked next button")
        if self.index < len(self.embeds) - 1:
            self.index += 1
            self.update_buttons()
            await interaction.response.edit_message(embed=self.embeds[self.index], view=self)
            logger.debug(f"Moved to next embed, new index: {self.index}")
        else:
            await interaction.response.defer()
            logger.debug("Next button clicked but already at last embed")

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        is_valid = interaction.user == self.user
        logger.debug(f"Interaction check: user={interaction.user.name} (ID: {interaction.user.id}), is_valid={is_valid}")
        return is_valid

def create_welcome_embed(user: discord.User):
    embed = discord.Embed(title=f"Welcome, {user.name}!", color=discord.Color.blue())
    embed.description = ("The RedditBot help system!\n\n"
                         "This bot was inspired by [NyNu](https://top.gg/bot/1049362593921368165) Redditard bot but I wanted a little more control over it and the hosting of the bot, so I decided to build this version.\n"
                         "I have tried to balance most things from Reddit posts and will try to improve on it overtime but I think for the moment I have struck a good balance of features.\n"
                         "If you have no intention of using Discord Forums then the basic commands will be more than enough for you to use the bot which are listed below.\n\n"
                         "You can subscribe to as many subreddits as you like and to as many different text chanels and can subscribe to multiple subreddits in single text channels.\n"
                         "But take care of the Discord and Reddit API limits.\n\n"
                         "For more information on what the bot can do please check out the [GitHub Repo](https://github.com/Trai60/Reddit-to-Discord-Bot)\n\n\n")
    embed.add_field(name="", value="Here are some of the basic commands to get you started and some information on the background tasks of the bot.\n\n")
    embed.add_field(name="**`/subscribe`**", value="Subscribe to a subreddit(s) in any text channel.", inline=False)
    embed.add_field(name="**`/unsubscribe`**", value="Unsubscribe from a subreddit from any text channel.", inline=False)
    embed.add_field(name="**`/list_subscriptions`**", value="List your current subscriptions for all of your text channels.", inline=False)
    embed.add_field(name="\u200b", value="\u200b", inline=False)  # Blank line
    embed.add_field(name="__Button Visibility__", value="Here you can set which buttons are displayed or not in Discord.", inline=False)
    embed.add_field(name="**`/set_button_visibility`**", value="To display which individual buttons are shown or you can turn off all buttons globally.", inline=False)
    embed.add_field(name="**`/get_button_visibility`**", value="Will output the state of each button is in either on/off or global on/off.", inline=False)
    embed.add_field(name="\u200b", value="\u200b", inline=False)  # Blank line
    embed.add_field(name="__Background Tasks__", value="", inline=False)
    embed.add_field(name="**`check_new_posts`**", value="This is the interval at which the bot will check for new Reddit posts for each subreddit you're subscribed to, the default is 2 minutes but this can be changed in the code.", inline=False)
    embed.add_field(name="**`cleanup_subscriptions`**", value="If a text channel or forum thread has been deleted then the bot will check every 24 hours to see if there are any missing from the current subscriptions and will remove those subscriptions that were assigned to a forum thread or text channel", inline=False)
    embed.add_field(name="**`consistency_check`**", value="This will attempt to look for any missing Reddit posts from subscribed subreddits that it may have missed during the normal new post check once every 3 hours, the time interval can be changed in the code", inline=False)
    embed.add_field(name="\u200b", value="\u200b", inline=False)  # Blank line
    embed.add_field(name="", value="Use the arrows below to navigate through the help pages", inline=False)
    return embed

def create_forum_settings_embed():
    embed = discord.Embed(title="__Forum Settings__", color=discord.Color.green())
    embed.description = "Manage your forum subscriptions and settings.\n\n\n"
    embed.add_field(name="**`/subscribe_forum`**", value="Subscribe a subreddit to a forum thread that be previously made by a Discord user.", inline=False)
    embed.add_field(name="**`/subscribe_forum_create`**", value="Subscribe to a subreddit and it will create the first thread in a forum.", inline=False)
    embed.add_field(name="**`/unsubscribe_forum`**", value="Unsubscribe you from a subreddit to any of the above types of forum threads ", inline=False)
    embed.add_field(name="**`/subscribe_forum_individual`**", value="This command will subscribe you to a subreddit and will create a new thread for each Reddit post.", inline=False)
    embed.add_field(name="**`/unsubscribe_forum_individual`**", value="This will allow you to remove any subreddits that you are subscribed to", inline=False)
    embed.add_field(name="**`/list_forum_subscriptions`**", value="Will list all of you current subscribed subreddit's to any forum channels you have.", inline=False)
    # Add more fields for other forum-related commands
    return embed

def create_flair_settings_embed():
    embed = discord.Embed(title="__Flair Settings__", color=discord.Color.purple())
    embed.add_field(name="\u200b", value="\u200b", inline=False)  # Blank line
    embed.description = ("Manage flairs and tags in your forums.\n\n"
                         "When the bot finds a new Reddit post to any subscribed subreddit it will check if the post has any flairs attached and will convert them into Discord tags for the forum channel.\n\n"
                         "Please be aware that Discord only allows 20 tags per forum channel and upto 5 tags per thread so constant monitoring of new Flairs should be regually done.\n\n")
    embed.add_field(name="**`/check_flair_settings`**", value="Checks current flair settings for a selected forum channel and will display if flair to tags is enabled, number of flairs allowed, blacklisted flair names.", inline=False)
    embed.add_field(name="**`/manage_flairs`**", value="Manage flair settings for each forum channel, enable flair to tag, max number of flairs allowed, add and remove flairs names from the blacklist.", inline=False)
    embed.add_field(name="**`/list_forum_tags`**", value="List all the current flair to tags being used on a selected forum channel, easy to see if you wish to add any to the black list", inline=False)
    embed.add_field(name="**`/remove_forum_tag`**", value="Will remove any tag that is currently being used in a forum channel.", inline=False)
    embed.add_field(name="**`/sync_forum_tags`**", value="This will check if any tags assigned to a forum channel but is present on the blacklist and will remove them from the forum channel.", inline=False)
    # Add more fields for other flair-related commands
    return embed

def create_debug_page1_embed():
    embed = discord.Embed(title="__Debug Commands Page 1__", color=discord.Color.red())
    embed.add_field(name="\u200b", value="\u200b", inline=False)  # Blank line
    embed.description = ("The commands list below are only available to server owners and users with the Debug role.\n\n"
                         "These commands are primarily for database management and troubleshooting.\n\n"
                         "They should be used carefully, especially those that modify data or database structure.\n\n")
    embed.add_field(name="**`/check_database`**", value="Performs a basic check on the database to ensure it's accessible and functioning.", inline=False)
    embed.add_field(name="**`/vacuum_database`**", value="Optimizes the database by reorganizing it, potentially improving performance and reducing file size.", inline=False)
    embed.add_field(name="**`/check_database_integrity`**", value="Verifies the structural integrity of the database, checking for corruption.", inline=False)
    embed.add_field(name="**`/check_database_lock`**", value="Determines if the database is currently locked by any process.", inline=False)
    embed.add_field(name="**`/check_database_permissions`**", value="Verifies that the bot has the necessary permissions to read from and write to the database.", inline=False)
    embed.add_field(name="**`/force_database_write`**", value="Attempts to write a test entry to the database, useful for troubleshooting write issues.", inline=False)
    embed.add_field(name="**`/query_database`**", value="Allows running a custom SQL query on the database for debugging purposes.", inline=False)
    embed.add_field(name="**`/check_wal_mode`**", value="Checks if Write-Ahead Logging (WAL) mode is enabled, which can improve concurrent database access.", inline=False)
    embed.add_field(name="**`/force_checkpoint`**", value="Manually triggers a database checkpoint, writing all changes to the main database file.", inline=False)
    embed.add_field(name="**`/check_db_lock_status`**", value="Provides detailed information about any locks on the database.", inline=False)
    embed.add_field(name="**`/force_close_connections`**", value="Attempts to close all open database connections, useful for resolving lock issues.", inline=False)
    # Add more fields for other debug commands
    return embed

def create_debug_page2_embed():
    embed = discord.Embed(title="__Debug Commands Page 2__", color=discord.Color.red())
    embed.add_field(name="\u200b", value="\u200b", inline=False)  # Blank line
    embed.description = ("The commands list below are only available to server owners and users with the Debug role.\n\n"
                         "These commands are primarily for database management and troubleshooting.\n\n"
                         "They should be used carefully, especially those that modify data or database structure.\n\n")
    embed.add_field(name="**`/kill_db_connections`**", value="Forcefully terminates all database connections, use with caution.", inline=False)
    embed.add_field(name="**`/force_update_blacklist`**", value="Manually updates the blacklist in the database, overriding normal update schedules.", inline=False)
    embed.add_field(name="**`/check_db_processes`**", value="Lists all processes currently interacting with the database.", inline=False)
    embed.add_field(name="**`/check_db_integrity`**", value="Similar to check_database_integrity, but may perform more thorough checks.", inline=False)
    embed.add_field(name="**`/compact_database`**", value="Reduces the size of the database by removing unused space.", inline=False)
    embed.add_field(name="**`/show_db_contents`**", value="Displays a summary of the database contents, useful for quick overviews.", inline=False)
    embed.add_field(name="**`/cleanup_database`**", value="Removes old or unnecessary data from the database to improve performance.", inline=False)
    embed.add_field(name="**`/recreate_database`**", value="Completely rebuilds the database from scratch. Use with extreme caution as it may result in data loss.", inline=False)
    embed.add_field(name="**`/test_warning`**", value="This will simulate a wanring message in your log channel for the logging system.", inline=False)
    embed.add_field(name="**`/rotate_logs`**", value="This will ask the logging system to do a rollover to send you bot log file to your log channel.", inline=False)
    # Add more fields for other debug commands
    return embed

def create_final_thoughts_embed():
    embed = discord.Embed(title="__Final Thoughts__", color=discord.Color.gold())
    embed.description = ("Thank you for using RedditBot!\n\n"
                         "We're constantly working on improvements and new features. "
                         "Stay tuned for updates!")
    embed.add_field(name="", value="[Support Server](https://discord.gg/KWxCVwTD) • [GitHub Repo](https://github.com/Trai60/Reddit-to-Discord-Bot)", inline=False)                     
    # Add any other final thoughts or future plans
    return embed

def generate_help_embeds(user: discord.User, is_debug: bool) -> list[discord.Embed]:
    logger.info(f"Generating help embeds for user {user.name} (ID: {user.id}), is_debug: {is_debug}")
    embeds = [
        create_welcome_embed(user),
        create_forum_settings_embed(),
        create_flair_settings_embed()
    ]
    if is_debug:
        embeds.append(create_debug_page1_embed())
        logger.debug("Debug embed added to help page1 embeds")
        embeds.append(create_debug_page2_embed())
        logger.debug("Debug embed added to help page2 embeds")
    embeds.append(create_final_thoughts_embed())
    logger.debug(f"Generated {len(embeds)} help embeds")
    return embeds
    
@bot.tree.command(name="help", description="Show bot help information")
async def help_command(interaction: discord.Interaction):
    logger.info(f"Help command invoked by user {interaction.user.name} (ID: {interaction.user.id})")
    is_debug = interaction.user.guild_permissions.administrator or discord.utils.get(interaction.user.roles, name="Debug") is not None
    logger.debug(f"User debug status: {is_debug}")
    embeds = generate_help_embeds(interaction.user, is_debug)
    view = HelpView(embeds, interaction.user, is_debug)
    await interaction.response.send_message(embed=embeds[0], view=view, ephemeral=True)
    view.message = await interaction.original_response()
    logger.info(f"Help message sent to user {interaction.user.name} (ID: {interaction.user.id})")

# 13. Background Tasks
# ====================

async def check_new_posts():
    while True:
        logger.info("Starting check for new posts")
        start_time = time.time()

        # Clear the processed_submissions dictionary
        processed_submissions.clear()
        logger.debug("Cleared processed_submissions dictionary")
        
        async with aiohttp.ClientSession() as session:
            reddit = asyncpraw.Reddit(client_id=REDDIT_CLIENT_ID,
                                      client_secret=REDDIT_CLIENT_SECRET,
                                      user_agent=REDDIT_USER_AGENT,
                                      requestor_kwargs={'session': session})
            
            try:
                # Process regular subscriptions
                c.execute("SELECT subreddit, channel_id, last_check, last_submission_id FROM subscriptions")
                subscriptions = c.fetchall()
                logger.debug("Found %d regular subscriptions to process", len(subscriptions))
                
                for subreddit, channel_id, last_check, last_submission_id in subscriptions:
                    logger.debug("Processing regular subscription: r/%s for channel %d", subreddit, channel_id)
                    processed_ids = await process_subscription(reddit, subreddit, channel_id, last_check, last_submission_id)
                    if processed_ids:
                        newest_id = max(processed_ids)
                        c.execute("UPDATE subscriptions SET last_submission_id = ? WHERE subreddit = ? AND channel_id = ?",
                                  (newest_id, subreddit, channel_id))
                        conn.commit()
                        logger.info("Updated last_submission_id for r/%s in channel %d", subreddit, channel_id)
                
                # Process forum subscriptions
                c.execute("SELECT subreddit, channel_id, thread_id, last_check, last_submission_id FROM forum_subscriptions")
                forum_subscriptions = c.fetchall()
                logger.debug("Found %d forum subscriptions to process", len(forum_subscriptions))
                
                for subreddit, channel_id, thread_id, last_check, last_submission_id in forum_subscriptions:
                    logger.debug("Processing forum subscription: r/%s for channel %d, thread %d", subreddit, channel_id, thread_id)
                    await process_forum_subscription(reddit, subreddit, channel_id, thread_id, last_check, last_submission_id)
                
                # Process individual forum subscriptions
                c.execute("SELECT subreddit, channel_id, last_check FROM individual_forum_subscriptions")
                individual_forum_subscriptions = c.fetchall()
                logger.debug("Found %d individual forum subscriptions to process", len(individual_forum_subscriptions))
                
                for subreddit, channel_id, last_check in individual_forum_subscriptions:
                    logger.debug("Processing individual forum subscription: r/%s for channel %d", subreddit, channel_id)
                    await process_individual_forum_subscription(reddit, subreddit, channel_id, last_check)
            
            except Exception as e:
                logger.error("Error occurred while checking for new posts: %s", str(e), exc_info=True)
        
        end_time = time.time()
        duration = round(end_time - start_time, 2)
        logger.info("Finished checking for new posts. Duration: %d seconds", duration)

        # Wait for 2 minutes after the loop finishes
        logger.info("Waiting for 2 minutes before next check")
        await asyncio.sleep(120)

async def check_subreddit(reddit, subreddit_name, channel_id, thread_id, button_visibility):
    logger.info(f"Checking subreddit: r/{subreddit_name} for channel {channel_id}, thread {thread_id}")
    
    channel = bot.get_channel(channel_id)
    if channel is None:
        logger.warning(f"Channel not found: {channel_id}")
        return

    thread = None
    if thread_id:
        thread = bot.get_channel(thread_id)
        if thread is None:
            logger.warning(f"Thread not found: {thread_id}")
            return

    last_check, last_submission_id = await get_tracking(subreddit_name, channel_id)
    logger.debug(f"Last check for r/{subreddit_name}: {last_check}, Last submission ID: {last_submission_id}")

    max_attempts = 3
    attempts = 0
    while attempts < max_attempts:
        try:
            subreddit = await reddit.subreddit(subreddit_name)
            new_last_check = datetime.now(timezone.utc)
            new_last_submission_id = last_submission_id

            new_submissions = await fetch_new_submissions(subreddit, last_check)
            logger.debug(f"Fetched {len(new_submissions)} new submissions for r/{subreddit_name}")
            
            for submission in reversed(new_submissions):
                submission_time = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)
                
                if submission_time <= last_check or submission.id == last_submission_id:
                    break
                
                if new_last_submission_id is None:
                    new_last_submission_id = submission.id

                logger.info(f"New post found in r/{subreddit_name}: {submission.title}")
                
                # Check if the submission is a crosspost
                if hasattr(submission, 'crosspost_parent_list') and submission.crosspost_parent_list:
                    original_post_data = submission.crosspost_parent_list[0]
                    original_submission = asyncpraw.models.Submission(reddit, _data=original_post_data)
                    processing_submission = original_submission
                    processing_submission.subreddit = submission.subreddit
                    processing_submission.title = submission.title
                else:
                    processing_submission = submission

                target = thread if thread else channel
                if isinstance(target, discord.ForumChannel):
                    thread_name = truncate_string(processing_submission.title, 100)
                    
                    try:
                        if hasattr(processing_submission, 'poll_data') and processing_submission.poll_data:
                            embed = await create_simple_reddit_embed(processing_submission)
                            thread = await target.create_thread(name=thread_name, embed=embed)
                        else:
                            image_url = await get_primary_image_url(processing_submission)
                            if image_url:
                                thread = await target.create_thread(name=thread_name, content=image_url)
                            else:
                                embed = await create_simple_reddit_embed(processing_submission)
                                thread = await target.create_thread(name=thread_name, embed=embed)
                        
                        await process_submission(processing_submission, thread.thread, button_visibility)
                    except discord.errors.HTTPException as he:
                        logger.error(f"Discord HTTP error while creating thread for r/{subreddit_name}: {he}")
                        continue
                else:
                    await process_submission(processing_submission, target, button_visibility)

                logger.info(f"Posted to Discord: r/{subreddit_name} - {submission.title}")

            await update_tracking(subreddit_name, channel_id, new_last_check, new_last_submission_id)
            logger.debug(f"Updated tracking for r/{subreddit_name}: Last check: {new_last_check}, Last submission ID: {new_last_submission_id}")
            return  # Exit the function if successful

        except asyncprawcore.exceptions.Forbidden:
            logger.warning(f"Access to r/{subreddit_name} is forbidden. Skipping this subreddit.")
            return
        except asyncprawcore.exceptions.NotFound:
            logger.warning(f"Subreddit r/{subreddit_name} not found. Consider removing this subscription.")
            return
        except asyncprawcore.exceptions.TooManyRequests:
            logger.warning(f"Rate limit hit while processing r/{subreddit_name}. Waiting before next attempt.")
            await asyncio.sleep(60)  # Wait for 60 seconds before next attempt
        except (asyncprawcore.exceptions.ServerError, asyncprawcore.exceptions.RequestException) as e:
            logger.warning(f"Temporary error for r/{subreddit_name}: {str(e)}. Retrying...")
        except (UnicodeEncodeError, UnicodeDecodeError) as ue:
            logger.error(f"Unicode error for subreddit '{subreddit_name}': {ue}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected error for r/{subreddit_name}: {str(e)}", exc_info=True)
        
        attempts += 1
        if attempts >= max_attempts:
            logger.warning(f"Max attempts reached for subreddit '{subreddit_name}'. Skipping.")
        else:
            await asyncio.sleep(5)  # Wait for 5 seconds before trying again

    logger.warning(f"Failed to process subreddit '{subreddit_name}' after {max_attempts} attempts.")

@tasks.loop(hours=24)
async def cleanup_subscriptions():
    logger.info("Starting comprehensive cleanup of stale subscriptions")
    start_time = time.time()

    try:
        # Cleanup forum_subscriptions
        c.execute("SELECT subreddit, channel_id, thread_id FROM forum_subscriptions")
        forum_subscriptions = c.fetchall()
        logger.debug(f"Found {len(forum_subscriptions)} forum subscriptions to check")
        
        forum_subs_removed = 0
        for subreddit, channel_id, thread_id in forum_subscriptions:
            channel = bot.get_channel(channel_id)
            thread = bot.get_channel(thread_id)
            
            if channel is None or not isinstance(channel, discord.ForumChannel):
                logger.warning(f"Removing stale forum subscription: r/{subreddit} in channel {channel_id}, thread {thread_id} - Forum not found")
                c.execute("DELETE FROM forum_subscriptions WHERE subreddit = ? AND channel_id = ?", 
                          (subreddit, channel_id))
                forum_subs_removed += 1
            elif thread is None:
                logger.warning(f"Removing stale forum subscription: r/{subreddit} in channel {channel_id}, thread {thread_id} - Thread not found")
                c.execute("DELETE FROM forum_subscriptions WHERE subreddit = ? AND channel_id = ? AND thread_id = ?", 
                          (subreddit, channel_id, thread_id))
                forum_subs_removed += 1
        
        logger.info(f"Removed {forum_subs_removed} stale forum subscriptions")

        # Cleanup individual_forum_subscriptions
        c.execute("SELECT subreddit, channel_id FROM individual_forum_subscriptions")
        individual_subscriptions = c.fetchall()
        logger.debug(f"Found {len(individual_subscriptions)} individual forum subscriptions to check")
        
        individual_subs_removed = 0
        for subreddit, channel_id in individual_subscriptions:
            channel = bot.get_channel(channel_id)
            
            if channel is None or not isinstance(channel, discord.ForumChannel):
                logger.warning(f"Removing stale individual forum subscription: r/{subreddit} in channel {channel_id} - Forum not found")
                c.execute("DELETE FROM individual_forum_subscriptions WHERE subreddit = ? AND channel_id = ?", 
                          (subreddit, channel_id))
                individual_subs_removed += 1
        
        logger.info(f"Removed {individual_subs_removed} stale individual forum subscriptions")

        # Cleanup regular subscriptions
        c.execute("SELECT subreddit, channel_id FROM subscriptions")
        regular_subscriptions = c.fetchall()
        logger.debug(f"Found {len(regular_subscriptions)} regular subscriptions to check")
        
        regular_subs_removed = 0
        for subreddit, channel_id in regular_subscriptions:
            channel = bot.get_channel(channel_id)
            
            if channel is None:
                logger.warning(f"Removing stale regular subscription: r/{subreddit} in channel {channel_id} - Channel not found")
                c.execute("DELETE FROM subscriptions WHERE subreddit = ? AND channel_id = ?", 
                          (subreddit, channel_id))
                regular_subs_removed += 1
        
        logger.info(f"Removed {regular_subs_removed} stale regular subscriptions")

        conn.commit()
        logger.info("Database changes committed successfully")

    except Exception as e:
        logger.error(f"Error during cleanup_subscriptions: {str(e)}", exc_info=True)
        conn.rollback()
        logger.info("Database changes rolled back due to error")

    end_time = time.time()
    duration = round(end_time - start_time, 2)
    logger.info(f"Comprehensive cleanup of stale subscriptions completed. Duration: {duration} seconds")

@cleanup_subscriptions.before_loop
async def before_cleanup_subscriptions():
    logger.info("Waiting for bot to be ready before starting cleanup_subscriptions task")
    await bot.wait_until_ready()
    logger.info("Bot is ready, starting cleanup_subscriptions task")

@cleanup_subscriptions.after_loop
async def after_cleanup_subscriptions():
    if cleanup_subscriptions.is_being_cancelled():
        logger.warning("cleanup_subscriptions task is being cancelled")
    else:
        logger.info("cleanup_subscriptions task has completed its run")

@tasks.loop(hours=3)
async def consistency_check():
    logger.info("Starting consistency check...")
    start_time = time.time()

    try:
        async with aiohttp.ClientSession() as session:
            reddit = asyncpraw.Reddit(client_id=REDDIT_CLIENT_ID,
                                      client_secret=REDDIT_CLIENT_SECRET,
                                      user_agent=REDDIT_USER_AGENT,
                                      requestor_kwargs={'session': session})

            # Check regular subscriptions
            c.execute("SELECT subreddit, channel_id, last_check, last_submission_id FROM subscriptions")
            subscriptions = c.fetchall()
            logger.info(f"Total regular subscriptions to check: {len(subscriptions)}")
            for i, (subreddit, channel_id, last_check, last_submission_id) in enumerate(subscriptions, 1):
                logger.info(f"Checking regular subscription {i}/{len(subscriptions)}: r/{subreddit}")
                try:
                    processed_ids = await process_subscription(reddit, subreddit, channel_id, last_check, last_submission_id)
                    if processed_ids:
                        newest_id = max(processed_ids)
                        c.execute("UPDATE subscriptions SET last_submission_id = ? WHERE subreddit = ? AND channel_id = ?",
                                  (newest_id, subreddit, channel_id))
                        conn.commit()
                        logger.debug(f"Updated last_submission_id for r/{subreddit} in channel {channel_id}")
                except Exception as e:
                    logger.error(f"Error processing regular subscription for r/{subreddit}: {str(e)}", exc_info=True)
                await asyncio.sleep(2)  # Add a small delay between checks

            # Check forum subscriptions
            c.execute("SELECT subreddit, channel_id, thread_id, last_check, last_submission_id FROM forum_subscriptions")
            forum_subscriptions = c.fetchall()
            logger.info(f"Total forum subscriptions to check: {len(forum_subscriptions)}")
            for i, (subreddit, channel_id, thread_id, last_check, last_submission_id) in enumerate(forum_subscriptions, 1):
                logger.info(f"Checking forum subscription {i}/{len(forum_subscriptions)}: r/{subreddit}")
                try:
                    await process_forum_subscription(reddit, subreddit, channel_id, thread_id, last_check, last_submission_id)
                except Exception as e:
                    logger.error(f"Error processing forum subscription for r/{subreddit}: {str(e)}", exc_info=True)
                await asyncio.sleep(2)  # Add a small delay between checks

            # Check individual forum subscriptions
            c.execute("SELECT subreddit, channel_id, last_check FROM individual_forum_subscriptions")
            individual_forum_subscriptions = c.fetchall()
            logger.info(f"Total individual forum subscriptions to check: {len(individual_forum_subscriptions)}")
            for i, (subreddit, channel_id, last_check) in enumerate(individual_forum_subscriptions, 1):
                logger.info(f"Checking individual forum subscription {i}/{len(individual_forum_subscriptions)}: r/{subreddit}")
                try:
                    await process_individual_forum_subscription(reddit, subreddit, channel_id, last_check)
                except Exception as e:
                    logger.error(f"Error processing individual forum subscription for r/{subreddit}: {str(e)}", exc_info=True)
                await asyncio.sleep(2)  # Add a small delay between checks

    except Exception as e:
        logger.error(f"Error during consistency check: {str(e)}", exc_info=True)

    end_time = time.time()
    duration = round(end_time - start_time, 2)
    logger.info(f"Consistency check completed. Duration: {duration} seconds")

@consistency_check.before_loop
async def before_consistency_check():
    logger.info("Waiting for bot to be ready before starting consistency_check task")
    await bot.wait_until_ready()
    logger.info("Bot is ready, starting consistency_check task")

@consistency_check.after_loop
async def after_consistency_check():
    if consistency_check.is_being_cancelled():
        logger.warning("consistency_check task is being cancelled")
    else:
        logger.info("consistency_check task has completed its run")

# 14. Event Handlers
# ==================

@bot.event
async def on_ready():
    logger.info('Logged in as %s (ID: %s)', bot.user, bot.user.id)
    try:
        if commands_have_changed(bot):
            logger.info("Commands have changed. Starting sync...")
            sync_start = time.time()
            synced = await bot.tree.sync()
            sync_end = time.time()
            logger.info("Synced %d command(s) in %.2f seconds", len(synced), sync_end - sync_start)
            update_command_cache(bot)
        else:
            logger.info("Commands haven't changed. Skipping initial sync.")
        
        logger.info("Setting permissions for debug commands...")
        perm_start = time.time()
        debug_commands = [
            'check_active_transactions', 'check_database', 'vacuum_database', 
            'check_database_integrity', 'check_database_lock', 'check_database_permissions', 
            'force_database_write', 'query_database', 'check_wal_mode', 'force_checkpoint', 
            'check_db_lock_status', 'force_close_connections', 'kill_db_connections', 
            'recreate_database', 'force_update_blacklist', 'check_db_processes', 
            'check_db_integrity', 'compact_database', 'show_db_contents', 'cleanup_database'
        ]
        modified_commands = 0
        for command in bot.tree.get_commands():
            if command.name in debug_commands:
                command.default_permissions = discord.Permissions.none()
                modified_commands += 1
        perm_end = time.time()
        logger.info("Set permissions for %d commands in %.2f seconds", modified_commands, perm_end - perm_start)
        
        if modified_commands > 0:
            logger.info("Permissions changed. Syncing tree again...")
            resync_start = time.time()
            resynced = await bot.tree.sync()
            resync_end = time.time()
            logger.info("Resynced %d commands in %.2f seconds", len(resynced), resync_end - resync_start)
            update_command_cache(bot)
        else:
            logger.info("No permission changes. Skipping resync.")
        
        logger.info("Starting background tasks...")
        tasks_start = time.time()
        bot.loop.create_task(check_new_posts())
        logger.info("check_new_posts task created")
        cleanup_subscriptions.start()
        logger.info("cleanup_subscriptions started")
        consistency_check.start()
        logger.info("consistency_check started")
        periodic_log.start()
        logger.info("periodic_log started")
        tasks_end = time.time()
        logger.info("Started %d background tasks in %.2f seconds", 4, tasks_end - tasks_start)
        logger.info("Bot is fully ready and connected to %d guilds", len(bot.guilds))
        
    except Exception as e:
        logger.error("Error during startup: %s", e, exc_info=True)

last_disconnect_time = None
DISCONNECT_THRESHOLD = 300  # 5 minutes

@bot.event
async def on_disconnect():
    global last_disconnect_time
    last_disconnect_time = time.time()
    logger.info("Bot disconnected from Discord. Attempting to reconnect...")

@bot.event
async def on_connect():
    global last_disconnect_time
    if last_disconnect_time is not None:
        disconnect_duration = time.time() - last_disconnect_time
        if disconnect_duration > DISCONNECT_THRESHOLD:
            logger.warning(f"Bot reconnected after being disconnected for {disconnect_duration:.2f} seconds.")
        else:
            logger.info("Bot successfully reconnected to Discord.")
        last_disconnect_time = None
    else:
        logger.info("Bot connected to Discord.")

@bot.event
async def on_guild_join(guild):
    logger.info(f"Bot joined a new guild: {guild.name} (ID: {guild.id})")

@bot.event
async def on_guild_remove(guild):
    logger.info(f"Bot was removed from guild: {guild.name} (ID: {guild.id})")

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        logger.warning(f"User {ctx.author} (ID: {ctx.author.id}) attempted to use non-existent command: {ctx.message.content}")
    else:
        logger.error(f"An error occurred while executing a command: {error}", exc_info=True)

# 15. Main Execution
# =========================================

if __name__ == "__main__":
    logger.info("Starting bot...")
    try:
        # Run the bot
        bot.run(DISCORD_BOT_TOKEN)
    except discord.LoginFailure:
        logger.critical("Failed to login. Please check your Discord bot token.", exc_info=True)
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        logger.info("Bot is shutting down...")
        # Close the database connection
        try:
            conn.close()
            logger.info("Database connection closed successfully.")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}", exc_info=True)
        
        logger.info("Shutdown complete.")
        logging.shutdown()
        sys.exit(0)