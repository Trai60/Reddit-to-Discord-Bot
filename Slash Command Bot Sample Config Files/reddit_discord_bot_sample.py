import discord
from discord import app_commands
from discord.ext import commands, tasks
import asyncpraw
import asyncio
import aiohttp
import sqlite3
import re
import html
from datetime import datetime, timezone
import asyncprawcore
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv
import os
import json
import tempfile
import aiofiles

# Load environment variables
load_dotenv('.env_reddit')

# Configuration
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = 'DiscordBot/v1.1 by <ANYTHING YOU LIKE HERE>'
DISCORD_BOT_TOKEN = os.getenv('REDDIT_BOT_TOKEN')
MAX_VIDEO_SIZE = 24 * 1024 * 1024  # 24MB in bytes

# Add the truncate_string function here
def truncate_string(string, max_length):
    return (string[:max_length-3] + '...') if len(string) > max_length else string

# Initialize Discord client
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

# Database setup
conn = sqlite3.connect('subscriptions.db')
c = conn.cursor()

# Ensure the tables exist with all required columns
c.execute('''CREATE TABLE IF NOT EXISTS subscriptions
             (subreddit TEXT, channel_id INTEGER, last_check TEXT, last_submission_id TEXT)''')

c.execute('''CREATE TABLE IF NOT EXISTS button_visibility
             (button_name TEXT PRIMARY KEY, is_visible INTEGER)''')

conn.commit()

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

def get_button_visibility():
    c.execute("SELECT button_name, is_visible FROM button_visibility")
    return dict(c.fetchall())

def clean_selftext(selftext):
    cleaned_text = re.sub(r'\[(.*?)\]\((.*?)\)', r'\2', selftext)
    cleaned_text = re.sub(r'[\[\]]', '', cleaned_text)
    cleaned_text = re.sub(r'&nbsp;', ' ', cleaned_text)
    cleaned_text = html.unescape(cleaned_text)
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

async def get_reddit_video_url(submission):
    if submission.media and 'reddit_video' in submission.media:
        video = submission.media['reddit_video']
        fallback_url = video['fallback_url'].split('?')[0]  # Remove anything after .mp4
        return fallback_url, video.get('scrubber_media_url')
    elif hasattr(submission, 'preview') and 'reddit_video_preview' in submission.preview:
        video = submission.preview['reddit_video_preview']
        fallback_url = video['fallback_url'].split('?')[0]  # Remove anything after .mp4
        return fallback_url, video.get('scrubber_media_url')
    elif hasattr(submission, 'secure_media') and submission.secure_media:
        if 'reddit_video' in submission.secure_media:
            video = submission.secure_media['reddit_video']
            fallback_url = video['fallback_url'].split('?')[0] # Remove anything after .mp4
            return fallback_url, video.get('scrubber_media_url')
    return None, None

def extract_video_id(url):
    parsed_url = urlparse(url)
    if parsed_url.netloc in ('youtu.be', 'www.youtu.be'):
        return parsed_url.path[1:]
    if parsed_url.netloc in ('youtube.com', 'www.youtube.com'):
        query = parse_qs(parsed_url.query)
        return query.get('v', [None])[0]
    return None

async def get_youtube_info(video_id):
    url = f"https://www.youtube.com/oembed?url=http://www.youtube.com/watch?v={video_id}&format=json"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data.get('title', 'YouTube Video'), data.get('thumbnail_url')
    return 'YouTube Video', None

async def download_video(url, max_size):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                content = await response.read()
                if len(content) <= max_size:
                    return content
    return None

async def process_submission(submission, channel, button_visibility):
    author_name = submission.author.name if submission.author else "[deleted]"
    author_profile_url = f"https://www.reddit.com/user/{author_name}" if submission.author else None
    
    try:
        if submission.author:
            await submission.author.load()
            author_icon_url = submission.author.icon_img if hasattr(submission.author, 'icon_img') else None
        else:
            author_icon_url = None
    except Exception as e:
        print(f"Error fetching author details: {e}")
        author_icon_url = None
    
    embed = discord.Embed(
        title=truncate_string(submission.title, 256),
        url=f"https://www.reddit.com{submission.permalink}",
        color=discord.Color.green()
    )
    
    embed.set_author(name=author_name, url=author_profile_url, icon_url=author_icon_url)
    embed.set_footer(text=f"r/{submission.subreddit.display_name}")
    embed.timestamp = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)

    view = discord.ui.View()
    if button_visibility.get('Reddit Post', True):
        view.add_item(discord.ui.Button(label="Reddit Post", url=f"https://www.reddit.com{submission.permalink}"))

    if 'redgifs.com' in submission.url:
        fallback_url = None
        if hasattr(submission, 'preview') and 'reddit_video_preview' in submission.preview:
            fallback_url = submission.preview['reddit_video_preview'].get('fallback_url')
    
        if fallback_url:
            fallback_url = fallback_url.split('?')[0]  # Remove anything after .mp4
            video_content = await download_video(fallback_url, MAX_VIDEO_SIZE)
            if video_content:
                with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as temp_file:
                    temp_file.write(video_content)
                    temp_file_path = temp_file.name

                file = discord.File(temp_file_path, filename="redgifs_video.mp4")
                embed.add_field(name="RedGIFs Video", value=submission.url)
                if button_visibility.get('RedGIFs', True):
                    view.add_item(discord.ui.Button(label="RedGIFs Link", url=submission.url))
                await channel.send(embed=embed, view=view)
                await channel.send(file=file)
                os.unlink(temp_file_path)
                return  # Exit the function after sending the video
            else:
                embed.add_field(name="RedGIFs Link", value=submission.url)
                if button_visibility.get('RedGIFs', True):
                    view.add_item(discord.ui.Button(label="RedGIFs Link", url=submission.url))

        else:
            embed.add_field(name="RedGIFs Link", value=submission.url)
            if button_visibility.get('RedGIFs', True):
                view.add_item(discord.ui.Button(label="RedGIFs Link", url=submission.url))
    elif submission.is_self:
        cleaned_text = clean_selftext(submission.selftext)
        embed.description = cleaned_text[:4000] if len(cleaned_text) > 4000 else cleaned_text
    else:
        image_url = extract_image_url(submission)
        if image_url:
            embed.set_image(url=image_url)
        else:
            reddit_video_url, thumbnail_url = await get_reddit_video_url(submission)
            if reddit_video_url:
                video_content = await download_video(reddit_video_url, MAX_VIDEO_SIZE)
                if video_content:
                    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as temp_file:
                        temp_file.write(video_content)
                        temp_file_path = temp_file.name

                    file = discord.File(temp_file_path, filename="reddit_video.mp4")
                    embed.add_field(name="Reddit Video", value=reddit_video_url)
                    if button_visibility.get('Watch Video', True):
                        view.add_item(discord.ui.Button(label="Watch Video", url=reddit_video_url))
                    await channel.send(embed=embed, view=view)
                    await channel.send(file=file)
                    os.unlink(temp_file_path)
                    return  # Exit the function after sending the video
                else:
                    embed.add_field(name="Reddit Video", value=reddit_video_url)
                    embed.set_image(url=thumbnail_url if thumbnail_url else reddit_video_url)
                    if button_visibility.get('Watch Video', True):
                        view.add_item(discord.ui.Button(label="Watch Video", url=reddit_video_url))
            else:
                youtube_id = extract_video_id(submission.url)
                if youtube_id:
                    youtube_title, thumbnail_url = await get_youtube_info(youtube_id)
                    embed.add_field(name=youtube_title, value=f"https://www.youtube.com/watch?v={youtube_id}")
                    if thumbnail_url:
                        embed.set_image(url=thumbnail_url)
                    if button_visibility.get('YouTube Link', True):
                        view.add_item(discord.ui.Button(label="YouTube Link", url=f"https://www.youtube.com/watch?v={youtube_id}"))
                else:
                    if '/gallery/' in submission.url:
                        embed.add_field(name="Image Gallery Link", value=submission.url)
                        if button_visibility.get('Image Gallery', True):
                            view.add_item(discord.ui.Button(label="Image Gallery", url=submission.url))
                    else:
                        embed.add_field(name="Link", value=submission.url)
                        if button_visibility.get('Web Link', True):
                            view.add_item(discord.ui.Button(label="Web Link", url=submission.url))

    await channel.send(embed=embed, view=view)

@tasks.loop(minutes=2)
async def check_new_posts():
    button_visibility = get_button_visibility()
    async with aiohttp.ClientSession() as session:
        reddit = asyncpraw.Reddit(client_id=REDDIT_CLIENT_ID,
                                  client_secret=REDDIT_CLIENT_SECRET,
                                  user_agent=REDDIT_USER_AGENT,
                                  requestor_kwargs={'session': session})
        
        c.execute("SELECT DISTINCT subreddit, channel_id FROM subscriptions")
        subscriptions = c.fetchall()
        
        for subreddit_name, channel_id in subscriptions:
            channel = bot.get_channel(channel_id)
            if channel is None:
                print(f"Channel not found: {channel_id}")
                continue
            
            c.execute("SELECT last_check, last_submission_id FROM subscriptions WHERE subreddit = ? AND channel_id = ?", 
                      (subreddit_name, channel_id))
            result = c.fetchone()
            
            if result:
                last_check, last_submission_id = result
                last_check = datetime.strptime(last_check, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc)
            else:
                last_check = datetime.now(timezone.utc)
                last_submission_id = None
                c.execute("INSERT INTO subscriptions (subreddit, channel_id, last_check, last_submission_id) VALUES (?, ?, ?, ?)",
                          (subreddit_name, channel_id, last_check.strftime("%Y-%m-%d %H:%M:%S.%f"), last_submission_id))
                conn.commit()

            try:
                subreddit = await reddit.subreddit(subreddit_name)
                new_last_check = datetime.now(timezone.utc)
                new_last_submission_id = last_submission_id

                async for submission in subreddit.new(limit=10):
                    submission_time = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)
                    
                    if submission_time <= last_check or submission.id == last_submission_id:
                        break
                    
                    if new_last_submission_id is None:
                        new_last_submission_id = submission.id

                    print(f"New post found: {submission.title}")
                    
                    await process_submission(submission, channel, button_visibility)

                    print(f"Posted to Discord: r/{subreddit_name} - {submission.title}")

                c.execute("UPDATE subscriptions SET last_check = ?, last_submission_id = ? WHERE subreddit = ? AND channel_id = ?",
                          (new_last_check.strftime("%Y-%m-%d %H:%M:%S.%f"), new_last_submission_id, subreddit_name, channel_id))
                conn.commit()

            except asyncprawcore.exceptions.BadRequest as e:
                print(f"BadRequest error for subreddit '{subreddit_name}': {e}")
            except asyncprawcore.exceptions.RequestException as e:
                print(f"RequestException error for subreddit '{subreddit_name}': {e}")
            except Exception as e:
                print(f"An error occurred for subreddit '{subreddit_name}': {e}")

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user}')
    await bot.tree.sync()
    check_new_posts.start()

@bot.tree.command(name="subscribe", description="Subscribe to a subreddit for a specific channel")
@app_commands.describe(
    subreddit="The name of the subreddit to subscribe to",
    channel="The channel to post updates in"
)
async def subscribe(interaction: discord.Interaction, subreddit: str, channel: discord.TextChannel):
    c.execute("SELECT * FROM subscriptions WHERE subreddit = ? AND channel_id = ?", (subreddit, channel.id))
    if c.fetchone():
        await interaction.response.send_message(f"Already subscribed to r/{subreddit} in {channel.mention}")
        return

    c.execute("INSERT INTO subscriptions (subreddit, channel_id, last_check, last_submission_id) VALUES (?, ?, ?, ?)",
              (subreddit, channel.id, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"), None))
    conn.commit()
    await interaction.response.send_message(f"Subscribed to r/{subreddit} in {channel.mention}")

@bot.tree.command(name="unsubscribe", description="Unsubscribe from a subreddit for a specific channel")
@app_commands.describe(
    subreddit="The name of the subreddit to unsubscribe from",
    channel="The channel to unsubscribe from"
)
async def unsubscribe(interaction: discord.Interaction, subreddit: str, channel: discord.TextChannel):
    c.execute("DELETE FROM subscriptions WHERE subreddit = ? AND channel_id = ?", (subreddit, channel.id))
    if c.rowcount > 0:
        conn.commit()
        await interaction.response.send_message(f"Unsubscribed from r/{subreddit} in {channel.mention}")
    else:
        await interaction.response.send_message(f"No subscription found for r/{subreddit} in {channel.mention}")

@bot.tree.command(name="list_subscriptions", description="List all subreddit subscriptions")
async def list_subscriptions(interaction: discord.Interaction):
    c.execute("SELECT subreddit, channel_id FROM subscriptions ORDER BY channel_id, subreddit")
    subscriptions = c.fetchall()
    
    if not subscriptions:
        await interaction.response.send_message("No subscriptions found.")
        return

    response = "Subreddit subscriptions:\n\n"
    current_channel = None
    for subreddit, channel_id in subscriptions:
        channel = bot.get_channel(channel_id)
        if channel:
            if channel != current_channel:
                response += f"#{channel.name}:\n"
                current_channel = channel
            response += f"- r/{subreddit}\n"

    await interaction.response.send_message(response)

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
    if button.value == "all":
        for btn in button_list:
            c.execute("UPDATE button_visibility SET is_visible = ? WHERE button_name = ?", (int(visible), btn))
        message = f"All buttons are now {'visible' if visible else 'hidden'}."
    else:
        c.execute("UPDATE button_visibility SET is_visible = ? WHERE button_name = ?", (int(visible), button.value))
        message = f"The '{button.value}' button is now {'visible' if visible else 'hidden'}."
    
    conn.commit()
    await interaction.response.send_message(message)

@bot.tree.command(name="get_button_visibility", description="Get current visibility settings for message buttons")
async def get_button_visibility_command(interaction: discord.Interaction):
    visibility = get_button_visibility()
    response = "Current button visibility settings:\n\n"
    for button, is_visible in visibility.items():
        response += f"{button}: {'Visible' if is_visible else 'Hidden'}\n"
    await interaction.response.send_message(response)

# Run the bot
bot.run(DISCORD_BOT_TOKEN)