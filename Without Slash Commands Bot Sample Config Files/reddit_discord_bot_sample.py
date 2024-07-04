import discord
from discord.ext import commands
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

# Load environment variables
load_dotenv('.env_reddit')

# Configuration
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = 'Reddit 2 Discord Bot v1.0 by <BY ANYTHING YOU WANT>'
DISCORD_BOT_TOKEN = os.getenv('REDDIT_BOT_TOKEN')

# Button visibility configuration
BUTTON_CONFIG = {
    "show_reddit_post_button": True,
    "show_redgifs_button": True,
    "show_video_button": True,
    "show_youtube_button": True,
    "show_gallery_button": True,
    "show_web_link_button": True
}

# Subreddit subscriptions Remember to change your channel ID and subreddit as required
SUBSCRIPTIONS = [
    {"channel_id": 1234567890123456789, "subreddits": ["AskReddit"]},
    {"channel_id": 1234567890123456789, "subreddits": ["funny", "pics"]},
    {"channel_id": 1234567890123456789, "subreddits": ["memes", "videos", "Sports"]},
]
#Add more channels and subreddits as needed above

# Initialize Discord client
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

# Database setup
conn = sqlite3.connect('subscriptions.db')
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS subscriptions
             (subreddit TEXT, channel_id INTEGER, last_check TEXT)''')
conn.commit()

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

async def get_youtube_title(video_id):
    url = f"https://www.youtube.com/oembed?url=http://www.youtube.com/watch?v={video_id}&format=json"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data.get('title', 'YouTube Video')
    return 'YouTube Video'

async def get_redgifs_direct_url(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                text = await response.text()
                match = re.search(r'https://\w+\.redgifs\.com/\w+\.mp4', text)
                if match:
                    return match.group(0)
    return None

async def check_new_posts():
    await bot.wait_until_ready()
    
    async with aiohttp.ClientSession() as session:
        reddit = asyncpraw.Reddit(client_id=REDDIT_CLIENT_ID,
                                  client_secret=REDDIT_CLIENT_SECRET,
                                  user_agent=REDDIT_USER_AGENT,
                                  requestor_kwargs={'session': session})
        
        while not bot.is_closed():
            for subscription in SUBSCRIPTIONS:
                channel_id = subscription['channel_id']
                subreddits = subscription['subreddits']
                
                channel = bot.get_channel(channel_id)
                if channel is None:
                    print(f"Channel not found: {channel_id}")
                    continue
                
                for subreddit_name in subreddits:
                    c.execute("SELECT last_check FROM subscriptions WHERE subreddit = ? AND channel_id = ?", 
                              (subreddit_name, channel_id))
                    result = c.fetchone()
                    
                    if result:
                        last_check = datetime.strptime(result[0], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc)
                    else:
                        last_check = datetime.now(timezone.utc)
                        c.execute("INSERT INTO subscriptions (subreddit, channel_id, last_check) VALUES (?, ?, ?)",
                                  (subreddit_name, channel_id, last_check.strftime("%Y-%m-%d %H:%M:%S.%f")))
                        conn.commit()
                    
                    try:
                        subreddit = await reddit.subreddit(subreddit_name)
                        async for submission in subreddit.new(limit=5):
                            await submission.load()
                            submission_time = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)
                            if submission_time > last_check:
                                print(f"New post found: {submission.title}")
                                
                                embed = discord.Embed(
                                    title=submission.title,
                                    url=f"https://www.reddit.com{submission.permalink}",
                                    color=discord.Color.green()
                                )
                                
                                author_name = submission.author.name if submission.author else "[deleted]"
                                author_profile_url = f"https://www.reddit.com/user/{author_name}" if submission.author else None
                                
                                try:
                                    if submission.author:
                                        author = await reddit.redditor(author_name, fetch=True)
                                        author_icon_url = author.icon_img if hasattr(author, 'icon_img') else None
                                    else:
                                        author_icon_url = None
                                except Exception as e:
                                    print(f"Error fetching author details: {e}")
                                    author_icon_url = None
                                
                                embed.set_author(name=author_name, url=author_profile_url, icon_url=author_icon_url)
                                embed.set_footer(text=f"r/{subreddit_name}")
                                embed.timestamp = submission_time

                                view = discord.ui.View()
                                if BUTTON_CONFIG["show_reddit_post_button"]:
                                    view.add_item(discord.ui.Button(label="Reddit Post", url=f"https://www.reddit.com{submission.permalink}"))

                                if 'redgifs.com' in submission.url:
                                    direct_url = await get_redgifs_direct_url(submission.url)
                                    if direct_url:
                                        embed.add_field(name="RedGIFs Video", value=direct_url)
                                        if BUTTON_CONFIG["show_redgifs_button"]:
                                            view.add_item(discord.ui.Button(label="RedGIFs Link", url=direct_url))
                                    else:
                                        embed.add_field(name="RedGIFs Link", value=submission.url)
                                        if BUTTON_CONFIG["show_redgifs_button"]:
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
                                            embed.add_field(name="Reddit Video", value=reddit_video_url)
                                            embed.set_image(url=thumbnail_url if thumbnail_url else reddit_video_url)
                                            if BUTTON_CONFIG["show_video_button"]:
                                                view.add_item(discord.ui.Button(label="Watch Video", url=reddit_video_url))
                                        else:
                                            youtube_id = extract_video_id(submission.url)
                                            if youtube_id:
                                                youtube_title = await get_youtube_title(youtube_id)
                                                embed.add_field(name=youtube_title, value=f"https://www.youtube.com/watch?v={youtube_id}")
                                                if BUTTON_CONFIG["show_youtube_button"]:
                                                    view.add_item(discord.ui.Button(label="YouTube Link", url=f"https://www.youtube.com/watch?v={youtube_id}"))
                                            else:
                                                if '/gallery/' in submission.url:
                                                    embed.add_field(name="Image Gallery Link", value=submission.url)
                                                    if BUTTON_CONFIG["show_gallery_button"]:
                                                        view.add_item(discord.ui.Button(label="Image Gallery", url=submission.url))
                                                else:
                                                    embed.add_field(name="Link", value=submission.url)
                                                    if BUTTON_CONFIG["show_web_link_button"]:
                                                        view.add_item(discord.ui.Button(label="Web Link", url=submission.url))

                                await channel.send(embed=embed, view=view)
                                print(f"Posted to Discord: r/{subreddit_name} - {submission.title}")

                        new_last_check = datetime.now(timezone.utc)
                        c.execute("UPDATE subscriptions SET last_check = ? WHERE subreddit = ? AND channel_id = ?",
                                  (new_last_check.strftime("%Y-%m-%d %H:%M:%S.%f"), subreddit_name, channel_id))
                        conn.commit()

                    except asyncprawcore.exceptions.BadRequest as e:
                        print(f"BadRequest error for subreddit '{subreddit_name}': {e}")
                    except asyncprawcore.exceptions.RequestException as e:
                        print(f"RequestException error for subreddit '{subreddit_name}': {e}")
                    except Exception as e:
                        print(f"An error occurred for subreddit '{subreddit_name}': {e}")

            await asyncio.sleep(120)  # Check every 2 minutes

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user}')
    bot.loop.create_task(check_new_posts())

async def main():
    await bot.start(DISCORD_BOT_TOKEN)

if __name__ == "__main__":
    asyncio.run(main())