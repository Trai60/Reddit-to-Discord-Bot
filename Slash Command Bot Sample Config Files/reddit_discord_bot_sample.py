# Standard library imports
import asyncio
import html
import io
import json
import os
import re
import sqlite3
import tempfile
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs

# Third-party library imports
import aiofiles
import aiohttp
import asyncpraw
import asyncprawcore
import discord
from discord.ext import commands, tasks
from discord import app_commands
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env_reddit')

# Configuration
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = 'DiscordBot/v1.2 by <ANYTHING YOU LIKE HERE>'
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

def ensure_valid_url(url):
    if url.startswith('//'):
        return f'https:{url}'
    elif url.startswith('/'):
        return f'https://www.reddit.com{url}'
    elif not url.startswith(('http://', 'https://')):
        return f'https://{url}'
    return url

def create_button(label, url, button_visibility):
    if button_visibility.get(label, True):
        return discord.ui.Button(label=label, url=ensure_valid_url(url))
    return None

def get_button_visibility():
    c.execute("SELECT button_name, is_visible FROM button_visibility")
    return dict(c.fetchall())

def clean_selftext(selftext):
    # Remove URLs from preview.redd.it and i.redd.it
    cleaned_text = re.sub(r'https?://(?:preview|i)\.redd\.it/\S+', '', selftext)
    
    # Existing cleaning operations
    cleaned_text = re.sub(r'\[(.*?)\]\((.*?)\)', r'\2', cleaned_text)
    cleaned_text = re.sub(r'[\[\]]', '', cleaned_text)
    cleaned_text = re.sub(r'&nbsp;', ' ', cleaned_text)
    cleaned_text = html.unescape(cleaned_text)
    
    # Preserve line breaks and paragraphs
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
    
async def process_selftext_images(selftext, channel, embed, view):
    image_urls = []
    oversized_images = []
    preview_pattern = r'(https://preview\.redd\.it/\w+\.(jpg|png|gif)(?:\S+))'
    i_redd_it_pattern = r'(https://i\.redd\.it/\w+\.(jpg|png|gif))'
    
    # Find all matches for the patterns
    preview_matches = re.findall(preview_pattern, selftext)
    i_redd_it_matches = re.findall(i_redd_it_pattern, selftext)
    
    # Combine all matches
    all_matches = preview_matches + i_redd_it_matches
    
    async with aiohttp.ClientSession() as session:
        for match in all_matches[:10]:  # Limit to 10 images
            full_url = match[0]
            
            # Check image size before adding to image_urls
            async with session.head(full_url) as resp:
                if resp.status == 200:
                    content_length = int(resp.headers.get('Content-Length', 0))
                    if content_length <= MAX_VIDEO_SIZE:
                        image_urls.append(full_url)
                    else:
                        oversized_images.append(full_url)
    
    if image_urls or oversized_images:
        # Create a formatted string for the embed
        image_count = len(all_matches)
        if image_count > 10:
            embed.add_field(name="Reddit Images", value=f"This post contains {image_count} images. Showing up to 10.", inline=False)
        elif image_count > 1:
            embed.add_field(name="Reddit Images", value=f"This post contains {image_count} images.", inline=False)
        elif image_count == 1:
            embed.add_field(name="Reddit Image", value="This post contains 1 image.", inline=False)
        
        if oversized_images:
            embed.add_field(name="Note", value=f"{len(oversized_images)} image{'s' if len(oversized_images) != 1 else ''} exceeded the 24MB size limit and will not be displayed.", inline=False)
        
        # Send the embed with the text content
        await channel.send(embed=embed, view=view)
        
        # Then, send the images
        await send_image_carousel(channel, image_urls)
        
        # Send links for oversized images
        if oversized_images:
            oversized_links = "\n".join(oversized_images)
            await channel.send(f"The following image{'s' if len(oversized_images) > 1 else ''} exceeded the 24MB size limit. You can view them directly:\n{oversized_links}")
        
        return True
    else:
        # If no images, don't send anything here
        return False

def extract_all_images(text):
    # Pattern to match all Reddit image URLs
    image_pattern = r'(https?://(?:i\.redd\.it|preview\.redd\.it)/\S+?\.(?:jpg|png|gif))(?:\?[^\)\s]+)?'
    return re.findall(image_pattern, text)

def extract_captions(text):
    # Pattern to match image captions
    caption_pattern = r'\[(.*?)\]\((https?://(?:i\.redd\.it|preview\.redd\.it)/\S+?\.(?:jpg|png|gif))(?:\?[^\)]+)?\)'
    return re.findall(caption_pattern, text)

def normalize_url(url):
    parsed = urlparse(url)
    path = parsed.path
    return f"https://i.redd.it{path}"

async def process_all_images(submission, channel, embed, view):
    print(f"Debug: Processing submission: {submission.id}")
    
    # Process normal text first
    if submission.selftext:
        cleaned_text = clean_selftext(submission.selftext)
        if cleaned_text:
            embed.description = cleaned_text[:4000]
            print(f"Debug: Added normal text to embed description")

    # Extract all images and captions
    all_images = extract_all_images(submission.selftext)
    captioned_images = extract_captions(submission.selftext)
    
    # Normalize URLs and create a set of unique image URLs
    image_urls = set(normalize_url(url) for url in all_images)
    
    # Create a dictionary to store captions for images that have them
    captions = {normalize_url(url): caption for caption, url in captioned_images}
    
    print(f"Debug: Found {len(image_urls)} unique images")
    print(f"Debug: Found {len(captions)} images with captions")
    
    # Add captions to the embed
    if captions:
        caption_text = "\n\n".join(f"{caption}" for caption in captions.values())
        embed.add_field(name="Image Captions", value=caption_text[:1024], inline=False)
        print(f"Debug: Added captions to embed field")
    
    # Add field for image count only if images are present
    image_count = len(image_urls)
    if image_count > 0:
        if image_count > 1:
            embed.add_field(name="Reddit Images", 
                            value=f"This post contains {image_count} images.", 
                            inline=False)
        else:
            embed.add_field(name="Reddit Image", 
                            value="This post contains 1 image.", 
                            inline=False)
    
    # Send the embed with the text content
    await channel.send(embed=embed, view=view)
    
    # Send the images only if there are any
    if image_urls:
        await send_image_carousel(channel, list(image_urls))
    
    return True

async def send_image_carousel(channel, image_urls):
    print(f"Debug: send_image_carousel received {len(image_urls)} images")
    files = []
    for url in image_urls:
        # Convert preview.redd.it URLs to i.redd.it
        url = url.replace('preview.redd.it', 'i.redd.it')
        print(f"Debug: Processing image URL: {url}")
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    file_extension = url.split('.')[-1].split('?')[0].lower()
                    filename = f"image.{file_extension}"
                    files.append(discord.File(io.BytesIO(data), filename=filename))
                    print(f"Debug: Successfully added image {filename} to files list")
                else:
                    print(f"Debug: Failed to fetch image from {url}. Status code: {resp.status}")
    
    if files:
        print(f"Debug: Sending {len(files)} images to Discord")
        # Send up to 10 images in a single message
        for i in range(0, len(files), 10):
            await channel.send(files=files[i:i+10])
    else:
        print("Debug: No files to send")

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

async def process_reddit_video(submission, channel, button_visibility):
    # Check if the submission meets the conditions for a Reddit Video
    if (hasattr(submission, 'media_metadata') and 
        any(item.get('e') == 'RedditVideo' for item in submission.media_metadata.values())):
        
        # Extract the video URL from selftext
        video_url_match = re.search(r'https://reddit\.com/link/[^/]+/video/[^/]+/player', submission.selftext)
        if video_url_match:
            video_url = video_url_match.group(0)
            
            # Create the embed
            embed = discord.Embed(
                title=truncate_string(submission.title, 256),
                url=f"https://www.reddit.com{submission.permalink}",
                color=discord.Color.blue()
            )
            
            # Add author information
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
            
            # Add any text content from the post (excluding the video URL)
            if submission.selftext:
                cleaned_text = clean_selftext(submission.selftext)
                # Remove the video URL from the cleaned text
                cleaned_text = re.sub(re.escape(video_url), '', cleaned_text)
                # Remove any resulting empty lines and leading/trailing whitespace
                cleaned_text = '\n'.join(line for line in cleaned_text.split('\n') if line.strip())
                if cleaned_text:
                    embed.description = cleaned_text[:4000] if len(cleaned_text) > 4000 else cleaned_text
            
            # Add the note about the video
            embed.add_field(name="Reddit Video", value="This video can only be viewed online or via the app.", inline=False)
            
            # Add the Video Link field
            embed.add_field(name="Video Link", value=video_url, inline=False)
            
            # Add subreddit and timestamp to footer
            embed.set_footer(text=f"r/{submission.subreddit.display_name}")
            embed.timestamp = datetime.fromtimestamp(submission.created_utc, tz=timezone.utc)
            
            # Set up the view with buttons
            view = discord.ui.View()
            
            # Add Reddit Post button first
            reddit_post_button = create_button("Reddit Post", f"https://www.reddit.com{submission.permalink}", button_visibility)
            if reddit_post_button:
                view.add_item(reddit_post_button)
            
            # Add Watch Video button second
            watch_video_button = create_button("Watch Video", video_url, button_visibility)
            if watch_video_button:
                view.add_item(watch_video_button)
            
            # Send the message
            await channel.send(embed=embed, view=view)
            return True  # Indicate that we've handled this submission
    
    return False  # Indicate that this submission wasn't handled by this function

async def process_submission(submission, channel, button_visibility):
    try:
        # Ensure the submission is fully loaded
        await submission.load()
    except asyncprawcore.exceptions.RequestException as e:
        print(f"Error loading submission: {e}")
        return  # Exit if we can't load the submission
    
    # First, try to process as a Reddit Video
    if await process_reddit_video(submission, channel, button_visibility):
        return  # If it was a Reddit Video, we're done

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
    reddit_post_button = create_button("Reddit Post", f"https://www.reddit.com{submission.permalink}", button_visibility)
    if reddit_post_button:
        view.add_item(reddit_post_button)

    message_sent = False

    # Process text content (if exists)
    if submission.selftext:
        cleaned_text = clean_selftext(submission.selftext)
        if cleaned_text:
            embed.description = cleaned_text[:4000] if len(cleaned_text) > 4000 else cleaned_text

    # Now check if it's a gallery
    if hasattr(submission, 'is_gallery') and submission.is_gallery:
        gallery_items = submission.gallery_data['items']
        image_urls = []
        oversized_images = []
        async with aiohttp.ClientSession() as session:
            for item in gallery_items[:10]:  # Limit to 10 images
                media_id = item['media_id']
                media_info = submission.media_metadata[media_id]
                image_url = f"https://i.redd.it/{media_id}.{media_info['m'].split('/')[-1]}"
                
                # Check image size before adding to image_urls
                async with session.head(image_url) as resp:
                    if resp.status == 200:
                        content_length = int(resp.headers.get('Content-Length', 0))
                        if content_length <= MAX_VIDEO_SIZE:
                            image_urls.append(image_url)
                        else:
                            oversized_images.append(image_url)

        image_count = len(gallery_items)
        if image_count > 10:
            embed.add_field(name="Image Gallery", value=f"This Reddit Post contains more than 10 images ({image_count} in total)")
        else:
            embed.add_field(name="Image Gallery", value=f"This Reddit Post contains {image_count} image{'s' if image_count != 1 else ''}")

        if oversized_images:
            embed.add_field(name="Note", value=f"{len(oversized_images)} image{'s' if len(oversized_images) != 1 else ''} exceeded the 24MB size limit and will not be displayed.", inline=False)

        embed.add_field(name="Gallery Link", value=submission.url, inline=False)

        # Add Image Gallery button
        gallery_button = create_button("Image Gallery", submission.url, button_visibility)
        if gallery_button:
            view.add_item(gallery_button)

        # Send the initial message with embed and buttons
        await channel.send(embed=embed, view=view)
        message_sent = True

        # Now handle the images
        await send_image_carousel(channel, image_urls)
        
        # Send links for oversized images
        if oversized_images:
            oversized_links = "\n".join(oversized_images)
            await channel.send(f"The following image{'s' if len(oversized_images) > 1 else ''} may exceeded the 25MB size limit. You can view them directly:\n{oversized_links}")
        
        return  # Exit the function after handling the gallery

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
                redgifs_button = create_button("RedGIFs", submission.url, button_visibility)
                if redgifs_button:
                    view.add_item(redgifs_button)
                await channel.send(embed=embed, view=view)
                await channel.send(file=file)
                os.unlink(temp_file_path)
                return  # Exit the function after sending the video
            else:
                embed.add_field(name="RedGIFs Link", value=submission.url)
                redgifs_button = create_button("RedGIFs", submission.url, button_visibility)
                if redgifs_button:
                    view.add_item(redgifs_button)

        else:
            embed.add_field(name="RedGIFs Link", value=submission.url)
            redgifs_button = create_button("RedGIFs", submission.url, button_visibility)
            if redgifs_button:
                view.add_item(redgifs_button)
    elif submission.is_self:
        # Process selftext images
        image_urls = extract_all_images(submission.selftext)
        if image_urls:
            embed.add_field(name="Reddit Images", value=f"This post contains {len(image_urls)} image(s).")
            await channel.send(embed=embed, view=view)
            await send_image_carousel(channel, image_urls)
            message_sent = True
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
                    watch_video_button = create_button("Watch Video", reddit_video_url, button_visibility)
                    if watch_video_button:
                        view.add_item(watch_video_button)
                    await channel.send(embed=embed, view=view)
                    await channel.send(file=file)
                    os.unlink(temp_file_path)
                    return  # Exit the function after sending the video
                else:
                    embed.add_field(name="Reddit Video", value=reddit_video_url)
                    embed.set_image(url=thumbnail_url if thumbnail_url else reddit_video_url)
                    watch_video_button = create_button("Watch Video", reddit_video_url, button_visibility)
                    if watch_video_button:
                        view.add_item(watch_video_button)            
                    
                    # Add the new message for large files
                    embed.add_field(name="Note", value="Due to the Discord App upload limits, you'll need to view this video on Reddit using the link.", inline=False)
            else:
                youtube_id = extract_video_id(submission.url)
                if youtube_id:
                    youtube_title, thumbnail_url = await get_youtube_info(youtube_id)
                    embed.add_field(name=youtube_title, value=f"https://www.youtube.com/watch?v={youtube_id}")
                    if thumbnail_url:
                        embed.set_image(url=thumbnail_url)
                    youtube_button = create_button("YouTube Link", f"https://www.youtube.com/watch?v={youtube_id}", button_visibility)
                    if youtube_button:
                        view.add_item(youtube_button)
                else:
                    if '/gallery/' in submission.url:
                        embed.add_field(name="Image Gallery Link", value=submission.url)
                        gallery_button = create_button("Image Gallery", submission.url, button_visibility)
                        if gallery_button:
                            view.add_item(gallery_button)
                    else:
                        embed.add_field(name="Link", value=submission.url)
                        web_link_button = create_button("Web Link", submission.url, button_visibility)
                        if web_link_button:
                            view.add_item(web_link_button)

    # Send the message only if it hasn't been sent already
    if not message_sent:
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
    # Defer the response immediately
    await interaction.response.defer(ephemeral=True)

    c.execute("SELECT * FROM subscriptions WHERE subreddit = ? AND channel_id = ?", (subreddit, channel.id))
    if c.fetchone():
        await interaction.followup.send(f"Already subscribed to r/{subreddit} in {channel.mention}")
        return

    c.execute("INSERT INTO subscriptions (subreddit, channel_id, last_check, last_submission_id) VALUES (?, ?, ?, ?)",
              (subreddit, channel.id, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"), None))
    conn.commit()
    await interaction.followup.send(f"Subscribed to r/{subreddit} in {channel.mention}")

@bot.tree.command(name="unsubscribe", description="Unsubscribe from a subreddit for a specific channel")
@app_commands.describe(
    subreddit="The name of the subreddit to unsubscribe from",
    channel="The channel to unsubscribe from"
)
async def unsubscribe(interaction: discord.Interaction, subreddit: str, channel: discord.TextChannel):
    # Defer the response immediately
    await interaction.response.defer(ephemeral=True)
    
    c.execute("DELETE FROM subscriptions WHERE subreddit = ? AND channel_id = ?", (subreddit, channel.id))
    if c.rowcount > 0:
        conn.commit()
        await interaction.followup.send(f"Unsubscribed from r/{subreddit} in {channel.mention}")
    else:
        await interaction.followup.send(f"No subscription found for r/{subreddit} in {channel.mention}")

@bot.tree.command(name="list_subscriptions", description="List all subreddit subscriptions")
async def list_subscriptions(interaction: discord.Interaction):
    # Defer the response immediately
    await interaction.response.defer(ephemeral=True)

    c.execute("SELECT subreddit, channel_id FROM subscriptions ORDER BY channel_id, subreddit")
    subscriptions = c.fetchall()
    
    if not subscriptions:
        await interaction.followup.send("No subscriptions found.")
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

    # If the response is too long, split it into multiple messages
    if len(response) > 2000:
        chunks = [response[i:i+2000] for i in range(0, len(response), 2000)]
        for chunk in chunks:
            await interaction.followup.send(chunk)
    else:
        await interaction.followup.send(response)

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