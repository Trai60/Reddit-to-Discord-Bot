# R2D: Reddit to Discord Bot v1.3 Update

## How to update the bot from v1.0, v1.1, v1.2 to v1.3

1. First stop the bot

`sudo systemctl stop reddit_discord_bot.service`

2. If you wish to use your local timezone for the new logging system please check the updated server install guide.

3. If you're updating the bot from a previous version add backoff to your install

`pip install --upgrade backoff`

4. Navigate to the location of the reddit_discord_bot.py open the file to be edited

`nano reddit_discord_bot.py`

5. Relace the exsisting code with the updated version.

6. Add a new Log Channel and a Debug Role in your Discord Server and copy the ID's

- The Debug Role does not need any special Discord permissions as you will only need to add this role to anyone you wish to have access to the debug commands that is not a a Discord server owner.

- The Log channel is where the daily log file and any warnings or errors that the logging system will be sent to.


7. Update the .env_reddit file with the new Debug Role ID & Log Channel ID's

`nano .env_reddit`

Add these to the bottom of of the file rembering to the Debug Role & Channel ID's

- DEBUG_ROLE_ID=YOUR DEBUG ROLE ID FROM YOUR DISCORD SERVER
- LOG_CHANNEL_ID=YOUR LOG CHANNEL ID FROM YOUR DISCORD SERVER

8. Restart the bot service.

`sudo systemctl start reddit_discord_bot.service`

9. Check status of the bot if required

`sudo systemctl status reddit_discord_bot.service`

## Final Note

Any videos downloaded to the server will automatically remove once a sucessful message has been posted to Discord.

If you're looking at updating the previous hardcoded version (now depreciated) I would recommend a complete reinstall of the bot.
