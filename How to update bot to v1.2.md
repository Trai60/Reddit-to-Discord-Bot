# R2D: Reddit to Discord Bot v1.2 Update

## How to update the bot from v1.0 or v1.1 to v.1.2

1. First stop the bot

`sudo systemctl stop reddit_discord_bot.service`

2. Navigate to the location of the reddit_discord_bot.py open the file to be edited

`nano reddit_discord_bot.py`

3. Relace the exsisting code with the updated version.

4. Restart the bot service.

`sudo systemctl start reddit_discord_bot.service`

5. Check status of the bot if required

`sudo systemctl status reddit_discord_bot.service`

## Final Note

Any videos downloaded to the server will automatically remove once a sucessful message has been posted to Discord.

If you're looking at updating the previous hardcoded version (now depreciated) I would recommend a complete reinstall of the bot.