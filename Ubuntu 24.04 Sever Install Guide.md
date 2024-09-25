<p id="bkmrk-step-by-step-guide%3A">Step-by-step guide:</p>
<p id="bkmrk-%23remember-to-replace">#Remember to replace yourusername where you see that with the username you choose</p>
<p id="bkmrk-1%29-update-and-upgrad">1) Update and upgrade your system:</p>
<pre id="bkmrk-sudo-apt-update-%26%26-s"><code class="language-">sudo apt update && sudo apt upgrade -y</code></pre>
<p id="bkmrk-2%29-install-required-">2) Install required software:</p>
<pre id="bkmrk-sudo-apt-install-pyt"><code class="language-">sudo apt install python3 python3-venv python3-pip sqlite3 -y
sudo apt install libffi-dev libssl-dev libjpeg-dev zlib1g-dev -y</code></pre>
<p id="bkmrk-3%29-server-time-setup">3) Server time setup (this will help for log files later on)</p>
<p id="bkmrk-view-the-current-tim">View the current time zone run:</p>
<pre id="bkmrk-timedatectl"><code class="language-">timedatectl</code></pre>
<p id="bkmrk-view-a-list-of-avail">View a list of availble time zones run:</p>
<pre id="bkmrk-timedatectl-list-tim"><code class="language-">timedatectl list-timezones</code></pre>
<p id="bkmrk-once-you-know-which-">Once you know which timezone for your location run:</p>
<pre id="bkmrk-sudo-timedatectl-set"><code class="language-">sudo timedatectl set-timezone Europe/London</code></pre>
<p id="bkmrk-4%29-reboot-the-server">4) Reboot the server</p>
<pre id="bkmrk-reboot"><code class="language-">reboot</code></pre>
<p id="bkmrk-5%29-create-a-new-user">5) Create a new user and switch to it:</p>
<pre id="bkmrk-sudo-adduser-youruse"><code class="language-">sudo adduser yourusername
PASSOWRD OF YOUR CHOICE
sudo usermod -aG sudo yourusername
su - yourusername</code></pre>
<p id="bkmrk-6%29-create-a-director">6) Create a directory for your bot and navigate to it:</p>
<pre id="bkmrk-mkdir-reddit_discord"><code class="language-">mkdir reddit_discord_bot
cd reddit_discord_bot</code></pre>
<p id="bkmrk-7%29-set-up-a-python-v">7) Set up a Python virtual environment:</p>
<pre id="bkmrk-python3--m-venv-venv"><code class="language-">python3 -m venv venv
source venv/bin/activate</code></pre>
<p id="bkmrk-8%29-install-required-">8) Install required Python packages:</p>
<pre id="bkmrk-pip-install-discord."><code class="language-">pip install discord.py[voice] asyncpraw aiohttp Pillow python-dotenv backoff</code></pre>
<p id="bkmrk-9%29-create-.env_reddi">9) Create .env_reddit file</p>
<pre id="bkmrk-nano-.env_reddit"><code class="language-">nano .env_reddit</code></pre>
<p id="bkmrk-10%29-copy-and-paste-t">10) Copy and paste the code from .env_reddit_sample.txt file</p>
<p id="bkmrk-11%29-create-the-main">11) Create the main bot file:</p>
<pre id="bkmrk-nano-reddit_discord_"><code class="language-">nano reddit_discord_bot.py</code></pre>
<p id="bkmrk-12%29-copy-and-paste-t">12) Copy and paste the py code into reddit_discord_bot_sample.py:</p>
<p id="bkmrk-13%29-check-to-see-th">13) Check to see the bot is working after you invite the bot and set the correct permission for it in Discord Server.</p>
<pre id="bkmrk-python-reddit_discor"><code class="language-">python reddit_discord_bot.py</code></pre>
<p id="bkmrk-14%29-create-a-systemd">14) Create a systemd service file: (If running the Bot with more than one instance change the service name)</p>
<pre id="bkmrk-sudo-nano-%2Fetc%2Fsyste"><code class="language-">sudo nano /etc/systemd/system/reddit_discord_bot.service</code></pre>
<p id="bkmrk-15%29-copy-and-paste-t">15) Copy and paste the following content into the service file.</p>
<p id="bkmrk-%23remember-to-replace-1">#Remember to replace yourusername with your chosen Username</p>
<hr id="bkmrk-">
<p id="bkmrk-%5Bunit%5D">[Unit]<br>Description=Reddit Discord Bot<br>After=network.target</p>
<p id="bkmrk-%5Bservice%5D">[Service]<br>ExecStart=/home/yourusername/reddit_discord_bot/venv/bin/python /home/yourusername/reddit_discord_bot/reddit_discord_bot.py<br>WorkingDirectory=/home/yourusername/reddit_discord_bot<br>StandardOutput=inherit<br>StandardError=inherit<br>Restart=always<br>User=yourusername</p>
<p id="bkmrk-%5Binstall%5D">[Install]<br>WantedBy=multi-user.target</p>
<hr id="bkmrk--1">
<p id="bkmrk-16%29-enable-and-start">16) Enable and start the service:</p>
<pre id="bkmrk-sudo-systemctl-enabl"><code class="language-">sudo systemctl enable reddit_discord_bot.service
sudo systemctl start reddit_discord_bot.service</code></pre>
<p id="bkmrk-17%29-check-the-status">17) Check the status of the service:</p>
<pre id="bkmrk-sudo-systemctl-statu"><code class="language-">sudo systemctl status reddit_discord_bot.service</code></pre>
<p id="bkmrk-18%29-stopping-the-ser">18) Stopping the service should need make any changes.</p>
<pre id="bkmrk-sudo-systemctl-stop-"><code class="language-">sudo systemctl stop reddit_discord_bot.service</code></pre>
