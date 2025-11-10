# Spin up Virtual Environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# *** To run the fetcher manually, use the command below ***
python3 -m filingfetcher --user-agent "FilingFetcher/0.1 (floridamanfinance@gmail.com)" --poll 7 --validate 300


# *** Run the launchctl job manually ***
###launchctl kickstart -k gui/$(id -u)/com.you.OCC_Fetcher