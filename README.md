### Step 1: Install Poetry
[Poetry Installation](https://python-poetry.org/docs/###installation)

### Step 2:
Locate the MySQL database you want to use, or create a new one.
[Download MySQL](https://dev.mysql.com/downloads/mysql/)

### Step 3:
Set your DATABASE and CB_API_KEY environment variable.

Run in the command line, replacing with your own values as appropriate. If you wish this to be permanent, add the line to your .bashrc or .zshrc file.
```
export DATABASE="mysql+mysqlconnector://username:password@host:port/database"
export CB_API_KEY="your_api_key"
```

### Step 4:
Configure google sheet credentials in a file called "scrape.json" in the root directory. See [here](https://developers.google.com/sheets/api/quickstart/python) for more information.

### Step 5:
```
poetry install
poetry shell
python scrape.py
```