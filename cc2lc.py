import requests
import sqlite3
import os
import time


class Month:
    def __init__(self, month, year) -> None:
        self.month = month
        self.year = year

    def __lt__(self, other) -> bool:
        if not isinstance(other, Month):
            raise RuntimeError(f'Cannot compare Month with {other}')
        if self.year < other.year:
            return True
        return self.month < other.month

    def __eq__(self, other) -> bool:
        if not isinstance(other, Month):
            raise RuntimeError(f'Cannot compare Month with {other}')
        return self.year == other.year and self.month == other.month


def create_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
CREATE TABLE IF NOT EXISTS months(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    month INTEGER NOT NULL,
    year INTEGER NOT NULL
);
""")
    conn.execute("""
CREATE TABLE IF NOT EXISTS games(
    id INTEGER PRIMARY KEY AUTOINCREMENT, 
    uuid TEXT NOT NULL,
    pgn TEXT NOT NULL,
    lc_url TEXT NOT NULL,
    cc_url TEXT NOT NULL,
    time_control TEXT NOT NULL,
    white TEXT NOT NULL,
    white_url TEXT NOT NULL,
    white_rating INTEGER NOT NULL,
    black TEXT NOT NULL,
    black_url TEXT NOT NULL,
    black_rating INTEGER NOT NULL,
    result TEXT NOT NULL
);
""")
    conn.execute("""
CREATE TABLE IF NOT EXISTS month_games(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    month_id INTEGER NOT NULL,
    game_id INTEGER NOT NULL,
    FOREIGN KEY (month_id) REFERENCES months(id),
    FOREIGN KEY (game_id) REFERENCES games(id)
);
""")
    conn.commit()


def get_db() -> sqlite3.Connection:
    db_name = 'cc2lc.db'
    conn = sqlite3.connect(db_name)
    create_table(conn)
    return conn


def archive_url_extract_month(url: str) -> Month:
    parts = url.split('/')
    year = int(parts[-2])
    month = int(parts[-1])
    return Month(month, year)


def most_recent_month(months: list[Month]) -> int:
    i = 0
    for idx, month in enumerate(months):
        if month < months[i]:
            i = idx
    return i


def export_to_lc(pgn: str) -> str:
    token = os.environ['TOKEN']
    lc_import_url = 'https://lichess.org/api/import'
    lc_headers = {
        'Accept-Encoding': 'gzip, deflate',
        'User-Agent': 'cc2lc',
        'Authorization': f'Bearer {token}'
    }
    data = {'pgn': pgn}
    import_response = requests.post(lc_import_url, headers=lc_headers, data=data)
    while import_response.status_code == 429:
        print('Rate limited! Waiting for a minute ...')
        time.sleep(61)
        print('... resuming')
        import_response = requests.post(lc_import_url, headers=lc_headers, data=data)
    import_response.raise_for_status()
    import_json = import_response.json()
    return import_json['url']


def is_game_exported(conn: sqlite3.Connection, uuid: str) -> bool:
    data = conn.execute('SELECT * from games WHERE uuid = ?', (uuid,)).fetchall()
    return len(data) > 0


def is_month_inserted(conn: sqlite3.Connection, month: Month) -> bool:
    data = conn.execute('SELECT * from months WHERE month = ? AND year = ?',
                        (month.month, month.year)).fetchall()
    return len(data) > 0


def is_game_associated(conn: sqlite3.Connection, game_id: int) -> bool:
    data = conn.execute('SELECT * from month_games WHERE game_id = ?', (game_id,)).fetchall()
    return len(data) > 0


def export_month(month: Month,
                 url: str,
                 conn: sqlite3.Connection,
                 cc_headers) -> None:
    games_response = requests.get(url, headers=cc_headers)
    games_response.raise_for_status()
    games = games_response.json()['games']
    print(f'Exporting {month.month}/{month.year} ...')
    uuids = []
    for game in games:
        uuid = game['uuid']
        uuids.append(uuid)
        if is_game_exported(conn, uuid):
            print(f'Already imported {uuid}, skipping')
            continue
        pgn = game['pgn']
        cc_url = game['url']
        time_control = game['time_control']
        white = game['white']['username']
        white_url = game['white']['@id']
        white_rating = int(game['white']['rating'])
        black = game['black']['username']
        black_url = game['black']['@id']
        black_rating = int(game['black']['rating'])
        if game['white']['result'] == 'win':
            result = 'white'
        elif game['black']['result'] == 'win':
            result = 'black'
        else:
            result = 'draw'
        lc_url = export_to_lc(pgn)
        print(f'Exported game {uuid} to {lc_url}')
        conn.execute("""
INSERT INTO games(uuid, pgn, lc_url, cc_url, time_control, white, white_url, white_rating, black, black_url, black_rating, result)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""",
        (uuid, pgn, lc_url, cc_url, time_control, white, white_url, white_rating, black, black_url, black_rating, result)
        )
        conn.commit()
    if not is_month_inserted(conn, month):
        conn.execute('INSERT INTO months(month, year) VALUES (?, ?)', (month.month, month.year))
        conn.commit()
    month_id = conn.execute('SELECT id FROM months WHERE month = ? AND year = ?',
                            (month.month, month.year)).fetchone()[0]
    for uuid in uuids:
        game_id = conn.execute('SELECT id FROM games WHERE uuid = ?', (uuid,)).fetchone()[0]
        if not is_game_associated(conn, game_id):
            conn.execute('INSERT INTO month_games(game_id, month_id) VALUES (?, ?)',
                         (game_id, month_id))
            conn.commit()
    print(f'... exported {month.month}/{month.year}')


conn = get_db()


cc_username = 'reddocmd'
cc_base_url = 'https://api.chess.com/pub/player'
cc_games_url = f'{cc_base_url}/{cc_username}/games'
cc_archives_url = f'{cc_games_url}/archives'

cc_headers = {
    'Accept-Encoding': 'gzip, deflate',
    'User-Agent': 'cc2lc'
}

archive_response = requests.get(cc_archives_url, headers=cc_headers)
archive_response.raise_for_status()
archives = archive_response.json()['archives']
months = list(map(lambda a: archive_url_extract_month(a), archives))

for url, month in zip(archives, months):
    export_month(month, url, conn, cc_headers)

conn.close()