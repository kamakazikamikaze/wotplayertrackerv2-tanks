import asyncpg
from datetime import datetime, timedelta

MASTER_COLUMNS = {
    'account_id': 'integer NOT NULL',
    'tank_id': 'integer NOT NULL',
    'battles': 'integer NOT NULL',
    'console': 'varchar(4) NOT NULL',
    'spotted': 'integer',
    'wins': 'integer',
    'damage_dealt': 'integer',
    'frags': 'integer',
    'dropped_capture_points': 'integer',
    '_last_api_pull': 'timestamp NOT NULL',
}

SERIES_COLUMNS = {
    'account_id': 'integer NOT NULL',
    'tank_id': 'integer NOT NULL',
    'battles': 'integer',
    'console': 'varchar(4)',
    'spotted': 'integer',
    'wins': 'integer',
    'damage_dealt': 'integer',
    'frags': 'integer',
    'dropped_capture_points': 'integer',
    '_date': 'date NOT NULL'
}

async def add_missing_columns(conn, table, schema):
    columns = await conn.fetch(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}' ")
    columns = list(column['column_name'] for column in columns)
    for column, definition in schema.items():
        if column not in columns:
            __ = await conn.execute(f'ALTER TABLE {table} ADD COLUMN {column} {definition}')


async def setup_database(db, use_temp=False):
    now = datetime.utcnow()
    conn = await asyncpg.connect(**db)
    __ = await conn.execute('''
        CREATE TABLE IF NOT EXISTS player_tanks (
            account_id integer NOT NULL,
            tank_id integer NOT NULL,
            battles integer NOT NULL,
            console varchar(4) NOT NULL,
            spotted integer NOT NULL,
            wins integer NOT NULL,
            damage_dealt integer NOT NULL,
            frags integer NOT NULL,
            dropped_capture_points integer NOT NULL,
            _last_api_pull timestamp NOT NULL,
            PRIMARY KEY (account_id, tank_id)
        )''')

    __ = await add_missing_columns(conn, 'player_tanks', MASTER_COLUMNS)

    if use_temp:
        __ = await conn.execute('DROP TABLE IF EXISTS temp_player_tanks')

        __ = await conn.execute('''
            CREATE TABLE temp_player_tanks (
                account_id integer REFERENCES players (account_id),
                tank_id integer NOT NULL,
                battles integer NOT NULL,
                console varchar(4) NOT NULL,
                spotted integer NOT NULL,
                wins integer NOT NULL,
                damage_dealt integer NOT NULL,
                frags integer NOT NULL,
                dropped_capture_points integer NOT NULL,
                _last_api_pull timestamp NOT NULL,
                PRIMARY KEY (account_id, tank_id)
            )''')

    __ = await conn.execute('''
        CREATE TABLE IF NOT EXISTS total_tanks
        (
            account_id integer NOT NULL,
            tank_id integer NOT NULL,
            battles integer,
            console varchar(4),
            spotted integer,
            wins integer,
            damage_dealt integer,
            frags integer,
            dropped_capture_points integer,
            _date date NOT NULL
        ) PARTITION BY RANGE (_date);

        CREATE UNIQUE INDEX IF NOT EXISTS total_tanks_account_id_tank_id_idx
            ON total_tanks USING btree (account_id, tank_id, _date);

        CREATE INDEX IF NOT EXISTS total_tanks_date_idx
            ON total_tanks USING btree (_date);
        ''')

    __ = await add_missing_columns(conn, 'total_tanks', SERIES_COLUMNS)

    __ = await conn.execute('''
        CREATE TABLE IF NOT EXISTS diff_tanks
        (
            account_id integer NOT NULL,
            tank_id integer NOT NULL,
            battles integer,
            console varchar(4),
            spotted integer,
            wins integer,
            damage_dealt integer,
            frags integer,
            dropped_capture_points integer,
            _date date NOT NULL
        ) PARTITION BY RANGE (_date);

        CREATE UNIQUE INDEX IF NOT EXISTS diff_tanks_account_id_tank_id_idx
            ON diff_tanks USING btree (account_id, tank_id, _date);

        CREATE INDEX IF NOT EXISTS diff_tanks_date_idx
            ON diff_tanks USING btree (_date);
        ''')

    __ = await add_missing_columns(conn, 'diff_tanks', SERIES_COLUMNS)

    # Cannot set new columns to NOT NULL until data is retroactively added
    __ = await conn.execute('''
        CREATE TABLE {}
        PARTITION OF total_tanks
        FOR VALUES FROM ('{}') TO ('{}')
        '''.format(
            (now - timedelta(days=1)).strftime('total_tanks_%Y_%m_%d'),
            (now - timedelta(days=1)).strftime('%Y-%m-%d'),
            now.strftime('%Y-%m-%d')
        )
    )

    # Cannot set new columns to NOT NULL until data is retroactively added
    __ = await conn.execute('''
        CREATE TABLE {}
        PARTITION OF diff_tanks
        FOR VALUES FROM ('{}') TO ('{}')
        '''.format(
            (now - timedelta(days=1)).strftime('diff_tanks_%Y_%m_%d'),
            (now - timedelta(days=1)).strftime('%Y-%m-%d'),
            now.strftime('%Y-%m-%d')
        )
    )

    # We shouldn't get a duplicate error because of the REPLACE statement
    try:
        __ = await conn.execute('''
            CREATE OR REPLACE FUNCTION update_tank_total()
              RETURNS trigger AS
            $func$
            BEGIN
              IF (OLD.battles < NEW.battles) THEN
                EXECUTE 'INSERT INTO total_tanks ('
                  'account_id, tank_id, battles, console, spotted, wins, damage_dealt, '
                  'frags, dropped_capture_points, _date'
                  ') VALUES ('
                  '$1.account_id, $1.tank_id, $1.battles, $1.console, $1.spotted, '
                  '$1.wins, $1.damage_dealt, $1.frags, $1.dropped_capture_points, (now() - INTERVAL ''1 DAY'')::date'
                  ') ON CONFLICT DO NOTHING' USING NEW;
                EXECUTE 'INSERT INTO diff_tanks ('
                  'account_id, tank_id, battles, console, spotted, wins, damage_dealt, '
                  'frags, dropped_capture_points, _date'
                  ') VALUES ('
                  '$1.account_id, $1.tank_id, $1.battles - $2.battles, $1.console, '
                  '$1.spotted - $2.spotted, $1.wins - $2.wins, '
                  '$1.damage_dealt - $2.damage_dealt, $1.frags - $2.frags, '
                  '$1.dropped_capture_points - $2.dropped_capture_points, (now() - INTERVAL ''1 DAY'')::date'
                  ') ON CONFLICT DO NOTHING' USING NEW, OLD;
              END IF;
              RETURN NEW;
            END;
            $func$ LANGUAGE plpgsql;''')
    except asyncpg.exceptions.DuplicateObjectError:
        pass

    try:
        __ = await conn.execute('CREATE TRIGGER update_tank_total_table BEFORE UPDATE ON player_tanks FOR EACH ROW EXECUTE PROCEDURE update_tank_total();')
    except asyncpg.exceptions.DuplicateObjectError:
        pass

    # We shouldn't get a duplicate error because of the REPLACE statement.
    # Why do we insert into diff_battles_*? If the player is brand new, then
    # their previous total battle count is 0. We want to include their battles
    # too in each day's count
    try:
        __ = await conn.execute('''
            CREATE OR REPLACE FUNCTION new_tank()
              RETURNS trigger AS
            $func$
            BEGIN
              EXECUTE 'INSERT INTO total_tanks ('
                'account_id, tank_id, battles, console, spotted, wins, '
                'damage_dealt, frags, dropped_capture_points, _date'
                ') VALUES ('
                '$1.account_id, $1.tank_id, $1.battles, $1.console, $1.spotted, '
                '$1.wins, $1.damage_dealt, $1.frags, $1.dropped_capture_points, (now() - INTERVAL ''1 DAY'')::date'
                ') ON CONFLICT DO NOTHING' USING NEW;
              IF (NEW.battles > 0) THEN
                  EXECUTE 'INSERT INTO diff_tanks ('
                    'account_id, tank_id, battles, console, spotted, wins, '
                    'damage_dealt, frags, dropped_capture_points, _date'
                    ') VALUES ('
                    '$1.account_id, $1.tank_id, $1.battles, $1.console, $1.spotted, '
                    '$1.wins, $1.damage_dealt, $1.frags, $1.dropped_capture_points, (now() - INTERVAL ''1 DAY'')::date'
                    ') ON CONFLICT DO NOTHING' USING NEW;
              END IF;
              RETURN NEW;
            END
            $func$ LANGUAGE plpgsql;''')
    except asyncpg.exceptions.DuplicateObjectError:
        pass

    try:
        __ = await conn.execute('CREATE TRIGGER new_tank_total AFTER INSERT ON player_tanks FOR EACH ROW EXECUTE PROCEDURE new_tank();')
    except asyncpg.exceptions.DuplicateObjectError:
        pass

    # In the future, when a more powerful server is being used, have the query server
    # use the following solution instead: https://sqlperformance.com/2020/09/locking/upsert-anti-pattern
    try:
        # https://dba.stackexchange.com/a/123247
        __ = await conn.execute('''
            CREATE OR REPLACE FUNCTION merge_player_tanks()
                RETURNS void AS
            $func$
            DECLARE
                i integer;
            BEGIN
                FOR i IN 0..(SELECT MAX(account_id) / 100 FROM temp_player_tanks)
                LOOP
                    INSERT INTO player_tanks (
                        account_id, tank_id, battles, console, spotted, wins,
                        damage_dealt, frags, dropped_capture_points, _last_api_pull)
                    SELECT * FROM temp_player_tanks
                    WHERE account_id BETWEEN i AND (100 * i)
                    ON CONFLICT (account_id, tank_id) DO UPDATE
                    SET (
                        battles, console, spotted, wins, damage_dealt,
                        frags, dropped_capture_points, _last_api_pull
                    ) = (
                        EXCLUDED.battles, EXCLUDED.console, EXCLUDED.spotted,
                        EXCLUDED.wins, EXCLUDED.damage_dealt, EXCLUDED.frags,
                        EXCLUDED.dropped_capture_points, EXCLUDED._last_api_pull)
                    WHERE player_tanks.battles <> EXCLUDED.battles;
                END LOOP;
            END
            $func$ LANGUAGE plpgsql;
            ''')
    except asyncpg.exceptions.DuplicateObjectError:
        pass
