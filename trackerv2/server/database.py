import asyncpg
from datetime import datetime


async def setup_database(db):
    conn = await asyncpg.connect(**db)
    __ = await conn.execute('''
        CREATE TABLE player_tanks (
            account_id integer REFERENCES players (account_id),
            tank_id integer NOT NULL,
            battles integer NOT NULL,
            console varchar(4) NOT NULL,
            _last_api_pull timestamp NOT NULL,
            PRIMARY KEY (account_id, tank_id)
        )''')

    __ = await conn.execute('DROP TABLE IF EXISTS temp_player_tanks')

    __ = await conn.execute('''
        CREATE TABLE temp_player_tanks (
            account_id integer REFERENCES players (account_id),
            tank_id integer NOT NULL,
            battles integer NOT NULL,
            console varchar(4) NOT NULL,
            _last_api_pull timestamp NOT NULL,
            PRIMARY KEY (account_id, tank_id)
        )''')

    __ = await conn.execute('''
        CREATE TABLE {} (
            account_id integer REFERENCES players (account_id),
            tank_id integer NOT NULL,
            battles integer NOT NULL,
            console varchar(4)NOT NULL,
            FOREIGN KEY (account_id, tank_id) REFERENCES player_tanks (account_id, tank_id))'''.format(
        datetime.utcnow().strftime('total_tanks_%Y_%m_%d'))
    )

    __ = await conn.execute('''
        CREATE TABLE {} (
            account_id integer REFERENCES players (account_id),
            tank_id integer NOT NULL,
            battles integer NOT NULL,
            console varchar(4) NOT NULL,
            FOREIGN KEY (account_id, tank_id) REFERENCES player_tanks (account_id, tank_id))'''.format(
        datetime.utcnow().strftime('diff_tanks_%Y_%m_%d'))
    )

    # We shouldn't get a duplicate error because of the REPLACE statement
    try:
        __ = await conn.execute('''
            CREATE OR REPLACE FUNCTION update_tank_total()
              RETURNS trigger AS
            $func$
            BEGIN
              IF (OLD.battles < NEW.battles) THEN
                EXECUTE format('INSERT INTO total_tanks_%s (account_id, tank_id, battles, console) VALUES ($1.account_id, $1.tank_id, $1.battles, $1.console) ON CONFLICT DO NOTHING', to_char(timezone('UTC'::text, now()), 'YYYY_MM_DD')) USING NEW;
                EXECUTE format('INSERT INTO diff_tanks_%s (account_id, tank_id, battles, console) VALUES ($1.account_id, $1.tank_id, $1.battles - $2.battles, $1.console) ON CONFLICT DO NOTHING', to_char(timezone('UTC'::text, now()), 'YYYY_MM_DD')) USING NEW, OLD;
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
              EXECUTE format('INSERT INTO total_tanks_%s (account_id, tank_id, battles, console) VALUES ($1.account_id, $1.tank_id, $1.battles, $1.console) ON CONFLICT DO NOTHING', to_char(timezone('UTC'::text, now()), 'YYYY_MM_DD')) USING NEW;
              IF (NEW.battles > 0) THEN
                  EXECUTE format('INSERT INTO diff_tanks_%s (account_id, tank_id, battles, console) VALUES ($1.account_id, $1.tank_id, $1.battles, $1.console) ON CONFLICT DO NOTHING', to_char(timezone('UTC'::text, now()), 'YYYY_MM_DD')) USING NEW;
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

    try:
        # https://dba.stackexchange.com/a/123247
        __ = await conn.execute('''
            CREATE OR REPLACE FUNCTION merge_player_tanks()
                RETURNS void AS
            $func$
            DECLARE
                i integer;
            BEGIN
                FOR i IN 0..(SELECT MAX(account_id) / 1000 FROM temp_player_tanks)
                LOOP
                    INSERT INTO player_tanks (
                        account_id, tank_id, battles, console)
                    SELECT * FROM temp_player_tanks
                    WHERE account_id > (1000 * i) AND account_id <= (1000 * i)
                    ON CONFLICT DO UPDATE
                    SET (battles, console) = (EXCLUDED.battles, EXCLUDED.console)
                    WHERE player_tanks.battles <> EXCLUDED.battles;
                END LOOP;
            END
            $func$ LANGUAGE plpgsql;
            ''')
    except asyncpg.exceptions.DuplicateObjectError:
        pass
