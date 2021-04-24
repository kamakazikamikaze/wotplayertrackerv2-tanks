import asyncio
from asyncpg import create_pool, connect
from collections import deque
from datetime import datetime
from functools import partial
from ipaddress import ip_address
from json.decoder import JSONDecodeError
import linecache
import logging
from multiprocessing import Process, Manager, cpu_count
from os import mkdir, sep
from os.path import join as pjoin
from os.path import split as psplit
from os.path import exists
from pickle import loads, dumps, UnpicklingError, load, dump, HIGHEST_PROTOCOL
from queue import Empty
from sys import exit
from tornado import ioloop, web, websocket
from tornado.escape import json_decode, json_encode
import tracemalloc

from database import setup_database
from sendtoindexer import create_generator_diffs, create_generator_players
from sendtoindexer import create_generator_players_sync, send_data
from sendtoindexer import create_generator_totals, _send_to_cluster_skip_errors
from utils import genuuid, genhashes, load_config, nested_dd, write_config
# Import APIResult and Player as we will unpickle them. Ignore unused warnings
from utils import create_client_config, create_server_config, APIResult, Tank
from utils import expand_debug_access_ips
#from work import setup_work, calculate_total_batches

workgenerator = None
workpop = 0
assignedwork = None
assignedworkcount = 0
completedcount = 0
totalbatches = 0
timeouts = None
stalework = None
server_config = None
received_queue = None
registered = set()
startwork = False
logger = logging.getLogger('WoTServer')
telelogger = logging.getLogger('Telemetry')
statlogger = logging.getLogger('ServerStats')
db_helpers = None
allowed_debug = None


def _setupLogging(conf):
    if 'logging' in conf:
        formatter = logging.Formatter(
            '%(asctime)s.%(msecs)03d | %(name)s | %(levelname)-8s | %(message)s',
            datefmt='%m-%d %H:%M:%S')
        parent_dir = psplit(conf['logging']['file'])[0]
        if not exists(parent_dir):
            mkdir(parent_dir)
        ch = logging.StreamHandler()
        if conf['logging']['level'].lower() == 'debug':
            level = logging.DEBUG
        elif conf['logging']['level'].lower() == 'info':
            level = logging.INFO
        elif conf['logging']['level'].lower() == 'notice':
            level = logging.NOTICE
        elif conf['logging']['level'].lower() == 'warning':
            level = logging.WARNING
        else:
            level = logging.ERROR
        ch.setLevel(level)
        ch.setFormatter(formatter)
        fh = logging.FileHandler(
            datetime.now().strftime(conf['logging']['file']))
        fh.setLevel(level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        logger.addHandler(ch)
        logger.setLevel(level)
    else:
        nu = logging.NullHandler()
        logger.addHandler(nu)
        logger.setLevel(logging.ERROR)

    if 'telemetry' in conf:
        telelogger.propagate = False
        formatter = logging.Formatter(
            '%(asctime)s.%(msecs)03d,%(message)s',
            datefmt='%H:%M:%S')
        parent_dir = psplit(conf['logging']['file'])[0]
        if not exists(parent_dir):
            mkdir(parent_dir)
        fh = logging.FileHandler(
            datetime.now().strftime(conf['telemetry']['file']))
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        telelogger.addHandler(fh)
        telelogger.setLevel(logging.DEBUG)
        telelogger.debug(
            'IP,Completed,Timeouts,Errors,Empty Queue,Active Queries,Work Queue,Result Queue,Return Queue')
    else:
        nu = logging.NullHandler()
        telelogger.addHandler(nu)
        telelogger.setLevel(logging.ERROR)

    if 'stats' in conf:
        statlogger.propagate = False
        formatter = logging.Formatter(
            '%(asctime)s.%(msecs)03d,%(message)s',
            datefmt='%H:%M:%S')
        parent_dir = psplit(conf['logging']['file'])[0]
        if not exists(parent_dir):
            mkdir(parent_dir)
        fh = logging.FileHandler(
            datetime.now().strftime(conf['stats']['file']))
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        statlogger.addHandler(fh)
        statlogger.setLevel(logging.DEBUG)
        statlogger.debug('Completed,Stale,Assigned,Queue')
    else:
        nu = logging.NullHandler()
        statlogger.addHandler(nu)
        statlogger.setLevel(logging.ERROR)


def move_to_stale(ipaddress, work):
    r"""
    Work that does not return before the timeout is appended to a queue
    """
    global assignedworkcount
    del assignedwork[ipaddress][work[0]]
    stalework.append(work)
    assignedworkcount -= 1


def write_stats():
    statlogger.debug(
        '%i,%i,%i,%i',
        completedcount,
        len(stalework),
        assignedworkcount,
        received_queue.qsize()
    )


class MainHandler(web.RequestHandler):
    r"""
    Quick running check
    """

    def get(self):
        self.write('WoT Console Player Tracker v2 (Tanks) is running!')


class DebugHandler(web.RequestHandler):
    r"""
    Temporary endpoint until StatusHandler is completed
    """

    def get(self, uri=None):
        source_ip = ip_address(self.request.remote_ip)
        if not any(source_ip in network for network in allowed_debug):
            self.set_status(400)
            logger.info(
                'Unauthorized host %s attempted to access debug panel',
                self.request.remote_ip
            )
            return
        if uri is None:
            return
        elif uri == 'hashes':
            self.write(json_encode(hashes))
        elif uri == 'work':
            self.write(json_encode(assignedwork))
        elif uri == 'complete':
            self.write(f'{completedcount} of {totalbatches}')
        elif uri == 'queue':
            self.write(str(received_queue.qsize()))
        elif uri == 'registered':
            self.write(str(registered))
        elif uri == 'stale':
            self.write(str(stalework))
        elif uri == 'assignedcount':
            self.write(str(assignedworkcount))
        elif uri == 'dump':
            try:
                now = datetime.utcnow()
                with open(now.strftime('recovery-tanks-%Y-%m-%d.dump'), 'wb') as f:
                    dump(
                        [workpop, completedcount, stalework],
                        f,
                        HIGHEST_PROTOCOL
                    )
                self.write('Dump successful')
            except Exception as e:
                self.write('Exception occurred: {}'.format(e))
        else:
            self.write('No debug output for: ' + uri)


class UpdateHandler(web.RequestHandler):
    r"""
    Client updates (scripts) as served here
    """

    def get(self):
        try:
            client = json_decode(self.request.body)
        except JSONDecodeError:
            self.set_status(400)
        else:
            if client['hash'] == hashes[client['os']][client['filename']]:
                # 204 - No Content. File matches, no updates to pass along
                self.set_status(204)
            else:
                self.redirect(
                    '/files/' + client['os'] + '/' + client['filename']
                )


class TraceHandler(web.RequestHandler):
    r"""
    View system memory usage and profiling for debugging purposes
    """

    def get(self, uri=None):
        if tracemalloc.is_tracing():
            try:
                uri = 10 if uri is None else int(uri)
            except ValueError:
                uri = 10
            snapshot = tracemalloc.take_snapshot()
            snapshot = snapshot.filter_traces((
                tracemalloc.Filter(False, '<frozen importlib._bootstrap>'),
                tracemalloc.Filter(False, '<unknown>')
            ))
            top_stats = snapshot.statistics('lineno')
            output = []
            for index, stat in enumerate(top_stats[:uri], 1):
                frame = stat.traceback[0]
                filename = sep.join(frame.filename.split(sep)[-2:])
                output.append('#%s: %s:%s: %1.1f KiB<br />' %
                              (index, filename, frame.lineno, stat.size / 1024)
                              )
                line = linecache.getline(frame.filename, frame.lineno).strip()
                if line:
                    output.append('&nbsp;&nbsp;&nbsp;&nbsp;%s<br />' % line)
            self.write(
                '<html><body>' +
                ''.join(output) +
                '</body></html>')
        else:
            self.set_status(404)


class SetupHandler(web.RequestHandler):
    r"""
    Endpoint for fetching client-side configuration parameters
    """

    def initialize(self, serverconfig, clientconfig):
        self.serverconfig = serverconfig
        self.clientconfig = clientconfig

    def get(self):
        client = self.request.remote_ip
        # Previously I thought it best to make a local copy of the config so
        # that we do not have a different GET request overwrite the API key.
        # However, this method is not async and is therefore thread-safe. We
        # can overwrite the self.clientconfig for now without any concern.
        # clientconf = copy(self.clientconfig)
        # Immediately assign to catchall
        self.clientconfig['application_id'] = self.serverconfig[
            'application_id']['catchall']
        for key, subdict in self.serverconfig['application_id'].items():
            if key == 'catchall':
                continue
            if client in subdict['addresses']:
                self.clientconfig['application_id'] = subdict['key']
                self.clientconfig['throttle'] = subdict['throttle']
                break
        self.write(json_encode(self.clientconfig))
        WorkWSHandler.maxwork[client] = self.clientconfig[
            'throttle'] + self.serverconfig['extra tasks']

    def post(self):
        if (self.serverconfig['use whitelist'] and
                self.request.remote_ip in self.serverconfig['whitelist']):
            registered.add(self.request.remote_ip)
        elif self.request.remote_ip in self.serverconfig['blacklist']:
            self.set_status(400)
            logger.info(
                'Blacklisted %s attempted to register',
                self.request.remote_ip)
        else:
            registered.add(self.request.remote_ip)


class WorkWSHandler(websocket.WebSocketHandler):
    r"""
    Endpoint for delegating work to client machines
    """

    wschecks = dict()
    wsconns = set()
    maxwork = dict()

    def get_compression_options(self):
        # TODO: Read in configuration from server.json
        return {'compression_level': 9}

    async def open(self, *args, **kwargs):
        client = self.request.remote_ip
        if client not in registered:
            self.close()
            return
        logger.info('Worker %s joined', genuuid(client))
        await self.send_work()
        WorkWSHandler.wschecks[client] = ioloop.PeriodicCallback(
            self.send_work, 250)
        WorkWSHandler.wschecks[client].start()
        WorkWSHandler.wsconns.add(self)

    async def send_work(self):
        global assignedworkcount
        global workpop
        if not startwork:
            return
        if len(workdone):
            self.close()
            return
        client = self.request.remote_ip
        loop = ioloop.IOLoop.current()
        assignments = []
        while len(assignedwork[client]) < WorkWSHandler.maxwork[client]:
            try:
                work = stalework.pop()
            except IndexError:
                try:
                    work = next(workgenerator)
                    workpop += 1
                    if work is None:
                        raise StopIteration
                except StopIteration:
                    if len(stalework) == 0 and assignedworkcount == 0:
                        workdone.append(True)
                        logger.info('Work done')
                    break
            assignments.append(work)
            assignedwork[client][work[0]] = work
            timeouts[client][work[0]] = ioloop.IOLoop.current().call_later(
                server_config['timeout'],
                move_to_stale,
                client,
                work
            )
            assignedworkcount += 1
        if assignments:
            await self.write_message(dumps(assignments), True)

    def on_close(self):
        client = self.request.remote_ip
        WorkWSHandler.wschecks[client].stop()
        del WorkWSHandler.wschecks[client]
        WorkWSHandler.wsconns.remove(self)
        logger.info('Worker %s disconnected', genuuid(client))

    async def on_message(self, message):
        global assignedworkcount
        global completedcount
        client = self.request.remote_ip
        try:
            results = loads(message)
            received_queue.put_nowait(results)
        except UnpicklingError:
            logger.error('Received bad result message from %s', client)
            return
        try:
            ioloop.IOLoop.current().remove_timeout(
                timeouts[client][results.batch]
            )
        except AttributeError:
            # Server got work from a client that is not assigned to it. This
            # can occur when a client is running on the same machine as the
            # server, especially if IPv4 and IPv6 is enabled. Avoid using a
            # target of "localhost" to mitigate this.
            return
        # Remove timeout first
        del timeouts[client][results.batch]
        try:
            del assignedwork[client][results.batch]
        except KeyError:
            # work has already been moved to stale. How do we correct this?
            pass
        else:
            # Don't decrement count unless assigned work is removed
            assignedworkcount -= 1
        completedcount += 1
        # ioloop.IOLoop.current().add_callback(self.send_work)
        await self.send_work()


class TelemetryWSHandler(websocket.WebSocketHandler):
    r"""
    Endpoint for receiving debugging/statistical data.

    This endpoint is to be used for collecting performance metrics only, such
    as retry count, completed batches, and
    """

    def open(self, *args, **kwargs):
        if self.request.remote_ip not in registered:
            self.close()
            return

    def on_message(self, message):
        telelogger.debug(genuuid(self.request.remote_ip) + message)


# TODO: Modify
async def send_to_elasticsearch(conf, conn, day=datetime.utcnow()):
    r"""
    Send updates to Elasticsearch.

    This method should be called once work has concluded.
    """
    # Generators get exhausted after a single Elasticsearch instance. Although
    # the intent was to save on memory, I'm afraid we need to gather all docs
    totals = create_generator_totals(
        day,
        await conn.fetch('SELECT * FROM total_tanks_{}'.format(
            day.strftime('%Y_%m_%d'))))
    logger.info('ES: Sending totals')
    totals = [t for t in totals]
    await send_data(conf, totals)
    diffs = create_generator_diffs(
        day,
        await conn.fetch('SELECT * FROM diff_tanks_{}'.format(
            day.strftime('%Y_%m_%d'))))
    logger.info('ES: Sending diffs')
    diffs = [d for d in diffs]
    await send_data(conf, diffs)
    player_ids = set.union(
        set(map(lambda p: int(p['_source']['account_id']), totals)),
        set(map(lambda p: int(p['_source']['account_id']), diffs))
    )
    del diffs, totals
    stmt = await conn.prepare('SELECT * FROM players WHERE account_id = $1')
    players = create_generator_players(stmt, player_ids)
    players = [p async for p in players]
    eslog = logging.getLogger('elasticsearch')
    eslog.setLevel(logging.ERROR)
    logger.info('ES: Sending players')
    await send_data(conf, players)
    logger.info('ES: Finished')


async def send_everything_to_elasticsearch(conf, conn):
    tables = await conn.fetch(
        (
            "SELECT table_name FROM information_schema.tables WHERE "
            "table_schema='public' AND table_type='BASE TABLE'"
        )
    )
    for table in tables:
        logger.info('ES: Sending %s', table['table_name'])
        if 'diff_tanks' in table['table_name']:
            diffs = create_generator_diffs(
                datetime.strptime(
                    table['table_name'],
                    'diff_tanks_%Y_%m_%d'),
                await conn.fetch(
                    'SELECT * from {}'.format(table['table_name'])))
            await send_data(conf, diffs)
        elif 'total_tanks' in table['table_name']:
            totals = create_generator_totals(
                datetime.strptime(
                    table['table_name'],
                    'total_tanks_%Y_%m_%d'),
                await conn.fetch(
                    'SELECT * from {}'.format(table['table_name'])))
            await send_data(conf, totals)
        elif 'player_tanks' == table['table_name']:
            players = create_generator_players_sync(
                await conn.fetch('SELECT * FROM player_tanks'))
            await send_data(conf, players)


async def send_missing_players_to_elasticsearch(conf, conn):
    r"""
    Synchronizes players from an existing database to Elasticsearch.

    Only updated players have information sent to ES. If a new cluster
    is added, it will not have all players in it as a result.
    """
    players = [p for p in create_generator_players_sync(
        await conn.fetch('SELECT * FROM player_tanks'))]
    for name, cluster in conf['elasticsearch']['clusters'].items():
        await _send_to_cluster_skip_errors(cluster, players)


async def send_results_to_database(db_pool, res_queue, work_done, par, chi):
    logger = logging.getLogger('WoTServer')
    logger.debug('Process-%i: Async-%i created', par, chi)
    while True:
        if not res_queue.qsize():
            if len(work_done):
                break
            continue
        # Use the async here instead of before the `while` statement. Failure
        # to do so can pin to a specific helper waiting for work instead of
        # context switching to another that already has something to process
        async with db_pool.acquire() as conn:
            try:
                results = res_queue.get_nowait()
            except Empty:
                continue
            try:
                __ = await conn.executemany(
                    (
                        'INSERT INTO temp_player_tanks ('
                        'account_id, tank_id, battles, console)'
                        'VALUES ('
                        '$1::int, '
                        '$2::int, '
                        '$3::int, '
                        '$4::text) '
                        'ON CONFLICT DO NOTHING'
                    ),
                    results[0]
                )
                logger.debug(
                    'Process-%i: Async-%i submitted batch %i',
                    par,
                    chi,
                    results.batch)
            except Exception as e:
                logger.error(
                    'Process-%i: Async-%i encountered: %s',
                    par,
                    chi,
                    e)
                with open('error-tanks-batch-{}.dump'.format(results.batch), 'wb') as f:
                    dump(results, f, HIGHEST_PROTOCOL)
    logger.debug('Process-%i: Async-%i exiting', par, chi)


def result_handler(dbconf, res_queue, work_done, par, pool_size=3):
    logger = logging.getLogger('WoTServer')
    # Not availabile until Python 3.7. Use 3.6-compatible syntax for now
    # asyncio.run(create_helpers(db_pool, res_queue, work_done))
    logger.debug('Creating event loop')
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    db_pool = loop.run_until_complete(
        create_pool(min_size=pool_size, max_size=pool_size, **dbconf))
    logger.debug('Event loop created for Process-%i', par)
    try:
        loop.run_until_complete(
            asyncio.gather(*[
                send_results_to_database(db_pool, res_queue, work_done, par, c)
                for c in range(pool_size)])
        )
    finally:
        loop.close()


async def advance_work(config):
    global completedcount
    conn = await connect(**config['database'])
    logger.info('Fetching data from table')
    result = await conn.fetch('SELECT MAX(account_id) FROM temp_player_tanks')
    for record in result:
        max_account = record['max']
    while True:
        popped = next(workgenerator)
        completedcount += 1
        if popped[1][0] <= max_account <= popped[1][1]:
            break


async def try_exit(config, configpath):
    if len(workdone):
        if WorkWSHandler.wsconns:
            for conn in WorkWSHandler.wsconns:
                conn.close()
            logger.info('Released all clients')

        if received_queue.qsize():
            # We still have data to send to the database. Don't exit yet.
            return
        for helper in db_helpers:
            helper.join()
        logger.info('Proceeding with post-run cleanup')
        exitcall.stop()
        if 'stats' in config:
            serverstatcall.stop()
        update = False
        conn = await connect(**config['database'])
        logger.info('Merging temporary table into primary table')
        __ = await conn.execute('SELECT merge_player_tanks()')
        __ = await conn.execute('DROP TABLE temp_player_tanks')
        logger.info('Dropped temporary table')

        if 'elasticsearch' in config:
            logger.info('Sending data to Elasticsearch')
            await send_to_elasticsearch(config, conn)

        logger.info('Shutting down server')
        ioloop.IOLoop.current().stop()


def make_app(sfiles, serverconfig, clientconfig):
    return web.Application([
        (r"/", MainHandler),
        (r"/setup", SetupHandler,
         dict(serverconfig=serverconfig, clientconfig=clientconfig)),
        (r"/updates", UpdateHandler),
        (r"/work", WorkWSHandler),
        (r"/telemetry", TelemetryWSHandler),
        (r"/debug/([^/]*)", DebugHandler),
        (r"/trace/([^/]*)", TraceHandler),
        (r"/files/nix/([^/]*)", web.StaticFileHandler,
         {'path': pjoin(sfiles, 'nix')}),
        (r"/files/win/([^/]*)", web.StaticFileHandler,
         {'path': pjoin(sfiles, 'win')}),
    ])


def setup_work(config):
    day = datetime.utcnow()
    conn = ioloop.IOLoop.current().run_sync(
        lambda: connect(**config['database']))
    result = ioloop.IOLoop.current().run_sync(
        lambda: conn.fetch(
            'SELECT account_id, console FROM total_battles_{0} UNION SELECT account_id, console FROM diff_battles_{0}'.format(
                day.strftime('%Y_%m_%d'))))
    del conn
    for i, row in enumerate(result):
        yield (i, row['account_id'], row['console'])


def calculate_total_batches(config):
    day = datetime.utcnow()
    conn = ioloop.IOLoop.current().run_sync(
        lambda: connect(**config['database']))
    result = ioloop.IOLoop.current().run_sync(
        lambda: conn.fetch(
            'SELECT account_id, console FROM total_battles_{0} UNION SELECT account_id, console FROM diff_battles_{0}'.format(
                day.strftime('%Y_%m_%d'))))
    return len(result)


if __name__ == '__main__':
    from argparse import ArgumentParser
    agp = ArgumentParser()
    agp.add_argument(
        '-f',
        '--static-files',
        help='Static files to serve',
        default='./files')
    agp.add_argument(
        '-c',
        '--client-config',
        help='Client configuration to use',
        default='./config/client.json')
    agp.add_argument(
        'config',
        help='Server configuration file to use',
        default='./config/server.json')
    agp.add_argument(
        '-g',
        '--generate-config',
        help='Generate first-time configuration',
        default=False,
        action='store_true')
    agp.add_argument(
        '-t',
        '--trace-memory',
        help='Debug memory consumption issues',
        default=False,
        action='store_true')
    agp.add_argument(
        '-p',
        '--processes',
        help='Number of processes to spawn for sending results to database',
        type=int,
        default=(cpu_count() - 1))
    agp.add_argument(
        '-a',
        '--async-helpers',
        help='Number of asynchronous helpers to spawn per result sender',
        type=int,
        default=3)
    agp.add_argument(
        '-r',
        '--recover',
        help='Recover server from a previous dump state',
        type=str)
    agp.add_argument(
        '--aggressive-recover',
        action='store_true',
        help='Recover server from a crash without additional information')
    args = agp.parse_args()

    if args.generate_config:
        create_client_config(args.client_config)
        create_server_config(args.config)
        exit()

    # Reassign arguments
    static_files = args.static_files
    server_config = load_config(args.config)
    client_config = load_config(args.client_config)
    if 'telemetry' in server_config:
        if 'interval' in server_config['telemetry']:
            client_config['telemetry'] = server_config['telemetry']['interval']
        else:
            # Default to 10 seconds
            client_config['telemetry'] = 10000
    else:
        try:
            del client_config['telemetry']
        except KeyError:
            pass

    # Setup server
    manager = Manager()
    workdone = manager.list()
    received_queue = manager.Queue()
    # TODO: Update setup_work to pull from DB
    workgenerator = setup_work(server_config)
    totalbatches = calculate_total_batches(server_config)
    assignedwork = nested_dd()
    timeouts = nested_dd()
    stalework = deque()
    hashes = genhashes(static_files)
    client_config['files'] = list(hashes['win'].keys())
    allowed_debug = expand_debug_access_ips(server_config)
    # TODO: Create a timer to change startwork
    startwork = True
    _setupLogging(server_config)
    if args.trace_memory:
        logger.debug('Starting memory trace')
        tracemalloc.start()

    if args.recover:
        with open(args.recover, 'rb') as f:
            workpop, completedcount, stalework = load(f)
            for __ in range(workpop):
                __ = next(workgenerator)

    if args.aggressive_recover:
        ioloop.IOLoop.current().run_sync(
            lambda: advance_work(server_config))

    try:
        start = datetime.now()
        # Don't set up tables when recovering. We have explicitly coded to exit
        # if the tables already exist. Not sure if we need to modify this
        if not args.recover and not args.aggressive_recover:
            ioloop.IOLoop.current().run_sync(
                lambda: setup_database(server_config['database']))
        app = make_app(static_files, server_config, client_config)
        app.listen(server_config['port'])
        exitcall = ioloop.PeriodicCallback(
            lambda: try_exit(server_config, args.config), 1000)
        if 'stats' in server_config:
            if 'interval' not in server_config['stats']:
                server_config['stats'] = 1
            serverstatcall = ioloop.PeriodicCallback(
                write_stats,
                server_config['stats']['interval'] * 1000
            )
        db_helpers = [
            Process(
                target=result_handler,
                args=(
                    server_config['database'],
                    received_queue,
                    workdone,
                    parent,
                    args.async_helpers
                )
            ) for parent in range(args.processes or 1)
        ]
        for helper in db_helpers:
            helper.start()
        exitcall.start()
        if 'stats' in server_config:
            serverstatcall.start()
        logger.info('Starting server')
        ioloop.IOLoop.current().start()
        end = datetime.now()
        logger.info('Finished')
        logger.info('Total runtime: %s', end - start)
    except KeyboardInterrupt:
        logger.info('Shutting down')
        ioloop.IOLoop.current().stop()
        try:
            exitcall.stop()
            if 'stats' in server_config:
                serverstatcall.stop()
            for helper in db_helpers:
                helper.terminate()
        except NameError:
            pass
