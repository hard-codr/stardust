#!/usr/bin/python

import getopt
import json
import logging
import os
import sqlite3
import sys
import time

import stellar
import yaml

from stardust.data import Candle, set_db, get_backtest_db


def perform_recovery():
    start_trade = None
    last_unprocessed_candles = None

    db_conn = sqlite3.connect(get_backtest_db())
    cursor = db_conn.execute('SELECT key, value FROM state')
    for row in cursor:
        if row[0] == 'LAST_HANDLED_TRADE':
            start_trade = row[1]
        elif row[0] == 'UNPROCESSED_CANDLES':
            last_unprocessed_candles = json.loads(row[1])
        else:
            logging.error('Unhandled state variables = %s %s ', row[0], row[1])
    db_conn.close()

    return start_trade, last_unprocessed_candles


def usage():
    print('backtester -c/--config <config-file>')


if __name__ == '__main__':
    try:
        opts, args = getopt.getopt(sys.argv, "c:", ["config="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    configfile = 'engine.yaml'
    for opt, val in opts:
        if opt in ('-c', '--config'):
            configfile = val

    if not os.path.isfile(configfile):
        print('config file %s doesnt exist' % configfile)
        usage()
        sys.exit(2)

    with open(configfile, 'r') as f:
        configcontent = f.read()
    try:
        config = yaml.load(configcontent)
    except yaml.YAMLError as e:
        print('Incorrect config file content. ex = %s' % str(e))
        sys.exit(2)

    loglevels = {
        'fatal': logging.FATAL,
        'error': logging.ERROR,
        'warn': logging.WARN,
        'info': logging.INFO,
        'debug': logging.DEBUG,
    }
    s = logging.INFO
    if 'logging' in config:
        s = config['logging']
        if s not in loglevels:
            print('logging parameter is not valid. valid values = %s ' % str(loglevels.keys()))
        else:
            s = loglevels[s]

    logpath = '/tmp'
    if 'logpath' in config:
        lp = config['logpath']
        if not os.path.exists(lp):
            print('logpath %s doesnt exist. Using default %s' % (lp, logpath))

    logfile = os.path.join(logpath, 'backtester.log')

    root = logging.getLogger()
    root.setLevel(s)
    root.propagate = False
    fh = logging.FileHandler(filename=logfile)
    fh.setLevel(s)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    root.addHandler(fh)

    network = 'test'
    horizon_url = ''
    network_password = ''
    if 'stellar' in config:
        stellarconfig = config['stellar']
        if 'network_type' in stellarconfig:
            nt = stellarconfig['network_type']
            if nt in ('public', 'test'):
                network = nt
            elif nt == 'custom':
                if 'horizon_url' in stellarconfig and 'network_password' in stellarconfig:
                    horizon_url = stellarconfig['horizon_url']
                    network_password = stellarconfig['network_password']
                else:
                    print('Custom stellar network configuration needs horizon_url and network_password')
                    print('Check your configuration engine.yaml')
                    sys.exit(2)
            else:
                print('Incorrect stellar network_type. valid values = public,test,custom')
                print('Check your configuration engine.yaml')
                sys.exit(2)

    logging.info('Using %s stellar network' % network)
    if network == 'public':
        stellar.setup_public_network()
    elif network == 'custom':
        stellar.setup_custom_network(horizon_url, network_password)
    else:
        stellar.setup_test_network()

    main_db = 'engine.db'
    backtest_db = 'backtest.db'
    if 'db' in config:
        dbconfig = config['db']
        if 'connection_main' in dbconfig:
            main_db = dbconfig['connection_main']
        if 'connection_backtest' in dbconfig:
            backtest_db = dbconfig['connection_backtest']
    set_db(main_db, backtest_db)

    start_cursor_ = None
    fetchsize = 100
    fetchwait = 10
    if 'importer' in config:
        importerconfig = config['importer']
        if 'start_from' in importerconfig:
            start_cursor_ = importerconfig['start_from']
        if 'fetch_size' in importerconfig:
            fetchsize = importerconfig['fetch_size']
        if 'fetch_wait' in importerconfig:
            fetchwait = importerconfig['fetch_wait']

    start_cursor, unprocessed_candles = perform_recovery()
    if not start_cursor:
        start_cursor = start_cursor_
    if not unprocessed_candles:
        unprocessed_candles = {}

    asset_candles = {}
    for key, value in unprocessed_candles.items():
        asset_candles[key] = Candle(key)  # create new candle
        asset_candles[key].from_dict(value)

    def asset_format(asset):
        if asset.asset_type == 'native':
            res = 'XLM_native'
        else:
            res = asset.asset_code + '_' + asset.asset_issuer
        return res

    try:
        # TODO: improve this code such that it accepts time period to import data
        # so that if start_cursor is not given then derive start_cursor from
        # the time period fetch trades
        logging.info('Requesting entries from start_cursor = %s' % start_cursor)
        p = stellar.trades().fetch(cursor=start_cursor, limit=fetchsize)
    except:
        logging.exception('First request failed. Exiting.')
        sys.exit(-1)

    while True:
        entries = p.entries()
        logging.info('Processing %s entries' % len(entries))
        data = []
        for e in entries:
            base = e.base_asset
            counter = e.counter_asset

            key = asset_format(base) + '_' + asset_format(counter)
            if key not in asset_candles:
                asset_candles[key] = Candle(key)
            if not asset_candles[key].process_row(e):
                data += [asset_candles[key].db_values()]

                asset_candles[key] = Candle(key)  # create new candle
                asset_candles[key].process_row(e)  # process new candle

        unprocessed_candles = {}
        for key, candle in asset_candles.items():
            unprocessed_candles[key] = candle.to_dict()
        unprocessed_candles = json.dumps(unprocessed_candles)

        if len(data) > 0:
            conn = sqlite3.connect(get_backtest_db())
            conn.isolation_level = None
            c = conn.cursor()

            try:
                c.execute('BEGIN')

                c.executemany(
                    'INSERT INTO SDEX_OHLCV(TRADE_PAIR, TS, YEAR, MONTH, WEEK, DAY, HOUR4, HOUR, MINUTE15, MINUTE5, MINUTE,\
                    OPEN, HIGH, LOW, CLOSE, BASE_VOLUME, COUNTER_VOLUME) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    data)

                c.execute('UPDATE state SET value = ? WHERE key = ?', (e.paging_token, 'LAST_HANDLED_TRADE'))
                if c.rowcount == 0:
                    c.execute('INSERT INTO state(value, key) values(?, ?)', (e.paging_token, 'LAST_HANDLED_TRADE'))

                c.execute('UPDATE state SET value = ? WHERE key = ?', (unprocessed_candles, 'UNPROCESSED_CANDLES'))
                if c.rowcount == 0:
                    c.execute('INSERT INTO state(value, key) values(?, ?)',
                              (unprocessed_candles, 'UNPROCESSED_CANDLES'))

                c.execute('COMMIT')

                logging.info('Commited %s rows to db' % len(data))
            except:
                c.execute('ROLLBACK')

                logging.exception('Error occurred while persisting to DB');

            conn.close()

        try:
            logging.info('Sleeping for %s sec' % fetchwait)
            time.sleep(fetchwait)
        except:
            break

        sleep = fetchwait
        while True:
            try:
                logging.info('Requesting entries from start_cursor = %s' % e.paging_token)
                p = p.next()
                sleep = fetchwait
                break
            except:
                logging.info('Paging request failed. Sleeping for %s sec' % sleep)
                if sleep > 600:
                    break
                time.sleep(sleep)
                sleep = sleep * 2

    for k, v in asset_candles.items():
        logging.info('Incomplete %s = %s' % (k, v))
