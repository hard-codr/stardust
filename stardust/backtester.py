import datetime
import getopt
import json
import logging
import os
import sqlite3
import sys
import time

import numpy as np
import yaml

from stardust.data import get_backtest_db, EPOCH, TradeAdvice, Candle, set_db, Backtest
from stardust.strategy import STRATEGY_FACTORY as strategy_factory


def get_asset(code, issuer):
    if code == 'XLM' and issuer == 'native':
        return 'native'
    else:
        return code, issuer


def format_asset(asset):
    if type(asset) == list or type(asset) == tuple:
        return '%s_%s' % (asset[0], asset[1])
    else:
        return asset


class SdexHistory(object):
    NATIVE_ASSET = ('XLM', 'native')

    class TradingPair(object):
        def __init__(self, code, first, second):
            self.code = code
            self.first = first
            self.second = second

        def __repr__(self):
            return self.code

    class Ohlcv(object):
        def __init__(self):
            self.page_token = None
            self.time = []
            self.open = []
            self.high = []
            self.low = []
            self.close = []
            self.volume = []
            self.counter_volume = []

    def __init__(self, sdex_db):
        self.sdex_db = sdex_db
        self.conn = None

    def init(self):
        try:
            self.conn = sqlite3.connect(self.sdex_db)
        except:
            logging.exception('Problem while opening database = %s' % (self.sdex_db,))
            raise Exception('Can\'t connect to db %s' % self.sdex_db)

        return self

    def close(self):
        if self.conn:
            self.conn.close()

    def get_trading_pairs(self, asset=None):
        """
        Return all trading pairs in SDEX or just the ones where input 'asset' is on of the trading asset.
        asset: An asset in trading pair in (code, issuer) format, or SdexHistory.NATIVE_ASSET in case of native asset
        returns: list of TradingPair objects

        e.g.
            stardust.get_trading_pairs(SdexHistory.NATIVE_ASSET)
            stardust.get_trading_pairs(('JPY', 'GBVAOIACNSB7OVUXJYC5UE2D4YK2F7A24T7EE5YOMN4CE6GCHUTOUQXM'))
        """

        if not self.conn:
            raise Exception('No DB connection: call init first')

        where_stmt = ''
        where_params = []

        if asset:
            where_stmt = ' trade_pair like ? and '
            where_params = [asset[0] + '_' + asset[1] + '%']

        if where_stmt:
            where_stmt = where_stmt[:-5]

        if where_stmt:
            cur = self.conn.execute('SELECT distinct(trade_pair) FROM sdex_ohlcv where ' + where_stmt, where_params)
        else:
            cur = self.conn.execute('SELECT distinct(trade_pair) FROM sdex_ohlcv')

        pairs = []
        for row in cur:
            trade_pair = row[0].split('_')
            pairs += [SdexHistory.TradingPair(row[0], (trade_pair[0], trade_pair[1]), (trade_pair[2], trade_pair[3]))]

        return pairs

    @staticmethod
    def resolution_based_projection(resolution, where_clause):
        selections = {
            'min': 'year, month, day, hour, minute',
            '5min': 'year, month, day, hour, minute5',
            '15min': 'year, month, day, hour, minute15',
            '1hr': 'year, month, day, hour',
            '4hr': 'year, month, day, hour4',
            '1d': 'year, month, day',
            '1w': 'year, week',
        }

        if resolution not in selections.keys():
            raise Exception('Invalid resolution. Supported = [min, 5min, 15min, 1hr, 4hr, 1d, 1w]')

        return """
            SELECT mints,
                maxhigh,
                minlow,
                (SELECT open
                    FROM sdex_ohlcv
                    WHERE rowid = minrow) AS firstopen,
                (SELECT close
                    FROM sdex_ohlcv
                    WHERE rowid = maxrow) AS lastclose,
                sum_base_volume,
                sum_counter_volume,
                maxid
            FROM
                (SELECT min(ts) AS mints,
                    min(rowid) AS minrow,
                    max(rowid) AS maxrow,
                    max(high) AS maxhigh,
                    min(low) AS minlow,
                    sum(base_volume) AS sum_base_volume,
                    sum(counter_volume) AS sum_counter_volume,
                    max(id) AS maxid
                FROM sdex_ohlcv
                WHERE %s GROUP BY %s 
                ORDER BY ts) """ % (where_clause, selections[resolution])

    def get_candles(self, trade_pair_code, period_from=None, period_to=None, resolution=None, page_size=100,
                    page_token=None):
        """
        trade_pair: trading pair for which data is needed (mandatory)
        period_from: seconds since epoch indicating start from where SDEX data is needed (optional)
        period_to: seconds since epoch indicating end from where SDEX is needed (optional)
        resolution: [min, 5min, 15min, 1hr, 4hr, 1d, 1w]
        page_size: size of the result set (optional, default:100)
        page_token: page_token to fetch next page (optional)

        returns: Ohlcv containing numpy arrays for open, high, low, close, volume
        """

        if not trade_pair_code:
            raise Exception('Trading pair is mandatory')

        if not self.conn:
            raise Exception('No DB connection: call init first')

        where_stmt = ' trade_pair = ? and '
        where_params = [trade_pair_code]

        if period_from:
            where_stmt += ' ts >= ? and '
            where_params += [period_from]

        if period_to:
            where_stmt += ' ts <= ? and '
            where_params += [period_to]

        where_stmt = where_stmt[:-5]

        if page_token:
            where_stmt = where_stmt + ' and id > ? '
            where_params += [page_token]

        if not resolution or resolution == 'min':
            ohlcv_select_stmt = 'SELECT ts, high, low, open, close, base_volume, counter_volume, id FROM '
            ohlcv_select_stmt += ' ( SELECT * FROM sdex_ohlcv WHERE %s ORDER BY ts)' % (where_stmt,)
        else:
            ohlcv_select_stmt = SdexHistory.resolution_based_projection(resolution, where_stmt)

        ohlcv_select_stmt += ' LIMIT %s' % (page_size,)

        cur = self.conn.execute(ohlcv_select_stmt, where_params)

        result = SdexHistory.Ohlcv()
        i = 0
        for row in cur:
            result.time += [datetime.datetime.utcfromtimestamp(row[0])]
            result.high += [row[1]]
            result.low += [row[2]]
            result.open += [row[3]]
            result.close += [row[4]]
            result.volume += [row[5]]
            result.counter_volume += [row[6]]

            result.page_token = row[7]
            i += 1

        return i, result

    def run(self, backtest_req: Backtest):
        bid, algoname, tradepair, start_ts, end_ts, candlesize, strategyname, parameters = \
            backtest_req.bid, backtest_req.algoname, backtest_req.tradepair, backtest_req.start_ts, \
            backtest_req.end_ts, backtest_req.candlesize, backtest_req.strategyname, backtest_req.parameters

        logging.debug('Starting backtest for bid = %s' % bid)

        try:
            strategy = strategy_factory[strategyname](bid, parameters)
        except Exception as e:
            logging.exception('Error occurred while instantiating strategy')
            return False, str(e)

        page_size = 100

        last_advice = None
        last_bought = 0
        count, candle = self.get_candles(tradepair, start_ts, end_ts, candlesize, page_size)
        logging.debug('Got %s candles to process' % count)
        while True:
            i = 0
            while i < count:
                current_candle = Candle(tradepair)
                current_candle.c_open = candle.open[i]
                current_candle.c_high = candle.high[i]
                current_candle.c_low = candle.low[i]
                current_candle.c_close = candle.close[i]
                current_candle.c_base_volume = candle.volume[i]
                current_candle.c_counter_volume = candle.counter_volume[i]
                current_candle.c_date = candle.time[i]

                strategy.ohlcv['open'] += [candle.open[i]]
                strategy.ohlcv['high'] += [candle.high[i]]
                strategy.ohlcv['low'] += [candle.low[i]]
                strategy.ohlcv['close'] += [candle.close[i]]
                strategy.ohlcv['volume'] += [candle.volume[i]]

                result = {}
                for k, v in strategy.indicator_values.items():
                    # compute indicator
                    ohlcv = {}
                    ohlcv['open'] = np.array(strategy.ohlcv['open'])
                    ohlcv['high'] = np.array(strategy.ohlcv['high'])
                    ohlcv['low'] = np.array(strategy.ohlcv['low'])
                    ohlcv['close'] = np.array(strategy.ohlcv['close'])
                    ohlcv['volume'] = np.array(strategy.ohlcv['volume'])

                    indicator = strategy.indicators[strategy.indicator_type[k]]
                    result[k] = indicator(ohlcv, strategy.indicator_params[k])

                for k, vals in result.items():
                    for param_name, param_val in vals.items():
                        # get the last value of the indicator
                        lastval = param_val[len(param_val) - 1]
                        strategy.indicator_values[k][param_name] = None if lastval == np.nan else lastval

                try:
                    logging.debug('Starting strategy execution for bid = %s' % bid)
                    strategy.process_candle(current_candle)
                    strategy.current_candle = candle
                    strategy.execute(strategy.indicator_values)
                except Exception as e:
                    logging.exception('Strategy generated error')
                    return False, e

                advice = strategy.current_advice
                logging.debug('Done executing. generated advice = %s' % advice)
                if advice:
                    if last_advice and last_advice == advice:
                        logging.info(
                            'Got sequential %s order from bid=%s. Ignoring recent advice.' % (last_advice, bid))
                        continue
                    if not last_advice and advice == TradeAdvice.SELL:
                        logging.info('Sell order without first buy order from bid=%s. Ignoring advice' % bid)
                        continue;

                    logging.debug('Saving %s from strategy %s of backtest_request %s' %
                                  (strategy.current_advice, strategyname, bid))

                    asset_pairs = tradepair.split('_')
                    base_asset = get_asset(asset_pairs[0], asset_pairs[1])
                    counter_asset = get_asset(asset_pairs[2], asset_pairs[3])

                    if advice == TradeAdvice.BUY:
                        sell_asset, buy_asset = base_asset, counter_asset
                        total_sold = 1
                        last_bought = total_bought = current_candle.c_close * total_sold
                    elif advice == TradeAdvice.SELL:
                        sell_asset, buy_asset = counter_asset, base_asset
                        total_sold = last_bought
                        total_bought = total_sold / current_candle.c_close
                    else:
                        logging.error('Algo generated incorrect advice %s' % advice)
                        continue

                    ts = (datetime.datetime.utcnow() - EPOCH).total_seconds()
                    num_tries = 0
                    while num_tries < 3:
                        try:
                            with sqlite3.connect(get_backtest_db()) as db:
                                db.execute("insert into backtest_trades"
                                           "(ts, backtest_id, advice, sold_asset, sold_amount, bought_asset, bought_amount)"
                                           " values (?, ?, ?, ?, ?, ?, ?)",
                                           [ts, bid, strategy.current_advice,
                                            format_asset(sell_asset), float(total_sold),
                                            format_asset(buy_asset), float(total_bought)])
                                db.commit()
                            break
                        except:
                            num_tries += 1
                    else:
                        logging.fatal('Cannot update db after retries')
                        return False, 'Cannot update db after retries'

                    logging.debug('Trade executed for did=%s, sold_asset=%s, sold_amount=%s, '
                                  'bought_asset=%s, bought_amount=%s'
                                  % (bid, sell_asset, total_sold, buy_asset, total_bought))

                    last_advice = advice

                i += 1

            if count < page_size:
                break
            else:
                count, candle = self.get_candles(tradepair, start_ts, end_ts, candlesize, page_size, candle.page_token)
        return True, None


def update_backtest_status(bid, status):
    num_tries = 0
    while num_tries < 3:
        try:
            with sqlite3.connect(get_backtest_db()) as db:
                db.execute("update backtest_request set status = ? where id = ?", [status, bid])
                db.commit()
            break
        except:
            num_tries += 1
    else:
        return False

    return True


def run_backtester():
    while True:
        backtests = []

        num_tries = 0
        while num_tries < 3:
            try:
                logging.debug("Querying backtest_request for new requests")
                with sqlite3.connect(get_backtest_db()) as db:
                    cursor = db.execute("select id, algoname, start_ts, end_ts, "
                                        "tradepair, candlesize, strategyname, parameters "
                                        "from backtest_request where status = ?", [Backtest.STATUS_NEW])

                    for row in cursor:
                        backtests += [
                            Backtest(row[0], row[1], row[2], row[3], row[4], row[5], row[6], json.loads(row[7]))]
                break
            except:
                num_tries += 1
        else:
            time.sleep(1)
            continue

        logging.info("Found %s new backtest_request" % len(backtests))

        sdex_history = SdexHistory(sdex_db=get_backtest_db())
        sdex_history.init()
        try:
            for backtest in backtests:
                if not update_backtest_status(backtest.bid, Backtest.STATUS_RUNNING):
                    continue

                r, err = sdex_history.run(backtest)
                if r:
                    if not update_backtest_status(backtest.bid, Backtest.STATUS_FINISHED):
                        logging.error('Cannot updated db for bid = %s with status = %s',
                                      backtest.bid, Backtest.STATUS_FINISHED)
                elif not update_backtest_status(backtest.bid, Backtest.STATUS_ERROR):
                    logging.error('Cannot updated db for bid = %s with status = %s',
                                  backtest.bid, Backtest.STATUS_ERROR)
        finally:
            sdex_history.close()

        if len(backtests) == 0:
            time.sleep(1)


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

    main_db = 'engine.db'
    backtest_db = 'backtest.db'
    if 'db' in config:
        dbconfig = config['db']
        if 'connection_main' in dbconfig:
            main_db = dbconfig['connection_main']
        if 'connection_backtest' in dbconfig:
            backtest_db = dbconfig['connection_backtest']
    set_db(main_db, backtest_db)

    run_backtester()
