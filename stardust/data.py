import datetime
import logging

EPOCH = datetime.datetime.utcfromtimestamp(0)

_main_db = 'engine.db'
_backtest_db = 'engine.db'


def set_db(main_db, backtest_db):
    global _main_db, _backtest_db
    _main_db = main_db
    _backtest_db = backtest_db

    logging.info('Using main_db = %s , bactest_db = %s' % (_main_db, _backtest_db))


def get_main_db():
    return _main_db


def get_backtest_db():
    return _backtest_db


class Candle(object):
    # min, 5min, 15min, 1hr, 4hr, 1d, 1w
    CANDLESIZE_1MIN = '1min'
    CANDLESIZE_5MIN = '5min'
    CANDLESIZE_15MIN = '15min'
    CANDLESIZE_1HR = '1hr'
    CANDLESIZE_4HR = '4hr'
    CANDLESIZE_1DAY = '1day'
    CANDLESIZE_1WK = '1wk'

    VALID_CANDLE_SIZES = (
        CANDLESIZE_1MIN,
        CANDLESIZE_5MIN,
        CANDLESIZE_15MIN,
        CANDLESIZE_1HR,
        CANDLESIZE_4HR,
        CANDLESIZE_1DAY,
        CANDLESIZE_1WK,
    )

    def __init__(self, key):
        self.key = key
        self.is_first = True
        self.c_open = 0
        self.c_high = 0
        self.c_low = 0
        self.c_close = 0
        self.c_base_volume = 0
        self.c_counter_volume = 0

        self.c_date = None

    def is_same_candle(self, other_date, size=CANDLESIZE_1MIN):
        if self.c_date.year == other_date.year and self.c_date.month == other_date.month:
            if size == Candle.CANDLESIZE_1WK and self.c_date.strftime('%W') == other_date.strftime('%W'):
                return True
            if size == Candle.CANDLESIZE_1DAY and self.c_date.day == other_date.day:
                return True
            if size == Candle.CANDLESIZE_4HR and (self.c_date.hour / 4) == (other_date.hour / 4):
                return True
            if size == Candle.CANDLESIZE_1HR and self.c_date.hour == other_date.hour:
                return True
            if size == Candle.CANDLESIZE_15MIN and (self.c_date.minute / 15) == (other_date.minute / 15):
                return True
            if size == Candle.CANDLESIZE_5MIN and (self.c_date.minute / 5) == (other_date.minute / 5):
                return True
            if size == Candle.CANDLESIZE_1MIN and self.c_date.minute == other_date.minute:
                return True
        return False

    def process_row(self, row):
        price = float(row.price['n']) / row.price['d']
        if self.is_first:
            self.c_open = price
            self.c_high = price
            self.c_low = price
            self.c_close = price
            self.c_base_volume = float(row.base_amount)
            self.c_counter_volume = float(row.counter_amount)
            self.c_date = row.ledger_close_time
            self.is_first = False
        elif self.is_same_candle(row.ledger_close_time):
            self.c_close = price
            self.c_high = price if price > self.c_high else self.c_high
            self.c_low = price if price < self.c_low else self.c_low
            self.c_base_volume += float(row.base_amount)
            self.c_counter_volume += float(row.counter_amount)
        else:
            # didnt process the row, move to next candle
            return False

        # process the row successfully
        return True

    def to_dict(self):
        values = {
            'ts': (self.c_date - EPOCH).total_seconds(),
            'open': self.c_open,
            'high': self.c_high,
            'low': self.c_low,
            'close': self.c_close,
            'base_volume': self.c_base_volume,
            'counter_volume': self.c_counter_volume,
        }
        return values

    def from_dict(self, values):
        self.c_date = datetime.datetime.utcfromtimestamp(values['ts'])
        self.c_open = values['open']
        self.c_high = values['high']
        self.c_low = values['low']
        self.c_close = values['close']
        self.c_base_volume = values['base_volume']
        self.c_counter_volume = values['counter_volume']
        self.is_first = False

    def __repr__(self):
        return '(%.6f,%.6f,%.6f,%.6f,%.6f,%.6f)' % \
               (self.c_open, self.c_high, self.c_low, self.c_close,
                self.c_base_volume, self.c_counter_volume)

    def db_values(self):
        d = self.c_date
        wk = int(d.strftime('%W'))

        return (self.key, (self.c_date - EPOCH).total_seconds(),
                d.year, d.month, wk, d.day, d.hour / 4, d.hour, d.minute / 15, d.minute / 5, d.minute,
                self.c_open, self.c_high, self.c_low, self.c_close,
                self.c_base_volume, self.c_counter_volume)

    @staticmethod
    def is_valid_candlsize(candlesize):
        if candlesize in Candle.VALID_CANDLE_SIZES:
            return False, 'Not valid candle size. Valid values = %s' % str(Candle.VALID_CANDLE_SIZES)

        return True, 'Valid'


class UserProfile(object):
    def __init__(self, userid, account, account_secret):
        self.userid = userid
        self.account = account
        self.account_secret = account_secret

    def __repr__(self):
        return 'User(%s)' % self.userid


class Algo(object):
    def __init__(self, algoname, tradepair, candlesize, strategyname, parameters):
        self.algoname = algoname
        self.tradepair = tradepair
        self.candlesize = candlesize
        self.strategyname = strategyname
        self.parameters = parameters

    @staticmethod
    def from_dict(kv):
        return Algo(kv['algo_name'], kv['trade_pair'], kv['candle_size'], kv['strategy_name'],
                    kv['strategy_parameters'])

    def __repr__(self):
        return 'Algo(%s %s)' % (self.algoname, self.tradepair)


class Backtest(object):
    STATUS_NEW = 'new'
    STATUS_RUNNING = 'running'
    STATUS_ERROR = 'error'
    STATUS_FINISHED = 'finished'

    def __init__(self, bid, algoname, start_ts, end_ts, tradepair, candlesize, strategyname, parameters):
        self.bid = bid
        self.algoname = algoname
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.tradepair = tradepair
        self.candlesize = candlesize
        self.strategyname = strategyname
        self.parameters = parameters


class DeployedAlgo(object):
    STATUS_NEW = 'new'
    STATUS_RUNNING = 'running'
    STATUS_ERROR = 'error'
    STATUS_FINISHED = 'finished'
    STATUS_STOPPED = 'stopped'

    def __init__(self, algo, id, amount, num_cycles):
        self.algo = algo
        self.id = id
        self.amount = amount
        self.num_cycles = num_cycles

    def __repr__(self):
        return 'DeployedAlgo(%s, %s)' % (self.id, self.algo)


class Engine(object):
    COMMAND_DEPLOY = 'deploy'
    COMMAND_UNDEPLOY = 'undeploy'
    COMMAND_DONE = 'done'
    COMMAND_STOP = 'stop'


class TradeAdvice(object):
    BUY = 'buy'
    SELL = 'sell'

    def __init__(self, user_profile, deployment_id, tradepair, advice, amount, num_cycles):
        self.user_profile = user_profile
        self.deployment_id = deployment_id
        self.tradepair = tradepair
        self.advice = advice
        self.amount = amount
        self.num_cycles = num_cycles

    def __repr__(self):
        return 'TradeAdvice(%s, %s, %s)' % (self.deployment_id, self.tradepair, self.advice)


class Trade(object):
    def __init__(self):
        pass
