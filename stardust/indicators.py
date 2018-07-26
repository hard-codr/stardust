# https://mrjbq7.github.io/ta-lib/doc_index.html

from talib import abstract as talib


def _get_params(kw, defvals, order):
    r = []
    for k in order:
        if k in kw.keys():
            v = kw[k]
        else:
            v = defvals[k]
        if v is None:
            raise Exception('Mandatory parameter %s not provided' % k)
        r += [v]
    return r


def get_all_indicators():
    return {
        'BBANDS': BBANDS,
        'DEMA': DEMA,
        'EMA': EMA,
        'HT_TRENDLINE': HT_TRENDLINE,
        'KAMA': KAMA,
        'MA': MA,
        'MAMA': MAMA,
        'MAVP': MAVP,
        'MIDPOINT': MIDPOINT,
        'MIDPRICE': MIDPRICE,
        'SAR': SAR,
        'SAREXT': SAREXT,
        'SMA': SMA,
        'T3': T3,
        'TEMA': TEMA,
        'TRIMA': TRIMA,
        'WMA': WMA,
        'MACD': MACD,
    }


def BBANDS(ohlcv, kw):
    """ :return Bollinger Bands (upperband, middleband, lowerband) """
    params = {'timeperiod': 5, 'nbdevup': 2, 'nbdevdn': 2, 'matype': 0}
    timeperiod, nbdevup, nbdevdn, matype = _get_params(kw, params, ['timeperiod', 'nbdevup', 'nbdevdn', 'matype'])
    result = talib.BBANDS(ohlcv, timeperiod, nbdevup, nbdevdn, matype)
    return {
        'upperband': result[0],
        'middleband': result[1],
        'lowerband': result[2]
    }


def DEMA(ohlcv, kw):
    """ :return Double Exponential Moving Average (dema) """
    params = {'timeperiod': 30}
    timeperiod = _get_params(kw, params, ['timeperiod'])[0]
    result = talib.DEMA(ohlcv, timeperiod)
    return {
        'dema': result
    }


def EMA(ohlcv, kw):
    """ :return  Exponential Moving Average (ema) """
    params = {'timeperiod': 30}
    timeperiod = _get_params(kw, params, ['timeperiod'])[0]
    result = talib.EMA(ohlcv, timeperiod)
    return {
        'ema': result
    }


def HT_TRENDLINE(ohlcv, kw):
    """ :return Hilbert Transform - Instantaneous Trendline (trendline) """
    result = talib.HT_TRENDLINE(ohlcv)
    return {
        'trendline': result
    }


def KAMA(ohlcv, kw):
    """ :return Kaufman Adaptive Moving Average (kama) """
    params = {'timeperiod': 30}
    timeperiod = _get_params(kw, params, ['timeperiod'])[0]
    result = talib.KAMA(ohlcv, timeperiod)
    return {
        'kama': result
    }


def MA(ohlcv, kw):
    """ :return Moving Average (ma) """
    params = {'timeperiod': 30, 'matype': 0}
    timeperiod, matype = _get_params(kw, params, ['timeperiod', 'matype'])
    result = talib.MA(ohlcv, timeperiod, matype)
    return {
        'ma': result
    }


def MAMA(ohlcv, kw):
    """ :return MESA Adaptive Moving Average (mama, fama) """
    params = {'fastlimit': 0, 'slowlimit': 0}
    fastlimit, slowlimit = _get_params(kw, params, ['fastlimit', 'slowlimit'])
    result = talib.MAMA(ohlcv, fastlimit, slowlimit)
    return {
        'mama': result[0],
        'fama': result[1]
    }


def MAVP(ohlcv, kw):
    """ :return Moving average with variable period (mavp) """
    params = {'periods': None, 'minperiod': 2, 'maxperiod': 30, 'matype': 0}
    periods, minperiod, maxperiod, matype = _get_params(kw, params, ['periods', 'minperiod', 'maxperiod', 'matype'])
    result = talib.MAVP(ohlcv, periods, minperiod, maxperiod, matype)
    return {
        'mavp': result
    }


def MIDPOINT(ohlcv, kw):
    """ :return MidPoint over period (midpoint) """
    params = {'timeperiod': 14}
    timeperiod = _get_params(kw, params, ['timeperiod'])[0]
    result = talib.MIDPOINT(ohlcv, timeperiod)
    return {
        'midpoint': result
    }


def MIDPRICE(ohlcv, kw):
    """ :return Midpoint Price over period (midprice) """
    params = {'timeperiod': 14}
    timeperiod = _get_params(kw, params, ['timeperiod'])[0]
    result = talib.MIDPRICE(ohlcv, timeperiod)
    return {
        'midprice': result
    }


def SAR(ohlcv, kw):
    """ :return Parabolic SAR (sar) """
    params = {'acceleration': 0, 'maximum': 0}
    acceleration, maximum = _get_params(kw, params, ['acceleration', 'maximum'])
    result = talib.SAR(ohlcv, acceleration, maximum)
    return {
        'sar': result,
    }


def SAREXT(ohlcv, kw):
    """ :return Parabolic SAR - Extended (sarext) """
    params = {'startvalue': 0, 'offsetonreverse': 0, 'accelerationinitlong': 0, 'accelerationlong': 0,
              'accelerationmaxlong': 0, 'accelerationinitshort': 0, 'accelerationshort': 0, 'accelerationmaxshort': 0}
    startvalue, offsetonreverse, accelerationinitlong, accelerationlong, accelerationmaxlong, accelerationinitshort, accelerationshort, accelerationmaxshort = _get_params(
        kw, params, ['startvalue', 'offsetonreverse', 'accelerationinitlong', 'accelerationlong', 'accelerationmaxlong',
                     'accelerationinitshort', 'accelerationshort', 'accelerationmaxshort'])
    result = talib.SAREXT(ohlcv, startvalue, offsetonreverse, accelerationinitlong, accelerationlong,
                          accelerationmaxlong, accelerationinitshort, accelerationshort, accelerationmaxshort)
    return {
        'sarext': result
    }


def SMA(ohlcv, kw):
    """ :return Simple Moving Average (sma) """
    params = {'timeperiod': 30}
    timeperiod = _get_params(kw, params, ['timeperiod'])[0]
    result = talib.SMA(ohlcv, timeperiod)
    return {
        'sma': result
    }


def T3(ohlcv, kw):
    """ :return Triple Exponential Moving Average (t3) """
    params = {'timeperiod': 5, 'vfactor': 0}
    timeperiod, vfactor = _get_params(kw, params, ['timeperiod', 'vfactor'])
    result = talib.T3(ohlcv, timeperiod, vfactor)
    return {
        't3': result
    }


def TEMA(ohlcv, kw):
    """ :return Triple Exponential Moving Average (tema) """
    params = {'timeperiod': 30}
    timeperiod = _get_params(kw, params, ['timeperiod'])[0]
    result = talib.TEMA(ohlcv, timeperiod)
    return {
        'tema': result
    }


def TRIMA(ohlcv, kw):
    """ :return Triangular Moving Average (trima) """
    params = {'timeperiod': 30}
    timeperiod = _get_params(kw, params, ['timeperiod'])[0]
    result = talib.TRIMA(ohlcv, timeperiod)
    return {
        'trima': result
    }


def WMA(ohlcv, kw):
    """ :return Weighted Moving Average (wma) """
    params = {'timeperiod': 30}
    timeperiod = _get_params(kw, params, ['timeperiod'])[0]
    result = talib.WMA(ohlcv, timeperiod)
    return {
        'wma': result
    }


def MACD(ohlcv, kw):
    """ :return Moving Average Convergence/Divergence (macd, macdsignal, macdhist =) """
    params = {'fastperiod': 12, 'slowperiod': 26, 'signalperiod': 9}
    fastperiod, slowperiod, signalperiod = _get_params(kw, params, ['fastperiod', 'slowperiod', 'signalperiod'])
    result = talib.MACD(ohlcv, fastperiod, slowperiod, signalperiod)
    return {
        'macd': result[0],
        'macdsignal': result[1],
        'macdhist': result[2]
    }
