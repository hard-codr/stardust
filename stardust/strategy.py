import asyncio
import logging

import numpy as np

import stardust.indicators as ind
from stardust.data import TradeAdvice


class TradingException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)


# Strategy that needs to be impletemented to customize
class BaseTradingStrategy(object):
    SLEEP_TIME = 1
    CANDLE_HISTORY_MIN = 1440

    def __init__(self):
        self.deployment_id = None
        self.parameters = None
        self.candle_pipeline = None
        self.order_pipeline = None
        self.indicators = None

        self.current_advice = None
        self.current_candle = None
        self.indicator_params = {}
        self.indicator_values = {}
        self.ohlcv = None

    def setup(self, deployment_id, parameters, in_candle_pipeline, out_order_pipeline):
        self.deployment_id = deployment_id
        self.parameters = parameters
        self.candle_pipeline = in_candle_pipeline
        self.order_pipeline = out_order_pipeline
        self.indicators = ind.get_all_indicators()

        self.current_advice = None
        self.current_candle = None
        self.indicator_type = {}
        self.indicator_params = {}
        self.indicator_values = {}
        self.ohlcv = {
            'open': [],
            'high': [],
            'low': [],
            'close': [],
            'volume': []
        }

    def name(self):
        return 'Base'

    def get_parameters(self):
        """
        :return: Parameters that is set by the user
        """
        return self.parameters

    def init(self):
        """
        callback: executed once before strategy starts processing data
        """
        pass

    def process_candle(self, candle):
        """
        callback: for each candle (of given length), process_candle will be called. You can use this function to process
        candle and save state that can be later used in execute function.
        :param candle: candle of given length e.g. if the candle length is set 1 hour, then this function will be called once an hour.
        """
        pass

    def execute(self, indicators):
        """
        callback: this function will be called periodically based upon configured frequency.
        This function is the place where actual logic of strategy should be written.
        :param indicators: provides values of the indicators that is added using 'add_indicator' function.
        so e.g. you have added MACD, self.add_indicator('sma', { 'period' : 20}), then you can access indicators['sma'].sma
        note that since indicators need some time lag to process the candle and spit out the output, execute function
        will not always have indicator value ready. check indicators['sma'].is_ready before accessing output
        """
        pass

    def add_indicator(self, name, itype, parameters):
        """
        add this indicator to be provided while calling execute function. see list of parameters and required input [TODO:here]
        :param name: name of the indicator
        :param parameters: parameters for indicator in dict format, if not required parameters are provided then
        it will throw TradingException.
        """
        if name in self.indicator_values:
            return

        if itype not in self.indicators:
            raise TradingException('Indicator not found = %s ' % itype)

        try:
            # check whether all the parameters are correct for indicator
            tmp = {
                'open': np.random.random(100),
                'high': np.random.random(100),
                'low': np.random.random(100),
                'close': np.random.random(100),
                'volume': np.random.random(100)
            }
            # test the indicators with parameters to check whether they are ok
            self.indicators[itype](tmp, parameters)
        except Exception as e:
            raise TradingException('Incorrect indicator configuration = %s' % e)

        self.indicator_type[name] = itype
        self.indicator_params[name] = parameters
        self.indicator_values[name] = {}

    def buy(self):
        """
        Call this function from execute to generate buy advice
        """
        self.current_advice = TradeAdvice.BUY

    def sell(self):
        """
        Call this function from execute to generate sell advice
        """
        self.current_advice = TradeAdvice.SELL

    async def _get_next_available_candle(self):
        """
        Gets the next available candle in the pipeline. Until next candle is available this function will return same
        Candle every time.
        :return:  Next available candle from pipeline if any otherwise current available candle
        """
        if not self.candle_pipeline.empty():
            return await self.candle_pipeline.get()
        return self.current_candle

    def _is_new_candle(self, candle):
        if not self.current_candle:
            return True
        return candle.c_date > self.current_candle.c_date

    async def run(self):
        # This function receives candles from the pipeline, generates indicators and then
        # periodically calls execute function
        """
        Internal function. Should not be called from outside. Executes internal logic.
        """
        logging.info('Starting loop for strategy %s with deployment %s' % (self.name(), self.deployment_id))
        while True:
            candle = await self._get_next_available_candle()
            logging.debug("Starting strategy execution [candle = %s]" % (candle,))

            try:
                if candle and self._is_new_candle(candle):
                    logging.info('Got new candle in strategy %s for deployment %s' % (self.name(), self.deployment_id))

                    # todo write logic to purge old candles
                    self.ohlcv['open'] += [candle.c_open]
                    self.ohlcv['high'] += [candle.c_high]
                    self.ohlcv['low'] += [candle.c_low]
                    self.ohlcv['close'] += [candle.c_close]
                    self.ohlcv['volume'] += [candle.c_base_volume]

                    result = {}
                    for k, itype in self.indicator_type.items():
                        # compute indicator
                        ohlcv = {}
                        ohlcv['open'] = np.array(self.ohlcv['open'])
                        ohlcv['high'] = np.array(self.ohlcv['high'])
                        ohlcv['low'] = np.array(self.ohlcv['low'])
                        ohlcv['close'] = np.array(self.ohlcv['close'])
                        ohlcv['volume'] = np.array(self.ohlcv['volume'])

                        indicator = self.indicators[itype]
                        result[k] = indicator(ohlcv, self.indicator_params[k])

                    for k, vals in result.items():
                        for param_name, param_val in vals.items():
                            # get the last value of the indicator
                            lastval = param_val[len(param_val) - 1]
                            self.indicator_values[k][param_name] = None if lastval == np.nan else lastval

                    # call trading strategy's process candle callback
                    logging.debug(
                        'Processing candle for strategy %s of deployment %s' % (self.name(), self.deployment_id))
                    self.process_candle(candle)

                    self.current_candle = candle
            except:
                logging.exception('Exception in processing candle')

            # execute actual trading strategy callback
            logging.debug('Processing logic for strategy %s of deployment %s' % (self.name(), self.deployment_id))
            self.execute(self.indicator_values)

            if self.current_advice:
                logging.debug('Sending %s from strategy %s of deployment %s' %
                              (self.current_advice, self.name(), self.deployment_id))
                await self.order_pipeline.put(self.current_advice)
                self.current_advice = None

            await asyncio.sleep(BaseTradingStrategy.SLEEP_TIME)


STRATEGY_COROUTINE_FACTORY = {}
STRATEGY_FACTORY = {}


def register_strategy(name, strategy_class):
    logging.info('Registering strategy %s' % (name,))
    if name in STRATEGY_COROUTINE_FACTORY:
        return

    def return_coroutine(did, params, candle_pipe, advice_pipe):
        c = strategy_class()
        c.setup(did, params, candle_pipe, advice_pipe)
        c.init()
        return c.run()

    def return_strategy(did, params):
        c = strategy_class()
        c.setup(did, params, None, None)
        c.init()
        return c

    STRATEGY_FACTORY[name] = return_strategy
    STRATEGY_COROUTINE_FACTORY[name] = return_coroutine
