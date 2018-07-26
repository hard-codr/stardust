import logging

from stardust.strategy import register_strategy, BaseTradingStrategy


# Strategy based on MACD
#
# Input parameters:
#
# MACD configuration parameters (see here for reference http://trader.wikia.com/wiki/MACD):
# fastperiod = 10
# slowperiod = 21
# signalperiod = 9
#
# the difference between the EMAs (to act as triggers):
# threshold_down = -0.025
# threshold_up = 0.025
#
# Candle to process before considering it a trend:
# trend_stickiness = 1
class MACD(BaseTradingStrategy):
    def __init__(self):
        BaseTradingStrategy.__init__(self)
        self.trend_direction = 'none'
        self.trend_duration = 0
        self.trend_persisted = False
        self.trend_advised = False

    def name(self):
        return 'MACD'

    def init(self):
        self.trend_direction = 'none'
        self.trend_duration = 0
        self.trend_persisted = False
        self.trend_advised = False

        self.add_indicator('macdx', 'MACD', self.get_parameters())

    def process_candle(self, candle):
        self.trend_duration += 1

    def execute(self, indicators):
        params = self.get_parameters()
        threshold_up = params['threshold_up']
        threshold_down = params['threshold_down']
        trend_stickiness = params['trend_stickiness']

        macd = indicators['macdx']['macd']

        if not macd:
            # warmup yet not done
            return

        if macd > threshold_up:
            if self.trend_direction != 'up':
                self.trend_direction = 'up'
                self.trend_duration = 0
                self.trend_persisted = False
                self.trend_advised = False

            logging.debug('In up-trend since %s candles ' % self.trend_duration)

            if self.trend_duration >= trend_stickiness:
                self.trend_persisted = True;

            if self.trend_persisted and not self.trend_advised:
                self.trend_advised = True
                self.buy()
        elif macd < threshold_down:
            if self.trend_direction != 'down':
                self.trend_direction = 'down'
                self.trend_duration = 0
                self.trend_persisted = False
                self.trend_advised = False

            logging.debug('In down-trend since %s candles ' % self.trend_duration)

            if self.trend_duration >= trend_stickiness:
                self.trend_persisted = True;

            if self.trend_persisted and not self.trend_advised:
                self.trend_advised = True
                self.sell()


register_strategy('macd', MACD)
