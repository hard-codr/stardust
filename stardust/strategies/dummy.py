from stardust.strategy import register_strategy, BaseTradingStrategy


class Dummy(BaseTradingStrategy):
    def __init__(self):
        BaseTradingStrategy.__init__(self)
        self.last_advice = None
        self.skipped = 0

    def name(self):
        return 'Dummy'

    def init(self):
        self.last_advice = None
        self.skipped = 0

        self.add_indicator('macdx', 'MACD', self.get_parameters())

    def process_candle(self, candle):
        self.skipped += 1

    def execute(self, indicators):
        if 'macdx' in indicators:
            print('=============================')
            print('macdx = %s' % str(indicators))
            print('=============================')

        if self.skipped == 100:
            # generate advice after each 100 minute
            self.skipped = 0
            if not self.last_advice or self.last_advice == 'SELL':
                self.buy()
                self.last_advice = 'BUY'
            else:
                self.sell()
                self.last_advice = 'SELL'


register_strategy('dummy', Dummy)
