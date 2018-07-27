Algorithms
----------
Trading engine is highly customizable via algorithms and can do highly sophisticated things in python.

#### Interface
Algorithms in the context of this engine corresponds to a piece of python code that runs 
inside the engine and decides when to place a buy or a sell order based on certain
conditions such as the indicators with favourable value etc..

Each algorithms follows common interface given below:
```python
from stardust.strategy import register_strategy, BaseTradingStrategy


class SampleAlgo(BaseTradingStrategy):
    def __init__(self):
        BaseTradingStrategy.__init__(self)
        self.skipped = 0
        self.last_advice = None

    def name(self):
        return 'SampleAlgo'

    def init(self):
        # perform initialization of the algo here.
        # here you can add indicators you are interested in as follows
        # first parameter is the name you want to refer this indicator in rest
        # of the algo code and get_parameters() returns parameters which 
        # passed by the user while instantiating this algorithms via REST API
        # You can choose to have user defined parameters or not accept any 
        # parameters from user and pass hard-code value to indicator
        # list of available indicators are given below
        self.add_indicator('macd_x', 'MACD', self.get_parameters())
        self.skipped = 0
        self.last_advice = None
        

    def process_candle(self, candle):
        # process the candle here.
        # you can choose to save any state based on candle as object field
        # in this class. e.g. self.skipped is incremented here and next
        # execute will see incremented value
        self.skipped += 1
        pass

    def execute(self, indicators):
        # since indicators generated only when candle occurs
        # and execute is called periodically irrespective of whether
        # candle arrives or not, you need to check whether
        # indicator is populated or not.
        if 'macd_x' in indicators:
            macd = indicators['macd_x']['macd']
        else:
            macd = None
        # generate buy and sell order here based on candle and indicators
        # this algorithms
        # following algorithm generates alternating buy and sell orders
        # after each 10 candle.
        if self.skipped == 10:
            self.skipped = 0
            if not self.last_advice or self.last_advice == 'SELL':
                self.buy()
                self.last_advice = 'BUY'
            else:
                self.sell()
                self.last_advice = 'SELL'


# register this strategy with engine
register_strategy('sampleAlgo', SampleAlgo)

```

#### Indicators
Following is list of the indicators available:
- BBANDS - Bollinger Bands 
    - Returns (upperband, middleband, lowerband) 
- DEMA - Double Exponential Moving Average 
    - Returns (dema) 
- EMA - Exponential Moving Average 
  - Returns (ema) 
- HT_TRENDLINE - Hilbert Transform - Instantaneous Trendline 
	- Returns (trendline) 
- KAMA - Kaufman Adaptive Moving Average 
	- Returns(kama) 
- MA - Moving Average 
	- Returns (ma) 
- MAMA - MESA Adaptive Moving Average 
	- Returns (mama, fama) 
- MAVP - Moving average with variable period 
	- Returns (mavp) 
- MIDPOINT - MidPoint over period 
	- Returns (midpoint) 
- MIDPRICE - Midpoint Price over period 
	- Returns (midprice) 
- SAR - Parabolic SAR 
	- Returns (sar) 
- SAREXT - Parabolic SAR - Extended 
	- Returns (sarext) 
- SMA - Simple Moving Average 
	- Returns (sma) 
- T3 - Triple Exponential Moving Average 
	- Returns (t3) 
- TEMA - Triple Exponential Moving Average 
	- Returns (tema) 
- TRIMA - Triangular Moving Average 
	- Returns (trima) 
- WMA - Weighted Moving Average 
	- Returns (wma) 
- MACD - Moving Average Convergence/Divergence 
	- Returns (macd, macdsignal, macdhist) 

