REST API
========

Following are supported REST methods:
#### Create methods
   - /algo/create - Creates new algo with given payload
```
/algo/create
{
	"algo_name" : "dummy-indicator-1",
	"trade_pair" : "XLM_native_CNY_GAREELUB43IRHWEASCFBLKHURCGMHE5IF6XSE7EXDLACYHGRHM43RFOX",
	"candle_size" : "5min",
	"strategy_name" : "dummy",
	"strategy_parameters" : {
		"fastperiod" : 5,
		"slowperiod" : 7,
		"signalperiod" : 3,
		"threshold_up" : -0.025,
		"threshold_down" : 0.025,
		"trend_stickiness" : 1
	}
} 
```
   - /backtest/run - Run backtest for the algo with given payload. Returns backtest-id.
```
/backtest/run
{
    "algo_name" : "dummy-indicator-1",
    "start_ts" : 1529462800,
    "end_ts" : 1529481860
}
```
   - /algo/deploy - Deploys the the algo with given parametes specified in payload. Returns deployment-id.
```
/algo/deploy
{
	"algo_name" : "dummy-indicator-1",
	"amount" : 1000,
	"num_cycles" : 1000
}
```
   - /algo/undeploy/{deploy_id} - Stops the running algo.
```
/algo/undeploy/42
```
   - /delete/algo/{algo_name} - Deletes algo, its backtests and deployment related info.
```
/delete/algo/dummy-indicator-1
```

#### Get methods
   - /list/algos - Returns list of the algos (by a given user)
   - /algo/{algoname} - Returns info of single algo
   - /list/backtests - Retuns list of the backtest (by a given user)
   - /backtest/status/{req_id} - Returns status of the backtest
   - /backtest/trades/{backtest_id} - Returns trades of the backtest
   - /list/algos/deployed - Returns list of deployed algos (by a given user)
   - /algo/deployed/status/{deployment_id} - Returns status of deployed algo
   - /algo/deployed/trades/{deployment_id} - Returns trades by the deployed algo
