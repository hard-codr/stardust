import asyncio
import getopt
import logging
import os.path
import sys
from concurrent.futures import ThreadPoolExecutor

import aiosqlite
import stellar
import yaml

import stardust.fetcher as real_fetcher
import stardust.trader as real_trader
import stardust.webapp as webapp
from stardust.data import Engine, DeployedAlgo, TradeAdvice, Candle
from stardust.data import set_db, get_main_db
from stardust.strategy import STRATEGY_COROUTINE_FACTORY as strategy_coroutine_factory

DEPLOYMENT = {}


async def update_deployed_status(deployment_id, status):
    try:
        async with aiosqlite.connect(get_main_db()) as db:
            await db.execute("update deployed_algos set status = ? where id = ?", [status, deployment_id])
            await db.commit()
        logging.debug('Updated deployed algo with deployment_id = %s, status = %s' % (deployment_id, status))
    except:
        logging.exception('Error occurred while deployment_id = %s, status = %s' % (deployment_id, status))
        raise


async def put_deployment(did, algo, st, candle_pipe, s2a):
    # todo does this needs to be in database?
    DEPLOYMENT[did] = {
        'algo': algo,
        'st': st,
        'c2s': candle_pipe,
        's2a': s2a,
    }


async def get_deployment(did):
    # todo does this needs to be in database?
    d = DEPLOYMENT[did]
    return d['algo'], d['st'], d['c2s'], d['s2a']


async def candle_consumer(candle_pipeline, strategy_candle_pipelines):
    candle_per_size_per_key = {
        Candle.CANDLESIZE_5MIN: {},
        Candle.CANDLESIZE_15MIN: {},
        Candle.CANDLESIZE_1HR: {},
        Candle.CANDLESIZE_4HR: {},
        Candle.CANDLESIZE_1DAY: {},
        Candle.CANDLESIZE_1WK: {},
    }
    while True:
        candle = await candle_pipeline.get()
        logging.info('Processing candle %s in engine' % candle.key)
        if candle.key in strategy_candle_pipelines:
            for candlesize, pipe in strategy_candle_pipelines[candle.key]:
                # since min candle generation frequency in fetcher is 1min, why bother
                # comparing it to other candles
                if candlesize == Candle.CANDLESIZE_1MIN:
                    await pipe.put(candle)
                    continue

                if candle.key in candle_per_size_per_key[candlesize]:
                    old_candle = candle_per_size_per_key[candlesize][candle.key]
                    # check whether its same candle, if yes then combine otherwise
                    # release the old candle and start combining current candle with future candles
                    if old_candle.is_same_candle(candle.c_date, candlesize):
                        old_candle.c_close = candle.c_close
                        old_candle.c_high = candle.c_high if candle.c_high > old_candle.c_high else old_candle.c_high
                        old_candle.c_low = candle.c_low if candle.c_low < old_candle.c_low else old_candle.c_low
                        old_candle.c_base_volume += float(candle.c_base_volume)
                        old_candle.c_counter_volume += float(candle.c_counter_volume)
                    else:
                        # current candle is new candle, hence release the old candle
                        # todo: can be improved. Now old candle only get expired when new candle arrives
                        candle_per_size_per_key[candlesize][candle.key] = candle
                        logging.debug('Releasing candle of size = %s for tradepair = %s' % (candlesize, candle.key))
                        await pipe.put(old_candle)
                else:
                    candle_per_size_per_key[candlesize][candle.key] = candle


async def run_engine(loop, engine_pipeline, candle_pipeline, advice_pipeline):
    strategy_candle_pipelines = {}

    logging.info('Starting candle consumer')
    asyncio.ensure_future(candle_consumer(candle_pipeline, strategy_candle_pipelines), loop=loop)

    while True:
        logging.debug('Waiting for engine command')
        cmd = await engine_pipeline.get()
        cmd_code = cmd[0]
        if cmd_code == Engine.COMMAND_DEPLOY:
            user_profile = cmd[1]
            deployed_algo = cmd[2]
            did = deployed_algo.id
            algo = deployed_algo.algo
            amount = deployed_algo.amount
            num_cycles = deployed_algo.num_cycles

            logging.info('Got new deploy command user=%s did=%s algo=%s amount=%s cycles=%s' %
                         (user_profile.userid, did, algo, amount, num_cycles))

            st_candle_pipe = asyncio.Queue(loop=loop)
            st_advice_pipe = asyncio.Queue(loop=loop)

            candle_pipe = (algo.candlesize, st_candle_pipe)
            if algo.tradepair in strategy_candle_pipelines:
                strategy_candle_pipelines[algo.tradepair] += [candle_pipe]
            else:
                strategy_candle_pipelines[algo.tradepair] = [candle_pipe]

            async def strategy_to_advice(user_profile_, did_, tradepair_, amount_, num_cycles_, st_pipe, main_pipe):
                while True:
                    advice = await st_pipe.get()
                    await main_pipe.put(TradeAdvice(user_profile_, did_, tradepair_, advice, amount_, num_cycles_))

            try:
                strategy_coro = strategy_coroutine_factory[algo.strategyname](
                    did, algo.parameters, st_candle_pipe, st_advice_pipe)
            except:
                logging.exception('Error occurred while instantiating strategy')

                # revert changes and change status to error
                strategy_candle_pipelines[algo.tradepair].remove(candle_pipe)
                try:
                    await update_deployed_status(did, DeployedAlgo.STATUS_ERROR)
                except:
                    logging.exception('Error occurred while updating db')
                continue

            try:
                await update_deployed_status(did, DeployedAlgo.STATUS_RUNNING)
            except:
                logging.exception('Error occurred while updating db')
                continue

            s2a = asyncio.ensure_future(strategy_to_advice(
                user_profile, did, algo.tradepair, amount, num_cycles, st_advice_pipe, advice_pipeline), loop=loop)
            # instantiate strategy instance and supply parameters for it to execute
            st = asyncio.ensure_future(strategy_coro, loop=loop)

            logging.debug('Algo deployed user=%s did=%s algo=%s amount=%s cycles=%s' %
                          (user_profile.userid, did, algo, amount, num_cycles))

            await put_deployment(did, algo, st, candle_pipe, s2a)
        elif cmd_code == Engine.COMMAND_UNDEPLOY or cmd_code == Engine.COMMAND_STOP or cmd_code == Engine.COMMAND_DONE:
            did = cmd[1]
            algo, st, candle_pipe, s2a = await get_deployment(did)

            logging.info('Got command to stop deployed algo did=%s algo=%s' % (did, algo))

            try:
                strategy_candle_pipelines[algo.tradepair].remove(candle_pipe)

                st.cancel()
                s2a.cancel()

                if cmd_code == Engine.COMMAND_UNDEPLOY:
                    await update_deployed_status(did, DeployedAlgo.STATUS_STOPPED)
                elif cmd_code == Engine.COMMAND_DONE:
                    await update_deployed_status(did, DeployedAlgo.STATUS_FINISHED)
                elif cmd_code == Engine.COMMAND_STOP:
                    await update_deployed_status(did, DeployedAlgo.STATUS_ERROR)
            except:
                logging.exception('Exception occurred in engine while stopping strategy')
                continue


def usage():
    print('engine -c/--config <config-file>')


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
        usage();
        sys.exit(2)

    configcontent = ''
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

    logfile = os.path.join(logpath, 'engine.log')

    root = logging.getLogger()
    root.setLevel(s)
    root.propagate = False
    fh = logging.FileHandler(filename=logfile)
    fh.setLevel(s)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    root.addHandler(fh)

    executor = ThreadPoolExecutor(max_workers=10)

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

    logging.info('Starting engine')


    async def startup(app):
        trader_config = {}
        fetcher_config = {}

        loop = app.loop

        candle_pipeline = asyncio.Queue(loop=loop)
        advice_pipeline = asyncio.Queue(loop=loop)
        engine_pipeline = asyncio.Queue(loop=loop)

        fetcher = real_fetcher.run_fetcher(loop, executor, fetcher_config, candle_pipeline)
        trader = real_trader.run_trader(loop, executor, trader_config, advice_pipeline, engine_pipeline)
        engine = run_engine(loop, engine_pipeline, candle_pipeline, advice_pipeline)

        app['engine_pipeline'] = engine_pipeline

        logging.info('Starting engine components')
        app['fetcher'] = asyncio.ensure_future(fetcher, loop=loop)
        app['trader'] = asyncio.ensure_future(trader, loop=loop)
        app['engine'] = asyncio.ensure_future(engine, loop=loop)

        logging.info('Engine startup complete')


    async def cleanup(app):
        app['fetcher'].cancel()
        app['trader'].cancel()
        app['engine'].cancel()

        await app['fetcher']
        await app['trader']
        await app['engine']

        logging.info('Engine cleanup complete')


    host = '0.0.0.0'
    port = 12321
    if 'restapi' in config:
        restapiconfig = config['restapi']
        if 'host' in restapiconfig:
            host = restapiconfig['host']
        if 'port' in restapiconfig:
            post = restapiconfig['port']

    main_db = 'engine.db'
    backtest_db = 'backtest.db'
    if 'db' in config:
        dbconfig = config['db']
        if 'connection_main' in dbconfig:
            main_db = dbconfig['connection_main']
        if 'connection_backtest' in dbconfig:
            backtest_db = dbconfig['connection_backtest']
    set_db(main_db, backtest_db)

    logging.info('RestApi is configured to run on %s:%s' % (host, port))

    logging.info('Starting webapp')
    webapp.run_api_server(host, port, startup, cleanup, config)
