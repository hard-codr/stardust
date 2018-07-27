import json
import logging
import sys

import aiosqlite
import stellar
from aiohttp import web

from stardust.data import Algo, Engine, UserProfile
from stardust.data import Backtest
from stardust.data import DeployedAlgo
from stardust.data import get_main_db, get_backtest_db

routes = web.RouteTableDef()

STATUS_OK = '{ "status" : "OK" }'
STATUS_ERR = '{ "status" : "ERROR", "error_code" : %s, "error_desc" : "%s" }'

ERRORS = [
    (-1, "Auth required"),
    (-2, "Internal error"),
    (-3, "Incorrect or missing request parameters"),
    (-4, "Resource not found"),
    (-5, "Resource already exist"),
]
ERR_AUTH_REQUIRED = 0
ERR_INTERNAL_ERROR = 1
ERR_INCORRECT_REQUEST = 2
ERR_RESOURCE_NOT_FOUND = 3
ERR_RESOURCE_ALREADY_EXIST = 4


def json_response(body='', **kwargs):
    # kwargs['body'] = json.dumps(body or kwargs['body'])
    kwargs['body'] = body
    kwargs['content_type'] = 'text/json'
    return web.Response(**kwargs)


def login_required(func):
    def wrapper(request):
        if not request.user:
            return json_response(STATUS_ERR % ERRORS[ERR_AUTH_REQUIRED], status=401)
        return func(request)

    return wrapper


async def get_existing_algo(userid, algoname):
    try:
        async with aiosqlite.connect(get_main_db()) as db:
            async with db.execute("select algoname, tradepair, candlesize, strategyname, parameters from "
                                  "algos where userid = ? and algoname = ?", [userid, algoname]) as cursor:
                async for row in cursor:
                    return {
                        'algo_name': row[0],
                        'trade_pair': row[1],
                        'candle_size': row[2],
                        'strategy_name': row[3],
                        'strategy_parameters': json.loads(row[4]),
                    }
    except:
        raise

    return None


async def get_deployed_algo(userid, deployment_id):
    try:
        async with aiosqlite.connect(get_main_db()) as db:
            async with db.execute("select id, algoname, amount, num_cycles, status from "
                                  "deployed_algos where userid = ? and id = ?", [userid, deployment_id]) as cursor:
                async for row in cursor:
                    return {
                        'id': row[0],
                        'algo_name': row[1],
                        'amount': row[2],
                        'num_cycles': row[3],
                        'status': row[4],
                    }
    except:
        raise

    return None


@login_required
@routes.post('/algo/create')
async def create(request):
    # get request parameter
    reqparams = await request.json()

    if type(reqparams) != dict:
        return json_response(STATUS_ERR % ERRORS[ERR_INCORRECT_REQUEST], status=400)

    userid = request.user
    algoname = reqparams['algo_name'] if 'algo_name' in reqparams else ''
    tradepair = reqparams['trade_pair'] if 'trade_pair' in reqparams else ''
    candlesize = reqparams['candle_size'] if 'candle_size' in reqparams else ''
    strategyname = reqparams['strategy_name'] if 'strategy_name' in reqparams else ''
    parameters = reqparams['strategy_parameters'] if 'strategy_parameters' in reqparams else ''

    if not (algoname and tradepair and candlesize and strategyname):
        return json_response(STATUS_ERR % ERRORS[ERR_INCORRECT_REQUEST], status=400)

    try:
        existing_algo = await get_existing_algo(userid, algoname)
        if existing_algo:
            return json_response(STATUS_ERR % ERRORS[ERR_RESOURCE_ALREADY_EXIST], status=400)

        async with aiosqlite.connect(get_main_db()) as db:
            await db.execute("insert into algos(userid, algoname, tradepair, candlesize, strategyname, parameters) "
                             "values (?, ?, ?, ?, ?, ?)",
                             [userid, algoname, tradepair, candlesize, strategyname, json.dumps(parameters)])
            await db.commit()
    except:
        return json_response(STATUS_ERR % ERRORS[ERR_INTERNAL_ERROR], status=500)

    return json_response(STATUS_OK)


@login_required
@routes.get('/list/algos')
async def algo_list(request):
    userid = request.user
    algolist = []
    try:
        async with aiosqlite.connect(get_main_db()) as db:
            async with db.execute("select algoname, tradepair, candlesize, strategyname, parameters from "
                                  "algos where userid = ?", [userid]) as cursor:
                async for row in cursor:
                    algolist += [
                        {
                            'algo_name': row[0],
                            'trade_pair': row[1],
                            'candle_size': row[2],
                            'strategy_name': row[3],
                            'strategy_parameters': json.loads(row[4]),
                        }
                    ]
    except:
        return json_response(STATUS_ERR % ERRORS[ERR_INTERNAL_ERROR], status=500)

    return json_response(json.dumps(algolist))


@login_required
@routes.get('/algo/{algoname}')
async def get_algo(request):
    userid = request.user
    algoname = request.match_info['algoname']
    try:
        algo = await get_existing_algo(userid, algoname)
    except:
        return json_response(STATUS_ERR % ERRORS[ERR_INTERNAL_ERROR], status=500)

    if algo:
        return json_response(json.dumps(algo))
    else:
        return json_response(STATUS_ERR % ERRORS[ERR_RESOURCE_NOT_FOUND], status=400)


@login_required
@routes.post('/backtest/run/')
async def backtest_run(request):
    userid = request.user
    reqparams = await request.json()

    if type(reqparams) != dict:
        return json_response(STATUS_ERR % ERRORS[ERR_INCORRECT_REQUEST], status=400)

    algoname = reqparams['algo_name'] if 'algo_name' in reqparams else ''
    start_ts = reqparams['start_ts'] if 'start_ts' in reqparams else ''
    end_ts = reqparams['end_ts'] if 'end_ts' in reqparams else ''

    if not algoname:
        return json_response(STATUS_ERR % ERRORS[ERR_INCORRECT_REQUEST], status=400)

    try:
        algo = await get_existing_algo(userid, algoname)
    except:
        return json_response(STATUS_ERR % ERRORS[ERR_INTERNAL_ERROR], status=500)

    if algo:
        num_tries = 0
        while num_tries < 3:
            try:
                async with aiosqlite.connect(get_backtest_db()) as db:
                    cursor = await db.execute("insert into backtest_request(userid, algoname, start_ts, end_ts, "
                                              "tradepair, candlesize, strategyname, parameters, status) "
                                              "values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                              [userid, algoname, start_ts, end_ts,
                                               algo['trade_pair'], algo['candle_size'], algo['strategy_name'],
                                               json.dumps(algo['strategy_parameters']), Backtest.STATUS_NEW])
                    await db.commit()

                    breq = {'req_id': cursor.lastrowid}
                break
            except:
                logging.exception('Exception occurred while updating backtest_request')
                num_tries += 1
        else:
            return json_response(STATUS_ERR % ERRORS[ERR_INTERNAL_ERROR], status=400)
    else:
        return json_response(STATUS_ERR % ERRORS[ERR_RESOURCE_NOT_FOUND], status=400)

    return json_response(json.dumps(breq))


@login_required
@routes.get('/backtest/status/{req_id}')
async def backtest_status(request):
    userid = request.user
    breq_id = request.match_info['req_id']

    bstatus = None
    num_tries = 0
    while num_tries < 3:
        try:
            async with aiosqlite.connect(get_backtest_db()) as db:
                async with db.execute("select id, algoname, start_ts, end_ts, "
                                      "tradepair, candlesize, strategyname, parameters, status "
                                      "from backtest_request where userid = ? and id = ?",
                                      [userid, breq_id]) as cursor:
                    async for row in cursor:
                        bstatus = {
                            'id': row[0],
                            'algo_name': row[1],
                            'start_ts': row[2],
                            'end_ts': row[3],
                            'trade_pair': row[4],
                            'candle_size': row[5],
                            'strategy_name': row[6],
                            'strategy_parameters': row[7],
                            'status': row[8],
                        }
            break
        except:
            logging.exception('Exception occurred while updating backtest_request')
            num_tries += 1
    else:
        return json_response(STATUS_ERR % ERRORS[ERR_INTERNAL_ERROR], status=500)

    if bstatus:
        return json_response(json.dumps(bstatus))
    else:
        return json_response(STATUS_ERR % ERRORS[ERR_RESOURCE_NOT_FOUND], status=400)


@login_required
@routes.get('/backtest/trades/{backtest_id}')
async def backtest_trades(request):
    userid = request.user
    backtest_id = request.match_info['backtest_id']

    num_tries = 0
    exist = False
    while num_tries < 3:
        try:
            async with aiosqlite.connect(get_backtest_db()) as db:
                async with db.execute("select id, algoname, start_ts, end_ts, "
                                      "tradepair, candlesize, strategyname, parameters, status "
                                      "from backtest_request where userid = ? and id = ?",
                                      [userid, backtest_id]) as cursor:
                    async for row in cursor:
                        exist = True
            break
        except:
            logging.exception('Exception occurred while updating backtest_request')
            num_tries += 1
    else:
        return json_response(STATUS_ERR % ERRORS[ERR_INTERNAL_ERROR], status=500)

    if not exist:
        return json_response(STATUS_ERR % ERRORS[ERR_RESOURCE_NOT_FOUND], status=500)

    trades = []
    try:
        async with aiosqlite.connect(get_backtest_db()) as db:
            async with db.execute("select ts, advice, sold_asset, sold_amount, bought_asset, bought_amount "
                                  "from backtest_trades where backtest_id = ?",
                                  [backtest_id]) as cursor:
                async for row in cursor:
                    trades += [
                        {
                            'ts': row[0],
                            'advice': row[1],
                            'sold_asset': row[2],
                            'sold_amount': row[3],
                            'bought_asset': row[4],
                            'bought_amount': row[5],
                        }
                    ]
    except:
        return json_response(STATUS_ERR % ERRORS[ERR_INTERNAL_ERROR], status=500)

    return json_response(json.dumps(trades))


@login_required
@routes.get('/list/backtests')
async def backtest_list(request):
    userid = request.user
    reqlist = []

    num_tries = 0
    while num_tries < 3:
        try:
            async with aiosqlite.connect(get_backtest_db()) as db:
                async with db.execute(
                        "select id, algoname, start_ts, end_ts, status from backtest_request where userid = ?",
                        [userid]) as cursor:
                    async for row in cursor:
                        reqlist += [
                            {
                                'id': row[0],
                                'algo_name': row[1],
                                'start_ts': row[2],
                                'end_ts': row[3],
                                'status': row[4],
                            }
                        ]
            break
        except:
            logging.exception('Exception occurred while updating backtest_request')
            num_tries += 1
    else:
        return json_response(STATUS_ERR % ERRORS[ERR_INTERNAL_ERROR], status=500)

    return json_response(json.dumps(reqlist))


@login_required
@routes.post('/algo/deploy')
async def deploy(request):
    userid = request.user
    reqparams = await request.json()

    if type(reqparams) != dict:
        return json_response(STATUS_ERR % ERRORS[ERR_INCORRECT_REQUEST], status=400)

    algoname = reqparams['algo_name'] if 'algo_name' in reqparams else ''
    amount = reqparams['amount'] if 'amount' in reqparams else ''
    num_cycles = reqparams['num_cycles'] if 'num_cycles' in reqparams else ''

    if not (algoname and amount and num_cycles):
        return json_response(STATUS_ERR % ERRORS[ERR_INCORRECT_REQUEST], status=400)

    try:
        existing_algo = await get_existing_algo(userid, algoname)
        if not existing_algo:
            return json_response(STATUS_ERR % ERRORS[ERR_RESOURCE_NOT_FOUND], status=400)

        async with aiosqlite.connect(get_main_db()) as db:
            cursor = await db.execute("insert into deployed_algos(userid, algoname, amount, num_cycles, status) "
                                      "values (?, ?, ?, ?, ?)",
                                      [userid, algoname, amount, num_cycles, DeployedAlgo.STATUS_NEW])
            await db.commit()
            deployment_id = cursor.lastrowid

            dreq = {'deploy_id': deployment_id}

        user_profile = UserProfile(userid, request.account, request.account_secret)
        deployment_details = DeployedAlgo(Algo.from_dict(existing_algo), deployment_id, amount, num_cycles)
        await request.app['engine_pipeline'].put((Engine.COMMAND_DEPLOY, user_profile, deployment_details))
    except:
        logging.exception('Exception occurred while processing deploy request')
        return json_response(STATUS_ERR % ERRORS[ERR_INTERNAL_ERROR], status=500)

    return json_response(json.dumps(dreq))


@login_required
@routes.post('/algo/undeploy/{deploy_id}')
async def undeploy(request):
    userid = request.user
    deployment_id = request.match_info['deploy_id']

    try:
        deployed_algo = await get_deployed_algo(userid, deployment_id)
        if not deployed_algo:
            return json_response(STATUS_ERR % ERRORS[ERR_RESOURCE_NOT_FOUND], status=400)

        existing_algo = await get_existing_algo(userid, deployed_algo['algo_name'])
        if not existing_algo:
            return json_response(STATUS_ERR % ERRORS[ERR_RESOURCE_NOT_FOUND], status=400)

        await request.app['engine_pipeline'].put((Engine.COMMAND_UNDEPLOY, deployment_id))
    except:
        return json_response(STATUS_ERR % ERRORS[ERR_INTERNAL_ERROR], status=500)

    return web.Response(text="OK")


@login_required
@routes.get('/algo/deployed/status/{deployment_id}')
async def deployed_algo_status(request):
    userid = request.user
    deployment_id = request.match_info['deployment_id']

    try:
        deployed = await get_deployed_algo(userid, deployment_id)
    except:
        return json_response(STATUS_ERR % ERRORS[ERR_INTERNAL_ERROR], status=500)

    if deployed:
        return json_response(json.dumps(deployed))
    return json_response(STATUS_ERR % ERRORS[ERR_RESOURCE_NOT_FOUND], status=400)


@login_required
@routes.get('/algo/deployed/trades/{deployment_id}')
async def deployed_algo_trades(request):
    userid = request.user
    deployment_id = request.match_info['deployment_id']

    try:
        deployed = await get_deployed_algo(userid, deployment_id)
    except:
        return json_response(STATUS_ERR % ERRORS[ERR_INTERNAL_ERROR], status=500)

    trades = []
    try:
        async with aiosqlite.connect(get_main_db()) as db:
            async with db.execute("select ts, advice, sold_asset, sold_amount, bought_asset, bought_amount "
                                  "from trades where deployment_id = ?",
                                  [deployment_id]) as cursor:
                async for row in cursor:
                    trades += [
                        {
                            'ts': row[0],
                            'advice': row[1],
                            'sold_asset': row[2],
                            'sold_amount': row[3],
                            'bought_asset': row[4],
                            'bought_amount': row[5],
                        }
                    ]
    except:
        return json_response(STATUS_ERR % ERRORS[ERR_INTERNAL_ERROR], status=500)

    return json_response(json.dumps(trades))


@login_required
@routes.get('/list/algos/deployed')
async def deployed_algo_list(request):
    userid = request.user
    deployed_list = []
    try:
        async with aiosqlite.connect(get_main_db()) as db:
            async with db.execute("select id, algoname, amount, num_cycles, status "
                                  "from deployed_algos where userid = ?", [userid]) as cursor:
                async for row in cursor:
                    deployed_list += [
                        {
                            'id': row[0],
                            'algo_name': row[1],
                            'amount': row[2],
                            'num_cycles': row[3],
                            'status': row[4],
                        }
                    ]
    except:
        return json_response(STATUS_ERR % ERRORS[ERR_INTERNAL_ERROR], status=500)

    return json_response(json.dumps(deployed_list))


@login_required
@routes.post('/delete/algo/{algo_name}')
async def delete(request):
    userid = request.user
    algoname = request.match_info['algoname']

    try:
        algo = await get_existing_algo(userid, algoname)
    except:
        return json_response(STATUS_ERR % ERRORS[ERR_INTERNAL_ERROR], status=500)

    if algo:
        try:
            async with aiosqlite.connect(get_main_db()) as db:
                await db.execute("delete from algos where userid = ? and algoname = ?", [userid, algoname])
                await db.execute("delete from deployed_algos where userid = ? and algoname = ?", [userid, algoname])
                await db.commit()

            try:
                async with aiosqlite.connect(get_backtest_db()) as db:
                    await db.execute("delete from backtest_request where userid = ? and algoname = ?",
                                     [userid, algoname])
                    await db.commit()
            except:
                pass
        except:
            return json_response(STATUS_ERR % ERRORS[ERR_INTERNAL_ERROR], status=500)
    else:
        return json_response(STATUS_ERR % ERRORS[ERR_RESOURCE_NOT_FOUND], status=400)

    return web.Response(text="OK")


async def auth_middleware(app, handler):
    async def middleware(request):
        request.user = app['engine.username']
        request.account = app['engine.user_account']
        request.account_secret = app['engine.user_secret']
        return await handler(request)

    return middleware


def run_api_server(host, port, startup, cleanup, config):
    app = web.Application(middlewares=[auth_middleware])

    # Hack start
    # Until authentication middleware is introduced
    # following hack can be used to run it for single user
    if 'user' in config:
        userprofile = config['user']
        if 'username' in userprofile and 'secret_key' in userprofile:
            username = userprofile['username']
            secret_key = userprofile['secret_key']
        else:
            print('Username and secret_key is missing in user configuration section of engine.yaml')
            sys.exit(2)
    else:
        print('User configuration is missing in engine.yaml')
        sys.exit(2)

    try:
        useraccount = stellar.account_from_secret(secret_key)
    except:
        print('Incorrect secret_key in engine.yaml')
        sys.exit(2)
    # Hack end

    app['engine.username'] = username
    app['engine.user_account'] = useraccount
    app['engine.user_secret'] = secret_key

    app.add_routes(routes)
    app.on_startup.append(startup)
    app.on_cleanup.append(cleanup)
    web.run_app(app, host=host, port=port)
