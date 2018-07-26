import asyncio
import datetime
import logging
import math
import sqlite3 as sqlite
from threading import Lock

import stellar

from stardust.data import TradeAdvice, Engine, get_main_db, EPOCH

ALGO_TRADING_CONTEXT = {}

tradelock = Lock()


def get_trade_context(deployment_id):
    try:
        tradelock.acquire()
        return ALGO_TRADING_CONTEXT[deployment_id] if deployment_id in ALGO_TRADING_CONTEXT else {}
    finally:
        tradelock.release()


def add_trade_context(deployment_id, trade):
    try:
        tradelock.acquire()
        if deployment_id in ALGO_TRADING_CONTEXT:
            return ALGO_TRADING_CONTEXT[deployment_id]
        ALGO_TRADING_CONTEXT[deployment_id] = trade
    finally:
        tradelock.release()

    return None


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


ALGO_CONT = 0
ALGO_ERROR = 1
ALGO_DONE = 2


def execute_trade(trading_config, user_profile, deployment_id, trade_pair, advice, amount, num_cycles):
    asset_pairs = trade_pair.split('_')
    base_asset = get_asset(asset_pairs[0], asset_pairs[1])
    counter_asset = get_asset(asset_pairs[2], asset_pairs[3])

    logging.debug('Executing trade for did=%s for trade_pair=%s' % (deployment_id, trade_pair))

    tcontext = get_trade_context(deployment_id)
    while True:
        if tcontext:
            lock = tcontext['lock']

            lock.acquire()
            try:
                last_advice = tcontext['last_advice']
                first_advice = tcontext['first_advice']
                current_cycles = tcontext['current_cycles']
                buy_amount = tcontext['amount']
                sell_amount = tcontext['sell_amount']

                if current_cycles >= num_cycles:
                    logging.info('Did = %s is completed the %s cycles. Stopping.' % (deployment_id, num_cycles))
                    return False, ALGO_DONE, None

                if last_advice == advice:
                    logging.info(
                        'Got sequential %s order from did=%s. Ignoring recent advice.' % (advice, deployment_id))
                    return False, ALGO_CONT, None

                if first_advice != advice:
                    current_cycles += 1

                tcontext['last_advice'] = advice
                tcontext['current_cycles'] = current_cycles
            finally:
                lock.release()
            break
        else:
            if advice == TradeAdvice.SELL:
                logging.info('Sell order without first buy order from did=%s. Ignoring advice.' % advice)
                return False, ALGO_CONT, None

            tcontext = dict()
            tcontext['lock'] = Lock()
            tcontext['first_advice'] = advice
            tcontext['last_advice'] = advice
            tcontext['current_cycles'] = 0
            tcontext['amount'] = buy_amount = amount
            tcontext['sell_amount'] = sell_amount = 0

            last_context = add_trade_context(deployment_id, tcontext)
            if last_context:
                tcontext = last_context
                continue
            else:
                break

    account = user_profile.account
    signer = user_profile.account_secret

    trxid = None
    err = None

    if advice == TradeAdvice.BUY:
        sell_asset, buy_asset = base_asset, counter_asset

        if math.floor(buy_amount) == 0:
            logging.info(
                'Algo %d trying to generate buy order without available asset %s' % (deployment_id, sell_asset))
            return False, ALGO_ERROR, ('Ran out of fund for asset = %s' % format_asset(sell_asset))

        book = stellar.orderbook(selling=base_asset, buying=counter_asset).fetch()
        if len(book.bids):
            # get current market price of the asset
            market_bid = book.bids[0]
            logging.debug('Executing trade for did=%s amount=%s, at market_price=%s [%s -> %s]' %
                          (deployment_id, buy_amount, market_bid[1], sell_asset, buy_asset))
            with stellar.new_transaction(account, signers=[signer]) as t:
                # buy counter asset buy exchanging base asset
                t.add_offer(buy_amount, sell_asset, buy_asset, market_bid[1])

            if t.is_success():
                trxid, _ = t.result()
                logging.debug('Trade executed for did = %s, trxid = %s' % (deployment_id, trxid))
            else:
                err = str(t.errors())
                logging.error('Trade execution failed for did = %s, err = %s' % (deployment_id, err))
    elif advice == TradeAdvice.SELL:
        sell_asset, buy_asset = counter_asset, base_asset

        if math.floor(sell_amount) == 0:
            logging.info(
                'Algo %d trying to generate buy order without available asset %s' % (deployment_id, sell_asset))
            return False, ALGO_ERROR, ('Ran out of fund for asset = %s' % format_asset(sell_asset))

        book = stellar.orderbook(selling=counter_asset, buying=base_asset).fetch()
        if len(book.bids):
            # get current market price of the asset
            market_bid = book.bids[0]
            logging.debug('Executing trade for did=%s amount=%s, at market_price=%s [%s -> %s]' %
                          (deployment_id, sell_amount, market_bid[1], sell_asset, buy_asset))
            with stellar.new_transaction(account, signers=[signer]) as t:
                # sell counter asset buy exchanging base asset
                t.add_offer(sell_amount, sell_asset, buy_asset, market_bid[1])

            if t.is_success():
                trxid, _ = t.result()
                logging.debug('Trade executed for did = %s, trxid = %s' % (deployment_id, trxid))
            else:
                err = str(t.errors())
                logging.error('Trade execution failed for did = %s, err = %s' % (deployment_id, err))
    else:
        logging.error('Incorrect advice %s' % advice)
        return False, ALGO_ERROR, 'Incorrect advice %s' % advice

    # todo log the trade in database recoconciliation

    if not trxid:
        # todo log error
        return False, ALGO_ERROR, err

    try:
        offerid = stellar.transaction(trxid).effects().first().offer_id
        if offerid:
            # remove offer if it still exist
            offers = stellar.account(account).offers().fetch().records
            for offer in offers:
                if offer.offerid == offerid:
                    logging.debug('Removing residue offer for did = %s, offerid = %s' % (deployment_id, offerid))
                    # ignore the error assuming offer got fulfilled (hence offer-not-found)
                    # we will check the final bought/sold below
                    with stellar.new_transaction(account, signers=[signer]) as t:
                        t.remove_offer(offerid, sell_asset, buy_asset)

        total_sold = 0
        total_bought = 0
        effects = stellar.transaction(trxid).effects().fetch().records
        for effect in effects:
            if effect.type == 'trade' and effect.account == account:
                total_sold += float(effect.sold_amount)
                total_bought += float(effect.bought_amount)

        if advice == TradeAdvice.BUY:
            lock = tcontext['lock']
            try:
                lock.acquire()
                tcontext['amount'] = buy_amount - total_sold
                tcontext['sell_amount'] = sell_amount + total_bought
            finally:
                lock.release()
        elif advice == TradeAdvice.SELL:
            lock = tcontext['lock']
            try:
                lock.acquire()
                tcontext['amount'] = buy_amount + total_bought
                tcontext['sell_amount'] = sell_amount - total_sold
            finally:
                lock.release()

        ts = (datetime.datetime.utcnow() - EPOCH).total_seconds()
        with sqlite.connect(get_main_db()) as db:
            db.execute("insert into trades"
                       "(ts, deployment_id, advice, sold_asset, sold_amount, bought_asset, bought_amount)"
                       " values (?, ?, ?, ?, ?, ?, ?)",
                       [ts, deployment_id, advice,
                        format_asset(sell_asset), float(total_sold),
                        format_asset(buy_asset), float(total_bought)])
            db.commit()
    except Exception as e:
        logging.exception('Exception occurred while processing trade advice')
        return False, ALGO_ERROR, ('Internal error %s' % str(e))

    logging.info('Trade executed for did=%s, sold_asset=%s, sold_amount=%s, bought_asset=%s, bought_amount=%s' %
                 (deployment_id, sell_asset, total_sold, buy_asset, total_bought))

    return True, None, None


async def process_result(engine_pipeline, deployment_id, future):
    try:
        is_success, action, err = future.result()
    except:
        logging.exception('Exception occurred while processing trade')
        return

    if not is_success:
        if action == ALGO_DONE:
            logging.info('Sending engine to stop processing did = %s [DONE]' % deployment_id)
            await engine_pipeline.put((Engine.COMMAND_DONE, deployment_id))
        elif action == ALGO_ERROR:
            logging.info('Sending engine to stop processing did = %s [ERROR]' % deployment_id)
            await engine_pipeline.put((Engine.COMMAND_STOP, deployment_id, err))
        else:
            # todo log error
            pass


FUTURE_CLEANUP_FREQUENCY = 5


async def run_trader(loop, executor, trading_config, advice_pipeline, engine_pipeline):
    futures = []
    logging.info('Starting trader loop')

    future_cleanup_wait = 0
    while True:
        await asyncio.sleep(1)

        if future_cleanup_wait >= FUTURE_CLEANUP_FREQUENCY:
            # do cleanup every x seconds
            future_cleanup_wait = 0
            i = 0
            to_remove = []
            for df in futures:
                did, f = df[0], df[1]
                if f.done():
                    await process_result(engine_pipeline, did, f)
                    to_remove += [i]
                i += 1

            for rem in to_remove:
                del futures[rem]
        future_cleanup_wait += 1

        if advice_pipeline.empty():
            continue
        advice = await advice_pipeline.get()
        logging.debug('Trader processing new advice in did=%s advice=%s' % (advice.deployment_id, advice.advice))
        future = loop.run_in_executor(executor, execute_trade, trading_config, advice.user_profile,
                                      advice.deployment_id, advice.tradepair, advice.advice, advice.amount,
                                      advice.num_cycles)
        if not future.done():
            futures += [(advice.deployment_id, future)]
        else:
            await process_result(engine_pipeline, advice.deployment_id, future)
