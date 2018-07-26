import asyncio
import logging

import stellar

from stardust.data import Candle


def fetch_trade(fetcher_config, cursor):
    try:
        if not cursor:
            # if cursor is None then start from the latest trade
            cursor = stellar.trades().last().paging_token
            logging.info('Last trade = %s' % cursor)
        logging.debug('Fetching trades from stellar network at cursor = %s' % cursor)
        return cursor, stellar.trades().fetch(cursor=cursor, limit=10).records
    except:
        logging.exception('Error occurred while fetching trades from stellar network')
        return cursor, None


async def run_fetcher(loop, executor, fetcher_config, candle_pipeline):
    cursor = None
    asset_candles = {}

    def asset_format(asset):
        if asset.asset_type == 'native':
            res = 'XLM_native'
        else:
            res = asset.asset_code + '_' + asset.asset_issuer
        return res

    while True:
        await asyncio.sleep(10)

        cursor, entries = await loop.run_in_executor(executor, fetch_trade, fetcher_config, cursor)

        logging.debug('Fetch complete = %s entries' % ('0' if not entries else len(entries)))

        if not entries:
            continue

        for e in entries:
            logging.debug('Processing %s trades' % len(entries))

            base = e.base_asset
            counter = e.counter_asset

            key = asset_format(base) + '_' + asset_format(counter)
            if key not in asset_candles:
                asset_candles[key] = Candle(key)
            if not asset_candles[key].process_row(e):
                candle = asset_candles[key]
                logging.info('Sending new candle for processing = %s', candle.key)
                await candle_pipeline.put(candle)

                asset_candles[key] = Candle(key)  # create new candle
                asset_candles[key].process_row(e)  # process new candle

            cursor = e.paging_token
