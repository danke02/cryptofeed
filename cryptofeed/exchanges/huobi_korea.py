'''
Copyright (C) 2017-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from cryptofeed.symbols import Symbol
from cryptofeed.util.time import timedelta_str_to_sec
import logging
from typing import Dict, Tuple
import zlib
from decimal import Decimal

from sortedcontainers import SortedDict as sd
from yapic import json

from cryptofeed.connection import AsyncConnection
from cryptofeed.defines import BID, ASK, BUY, L2_BOOK, SELL, TRADES, CANDLES, HUOBI_KOREA
from cryptofeed.feed import Feed

LOG = logging.getLogger('feedhandler')


class HuobiKorea(Feed):
    id = HUOBI_KOREA
    symbol_endpoint = 'https://api-cloud.huobi.co.kr/v1/common/symbols'
    websocket_channels = {
        L2_BOOK: 'depth.step0',
        TRADES: 'trade.detail',
    }

    @classmethod
    def timestamp_normalize(cls, ts: float) -> float:
        return ts / 1000.0

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:
        ret = {}
        info = {'instrument_type': {}}

        for e in data['data']:
            if e['state'] == 'offline':
                continue
            base, quote = e['base-currency'].upper(), e['quote-currency'].upper()
            s = Symbol(base, quote)

            ret[s.normalized] = e['symbol']
            info['instrument_type'][s.normalized] = s.type
        return ret, info

    def __init__(self, **kwargs):
        super().__init__('wss://api-cloud.huobi.co.kr/ws', **kwargs)
        self.__reset()

    def __reset(self):
        self._l2_book = {}

    async def _book(self, msg: dict, timestamp: float):
        pair = self.exchange_symbol_to_std_symbol(msg['ch'].split('.')[1])
        data = msg['tick']
        forced = pair not in self._l2_book

        update = {
            BID: sd({
                Decimal(price): Decimal(amount)
                for price, amount in data['bids']
            }),
            ASK: sd({
                Decimal(price): Decimal(amount)
                for price, amount in data['asks']
            })
        }

        if not forced:
            self.previous_book[pair] = self._l2_book[pair]
        self._l2_book[pair] = update

        await self.book_callback(self._l2_book[pair], L2_BOOK, pair, forced, False, self.timestamp_normalize(msg['ts']), timestamp)

    async def _trade(self, msg: dict, timestamp: float):
        """
        {
            'ch': 'market.adausdt.trade.detail',
            'ts': 1597792835344,
            'tick': {
                'id': 101801945127,
                'ts': 1597792835336,
                'data': [
                    {
                        'id': Decimal('10180194512782291967181675'),   <- per docs this is deprecated
                        'ts': 1597792835336,
                        'tradeId': 100341530602,
                        'amount': Decimal('0.1'),
                        'price': Decimal('0.137031'),
                        'direction': 'sell'
                    }
                ]
            }
        }
        """
        for trade in msg['tick']['data']:
            await self.callback(TRADES,
                                feed=self.id,
                                symbol=self.exchange_symbol_to_std_symbol(msg['ch'].split('.')[1]),
                                order_id=trade['tradeId'],
                                side=BUY if trade['direction'] == 'buy' else SELL,
                                amount=Decimal(trade['amount']),
                                price=Decimal(trade['price']),
                                timestamp=self.timestamp_normalize(trade['ts']),
                                receipt_timestamp=timestamp)

    async def message_handler(self, msg: str, conn, timestamp: float):
        # unzip message
        msg = zlib.decompress(msg, 16 + zlib.MAX_WBITS)
        msg = json.loads(msg, parse_float=Decimal)

        # HuobiKorea sends a ping evert 5 seconds and will disconnect us if we do not respond to it
        if 'ping' in msg:
            await conn.write(json.dumps({'pong': msg['ping']}))
        elif 'status' in msg and msg['status'] == 'ok':
            return
        elif 'ch' in msg:
            if 'trade' in msg['ch']:
                await self._trade(msg, timestamp)
            elif 'depth' in msg['ch']:
                await self._book(msg, timestamp)
            else:
                LOG.warning("%s: Invalid message type %s", self.id, msg)
        else:
            LOG.warning("%s: Invalid message type %s", self.id, msg)

    async def subscribe(self, conn: AsyncConnection):
        self.__reset()
        client_id = 0
        for chan in self.subscription:
            for pair in self.subscription[chan]:
                client_id += 1
                await conn.write(json.dumps(
                    {
                        "sub": f"market.{pair}.{chan}",
                        "id": client_id
                    }
                ))
