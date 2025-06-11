from typing import List, Optional
import ccxt.async_support as ccxt
import asyncio
import pandas as pd
import time
import itertools
from pydantic import BaseModel


class UsdtBalance(BaseModel):
    total: float
    free: float
    used: float


class Info(BaseModel):
    success: bool
    message: str


class Order(BaseModel):
    id: str
    pair: str
    type: str
    side: str
    price: float
    size: float
    reduce: bool
    filled: float
    remaining: float
    timestamp: int


class TriggerOrder(BaseModel):
    id: str
    pair: str
    type: str
    side: str
    price: float
    trigger_price: float
    size: float
    reduce: bool
    timestamp: int


class Position(BaseModel):
    pair: str
    side: str
    size: float
    usd_size: float
    entry_price: float
    current_price: float
    unrealizedPnl: float
    liquidation_price: float
    margin_mode: str
    leverage: float
    hedge_mode: bool
    open_timestamp: int
    take_profit_price: float
    stop_loss_price: float


class PerpBitget:
    def __init__(self, public_api=None, secret_api=None, password=None):
        bitget_auth_object = {
            "apiKey": public_api,
            "secret": secret_api,
            "password": password,
            "enableRateLimit": True,
            "rateLimit": 100,
            "options": {"defaultType": "future"},
        }
        if bitget_auth_object["secret"] is None:
            self._auth = False
            self._session = ccxt.bitget()
        else:
            self._auth = True
            self._session = ccxt.bitget(bitget_auth_object)
        self.markets: dict = {}

    async def load_markets(self):
        """
        Load exchange markets for precision and limits.
        """
        self.markets = await self._session.load_markets()

    async def close(self):
        await self._session.close()

    def ext_pair_to_pair(self, ext_pair: str) -> str:
        return f"{ext_pair}:USDT"

    def pair_to_ext_pair(self, pair: str) -> str:
        return pair.replace(":USDT", "")

    def get_pair_info(self, ext_pair: str) -> Optional[dict]:
        pair = self.ext_pair_to_pair(ext_pair)
        return self.markets.get(pair)

    def amount_to_precision(self, pair: str, amount: float) -> Optional[str]:
        market = self.get_pair_info(pair)
        if not market:
            return None
        try:
            amt = self._session.amount_to_precision(pair, amount)
            min_amt = market['limits']['amount']['min'] or 0
            if float(amt) < float(min_amt):
                print(f"Skip {pair} order: size {amt} < min {min_amt}")
                return None
            return amt
        except Exception as e:
            print(f"Precision error for amount on {pair}: {e}")
            return None

    def price_to_precision(self, pair: str, price: float) -> Optional[str]:
        market = self.get_pair_info(pair)
        if not market:
            return None
        try:
            return self._session.price_to_precision(pair, price)
        except Exception as e:
            print(f"Precision error for price on {pair}: {e}")
            return None

    async def get_last_ohlcv(self, pair: str, timeframe: str, limit: int = 1000) -> pd.DataFrame:
        symbol = self.ext_pair_to_pair(pair)
        bitget_limit = 200
        ts_dict = {"1m": 60_000, "5m": 5*60_000, "15m": 15*60_000,
                   "1h": 60*60_000, "2h": 2*60*60_000, "4h": 4*60*60_000,
                   "1d": 24*60*60_000}
        end_ts = int(time.time() * 1000)
        start_ts = end_ts - limit * ts_dict[timeframe]
        current_ts = start_ts
        tasks = []
        while current_ts < end_ts:
            req_end = min(current_ts + bitget_limit * ts_dict[timeframe], end_ts)
            tasks.append(
                self._session.fetch_ohlcv(
                    symbol,
                    timeframe,
                    params={"limit": bitget_limit,
                            "startTime": str(current_ts),
                            "endTime": str(req_end)},
                )
            )
            current_ts = req_end + 1
        ohlcv = await asyncio.gather(*tasks)
        data = list(itertools.chain.from_iterable(ohlcv))
        df = pd.DataFrame(data, columns=["date","open","high","low","close","volume"])
        df['date'] = pd.to_datetime(df['date'], unit='ms')
        df.set_index('date', inplace=True)
        df.sort_index(inplace=True)
        return df

    async def get_balance(self) -> UsdtBalance:
        resp = await self._session.fetch_balance()
        bal = resp.get('USDT', {})
        return UsdtBalance(
            total=bal.get('total', 0.0),
            free=bal.get('free', 0.0),
            used=bal.get('used', 0.0),
        )

    async def set_margin_mode_and_leverage(self, pair: str, margin_mode: str, leverage: int) -> Info:
        if margin_mode not in ['crossed','isolated']:
            raise ValueError("Margin mode must be 'crossed' or 'isolated'")
        symbol = self.ext_pair_to_pair(pair)
        try:
            await self._session.set_margin_mode(
                margin_mode,
                symbol,
                params={'productType':'USDT-FUTURES','marginCoin':'USDT'},
            )
        except:
            pass
        try:
            if margin_mode == 'isolated':
                tasks = [
                    self._session.set_leverage(
                        leverage,
                        symbol,
                        params={
                            'productType':'USDT-FUTURES',
                            'marginCoin':'USDT',
                            'holdSide': side
                        },
                    ) for side in ['long','short']
                ]
                await asyncio.gather(*tasks)
            else:
                await self._session.set_leverage(
                    leverage,
                    symbol,
                    params={'productType':'USDT-FUTURES','marginCoin':'USDT'},
                )
        except:
            pass
        return Info(success=True,
                    message=f"Margin mode set to {margin_mode} and leverage {leverage}x")

    async def get_open_positions(self, pairs: List[str]) -> List[Position]:
        symbols = [self.ext_pair_to_pair(p) for p in pairs]
        resp = await self._session.fetch_positions(
            symbols=symbols,
            params={'productType':'USDT-FUTURES','marginCoin':'USDT'}
        )
        result = []
        for pos in resp:
            lp = pos.get('liquidationPrice') or 0.0
            tp = pos.get('takeProfitPrice') or 0.0
            sl = pos.get('stopLossPrice') or 0.0
            contracts = pos.get('contracts', 0)
            size = contracts * pos.get('contractSize', 0)
            result.append(
                Position(
                    pair=self.pair_to_ext_pair(pos['symbol']),
                    side=pos['side'],
                    size=size,
                    usd_size=round(size * pos.get('markPrice', 0), 2),
                    entry_price=pos['entryPrice'],
                    current_price=pos['markPrice'],
                    unrealizedPnl=pos.get('unrealizedPnl', 0),
                    liquidation_price=lp,
                    margin_mode=pos.get('marginMode', ''),
                    leverage=pos.get('leverage', 0),
                    hedge_mode=pos.get('hedged', False),
                    open_timestamp=pos.get('timestamp', 0),
                    take_profit_price=tp,
                    stop_loss_price=sl,
                )
            )
        return result

    async def place_order(
        self,
        pair: str,
        side: str,
        price: float,
        size: float,
        type: str = 'limit',
        reduce: bool = False,
        margin_mode: str = 'crossed',
        error: bool = False,
    ) -> Optional[Order]:
        try:
            symbol = self.ext_pair_to_pair(pair)
            trade_side = 'Open' if not reduce else 'Close'
            mm = 'cross' if margin_mode == 'crossed' else 'isolated'
            resp = await self._session.create_order(
                symbol=symbol,
                type=type,
                side=side,
                amount=size,
                price=price,
                params={
                    'reduceOnly': reduce,
                    'tradeSide': trade_side,
                    'marginMode': mm,
                },
            )
            oid = resp.get('id')
            return await self.get_order_by_id(oid, pair)
        except Exception as e:
            print(f"Error {type} {side} {size} {pair} - Price {price} - {e}")
            if error:
                raise
            return None

    async def place_trigger_order(
        self,
        pair: str,
        side: str,
        price: float,
        trigger_price: float,
        size: float,
        type: str = 'limit',
        reduce: bool = False,
        margin_mode: str = 'crossed',
        error: bool = False,
    ) -> Optional[Info]:
        try:
            symbol = self.ext_pair_to_pair(pair)
            trade_side = 'Open' if not reduce else 'Close'
            mm = 'cross' if margin_mode == 'crossed' else 'isolated'
            await self._session.create_trigger_order(
                symbol=symbol,
                type=type,
                side=side,
                amount=size,
                price=price,
                triggerPrice=trigger_price,
                params={
                    'reduceOnly': reduce,
                    'tradeSide': trade_side,
                    'marginMode': mm,
                },
            )
            return Info(success=True, message='Trigger Order set up')
        except Exception as e:
            print(f"Error {type} {side} {size} {pair} - Trigger {trigger_price} - Price {price} - {e}")
            if error:
                raise
            return None

    async def get_open_orders(self, pair: str) -> List[Order]:
        symbol = self.ext_pair_to_pair(pair)
        resp = await self._session.fetch_open_orders(symbol)
        orders = []
        for o in resp:
            orders.append(
                Order(
                    id=o['id'],
                    pair=self.pair_to_ext_pair(o['symbol']),
                    type=o['type'],
                    side=o['side'],
                    price=o['price'],
                    size=o['amount'],
                    reduce=o.get('reduceOnly', False),
                    filled=o.get('filled', 0),
                    remaining=o.get('remaining', 0),
                    timestamp=o.get('timestamp', 0),
                )
            )
        return orders

    async def get_open_trigger_orders(self, pair: str) -> List[TriggerOrder]:
        symbol = self.ext_pair_to_pair(pair)
        resp = await self._session.fetch_open_orders(symbol, params={'stop': True})
        tos = []
        for o in resp:
            red = o['info'].get('tradeSide','').lower() == 'close'
            price = o.get('price') or 0.0
            tos.append(
                TriggerOrder(
                    id=o['id'],
                    pair=self.pair_to_ext_pair(o['symbol']),
                    type=o['type'],
                    side=o['side'],
                    price=price,
                    trigger_price=o.get('triggerPrice', 0),
                    size=o.get('amount', 0),
                    reduce=red,
                    timestamp=o.get('timestamp', 0),
                )
            )
        return tos

    async def get_order_by_id(self, order_id: str, pair: str) -> Order:
        symbol = self.ext_pair_to_pair(pair)
        resp = await self._session.fetch_order(order_id, symbol)
        return Order(
            id=resp['id'],
            pair=self.pair_to_ext_pair(resp['symbol']),
            type=resp['type'],
            side=resp['side'],
            price=resp['price'],
            size=resp['amount'],
            reduce=resp.get('reduceOnly', False),
            filled=resp.get('filled', 0),
            remaining=resp.get('remaining', 0),
            timestamp=resp.get('timestamp', 0),
        )

    async def cancel_orders(self, pair: str, ids: List[str] = []) -> Info:
        symbol = self.ext_pair_to_pair(pair)
        try:
            resp = await self._session.cancel_orders(ids=ids, symbol=symbol)
            return Info(success=True, message=f"{len(resp)} orders cancelled")
        except Exception:
            return Info(success=False, message="Error or no orders to cancel")

    async def cancel_trigger_orders(self, pair: str, ids: List[str] = []) -> Info:
        symbol = self.ext_pair_to_pair(pair)
        try:
            resp = await self._session.cancel_orders(ids=ids, symbol=symbol, params={'stop': True})
            return Info(success=True, message=f"{len(resp)} trigger orders cancelled")
        except Exception:
            return Info(success=False, message="Error or no trigger orders to cancel")
