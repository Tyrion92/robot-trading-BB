import datetime
import sys
import asyncio
import ta

sys.path.append("./robot-tradingV2-main")
from utilities.bitget_perp import PerpBitget
from secret import ACCOUNTS

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def round_size(exchange, symbol, raw_amount, market_info):
    """
    Round raw_amount to the exchange precision and ensure it's above the minimum.
    Returns string amount or None if below minimum.
    """
    size_str = exchange.amount_to_precision(symbol, raw_amount)
    try:
        size_val = float(size_str)
    except (TypeError, ValueError):
        return None
    if size_val < market_info['min_amount']:
        print(f"Skip {symbol} order: size {size_val} < min {market_info['min_amount']}")
        return None
    return size_str


def round_price(exchange, symbol, raw_price):
    """
    Round raw_price to the exchange precision.
    Returns string price.
    """
    return exchange.price_to_precision(symbol, raw_price)


async def main():
    account = ACCOUNTS["bitget1"]
    margin_mode = "isolated"
    exchange_leverage = 4
    tf = "1h"
    size_leverage = 4
    sl_pct = 0.2

    # Strategy parameters per pair
    params = {
        "XRP/USDT": {
            "src": "close",
            "ma_base_window": 5,
            "envelopes": [0.05,0.075],
            "size": 0.1,
            "sides": ["long"],
        },
        "ADA/USDT": {
            "src": "close",
            "ma_base_window": 5,
            "envelopes": [0.05,0.075],
            "size": 0.1,
            "sides": ["long"]#, "short"],
        },
        "ICP/USDT": {
            "src": "close",
            "ma_base_window": 5,
            "envelopes": [0.05,0.075],
            "size": 0.1,
            "sides": ["long"],
        },
        "XTZ/USDT": {
            "src": "close",
            "ma_base_window": 5,
            "envelopes": [0.05,0.075],
            "size": 0.1,
            "sides": ["long"],
        },
        "DOGE/USDT": {
            "src": "close",
            "ma_base_window": 5,
            "envelopes": [0.05,0.075],
            "size": 0.1,
            "sides": ["long"],
        },
        "SHIB/USDT": {
            "src": "close",
            "ma_base_window": 5,
            "envelopes": [0.05,0.075],
            "size": 0.1,
            "sides": ["long"],
        },
        "SOL/USDT": {
            "src": "close",
            "ma_base_window": 5,
            "envelopes": [0.05,0.075],
            "size": 0.1,
            "sides": ["long"],
        },
        "SAND/USDT": {
            "src": "close",
            "ma_base_window": 5,
            "envelopes": [0.05,0.075],
            "size": 0.1,
            "sides": ["long"],
        },
        "AVAX/USDT": {
            "src": "close",
            "ma_base_window": 5,
            "envelopes": [0.05,0.075],
            "size": 0.1,
            "sides": ["long"],
        },
        "ETH/USDT": {
            "src": "close",
            "ma_base_window": 5,
            "envelopes": [0.05,0.075],
            "size": 0.1,
            "sides": ["long"],
        },

    }

    exchange = PerpBitget(
        public_api=account["public_api"],
        secret_api=account["secret_api"],
        password=account["password"],
    )
    invert_side = {"long": "sell", "short": "buy"}

    print(f"--- Execution started at {datetime.datetime.now():%Y-%m-%d %H:%M:%S} ---")
    try:
        # Load markets and gather market info
        markets_info = {}
        await exchange.load_markets()
        for pair in list(params.keys()):
            market = exchange.markets.get(pair)
            if not market:
                print(f"Pair {pair} not found, removing from params...")
                params.pop(pair)
                continue
            min_amt = market['limits']['amount']['min'] or 0
            markets_info[pair] = {
                'min_amount': float(min_amt),
                'amount_precision': market['precision']['amount'],
                'price_precision': market['precision']['price'],
            }
        pairs = list(params.keys())

        # Set margin mode and leverage
        print(f"Setting {margin_mode} x{exchange_leverage} on {len(pairs)} pairs...")
        await asyncio.gather(*[
            exchange.set_margin_mode_and_leverage(pair, margin_mode, exchange_leverage)
            for pair in pairs
        ])

        # Fetch OHLCV and compute indicators
        print(f"Getting data and indicators on {len(pairs)} pairs...")
        dfs = await asyncio.gather(*[exchange.get_last_ohlcv(pair, tf, 50) for pair in pairs])
        df_list = dict(zip(pairs, dfs))
        for pair, df in df_list.items():
            p = params[pair]
            src = df['close'] if p['src']=='close' else (df[['open','high','low','close']].mean(axis=1))
            df['ma_base'] = ta.trend.sma_indicator(src, p['ma_base_window'])
            highs = [round(1/(1-e)-1,3) for e in p['envelopes']]
            for i, env in enumerate(p['envelopes'], start=1):
                df[f'ma_high_{i}'] = df['ma_base']*(1+highs[i-1])
                df[f'ma_low_{i}'] = df['ma_base']*(1-env)

        # Balance and cancel orders
        usdt_balance = (await exchange.get_balance()).total
        print(f"Balance: {usdt_balance:.2f} USDT")

        # Cancel existing trigger and limit orders
        print("Canceling trigger and limit orders...")
        trigger_lists = await asyncio.gather(*[exchange.get_open_trigger_orders(p) for p in pairs])
        order_lists = await asyncio.gather(*[exchange.get_open_orders(p) for p in pairs])
        await asyncio.gather(*[
            exchange.cancel_trigger_orders(p, [o.id for o in tl]) for p, tl in zip(pairs, trigger_lists)
        ])
        await asyncio.gather(*[
            exchange.cancel_orders(p, [o.id for o in ol]) for p, ol in zip(pairs, order_lists)
        ])

        # Get positions
        print("Getting live positions...")
        positions = await exchange.get_open_positions(pairs)
        tasks_close, tasks_open = [], []
        # Close existing positions and set SL
        for pos in positions:
            pair = pos.pair
            prev = df_list[pair].iloc[-2]
            # Close limit order at MA
            size_str = round_size(exchange, pair, pos.size, markets_info[pair])
            if size_str:
                price_str = round_price(exchange, pair, prev['ma_base'])
                tasks_close.append(exchange.place_order(
                    pair=pair, side=invert_side[pos.side], price=price_str,
                    size=size_str, type='limit', reduce=True,
                    margin_mode=margin_mode, error=False
                ))
            # Stop-loss trigger
            if size_str:
                if pos.side=='long':
                    sl_side, raw_sl = 'sell', pos.entry_price*(1-sl_pct)
                else:
                    sl_side, raw_sl = 'buy', pos.entry_price*(1+sl_pct)
                trigger_str = round_price(exchange, pair, raw_sl)
                tasks_close.append(exchange.place_trigger_order(
                    pair=pair, side=sl_side, trigger_price=trigger_str,
                    price=None, size=size_str, type='market',
                    reduce=True, margin_mode=margin_mode, error=False
                ))
            # Re-open envelopes
            canceled_buys = sum(1 for o in trigger_lists[pairs.index(pair)] if o.side=='buy' and not o.reduce)
            canceled_sells = sum(1 for o in trigger_lists[pairs.index(pair)] if o.side=='sell' and not o.reduce)
            for side, count, label in [('buy', canceled_buys, 'ma_low_'), ('sell', canceled_sells, 'ma_high_')]:
                for i in range(len(params[pair]['envelopes'])-count, len(params[pair]['envelopes'])):
                    price_key = f"{label}{i+1}"
                    raw_price = prev[price_key]
                    raw_trigger = raw_price * (1.005 if side=='buy' else 0.995)
                    raw_size = (params[pair]['size']*usdt_balance/len(params[pair]['envelopes'])*size_leverage)/raw_price
                    size2 = round_size(exchange, pair, raw_size, markets_info[pair])
                    if size2:
                        tasks_open.append(exchange.place_trigger_order(
                            pair=pair, side=side,
                            price=round_price(exchange, pair, raw_price),
                            trigger_price=round_price(exchange, pair, raw_trigger),
                            size=size2, type='limit', reduce=False,
                            margin_mode=margin_mode, error=False
                        ))

        print(f"Placing {len(tasks_close)} close SL/limit orders...")
        await asyncio.gather(*tasks_close)

        # Open new positions where none exist
        existing = {pos.pair for pos in positions}
        for pair in pairs:
            if pair in existing: continue
            prev = df_list[pair].iloc[-2]
            for i, env in enumerate(params[pair]['envelopes'], start=1):
                for side in params[pair]['sides']:
                    key = 'ma_low_' if side=='long' else 'ma_high_'
                    raw_price = prev[f"{key}{i}"]
                    raw_trigger = raw_price*(1.005 if side=='long' else 0.995)
                    raw_size = (params[pair]['size']*usdt_balance/len(params[pair]['envelopes'])*size_leverage)/raw_price
                    size2 = round_size(exchange, pair, raw_size, markets_info[pair])
                    if not size2: continue
                    tasks_open.append(exchange.place_trigger_order(
                        pair=pair, side=('buy' if side=='long' else 'sell'),
                        price=round_price(exchange, pair, raw_price),
                        trigger_price=round_price(exchange, pair, raw_trigger),
                        size=size2, type='limit', reduce=False,
                        margin_mode=margin_mode, error=False
                    ))

        print(f"Placing {len(tasks_open)} open limit orders...")
        await asyncio.gather(*tasks_open)

        await exchange.close()
        print(f"--- Execution finished at {datetime.datetime.now():%Y-%m-%d %H:%M:%S} ---")

    except Exception as e:
        await exchange.close()
        raise


if __name__ == "__main__":
    asyncio.run(main())
