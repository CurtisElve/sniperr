import asyncio
import aiohttp
import json
import math
import requests
from datetime import datetime, timezone
from gql import Client, gql
from gql.transport.websockets import WebsocketsTransport

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer ory_at_DY590MZfukhtgf6_ryUQW1NdovC9Pbl79GoxZCIjMJY.tI-wUv9C0b9RzKv3KVKcXHMwMNz4IQt4X-lUDFxYP44'
}
tasks = {}
url = "https://streaming.bitquery.io/eap"
pump_url = "https://pumpportal.fun/api/trade?api-key=b14qmjj3e5bn6xancn4pgpkaath38k3q9ngn0ebrcxpq4tv9chw2pua69n234bv6e1w48pjmf9rn8dbaf5m5ggkcehbpyutgd14k8dkq8xt5auucah64pxkj699pavvu95unawvtewykuagvm4w9r9d4n6y3db11kgju5cgch7jpva175n54ujqctwq0hjmedw3gta48h8kuf8"
devToken = {}
currentTask = None
subscriptionmanagerrrr = None
devQuery = ""
socketa = WebsocketsTransport(
    url="wss://streaming.bitquery.io/eap?token=ory_at_DY590MZfukhtgf6_ryUQW1NdovC9Pbl79GoxZCIjMJY.tI-wUv9C0b9RzKv3KVKcXHMwMNz4IQt4X-lUDFxYP44",
    headers={"Sec-WebSocket-Protocol": "graphql-ws"},
)
socketb = socketa = WebsocketsTransport(
    url="wss://streaming.bitquery.io/eap?token=ory_at_DY590MZfukhtgf6_ryUQW1NdovC9Pbl79GoxZCIjMJY.tI-wUv9C0b9RzKv3KVKcXHMwMNz4IQt4X-lUDFxYP44",
    headers={"Sec-WebSocket-Protocol": "graphql-ws"},
)
holding = {}
last3 = {}
snipe = ""
moneyyy = 0.05
lastcall = {"time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
last_activity = datetime.now(timezone.utc)

def is_timestamp_good(timestamp_str):
    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - timestamp).total_seconds() / 3600 < 0.0333

def pop_oldest(d):
    if not d:
        raise KeyError("Dict is empty, fam")
    oldest_key = next(iter(d))  # Get first key
    return d.pop(oldest_key)    # Pop and return value

async def safe_request(url, data, retries=3):
    for attempt in range(retries):
        try:
            response = requests.post(url, data=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Trade failed, retrying ({attempt+1}/{retries}): {e}")
            await asyncio.sleep(2 ** attempt)
    print("Trade API down, skipping...")
    return None

async def spawnCamp():
    global devToken
    while True:
        if len(holding) == 0:
            print("Spawning camp")
            global lastcall
            payload = json.dumps({
                "query": f"""{{
                    Solana {{
                        TokenSupplyUpdates(
                            where: {{
                                Instruction: {{ Program: {{ Method: {{ is: "create" }} }} }},
                                TokenSupplyUpdate: {{ Currency: {{ MintAddress: {{ endsWith: "pump" }} }} }},
                                Block: {{ Time: {{ after: "{lastcall["time"]}" }} }}
                            }},
                            limit: {{ count: 100 }},
                            orderBy: {{ ascending: Block_Time }}
                        ) {{
                            Block {{ Time }}
                            Transaction {{ Signer }}
                            TokenSupplyUpdate {{ Currency {{ Symbol MintAddress }} }}
                        }}
                    }}
                }}""",
            })

            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, data=payload) as response:
                    data = await response.json()
                    data = data.get('data', {}).get('Solana', {}).get('TokenSupplyUpdates', [])

            if data:
                lastcall = {"time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
                for token in data:
                    realshit = await isDevABaller(token["TokenSupplyUpdate"]["Currency"]["MintAddress"])
                    if realshit and token["TokenSupplyUpdate"]["Currency"]["MintAddress"] not in [v[0] for v in devToken.values()]:
                        if len(devToken) > 5:
                            print("popped")
                            pop_oldest(devToken)
                        devToken[token["Transaction"]["Signer"]] = [
                            token["TokenSupplyUpdate"]["Currency"]["MintAddress"],
                            token["Block"]["Time"],
                            realshit[0],
                            realshit[1],
                        ]
        await asyncio.sleep(22)

async def isDevABaller(token):
    payload = json.dumps({
        "query": f'''query MyQuery {{
            Solana {{
                DEXTrades(
                where: {{Trade: {{Buy: {{Currency: {{MintAddress: {{is: "{token}"}}}}}}}}}}
                limit: {{count: 1}}
                orderBy: {{ascending: Block_Time}}
                ) {{
                Trade {{
                        Buy {{
                            Amount
                            Account {{
                                Owner
                            }}
                            PriceInUSD
                            Price
                        }}
                    }}
                }}
            }}
        }}''',
        "variables": "{}"
    })

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=payload) as response:
            data = await response.json()
            data = data.get('data', {}).get('Solana', {}).get('DEXTrades', [])
    if data:
        trade = data[0]['Trade']['Buy']
        amount = float(trade['Amount'])
        price = trade['Price']
    else:
        return False

    if price > .00000444 and amount*price > 100:
        print("big baller found: " + token)
        return [trade['Price'], amount * price]
    else:
        return False

async def sell(mint):
    response = await safe_request(pump_url, data={
        "action": "sell",
        "mint": mint,
        "amount": "100%",
        "denominatedInSol": "false",
        "slippage": 100,
        "priorityFee": 0.00005,
        "pool": "pump"
    })
    if response:
        print(response)
    else:
        print("Sell failed")

async def restart_task(taskname):
    global socket, tasks
    tasks[taskname].cancel()
    try:
        await tasks[taskname]
        print(f"Task {taskname} cancelled")
    except Exception as e:
        print(f"{e}")

    if taskname == "subscriptionManager":
        tasks["subscriptionManager"] = asyncio.create_task(subscriptionManager(), name="subscriptionManager")
    elif taskname == "smgNoSnipes":
        tasks["smgNoSnipes"] = asyncio.create_task(smgNoSnipes(), name="smgNoSnipes")
    elif taskname == "spawnCamp":
        tasks["spawnCamp"] = asyncio.create_task(spawnCamp(), name="spawnCamp")

async def heartbeat_check(batch):
    global last_activity, moneyyy, holding
    while True:
        await asyncio.sleep(22)
        now = datetime.now(timezone.utc)
        if (now - last_activity).total_seconds() > 22 and batch:  # Only nuke if batch isn’t empty
            print("No buys for 22s - nuking batch")
            for dev in list(batch.keys()):
                try:
                    await sell(batch[dev]["mint"])
                    moneyyy += batch[dev]["bought"]  # 1x since no price updates
                    print(f"Cash: {moneyyy:.6f} SOL")
                except Exception as e:
                    print(f"Sell failed for {dev}: {e}")
                finally:
                    holding.pop(dev, None)
                    batch.pop(dev)
            await restart_task("subscriptionManager")
        else:
            await asyncio.sleep(22)

async def timerKillSwitch(batch):
    await asyncio.sleep(22)
    for dev, _ in batch:
        devToken.pop(dev, None)
    await restart_task("smgNoSnipes")

trade_timestamps = {}
peak_tpm = {}
async def ate(query, batch):
    global holding, moneyyy, last_activity, trade_timestamps, last3, peak_tpm
    window_seconds = 15
    print(f"Ate started - Batch: {len(batch)} coins, SOL: {moneyyy:.6f}")
    try:
        # Concurrent subscription loop—optimized inside
        async for result in socketa.subscribe(query):
            last_activity = datetime.now(timezone.utc)
            print("Subscription tick")
            if not result.data:
                print("No data in result, skipping")
                continue
            for dev, trades in result.data.items():
                if dev[1:] not in batch:
                    print(f"Dev {dev[1:]} not in batch, skipping")
                    continue
                dex_trades = trades.get("DEXTrades", [])
                if not dex_trades:
                    print(f"No DEX trades for {dev[1:]}, skipping")
                    continue
                trade = dex_trades[0]
                current_price = trade["Trade"]["Buy"]["Price"]
                if last3.get(dev[1:]) is None:
                    last3[dev[1:]] = []
                last3[dev[1:]].append(current_price)
                if len(last3[dev[1:]]) > 3:
                    last3[dev[1:]].pop(0)
                amount = float(trade["Trade"]["Buy"]["Amount"])
                block_time = trade["Block"]["Time"]

                if dev[1:] not in trade_timestamps:
                    trade_timestamps[dev[1:]] = {"times": [], "start": block_time}
                trade_timestamps[dev[1:]]["times"].append(block_time)

                now = datetime.now(timezone.utc)
                trade_timestamps[dev[1:]]["times"] = [
                    t for t in trade_timestamps[dev[1:]]["times"]
                    if (now - datetime.strptime(t, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)).total_seconds() <= window_seconds
                ]

                trades_per_30sec = len(trade_timestamps[dev[1:]]["times"])
                trades_per_min = (trades_per_30sec / window_seconds) * 60
                take_profit = trades_per_min * 0.01 + .9
                pricecap = batch[dev[1:]]["firstprice"] * (-666 / (batch[dev[1:]]["firstbuy"] + 666) + 2)
                stoploss = .95
                elapsed = (now - datetime.strptime(trade_timestamps[dev[1:]]["start"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)).total_seconds()
                if elapsed > 22:
                    stoploss = 0.90
                if "bought" not in batch[dev[1:]]:
                    if trades_per_min > 7 and current_price < pricecap and current_price > batch[dev[1:]]["firstprice"] * 0.8 and (last3[dev[1:]][-1] > last3[dev[1:]][-2] or last3[dev[1:]][-2] > last3[dev[1:]][-3]):
                        entry_price = current_price
                        bought = ((-666 / (batch[dev[1:]]["firstbuy"] + 666) + 1) * moneyyy) / len(batch)
                        batch[dev[1:]]["bought"] = bought
                        batch[dev[1:]]["entry_price"] = entry_price
                        if moneyyy < bought:
                            print(f"Not enough SOL ({moneyyy:.6f} < {bought:.6f})")
                            batch.pop(dev[1:])
                            holding.pop(dev[1:], None)
                            continue
                        response = await safe_request(pump_url, data={
                            "action": "buy",
                            "mint": batch[dev[1:]]["mint"],
                            "amount": bought,
                            "denominatedInSol": "true",
                            "slippage": 20,
                            "priorityFee": 0.00005,
                            "pool": "pump"
                        })
                        if not response:
                            print("Buy failed")
                            batch.pop(dev[1:])
                            holding.pop(dev[1:], None)
                            continue
                        moneyyy -= bought
                        print(f"Bought {batch[dev[1:]]['mint']} at {entry_price:.10f} SOL - Cash: {moneyyy:.6f} SOL")
                        print(response)
                    else:
                        print(f"Waiting - TPM: {trades_per_min:.2f} (< 7), Price: {current_price:.10f}, Elapsed: {elapsed:.1f}s")
                        if elapsed > 22:
                            print(f"Timeout - TPM {trades_per_min:.2f} never hit 7, no buy")
                            batch.pop(dev[1:])
                            holding.pop(dev[1:], None)
                else:
                    entry_price = batch[dev[1:]]["entry_price"]
                    price_ratio = current_price / entry_price
                    print(f"Stats - TPM: {trades_per_min:.2f}, Ratio: {price_ratio:.2f}, Price: {current_price:.10f}, Elapsed: {elapsed:.1f}s")
                    peak_tpm[dev[1:]] = max(peak_tpm.get(dev[1:], trades_per_min), trades_per_min)
                    if (trades_per_min < elapsed and elapsed > 20) or trades_per_min < 0.84 * peak_tpm[dev[1:]]:
                        print(f"sloweddown - Sold at {current_price:.10f} ({price_ratio:.2f}x) after {elapsed:.1f}s")
                        await sell(batch[dev[1:]]["mint"])
                        moneyyy += batch[dev[1:]]["bought"] * price_ratio
                        print(f"Cash: {moneyyy:.6f} SOL")
                        batch.pop(dev[1:])
                        holding.pop(dev[1:], None)
                    elif current_price > pricecap:
                        print(f"stock peaking - Sold at {current_price:.10f} ({price_ratio:.2f}x) after {elapsed:.1f}s")
                        await sell(batch[dev[1:]]["mint"])
                        moneyyy += batch[dev[1:]]["bought"] * price_ratio
                        print(f"Cash: {moneyyy:.6f} SOL")
                        batch.pop(dev[1:])
                        holding.pop(dev[1:], None)
                    elif price_ratio <= 1.1 and elapsed > 20:
                        print(f"Timeout - Sold at {current_price:.10f} ({price_ratio:.2f}x) after {elapsed:.1f}s")
                        await sell(batch[dev[1:]]["mint"])
                        moneyyy += batch[dev[1:]]["bought"] * price_ratio
                        print(f"Cash: {moneyyy:.6f} SOL")
                        batch.pop(dev[1:])
                        holding.pop(dev[1:], None)
                    elif price_ratio <= stoploss:
                        print(f"Stop Loss - Sold at {current_price:.10f} ({price_ratio:.2f}x), SL: {stoploss:.2f}")
                        await sell(batch[dev[1:]]["mint"])
                        moneyyy += batch[dev[1:]]["bought"] * price_ratio
                        print(f"Cash: {moneyyy:.6f} SOL")
                        batch.pop(dev[1:])
                        holding.pop(dev[1:], None)
                    elif price_ratio >= take_profit:
                        print(f"Take Profit - Sold at {current_price:.10f} ({price_ratio:.2f}x), TP: {take_profit:.2f}")
                        await sell(batch[dev[1:]]["mint"])
                        moneyyy += batch[dev[1:]]["bought"] * price_ratio
                        print(f"Cash: {moneyyy:.6f} SOL")
                        batch.pop(dev[1:])
                        holding.pop(dev[1:], None)
                    else:
                        print(f"Holding - Bought at {entry_price:.10f}, Now {current_price:.10f} ({price_ratio:.2f}x), SL: {stoploss:.2f}, TP: {take_profit:.2f}")

            # Check if batch is empty inside the loop
            if not batch:
                print("Batch empty - all processed")
                break  # Exit subscription loop, let outer block finish
    except Exception as e:
        print(f"Subscription crashed: {e} - proceeding to cleanup")

    # After subscription ends (break, error, or natural end), finalize
    if batch:
        print(f"Subscription ended with {len(batch)} keys left")

async def subscriptionTask(query):
    global holding
    if len(holding) == 0:
        try:
            print("fishing out here yo")
            async for result in socketb.subscribe(query):
                if not result.data:
                    continue
                for dev, trades in result.data.items():
                    if dev[1:] in devToken:
                        dex_trades = trades.get("DEXTrades", [])
                        if dex_trades:
                            first_trade = dex_trades[0]
                            if rugcheck(first_trade["Trade"]["Buy"]["Currency"]["MintAddress"]):
                                coin = {
                                    "mint": first_trade["Trade"]["Buy"]["Currency"]["MintAddress"],
                                    "amount": first_trade["Trade"]["Buy"]["Amount"],
                                    "price": first_trade["Trade"]["Buy"]["Price"],
                                    "firstbuy": devToken[dev[1:]][3],
                                    "firstprice": devToken[dev[1:]][2],
                                }
                                holding[dev[1:]] = coin
                                print("SNIPE!!" + coin["mint"] + " -- " + str(coin["price"]) + " -- firstprice=" + str(coin["firstprice"]) + " -- firstbuy=" + str(coin["firstbuy"]))
                            devToken.pop(dev[1:], None)
        except Exception as e:
            print(f"SubscriptionTask crashed: {e}")
    else:
        await asyncio.sleep(11)

async def smgNoSnipes():
    global devQuery, currentTask, holding
    while True:
        if len(devToken) != 0 and len(holding) == 0:
            devQuery = ""
            x = devToken.copy().items()
            for dev, token in x:
                if is_timestamp_good(token[1]):
                    devQuery += f'''
                        {"a" + dev} : Solana {{
                            DEXTrades(
                                where: {{
                                    Trade: {{
                                        Dex: {{ ProtocolName: {{ is: "pump" }} }},
                                        Buy: {{
                                            Currency: {{ MintAddress: {{ is: "{token[0]}" }} }},
                                        }},
                                        Sell: {{ AmountInUSD: {{ gt: "55.5" }} }},
                                    }},
                                    Transaction: {{ Result: {{ Success: true }} }}
                                }}
                            ) {{
                                Trade {{ 
                                    Buy {{ 
                                        Amount 
                                        Price 
                                        PriceInUSD 
                                        Currency {{ Symbol MintAddress }}
                                        Account {{ Owner }}
                                    }} 
                                }}
                                Block {{ Time }}
                            }}
                        }}\n
                    '''
                else:
                    devToken.pop(dev, None)
            if len(devQuery) != 0:
                query = gql(f"""
                    subscription {{
                        {devQuery}
                    }}
                """)
                await asyncio.wait_for(subscriptionTask(query), timeout=22)
                print("done!!!!!")
        else:
            await asyncio.sleep(11)

async def subscriptionManager():
    global trade_timestamps, moneyyy, holding, last3
    while True:
        if len(holding) != 0:
            snipe = ""
            batch = {}
            for dev, coin in holding.items():
                batch[dev] = coin
                snipe += f'''
                    {"a" + dev} : Solana {{
                        DEXTrades(
                            where: {{
                                Trade: {{
                                    Buy: {{ Currency: {{ MintAddress: {{ is: "{coin["mint"]}" }} }} }},
                                    Dex: {{ ProtocolName: {{ is: "pump" }} }},
                                    Sell: {{ AmountInUSD: {{ gt: "3.33" }} }},
                                }}
                                Transaction: {{ Result: {{ Success: true }} }}
                            }}
                        ) {{
                            Trade {{ 
                                Buy {{ 
                                    Amount 
                                    Price 
                                    Currency {{ Symbol MintAddress }}
                                    Account {{ Owner }}
                                }} 
                            }}
                            Block {{ Time }}
                        }}
                    }}\n
                '''
            if len(snipe) != 0:
                letsGetThisBRead = gql(f"""
                    subscription {{
                        {snipe}
                    }}
                """)
                keepgoingyurr = True
                crashes = 0
                trade_timestamps = {}
                last3 = {}
                initialtime = datetime.now()
                while keepgoingyurr:
                    await asyncio.wait_for(ate(letsGetThisBRead, batch), timeout=66)
                    crashes += 1
                    keepgoingyurr = False
                    for key in batch.keys():
                        if key in holding:
                            keepgoingyurr = True
                        else:
                            batch.pop(key)
                    if (datetime.now() - initialtime).total_seconds() > 22:
                        print("crashing out bad coin in the batch")
                        for dev in list(batch.keys()):
                            try:
                                await sell(batch[dev]["mint"])
                                moneyyy += batch[dev]["bought"]
                            except Exception as e:
                                print(f"Sell failed for {dev}: {e}")
                            holding.pop(dev, None)
                            keepgoingyurr = False
        else:
            await asyncio.sleep(1)

def rugcheck(token):
    check = requests.get(f"https://api.rugcheck.xyz/v1/tokens/{token}/report/summary")
    print(check.json())
    if check.json()["score_normalised"] == 1:
        return True
    else:
        print("fuck these devs hope they perish")
        return False


async def main():
    global socketa, socketb
        # Only connect if not already connected
    if socketa.websocket is None or socketa.websocket.closed:
        await socketa.connect()
    if socketb.websocket is None or socketb.websocket.closed:
        await socketb.connect()
    tasks["subscriptionManager"] = asyncio.create_task(subscriptionManager(), name="subscriptionManager")
    tasks["spawnCamp"] = asyncio.create_task(spawnCamp(), name="spawnCamp")
    tasks["smgNoSnipes"] = asyncio.create_task(smgNoSnipes(), name="smgNoSnipes")
    await asyncio.Future()

asyncio.run(main())