import asyncio
import aiohttp
import json
import math
from os import getenv
from dotenv import load_dotenv
import requests
from datetime import datetime, timezone
from gql import Client, gql
from gql.transport.websockets import WebsocketsTransport

# Must have env variables BITQUERY_API_KEY and PUMPPORTAL_API_KEY
load_dotenv()

headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer ' + getenv("BITQUERY_API_KEY")
}
tasks = {}
url = "https://streaming.bitquery.io/eap"
pump_url = "https://pumpportal.fun/api/trade?api-key=" + getenv("PUMPPORTAL_API_KEY")
monitored_tokens = {}
currentTask = None
sub_manager_task = None
devQuery = ""
primary_socket = WebsocketsTransport(
    url="wss://streaming.bitquery.io/eap?token=" + getenv("BITQUERY_API_KEY"),
    headers={"Sec-WebSocket-Protocol": "graphql-ws"},
)
secondary_socket = WebsocketsTransport(
    url="wss://streaming.bitquery.io/eap?token=" + getenv("BITQUERY_API_KEY"),
    headers={"Sec-WebSocket-Protocol": "graphql-ws"},
)
holding = {}
price_history_brief = {}
snipe = ""
sol_balance = 0.04
last_sync_time = {"time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
last_activity = datetime.now(timezone.utc)

def is_timestamp_good(timestamp_str):
    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - timestamp).total_seconds() / 3600 < 0.0333

def pop_oldest(d):
    if not d:
        raise KeyError("Dictionary is empty.")
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
    print("Trade API unavailable, skipping...")
    return None

async def monitor_new_mints():
    global monitored_tokens
    while True:
        if len(holding) == 0:
            print("Scanning for new mints...")
            global last_sync_time
            payload = json.dumps({
                "query": f"""{{
                    Solana {{
                        TokenSupplyUpdates(
                            where: {{
                                Instruction: {{ Program: 
                                    {{ 
                                    Address: {{ is: "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P" }},
                                    Method: {{in: ["create","create_v2"]}}
                                    }} 
                                }},
                                TokenSupplyUpdate: {{ Currency: {{ MintAddress: {{ endsWith: "pump" }} }} }},
                                Block: {{ Time: {{ after: "{last_sync_time["time"]}" }} }}
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
                last_sync_time = {"time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
                for token in data:
                    print(f"New token detected: {token['TokenSupplyUpdate']['Currency']['MintAddress']}")
                    market_data = await check_dev_liquidity(token["TokenSupplyUpdate"]["Currency"]["MintAddress"])
                    if market_data and token["TokenSupplyUpdate"]["Currency"]["MintAddress"] not in [v[0] for v in monitored_tokens.values()]:
                        if len(monitored_tokens) > 5:
                            print("Rotating monitor queue (limit reached).")
                            pop_oldest(monitored_tokens)
                        monitored_tokens[token["Transaction"]["Signer"]] = [
                            token["TokenSupplyUpdate"]["Currency"]["MintAddress"],
                            token["Block"]["Time"],
                            market_data[0],
                            market_data[1],
                        ]
        await asyncio.sleep(22)

async def check_dev_liquidity(token):
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
            }}
            ''',
    "variables": "{}"
    })

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, data=payload) as response:
            data = await response.json()
            data = data.get('data', {}).get('Solana', {}).get('DEXTrades', [])
    if data:
        trade = data[0]['Trade']['Buy']
        amount = float(trade['Amount'])
        price = trade['PriceInUSD']
    else:
        return False

    if amount*price > 333:
        print(f"Significant liquidity found: {token}")
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
        print(f"Sell executed: {response}")
    else:
        print("Sell operation failed")

async def restart_task(taskname):
    global tasks
    tasks[taskname].cancel()
    try:
        await tasks[taskname]
        print(f"Task {taskname} terminated")
    except Exception as e:
        print(f"Shutdown log: {e}")

    if taskname == "subscriptionManager":
        tasks["subscriptionManager"] = asyncio.create_task(subscriptionManager(), name="subscriptionManager")
    elif taskname == "monitor_untracked_tokens":
        tasks["monitor_untracked_tokens"] = asyncio.create_task(monitor_untracked_tokens(), name="monitor_untracked_tokens")
    elif taskname == "monitor_new_mints":
        tasks["monitor_new_mints"] = asyncio.create_task(monitor_new_mints(), name="monitor_new_mints")

async def heartbeat_check(batch):
    global last_activity, sol_balance, holding
    while True:
        await asyncio.sleep(22)
        now = datetime.now(timezone.utc)
        if (now - last_activity).total_seconds() > 22 and batch:
            print("Inactivity detected (22s) - liquidating batch")
            for dev in list(batch.keys()):
                try:
                    await sell(batch[dev]["mint"])
                    sol_balance += batch[dev]["bought"]
                    print(f"Balance: {sol_balance:.6f} SOL")
                except Exception as e:
                    print(f"Liquidation failed for {dev}: {e}")
                finally:
                    holding.pop(dev, None)
                    batch.pop(dev)
            await restart_task("subscriptionManager")
        else:
            await asyncio.sleep(22)

async def timerKillSwitch(batch):
    await asyncio.sleep(22)
    for dev, _ in batch:
        monitored_tokens.pop(dev, None)
    await restart_task("monitor_untracked_tokens")

trade_timestamps = {}
peak_tpm = {}
async def track_live_trades(query, batch):
    global holding, sol_balance, last_activity, trade_timestamps, price_history_brief, peak_tpm
    window_seconds = 15
    print(f"Trade tracking active - Batch size: {len(batch)}, Current SOL: {sol_balance:.6f}")
    try:
        async for result in primary_socket.subscribe(query):
            last_activity = datetime.now(timezone.utc)
            if not result.data:
                continue
            for dev, trades in result.data.items():
                if dev[1:] not in batch:
                    continue
                dex_trades = trades.get("DEXTrades", [])
                if not dex_trades:
                    continue
                trade = dex_trades[0]
                current_price = trade["Trade"]["Buy"]["Price"]
                if price_history_brief.get(dev[1:]) is None:
                    price_history_brief[dev[1:]] = []
                price_history_brief[dev[1:]].append(current_price)
                if len(price_history_brief[dev[1:]]) > 3:
                    price_history_brief[dev[1:]].pop(0)
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
                    if trades_per_min > 7 and current_price < pricecap and current_price > batch[dev[1:]]["firstprice"] * 0.8 and (price_history_brief[dev[1:]][-1] > price_history_brief[dev[1:]][-2] or price_history_brief[dev[1:]][-2] > price_history_brief[dev[1:]][-3]):
                        entry_price = current_price
                        bought = ((-666 / (batch[dev[1:]]["firstbuy"] + 666) + 1) * sol_balance) / len(batch)
                        batch[dev[1:]]["bought"] = bought
                        batch[dev[1:]]["entry_price"] = entry_price
                        if sol_balance < bought:
                            print(f"Insufficient funds ({sol_balance:.6f} < {bought:.6f})")
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
                            print("Entry buy failed")
                            batch.pop(dev[1:])
                            holding.pop(dev[1:], None)
                            continue
                        sol_balance -= bought
                        print(f"Bought {batch[dev[1:]]['mint']} at {entry_price:.10f} SOL - Balance: {sol_balance:.6f} SOL")
                    else:
                        if elapsed > 22:
                            print(f"Entry timeout - TPM {trades_per_min:.2f} insufficient")
                            batch.pop(dev[1:])
                            holding.pop(dev[1:], None)
                else:
                    entry_price = batch[dev[1:]]["entry_price"]
                    price_ratio = current_price / entry_price
                    print(f"Stats - TPM: {trades_per_min:.2f}, ROI: {price_ratio:.2f}x, Price: {current_price:.10f}")
                    peak_tpm[dev[1:]] = max(peak_tpm.get(dev[1:], trades_per_min), trades_per_min)
                    
                    if (trades_per_min < elapsed and elapsed > 20) or trades_per_min < 0.84 * peak_tpm[dev[1:]]:
                        print(f"Momentum slowing - Exit at {current_price:.10f} ({price_ratio:.2f}x)")
                        await sell(batch[dev[1:]]["mint"])
                        sol_balance += batch[dev[1:]]["bought"] * price_ratio
                        batch.pop(dev[1:])
                        holding.pop(dev[1:], None)
                    elif current_price > pricecap:
                        print(f"Price ceiling hit - Exit at {current_price:.10f} ({price_ratio:.2f}x)")
                        await sell(batch[dev[1:]]["mint"])
                        sol_balance += batch[dev[1:]]["bought"] * price_ratio
                        batch.pop(dev[1:])
                        holding.pop(dev[1:], None)
                    elif price_ratio <= 1.1 and elapsed > 20:
                        print(f"Trade duration limit reached - Exit at {current_price:.10f}")
                        await sell(batch[dev[1:]]["mint"])
                        sol_balance += batch[dev[1:]]["bought"] * price_ratio
                        batch.pop(dev[1:])
                        holding.pop(dev[1:], None)
                    elif price_ratio <= stoploss:
                        print(f"Stop Loss triggered - Exit at {current_price:.10f}")
                        await sell(batch[dev[1:]]["mint"])
                        sol_balance += batch[dev[1:]]["bought"] * price_ratio
                        batch.pop(dev[1:])
                        holding.pop(dev[1:], None)
                    elif price_ratio >= take_profit:
                        print(f"Take Profit triggered - Exit at {current_price:.10f}")
                        await sell(batch[dev[1:]]["mint"])
                        sol_balance += batch[dev[1:]]["bought"] * price_ratio
                        batch.pop(dev[1:])
                        holding.pop(dev[1:], None)

            if not batch:
                print("Batch processing complete.")
                break 
    except Exception as e:
        print(f"Stream error: {e}")

    if batch:
        print(f"Subscription finished with {len(batch)} remaining items.")

async def subscriptionTask(query):
    global holding
    if len(holding) == 0:
        try:
            print("Monitoring for target token activity...")
            async for result in secondary_socket.subscribe(query):
                if not result.data:
                    continue
                for dev, trades in result.data.items():
                    if dev[1:] in monitored_tokens:
                        dex_trades = trades.get("DEXTrades", [])
                        if dex_trades:
                            first_trade = dex_trades[0]
                            if rugcheck(first_trade["Trade"]["Buy"]["Currency"]["MintAddress"]):
                                coin = {
                                    "mint": first_trade["Trade"]["Buy"]["Currency"]["MintAddress"],
                                    "amount": first_trade["Trade"]["Buy"]["Amount"],
                                    "price": first_trade["Trade"]["Buy"]["Price"],
                                    "firstbuy": monitored_tokens[dev[1:]][3],
                                    "firstprice": monitored_tokens[dev[1:]][2],
                                }
                                holding[dev[1:]] = coin
                                print(f"TARGET ACQUIRED: {coin['mint']} | Initial Price: {coin['firstprice']}")
                            monitored_tokens.pop(dev[1:], None)
        except Exception as e:
            print(f"Subscription task error: {e}")
    else:
        await asyncio.sleep(11)

async def monitor_untracked_tokens():
    global devQuery, currentTask, holding
    while True:
        if len(monitored_tokens) != 0 and len(holding) == 0:
            devQuery = ""
            x = monitored_tokens.copy().items()
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
                    monitored_tokens.pop(dev, None)
            if len(devQuery) != 0:
                query = gql(f"""
                    subscription {{
                        {devQuery}
                    }}
                """)
                await asyncio.wait_for(subscriptionTask(query), timeout=22)
                print("Monitoring cycle finished.")
        else:
            await asyncio.sleep(11)

async def subscriptionManager():
    global trade_timestamps, sol_balance, holding, price_history_brief
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
                active_query = gql(f"""
                    subscription {{
                        {snipe}
                    }}
                """)
                is_processing = True
                trade_timestamps = {}
                price_history_brief = {}
                initialtime = datetime.now()
                while is_processing:
                    await asyncio.wait_for(track_live_trades(active_query, batch), timeout=66)
                    is_processing = False
                    for key in batch.keys():
                        if key in holding:
                            is_processing = True
                        else:
                            batch.pop(key)
                    if (datetime.now() - initialtime).total_seconds() > 22:
                        print("Performance threshold met; resetting batch for safety.")
                        for dev in list(batch.keys()):
                            try:
                                await sell(batch[dev]["mint"])
                                sol_balance += batch[dev]["bought"]
                            except Exception as e:
                                print(f"Reset liquidation failed: {e}")
                            holding.pop(dev, None)
                            is_processing = False
        else:
            await asyncio.sleep(1)

def rugcheck(token):
    try:
        check = requests.get(f"https://api.rugcheck.xyz/v1/tokens/{token}/report/summary")
        if check.json().get("score_normalised") == 1:
            return True
        else:
            print(f"Rug check failed: Token flagged as high risk.")
            return False
    except:
        return False

async def main():
    global primary_socket, secondary_socket
    if primary_socket.websocket is None or primary_socket.websocket.closed:
        await primary_socket.connect()
    if secondary_socket.websocket is None or secondary_socket.websocket.closed:
        await secondary_socket.connect()
    
    tasks["subscriptionManager"] = asyncio.create_task(subscriptionManager(), name="subscriptionManager")
    tasks["monitor_new_mints"] = asyncio.create_task(monitor_new_mints(), name="monitor_new_mints")
    tasks["monitor_untracked_tokens"] = asyncio.create_task(monitor_untracked_tokens(), name="monitor_untracked_tokens")
    await asyncio.Future()

asyncio.run(main())