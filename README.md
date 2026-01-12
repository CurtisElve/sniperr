# Solana Token Sniper

A Python-based trading bot designed to monitor the Solana blockchain for new token launches, filter for high-liquidity events, and execute trades based on real-time momentum.

> **Disclaimer:** This is an educational project built to explore asynchronous programming and blockchain streaming. It does not guarantee profit.

## Purpose

The bot automates the process of finding and "sniping" new tokens launched via the Pump.fun bonding curve. It aims to identify tokens with high initial developer investment and exit positions based on trade volume and price action.

## Tech Stack

- **Python:** The core language.
    
- **Asyncio & Aiohttp:** Used for non-blocking concurrent tasks and API requests.
    
- **GraphQL (GQL):** Handles real-time blockchain subscriptions via Bitquery's streaming API.
    
- **WebSockets:** Maintained for low-latency data feeds.
    
- **APIs:** Bitquery (Data), PumpPortal (Execution), and RugCheck (Security).
    

## Core Logic

The bot operates in a continuous loop through three main asynchronous tasks:

1. **Scanning:** It monitors the blockchain for `create` instructions on the Pump.fun program. When a new token is found, it checks the developer's initial buy-in.
    
2. **Filtering:** If a token meets a specific liquidity threshold (e.g., > 333 USD initial buy), it is added to a monitoring queue. It also runs a security check via RugCheck to filter out high-risk contracts.
    
3. **Execution & Tracking:**
    
    - **Buy:** Triggered when Trades Per Minute (TPM) hits a specific momentum threshold.
        
    - **Sell:** The bot uses a dynamic exit strategy based on Take Profit (TP), Stop Loss (SL), or "momentum cooling" (if trade volume drops relative to the time elapsed).
        

## Key Technical Features

- **Concurrency:** Uses `asyncio.create_task` to manage multiple streams (monitoring new mints vs. tracking active positions) simultaneously without bottlenecking.
    
- **Error Handling:** Implements exponential backoff for API requests and heartbeat checks to ensure the bot doesn't hang on dead connections.
    
- **Dynamic Scaling:** The buy amount is calculated relative to the current wallet balance and the number of active trades in a batch.
    

## Potential Improvements

- **Enhanced Security:** Adding more filters for developer history or social media presence.
    
- **Database Integration:** Logging all trades to a local SQL database to analyze long-term performance and refine the TP/SL math.
    
- **Multi-DEX Support:** Expanding beyond Pump.fun to monitor Raydium or Meteora migrations.