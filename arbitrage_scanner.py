#!/usr/bin/env python3
"""
Arbitrage Scanner for Kalshi, Polymarket, and OPINION
Finds profitable trading opportunities across prediction markets
"""

import asyncio
import aiohttp
import time
from typing import Dict, List
from dataclasses import dataclass
from datetime import datetime
from collections import defaultdict
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MIN_SPREAD_PCT = 2.5
POLL_INTERVAL = 5

@dataclass
class MarketPrice:
    platform: str
    event_id: str
    event_name: str
    outcome: str
    bid: float
    ask: float
    timestamp: float
    
    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2 if self.bid and self.ask else 0.5

class KalshiCollector:
    def __init__(self):
        self.base_url = "https://trading-api.kalshi.com/v1"
    
    async def fetch_markets(self) -> List[MarketPrice]:
        prices = []
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.base_url}/markets",
                    params={"status": "active", "limit": 30},
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        markets = data.get("markets", [])
                        for market in markets:
                            market_id = market.get("market_id", "")
                            title = market.get("title", "")
                            try:
                                async with session.get(
                                    f"{self.base_url}/orderbooks/{market_id}",
                                    timeout=aiohttp.ClientTimeout(total=5)
                                ) as ob_resp:
                                    if ob_resp.status == 200:
                                        ob = await ob_resp.json()
                                        prices.append(MarketPrice(
                                            platform="Kalshi",
                                            event_id=market_id,
                                            event_name=title,
                                            outcome="YES",
                                            bid=ob.get("yes_bid", 0.5),
                                            ask=ob.get("yes_ask", 0.5),
                                            timestamp=time.time()
                                        ))
                            except:
                                pass
        except Exception as e:
            logger.error(f"Kalshi error: {e}")
        
        logger.info(f"Kalshi: {len(prices)} prices")
        return prices

class PolymarketCollector:
    def __init__(self):
        self.api_url = "https://clob.polymarket.com"
    
    async def fetch_markets(self) -> List[MarketPrice]:
        prices = []
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.api_url}/markets",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        markets = data.get("markets", [])[:30]
                        for market in markets:
                            market_id = market.get("condition_id", "")
                            title = market.get("question", "")
                            try:
                                async with session.get(
                                    f"{self.api_url}/orderbooks/{market_id}",
                                    timeout=aiohttp.ClientTimeout(total=5)
                                ) as ob_resp:
                                    if ob_resp.status == 200:
                                        ob = await ob_resp.json()
                                        bids = ob.get("bids", [])
                                        asks = ob.get("asks", [])
                                        if bids and asks:
                                            prices.append(MarketPrice(
                                                platform="Polymarket",
                                                event_id=market_id,
                                                event_name=title,
                                                outcome="YES",
                                                bid=float(bids[0][0]),
                                                ask=float(asks[0][0]),
                                                timestamp=time.time()
                                            ))
                            except:
                                pass
        except Exception as e:
            logger.error(f"Polymarket error: {e}")
        
        logger.info(f"Polymarket: {len(prices)} prices")
        return prices

class ArbitrageEngine:
    def __init__(self, min_spread: float = 2.5):
        self.min_spread = min_spread
        self.fee_pct = 1.0
    
    def process_prices(self, prices: List[MarketPrice]) -> List[Dict]:
        opportunities = []
        grouped = defaultdict(list)
        
        for price in prices:
            key = f"{price.event_name.lower()}|{price.outcome}"
            grouped[key].append(price)
        
        for event_key, event_prices in grouped.items():
            if len(event_prices) < 2:
                continue
            
            platforms = {p.platform: p for p in event_prices}
            platform_list = list(platforms.items())
            
            for i in range(len(platform_list)):
                for j in range(i + 1, len(platform_list)):
                    p1_name, p1 = platform_list[i]
                    p2_name, p2 = platform_list[j]
                    
                    price1 = p1.mid
                    price2 = p2.mid
                    
                    if price1 > price2:
                        price1, price2 = price2, price1
                        p1_name, p2_name = p2_name, p1_name
                    
                    spread = ((price2 - price1) / price1) * 100
                    net_spread = spread - (2 * self.fee_pct)
                    
                    if net_spread >= self.min_spread:
                        opportunities.append({
                            'event': event_key,
                            'buy_platform': p1_name,
                            'buy_price': price1,
                            'sell_platform': p2_name,
                            'sell_price': price2,
                            'net_spread': net_spread
                        })
        
        return opportunities

async def main():
    print("\n" + "="*60)
    print("ARBITRAGE SCANNER")
    print("="*60 + "\n")
    
    collectors = [
        KalshiCollector(),
        PolymarketCollector()
    ]
    
    engine = ArbitrageEngine(min_spread=MIN_SPREAD_PCT)
    iteration = 0
    
    while True:
        try:
            iteration += 1
            print(f"\n[Scan #{iteration}] {datetime.now().strftime('%H:%M:%S')}")
            
            all_prices = []
            for collector in collectors:
                prices = await collector.fetch_markets()
                all_prices.extend(prices)
            
            opportunities = engine.process_prices(all_prices)
            
            if opportunities:
                print(f"\n FOUND {len(opportunities)} opportunities:\n")
                for opp in opportunities:
                    print(f"  {opp['event']}")
                    print(f"    BUY {opp['buy_platform']:12} @ {opp['buy_price']:.4f}")
                    print(f"    SELL {opp['sell_platform']:11} @ {opp['sell_price']:.4f}")
                    print(f"    SPREAD: {opp['net_spread']:.2f}%\n")
            else:
                print("  No opportunities found")
            
            await asyncio.sleep(POLL_INTERVAL)
        
        except Exception as e:
            logger.error(f"Error: {e}")
            await asyncio.sleep(10)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nScanner stopped")
        exit(0)
