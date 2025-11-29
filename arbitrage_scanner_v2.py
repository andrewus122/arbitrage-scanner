#!/usr/bin/env python3
"""
Arbitrage Scanner для Polymarket та OPINION
Selenium для OPINION, aiohttp для Polymarket
"""

import asyncio
import aiohttp
import time
from typing import Dict, List
from dataclasses import dataclass
from datetime import datetime
from collections import defaultdict
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MIN_SPREAD_PCT = 2.5
POLL_INTERVAL = 10
