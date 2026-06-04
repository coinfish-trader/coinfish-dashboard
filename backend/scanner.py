import re
import json
import time
import threading
import numpy as np
import pandas as pd
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
import yfinance as yf

try:
    from curl_cffi import requests as curl_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False

WATCHLIST = [
    "NVDA", "TSLA", "AAPL", "AMD", "AMZN", "GOOGL", "NFLX", "MSFT", "ORCL", "META",
    "BAC", "WFC", "C", "JPM", "MS", "SCHW", "COF", "AXP", "GS",
    "XOM", "SLB", "CVX", "OXY", "COP", "EOG", "VLO", "MPC",
    "PFE", "MRK", "JNJ", "BMY", "ABBV", "LLY", "TMO", "AMGN",
    "WMT", "NKE", "DIS", "SBUX", "HD", "TGT", "LOW", "COST", "MCD",
    "HAL", "HON", "BA", "MMM", "RTX", "UPS", "GE", "CAT", "DE", "UNP", "LMT",
]

COMPANY_NAMES = {
    "NVDA": "NVIDIA Corp", "TSLA": "Tesla Inc", "AAPL": "Apple Inc",
    "AMD": "Advanced Micro Devices", "AMZN": "Amazon.com", "GOOGL": "Alphabet Inc",
    "NFLX": "Netflix Inc", "MSFT": "Microsoft Corp", "ORCL": "Oracle Corp",
    "META": "Meta Platforms", "BAC": "Bank of America", "WFC": "Wells Fargo",
    "C": "Citigroup Inc", "JPM": "JPMorgan Chase", "MS": "Morgan Stanley",
    "SCHW": "Charles Schwab", "COF": "Capital One", "AXP": "American Express",
    "GS": "Goldman Sachs", "XOM": "ExxonMobil", "SLB": "SLB (Schlumberger)",
    "CVX": "Chevron Corp", "OXY": "Occidental
