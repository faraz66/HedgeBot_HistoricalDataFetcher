import ccxt
import pandas as pd
from datetime import datetime, timedelta
import time
from typing import List, Dict
import logging
import os
from tqdm import tqdm
import sys
from colorama import init, Fore, Style
import concurrent.futures
import gzip
import json
import pickle
from functools import lru_cache, wraps
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
from pathlib import Path
import queue
import psutil
import statistics
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from collections import defaultdict

# Initialize colorama for cross-platform colored output
init()

# Configure logging with more detailed format and colors
class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors"""
    def format(self, record):
        if record.levelname == 'INFO':
            color = Fore.GREEN
        elif record.levelname == 'ERROR':
            color = Fore.RED
        elif record.levelname == 'WARNING':
            color = Fore.YELLOW
        else:
            color = Fore.WHITE
        record.msg = f"{color}{record.msg}{Style.RESET_ALL}"
        return super().format(record)

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(ColoredFormatter('%(message)s'))
logger.addHandler(handler)

@dataclass
class OperationMetrics:
    """Stores metrics for a specific operation"""
    durations: List[float] = field(default_factory=list)
    success_count: int = 0
    error_count: int = 0
    total_data_size: int = 0
    memory_usage: List[float] = field(default_factory=list)
    thread_count: List[int] = field(default_factory=list)

class PerformanceMonitor:
    """Monitors and analyzes performance metrics"""
    def __init__(self):
        self.metrics = defaultdict(OperationMetrics)
        self.lock = threading.Lock()
        self.start_time = time.time()
        
    def record_metric(self, operation: str, duration: float, success: bool = True, 
                     data_size: int = 0, memory_used: float = 0, threads: int = 0):
        """Record metrics for an operation"""
        with self.lock:
            metrics = self.metrics[operation]
            metrics.durations.append(duration)
            if success:
                metrics.success_count += 1
            else:
                metrics.error_count += 1
            metrics.total_data_size += data_size
            if memory_used:
                metrics.memory_usage.append(memory_used)
            if threads:
                metrics.thread_count.append(threads)
    
    def get_summary(self) -> Dict[str, Dict[str, Any]]:
        """Get a summary of all recorded metrics"""
        summary = {}
        total_time = time.time() - self.start_time
        
        with self.lock:
            for op, metrics in self.metrics.items():
                if not metrics.durations:
                    continue
                
                summary[op] = {
                    'avg_duration': statistics.mean(metrics.durations),
                    'min_duration': min(metrics.durations),
                    'max_duration': max(metrics.durations),
                    'success_rate': (metrics.success_count / (metrics.success_count + metrics.error_count)) * 100,
                    'total_operations': metrics.success_count + metrics.error_count,
                    'total_data_processed': metrics.total_data_size,
                    'avg_memory_usage': statistics.mean(metrics.memory_usage) if metrics.memory_usage else 0,
                    'avg_thread_count': statistics.mean(metrics.thread_count) if metrics.thread_count else 0
                }
                
                # Calculate throughput
                if metrics.total_data_size > 0:
                    summary[op]['throughput'] = metrics.total_data_size / total_time
        
        return summary
    
    def print_summary(self):
        """Print a formatted summary of performance metrics"""
        summary = self.get_summary()
        
        print(f"\n{Fore.CYAN}{'='*80}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}📊 Performance Analysis{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'='*80}{Style.RESET_ALL}\n")
        
        for op, metrics in summary.items():
            print(f"{Fore.YELLOW}Operation: {op}{Style.RESET_ALL}")
            print(f"  Average Duration: {metrics['avg_duration']:.3f}s")
            print(f"  Min/Max Duration: {metrics['min_duration']:.3f}s / {metrics['max_duration']:.3f}s")
            print(f"  Success Rate: {metrics['success_rate']:.1f}%")
            print(f"  Total Operations: {metrics['total_operations']}")
            
            if metrics['total_data_processed'] > 0:
                print(f"  Data Processed: {metrics['total_data_processed']:,} bytes")
                print(f"  Throughput: {metrics['throughput']:.2f} bytes/sec")
            
            if metrics['avg_memory_usage'] > 0:
                print(f"  Avg Memory Usage: {metrics['avg_memory_usage'] / 1024 / 1024:.1f} MB")
            
            if metrics['avg_thread_count'] > 0:
                print(f"  Avg Thread Count: {metrics['avg_thread_count']:.1f}")
            
            print()

def monitor_performance(operation: str):
    """Decorator to monitor performance of operations"""
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            start_time = time.time()
            start_memory = psutil.Process().memory_info().rss
            thread_count = threading.active_count()
            
            try:
                result = func(self, *args, **kwargs)
                success = True
            except Exception as e:
                success = False
                raise e
            finally:
                duration = time.time() - start_time
                memory_used = psutil.Process().memory_info().rss - start_memory
                
                # Get data size if result is DataFrame
                data_size = 0
                if isinstance(result, pd.DataFrame):
                    data_size = result.memory_usage(deep=True).sum()
                
                self.performance_monitor.record_metric(
                    operation=operation,
                    duration=duration,
                    success=success,
                    data_size=data_size,
                    memory_used=memory_used,
                    threads=thread_count
                )
            
            return result
        return wrapper
    return decorator

class CacheManager:
    """Manages caching of OHLCV data with optimized compression"""
    def __init__(self, cache_dir: str = "cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.cache_lock = threading.Lock()
        self.memory_cache = {}  # In-memory cache for frequently accessed data
        self.cache_hits = 0
        self.cache_misses = 0
        
    def _get_cache_key(self, symbol: str, timeframe: str, start_date: datetime, end_date: datetime) -> str:
        """Generate a unique cache key based on request parameters with optimized hashing"""
        key_string = f"{symbol}_{timeframe}_{start_date.isoformat()}_{end_date.isoformat()}"
        return hashlib.blake2b(key_string.encode(), digest_size=16).hexdigest()
    
    def get_cached_data(self, symbol: str, timeframe: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Retrieve cached data with memory and disk caching"""
        cache_key = self._get_cache_key(symbol, timeframe, start_date, end_date)
        
        # Check memory cache first
        if cache_key in self.memory_cache:
            self.cache_hits += 1
            return self.memory_cache[cache_key]
        
        cache_file = self.cache_dir / f"{cache_key}.gz"
        
        if cache_file.exists():
            with self.cache_lock:
                try:
                    with gzip.open(cache_file, 'rb') as f:
                        data = pickle.load(f)
                    df = pd.DataFrame(data)
                    
                    # Store in memory cache if size is reasonable (< 100MB)
                    if df.memory_usage(deep=True).sum() < 100 * 1024 * 1024:
                        self.memory_cache[cache_key] = df
                    
                    self.cache_hits += 1
                    return df
                except Exception as e:
                    logger.warning(f"Cache read error: {e}")
        
        self.cache_misses += 1
        return pd.DataFrame()
    
    def cache_data(self, df: pd.DataFrame, symbol: str, timeframe: str, start_date: datetime, end_date: datetime):
        """Cache data with optimized compression"""
        if df.empty:
            return
            
        cache_key = self._get_cache_key(symbol, timeframe, start_date, end_date)
        cache_file = self.cache_dir / f"{cache_key}.gz"
        
        with self.cache_lock:
            try:
                # Optimize DataFrame for storage
                df_optimized = df.copy()
                for col in df_optimized.select_dtypes(include=['float64']).columns:
                    df_optimized[col] = df_optimized[col].astype('float32')
                for col in df_optimized.select_dtypes(include=['int64']).columns:
                    df_optimized[col] = df_optimized[col].astype('int32')
                
                # Use higher compression for better storage efficiency
                with gzip.open(cache_file, 'wb', compresslevel=6) as f:
                    pickle.dump(df_optimized.to_dict('records'), f, protocol=pickle.HIGHEST_PROTOCOL)
                
                # Store in memory cache if size is reasonable
                if df_optimized.memory_usage(deep=True).sum() < 100 * 1024 * 1024:
                    self.memory_cache[cache_key] = df_optimized
                
            except Exception as e:
                logger.warning(f"Cache write error: {e}")
    
    def clear_cache(self):
        """Clear all cached data"""
        with self.cache_lock:
            # Clear memory cache
            self.memory_cache.clear()
            
            # Clear disk cache
            for cache_file in self.cache_dir.glob("*.gz"):
                try:
                    cache_file.unlink()
                except Exception as e:
                    logger.warning(f"Cache delete error: {e}")
    
    def get_cache_stats(self) -> Dict:
        """Get cache performance statistics"""
        total_requests = self.cache_hits + self.cache_misses
        hit_rate = (self.cache_hits / total_requests * 100) if total_requests > 0 else 0
        
        return {
            'cache_hits': self.cache_hits,
            'cache_misses': self.cache_misses,
            'hit_rate': hit_rate,
            'memory_cache_size': len(self.memory_cache),
            'disk_cache_files': len(list(self.cache_dir.glob("*.gz")))
        }

class ThreadManager:
    """Manages thread pools and resource allocation"""
    def __init__(self):
        self.cpu_count = os.cpu_count() or 1
        self.memory = psutil.virtual_memory()
        self.io_bound_workers = min(32, self.cpu_count * 4)  # More threads for I/O
        self.cpu_bound_workers = self.cpu_count  # One thread per CPU core for CPU tasks
        self.memory_per_thread = (self.memory.available * 0.8) // self.io_bound_workers  # 80% of available memory
        
        # Thread pools for different types of operations
        self.io_pool = ThreadPoolExecutor(max_workers=self.io_bound_workers, thread_name_prefix="IO")
        self.cpu_pool = ThreadPoolExecutor(max_workers=self.cpu_bound_workers, thread_name_prefix="CPU")
        self.network_pool = ThreadPoolExecutor(max_workers=self.io_bound_workers, thread_name_prefix="NET")
        
        # Task queues
        self.io_queue = queue.Queue()
        self.cpu_queue = queue.Queue()
        self.network_queue = queue.Queue()
        
        # Locks
        self.io_lock = threading.Lock()
        self.cpu_lock = threading.Lock()
        self.network_lock = threading.Lock()
        
        # Start worker threads
        self._start_workers()
    
    def _start_workers(self):
        """Start background worker threads"""
        threading.Thread(target=self._io_worker, daemon=True).start()
        threading.Thread(target=self._cpu_worker, daemon=True).start()
        threading.Thread(target=self._network_worker, daemon=True).start()
    
    def _io_worker(self):
        """Process I/O tasks"""
        while True:
            try:
                task, args, kwargs = self.io_queue.get()
                self.io_pool.submit(task, *args, **kwargs)
                self.io_queue.task_done()
            except Exception as e:
                logger.error(f"IO worker error: {e}")
    
    def _cpu_worker(self):
        """Process CPU-intensive tasks"""
        while True:
            try:
                task, args, kwargs = self.cpu_queue.get()
                self.cpu_pool.submit(task, *args, **kwargs)
                self.cpu_queue.task_done()
            except Exception as e:
                logger.error(f"CPU worker error: {e}")
    
    def _network_worker(self):
        """Process network tasks"""
        while True:
            try:
                task, args, kwargs = self.network_queue.get()
                self.network_pool.submit(task, *args, **kwargs)
                self.network_queue.task_done()
            except Exception as e:
                logger.error(f"Network worker error: {e}")
    
    def submit_io_task(self, task, *args, **kwargs):
        """Submit an I/O task"""
        self.io_queue.put((task, args, kwargs))
    
    def submit_cpu_task(self, task, *args, **kwargs):
        """Submit a CPU-intensive task"""
        self.cpu_queue.put((task, args, kwargs))
    
    def submit_network_task(self, task, *args, **kwargs):
        """Submit a network task"""
        self.network_queue.put((task, args, kwargs))
    
    def wait_completion(self):
        """Wait for all tasks to complete"""
        self.io_queue.join()
        self.cpu_queue.join()
        self.network_queue.join()
    
    def shutdown(self):
        """Shutdown thread pools"""
        self.io_pool.shutdown(wait=True)
        self.cpu_pool.shutdown(wait=True)
        self.network_pool.shutdown(wait=True)

class CandleCounter:
    def __init__(self, exchange_id: str = 'binance', **kwargs):
        """Initialize the CandleCounter with performance optimization settings"""
        self.exchange_id = exchange_id
        self.exchange = getattr(ccxt, exchange_id)()
        
        # Define symbol mappings
        self.crypto_symbols = {
            'BTC': '₿',   # Bitcoin
            'ETH': 'Ξ',   # Ethereum
            'BNB': 'BNB', # Binance Coin
            'SOL': 'SOL', # Solana
            'XRP': 'XRP', # Ripple
            'ADA': 'ADA', # Cardano
            'AVAX': 'AVAX', # Avalanche
            'DOGE': 'Ð',  # Dogecoin
            'DOT': 'DOT', # Polkadot
            'MATIC': 'MATIC', # Polygon
            'LINK': 'LINK', # Chainlink
            'UNI': 'UNI',  # Uniswap
            'ATOM': 'ATOM', # Cosmos
            'LTC': 'Ł',    # Litecoin
            'ETC': 'ETC',  # Ethereum Classic
        }
        
        self.forex_symbols = {
            'EUR': '€',    # Euro
            'GBP': '£',    # British Pound
            'JPY': '¥',    # Japanese Yen
            'AUD': 'A$',   # Australian Dollar
            'CAD': 'C$',   # Canadian Dollar
            'CHF': 'Fr',   # Swiss Franc
            'NZD': 'NZ$',  # New Zealand Dollar
            'USDT': '$',   # Tether (USD)
            'USDC': '$',   # USD Coin
            'BUSD': '$',   # Binance USD
        }
        
        # Initialize timeframes with seconds
        self.timeframes = {
            '1m': 60,        # 1 minute = 60 seconds
            '3m': 180,       # 3 minutes = 180 seconds
            '5m': 300,       # 5 minutes = 300 seconds
            '15m': 900,      # 15 minutes = 900 seconds
            '30m': 1800,     # 30 minutes = 1800 seconds
            '1h': 3600,      # 1 hour = 3600 seconds
            '2h': 7200,      # 2 hours = 7200 seconds
            '4h': 14400,     # 4 hours = 14400 seconds
            '6h': 21600,     # 6 hours = 21600 seconds
            '8h': 28800,     # 8 hours = 28800 seconds
            '12h': 43200,    # 12 hours = 43200 seconds
            '1d': 86400,     # 1 day = 86400 seconds
            '3d': 259200,    # 3 days = 259200 seconds
            '1w': 604800,    # 1 week = 604800 seconds
            '1M': 2592000,   # 1 month (30 days) = 2592000 seconds
        }
        
        # Initialize valid Binance intervals
        self.valid_intervals = [
            '1m', '3m', '5m', '15m', '30m',  # minutes
            '1h', '2h', '4h', '6h', '8h', '12h',  # hours
            '1d', '3d',  # days
            '1w',  # weeks
            '1M'   # months
        ]
        
        # Initialize custom timeframe support with valid intervals only
        self.supported_units = {
            'm': ['1m', '3m', '5m', '15m', '30m'],
            'h': ['1h', '2h', '4h', '6h', '8h', '12h'],
            'd': ['1d', '3d'],
            'w': ['1w'],
            'M': ['1M']
        }
        
        # Initialize rate limiting with token bucket algorithm
        try:
            # Get exchange info
            exchange_info = self.exchange.load_markets()
            
            # Initialize rate limits - Binance specific
            self.rate_limit = 1200  # Base rate limit in milliseconds for Binance
            self.weight_limit = 1200  # Weight limit per minute for Binance
            
            # Token bucket parameters
            self.tokens = float(self.weight_limit)  # Current number of tokens
            self.last_update = time.time()  # Last token update time
            self.token_rate = float(self.weight_limit) / 60.0  # Token replenishment rate per second
            
            logger.info(f"Initialized rate limiting - Base limit: {self.rate_limit}ms, Weight limit: {self.weight_limit}/min")
        except Exception as e:
            logger.warning(f"Could not get exchange rate limit: {e}. Using default values.")
            self.rate_limit = 1200  # Default to 1200ms
            self.weight_limit = 1200
            self.tokens = 1200.0
            self.last_update = time.time()
            self.token_rate = 20.0  # Conservative default
        
        self.rate_limit_lock = threading.Lock()  # Lock for thread-safe rate limiting
        self.request_count = 0
        self.last_request_time = time.time()
        
        # Initialize performance monitor
        self.performance_monitor = PerformanceMonitor()
        
        # Initialize cache and thread management
        self.cache_manager = CacheManager()
        self.thread_manager = ThreadManager()
        
        # Performance settings
        self.batch_size = kwargs.get('batch_size', 30)  # Default to 30 4-hour periods (5 days)
        self.compression_threshold = kwargs.get('compression_threshold', 1000000)
        self.memory_buffer = kwargs.get('memory_buffer', 0.8)  # 80% memory threshold
        self.chunk_size = kwargs.get('chunk_size', 500)
        
        logger.info(f"Initialized {exchange_id} CandleCounter with optimized settings")

    def _update_tokens(self):
        """Update available tokens based on elapsed time"""
        current_time = time.time()
        elapsed = current_time - self.last_update
        self.tokens = min(
            self.weight_limit,
            self.tokens + elapsed * self.token_rate
        )
        self.last_update = current_time

    def _handle_rate_limit(self, weight: int = 1):
        """Handle rate limiting for API requests with token bucket algorithm
        
        Args:
            weight: The weight of the current request (default: 1)
        """
        with self.rate_limit_lock:
            try:
                # Update available tokens
                self._update_tokens()
                
                # Calculate required wait time if needed
                if self.tokens < weight:
                    required_tokens = weight - self.tokens
                    wait_time = required_tokens / self.token_rate
                    logger.debug(f"Rate limiting: sleeping for {wait_time:.2f} seconds")
                    time.sleep(max(0, wait_time))
                    self._update_tokens()  # Update tokens after waiting
                
                # Consume tokens
                self.tokens -= weight
                
                # Apply base rate limit
                current_time = time.time()
                time_since_last = current_time - self.last_request_time
                base_wait = (self.rate_limit / 1000.0) - time_since_last
                
                if base_wait > 0:
                    time.sleep(base_wait)
                
                # Update last request time
                self.last_request_time = time.time()
                self.request_count += 1
                
            except Exception as e:
                logger.error(f"Error in rate limiting: {e}")
                # Use a more conservative wait time if there's an error
                time.sleep(2.0)
                
    def _handle_api_error(self, e: Exception, method: str, params: dict) -> None:
        """Handle API errors with appropriate backoff and logging
        
        Args:
            e: The exception that occurred
            method: The API method that was called
            params: The parameters used in the API call
        """
        error_msg = str(e)
        
        if 'TOO_MANY_REQUESTS' in error_msg:
            logger.warning(f"Rate limit exceeded: {error_msg}")
            # Use exponential backoff
            time.sleep(min(60, 2 ** (self.request_count % 6)))  # Max 60 second wait
            
        elif 'INVALID_SYMBOL' in error_msg:
            logger.error(f"Invalid symbol in request: {params.get('symbol')}")
            raise ValueError(f"Invalid symbol: {params.get('symbol')}")
            
        elif 'Invalid API-key' in error_msg:
            logger.error("Invalid API key or authentication error")
            raise ValueError("Authentication error - check your API credentials")
            
        else:
            logger.error(f"API error in {method}: {error_msg}")
            logger.error(f"Request parameters: {params}")
            # Default backoff
            time.sleep(5.0)

    @lru_cache(maxsize=128)
    def _get_symbol_info(self, symbol: str) -> Dict:
        """Cache symbol information to reduce API calls"""
        return self.exchange.load_markets()[symbol]

    def get_available_symbols(self) -> List[str]:
        """Get popular trading pairs including major cryptocurrencies, gold, and forex"""
        try:
            markets = self.exchange.load_markets()
            
            # Filter for active spot markets that are in our popular pairs list
            popular_pairs = [
                # Top Cryptocurrencies by Market Cap (USD equivalents)
                'BTC/USDT',  # Bitcoin (USD equivalent)
                'ETH/USDT',  # Ethereum (USD equivalent)
                'BNB/USDT',  # Binance Coin (USD equivalent)
                'SOL/USDT',  # Solana (USD equivalent)
                'XRP/USDT',  # Ripple (USD equivalent)
                'ADA/USDT',  # Cardano (USD equivalent)
                'AVAX/USDT', # Avalanche (USD equivalent)
                'DOGE/USDT', # Dogecoin (USD equivalent)
                'DOT/USDT',  # Polkadot (USD equivalent)
                'MATIC/USDT',# Polygon (USD equivalent)
                'LINK/USDT', # Chainlink (USD equivalent)
                'UNI/USDT',  # Uniswap (USD equivalent)
                'ATOM/USDT', # Cosmos (USD equivalent)
                'LTC/USDT',  # Litecoin (USD equivalent)
                'ETC/USDT',  # Ethereum Classic (USD equivalent)
                
                # USD Stablecoins (USD equivalents)
                'BTC/USDC',  # Bitcoin (USD Coin)
                'ETH/USDC',  # Ethereum (USD Coin)
                'BTC/BUSD',  # Bitcoin (Binance USD)
                'ETH/BUSD',  # Ethereum (Binance USD)
                
                # Gold Spot Trading (USD equivalents)
                'PAXG/USDT',  # Gold Spot (XAUUSD equivalent)
                'PAXG/USDC',  # Gold Spot (USD Coin)
                'PAXG/BUSD',  # Gold Spot (Binance USD)
                'PAXG/DAI',   # Gold Spot (DAI - USD equivalent)
                
                # Major Forex Pairs (USD equivalents)
                'EUR/USDT',   # Euro (USD equivalent)
                'GBP/USDT',   # British Pound (USD equivalent)
                'JPY/USDT',   # Japanese Yen (USD equivalent)
                'AUD/USDT',   # Australian Dollar (USD equivalent)
                'CAD/USDT',   # Canadian Dollar (USD equivalent)
                'CHF/USDT',   # Swiss Franc (USD equivalent)
                'NZD/USDT',   # New Zealand Dollar (USD equivalent)
                
                # Cross Currency Pairs (Crosses)
                'EUR/GBP',    # Euro/British Pound
                'EUR/JPY',    # Euro/Japanese Yen
                'GBP/JPY',    # British Pound/Japanese Yen
                'AUD/JPY',    # Australian Dollar/Japanese Yen
                'EUR/AUD',    # Euro/Australian Dollar
                'GBP/AUD',    # British Pound/Australian Dollar
                
                # Crypto Cross Pairs
                'ETH/BTC', 'BNB/BTC', 'XRP/BTC', 'SOL/BTC', 'ADA/BTC',
                'DOGE/BTC', 'DOT/BTC', 'MATIC/BTC', 'LINK/BTC', 'AVAX/BTC'
            ]
            
            symbols = [
                symbol for symbol in popular_pairs 
                if symbol in markets and 
                markets[symbol]['active'] and 
                markets[symbol]['spot']
            ]
            
            return sorted(symbols)
            
        except Exception as e:
            logger.error(f"Error fetching available symbols: {e}")
            return []

    @monitor_performance("verify_data")
    def verify_data(self, symbol: str, timeframe: str, start_date: datetime, end_date: datetime, df: pd.DataFrame) -> bool:
        """Verify the fetched data against Binance's data with strict date range checking"""
        try:
            print(f"\n{Fore.CYAN}Verifying data accuracy...{Style.RESET_ALL}")
            
            # Check date range
            actual_start = df['timestamp'].min()
            actual_end = df['timestamp'].max()
            
            # Calculate the maximum allowed time difference (1 day)
            max_time_diff = timedelta(days=1)
            
            start_diff = abs(actual_start - start_date)
            end_diff = abs(actual_end - end_date)
            
            if actual_start > start_date:
                print(f"\n{Fore.YELLOW}⚠️ Date Range Notice:{Style.RESET_ALL}")
                print(f"Data starts later than requested:")
                print(f"- Requested: {start_date}")
                print(f"- Actual: {actual_start}")
                if start_diff > max_time_diff:
                    return False
            
            if actual_end < end_date:
                print(f"\n{Fore.YELLOW}⚠️ Date Range Notice:{Style.RESET_ALL}")
                print(f"Data ends earlier than requested:")
                print(f"- Requested: {end_date}")
                print(f"- Actual: {actual_end}")
                if end_diff > max_time_diff:
                    return False
            
            # Check time intervals
            time_diffs = df['timestamp'].diff().dropna()
            expected_interval = pd.Timedelta(seconds=self.timeframes[timeframe])
            
            irregular_intervals = time_diffs[time_diffs != expected_interval]
            if not irregular_intervals.empty:
                print(f"{Fore.RED}Warning: Found {len(irregular_intervals)} irregular time intervals{Style.RESET_ALL}")
                # Only fail if more than 5% of intervals are irregular
                if len(irregular_intervals) > len(df) * 0.05:
                    return False
            
            # Verify data continuity
            expected_periods = int((end_date - start_date).total_seconds() / self.timeframes[timeframe]) + 1
            actual_periods = len(df)
            missing_percentage = (expected_periods - actual_periods) / expected_periods * 100
            
            if missing_percentage > 5:  # Allow up to 5% missing candles
                print(f"{Fore.RED}Warning: Significant missing candles{Style.RESET_ALL}")
                print(f"Expected {expected_periods} candles, got {actual_periods}")
                print(f"Missing {missing_percentage:.1f}% of candles")
                return False
            elif missing_percentage > 0:
                print(f"{Fore.YELLOW}Note: Missing {missing_percentage:.1f}% of candles{Style.RESET_ALL}")
                print(f"Expected {expected_periods} candles, got {actual_periods}")
            
            # Verify OHLC price relationships
            invalid_high = df[df['high'] < df[['open', 'close']].max(axis=1)]
            invalid_low = df[df['low'] > df[['open', 'close']].min(axis=1)]
            
            if not invalid_high.empty or not invalid_low.empty:
                print(f"\n{Fore.RED}Warning: Invalid OHLC relationships detected{Style.RESET_ALL}")
                if not invalid_high.empty:
                    print(f"Found {len(invalid_high)} candles where high is not the highest price")
                if not invalid_low.empty:
                    print(f"Found {len(invalid_low)} candles where low is not the lowest price")
                # Only fail if more than 1% of candles have invalid relationships
                if (len(invalid_high) + len(invalid_low)) > len(df) * 0.01:
                    return False
            
            # Check for extreme price movements
            price_changes = df['close'].pct_change().abs()
            extreme_changes = price_changes[price_changes > 0.1]  # 10% change threshold
            if not extreme_changes.empty:
                print(f"\n{Fore.YELLOW}Note: Found {len(extreme_changes)} extreme price movements (>10%){Style.RESET_ALL}")
                print("This is not necessarily an error, but worth noting for analysis.")
            
            print(f"\n{Fore.GREEN}Data verification passed:{Style.RESET_ALL}")
            print(f"✓ Date range is within acceptable limits")
            print(f"✓ Time intervals are consistent")
            print(f"✓ Data continuity is acceptable")
            print(f"✓ OHLC relationships are valid")
            
            return True
            
        except Exception as e:
            logger.error(f"Error verifying data: {e}")
            return False

    @monitor_performance("fetch_chunk")
    def _fetch_chunk(self, symbol: str, timeframe: str, start: datetime, end: datetime) -> pd.DataFrame:
        """Fetch a single chunk of data with optimized processing"""
        try:
            # Ensure symbol format is correct for the API
            formatted_symbol = symbol.replace('/', '')
            
            # Convert timestamps to milliseconds
            start_ms = int(start.timestamp() * 1000)
            end_ms = int(end.timestamp() * 1000)
            
            # Ensure chunk_size is an integer and within limits
            chunk_size = min(int(self.chunk_size), 1000)  # Cap at 1000 to prevent memory issues
            
            # Prepare API parameters
            params = {
                'symbol': formatted_symbol,
                'interval': timeframe,
                'startTime': start_ms,
                'endTime': end_ms,
                'limit': chunk_size
            }
            
            # Debug logging
            logger.debug(f"Fetching chunk with params: {params}")
            
            # Handle rate limiting before API call
            # Klines endpoint weight is 1
            self._handle_rate_limit(weight=1)
            
            try:
                # Make API call
                response = self.exchange.publicGetKlines(params)
            except Exception as e:
                # Handle API-specific errors
                self._handle_api_error(e, 'publicGetKlines', params)
                # Retry once after error handling
                self._handle_rate_limit(weight=1)
                response = self.exchange.publicGetKlines(params)
            
            if not response:
                logger.warning(f"No data returned for chunk: {params}")
                return pd.DataFrame()
            
            # Pre-allocate lists with estimated size for better memory efficiency
            estimated_size = min(len(response), chunk_size)
            timestamps = [None] * estimated_size
            close_times = [None] * estimated_size
            numeric_data = {
                'open': [0.0] * estimated_size,
                'high': [0.0] * estimated_size,
                'low': [0.0] * estimated_size,
                'close': [0.0] * estimated_size,
                'volume': [0.0] * estimated_size,
                'quote_asset_volume': [0.0] * estimated_size,
                'taker_buy_base_asset_volume': [0.0] * estimated_size,
                'taker_buy_quote_asset_volume': [0.0] * estimated_size
            }
            trades = [0] * estimated_size
            
            # Process data with minimal memory allocation
            valid_count = 0
            for candle in response:
                try:
                    # Validate candle data structure
                    if not isinstance(candle, (list, tuple)) or len(candle) < 11:
                        logger.warning(f"Invalid candle data structure: {candle}")
                        continue
                    
                    # Convert timestamps with error checking
                    try:
                        timestamp = float(candle[0]) / 1000
                        close_time = float(candle[6]) / 1000
                        timestamps[valid_count] = pd.Timestamp.fromtimestamp(timestamp)
                        close_times[valid_count] = pd.Timestamp.fromtimestamp(close_time)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Invalid timestamp in candle: {e}")
                        continue
                    
                    # Process numeric columns with explicit type conversion and validation
                    try:
                        numeric_data['open'][valid_count] = float(candle[1])
                        numeric_data['high'][valid_count] = float(candle[2])
                        numeric_data['low'][valid_count] = float(candle[3])
                        numeric_data['close'][valid_count] = float(candle[4])
                        numeric_data['volume'][valid_count] = float(candle[5])
                        numeric_data['quote_asset_volume'][valid_count] = float(candle[7])
                        numeric_data['taker_buy_base_asset_volume'][valid_count] = float(candle[9])
                        numeric_data['taker_buy_quote_asset_volume'][valid_count] = float(candle[10])
                        trades[valid_count] = int(candle[8])
                        valid_count += 1
                    except (IndexError, ValueError, TypeError) as e:
                        logger.warning(f"Error processing numeric data: {e}")
                        logger.warning(f"Problematic candle: {candle}")
                        continue
                    
                except Exception as e:
                    logger.error(f"Unexpected error processing candle: {e}")
                    continue
            
            if valid_count == 0:
                logger.warning("No valid candles processed")
                return pd.DataFrame()
            
            # Trim arrays to actual size
            timestamps = timestamps[:valid_count]
            close_times = close_times[:valid_count]
            for k in numeric_data:
                numeric_data[k] = numeric_data[k][:valid_count]
            trades = trades[:valid_count]
            
            # Create DataFrame efficiently with minimal memory usage
            try:
                df = pd.DataFrame({
                    'timestamp': pd.Series(timestamps, dtype='datetime64[ns]'),
                    'close_time': pd.Series(close_times, dtype='datetime64[ns]'),
                    **{k: pd.Series(v, dtype='float32') for k, v in numeric_data.items()},
                    'number_of_trades': pd.Series(trades, dtype='int32')
                })
                
                # Validate DataFrame
                if df.empty:
                    logger.warning("Created DataFrame is empty")
                    return df
                
                # Check for missing values
                missing_values = df.isnull().sum()
                if missing_values.any():
                    logger.warning(f"Missing values in DataFrame: {missing_values[missing_values > 0]}")
                
                return df
                
            except Exception as e:
                logger.error(f"Error creating DataFrame: {e}")
                return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Error fetching chunk: {e}")
            logger.error(f"Parameters: symbol={symbol}, timeframe={timeframe}, start={start}, end={end}")
            return pd.DataFrame()

    @monitor_performance("fetch_ohlcv")
    def fetch_ohlcv_data(
        self,
        symbol: str,
        timeframe: str,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """Fetch OHLCV data with optimized parallel processing and caching"""
        try:
            # Check cache first with optimized key generation
            cached_data = self.cache_manager.get_cached_data(symbol, timeframe, start_date, end_date)
            if not cached_data.empty:
                logger.info(f"{Fore.GREEN}✓ Retrieved data from cache{Style.RESET_ALL}")
                return cached_data

            # Calculate optimal chunk size based on timeframe
            timeframe_seconds = self.timeframes.get(timeframe)
            if timeframe_seconds is None:
                raise ValueError(f"Invalid timeframe: {timeframe}")
            
            total_seconds = int((end_date - start_date).total_seconds())
            total_candles = total_seconds // timeframe_seconds  # Use integer division
            
            # Set chunk size based on timeframe and total candles
            if timeframe in ['1m', '5m']:
                self.chunk_size = min(500, total_candles)  # More frequent timeframes = smaller chunks
            elif timeframe in ['15m', '30m', '1h']:
                self.chunk_size = min(200, total_candles)
            else:
                self.chunk_size = min(100, total_candles)  # For 4h, 1d timeframes
            
            # Ensure chunk_size is at least 1
            self.chunk_size = max(1, self.chunk_size)
            
            # Calculate time chunks for parallel processing
            chunks = []
            current_start = start_date
            
            # Calculate batch size in seconds
            batch_seconds = timeframe_seconds * self.chunk_size
            batch_timedelta = timedelta(seconds=batch_seconds)
            
            while current_start < end_date:
                current_end = min(current_start + batch_timedelta, end_date)
                chunks.append((current_start, current_end))
                current_start = current_end

            # Create progress bar
            total_chunks = len(chunks)
            pbar = tqdm(
                total=total_chunks,
                desc=f"{Fore.CYAN}📊 Fetching {symbol} {timeframe} data{Style.RESET_ALL}",
                unit="chunks",
                bar_format="{l_bar}%s{bar}%s{r_bar}" % (Fore.CYAN, Style.RESET_ALL)
            )
            
            # Create shared data structure for results with priority queue
            results = queue.PriorityQueue()
            
            def fetch_and_process_chunk(chunk_idx, start, end):
                """Fetch and process a single chunk with optimized error handling"""
                max_retries = 3
                retry_delay = 1
                
                for attempt in range(max_retries):
                    try:
                        chunk_data = self._fetch_chunk(symbol, timeframe, start, end)
                        if not chunk_data.empty:
                            # Process the chunk data in parallel with memory optimization
                            self.thread_manager.submit_cpu_task(
                                self._process_chunk_data,
                                chunk_data,
                                chunk_idx,
                                results
                            )
                        return
                    except Exception as e:
                        if attempt < max_retries - 1:
                            logger.warning(f"Retry {attempt + 1}/{max_retries} for chunk {chunk_idx}: {e}")
                            time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                        else:
                            logger.error(f"Failed to fetch chunk {chunk_idx} after {max_retries} attempts: {e}")
            
            # Submit network tasks for fetching data with optimized thread allocation
            active_threads = min(total_chunks, self.thread_manager.io_bound_workers)
            with ThreadPoolExecutor(max_workers=active_threads) as executor:
                futures = []
                for idx, (chunk_start, chunk_end) in enumerate(chunks):
                    future = executor.submit(fetch_and_process_chunk, idx, chunk_start, chunk_end)
                    futures.append(future)
                
                # Process results as they come in with timeout
                processed_chunks = 0
                all_data = []
                
                while processed_chunks < total_chunks:
                    try:
                        chunk_idx, chunk_data = results.get(timeout=5)
                        all_data.append(chunk_data)
                        processed_chunks += 1
                        pbar.update(1)
                    except queue.Empty:
                        # Check if any workers are still alive
                        if not any(f.running() for f in futures):
                            break
            
            pbar.close()

            if not all_data:
                return pd.DataFrame()

            # Combine all chunks efficiently with optimized memory usage
            df = pd.concat(all_data, ignore_index=True, copy=False)
            
            # Sort and remove duplicates efficiently
            df.sort_values('timestamp', inplace=True)
            df.drop_duplicates(subset=['timestamp'], inplace=True)
            
            # Ensure exact date range
            mask = (df['timestamp'] >= pd.Timestamp(start_date)) & (df['timestamp'] <= pd.Timestamp(end_date))
            df = df.loc[mask]

            # Cache the result in a separate thread with compression
            self.thread_manager.submit_io_task(
                self.cache_manager.cache_data,
                df.copy(),
                symbol,
                timeframe,
                start_date,
                end_date
            )

            return df
            
        except Exception as e:
            logger.error(f"Error fetching OHLCV data: {e}")
            return pd.DataFrame()

    @monitor_performance("process_chunk")
    def _process_chunk_data(self, chunk_data: pd.DataFrame, chunk_idx: int, results_queue: queue.PriorityQueue):
        """Process chunk data in parallel"""
        try:
            # Optimize the chunk data
            chunk_data = self._optimize_chunk_memory(chunk_data)
            
            # Put the processed chunk in the queue with its index for ordering
            results_queue.put((chunk_idx, chunk_data))
        except Exception as e:
            logger.error(f"Error processing chunk {chunk_idx}: {e}")
            results_queue.put((chunk_idx, pd.DataFrame()))

    @monitor_performance("optimize_memory")
    def _optimize_chunk_memory(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize DataFrame memory usage"""
        try:
            # Convert float64 to float32
            float_cols = df.select_dtypes(include=['float64']).columns
            for col in float_cols:
                df[col] = df[col].astype('float32')
            
            # Convert int64 to int32
            int_cols = df.select_dtypes(include=['int64']).columns
            for col in int_cols:
                df[col] = df[col].astype('int32')
            
            # Optimize object columns (usually strings)
            obj_cols = df.select_dtypes(include=['object']).columns
            for col in obj_cols:
                if col not in ['timestamp', 'close_time']:  # Skip datetime columns
                    df[col] = pd.Categorical(df[col])
            
            return df
        except Exception as e:
            logger.error(f"Error optimizing chunk memory: {e}")
            return df

    def calculate_candles_for_date_range(
        self,
        timeframe: str,
        start_date: datetime,
        end_date: datetime
    ) -> int:
        """Calculate the total number of candles for a given date range and timeframe"""
        timeframe_seconds = self.parse_timeframe(timeframe)
        total_seconds = (end_date - start_date).total_seconds()
        total_candles = int(total_seconds / timeframe_seconds)
        return total_candles

    def get_available_date_range(self, symbol: str, timeframe: str) -> Dict:
        """Get the available date range for a given symbol and timeframe"""
        try:
            # Get current timestamp in milliseconds
            current_time = int(time.time() * 1000)
            
            # First try to get the actual first data point
            try:
                # For Binance, we'll try from 2017-07-01 (Binance launch date)
                binance_start = int(datetime(2017, 7, 1).timestamp() * 1000)
                first_candle = self.exchange.fetch_ohlcv(
                    symbol,
                    timeframe,
                    since=binance_start,
                    limit=1
                )
                
                if first_candle:
                    first_date = datetime.fromtimestamp(first_candle[0][0] / 1000)
                else:
                    # If no data from launch, try more recent date
                    recent_start = int((datetime.now() - timedelta(days=365)).timestamp() * 1000)
                    first_candle = self.exchange.fetch_ohlcv(
                        symbol,
                        timeframe,
                        since=recent_start,
                        limit=1
                    )
                    if first_candle:
                        first_date = datetime.fromtimestamp(first_candle[0][0] / 1000)
                    else:
                        logger.warning(f"Could not find first available data point for {symbol}")
                        first_date = datetime(2017, 7, 1)  # Default to Binance launch date
            except Exception as e:
                logger.warning(f"Error fetching first candle: {e}")
                first_date = datetime(2017, 7, 1)  # Default to Binance launch date
            
            # Get the last available data point
            try:
                last_candle = self.exchange.fetch_ohlcv(
                    symbol,
                    timeframe,
                    since=current_time - (1000 * 60 * 60 * 24 * 7),  # Look back 7 days
                    limit=1
                )
                
                if last_candle:
                    last_date = datetime.fromtimestamp(last_candle[0][0] / 1000)
                else:
                    last_date = datetime.now()
                    
                # Ensure dates are in the past
                if last_date > datetime.now():
                    last_date = datetime.now()
            except Exception as e:
                logger.warning(f"Error fetching last candle: {e}")
                last_date = datetime.now()
            
            # Print warning if the date range seems unusual
            if (last_date - first_date).days < 30:
                logger.warning(f"Warning: Available data range for {symbol} is less than 30 days")
            
            return {
                'first_date': first_date,
                'last_date': last_date,
                'total_days': (last_date - first_date).days
            }
        except Exception as e:
            logger.error(f"Error fetching date range: {e}")
            return None

    def parse_timeframe(self, timeframe: str) -> int:
        """Parse a timeframe string into seconds
        
        Args:
            timeframe: String in format like '5m', '2h', '3d', etc.
            
        Returns:
            Number of seconds for the timeframe
            
        Raises:
            ValueError: If the timeframe format is invalid
        """
        try:
            # If it's a predefined timeframe, return its value
            if timeframe in self.timeframes:
                return self.timeframes[timeframe]
            
            # Parse custom timeframe
            if len(timeframe) < 2:
                raise ValueError("Invalid timeframe format")
            
            # Split into number and unit
            number = timeframe[:-1]
            unit = timeframe[-1]
            
            if unit not in self.supported_units:
                raise ValueError(f"Unsupported time unit. Use: {', '.join(self.supported_units.keys())}")
            
            # Check if this exact interval is supported by Binance
            if timeframe not in self.valid_intervals:
                valid_intervals = self.supported_units[unit]
                raise ValueError(
                    f"Invalid interval '{timeframe}'. For {unit} unit, only these intervals are supported: {', '.join(valid_intervals)}"
                )
            
            return self.timeframes[timeframe]
            
        except (ValueError, IndexError) as e:
            raise ValueError(f"Invalid timeframe format: {str(e)}")

    def get_user_input(self) -> Dict:
        """Get user input for symbol, timeframe, and date range"""
        # Get available symbols
        symbols = self.get_available_symbols()
        if not symbols:
            logger.error("No symbols available")
            return None

        # Print available symbols in a formatted way
        print(f"\n{Fore.CYAN}Available Trading Pairs:{Style.RESET_ALL}")
        print(f"\n{Fore.YELLOW}Select a trading pair by number:{Style.RESET_ALL}")
        for i, symbol in enumerate(symbols, 1):
            base, quote = symbol.split('/')
            
            # Get the appropriate symbols
            base_symbol = self.crypto_symbols.get(base, base)
            quote_symbol = self.forex_symbols.get(quote, quote)
            
            # Determine category and format display
            if symbol.endswith('/USDT') and not symbol.startswith('PAXG'):
                if any(crypto in symbol for crypto in self.crypto_symbols.keys()):
                    category = f"(Cryptocurrency) {base_symbol}/{quote_symbol}"
                else:
                    category = f"(Forex) {base_symbol}/{quote_symbol}"
            elif symbol.endswith(('/USDC', '/BUSD')):
                category = f"(Stablecoin) {base_symbol}/{quote_symbol}"
            elif symbol.startswith('PAXG'):
                category = f"(Gold) 🏅/{quote_symbol}"
            elif symbol.endswith('/BTC'):
                category = f"(Crypto Cross) {base_symbol}/₿"
            
            print(f"{i}. {symbol:<12} {category}")
        
        # Get symbol selection
        while True:
            try:
                symbol_index = int(input(f"\n{Fore.YELLOW}Select trading pair (enter number): {Style.RESET_ALL}")) - 1
                if 0 <= symbol_index < len(symbols):
                    symbol = symbols[symbol_index]
                    break
                print(f"{Fore.RED}Invalid selection. Please try again.{Style.RESET_ALL}")
            except ValueError:
                print(f"{Fore.RED}Please enter a valid number.{Style.RESET_ALL}")

        # Get timeframe selection
        print(f"\n{Fore.CYAN}Available Timeframes:{Style.RESET_ALL}")
        timeframe_list = list(self.timeframes.keys())
        for i, tf in enumerate(timeframe_list, 1):
            print(f"{i}. {tf}")
        print(f"{len(timeframe_list) + 1}. Custom timeframe")
        
        while True:
            try:
                tf_index = int(input(f"\n{Fore.YELLOW}Select timeframe (enter number): {Style.RESET_ALL}"))
                if 1 <= tf_index <= len(timeframe_list):
                    timeframe = timeframe_list[tf_index - 1]
                    break
                elif tf_index == len(timeframe_list) + 1:
                    print(f"\n{Fore.CYAN}Custom Timeframe Format:{Style.RESET_ALL}")
                    print("Use one of these valid intervals:")
                    print("Minutes: 1m, 3m, 5m, 15m, 30m")
                    print("Hours: 1h, 2h, 4h, 6h, 8h, 12h")
                    print("Days: 1d, 3d")
                    print("Weeks: 1w")
                    print("Months: 1M")
                    print("\nNote: Only these specific intervals are supported by Binance")
                    
                    while True:
                        try:
                            custom_tf = input(f"\n{Fore.YELLOW}Enter custom timeframe: {Style.RESET_ALL}").strip()
                            # Validate the custom timeframe
                            self.parse_timeframe(custom_tf)
                            timeframe = custom_tf
                            break
                        except ValueError as e:
                            print(f"{Fore.RED}{str(e)}{Style.RESET_ALL}")
                    break
                else:
                    print(f"{Fore.RED}Invalid selection. Please try again.{Style.RESET_ALL}")
            except ValueError:
                print(f"{Fore.RED}Please enter a valid number.{Style.RESET_ALL}")

        # Show available date range for the selected symbol and timeframe
        print(f"\n{Fore.CYAN}Fetching available date range for {symbol}...{Style.RESET_ALL}")
        date_range = self.get_available_date_range(symbol, timeframe)
        if date_range:
            print(f"\n{Fore.CYAN}Available Data Range for {symbol} ({timeframe}):{Style.RESET_ALL}")
            print(f"First Available Date: {date_range['first_date'].strftime('%Y-%m-%d')}")
            print(f"Last Available Date: {date_range['last_date'].strftime('%Y-%m-%d')}")
            print(f"Total Days Available: {date_range['total_days']}")
            
            # Get date range with validation against available range
            while True:
                try:
                    start_date_str = input(f"\n{Fore.YELLOW}Enter start date (YYYY-MM-DD): {Style.RESET_ALL}")
                    end_date_str = input(f"{Fore.YELLOW}Enter end date (YYYY-MM-DD): {Style.RESET_ALL}")
                    
                    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
                    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
                    
                    if end_date < start_date:
                        print(f"{Fore.RED}End date must be after start date.{Style.RESET_ALL}")
                        continue
                    
                    if end_date > datetime.now():
                        print(f"{Fore.RED}End date cannot be in the future.{Style.RESET_ALL}")
                        continue
                    
                    if start_date < date_range['first_date']:
                        print(f"{Fore.RED}Warning: Start date is before first available data point.{Style.RESET_ALL}")
                        print(f"Adjusting start date to {date_range['first_date'].strftime('%Y-%m-%d')}")
                        start_date = date_range['first_date']
                    
                    if end_date > date_range['last_date']:
                        print(f"{Fore.RED}Warning: End date is after last available data point.{Style.RESET_ALL}")
                        print(f"Adjusting end date to {date_range['last_date'].strftime('%Y-%m-%d')}")
                        end_date = date_range['last_date']
                    
                    break
                except ValueError:
                    print(f"{Fore.RED}Invalid date format. Please use YYYY-MM-DD.{Style.RESET_ALL}")
        else:
            print(f"\n{Fore.RED}Error: Could not determine available date range for {symbol} ({timeframe}){Style.RESET_ALL}")
            return None

        return {
            'symbol': symbol,
            'timeframe': timeframe,
            'start_date': start_date,
            'end_date': end_date
        }

    def print_summary(self, params: Dict, total_candles: int, ohlcv_data: pd.DataFrame, show_ohlcv: bool = False):
        """Print a summary of the candle count and OHLCV data"""
        print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}📊 Data Summary{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
        
        print(f"\n{Fore.GREEN}Trading Pair:{Style.RESET_ALL} {params['symbol']}")
        print(f"{Fore.GREEN}Timeframe:{Style.RESET_ALL} {params['timeframe']}")
        print(f"{Fore.GREEN}Start Date:{Style.RESET_ALL} {params['start_date'].strftime('%Y-%m-%d')}")
        print(f"{Fore.GREEN}End Date:{Style.RESET_ALL} {params['end_date'].strftime('%Y-%m-%d')}")
        print(f"{Fore.GREEN}Expected Candles:{Style.RESET_ALL} {total_candles}")
        print(f"{Fore.GREEN}Actual Candles:{Style.RESET_ALL} {len(ohlcv_data)}")
        
        if not ohlcv_data.empty:
            # Calculate price statistics
            price_change = ((ohlcv_data['close'].iloc[-1] - ohlcv_data['open'].iloc[0]) / ohlcv_data['open'].iloc[0] * 100)
            color = Fore.GREEN if price_change >= 0 else Fore.RED
            
            print(f"\n{Fore.YELLOW}Price Statistics:{Style.RESET_ALL}")
            print(f"First Price: ${ohlcv_data['open'].iloc[0]:,.2f}")
            print(f"Last Price: ${ohlcv_data['close'].iloc[-1]:,.2f}")
            print(f"Price Change: {color}{price_change:+.2f}%{Style.RESET_ALL}")
            print(f"Highest Price: ${ohlcv_data['high'].max():,.2f}")
            print(f"Lowest Price: ${ohlcv_data['low'].min():,.2f}")
            print(f"Average Price: ${ohlcv_data['close'].mean():,.2f}")
            
            # Calculate volume statistics
            total_volume = ohlcv_data['volume'].sum()
            total_volume_usd = (ohlcv_data['volume'] * ohlcv_data['close']).sum()
            
            print(f"\n{Fore.YELLOW}Volume Statistics:{Style.RESET_ALL}")
            print(f"Total Volume: {total_volume:,.4f}")
            print(f"Volume in USD: ${total_volume_usd:,.2f}")
            print(f"Average Volume: {ohlcv_data['volume'].mean():,.4f}")
            
            # Calculate volatility
            returns = ohlcv_data['close'].pct_change().dropna()
            volatility = returns.std() * 100
            
            print(f"\n{Fore.YELLOW}Market Statistics:{Style.RESET_ALL}")
            print(f"Volatility: {volatility:.2f}%")
            print(f"Average Daily Range: ${(ohlcv_data['high'] - ohlcv_data['low']).mean():,.2f}")
            
            # Calculate candlestick patterns
            bullish_candles = (ohlcv_data['close'] > ohlcv_data['open']).sum()
            bearish_candles = (ohlcv_data['close'] < ohlcv_data['open']).sum()
            bullish_ratio = (bullish_candles / len(ohlcv_data)) * 100
            
            print(f"\n{Fore.YELLOW}Candlestick Patterns:{Style.RESET_ALL}")
            print(f"Bullish Candles: {bullish_candles} ({bullish_ratio:.1f}%)")
            print(f"Bearish Candles: {bearish_candles} ({100-bullish_ratio:.1f}%)")
        
        # Calculate additional statistics
        days = (params['end_date'] - params['start_date']).days
        candles_per_day = len(ohlcv_data) / days if days > 0 else 0
        
        print(f"\n{Fore.YELLOW}Additional Statistics:{Style.RESET_ALL}")
        print(f"Total Days: {days}")
        print(f"Candles per Day: {candles_per_day:.1f}")
        print(f"Total Hours: {days * 24}")
        print(f"Candles per Hour: {candles_per_day / 24:.1f}")
        
        print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
        
        if show_ohlcv and not ohlcv_data.empty:
            # Ask user if they want to view OHLCV data
            while True:
                view_data = input(f"\n{Fore.YELLOW}Would you like to view the OHLCV data? (y/n): {Style.RESET_ALL}").lower().strip()
                if view_data in ['y', 'n']:
                    break
                print(f"{Fore.RED}Please enter 'y' for yes or 'n' for no.{Style.RESET_ALL}")
            
            if view_data == 'y':
                # Create a copy of the DataFrame with formatted columns
                display_df = ohlcv_data.copy()
                display_df['timestamp'] = display_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                display_df['close_time'] = display_df['close_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                # Format price columns
                for col in ['open', 'high', 'low', 'close']:
                    display_df[col] = display_df[col].apply(lambda x: f"${x:,.2f}")
                
                # Format volume columns with 8 decimal places
                volume_cols = ['volume', 'quote_asset_volume', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
                for col in volume_cols:
                    display_df[col] = display_df[col].apply(lambda x: f"{x:,.8f}")
                
                # Format number of trades as integer with commas
                display_df['number_of_trades'] = display_df['number_of_trades'].apply(lambda x: f"{int(x):,}")
                
                print(f"\n{Fore.CYAN}OHLCV Data Navigation{Style.RESET_ALL}")
                print(f"{Fore.CYAN}{'='*120}{Style.RESET_ALL}")
                print(f"{Fore.YELLOW}Navigation Controls:{Style.RESET_ALL}")
                print("n: Next page")
                print("p: Previous page")
                print("f: First page")
                print("l: Last page")
                print("g: Go to page")
                print("s: Save to CSV")
                print("t: Sort by column")
                print("r: Reset sort/filter")
                print("h: Filter by high price")
                print("c: Filter by close price")
                print("v: Filter by volume")
                print("d: Filter by date range")
                print("q: Quit viewing")
                
                chunk_size = 20
                filtered_df = display_df.copy()
                total_pages = (len(filtered_df) + chunk_size - 1) // chunk_size
                current_page = 1
                sort_column = None
                sort_ascending = True
                
                while True:
                    # Clear previous output (print newlines)
                    print("\n" * 2)
                    
                    # Display current page information
                    start_idx = (current_page - 1) * chunk_size
                    end_idx = min(start_idx + chunk_size, len(filtered_df))
                    chunk = filtered_df.iloc[start_idx:end_idx]
                    
                    print(f"{Fore.CYAN}Page {current_page} of {total_pages}{Style.RESET_ALL}")
                    print(f"Showing records {start_idx + 1} to {end_idx} of {len(filtered_df)}")
                    if sort_column:
                        order = "ascending" if sort_ascending else "descending"
                        print(f"Sorted by {sort_column} ({order})")
                    print(f"{Fore.CYAN}{'='*120}{Style.RESET_ALL}")
                    
                    # Display the data
                    print(chunk.to_string(index=False))
                    print(f"\n{Fore.CYAN}{'='*120}{Style.RESET_ALL}")
                    
                    # Get user input for navigation
                    action = input(f"\n{Fore.YELLOW}Enter command (n/p/f/l/g/s/t/r/h/c/v/d/q): {Style.RESET_ALL}").lower().strip()
                    
                    if action == 'q':
                        break
                    elif action == 'n' and current_page < total_pages:
                        current_page += 1
                    elif action == 'p' and current_page > 1:
                        current_page -= 1
                    elif action == 'f':
                        current_page = 1
                    elif action == 'l':
                        current_page = total_pages
                    elif action == 'g':
                        try:
                            page = int(input(f"{Fore.YELLOW}Enter page number (1-{total_pages}): {Style.RESET_ALL}"))
                            if 1 <= page <= total_pages:
                                current_page = page
                            else:
                                print(f"{Fore.RED}Invalid page number. Please enter a number between 1 and {total_pages}.{Style.RESET_ALL}")
                        except ValueError:
                            print(f"{Fore.RED}Invalid input. Please enter a number.{Style.RESET_ALL}")
                    elif action == 's':
                        try:
                            filename = f"{params['symbol'].replace('/', '_')}_{params['timeframe']}_{params['start_date'].strftime('%Y%m%d')}_{params['end_date'].strftime('%Y%m%d')}.csv"
                            ohlcv_data.to_csv(filename, index=False)
                            print(f"\n{Fore.GREEN}✓ Data successfully saved to {filename}{Style.RESET_ALL}")
                        except Exception as e:
                            print(f"\n{Fore.RED}Error saving data: {str(e)}{Style.RESET_ALL}")
                    elif action == 't':
                        columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                        print("\nAvailable columns:")
                        for i, col in enumerate(columns, 1):
                            print(f"{i}. {col}")
                        try:
                            col_idx = int(input(f"{Fore.YELLOW}Select column number to sort by: {Style.RESET_ALL}")) - 1
                            if 0 <= col_idx < len(columns):
                                sort_column = columns[col_idx]
                                sort_ascending = input(f"{Fore.YELLOW}Sort ascending? (y/n): {Style.RESET_ALL}").lower().strip() == 'y'
                                # Convert price strings back to float for sorting
                                if sort_column in ['open', 'high', 'low', 'close']:
                                    filtered_df[sort_column] = filtered_df[sort_column].str.replace('$', '').str.replace(',', '').astype(float)
                                elif sort_column == 'volume':
                                    filtered_df[sort_column] = filtered_df[sort_column].str.replace(',', '').astype(float)
                                filtered_df = filtered_df.sort_values(sort_column, ascending=sort_ascending)
                                # Reformat the columns after sorting
                                if sort_column in ['open', 'high', 'low', 'close']:
                                    filtered_df[sort_column] = filtered_df[sort_column].apply(lambda x: f"${x:,.2f}")
                                elif sort_column == 'volume':
                                    filtered_df[sort_column] = filtered_df[sort_column].apply(lambda x: f"{x:,.8f}")
                                current_page = 1
                            else:
                                print(f"{Fore.RED}Invalid column number.{Style.RESET_ALL}")
                        except ValueError:
                            print(f"{Fore.RED}Invalid input. Please enter a number.{Style.RESET_ALL}")
                    elif action == 'r':
                        filtered_df = display_df.copy()
                        sort_column = None
                        current_page = 1
                        total_pages = (len(filtered_df) + chunk_size - 1) // chunk_size
                        print(f"\n{Fore.GREEN}✓ View reset to original data{Style.RESET_ALL}")
                    elif action in ['h', 'c', 'v']:
                        column = {'h': 'high', 'c': 'close', 'v': 'volume'}[action]
                        try:
                            min_val = float(input(f"{Fore.YELLOW}Enter minimum {column} value: {Style.RESET_ALL}"))
                            max_val = float(input(f"{Fore.YELLOW}Enter maximum {column} value: {Style.RESET_ALL}"))
                            # Convert price strings back to float for filtering
                            if column in ['high', 'close']:
                                filtered_df[column] = filtered_df[column].str.replace('$', '').str.replace(',', '').astype(float)
                            elif column == 'volume':
                                filtered_df[column] = filtered_df[column].str.replace(',', '').astype(float)
                            filtered_df = filtered_df[
                                (filtered_df[column] >= min_val) & 
                                (filtered_df[column] <= max_val)
                            ]
                            # Reformat the columns after filtering
                            if column in ['high', 'close']:
                                filtered_df[column] = filtered_df[column].apply(lambda x: f"${x:,.2f}")
                            elif column == 'volume':
                                filtered_df[column] = filtered_df[column].apply(lambda x: f"{x:,.8f}")
                            current_page = 1
                            total_pages = (len(filtered_df) + chunk_size - 1) // chunk_size
                            if filtered_df.empty:
                                print(f"{Fore.RED}No data matches the filter criteria.{Style.RESET_ALL}")
                                filtered_df = display_df.copy()
                            else:
                                print(f"\n{Fore.GREEN}✓ Successfully filtered data ({len(filtered_df)} records match){Style.RESET_ALL}")
                        except ValueError:
                            print(f"{Fore.RED}Invalid input. Please enter valid numbers.{Style.RESET_ALL}")
                    elif action == 'd':
                        try:
                            start = input(f"{Fore.YELLOW}Enter start date (YYYY-MM-DD HH:MM:SS): {Style.RESET_ALL}").strip()
                            end = input(f"{Fore.YELLOW}Enter end date (YYYY-MM-DD HH:MM:SS): {Style.RESET_ALL}").strip()
                            filtered_df = display_df[
                                (display_df['timestamp'] >= start) & 
                                (display_df['timestamp'] <= end)
                            ]
                            current_page = 1
                            total_pages = (len(filtered_df) + chunk_size - 1) // chunk_size
                            if filtered_df.empty:
                                print(f"{Fore.RED}No data matches the date range.{Style.RESET_ALL}")
                                filtered_df = display_df.copy()
                            else:
                                print(f"\n{Fore.GREEN}✓ Successfully filtered data ({len(filtered_df)} records match){Style.RESET_ALL}")
                        except Exception as e:
                            print(f"{Fore.RED}Invalid date format. Use YYYY-MM-DD HH:MM:SS{Style.RESET_ALL}")
                    else:
                        print(f"{Fore.RED}Invalid command. Please try again.{Style.RESET_ALL}")
                        
                    # Add a small delay to prevent screen flicker
                    time.sleep(0.1)

    @monitor_performance("analyze_csv")
    def analyze_csv_data(self, csv_file: str) -> None:
        """Analyze the CSV data with memory optimization"""
        try:
            # Read CSV in chunks to optimize memory usage
            chunk_size = 50000
            chunks = pd.read_csv(csv_file, chunksize=chunk_size)
            
            # Initialize aggregation variables
            total_rows = 0
            price_min = float('inf')
            price_max = float('-inf')
            volume_min = float('inf')
            volume_max = float('-inf')
            large_price_changes = []
            large_volume_changes = []
            first_timestamp = None
            last_timestamp = None
            
            # Process each chunk
            for i, chunk in enumerate(chunks):
                # Convert timestamps
                chunk['timestamp'] = pd.to_datetime(chunk['timestamp'])
                
                # Update first and last timestamps
                if i == 0:
                    first_timestamp = chunk['timestamp'].min()
                last_timestamp = chunk['timestamp'].max()
                
                # Update price and volume ranges
                price_min = min(price_min, chunk['low'].min())
                price_max = max(price_max, chunk['high'].max())
                volume_min = min(volume_min, chunk['volume'].min())
                volume_max = max(volume_max, chunk['volume'].max())
                
                # Calculate price and volume changes
                if i > 0:  # Skip first chunk as we can't calculate changes
                    price_changes = chunk['close'].pct_change()
                    volume_changes = chunk['volume'].pct_change()
                    
                    # Record significant changes
                    large_price_idx = price_changes[abs(price_changes) > 0.1].index
                    for idx in large_price_idx:
                        large_price_changes.append({
                            'timestamp': chunk['timestamp'][idx],
                            'change': price_changes[idx],
                            'from': chunk['close'][idx-1],
                            'to': chunk['close'][idx]
                        })
                    
                    large_vol_idx = volume_changes[abs(volume_changes) > 5].index
                    for idx in large_vol_idx:
                        large_volume_changes.append({
                            'timestamp': chunk['timestamp'][idx],
                            'change': volume_changes[idx],
                            'from': chunk['volume'][idx-1],
                            'to': chunk['volume'][idx]
                        })
                
                total_rows += len(chunk)
            
            # Print analysis results
            print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
            print(f"{Fore.CYAN}📊 Data Analysis Summary{Style.RESET_ALL}")
            print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
            
            print(f"\n{Fore.YELLOW}General Statistics:{Style.RESET_ALL}")
            print(f"Total Records: {total_rows:,}")
            print(f"Date Range: {first_timestamp} to {last_timestamp}")
            print(f"Duration: {last_timestamp - first_timestamp}")
            
            print(f"\n{Fore.YELLOW}Price Statistics:{Style.RESET_ALL}")
            print(f"Price Range: ${price_min:,.2f} - ${price_max:,.2f}")
            print(f"Price Spread: ${price_max - price_min:,.2f}")
            
            print(f"\n{Fore.YELLOW}Volume Statistics:{Style.RESET_ALL}")
            print(f"Volume Range: {volume_min:,.4f} - {volume_max:,.4f}")
            
            if large_price_changes:
                print(f"\n{Fore.YELLOW}Significant Price Changes (>10%):{Style.RESET_ALL}")
                for change in sorted(large_price_changes, key=lambda x: abs(x['change']), reverse=True)[:5]:
                    print(f"- {change['timestamp']}: {change['change']*100:+.2f}% (${change['from']:,.2f} → ${change['to']:,.2f})")
            
            if large_volume_changes:
                print(f"\n{Fore.YELLOW}Significant Volume Changes (>500%):{Style.RESET_ALL}")
                for change in sorted(large_volume_changes, key=lambda x: abs(x['change']), reverse=True)[:5]:
                    print(f"- {change['timestamp']}: {change['change']*100:+.2f}% ({change['from']:,.4f} → {change['to']:,.4f})")
            
            print(f"\n{Fore.GREEN}✓ Analysis complete{Style.RESET_ALL}")
            
        except Exception as e:
            logger.error(f"Error analyzing CSV data: {e}")

    def print_performance_stats(self):
        """Print a summary of performance statistics"""
        stats = self.performance_monitor.get_summary()
        print(f"\n{Fore.CYAN}Performance Statistics:{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'='*50}{Style.RESET_ALL}")
        
        for operation, metrics in stats.items():
            print(f"\n{Fore.YELLOW}{operation} Statistics:{Style.RESET_ALL}")
            print("-" * 30)
            print(f"Average Duration: {metrics['avg_duration']:.3f}s")
            print(f"Min Duration: {metrics['min_duration']:.3f}s")
            print(f"Max Duration: {metrics['max_duration']:.3f}s")
            print(f"Success Rate: {metrics['success_rate']:.1f}%")
            print(f"Total Operations: {metrics['total_operations']}")
            
            if metrics['total_data_processed'] > 0:
                print(f"Data Processed: {metrics['total_data_processed']:,} bytes")
                print(f"Throughput: {metrics['throughput']:.2f} bytes/sec")
            
            if metrics['avg_memory_usage'] > 0:
                print(f"Avg Memory Usage: {metrics['avg_memory_usage'] / 1024 / 1024:.1f} MB")
            
            if metrics['avg_thread_count'] > 0:
                print(f"Avg Thread Count: {metrics['avg_thread_count']:.1f}")
        
        print(f"\n{Fore.YELLOW}Overall System Statistics:{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'='*30}{Style.RESET_ALL}")
        
        # Calculate overall statistics
        total_operations = sum(m['total_operations'] for m in stats.values())
        avg_success_rate = statistics.mean(m['success_rate'] for m in stats.values())
        total_data_processed = sum(m['total_data_processed'] for m in stats.values())
        avg_memory = statistics.mean(m['avg_memory_usage'] for m in stats.values() if m['avg_memory_usage'] > 0)
        
        print(f"Total Operations: {total_operations}")
        print(f"Average Success Rate: {avg_success_rate:.1f}%")
        print(f"Total Data Processed: {total_data_processed:,} bytes")
        print(f"Average Memory Usage: {avg_memory / 1024 / 1024:.1f} MB")
        
        # Add performance recommendations
        print(f"\n{Fore.YELLOW}Performance Recommendations:{Style.RESET_ALL}")
        print(f"{Fore.CYAN}{'='*30}{Style.RESET_ALL}")
        
        for operation, metrics in stats.items():
            if metrics['avg_duration'] > 1.0:  # Operations taking more than 1 second
                print(f"- Consider optimizing {operation}: Average duration {metrics['avg_duration']:.2f}s")
            if metrics['success_rate'] < 90:  # Success rate below 90%
                print(f"- High error rate in {operation}: {100 - metrics['success_rate']:.1f}% errors")
            if metrics['avg_memory_usage'] > 500 * 1024 * 1024:  # Memory usage above 500MB
                print(f"- High memory usage in {operation}: {metrics['avg_memory_usage'] / 1024 / 1024:.1f} MB")
        
        print(f"\n{Fore.GREEN}Note: These statistics are from the current session only{Style.RESET_ALL}")

def main():
    # Initialize the counter
    counter = CandleCounter()
    
    # Print welcome message
    print(f"\n{Fore.CYAN}{'='*60}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}🚀 HedgeBot Candle Counter{Style.RESET_ALL}")
    print(f"{Fore.CYAN}⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*60}{Style.RESET_ALL}\n")
    
    while True:
        # Get user input
        params = counter.get_user_input()
        if not params:
            break
        
        # Calculate expected total candles
        total_candles = counter.calculate_candles_for_date_range(
            params['timeframe'],
            params['start_date'],
            params['end_date']
        )
        
        # Fetch actual OHLCV data
        ohlcv_data = counter.fetch_ohlcv_data(
            params['symbol'],
            params['timeframe'],
            params['start_date'],
            params['end_date']
        )
        
        # Print summary and ask for OHLCV data view
        counter.print_summary(params, total_candles, ohlcv_data, show_ohlcv=True)
        
        # Print performance metrics
        print(f"\n{Fore.CYAN}Performance Metrics:{Style.RESET_ALL}")
        counter.print_performance_stats()
        
        # Ask if user wants to save data to CSV
        while True:
            save_response = input(f"\n{Fore.YELLOW}Would you like to save the data to CSV? (y/n): {Style.RESET_ALL}").lower()
            if save_response in ['y', 'n']:
                break
            print(f"{Fore.RED}Please enter 'y' or 'n'.{Style.RESET_ALL}")
        
        if save_response == 'y':
            try:
                filename = f"{params['symbol'].replace('/', '_')}_{params['timeframe']}_{params['start_date'].strftime('%Y%m%d')}_{params['end_date'].strftime('%Y%m%d')}.csv"
                ohlcv_data.to_csv(filename, index=False)
                print(f"\n{Fore.GREEN}✓ Data successfully saved to {filename}{Style.RESET_ALL}")
            except Exception as e:
                print(f"\n{Fore.RED}Error saving data: {str(e)}{Style.RESET_ALL}")
        
        # Ask if user wants to continue
        while True:
            response = input(f"\n{Fore.YELLOW}Would you like to calculate another Symbol? (y/n): {Style.RESET_ALL}").lower()
            if response in ['y', 'n']:
                break
            print(f"{Fore.RED}Please enter 'y' or 'n'.{Style.RESET_ALL}")
        
        if response == 'n':
            break
    
    print(f"\n{Fore.GREEN}Thank you for using HedgeBot Candle Counter!{Style.RESET_ALL}")

if __name__ == "__main__":
    main() 