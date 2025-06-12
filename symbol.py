# symbol.py
from typing import Dict, List, Any, Callable, Optional
import threading
import time
import os
import gzip
import json
import sys
import psutil
import hashlib
from datetime import datetime, timezone
import logging
from collections import OrderedDict

logger = logging.getLogger(__name__)

class LRUCache:
    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity

    def get(self, key: str) -> bool:
        if key not in self.cache:
            return False
        self.cache.move_to_end(key)
        return self.cache[key]

    def put(self, key: str, value: bool):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

class Symbol:
    def __init__(
        self,
        symbol: str,
        data_dir: str,
        data_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        orderbook_depth: int = 50,
        kline_interval: str = "1m",
        memory_retention_seconds: int = int(os.getenv("SYMBOL_RETENTION_SECONDS", 60)),
        max_buffer_size: int = int(os.getenv("SYMBOL_MAX_BUFFER_SIZE", 5000))
    ):
        self.symbol = symbol.upper()
        self.subscribed_topics = {"publicTrade": False, "orderbook": False, "kline": False}
        self.orderbook_depth = orderbook_depth
        self.kline_interval = kline_interval
        self.data_callback = data_callback
        self.memory_retention_seconds = memory_retention_seconds
        self.max_buffer_size = max_buffer_size
        self.lock = threading.Lock()
        self.trade_buffer: List[Dict[str, Any]] = []
        self.last_trade_ids = LRUCache(10000)
        self.callback_buffer: List[Dict[str, Any]] = []
        self.trade_count = 0
        self.flush_thread = None
        self.callback_thread = None
        self.running = False
        self.last_update = None
        self.last_trade_time = time.time()
        self.start_time = time.time()
        self.data_dir = data_dir
        self.last_checksum = None
        self.commit_hash = os.getenv("GITHUB_COMMIT_HASH", "none")
        os.makedirs(data_dir, exist_ok=True)

    def get_topic_name(self, topic_type: str) -> str:
        if topic_type == "orderbook":
            return f"orderbook.{self.orderbook_depth}.{self.symbol}"
        elif topic_type == "kline":
            return f"kline.{self.kline_interval}.{self.symbol}"
        return f"{topic_type}.{self.symbol}"

    def start(self):
        self.running = True
        self.flush_thread = threading.Thread(
            target=self.flush_task, name=f"{self.symbol}-flush", daemon=True
        )
        self.callback_thread = threading.Thread(
            target=self.callback_task, name=f"{self.symbol}-callback", daemon=True
        )
        self.flush_thread.start()
        self.callback_thread.start()
        logger.debug(f"[{self.symbol}] Started Symbol")

    def stop(self):
        with self.lock:
            if not self.running:
                return
            self.running = False
        if self.flush_thread:
            self.flush_thread.join(timeout=2.0)
        if self.callback_thread:
            self.callback_thread.join(timeout=2.0)
        with self.lock:
            self.trade_buffer.clear()
            self.callback_buffer.clear()
            self.last_trade_ids = LRUCache(10000)
            self.flush_thread = None
            self.callback_thread = None
        logger.info(f"[{self.symbol}] Stopped Symbol")

    def request_add(self, ws_manager: 'WebSocketManager') -> bool:
        return ws_manager.add_symbol(self)

    def on_admitted(self):
        logger.info(f"[{self.symbol}] Admitted to WebSocket client")
        self.subscribe(WebSocketManager, ["publicTrade"])

    def subscribe(self, ws_manager: 'WebSocketManager', topic_types: List[str]):
        for client in ws_manager.clients:
            if self.symbol in client.symbol_map:
                client.subscribe(self, topic_types)
                break

    def unsubscribe(self, ws_manager: 'WebSocketManager', topic_types: List[str]):
        for client in ws_manager.clients:
            if self.symbol in client.symbol_map:
                client.unsubscribe(self, topic_types)
                break

    def process_trade(self, trade_data: Dict[str, Any]):
        with self.lock:
            if not self.running:
                return
            trade_id = trade_data.get("trade_id")
            if self.last_trade_ids.get(trade_id):
                logger.debug(f"[{self.symbol}] Skipped duplicate trade: {trade_id}")
                return
            if len(self.trade_buffer) >= self.max_buffer_size:
                logger.warning(f"[{self.symbol}] Buffer overflow, triggering flush")
                self._flush_old_trades()
            self.last_trade_ids.put(trade_id, True)
            self.trade_buffer.append(trade_data)
            self.callback_buffer.append({"type": "trade", "data": trade_data})
            self.trade_count += 1
            self.last_update = datetime.utcnow()
            self.last_trade_time = time.time()
            logger.info(f"[{self.symbol}] Processed trade: {trade_id}, price={trade_data['price']}")

    def callback_task(self):
        while self.running:
            time.sleep(0.1)
            with self.lock:
                if not self.callback_buffer:
                    continue
                batch = self.callback_buffer[:100]
                self.callback_buffer = self.callback_buffer[100:]
            if self.data_callback:
                try:
                    for item in batch:
                        self.data_callback(item)
                    logger.debug(f"[{self.symbol}] Sent {len(batch)} callback items")
                except Exception as e:
                    logger.error(f"[{self.symbol}] Data callback error: {e}")

    def process_orderbook(self, orderbook_data: Dict[str, Any]):
        logger.info(f"[{self.symbol}] Orderbook: {orderbook_data}")
        if self.data_callback:
            try:
                self.data_callback({"type": "orderbook", "data": orderbook_data})
            except Exception as e:
                logger.error(f"[{self.symbol}] Data callback error: {e}")

    def process_kline(self, kline_data: Dict[str, Any]):
        logger.info(f"[{self.symbol}] Kline: {kline_data}")
        if self.data_callback:
            try:
                self.data_callback({"type": "kline", "data": kline_data})
            except Exception as e:
                logger.error(f"[{self.symbol}] Data callback error: {e}")

    def on_connect(self):
        logger.info(f"[{self.symbol}] WebSocket connected")
        if not any(self.subscribed_topics.values()):
            self.subscribe(WebSocketManager, ["publicTrade"])

    def on_disconnect(self, reason: str):
        logger.info(f"[{self.symbol}] WebSocket disconnected: {reason}")
        for topic_type in self.subscribed_topics:
            self.subscribed_topics[topic_type] = False

    def set_subscribed(self, topic_type: str, status: bool):
        self.subscribed_topics[topic_type] = status
        logger.info(f"[{self.symbol}] {topic_type} subscribed: {status}")

    def flush_task(self):
        while self.running:
            time.sleep(10)
            self._flush_old_trades()

    def _flush_old_trades(self):
        cutoff = time.time() - self.memory_retention_seconds
        today = datetime.utcnow().strftime("%Y-%m-%d")
        file_path = os.path.join(self.data_dir, f"{self.symbol}_{today}.jsonl.gz")
        try:
            disk_usage = psutil.disk_usage(self.data_dir)
            free_percent = disk_usage.free / disk_usage.total * 100
            if free_percent < 10:
                logger.warning(f"[{self.symbol}] Low disk space: {free_percent:.1f}% free")
        except Exception as e:
            logger.error(f"[{self.symbol}] Disk space check failed: {e}")
        with self.lock:
            trades_to_flush = [t for t in self.trade_buffer if t["timestamp"] < cutoff]
            if not trades_to_flush:
                return
            self.trade_buffer[:] = [t for t in self.trade_buffer if t["timestamp"] >= cutoff]
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with gzip.open(file_path, "at", encoding="utf-8") as f:
                for trade in trades_to_flush:
                    f.write(json.dumps(trade) + "\n")
            logger.debug(f"[{self.symbol}] Flushed {len(trades_to_flush)} trades to {file_path}")
        except OSError as e:
            logger.error(f"[{self.symbol}] Failed to flush trades: {e}")

    def read_today_file(self) -> List[Dict[str, Any]]:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        file_path = os.path.join(self.data_dir, f"{self.symbol}_{today}.jsonl.gz")
        trades = []
        try:
            if os.path.exists(file_path):
                with gzip.open(file_path, "rt", encoding="utf-8") as f:
                    for line in f:
                        try:
                            trade = json.loads(line.strip())
                            trades.append(trade)
                        except json.JSONDecodeError:
                            logger.error(f"[{self.symbol}] Invalid JSON in {file_path}: {line}")
        except OSError as e:
            logger.error(f"[{self.symbol}] Failed to read {file_path}: {e}")
        return trades

    def get_trades(self, after_timestamp: float) -> List[Dict[str, Any]]:
        with self.lock:
            buffer_trades = [t for t in self.trade_buffer if t["timestamp"] > after_timestamp]
        disk_trades = self.read_today_file()
        disk_trades = [t for t in disk_trades if t["timestamp"] > after_timestamp]
        all_trades = sorted(buffer_trades + disk_trades, key=lambda x: x["timestamp"])
        return all_trades[:1000]

    def get_status(self) -> Dict[str, Any]:
        with self.lock:
            buffer_size = sum(sys.getsizeof(t) for t in self.trade_buffer) / (1024 * 1024)
            cache_size = sum(sys.getsizeof(k) + sys.getsizeof(v) for k, v in self.last_trade_ids.cache.items()) / (1024 * 1024)
            memory_usage_mb = buffer_size + cache_size
            if memory_usage_mb > 10:
                logger.warning(f"[{self.symbol}] High memory usage: {memory_usage_mb:.2f}MB")
            return {
                "symbol": self.symbol,
                "subscribed_topics": self.subscribed_topics.copy(),
                "last_update": self.last_update.isoformat() if self.last_update else None,
                "active": self.running and any(self.subscribed_topics.values()),
                "buffer_size": len(self.trade_buffer),
                "trade_count": self.trade_count,
                "memory_usage_mb": memory_usage_mb,
                "last_checksum": self.last_checksum,
                "commit_hash": self.commit_hash
            }