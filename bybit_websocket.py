# bybit_websocket.py
import websocket
import json
import time
import threading
import logging
import sys
import os
import hashlib
import psutil
from datetime import datetime, timezone
from typing import List, Dict, Optional

# Config
LOG_FILE = "C:/develop/trunk/python/tamagochi/trades/log/websocket.log"
WS_URL = "wss://stream.bybit.com/v5/public/spot"
PING_INTERVAL = 20
RECONNECT_DELAY = 5
MAX_RECONNECT_DELAY = 60
HEARTBEAT_TIMEOUT = 180
MAX_TOPICS = int(os.getenv("MAX_TOPICS_PER_CLIENT", 50))
MAX_MEMORY_MB = int(os.getenv("MAX_MEMORY_PER_CLIENT", 100))
MEMORY_THRESHOLD_MB = 50

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

# Compute MD5 checksum
def compute_checksum(content: str, algorithm: str = "md5") -> str:
    checksum = hashlib.md5(content.encode("utf-8")).hexdigest() if algorithm == "md5" else hashlib.sha256(content.encode("utf-8")).hexdigest()
    logger.debug(f"Computed {algorithm.upper()} checksum: {checksum}")
    return checksum

class BybitWebSocketClient:
    def __init__(self, url: str = WS_URL, config: dict = None):
        self.url = url
        self.symbols: List['Symbol'] = []
        self.symbol_map: Dict[str, 'Symbol'] = {}
        self.topic_count = 0
        self.ws = None
        self.running = False
        self.connected = False
        self.lock = threading.Lock()
        self.last_message = time.time()
        self.last_ping = 0
        self.error_count = 0
        self.subscription_status: Dict[str, bool] = {}
        self.last_checksum = None
        self.commit_hash = os.getenv("GITHUB_COMMIT_HASH", "none")
        self.config = {
            "ping_interval": PING_INTERVAL,
            "reconnect_delay": RECONNECT_DELAY,
            "max_reconnect_delay": MAX_RECONNECT_DELAY,
            "heartbeat_timeout": HEARTBEAT_TIMEOUT,
            "max_topics": MAX_TOPICS,
            "max_memory_mb": MAX_MEMORY_MB
        }
        if config:
            self.config.update(config)
        logger.debug("Initializing WebSocket client")
        try:
            disk_usage = psutil.disk_usage(os.path.dirname(LOG_FILE))
            if disk_usage.free / disk_usage.total * 100 < 10:
                logger.warning(f"Low disk space for {LOG_FILE}: {disk_usage.free / 1024**3:.1f}GB free")
        except Exception as e:
            logger.error(f"Disk space check failed: {e}")

    def start(self):
        self.running = True
        threading.Thread(target=self.connect, daemon=True).start()
        threading.Thread(target=self.check_heartbeat, daemon=True).start()

    def add_symbol(self, symbol: 'Symbol') -> bool:
        with self.lock:
            potential_topics = sum(1 for t in symbol.subscribed_topics.values() if t)
            if self.topic_count + potential_topics > self.config["max_topics"]:
                logger.warning(f"Cannot add {symbol.symbol}: Exceeds max topics ({self.config['max_topics']})")
                return False
            if symbol.symbol in self.symbol_map:
                logger.warning(f"Symbol {symbol.symbol} already registered")
                return False
            estimated_memory_mb = (self.topic_count + potential_topics) * 0.001
            if estimated_memory_mb > self.config["max_memory_mb"]:
                logger.warning(f"Memory estimate ({estimated_memory_mb:.2f}MB) exceeds max ({self.config['max_memory_mb']}MB)")
                return False
            self.symbols.append(symbol)
            self.symbol_map[symbol.symbol] = symbol
            self.topic_count += potential_topics
            logger.info(f"Admitted symbol: {symbol.symbol}")
            symbol.on_admitted()
            if self.connected:
                symbol.on_connect()
            return True

    def remove_symbol(self, symbol: 'Symbol'):
        with self.lock:
            if symbol.symbol in self.symbol_map:
                if any(symbol.subscribed_topics.values()):
                    self._send_unsubscribe([
                        symbol.get_topic_name(t) for t in symbol.subscribed_topics if symbol.subscribed_topics[t]
                    ])
                self.topic_count -= sum(1 for t in symbol.subscribed_topics.values() if t)
                self.symbols.remove(symbol)
                del self.symbol_map[symbol.symbol]
                for topic in list(self.subscription_status):
                    if topic.endswith(symbol.symbol):
                        self.subscription_status.pop(topic)
                logger.info(f"Removed symbol: {symbol.symbol}")

    def connect(self):
        attempt = 0
        while self.running:
            try:
                logger.info(f"Connecting to {self.url} (attempt {attempt + 1})")
                self.ws = websocket.WebSocketApp(
                    self.url,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close
                )
                self.ws.run_forever(ping_interval=self.config["ping_interval"], ping_payload='{"op": "ping"}')
                attempt = 0
                with self.lock:
                    self.connected = False
                    for symbol in self.symbols:
                        for topic_type in symbol.subscribed_topics:
                            symbol.set_subscribed(topic_type, False)
                        symbol.on_disconnect("WebSocket connection closed")
            except Exception as e:
                logger.error(f"Connection error: {e}")
                self.error_count += 1
                attempt += 1
                delay = min(self.config["reconnect_delay"] * (2 ** attempt), self.config["max_reconnect_delay"])
                logger.info(f"Reconnecting after {delay}s")
                time.sleep(delay)

    def close(self):
        self.running = False
        with self.lock:
            if self.ws:
                try:
                    self.ws.close()
                    logger.info("WebSocket closed")
                except Exception as e:
                    logger.error(f"Error closing WebSocket: {e}")
                    self.error_count += 1
            self.ws = None
            self.connected = False
            for symbol in self.symbols:
                for topic_type in symbol.subscribed_topics:
                    symbol.set_subscribed(topic_type, False)
                symbol.on_disconnect("WebSocket client closed")

    def subscribe(self, symbol: 'Symbol', topic_types: List[str]):
        with self.lock:
            if not self.connected:
                logger.warning(f"Cannot subscribe {symbol.symbol}, not connected")
                return
            if symbol.symbol not in self.symbol_map:
                logger.warning(f"Cannot subscribe {symbol.symbol}, not registered")
                return
            valid_topics = [
                t for t in topic_types
                if t in symbol.subscribed_topics and not symbol.subscribed_topics[t]
            ]
            if not valid_topics:
                logger.debug(f"No new topics to subscribe for {symbol.symbol}")
                return
            new_topic_count = self.topic_count + len(valid_topics)
            if new_topic_count > self.config["max_topics"]:
                logger.warning(f"Cannot subscribe {valid_topics}: Exceeds max topics")
                return
            self.topic_count = new_topic_count
            topics = [symbol.get_topic_name(t) for t in valid_topics]
            self._send_subscribe(topics)
            for topic in topics:
                self.subscription_status[topic] = True

    def unsubscribe(self, symbol: 'Symbol', topic_types: List[str]):
        with self.lock:
            if not self.connected:
                logger.warning(f"Cannot unsubscribe {symbol.symbol}, not connected")
                return
            if symbol.symbol not in self.symbol_map:
                logger.warning(f"Cannot unsubscribe {symbol.symbol}, not registered")
                return
            valid_topics = [
                t for t in topic_types if t in symbol.subscribed_topics and symbol.subscribed_topics[t]
            ]
            if not valid_topics:
                logger.debug(f"No topics to unsubscribe for {symbol.symbol}")
                return
            topics = [symbol.get_topic_name(t) for t in valid_topics]
            self._send_unsubscribe(topics)
            for topic in topics:
                self.subscription_status[topic] = False

    def _send_subscribe(self, topics: List[str]):
        for i in range(0, len(topics), 10):
            batch = topics[i:i+10]
            sub_msg = {"op": "subscribe", "args": batch}
            try:
                self.ws.send(json.dumps(sub_msg))
                logger.info(f"Sent subscribe: {batch}")
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Subscribe error: {e}")
                self.error_count += 1

    def _send_unsubscribe(self, topics: List[str]):
        for i in range(0, len(topics), 10):
            batch = topics[i:i+10]
            unsub_msg = {"op": "unsubscribe", "args": batch}
            try:
                self.ws.send(json.dumps(unsub_msg))
                logger.info(f"Sent unsubscribe: {batch}")
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Unsubscribe error: {e}")
                self.error_count += 1

    def on_open(self, ws):
        logger.info("WebSocket connected")
        with self.lock:
            self.connected = True
            self.last_message = time.time()
            self.last_ping = time.time()
            for symbol in self.symbols:
                symbol.on_connect()
        threading.Thread(target=self.ping_thread, daemon=True).start()

    def ping_thread(self):
        while self.running:
            time.sleep(1)
            now = time.time()
            with self.lock:
                if self.connected and now - self.last_ping >= self.config["ping_interval"]:
                    try:
                        self.ws.send('{"op": "ping"}')
                        logger.debug("Sent ping")
                        self.last_ping = now
                    except Exception as e:
                        logger.error(f"Ping error: {e}")
                        self.error_count += 1

    def check_heartbeat(self):
        while self.running:
            time.sleep(5)
            with self.lock:
                if self.connected and time.time() - self.last_message > self.config["heartbeat_timeout"]:
                    logger.warning("No messages received, triggering reconnect")
                    self.reconnect()

    def reconnect(self):
        with self.lock:
            if self.ws:
                try:
                    self.ws.close()
                except Exception as e:
                    logger.error(f"Error closing WebSocket for reconnect: {e}")
                    self.error_count += 1
            self.ws = None
            self.connected = False
            for symbol in self.symbols:
                for topic_type in symbol.subscribed_topics:
                    symbol.set_subscribed(topic_type, False)
                symbol.on_disconnect("WebSocket reconnection triggered")

    def on_message(self, ws, message):
        self.last_message = time.time()
        logger.debug(f"Received message: {message}")
        try:
            msg = json.loads(message)
            if msg.get("op") == "pong" or msg.get("ret_msg") == "pong":
                self.onPing(msg)
                return
            if msg.get("success") is False:
                self.onError(msg)
                return
            topic = msg.get("topic", "")
            if topic.startswith(("publicTrade.", "orderbook.", "kline.")):
                self.onData(msg)
            elif topic == "":
                if msg.get("op") == "subscribe" and msg.get("success"):
                    self.onSubscribe(msg)
                elif msg.get("op") == "unsubscribe" and msg.get("success"):
                    self.onUnsubscribe(msg)
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON: {message}")
            self.error_count += 1
        except Exception as e:
            logger.exception(f"Message processing error: {e}")
            self.error_count += 1

    def onPing(self, msg):
        logger.debug(f"Received pong: {msg}")

    def onError(self, msg):
        ret_msg = msg.get("ret_msg", "Unknown error")
        ret_code = msg.get("ret_code", 0)
        logger.error(f"Server error [code={ret_code}]: {ret_msg}")
        self.error_count += 1
        if ret_code in [10001, 10002]:
            logger.warning("Rate limit or auth error, slowing down reconnect")
            time.sleep(10)

    def onData(self, msg):
        topic = msg.get("topic", "")
        parts = topic.split(".")
        topic_type = parts[0] if parts[0] != "orderbook" else "orderbook"
        symbol_name = parts[-1]
        symbol = self.symbol_map.get(symbol_name)
        if not symbol:
            logger.warning(f"Received data for unknown symbol: {symbol_name}")
            return
        with self.lock:
            data = msg.get("data", [])
            try:
                if topic_type == "publicTrade":
                    for trade in data:
                        if len(symbol.trade_buffer) > 10000:  # Circuit breaker
                            logger.warning(f"[{symbol_name}] Buffer overflow, pausing 1s")
                            time.sleep(1)
                            continue
                        trade_data = {
                            "symbol": symbol_name,
                            "price": float(trade["p"]),
                            "quantity": float(trade["v"]),
                            "side": trade["S"],
                            "trade_id": trade["i"],
                            "timestamp": trade["T"] / 1000.0,
                            "datetime": datetime.utcfromtimestamp(trade["T"] / 1000.0).isoformat()
                        }
                        symbol.process_trade(trade_data)
                        logger.debug(f"Delegated trade to {symbol_name}")
                elif topic_type == "orderbook":
                    orderbook_data = {
                        "symbol": symbol_name,
                        "bids": data.get("b", []),
                        "asks": data.get("a", []),
                        "timestamp": datetime.utcfromtimestamp(data["ts"] / 1000.0).isoformat()
                    }
                    symbol.process_orderbook(orderbook_data)
                    logger.debug(f"Delegated orderbook to {symbol_name}")
                elif topic_type == "kline":
                    kline_data = {
                        "symbol": symbol_name,
                        "open": float(data["o"]),
                        "high": float(data["h"]),
                        "low": float(data["l"]),
                        "close": float(data["c"]),
                        "volume": float(data["v"]),
                        "timestamp": datetime.utcfromtimestamp(data["t"] / 1000.0).isoformat()
                    }
                    symbol.process_kline(kline_data)
                    logger.debug(f"Delegated kline to {symbol_name}")
            except Exception as e:
                logger.error(f"Error processing {topic_type} for {symbol_name}: {e}, partial data: {str(msg)[:50]}")
                self.error_count += 1

    def onSubscribe(self, msg):
        args = msg.get("args", [])
        for arg in args:
            parts = arg.split(".")
            topic_type = parts[0] if parts[0] != "orderbook" else "orderbook"
            symbol_name = parts[-1]
            symbol = self.symbol_map.get(symbol_name)
            if symbol:
                symbol.set_subscribed(topic_type, True)
                self.subscription_status[arg] = True
                logger.info(f"Subscribed to {arg}")
            else:
                logger.warning(f"Subscription for unknown symbol: {symbol_name}")

    def onUnsubscribe(self, msg):
        args = msg.get("args", [])
        for arg in args:
            parts = arg.split(".")
            topic_type = parts[0] if parts[0] != "orderbook" else "orderbook"
            symbol_name = parts[-1]
            symbol = self.symbol_map.get(symbol_name)
            if symbol:
                symbol.set_subscribed(topic_type, False)
                self.subscription_status[arg] = False
                self.topic_count -= 1
                logger.info(f"Unsubscribed from {arg}")
            else:
                logger.warning(f"Unsubscription for unknown symbol: {symbol_name}")

    def on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}")
        self.error_count += 1

    def on_close(self, ws, code, reason):
        logger.info(f"WebSocket closed: code={code}, reason={reason}")
        with self.lock:
            self.connected = False
            for symbol in self.symbols:
                for topic_type in symbol.subscribed_topics:
                    symbol.set_subscribed(topic_type, False)
                symbol.on_disconnect("WebSocket connection closed")

    def get_status(self):
        with self.lock:
            memory_usage_mb = (self.topic_count * 0.001)
            if memory_usage_mb > MEMORY_THRESHOLD_MB:
                logger.warning(f"Client memory usage: {memory_usage_mb:.2f}MB exceeds {MEMORY_THRESHOLD_MB}MB")
            return {
                "connected": self.connected,
                "subscribed_symbols": {s.symbol: s.subscribed_topics.copy() for s in self.symbols},
                "topic_count": self.topic_count,
                "last_message_time": datetime.utcfromtimestamp(self.last_message).isoformat(),
                "error_count": self.error_count,
                "subscription_status": self.subscription_status.copy(),
                "memory_usage_mb": memory_usage_mb,
                "last_checksum": self.last_checksum,
                "commit_hash": self.commit_hash
            }

class WebSocketManager:
    clients: List[BybitWebSocketClient] = []
    running = False

    @classmethod
    def start(cls):
        cls.running = True
        logger.info("WebSocketManager started")

    @classmethod
    def stop(cls):
        cls.running = False
        for client in cls.clients:
            client.close()
        cls.clients.clear()
        logger.info("WebSocketManager stopped")

    @classmethod
    def add_symbol(cls, symbol: 'Symbol') -> bool:
        config = {
            "ping_interval": PING_INTERVAL,
            "reconnect_delay": RECONNECT_DELAY,
            "max_reconnect_delay": MAX_RECONNECT_DELAY,
            "heartbeat_timeout": HEARTBEAT_TIMEOUT,
            "max_topics": MAX_TOPICS,
            "max_memory_mb": MAX_MEMORY_MB
        }
        for client in cls.clients:
            if client.topic_count < client.config["max_topics"]:
                if client.add_symbol(symbol):
                    return True
        new_client = BybitWebSocketClient(WS_URL, config=config)
        new_client.start()
        cls.clients.append(new_client)
        return new_client.add_symbol(symbol)

    @classmethod
    def remove_symbol(cls, symbol: 'Symbol'):
        for client in cls.clients:
            if symbol.symbol in client.symbol_map:
                client.remove_symbol(symbol)
                if not client.symbols:
                    client.close()
                    cls.clients.remove(client)
                break

    @classmethod
    def restart_symbol(cls, symbol_name: str):
        with threading.Lock():
            for client in cls.clients:
                symbol = client.symbol_map.get(symbol_name)
                if symbol:
                    client.remove_symbol(symbol)
                    cls.add_symbol(symbol)
                    logger.info(f"Restarted symbol: {symbol_name}")
                    break

    @classmethod
    def get_status(cls):
        return [client.get_status() for client in cls.clients]