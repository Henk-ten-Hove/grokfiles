# main.py
import json
import os
import threading
import queue
import socket
import logging
import signal
import sys
import hashlib
import asyncio
import websockets
from datetime import datetime, timezone
from typing import Dict, List
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import psutil
from logging.handlers import RotatingFileHandler
from bybit_websocket import WebSocketManager
from symbol import Symbol
from utils import setup_logging, check_network, validate_code, archive_files
from queue import PriorityQueue

# Config
VERSION = "2.0.0"
DATA_DIR = "C:/develop/trunk/python/tamagochi/trades"
SYMBOLS_FILE = os.path.join(DATA_DIR, "symbols.json")
LOG_DIR = os.path.join(DATA_DIR, "log")
ARCHIVE_DIR = os.path.join(DATA_DIR, "archive")
TCP_PORT = 9999
WEBSOCKET_PORT = 8765
ALLOWED_CORS_ORIGINS = ["http://localhost:3000", "http://localhost:5173", "http://127.0.0.1:8000"]
MAX_COINS = 200
RECONNECT_TIMEOUT = int(os.getenv("RECONNECT_TIMEOUT", 30))
COINS_PER_WEBSOCKET = 50
ACTION_TIMEOUT = 30
RETENTION_DAYS = 3
METRICS_LOG = os.path.join(LOG_DIR, "metrics.log")
MAX_QUEUE_SIZE = 100
MAX_WEBSOCKET_CLIENTS = 100
LOG_MAX_BYTES = 10 * 1024 * 1024
LOG_BACKUP_COUNT = 5

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log_handler = RotatingFileHandler(
    os.path.join(LOG_DIR, "log.txt"), maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT
)
log_handler.setFormatter(logging.Formatter(
    "%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
))
logger.addHandler(log_handler)
logger.addHandler(logging.StreamHandler())
logger.info(f"Bybit Trade Collector v{VERSION} started")

metrics_logger = logging.getLogger("metrics")
metrics_handler = RotatingFileHandler(
    METRICS_LOG, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT
)
metrics_handler.setFormatter(logging.Formatter(
    "%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
))
metrics_logger.addHandler(metrics_handler)
metrics_logger.setLevel(logging.INFO)

def compute_checksum(content: str, algorithm: str = "md5") -> str:
    checksum = hashlib.md5(content.encode("utf-8")).hexdigest() if algorithm == "md5" else hashlib.sha256(content.encode("utf-8")).hexdigest()
    logger.debug(f"Computed {algorithm.upper()} checksum: {checksum}")
    commit_hash = os.getenv("GITHUB_COMMIT_HASH", "none")
    logger.debug(f"Current commit hash: {commit_hash}")
    logger.debug(f"GitHub issue #1 posted for collaboration questions")
    logger.debug(f"Repositories: https://github.com/Henk-ten-Hove/grokfiles, https://github.com/Henk-ten-Hove/tamaserver")
    return checksum

def verify_checksum(content: str, expected_checksum: str, algorithm: str = "md5") -> bool:
    return compute_checksum(content, algorithm) == expected_checksum

app = FastAPI(title=f"Bybit Trade Collector v{VERSION}")
app.add_middleware(CORSMiddleware, allow_origins=ALLOWED_CORS_ORIGINS, allow_methods=["*"], allow_headers=["*"])

manager = WebSocketManager()
symbols = {}
action_queue = PriorityQueue()
shutdown_event = threading.Event()
clients = set()

async def data_server(websocket, path):
    if len(clients) >= MAX_WEBSOCKET_CLIENTS:
        logger.warning("Max WebSocket clients reached, rejecting connection")
        await websocket.close(code=1008, reason="Max clients reached")
        return
    retries = 3
    while retries > 0 and not shutdown_event.is_set():
        try:
            clients.add(websocket)
            async for message in websocket:
                logger.debug(f"Received client message: {message}")
                try:
                    if not message.strip():
                        logger.warning("Empty WebSocket message received, ignoring")
                        continue
                    try:
                        json.loads(message)
                    except json.JSONDecodeError:
                        logger.warning("Partial or malformed JSON fragment, attempting recovery")
                        message = message.strip() + '}'
                        json.loads(message)
                    except json.JSONDecodeError as e:
                        logger.warning(f"JSON parse failure: {e}, message: {message[:100]}... (length={len(message)}, line={e.lineno}, col={e.colno}, doc={e.doc[:50]}..., pos={e.pos}, msg={e.msg})")
                        continue
                except Exception as e:
                    logger.warning(f"WebSocket message error: {e}")
            break
        except websockets.exceptions.ConnectionClosed:
            logger.info("Client disconnected, retrying...")
            retries -= 1
            await asyncio.sleep(min(RECONNECT_TIMEOUT, 5 * (4 - retries)))
        finally:
            clients.discard(websocket)

def start_data_server():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    server = websockets.serve(data_server, "localhost", WEBSOCKET_PORT)
    loop.run_until_complete(server)
    loop.run_forever()

async def send_data(data: Dict):
    for client in clients.copy():
        try:
            await client.send(json.dumps(data))
        except Exception as e:
            logger.error(f"WebSocket send error: {e}")
            clients.discard(client)

def data_callback(data: Dict):
    asyncio.run_coroutine_threadsafe(send_data(data), asyncio.get_event_loop())

def save_symbols():
    with open(SYMBOLS_FILE, "w", encoding="utf-8") as f:
        json.dump(list(symbols.keys()), f)
    logger.debug(f"Saved symbols: {list(symbols.keys())}")

def load_symbols():
    try:
        if os.path.exists(SYMBOLS_FILE):
            with open(SYMBOLS_FILE, "r", encoding="utf-8") as f:
                symbols_list = json.load(f)
                valid_symbols = [
                    s.upper() for s in symbols_list
                    if isinstance(s, str) and s.upper().endswith("USDT")
                ]
                if not valid_symbols:
                    logger.warning("No valid symbols in symbols.json, using default")
                    return ["BTCUSDT"]
                return valid_symbols
    except Exception as e:
        logger.error(f"Failed to load symbols: {e}")
    return ["BTCUSDT"]

@app.get("/api/status")
async def get_status():
    status = []
    for symbol, s in symbols.items():
        s_status = s.get_status()
        s_status.update({
            "trade_throughput": s.trade_count / max((time.time() - s.start_time), 1),
            "connection_uptime": time.time() - s.start_time if s.get_status().get("active") else 0,
            "last_checksum": compute_checksum(open(__file__).read(), "md5"),
            "memory_usage_mb": s_status.get("memory_usage_mb", 0),
        })
        status.append(s_status)
    client_status = [
        {
            **c.get_status(),
            "memory_usage_mb": c.get_status().get("memory_usage_mb", 0)
        }
        for c in manager.clients
    ]
    process = psutil.Process()
    memory_usage = process.memory_info().rss / 1024 / 1024
    disk_free = psutil.disk_usage(DATA_DIR).free / 1024 / 1024 / 1024
    system_metrics = {
        "memory_usage_mb": memory_usage,
        "disk_free_gb": disk_free,
        "action_queue_size": action_queue.qsize(),
        "websocket_clients": len(clients),
        "commit_hash": os.getenv("GITHUB_COMMIT_HASH", "none"),
    }
    full_status = {
        "symbols": status,
        "clients": client_status,
        "system": system_metrics,
    }
    metrics_logger.info(f"Status: {json.dumps(full_status)}")
    return full_status

@app.get("/api/memory")
async def get_memory():
    process = psutil.Process()
    memory_usage = process.memory_info().rss / 1024 / 1024
    disk_free = psutil.disk_usage(DATA_DIR).free / 1024 / 1024 / 1024
    symbol_memory = [
        {
            "symbol": s.symbol,
            "buffer_size": s.get_status().get("buffer_size", 0),
            "memory_usage_mb": s.get_status().get("memory_usage_mb", 0)
        }
        for s in symbols.values()
    ]
    client_memory = [
        {
            "client_id": i,
            "topic_count": c.get_status().get("topic_count", 0),
            "memory_usage_mb": c.get_status().get("memory_usage_mb", 0)
        }
        for i, c in enumerate(manager.clients)
    ]
    metrics_logger.info(f"Memory: {json.dumps({'process_memory_mb': memory_usage, 'disk_free_gb': disk_free, 'symbols': symbol_memory, 'clients': client_memory, 'commit_hash': os.getenv('GITHUB_COMMIT_HASH', 'none')})}")
    return {
        "process_memory_mb": memory_usage,
        "disk_free_gb": disk_free,
        "symbols": symbol_memory,
        "clients": client_memory,
    }

@app.post("/api/reconnect/{symbol}")
async def reconnect_symbol(symbol: str):
    symbol = symbol.upper()
    if not symbol.endswith("USDT"):
        raise HTTPException(400, "Symbol must end with USDT")
    if symbol in symbols:
        manager.restart_symbol(symbol)
        return {"detail": f"{symbol} restarted"}
    raise HTTPException(404, "Unknown symbol")

@app.post("/api/add/{symbol}")
async def add_symbol(symbol: str):
    symbol = symbol.upper()
    if not symbol.endswith("USDT"):
        raise HTTPException(400, "Symbol must end with USDT")
    action_queue.put((2, ("add", symbol)))
    return {"detail": f"Queued addition of {symbol}"}

@app.post("/api/remove/{symbol}")
async def remove_symbol(symbol: str):
    symbol = symbol.upper()
    if not symbol.endswith("USDT"):
        raise HTTPException(400, "Symbol must end with USDT")
    q = queue.Queue()
    action_queue.put((1, ("remove", symbol, q)))
    if q.get(timeout=ACTION_TIMEOUT):
        return {"detail": f"{symbol} removed"}
    raise HTTPException(404, "Unknown symbol")

@app.get("/api/trades/{symbol}")
async def get_trades(symbol: str, after: float = 0):
    symbol = symbol.upper()
    if not symbol.endswith("USDT"):
        raise HTTPException(400, "Symbol must end with USDT")
    if symbol not in symbols:
        raise HTTPException(404, "Not subscribed")
    trades = symbols[symbol].get_trades(after)
    return {
        "coinpair": symbol,
        "status": "online" if symbols[symbol].get_status().get("active") else "offline",
        "last_update": max((t["timestamp"] for t in trades), default=None) if trades else None,
        "trades": trades,
        "count": len(trades),
    }

def process_actions():
    while not shutdown_event.is_set():
        try:
            if action_queue.qsize() > MAX_QUEUE_SIZE:
                logger.warning(f"Action queue size exceeded: {action_queue.qsize()} > {MAX_QUEUE_SIZE}")
            priority, (action, *args) = action_queue.get(timeout=1)
            symbol = args[0]
            q = args[-1] if isinstance(args[-1], queue.Queue) else None
            if action == "add":
                if symbol not in symbols and len(symbols) < MAX_COINS:
                    if not check_network():
                        logger.warning(f"[{symbol}] Cannot add: no network connection")
                        if q:
                            q.put(False)
                        continue
                    symbols[symbol] = Symbol(symbol, data_callback=data_callback, data_dir=DATA_DIR)
                    symbols[symbol].start()
                    if symbols[symbol].request_add(manager):
                        logger.info(f"[{symbol}] Added")
                        save_symbols()
                        if q:
                            q.put(True)
                    else:
                        logger.error(f"[{symbol}] Failed to add")
                        symbols[symbol].stop()
                        del symbols[symbol]
                        if q:
                            q.put(False)
            elif action == "remove":
                if symbol in symbols:
                    symbols[symbol].stop()
                    manager.remove_symbol(symbols[symbol])
                    del symbols[symbol]
                    save_symbols()
                    if q:
                        q.put(True)
                elif q:
                    q.put(False)
        except queue.Empty:
            continue
        except Exception as e:
            logger.error(f"Action processing error: {e}")
            if q:
                q.put(False)

def tcp_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server_socket.bind(("localhost", TCP_PORT))
        server_socket.listen(5)
        logger.info(f"TCP server started on port {TCP_PORT}")
        while not shutdown_event.is_set():
            server_socket.settimeout(1.0)
            try:
                client_socket, _ = server_socket.accept()
                data = client_socket.recv(16384).decode("utf-8")
                if not data:
                    client_socket.send(json.dumps({"error": "No data received"}).encode("utf-8"))
                    continue
                request = json.loads(data)
                cmd = request.get("command")
                symbol = request.get("coinpair", "").upper()
                response = {}
                if not symbol or not symbol.endswith("USDT"):
                    response = {"error": "Invalid or missing coinpair"}
                elif cmd == "add_coinpair":
                    action_queue.put((2, ("add", symbol)))
                    response = {"status": f"Queued {symbol}"}
                elif cmd == "remove_coinpair":
                    q = queue.Queue()
                    action_queue.put((1, ("remove", symbol, q)))
                    response = {"status": "Unsubscribed" if q.get(timeout=ACTION_TIMEOUT) else "Unknown coinpair"}
                elif cmd == "get_trades":
                    after = float(request.get("timestamp", 0))
                    trades = symbols[symbol].get_trades(after) if symbol in symbols else []
                    response = {
                        "coinpair": symbol,
                        "trades": trades,
                        "count": len(trades),
                        "status": "online" if symbol in symbols and symbols[symbol].get_status().get("active") else "offline",
                        "last_update": max((t["timestamp"] for t in trades), default=None) if trades else None,
                    }
                else:
                    response = {"error": f"Unknown command: {cmd}"}
                client_socket.send((json.dumps(response) + "\n").encode("utf-8"))
                client_socket.close()
            except socket.timeout:
                continue
            except Exception as e:
                logger.error(f"TCP server error: {e}")
    except Exception as e:
        logger.error(f"Failed to start TCP server: {e}")
    finally:
        server_socket.close()

def archive_task():
    while not shutdown_event.is_set():
        archive_files(DATA_DIR, ARCHIVE_DIR, RETENTION_DAYS)
        time.sleep(24 * 3600)

def signal_handler(sig, frame):
    logger.info("Shutting down...")
    shutdown_event.set()
    manager.stop()
    for symbol in symbols.values():
        symbol.stop()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def print_startup_banner(active_symbols: List[str]):
    banner = "\n" + "="*58 + "\n"
    banner += f"  Bybit Trade Collector v{VERSION} - STARTED\n"
    banner += "-"*58 + "\n"
    banner += f"  Data directory   : {DATA_DIR}\n"
    banner += f"  Log file         : {os.path.join(LOG_DIR, 'log.txt')}\n"
    banner += f"  TCP port         : {TCP_PORT}\n"
    banner += f"  WebSocket port   : {WEBSOCKET_PORT}\n"
    banner += f"  Active symbols   : {', '.join(active_symbols)}\n"
    banner += f"  HTTP API         : http://localhost:8000/api/status\n"
    banner += "="*58 + "\n"
    print(banner)
    logger.info(banner.strip())

if __name__ == "__main__":
    validate_code(__file__)
    active_symbols = load_symbols()
    for symbol in active_symbols:
        if len(symbols) < MAX_COINS:
            if not check_network():
                logger.warning(f"[{symbol}] Cannot add: no network connection")
                continue
            symbols[symbol] = Symbol(symbol, data_callback=data_callback, data_dir=DATA_DIR)
            symbols[symbol].start()
            if not symbols[symbol].request_add(manager):
                logger.error(f"[{symbol}] Failed to add")
                symbols[symbol].stop()
                del symbols[symbol]
    print_startup_banner(list(symbols.keys()))
    manager.start()
    threading.Thread(target=process_actions, daemon=True).start()
    threading.Thread(target=tcp_server, daemon=True).start()
    threading.Thread(target=start_data_server, daemon=True).start()
    threading.Thread(target=archive_task, daemon=True).start()
    uvicorn.run(app, host="localhost", port=8000)