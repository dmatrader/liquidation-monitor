import csv
import json
import threading
import time
from collections import deque, defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Deque, Dict, Iterable, List, Optional, Tuple

from flask import Flask, Response, jsonify, request
from flask_cors import CORS
import asyncio
import websockets
from urllib.parse import urlparse, parse_qs
import re


# -----------------------------
# 配置
# -----------------------------

CSV_BA_PATH = "liquidation_ba.csv"
CSV_OKX_PATH = "liquidation_okx.csv"

# 24 或 48 小时可调
RETENTION_HOURS_DEFAULT = 48

# SSE 推送频率（秒）
SSE_PUSH_INTERVAL_SEC = 1.0

# /latest_liquidations 返回的条目数
LATEST_LIST_SIZE = 50

# 聚合时间窗口（分钟）
AGG_WINDOWS_MINUTES = [3, 15, 60, 240, 1440]


# -----------------------------
# 数据结构
# -----------------------------


@dataclass
class LiquidationEvent:
    timestamp: datetime
    symbol: str  # 基础币，例如 BTC
    exchange: str  # "Binance" 或 "OKX"
    price: float
    direction: str  # "多头爆仓" 或 "空头爆仓"
    amount: float  # 改为 float 保留精度

    def to_public_dict(self) -> Dict:
        return {
            "datetime": self.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": self.symbol,
            "exchange": self.exchange,
            "price": self.price,
            "direction": self.direction,
            "amount": self.amount,
        }


# -----------------------------
# 全局状态（内存滚动窗口）
# -----------------------------


class EventStore:
    def __init__(self, retention_hours: int = RETENTION_HOURS_DEFAULT) -> None:
        self.retention = timedelta(hours=retention_hours)
        self.events: Deque[LiquidationEvent] = deque()
        self.lock = threading.RLock()
        self.last_seen_per_exchange: Dict[str, Optional[datetime]] = {"Binance": None, "OKX": None}

    def append(self, event: LiquidationEvent) -> None:
        with self.lock:
            self.events.append(event)
            self.last_seen_per_exchange[event.exchange] = event.timestamp
            self._evict_older_than(event.timestamp - self.retention)
        # 事件入库后通知 WS Broker
        try:
            if _WS_BROKER is not None:
                _WS_BROKER.notify(event)
        except Exception:
            pass

    def _evict_older_than(self, threshold: datetime) -> None:
        while self.events and self.events[0].timestamp < threshold:
            self.events.popleft()

    def prune(self) -> None:
        with self.lock:
            now = datetime.utcnow() + timedelta(hours=8)  # 维持北京时间语义
            self._evict_older_than(now - self.retention)

    def list_latest(self, limit: int = LATEST_LIST_SIZE) -> List[Dict]:
        with self.lock:
            return [e.to_public_dict() for e in list(self.events)[-limit:]]

    def iter_since(self, since_ts: datetime) -> Iterable[LiquidationEvent]:
        with self.lock:
            for e in self.events:
                if e.timestamp > since_ts:
                    yield e

    def query(self,
              since: Optional[datetime] = None,
              until: Optional[datetime] = None,
              symbols: Optional[List[str]] = None,
              exchanges: Optional[List[str]] = None,
              directions: Optional[List[str]] = None,
              limit: Optional[int] = None) -> List[Dict]:
        with self.lock:
            items: List[LiquidationEvent] = []
            for e in self.events:
                if since and e.timestamp < since:
                    continue
                if until and e.timestamp > until:
                    continue
                if symbols and e.symbol not in symbols:
                    continue
                if exchanges and e.exchange not in exchanges:
                    continue
                if directions and e.direction not in directions:
                    continue
                items.append(e)
            if limit is not None:
                items = items[-limit:]
            return [e.to_public_dict() for e in items]

    def aggregates(self) -> Dict[int, Dict]:
        with self.lock:
            now = datetime.utcnow() + timedelta(hours=8)
            results: Dict[int, Dict] = {}
            for minutes in AGG_WINDOWS_MINUTES:
                window_start = now - timedelta(minutes=minutes)
                longs: Dict[str, float] = defaultdict(float)
                shorts: Dict[str, float] = defaultdict(float)
                binance_long_total = 0.0
                binance_short_total = 0.0
                okx_long_total = 0.0
                okx_short_total = 0.0
                for e in self.events:
                    if e.timestamp < window_start:
                        continue
                    if e.direction == "多头爆仓":
                        longs[e.symbol] += e.amount
                        if e.exchange in ("Binance", "BA", "币安"):
                            binance_long_total += e.amount
                        elif e.exchange == "OKX":
                            okx_long_total += e.amount
                    else:
                        shorts[e.symbol] += e.amount
                        if e.exchange in ("Binance", "BA", "币安"):
                            binance_short_total += e.amount
                        elif e.exchange == "OKX":
                            okx_short_total += e.amount

                top_long = dict(sorted(longs.items(), key=lambda kv: kv[1], reverse=True)[:10])
                top_short = dict(sorted(shorts.items(), key=lambda kv: kv[1], reverse=True)[:10])
                results[minutes] = {
                    "top_long": top_long,
                    "top_short": top_short,
                    "binance_long": float(binance_long_total),
                    "binance_short": float(binance_short_total),
                    "okx_long": float(okx_long_total),
                    "okx_short": float(okx_short_total),
                }
            return results


STORE = EventStore()
_WS_BROKER = None  # 运行时注入


# -----------------------------
# CSV 追踪（tail）线程
# -----------------------------


class CsvTailer(threading.Thread):
    def __init__(self, path: str, exchange_alias: str, polling_interval: float = 0.5) -> None:
        super().__init__(daemon=True)
        self.path = path
        self.exchange_alias = exchange_alias
        self.polling_interval = polling_interval
        self._stop = threading.Event()
        self._line_index = 0  # 已处理到的行号（包含表头）
        self._file_inode = None
        self._has_header_checked = False

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                self._read_new_lines()
            except Exception as exc:
                # 打印错误但不中断另一方的数据
                print(f"[Tailer:{self.exchange_alias}] error: {exc}")
            time.sleep(self.polling_interval)

    def _read_new_lines(self) -> None:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                inode = self._get_inode(f)
                if self._file_inode != inode:
                    # 文件轮转或首次打开，重置
                    self._file_inode = inode
                    self._line_index = 0
                    self._has_header_checked = False

                reader = csv.reader(f)
                last_index = self._line_index
                for idx, row in enumerate(reader):
                    # 跳过已处理行
                    if idx < self._line_index:
                        continue
                    # 首行表头检查
                    if not self._has_header_checked and idx == 0:
                        if row == ["时间", "币对", "交易所", "价格", "方向", "金额"]:
                            self._has_header_checked = True
                            last_index = idx + 1
                            continue
                        else:
                            self._has_header_checked = True
                    if len(row) != 6:
                        last_index = idx + 1
                        continue
                    self._ingest_row(row)
                    last_index = idx + 1
                self._line_index = last_index
        except FileNotFoundError:
            # 文件暂时不存在，不阻断
            pass

    @staticmethod
    def _get_inode(f) -> Tuple[int, int]:
        try:
            s = f.fileno()
            import os
            st = os.fstat(s)
            return st.st_ino, st.st_dev
        except Exception:
            return (0, 0)

    def _ingest_row(self, row: List[str]) -> None:
        # row: [时间, 币对, 交易所, 价格, 方向, 金额]
        try:
            ts = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
            symbol = row[1]
            # 统一去掉 USDT/USDC 后缀，按基础币聚合
            symbol = re.sub(r"(USDT|USDC)$", "", symbol, flags=re.I)
            ex = row[2]
            price = float(row[3])
            direction = row[4]
            amount = float(row[5])  # 保留浮点精度，不转 int

            # 统一交易所名
            if ex in ("币安", "BA"):
                exchange_name = "Binance"
            else:
                exchange_name = "OKX"

            # 移除错误的时间戳去重逻辑
            # _line_index 已经保证逐行前进，不会重复处理
            # 同一秒内有多条清算是正常现象，不应该被过滤

            event = LiquidationEvent(
                timestamp=ts,
                symbol=symbol,
                exchange=exchange_name,
                price=price,
                direction=direction,
                amount=amount,
            )
            STORE.append(event)
        except Exception as exc:
            print(f"[Tailer:{self.exchange_alias}] skip bad row {row}: {exc}")


def start_tailers() -> None:
    CsvTailer(CSV_BA_PATH, "Binance").start()
    CsvTailer(CSV_OKX_PATH, "OKX").start()


# -----------------------------
# 周期性清理线程
# -----------------------------


def start_pruner() -> None:
    def _loop() -> None:
        while True:
            try:
                STORE.prune()
            except Exception as exc:
                print(f"[Pruner] error: {exc}")
            time.sleep(30)

    t = threading.Thread(target=_loop, daemon=True)
    t.start()


# -----------------------------
# Flask 应用与 API
# -----------------------------


app = Flask(__name__)
CORS(app)


def _parse_datetime_param(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    value = value.strip()
    # 支持 epoch 秒、epoch 毫秒、或格式 "YYYY-MM-DD HH:MM:SS"
    try:
        if value.isdigit():
            iv = int(value)
            if iv > 1_000_000_000_000:  # ms
                iv = iv // 1000
            return datetime.utcfromtimestamp(iv) + timedelta(hours=8)
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


@app.route("/data")
def api_data():
    return jsonify(STORE.aggregates())


@app.route("/latest_liquidations")
def api_latest():
    return jsonify(STORE.list_latest(LATEST_LIST_SIZE))


@app.route("/history")
def api_history():
    since = _parse_datetime_param(request.args.get("since"))
    until = _parse_datetime_param(request.args.get("until"))
    symbols = request.args.get("symbols")
    exchanges = request.args.get("exchanges")
    directions = request.args.get("directions")
    limit = request.args.get("limit")

    symbol_list = [s.strip() for s in symbols.split(",")] if symbols else None
    exchange_list = [e.strip() for e in exchanges.split(",")] if exchanges else None
    direction_list = [d.strip() for d in directions.split(",")] if directions else None
    limit_int = int(limit) if (limit and limit.isdigit()) else None

    data = STORE.query(
        since=since,
        until=until,
        symbols=symbol_list,
        exchanges=exchange_list,
        directions=direction_list,
        limit=limit_int,
    )
    return jsonify(data)


@app.route("/symbol_stats")
def api_symbol_stats():
    # 返回每个 symbol 在各窗口的：
    # long_total, short_total, long_vwap, short_vwap
    # 可选过滤：symbols=BTC,ETH（大小写不敏感）
    symbols = request.args.get("symbols")
    symbol_filter = set(s.strip().upper() for s in symbols.split(",")) if symbols else None

    now = datetime.utcnow() + timedelta(hours=8)
    results: Dict[str, Dict[int, Dict[str, float]]] = {}

    with STORE.lock:
        items = list(STORE.events)
    for minutes in AGG_WINDOWS_MINUTES:
        window_start = now - timedelta(minutes=minutes)
        # 为每个 symbol 分开累计
        per_symbol = {}
        for e in items:
            if e.timestamp < window_start:
                continue
            sym = e.symbol.upper()
            if symbol_filter and sym not in symbol_filter:
                continue
            rec = per_symbol.get(sym)
            if rec is None:
                rec = {
                    "long_total": 0.0,
                    "short_total": 0.0,
                    "long_vwap_num": 0.0,   # 价格×金额 累计
                    "long_vwap_den": 0.0,   # 金额累计
                    "short_vwap_num": 0.0,
                    "short_vwap_den": 0.0,
                }
                per_symbol[sym] = rec
            if e.direction == "多头爆仓":
                rec["long_total"] += float(e.amount)
                rec["long_vwap_num"] += float(e.price) * float(e.amount)
                rec["long_vwap_den"] += float(e.amount)
            else:
                rec["short_total"] += float(e.amount)
                rec["short_vwap_num"] += float(e.price) * float(e.amount)
                rec["short_vwap_den"] += float(e.amount)

        # 输出化简：计算 vwap = sum(price*amount)/sum(amount)
        for sym, rec in per_symbol.items():
            long_vwap = rec["long_vwap_num"] / rec["long_vwap_den"] if rec["long_vwap_den"] > 0 else None
            short_vwap = rec["short_vwap_num"] / rec["short_vwap_den"] if rec["short_vwap_den"] > 0 else None
            # 写入结果树
            if sym not in results:
                results[sym] = {}
            results[sym][minutes] = {
                "long_total": rec["long_total"],
                "short_total": rec["short_total"],
                "long_vwap": long_vwap,
                "short_vwap": short_vwap,
            }

    return jsonify(results)


@app.route("/health")
def api_health():
    now = datetime.utcnow() + timedelta(hours=8)
    status = {}
    for ex in ("Binance", "OKX"):
        last = STORE.last_seen_per_exchange.get(ex)
        lag = (now - last).total_seconds() if last else None
        status[ex] = {
            "last_seen": last.strftime("%Y-%m-%d %H:%M:%S") if last else None,
            "lag_seconds": lag,
        }
    return jsonify(status)


@app.route("/stream")
def api_stream():
    # 可选筛选：symbols=BTC,ETH&exchanges=Binance,OKX&directions=多头爆仓,空头爆仓
    symbols = request.args.get("symbols")
    exchanges = request.args.get("exchanges")
    directions = request.args.get("directions")

    symbol_set = set(s.strip().upper() for s in symbols.split(",")) if symbols else None
    exchange_set = set(e.strip() for e in exchanges.split(",")) if exchanges else None
    direction_set = set(d.strip() for d in directions.split(",")) if directions else None

    def _event_stream():
        last_ts = datetime.utcnow() + timedelta(hours=8)
        # 初始订阅后以 1s 间隔推送新增
        while True:
            try:
                new_items = []
                for e in STORE.iter_since(last_ts):
                    if symbol_set and e.symbol.upper() not in symbol_set:
                        continue
                    if exchange_set and e.exchange not in exchange_set:
                        continue
                    if direction_set and e.direction not in direction_set:
                        continue
                    new_items.append(e.to_public_dict())
                if new_items:
                    # 更新游标到最新事件时间
                    last_ts = datetime.strptime(new_items[-1]["datetime"], "%Y-%m-%d %H:%M:%S")
                    payload = json.dumps(new_items, ensure_ascii=False)
                    yield f"data: {payload}\n\n"
                else:
                    # 发送注释 keep-alive，避免中间设备断开
                    yield ": keep-alive\n\n"
            except GeneratorExit:
                break
            except Exception as exc:
                # 输出错误并继续
                print(f"[SSE] error: {exc}")
            time.sleep(SSE_PUSH_INTERVAL_SEC)

    headers = {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return Response(_event_stream(), headers=headers)


# -----------------------------
# WebSocket 推送（原生 websockets，端口 6681）
# -----------------------------


class WsBroker:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        # 订阅者：symbol -> set of websocket
        self.symbol_to_clients: Dict[str, set] = defaultdict(set)

    def notify(self, event: LiquidationEvent) -> None:
        # 将事件推送给订阅该 symbol 的客户端
        # 在独立线程中调度到 asyncio 循环
        data = event.to_public_dict()
        message = json.dumps(data, ensure_ascii=False)
        symbol_upper = event.symbol.upper()
        with self.lock:
            targets = list(self.symbol_to_clients.get(symbol_upper, []))
        for ws in targets:
            try:
                # 跳过已关闭连接并从订阅中移除
                if getattr(ws, "closed", False):
                    with self.lock:
                        self.symbol_to_clients[symbol_upper].discard(ws)
                    continue
                asyncio.run_coroutine_threadsafe(ws.send(message), ws.loop)
            except Exception:
                pass

    async def handler(self, websocket):
        # 解析订阅参数：/ws?symbols=BTC,ETH；如无参数，则等待首条消息作为订阅指令
        symbols: List[str] = []
        # 兼容不同 websockets 版本的路径属性
        path = None
        try:
            path = getattr(websocket, "path", None)
            if not path:
                req = getattr(websocket, "request", None)
                if req is not None:
                    path = getattr(req, "path", None)
        except Exception:
            path = None

        try:
            if path:
                query = parse_qs(urlparse(path).query)
                symbols_param = query.get("symbols", [""])[0]
                symbols = [s.strip().upper() for s in symbols_param.split(",") if s.strip()]
        except Exception:
            symbols = []

        if not symbols:
            # 等待客户端首条消息，支持纯逗号分隔字符串或JSON: {"symbols":"BTC,ETH"}
            try:
                first_msg = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                try:
                    obj = json.loads(first_msg)
                    val = obj.get("symbols", "") if isinstance(obj, dict) else ""
                    if val:
                        symbols = [s.strip().upper() for s in val.split(",") if s.strip()]
                except Exception:
                    # 非JSON，则按逗号分隔解析
                    symbols = [s.strip().upper() for s in str(first_msg).split(",") if s.strip()]
            except Exception:
                symbols = []

        if not symbols:
            await websocket.send(json.dumps({"error": "symbols required"}, ensure_ascii=False))
            await websocket.close()
            return

        # 注册订阅
        websocket.loop = asyncio.get_running_loop()
        with self.lock:
            for s in symbols:
                self.symbol_to_clients[s].add(websocket)
        try:
            async for _ in websocket:
                # 忽略客户端消息（可用作心跳）
                pass
        except Exception:
            # 客户端异常断开（如无关闭帧）时安静处理
            pass
        finally:
            with self.lock:
                for s in symbols:
                    self.symbol_to_clients[s].discard(websocket)


def start_ws_server() -> None:
    def _run() -> None:
        async def main():
            broker = WsBroker()
            global _WS_BROKER
            _WS_BROKER = broker
            async with websockets.serve(broker.handler, "0.0.0.0", 6681, ping_interval=20, ping_timeout=20):
                await asyncio.Future()  # run forever

        asyncio.run(main())

    t = threading.Thread(target=_run, daemon=True)
    t.start()


def _initial_load_from_csv(path: str) -> List[LiquidationEvent]:
    events: List[LiquidationEvent] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header_checked = False
            for row in reader:
                if not header_checked:
                    header_checked = True
                    if row == ["时间", "币对", "交易所", "价格", "方向", "金额"]:
                        continue
                if len(row) != 6:
                    continue
                try:
                    ts = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
                    symbol = row[1]
                    symbol = re.sub(r"(USDT|USDC)$", "", symbol, flags=re.I)
                    ex = row[2]
                    price = float(row[3])
                    direction = row[4]
                    amount = float(row[5])  # 保留浮点精度，不转 int
                    exchange_name = "Binance" if ex in ("币安", "BA") else "OKX"
                    events.append(
                        LiquidationEvent(
                            timestamp=ts,
                            symbol=symbol,
                            exchange=exchange_name,
                            price=price,
                            direction=direction,
                            amount=amount,
                        )
                    )
                except Exception:
                    continue
    except FileNotFoundError:
        pass
    return events


def initial_bootstrap(retention_hours: int = RETENTION_HOURS_DEFAULT) -> None:
    cutoff = (datetime.utcnow() + timedelta(hours=8)) - timedelta(hours=retention_hours)
    for e in _initial_load_from_csv(CSV_BA_PATH):
        if e.timestamp >= cutoff:
            STORE.append(e)
    for e in _initial_load_from_csv(CSV_OKX_PATH):
        if e.timestamp >= cutoff:
            STORE.append(e)


if __name__ == "__main__":
    # 启动顺序：先加载最近N小时历史，再启动追踪与清理线程，再启动API
    initial_bootstrap(RETENTION_HOURS_DEFAULT)
    start_tailers()
    start_pruner()
    start_ws_server()
    app.run(host="0.0.0.0", port=6680, debug=False, threaded=True)


