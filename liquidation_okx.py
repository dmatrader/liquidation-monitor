import csv
import json
import websockets
import asyncio
from datetime import datetime, timedelta
import os
import aiohttp

# CSV 文件的名称
csv_file = "liquidation_okx.csv"

# 缓存文件的名称
cache_file = "okx张币转换.json"

# 设定阈值参数（单位：USDT）
threshold = 10  # 例如，只记录金额大于等于10 USDT 的爆仓订单

# 统计计数器
event_counter = {
    'total_received': 0,
    'written': 0,
    'filtered': 0,
    'conversion_failed': 0,
    'api_calls': 0,
    'cache_hits': 0
}

# 检查 CSV 文件是否存在，如果不存在则创建并写入头
if not os.path.exists(csv_file):
    with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["时间", "币对", "交易所", "价格", "方向", "金额"])

# 读取缓存文件（永久缓存，因为合约面值是固定的）
conversion_cache = {}
if os.path.exists(cache_file):
    with open(cache_file, 'r', encoding='utf-8') as file:
        conversion_cache = json.load(file)
    print(f"[NEW] 加载张币转换缓存: {len(conversion_cache)} 个合约")

# CSV 写入锁（防止并发写入冲突）
csv_lock = asyncio.Lock()

# API 调用信号量（限制并发API调用，避免被限流）
api_semaphore = asyncio.Semaphore(2)  # 同时最多2个API请求

# 张币转换函数（带缓存、重试、限流）
async def convert_contract_coin(session, instId, sz, px, type="2"):
    """
    OKX 合约面值是固定的（如 BTC-USDT-SWAP 每张 = 0.01 BTC）
    所以转换比例可以永久缓存，只需要请求一次
    """
    # 优先使用缓存
    if instId in conversion_cache:
        event_counter['cache_hits'] += 1
        conversion_ratio = conversion_cache[instId]
        converted_sz = float(sz) * conversion_ratio
        return converted_sz
    
    # 缓存未命中，调用 API（带限流和重试）
    async with api_semaphore:  # 限制并发API调用
        event_counter['api_calls'] += 1
        
        for retry in range(3):  # 最多重试3次
            try:
                url = "https://www.okx.com/api/v5/public/convert-contract-coin"
                params = {
                    "type": type,  # 2：张转币
                    "instId": instId,
                    "sz": sz,
                    "px": px
                }
                
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data['code'] == '0' and len(data['data']) > 0:
                            # 计算转换比例（币 / 张）
                            conversion_ratio = float(data['data'][0]['sz']) / float(sz)
                            
                            # 永久缓存到内存和文件
                            conversion_cache[instId] = conversion_ratio
                            async with csv_lock:  # 文件写入也要加锁
                                with open(cache_file, 'w', encoding='utf-8') as file:
                                    json.dump(conversion_cache, file, indent=2)
                            
                            converted_sz = float(sz) * conversion_ratio
                            print(f"[NEW] 新合约缓存: {instId} → 每张={conversion_ratio:.8f}币 (API调用次数: {event_counter['api_calls']})")
                            return converted_sz
                        else:
                            print(f"[NEW] API返回错误: code={data.get('code')}, msg={data.get('msg')}")
                    elif response.status == 429:
                        print(f"[NEW] API限流 (429)，等待后重试...")
                        await asyncio.sleep(2)  # 被限流，等待更久
                        continue
                    else:
                        print(f"[NEW] API请求失败: status={response.status}")
                
            except asyncio.TimeoutError:
                print(f"[NEW] API超时，重试 {retry+1}/3")
            except Exception as e:
                print(f"[NEW] API异常: {e}，重试 {retry+1}/3")
            
            # 重试前等待
            if retry < 2:
                await asyncio.sleep(0.5 * (retry + 1))  # 递增等待时间
        
        # 3次重试全部失败
        event_counter['conversion_failed'] += 1
        print(f"[NEW] 张币转换失败3次: {instId} (累计失败: {event_counter['conversion_failed']})")
        return None

# 处理单个清算明细
async def handle_detail(session, instId, detail):
    try:
        event_time = datetime.utcfromtimestamp(int(detail['ts']) / 1000) + timedelta(hours=8)
        event_time_str = event_time.strftime('%Y-%m-%d %H:%M:%S')
        
        base_symbol = instId.split('-')[0]  # BTC-USDT-SWAP → BTC
        side = detail['side']
        pos_side = detail['posSide']
        bk_px = detail.get('bkPx', '')  # 破产价格
        sz = detail['sz']  # 张数
        
        # 价格缺失检查
        if not bk_px or bk_px == '':
            print(f"[NEW] 价格缺失，跳过: instId={instId}, detail={detail}")
            return
        
        try:
            bk_px = float(bk_px)
        except (ValueError, TypeError):
            print(f"[NEW] 价格无效: bkPx={bk_px}, instId={instId}")
            return
        
        event_counter['total_received'] += 1
        
        # 进行张币转换（优先使用缓存）
        converted_sz = await convert_contract_coin(session, instId, sz, bk_px)
        if converted_sz is None:
            return  # 转换失败，已记录日志
        
        # 计算金额（保留整数即可，用户说精度不重要）
        amount = round(float(converted_sz) * float(bk_px))
        
        # 严谨的方向判定
        if side == "sell" and pos_side == "long":
            direction = "多头爆仓"
        elif side == "buy" and pos_side == "short":
            direction = "空头爆仓"
        else:
            # 异常组合，记录并跳过
            print(f"[NEW] 未知方向组合: side={side}, pos_side={pos_side}, instId={instId}")
            return
        
        # 阈值过滤
        if amount >= threshold:
            # 使用锁保护 CSV 写入，防止并发写入导致文件损坏
            async with csv_lock:
                with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    writer.writerow([event_time_str, base_symbol, "OKX", bk_px, direction, amount])
            
            event_counter['written'] += 1
            print(f"[NEW] 写入: {event_time_str} {base_symbol} {direction} ${amount:,} | 统计: 收={event_counter['total_received']}, 写={event_counter['written']}, 缓存命中={event_counter['cache_hits']}")
        else:
            event_counter['filtered'] += 1
            if event_counter['filtered'] % 50 == 0:  # 每50条打印一次
                print(f"[NEW] 已过滤小额订单 {event_counter['filtered']} 条")
    
    except Exception as e:
        print(f"[NEW] 处理明细时出错: {e}, detail={detail}")

# 处理 WebSocket 消息（改为同步处理，避免竞态）
async def handle_message(session, message):
    try:
        data = json.loads(message)
        
        # 跳过订阅确认等非数据消息
        if 'event' in data:
            if data['event'] == 'subscribe':
                print(f"[NEW] 订阅成功: {data.get('arg', {})}")
            return
        
        # 处理清算数据
        if 'data' in data:
            for item in data['data']:
                instId = item['instId']
                if 'details' in item:
                    for detail in item['details']:
                        # 同步处理每个明细，避免并发写入冲突
                        await handle_detail(session, instId, detail)
    
    except json.JSONDecodeError as e:
        print(f"[NEW] JSON解析错误: {e}")
    except Exception as e:
        print(f"[NEW] 处理消息时出错: {e}")

# 监听 WebSocket
async def listen():
    url = "wss://ws.okx.com:8443/ws/v5/public"
    reconnect_count = 0
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                # 禁用 websockets 库的自动 ping，使用 OKX 的应用层文本 ping
                async with websockets.connect(url, ping_interval=None, ping_timeout=None) as websocket:
                    print(f"[NEW] WebSocket 连接成功 (重连次数: {reconnect_count})")
                    reconnect_count = 0
                    
                    # 订阅所有永续合约的清算信息
                    subscribe_message = {
                        "op": "subscribe",
                        "args": [
                            {
                                "channel": "liquidation-orders",
                                "instType": "SWAP"
                            }
                        ]
                    }
                    await websocket.send(json.dumps(subscribe_message))
                    print("[NEW] 已发送订阅请求")
                    
                    # 启动心跳协程（OKX 需要应用层文本 "ping"）
                    async def heartbeat():
                        while True:
                            await asyncio.sleep(25)  # 每25秒发送一次心跳
                            try:
                                await websocket.send("ping")
                                print("[NEW] 发送心跳 ping")
                            except Exception as e:
                                print(f"[NEW] 心跳发送失败: {e}")
                                break
                    
                    heartbeat_task = asyncio.create_task(heartbeat())
                    
                    try:
                        # 接收消息
                        while True:
                            try:
                                message = await asyncio.wait_for(websocket.recv(), timeout=60)
                                
                                # OKX 使用文本 'pong' 响应 'ping'
                                if message == 'pong':
                                    print("[NEW] 收到 pong")
                                    continue
                                
                                # 同步处理消息（不使用 create_task，避免竞态）
                                await handle_message(session, message)
                            
                            except asyncio.TimeoutError:
                                print("[NEW] 60秒无消息，连接可能异常")
                                break
                    finally:
                        # 取消心跳任务
                        heartbeat_task.cancel()
                        try:
                            await heartbeat_task
                        except asyncio.CancelledError:
                            pass
                
            except websockets.exceptions.ConnectionClosed:
                reconnect_count += 1
                print(f"[NEW] WebSocket 连接关闭，5秒后重连 (第{reconnect_count}次)")
            except Exception as e:
                reconnect_count += 1
                print(f"[NEW] WebSocket 错误: {e}，5秒后重连 (第{reconnect_count}次)")
            
            await asyncio.sleep(5)

# 主函数
async def main():
    print("=" * 60)
    print("[NEW] OKX 清算数据监控 - 改进版本")
    print(f"[NEW] CSV文件: {csv_file}")
    print(f"[NEW] 缓存文件: {cache_file}")
    print(f"[NEW] 金额阈值: {threshold} USDT")
    print(f"[NEW] 已缓存合约: {len(conversion_cache)} 个")
    print("=" * 60)
    await listen()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n[NEW] 程序终止")
        print(f"[NEW] 统计: 收到={event_counter['total_received']}, 写入={event_counter['written']}, 过滤={event_counter['filtered']}")
        print(f"[NEW] API: 调用={event_counter['api_calls']}, 缓存命中={event_counter['cache_hits']}, 转换失败={event_counter['conversion_failed']}")

