import csv
import json
import websockets
import asyncio
from datetime import datetime, timedelta
import os
import re

# CSV 文件的名称
csv_file = "liquidation_ba.csv"

# 设定阈值参数（单位：USDT）
threshold = 10  # 例如，只记录金额大于等于10 USDT 的爆仓订单

# 统计计数器
event_counter = {
    'total_received': 0,
    'written': 0,
    'filtered': 0
}

# 检查 CSV 文件是否存在，如果不存在则创建并写入头
if not os.path.exists(csv_file):
    with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["时间", "币对", "交易所", "价格", "方向", "金额"])

# 定义处理单个清算事件的函数
async def handle_event(ev):
    try:
        event_time = datetime.utcfromtimestamp(ev['E'] / 1000) + timedelta(hours=8)  # 转换为北京时间
        event_time_str = event_time.strftime('%Y-%m-%d %H:%M:%S')
        
        o = ev['o']
        symbol = o['s']
        # 同时兼容 USDT/USDC 计价：去掉末尾后缀，仅保留基础币
        base_symbol = re.sub(r'(USDT|USDC)$', '', symbol, flags=re.I)
        side = o['S']  # 'BUY' / 'SELL'
        
        # 兜底处理不同字段名
        qty = float(o.get('q', o.get('l', 0)))
        price = float(o.get('ap', o.get('p', 0)))
        
        # 保留小数精度，不要截断
        amount = qty * price
        
        # 确定方向
        direction = "空头爆仓" if side == "BUY" else "多头爆仓"
        
        event_counter['total_received'] += 1
        
        # 只有当金额大于等于阈值时才写入 CSV 文件
        if amount >= threshold:
            # 将数据写入 CSV 文件，金额保留2位小数
            with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow([event_time_str, base_symbol, "BA", f"{price:.8f}", direction, f"{amount:.2f}"])
            
            event_counter['written'] += 1
            print(f"[NEW] 写入数据: 时间={event_time_str}, 币对={base_symbol}, 价格={price:.8f}, 方向={direction}, 金额={amount:.2f} | 统计: 总收={event_counter['total_received']}, 已写={event_counter['written']}, 过滤={event_counter['filtered']}")
        else:
            event_counter['filtered'] += 1
            if event_counter['filtered'] % 100 == 0:  # 每100条过滤的才打印一次
                print(f"[NEW] 已过滤小额订单 {event_counter['filtered']} 条 (最新: {base_symbol} {amount:.2f})")
    
    except Exception as e:
        print(f"[NEW] 处理事件时出错: {e}, 事件数据: {ev}")

# 定义监听 WebSocket 的函数
async def listen():
    url = "wss://fstream.binance.com/ws/!forceOrder@arr"
    reconnect_count = 0
    
    while True:
        try:
            # websockets 库会自动处理底层的 ping/pong 帧
            async with websockets.connect(url, ping_interval=30, ping_timeout=15) as ws:
                print(f"[NEW] WebSocket 连接成功 (重连次数: {reconnect_count})")
                reconnect_count = 0
                
                while True:
                    try:
                        # 设置接收超时
                        msg = await asyncio.wait_for(ws.recv(), timeout=180)
                        data = json.loads(msg)
                        
                        # !forceOrder@arr 返回的可能是数组，也可能偶尔是单个对象
                        # 统一处理成列表
                        if isinstance(data, list):
                            events = data
                        else:
                            events = [data]
                        
                        # 遍历处理每一个清算事件
                        for ev in events:
                            # 确保是清算订单事件（包含 'e' 和 'o' 字段）
                            if 'e' in ev and ev['e'] == 'forceOrder' and 'o' in ev:
                                await handle_event(ev)
                    
                    except asyncio.TimeoutError:
                        print(f"[NEW] 接收超时 (180秒无数据)，连接可能异常")
                        break
                    except websockets.exceptions.ConnectionClosed:
                        print("[NEW] WebSocket 连接关闭")
                        break
                    except json.JSONDecodeError as e:
                        print(f"[NEW] JSON 解析错误: {e}")
                        continue
                    except Exception as e:
                        print(f"[NEW] 处理消息时发生错误: {e}")
                        continue
        
        except Exception as e:
            reconnect_count += 1
            print(f"[NEW] 连接失败 (第 {reconnect_count} 次): {e}")
        
        print("[NEW] 等待 5 秒后重新连接...")
        await asyncio.sleep(5)

# 主函数
async def main():
    print("=" * 60)
    print("[NEW] 币安清算数据监控 - 改进版本")
    print(f"[NEW] CSV文件: {csv_file}")
    print(f"[NEW] 金额阈值: {threshold} USDT")
    print("=" * 60)
    await listen()

# 运行异步事件循环
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n[NEW] 程序终止. 总计: 收到={event_counter['total_received']}, 写入={event_counter['written']}, 过滤={event_counter['filtered']}")

