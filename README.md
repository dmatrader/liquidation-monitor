# 加密货币爆仓监控系统 / Cryptocurrency Liquidation Monitor

> 实时监控币安和OKX交易所的爆仓数据，提供Web仪表盘展示 | Real-time monitoring of liquidation data from Binance and OKX exchanges with web dashboard

## 📖 项目介绍 / Project Description

**中文**: 这是一个基于 Flask 和 WebSocket 的实时加密货币爆仓监控系统，支持币安和OKX两大交易所的爆仓数据收集、分析和可视化展示。系统提供实时爆仓流、累计统计、热门爆仓币种排行等功能，帮助交易者及时了解市场风险。

**⚠️ 重要提醒**: 币安清算数据存在低报现象（可能低估100倍），本系统数据仅供参考，实际清算规模可能远大于显示数据。

**English**: This is a real-time cryptocurrency liquidation monitoring system based on Flask and WebSocket, supporting liquidation data collection, analysis and visualization from Binance and OKX exchanges. The system provides real-time liquidation streams, cumulative statistics, and top liquidation coins ranking to help traders understand market risks.

**⚠️ Important Notice**: Binance liquidation data may be underreported (potentially by 100x), this system data is for reference only, actual liquidation scale may be much larger than displayed.

## 🚀 快速体验 / Quick Experience

**本地运行体验**: 按照下方使用说明在本地运行程序即可体验完整功能

## ✨ 功能特性 / Features

- 🔥 **实时爆仓流** - 实时显示最新的爆仓事件，支持筛选和过滤
- 📊 **多维度统计** - 3分钟、15分钟、1小时、4小时、1天的累计爆仓统计
- 🏆 **热门排行** - 实时显示多头和空头爆仓最多的币种排行
- 🎯 **双交易所支持** - 同时监控币安(Binance)和OKX交易所
- 📱 **响应式设计** - 完美支持桌面和移动设备
- ⚡ **高性能** - 基于WebSocket的实时数据推送

## 📖 使用说明 / Usage

### 启动系统
```bash
# 1. 启动数据收集服务
python liquidation_hub.py

# 2. 启动Web展示服务
python liquidation_analysis_web.py

# 3. 访问 http://localhost:5000 查看仪表盘
```

### 功能操作
- **筛选币对**: 在顶部输入框输入要监控的币种（如 BTC, ETH, SOL）
- **最小金额过滤**: 设置最小爆仓金额阈值，只显示大于该金额的爆仓
- **实时更新**: 数据每1秒自动更新，无需手动刷新

## 🎨 视觉说明 / Visual Guide

- 🔴 **红色文字**: 多头爆仓 (Long Liquidation)
- 🟢 **绿色文字**: 空头爆仓 (Short Liquidation)
- 📈 **累计统计**: 不同时间窗口的爆仓金额汇总
- 🏅 **排行榜**: 按爆仓金额排序的热门币种

## 🛠️ 技术栈 / Tech Stack

- **Flask** - Web框架和API服务
- **WebSocket** - 实时数据推送
- **Pandas** - 数据处理和分析
- **asyncio** - 异步数据处理
- **CSV** - 数据存储格式

## 📊 数据来源 / Data Source

- **币安合约**: WebSocket实时爆仓数据流
- **OKX合约**: WebSocket实时爆仓数据流
- **数据格式**: 时间、币对、交易所、价格、方向、金额
- **更新频率**: 实时推送，1秒更新间隔

### ⚠️ 重要数据说明 / Important Data Notice

**币安清算数据局限性**: 根据 [Hyperliquid CEO Jeff Yan 的批评](https://crypto.news/hyperliquid-ceo-questions-binances-liquidation-transparency/)，币安的清算数据流存在严重的**低报现象**：

- **数据窗口限制**: 币安以1000毫秒为窗口，每个窗口内只推送一笔最新清算订单
- **低估风险**: 如果同一毫秒内有多笔清算，其他清算会被忽略，可能导致实际清算数量被**低估高达100倍**
- **高波动时期**: 在市场高波动时期，这种低报现象更加严重
- **对比**: 相比之下，Hyperliquid等去中心化交易所的所有清算数据都是链上实时公开透明的

**使用建议**: 本系统展示的币安清算数据仅供参考，实际清算规模可能远大于显示数据，特别是在市场剧烈波动时期。

## 🔗 相关链接 / Related Links

- [币安注册邀请](https://www.binance.com/join?ref=MAPAMBQ1) - 手续费永久返还
- [币安合约官网](https://www.binance.com/zh-CN/futures)
- [OKX合约官网](https://www.okx.com/zh-hans/trade-futures/btc-usdt-swap)

## 📸 预览 / Preview

![爆仓监控仪表盘](https://github.com/dmatrader/liquidation-monitor/blob/master/screenshot.png)

## 📁 项目结构 / Project Structure

```
git-liquidation-monitor/
├── liquidation_hub.py          # 主服务：数据收集和API服务
├── liquidation_analysis_web.py # Web展示服务
├── liquidation_ba.py           # 币安数据收集器
├── liquidation_okx.py          # OKX数据收集器
├── liquidation_ba.csv          # 币安爆仓数据存储
├── liquidation_okx.csv         # OKX爆仓数据存储
└── README.md                   # 项目说明文档
```

## 🚀 快速开始 / Quick Start

1. **克隆项目**
```bash
git clone https://github.com/dmatrader/liquidation-monitor.git
cd liquidation-monitor
```

2. **安装依赖**
```bash
pip install flask flask-cors pandas websockets aiohttp
```

3. **启动服务**
```bash
# 终端1：启动数据收集
python liquidation_hub.py

# 终端2：启动Web服务
python liquidation_analysis_web.py
```

4. **访问仪表盘**
打开浏览器访问 `http://localhost:5000`

## ⚙️ 配置说明 / Configuration

### 主要参数
- `threshold`: 最小爆仓金额阈值（默认10 USDT）
- `RETENTION_HOURS_DEFAULT`: 数据保留时间（默认48小时）
- `SSE_PUSH_INTERVAL_SEC`: 推送频率（默认1秒）
- `LATEST_LIST_SIZE`: 最新爆仓显示数量（默认50条）

### 自定义配置
```python
# 在 liquidation_hub.py 中修改
CSV_BA_PATH = "liquidation_ba.csv"      # 币安数据文件
CSV_OKX_PATH = "liquidation_okx.csv"    # OKX数据文件
RETENTION_HOURS_DEFAULT = 48            # 数据保留时间
SSE_PUSH_INTERVAL_SEC = 1.0             # 推送频率
```

## 📄 许可证 / License

MIT License - 可自由使用和修改 | Free to use and modify

---

⭐ **如果觉得这个项目有帮助，请给个星标！**  
⭐ **If you find this project helpful, please give it a star!**
