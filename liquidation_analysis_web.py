from flask import Flask, render_template_string, jsonify
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import threading
import logging
import json
from flask_cors import CORS

# 设置日志级别
logging.basicConfig(level=logging.WARNING)
# 禁用 werkzeug 的日志
logging.getLogger('werkzeug').setLevel(logging.ERROR)
# 禁用 watchdog 的日志
logging.getLogger('watchdog').setLevel(logging.ERROR)

app = Flask(__name__)
CORS(app)  # 添加这行来启用 CORS

# 全局变量来存储最新的分析结果和最新的爆仓数据
latest_results = {}
latest_liquidations = []
last_processed_time = {'Binance': None, 'OKX': None}

def read_liquidation_file(filename):
    try:
        df = pd.read_csv(filename, parse_dates=['时间'])
        df.columns = ['datetime', 'symbol', 'exchange', 'price', 'direction', 'amount']
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df = df.dropna()
        return df
    except Exception as e:
        logging.error(f"Error reading file {filename}: {e}")
        return pd.DataFrame()

def analyze_liquidations(df, time_window):
    if df.empty:
        return pd.Series(), pd.Series(), 0, 0, 0, 0

    end_time = df['datetime'].max()
    start_time = end_time - timedelta(minutes=time_window)
    df_window = df[(df['datetime'] >= start_time) & (df['datetime'] <= end_time)]
    
    long_liquidations = df_window[df_window['direction'] == '多头爆仓']
    short_liquidations = df_window[df_window['direction'] == '空头爆仓']
    
    top_long = long_liquidations.groupby('symbol')['amount'].sum().nlargest(10)
    top_short = short_liquidations.groupby('symbol')['amount'].sum().nlargest(10)
    
    binance_long_total = long_liquidations[long_liquidations['exchange'].isin(['币安', 'BA'])]['amount'].sum()
    binance_short_total = short_liquidations[short_liquidations['exchange'].isin(['币安', 'BA'])]['amount'].sum()
    okx_long_total = long_liquidations[long_liquidations['exchange'] == 'OKX']['amount'].sum()
    okx_short_total = short_liquidations[short_liquidations['exchange'] == 'OKX']['amount'].sum()
    
    return top_long, top_short, binance_long_total, binance_short_total, okx_long_total, okx_short_total

def load_initial_data():
    global latest_liquidations
    df_binance = read_liquidation_file('liquidation_ba.csv')
    df_okx = read_liquidation_file('liquidation_okx.csv')
    
    df_combined = pd.concat([df_binance, df_okx]).sort_values('datetime', ascending=False)
    latest_10 = df_combined.head(10)
    
    for _, row in latest_10.iterrows():
        liquidation = row.to_dict()
        liquidation['exchange'] = 'Binance' if row['exchange'] in ['币安', 'BA'] else 'OKX'
        latest_liquidations.append(liquidation)
    
    latest_liquidations.reverse()  # Reverse to have oldest first

def update_data():
    global latest_results, latest_liquidations, last_processed_time
    while True:
        try:
            df_binance = read_liquidation_file('liquidation_ba.csv')
            df_okx = read_liquidation_file('liquidation_okx.csv')
            
            # 检查是否有新数据
            if not df_binance.empty:
                latest_ba = df_binance.iloc[-1]
                if last_processed_time['Binance'] is None or latest_ba['datetime'] > last_processed_time['Binance']:
                    liquidation = latest_ba.to_dict()
                    liquidation['exchange'] = 'Binance'
                    latest_liquidations.append(liquidation)
                    last_processed_time['Binance'] = latest_ba['datetime']
            
            if not df_okx.empty:
                latest_okx = df_okx.iloc[-1]
                if last_processed_time['OKX'] is None or latest_okx['datetime'] > last_processed_time['OKX']:
                    liquidation = latest_okx.to_dict()
                    liquidation['exchange'] = 'OKX'
                    latest_liquidations.append(liquidation)
                    last_processed_time['OKX'] = latest_okx['datetime']
            
            # 保持最新的1000条记录
            if len(latest_liquidations) > 1000:
                latest_liquidations = latest_liquidations[-1000:]
            
            df_combined = pd.concat([df_binance, df_okx])
            
            time_windows = [3, 15, 60]
            results = {}
            
            for window in time_windows:
                top_long, top_short, binance_long, binance_short, okx_long, okx_short = analyze_liquidations(df_combined, window)
                results[window] = {
                    'top_long': top_long.to_dict(),
                    'top_short': top_short.to_dict(),
                    'binance_long': float(binance_long),
                    'binance_short': float(binance_short),
                    'okx_long': float(okx_long),
                    'okx_short': float(okx_short)
                }
            
            latest_results = results
        except Exception as e:
            logging.error(f"Error updating data: {e}")
        time.sleep(1)  # 每秒更新一次

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        return super(NumpyEncoder, self).default(obj)

@app.route('/')
def index():
    return render_template_string("""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>强平 Liquidation Analysis</title>
        <style>
            body {
                background-color: #2e2e2e;
                color: #d3d3d3;
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 10px;
                font-size: 12px;
            }
            h1 {
                color: #d3d3d3;
                text-align: center;
                font-size: 20px;
                margin-bottom: 10px;
            }
            .container {
                display: flex;
                justify-content: space-between;
            }
            .section {
                width: 32%;
                background-color: #444444;
                padding: 10px;
                border-radius: 5px;
            }
            h2 {
                color: #3498db;
                font-size: 16px;
                margin-top: 0;
                margin-bottom: 5px;
            }
            table {
                border-collapse: collapse;
                width: 100%;
                margin-bottom: 10px;
            }
            th, td {
                border: 1px solid #555;
                padding: 4px;
                text-align: left;
            }
            th {
                background-color: #333;
                font-weight: bold;
            }
            #liquidation-orders {
                width: 100%;
                height: 200px;
                background-color: #444444;
                margin-bottom: 10px;
                overflow-y: auto;
                border-radius: 5px;
            }
            .liquidation-order {
                display: grid;
                grid-template-columns: 120px 100px 160px 160px 70px 70px 100px;
                align-items: center;
                padding: 3px;
                border-bottom: 1px solid #555;
                font-size: 14px;
            }
            .liquidation-order div {
                padding: 0 5px;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
            }
            .symbol-link {
                color: #3498db;
                text-decoration: none;
                cursor: pointer;
            }
            .symbol-link:hover {
                text-decoration: underline;
            }
        </style>
        <script>
            let lastProcessedId = null;

            function getAmountColor(amount) {
                if (amount >= 100000) return '#cfff4d';
                if (amount >= 10000) return '#41f505';
                if (amount >= 5000) return '#04d404';
                if (amount >= 1000) return '#00b300';
                return '#018f01';
            }

            function formatAmount(amount) {
                return Math.round(amount).toLocaleString('en-US');
            }

            function updateData() {
                fetch(window.location.pathname + 'data')
                    .then(response => response.json())
                    .then(data => {
                        for (let window in data) {
                            updateTable('long-' + window, data[window].top_long);
                            updateTable('short-' + window, data[window].top_short);
                            updateExchangeData(window, data[window]);
                        }
                    })
                    .catch(error => console.error('Error:', error));
                
                fetch(window.location.pathname + 'latest_liquidations')
                    .then(response => response.json())
                    .then(data => {
                        updateLiquidationOrders(data);
                    })
                    .catch(error => console.error('Error:', error));
            }

            function updateTable(id, data) {
                let table = document.getElementById(id);
                let tbody = table.getElementsByTagName('tbody')[0];
                tbody.innerHTML = '';
                for (let [symbol, amount] of Object.entries(data)) {
                    let row = tbody.insertRow();
                    let symbolCell = row.insertCell(0);
                    symbolCell.innerHTML = `<a href="https://www.binance.com/zh-CN/futures/${symbol}USDT" target="_blank" class="symbol-link">${symbol}</a>`;
                    let amountCell = row.insertCell(1);
                    amountCell.textContent = formatAmount(amount);
                    amountCell.style.color = getAmountColor(amount);
                }
            }

            function updateExchangeData(window, data) {
                document.getElementById('binance-long-' + window).textContent = formatAmount(data.binance_long);
                document.getElementById('binance-short-' + window).textContent = formatAmount(data.binance_short);
                document.getElementById('okx-long-' + window).textContent = formatAmount(data.okx_long);
                document.getElementById('okx-short-' + window).textContent = formatAmount(data.okx_short);
            }

            function updateLiquidationOrders(data) {
                const liquidationOrdersList = document.getElementById('liquidation-orders-list');
                liquidationOrdersList.innerHTML = '';  // Clear existing orders
                data.forEach(order => {
                    const newOrder = document.createElement('div');
                    newOrder.classList.add('liquidation-order');
                    const direction = order.direction === '多头爆仓' ? "⬇️ 多头爆仓😭" : "⬆️ 空头爆仓😄";
                    const symbol = order.symbol.replace(/USDT$/, '');  // Remove USDT suffix if present
                    const amount = Math.round(order.amount);
                    newOrder.innerHTML = `
                        <div><a href="https://www.binance.com/zh-CN/futures/${symbol}USDT" target="_blank" class="symbol-link">${symbol}</a></div>
                        <div>${order.exchange}</div>
                        <div style="color: ${getAmountColor(amount)};">${formatAmount(amount)} USDT</div>
                        <div>${direction}</div>
                        <div>${order.price}</div>
                        <div>-</div>
                        <div>${new Date(order.datetime).toLocaleTimeString()}</div>
                    `;
                    liquidationOrdersList.prepend(newOrder);  // Prepend the new order to the top
                });
            }

            // Initial load
            updateData();
            // Then update every second
            setInterval(updateData, 1000);
        </script>
    </head>
    <body>
        <h1>Real-time Liquidation强平-重生之数字难民全⚽️打野让我👀康康谁在💣爆仓</h1>
        <div id="liquidation-orders">
            <div id="liquidation-orders-list"></div>
        </div>
        <div class="container">
        {% for window in [3, 15, 60] %}
        <div class="section">
            <h2>{{ window }} Minutes Analysis Results</h2>
            <table>
                <tr>
                    <th>Exchange</th>
                    <th>Long Total</th>
                    <th>Short Total</th>
                </tr>
                <tr>
                    <td>Binance</td>
                    <td id="binance-long-{{ window }}"></td>
                    <td id="binance-short-{{ window }}"></td>
                </tr>
                <tr>
                    <td>OKX</td>
                    <td id="okx-long-{{ window }}"></td>
                    <td id="okx-short-{{ window }}"></td>
                </tr>
            </table>
            <table id="long-{{ window }}">
                <thead>
                    <tr><th colspan="2">Top 10 Long Liquidations</th></tr>
                    <tr><th>Symbol</th><th>Amount</th></tr>
                </thead>
                <tbody></tbody>
            </table>
            <table id="short-{{ window }}">
                <thead>
                    <tr><th colspan="2">Top 10 Short Liquidations</th></tr>
                    <tr><th>Symbol</th><th>Amount</th></tr>
                </thead>
                <tbody></tbody>
            </table>
        </div>
        {% endfor %}
        </div>
    </body>
    </html>
    """)

@app.route('/data')
def data():
    return json.dumps(latest_results, cls=NumpyEncoder)

@app.route('/latest_liquidations')
def latest_liquidations_data():
    return json.dumps(latest_liquidations, cls=NumpyEncoder)

if __name__ == "__main__":
    load_initial_data()  # 加载初始数据
    threading.Thread(target=update_data, daemon=True).start()
    app.run(host='0.0.0.0', port=6677, debug=False)  # 关闭 debug 模式