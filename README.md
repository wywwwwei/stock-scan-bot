一个基于 Python 的自动化美股扫描机器人，能够根据预设的技术指标策略筛选股票，并将结果通过电子邮件发送。

## 功能特性

- **自定义策略：** 通过继承基类 `BaseStrategy` 可以轻松添加新的扫描策略。
- **灵活配置：**
  - 支持扫描所有纳斯达克上市股票。
  - 支持仅扫描特定的股票代码列表。
  - 支持为不同的股票配置不同的扫描策略组合。
- **定时运行：** 可部署到 GitHub Actions，实现定时自动扫描和邮件发送。

## 配置方法教程

### 1. 新增策略

可以创建一个新的策略类来实现自定义的扫描逻辑。

1. **继承 `BaseStrategy`：** 创建一个新类，继承自 `BaseStrategy` 基类。
2. **实现必要方法：**
   - `__init__(self)`: 在构造函数中定义 `self.days_needed`，即该策略需要的历史数据天数（包含当天）。
   - `get_required_days(self)`: 返回 `self.days_needed`。
   - `get_required_data_columns(self)`: 返回该策略需要的额外数据列（如 `['MA5', 'DollarVolume']`）。基础列 `['Close', 'Volume']` 已默认包含。
   - `check_condition(self, data_row, past_data)`: 定义扫描逻辑。`data_row` 是当天的数据，`past_data` 是 `days_needed - 1` 天的历史数据。返回 `True` 或 `False`。
   - `format_result(self, ticker, data_row, past_data)`: 定义符合条件的股票结果如何展示，返回一个字典。
   - `get_description(self)`: 返回策略的描述，用于邮件标题。
   - `get_sort_column(self)`: 返回用于排序的列名。
   - `get_sort_ascending(self)`: 返回排序顺序（`False` 为降序）。
3. **添加到执行列表：** 将新策略类的实例添加到 `EXECUTE_STRATEGIES` 列表中，以便在扫描时执行。

**示例：**

```python
class MyCustomStrategy(BaseStrategy):
    def __init__(self):
        self.days_needed = 20 # 需要20天数据

    def get_required_days(self):
        return self.days_needed

    def get_required_data_columns(self):
        cols = super().get_required_data_columns()
        return cols + ['MA10'] # 需要MA10

    def check_condition(self, data_row, past_data):
        # 假设策略是：当前价格高于MA10
        current_price = data_row['Close']
        current_ma10 = data_row['MA10']
        return current_price > current_ma10

    def format_result(self, ticker, data_row, past_data):
        current_price = data_row['Close']
        return {
            'Symbol': ticker,
            'Current Price': f"${current_price:.2f}",
            'Status': 'Price above MA10'
        }

    def get_description(self):
        return "自定义策略：价格高于MA10 (按价格排序)"

    def get_sort_column(self):
        return 'Current Price Num'

    def get_sort_ascending(self):
        return False
```

### 2. 新增扫描股票

可以配置机器人只扫描特定的股票代码，配置文件路径 `./scanner/config/scan.py`。

1. **编辑 `TARGET_STOCKS` 列表：** 在 配置区域部分找到 `TARGET_STOCKS`。
2. **添加股票代码：** 将想扫描的股票代码字符串添加到列表中。

**示例：**

```python
TARGET_STOCKS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "TSLA"
]
```

- **注意：** 如果 `TARGET_STOCKS` 列表为空 (`[]`)，机器人将扫描所有纳斯达克股票。

### 3. 新增不同股票扫描不同策略

可以为不同的股票配置不同的扫描策略组合，配置文件路径 `./scanner/config/scan.py`。

1. **编辑 `STOCK_STRATEGY_MAP` 字典：** 在配置区域部分找到 `STOCK_STRATEGY_MAP`。
2. **配置映射关系：** 以股票代码为键，以该股票要执行的**策略实例列表**为值。

**示例：**

```python
# 假设您已经定义了其他策略类，如 MyCustomStrategy
STOCK_STRATEGY_MAP = {
    "AAPL": [VolumeSurgeStrategy(), MACrossStrategy()], # AAPL只执行成交量和均线策略
    "MSFT": [MyCustomStrategy()],                       # MSFT只执行自定义策略
    "GOOGL": [CDSignalStrategy(), VolumeSurgeStrategy()] # GOOGL执行CD和成交量策略
}
```

- **注意：** 如果某只股票的代码不在 `STOCK_STRATEGY_MAP` 中，机器人将为其执行 `EXECUTE_STRATEGIES` 中定义的全局默认策略。

### 4. 配置并发与速率

配置文件路径 `./scanner/config/scan.py`

1. **编辑 `SCAN_MAX_WORKERS`**：配置最大并发线程数
2. **编辑 `YF_MAX_CALLS_PER_SEC`：** 配置 Yahoo Finance API 的最大请求速率，避免频繁的 API 错误。

```

```

## 注意事项

- 机器人目前仅支持扫描纳斯达克交易所的股票。如需扫描 NYSE 或 AMEX，需修改 `get_nasdaq_symbols` 函数以获取完整的股票列表。