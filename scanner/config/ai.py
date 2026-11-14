import os
from typing import Optional

# ==============================================================================
# AI 智能决策配置 (DeepSeek 驱动)
# ==============================================================================

# AI 决策开关：是否在扫描完成后启用 AI 研判
AI_ENABLED: bool = os.getenv("AI_ENABLED", "True").lower() in ("true", "1", "yes")

# DeepSeek API Key（优先读取环境变量，可与 GitHub Actions Secrets: DEEPSEEK_API_KEY 绑定）
DEEPSEEK_API_KEY: Optional[str] = os.getenv("DEEPSEEK_API_KEY")

# DeepSeek 基础端点（兼容 OpenAI 协议）
DEEPSEEK_BASE_URL: str = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com")

# 模型选用：
# - deepseek-flash: DeepSeek-V4.1-Flash 最新架构，速度极快且自带 Thinking 思考能力（默认推荐）
# - deepseek-v4-pro: DeepSeek-V4-Pro 满血旗舰，超强推演
DEEPSEEK_MODEL: str = os.getenv("DEEPSEEK_MODEL", "deepseek-flash")

# API 调用超时时间（秒，防止网络抖动阻塞主扫描流水线）
DEEPSEEK_TIMEOUT_SEC: float = float(os.getenv("DEEPSEEK_TIMEOUT_SEC", "25.0"))

# 送入 AI 上下文的最大标的数（避免超长提示词，将单次调用 Token 严格控制在超低成本区间）
AI_MAX_STOCKS_CONTEXT: int = 15

# 生成温度（较低温度以保证量化金融逻辑的严谨性与一致性）
AI_TEMPERATURE: float = 0.3
