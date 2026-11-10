#!/bin/bash
# claude_report.sh — A股 & 港股智能决策日报 fetch/send 封装
#
# 用法：
#   bash claude_report.sh fetch premarket   # 09:00 开盘前瞻取材
#   bash claude_report.sh fetch close       # 16:15 收盘复盘取材（默认）
#   bash claude_report.sh send              # 将 logs/report_cn.txt 与 logs/report_hk.txt 分别推送至 A股/港股飞书机器人

set -uo pipefail
cd "$(dirname "$0")" || exit 1

MODE="${1:-}"
SESSION="${2:-close}"

if [ "$MODE" != "fetch" ] && [ "$MODE" != "send" ]; then
    echo "ERROR: 用法 claude_report.sh fetch [premarket|close] | send" >&2
    exit 2
fi

PLIST="$HOME/Library/LaunchAgents/com.shirley.cn-hk-stock-bot.plist"
PY="/usr/bin/python3"

if [ ! -f "$PLIST" ]; then
    echo "ERROR: 找不到 $PLIST，无法加载环境变量" >&2
    exit 1
fi

while IFS= read -r line; do
    export "$line"
done < <(/usr/libexec/PlistBuddy -c "Print :EnvironmentVariables" "$PLIST" \
    | sed -n 's/^[[:space:]]*\([A-Za-z_][A-Za-z0-9_]*\) = \(.*\)$/\1=\2/p')

exec "$PY" cn_hk_report.py --mode "$MODE" --session "$SESSION"
