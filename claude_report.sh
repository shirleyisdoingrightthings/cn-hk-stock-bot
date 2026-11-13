#!/bin/bash
# claude_report.sh — A股 & 港股智能决策日报（委托 shared/report_base.sh）
DIR="$(cd "$(dirname "$0")" && pwd)"
PLIST="$HOME/Library/LaunchAgents/com.shirley.cn-hk-stock-bot.plist"
SCRIPT="cn_hk_report.py"
USAGE_MSG="用法: claude_report.sh {fetch [premarket|close] | send [premarket|close]}"
EXTRA_ARGS=()
if [ "${1:-}" = "fetch" ]; then
    EXTRA_ARGS=(--session "${2:-close}")
elif [ -n "${2:-}" ]; then
    EXTRA_ARGS=(--session "$2")
fi
source "$DIR/../shared/report_base.sh"
