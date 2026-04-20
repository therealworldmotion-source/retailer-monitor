#!/bin/bash
# Pokemon TCG Monitor — persistent launcher
# Runs in tmux sessions with caffeinate to prevent sleep
#
# Usage:
#   ./start_monitor.sh              — start retailer monitor
#   ./start_monitor.sh stop         — stop retailer monitor
#   ./start_monitor.sh logs         — view retailer monitor logs
#   ./start_monitor.sh status       — check all sessions
#
#   ./start_monitor.sh pc           — start Pokemon Center monitor
#   ./start_monitor.sh pcstop       — stop Pokemon Center monitor
#   ./start_monitor.sh pclogs       — view Pokemon Center logs
#
#   ./start_monitor.sh ry           — start Ryman monitor
#   ./start_monitor.sh rystop       — stop Ryman monitor
#   ./start_monitor.sh rylogs       — view Ryman logs
#
#   ./start_monitor.sh startall     — start all monitors
#   ./start_monitor.sh stopall      — stop all monitors

SESSION="pokemon-monitor"
PC_SESSION="pokemon-center"
RY_SESSION="ryman-monitor"
DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="/opt/anaconda3/bin/python3"

case "${1:-start}" in

    # ── Retailer monitor ──────────────────────────────────────────────────────
    start)
        if tmux has-session -t "$SESSION" 2>/dev/null; then
            echo "Retailer monitor is already running. Use: ./start_monitor.sh stop"
            exit 1
        fi
        tmux new-session -d -s "$SESSION" "cd \"$DIR\" && caffeinate -dims $PYTHON monitor.py; echo 'Monitor exited. Press Enter to close.'; read"
        echo "Monitor started in tmux session '$SESSION'"
        echo ""
        echo "  View logs:    ./start_monitor.sh logs"
        echo "  Stop:         ./start_monitor.sh stop"
        echo "  Attach tmux:  tmux attach -t $SESSION"
        ;;

    stop)
        if tmux has-session -t "$SESSION" 2>/dev/null; then
            tmux kill-session -t "$SESSION"
            echo "Retailer monitor stopped."
        else
            echo "Retailer monitor is not running."
        fi
        ;;

    logs)
        tail -f "$DIR/monitor.log"
        ;;

    # ── Pokemon Center monitor ────────────────────────────────────────────────
    pc)
        if tmux has-session -t "$PC_SESSION" 2>/dev/null; then
            echo "Pokemon Center monitor is already running. Use: ./start_monitor.sh pcstop"
            exit 1
        fi
        tmux new-session -d -s "$PC_SESSION" "cd \"$DIR\" && caffeinate -dims $PYTHON pokemon_center.py; echo 'Pokemon Center monitor exited. Press Enter to close.'; read"
        echo "Pokemon Center monitor started in tmux session '$PC_SESSION'"
        echo ""
        echo "  View logs:    ./start_monitor.sh pclogs"
        echo "  Stop:         ./start_monitor.sh pcstop"
        echo "  Attach tmux:  tmux attach -t $PC_SESSION"
        ;;

    pcstop)
        if tmux has-session -t "$PC_SESSION" 2>/dev/null; then
            tmux kill-session -t "$PC_SESSION"
            echo "Pokemon Center monitor stopped."
        else
            echo "Pokemon Center monitor is not running."
        fi
        ;;

    pclogs)
        tail -f "$DIR/pokemon_center.log"
        ;;

    # ── Ryman monitor ─────────────────────────────────────────────────────────
    ry)
        if tmux has-session -t "$RY_SESSION" 2>/dev/null; then
            echo "Ryman monitor is already running. Use: ./start_monitor.sh rystop"
            exit 1
        fi
        tmux new-session -d -s "$RY_SESSION" "cd \"$DIR\" && caffeinate -dims $PYTHON ryman.py; echo 'Ryman monitor exited. Press Enter to close.'; read"
        echo "Ryman monitor started in tmux session '$RY_SESSION'"
        echo ""
        echo "  View logs:    ./start_monitor.sh rylogs"
        echo "  Stop:         ./start_monitor.sh rystop"
        echo "  Attach tmux:  tmux attach -t $RY_SESSION"
        ;;

    rystop)
        if tmux has-session -t "$RY_SESSION" 2>/dev/null; then
            tmux kill-session -t "$RY_SESSION"
            echo "Ryman monitor stopped."
        else
            echo "Ryman monitor is not running."
        fi
        ;;

    rylogs)
        tail -f "$DIR/ryman.log"
        ;;

    # ── All monitors ──────────────────────────────────────────────────────────
    startall)
        bash "$0" start
        bash "$0" pc
        bash "$0" ry
        ;;

    stopall)
        bash "$0" stop
        bash "$0" pcstop
        bash "$0" rystop
        ;;

    # ── Status ────────────────────────────────────────────────────────────────
    status)
        echo "=== Monitor Status ==="
        if tmux has-session -t "$SESSION" 2>/dev/null; then
            echo "✅ Retailer monitor: RUNNING  (tmux: $SESSION)"
        else
            echo "❌ Retailer monitor: STOPPED  (start: ./start_monitor.sh)"
        fi
        if tmux has-session -t "$PC_SESSION" 2>/dev/null; then
            echo "✅ Pokemon Center:   RUNNING  (tmux: $PC_SESSION)"
        else
            echo "❌ Pokemon Center:   STOPPED  (start: ./start_monitor.sh pc)"
        fi
        if tmux has-session -t "$RY_SESSION" 2>/dev/null; then
            echo "✅ Ryman:            RUNNING  (tmux: $RY_SESSION)"
        else
            echo "❌ Ryman:            STOPPED  (start: ./start_monitor.sh ry)"
        fi
        ;;

    *)
        echo "Usage: $0 {start|stop|logs|pc|pcstop|pclogs|ry|rystop|rylogs|startall|stopall|status}"
        ;;
esac
