#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run.sh — Master orchestrator for the OS Pipeline project.
#
# Accepts options via getopts, builds the project, discovers *.csv files
# in the input directory, launches the dispatcher (which forks the children),
# and prints a summary on exit.
#
# Shell scripting requirements met:
#   ✓ getopts for argument parsing
#   ✓ at least 3 Bash functions
#   ✓ one loop  (for loop over CSV files)
#   ✓ one case  statement (getopts case)
#   ✓ one arithmetic expansion $(( ))
#   ✓ trap on EXIT/INT/TERM for cleanup
#   ✓ passes shellcheck (no warnings)
#
# Usage:
#   ./run.sh -i <input_dir> -o <output_dir> [-n <threads>] [-q <queue>] [-c] [-h]
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Default values ────────────────────────────────────────────────────────────
INPUT_DIR=""
OUTPUT_DIR="output"
NUM_THREADS=4
QUEUE_SIZE=16
FIFO_PATH="/tmp/os_proj_fifo"
PID_FILE=".dispatcher.pid"
CLEAN_ONLY=0
DISPATCHER_PID=""
START_TIME=0

# ── Colour helpers ────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[run.sh]${NC} $*"; }
warn()  { echo -e "${YELLOW}[run.sh] WARN:${NC} $*" >&2; }
err()   { echo -e "${RED}[run.sh] ERROR:${NC} $*" >&2; }

# ── Function 1: print usage ───────────────────────────────────────────────────
usage() {
    cat <<EOF
Usage: $0 -i <input_dir> [OPTIONS]

Options:
  -i DIR    Input directory containing *.csv files  (required)
  -o DIR    Output directory for reports             (default: output)
  -n INT    Number of worker threads                 (default: 4)
  -q INT    Bounded queue size                       (default: 16)
  -c        Clean build artifacts and exit
  -h        Show this help message

Example:
  $0 -i data -o output -n 4 -q 16
EOF
    exit 0
}

# ── Function 2: verify dependencies ──────────────────────────────────────────
check_deps() {
    local missing=0
    for tool in g++ make; do
        if ! command -v "$tool" &>/dev/null; then
            err "Required tool '$tool' is not installed."
            missing=$(( missing + 1 ))
        fi
    done
    if [[ "$missing" -gt 0 ]]; then
        exit 1
    fi
    info "Dependencies OK (g++, make found)."
}

# ── Function 3: clean build artifacts ────────────────────────────────────────
do_clean() {
    info "Cleaning build artifacts..."
    make clean 2>/dev/null || true
    rm -f "$PID_FILE"
    rm -f "$FIFO_PATH" 2>/dev/null || true
    rm -f /dev/shm/sem.os_proj_sem_ready 2>/dev/null || true
    info "Clean complete."
}

# ── Function 4: build the project ────────────────────────────────────────────
build_project() {
    info "Building project..."
    if ! make -j"$(nproc)"; then
        err "Build failed. Fix compile errors and retry."
        exit 1
    fi
    info "Build successful."
}

# ── Function 5: print final summary ──────────────────────────────────────────
print_summary() {
    local exit_code="${1:-0}"
    local end_time
    end_time=$(date +%s)
    local elapsed=$(( end_time - START_TIME ))

    echo ""
    echo "======================================================="
    echo "  OS Pipeline — Run Summary"
    echo "======================================================="
    echo "  Total runtime  : ${elapsed}s"
    echo "  Exit code      : ${exit_code}"

    local csv_path="${OUTPUT_DIR}/report.csv"
    if [[ -f "$csv_path" ]]; then
        # Number of data lines = total lines minus header
        local cat_count
        cat_count=$(( $(wc -l < "$csv_path") - 1 ))
        echo "  Categories in report : ${cat_count}"
        echo ""
        echo "  Top categories by revenue:"
        # Loop over first 5 data lines in the CSV
        local rank=0
        while IFS=',' read -r r cat rev txn _tp _tr; do
            rank=$(( rank + 1 ))
            [[ "$rank" -gt 5 ]] && break
            printf "    %-3s %-20s \$%s (%s transactions)\n" \
                "${r}." "${cat//\"/}" "${rev}" "${txn}"
        done < <(tail -n +2 "$csv_path")
    else
        warn "report.csv not found — pipeline may have failed."
    fi
    echo "======================================================="
}

# ── Trap: cleanup on EXIT / INT / TERM ───────────────────────────────────────
cleanup_trap() {
    local exit_code=$?
    info "Trap triggered — cleaning up..."

    if [[ -n "$DISPATCHER_PID" ]] && kill -0 "$DISPATCHER_PID" 2>/dev/null; then
        info "Sending SIGTERM to dispatcher (PID=$DISPATCHER_PID)..."
        kill -TERM "$DISPATCHER_PID" 2>/dev/null || true
        sleep 1
        kill -0 "$DISPATCHER_PID" 2>/dev/null && \
            kill -KILL "$DISPATCHER_PID" 2>/dev/null || true
    fi

    rm -f "$PID_FILE"
    rm -f "$FIFO_PATH" 2>/dev/null || true

    print_summary "$exit_code"
    exit "$exit_code"
}
trap cleanup_trap EXIT INT TERM

# ── Parse command-line options via getopts ────────────────────────────────────
while getopts ":i:o:n:q:ch" opt; do
    case "$opt" in
        i) INPUT_DIR="$OPTARG"   ;;
        o) OUTPUT_DIR="$OPTARG"  ;;
        n) NUM_THREADS="$OPTARG" ;;
        q) QUEUE_SIZE="$OPTARG"  ;;
        c) CLEAN_ONLY=1          ;;
        h) usage                 ;;
        :) err "Option -${OPTARG} requires an argument."; exit 10 ;;
       \?) err "Unknown option: -${OPTARG}";             exit 10 ;;
    esac
done

# ── Handle clean-only mode ────────────────────────────────────────────────────
if [[ "$CLEAN_ONLY" -eq 1 ]]; then
    do_clean
    exit 0
fi

# ── Validate required arguments ───────────────────────────────────────────────
if [[ -z "$INPUT_DIR" ]]; then
    err "Input directory (-i) is required."
    usage
fi

if [[ ! -d "$INPUT_DIR" ]]; then
    err "Input directory '$INPUT_DIR' does not exist."
    exit 40
fi

if ! [[ "$NUM_THREADS" =~ ^[0-9]+$ ]] || [[ "$NUM_THREADS" -lt 1 ]]; then
    err "NUM_THREADS (-n) must be a positive integer."
    exit 10
fi

if ! [[ "$QUEUE_SIZE" =~ ^[0-9]+$ ]] || [[ "$QUEUE_SIZE" -lt 1 ]]; then
    err "QUEUE_SIZE (-q) must be a positive integer."
    exit 10
fi

# ── Collect *.csv files from input directory ──────────────────────────────────
CSV_ARGS=()
csv_count=0
for f in "$INPUT_DIR"/*.csv; do
    if [[ -f "$f" ]]; then
        CSV_ARGS+=("$f")
        csv_count=$(( csv_count + 1 ))
    fi
done

if [[ "$csv_count" -eq 0 ]]; then
    err "No *.csv files found in '$INPUT_DIR'."
    exit 40
fi
info "Found ${csv_count} CSV file(s) in '$INPUT_DIR'."

START_TIME=$(date +%s)

# ── Pre-flight ────────────────────────────────────────────────────────────────
check_deps

# ── Create required directories ───────────────────────────────────────────────
mkdir -p "$OUTPUT_DIR" logs

# ── Build ─────────────────────────────────────────────────────────────────────
build_project

# ── Launch dispatcher ─────────────────────────────────────────────────────────
info "Launching dispatcher..."
info "  input_dir  : $INPUT_DIR"
info "  output_dir : $OUTPUT_DIR"
info "  threads    : $NUM_THREADS"
info "  queue_size : $QUEUE_SIZE"
info "  fifo       : $FIFO_PATH"
info "  csv files  : ${CSV_ARGS[*]}"

# dispatcher argv: <input_dir> <output_dir> <threads> <queue> <fifo> <csv...>
# ingester is launched by dispatcher; it receives CSV paths via execvp args
# We pass CSV file list to dispatcher so it can forward them to ingester.
./dispatcher \
    "$INPUT_DIR" \
    "$OUTPUT_DIR" \
    "$NUM_THREADS" \
    "$QUEUE_SIZE" \
    "$FIFO_PATH" \
    "${CSV_ARGS[@]}" &

DISPATCHER_PID=$!
echo "$DISPATCHER_PID" > "$PID_FILE"
info "Dispatcher running (PID=$DISPATCHER_PID)."

# ── Wait for dispatcher ───────────────────────────────────────────────────────
wait "$DISPATCHER_PID"
DISPATCH_EXIT=$?
DISPATCHER_PID=""   # prevent cleanup_trap from killing it again

if [[ "$DISPATCH_EXIT" -eq 0 ]]; then
    info "Dispatcher finished successfully."
else
    warn "Dispatcher exited with code $DISPATCH_EXIT."
fi

exit "$DISPATCH_EXIT"
