#!/bin/bash
# =============================================================================
# Schwadler Profiling Script
# =============================================================================
#
# Generates flamegraphs and performance profiles for Schwadler.
#
# Usage:
#   ./scripts/profile.sh [gemfile] [output_name]
#
# Examples:
#   ./scripts/profile.sh                                    # Profile with rails.gemfile
#   ./scripts/profile.sh benches/fixtures/massive.gemfile   # Profile with massive
#   ./scripts/profile.sh Gemfile my_app                     # Custom gemfile + name
#
# Prerequisites:
#   macOS: Install Xcode Command Line Tools (has `sample` command)
#   Linux: Install `perf` (linux-tools-generic)
#   All:   cargo-flamegraph (`cargo install flamegraph`)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

# Configuration
GEMFILE="${1:-benches/fixtures/rails.gemfile}"
OUTPUT_NAME="${2:-$(basename "$GEMFILE" .gemfile)}"
OUTPUT_DIR="$PROJECT_DIR/target/profiles"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "🔥 Schwadler Profiling"
echo "   Gemfile: $GEMFILE"
echo "   Output:  $OUTPUT_DIR/$OUTPUT_NAME"
echo ""

# Ensure output directory exists
mkdir -p "$OUTPUT_DIR"

# Build release binary with debug symbols
echo "📦 Building release binary with debug symbols..."
RUSTFLAGS="-C debuginfo=2" cargo build --release

BINARY="$PROJECT_DIR/target/release/schwadl"

if [[ ! -f "$BINARY" ]]; then
    echo "❌ Binary not found at $BINARY"
    exit 1
fi

# Detect platform and choose profiling method
OS=$(uname -s)

case "$OS" in
    Darwin)
        echo "🍎 Using macOS profiling tools..."
        
        # Method 1: sampler (if available)
        if command -v sample &> /dev/null; then
            echo ""
            echo "📊 Running sample profiler (10 seconds)..."
            echo "   Command: $BINARY lock -g $GEMFILE"
            
            # Clear cache for consistent profiling
            rm -rf ~/.schwadler/cache
            
            # Run with sample profiler
            PROFILE_FILE="$OUTPUT_DIR/${OUTPUT_NAME}_${TIMESTAMP}.sample"
            sample "$BINARY" 10 -f "$PROFILE_FILE" -wait -- lock -g "$GEMFILE" || true
            
            echo "   ✅ Sample output: $PROFILE_FILE"
        fi
        
        # Method 2: Instruments (if available)
        if command -v xcrun &> /dev/null && xcrun --find instruments &> /dev/null 2>&1; then
            echo ""
            echo "📊 Running Instruments Time Profiler..."
            
            rm -rf ~/.schwadler/cache
            
            TRACE_FILE="$OUTPUT_DIR/${OUTPUT_NAME}_${TIMESTAMP}.trace"
            xcrun xctrace record --template 'Time Profiler' --output "$TRACE_FILE" \
                --launch -- "$BINARY" lock -g "$GEMFILE" || echo "   ⚠️  Instruments failed (may need SIP disabled)"
            
            echo "   ✅ Trace output: $TRACE_FILE"
        fi
        ;;
        
    Linux)
        echo "🐧 Using Linux profiling tools..."
        
        # Check for perf
        if command -v perf &> /dev/null; then
            echo ""
            echo "📊 Running perf record..."
            
            rm -rf ~/.schwadler/cache
            
            PERF_FILE="$OUTPUT_DIR/${OUTPUT_NAME}_${TIMESTAMP}.perf.data"
            perf record -g --call-graph dwarf -o "$PERF_FILE" \
                "$BINARY" lock -g "$GEMFILE"
            
            echo "   ✅ Perf output: $PERF_FILE"
            echo "   View with: perf report -i $PERF_FILE"
        else
            echo "   ⚠️  perf not found. Install with: sudo apt install linux-tools-generic"
        fi
        ;;
        
    *)
        echo "⚠️  Unknown OS: $OS - skipping platform-specific profiling"
        ;;
esac

# Method 3: cargo-flamegraph (cross-platform)
if command -v cargo-flamegraph &> /dev/null || cargo flamegraph --help &> /dev/null 2>&1; then
    echo ""
    echo "🔥 Generating flamegraph..."
    
    rm -rf ~/.schwadler/cache
    
    FLAMEGRAPH_FILE="$OUTPUT_DIR/${OUTPUT_NAME}_${TIMESTAMP}_flamegraph.svg"
    
    # On macOS, need dtrace permissions (usually requires sudo or SIP disabled)
    if [[ "$OS" == "Darwin" ]]; then
        echo "   Note: On macOS, flamegraph may require 'sudo' or SIP disabled"
        cargo flamegraph --root --bin schwadl -o "$FLAMEGRAPH_FILE" -- lock -g "$GEMFILE" || \
        echo "   ⚠️  Flamegraph failed - try with sudo or disable SIP"
    else
        cargo flamegraph --bin schwadl -o "$FLAMEGRAPH_FILE" -- lock -g "$GEMFILE"
    fi
    
    if [[ -f "$FLAMEGRAPH_FILE" ]]; then
        echo "   ✅ Flamegraph: $FLAMEGRAPH_FILE"
        echo "   Open in browser to view"
    fi
else
    echo ""
    echo "💡 Install cargo-flamegraph for flamegraphs:"
    echo "   cargo install flamegraph"
fi

# Timing benchmark
echo ""
echo "⏱️  Running timing benchmark (5 iterations)..."
rm -rf ~/.schwadler/cache

TIMING_FILE="$OUTPUT_DIR/${OUTPUT_NAME}_${TIMESTAMP}_timing.txt"

echo "Schwadler Timing Benchmark" > "$TIMING_FILE"
echo "Gemfile: $GEMFILE" >> "$TIMING_FILE"
echo "Date: $(date)" >> "$TIMING_FILE"
echo "---" >> "$TIMING_FILE"

for i in {1..5}; do
    rm -rf ~/.schwadler/cache  # Cold cache
    START=$(perl -MTime::HiRes=time -e 'printf "%.3f\n", time')
    "$BINARY" lock -g "$GEMFILE" > /dev/null 2>&1
    END=$(perl -MTime::HiRes=time -e 'printf "%.3f\n", time')
    ELAPSED=$(echo "$END - $START" | bc)
    echo "Run $i (cold cache): ${ELAPSED}s" | tee -a "$TIMING_FILE"
done

echo "---" >> "$TIMING_FILE"

# Warm cache runs
"$BINARY" lock -g "$GEMFILE" > /dev/null 2>&1  # Prime cache

for i in {1..5}; do
    START=$(perl -MTime::HiRes=time -e 'printf "%.3f\n", time')
    "$BINARY" lock -g "$GEMFILE" > /dev/null 2>&1
    END=$(perl -MTime::HiRes=time -e 'printf "%.3f\n", time')
    ELAPSED=$(echo "$END - $START" | bc)
    echo "Run $i (warm cache): ${ELAPSED}s" | tee -a "$TIMING_FILE"
done

echo ""
echo "✅ Profiling complete!"
echo "   Results in: $OUTPUT_DIR"
ls -la "$OUTPUT_DIR" | grep "$OUTPUT_NAME"
