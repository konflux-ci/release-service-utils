#!/usr/bin/env bash
#
# Memory-aware job throttling utilities for Tekton tasks
#
# Usage:
#   source memory-throttle.sh
#   
#   # In your parallel job loop:
#   for item in "${items[@]}"; do
#       wait_for_memory "${MEMORY_THRESHOLD:-80}"
#       do_something &
#   done

# Read raw memory values from cgroups (v2 or v1)
# Outputs: "current max" (space-separated bytes) or empty if unavailable
# This is a helper used by get_memory_usage_percent and get_memory_stats
_read_cgroup_memory() {
    local current max

    # Try cgroups v2 first (newer Kubernetes/OpenShift)
    if [ -r /sys/fs/cgroup/memory.current ] && [ -r /sys/fs/cgroup/memory.max ]; then
        current=$(cat /sys/fs/cgroup/memory.current 2>/dev/null) || return
        max=$(cat /sys/fs/cgroup/memory.max 2>/dev/null) || return
        if [ -n "$max" ] && [ "$max" != "max" ] && [ "$max" -gt 0 ] 2>/dev/null; then
            echo "$current $max"
            return
        fi
    fi

    # Fall back to cgroups v1
    if [ -r /sys/fs/cgroup/memory/memory.usage_in_bytes ] && \
       [ -r /sys/fs/cgroup/memory/memory.limit_in_bytes ]; then
        current=$(cat /sys/fs/cgroup/memory/memory.usage_in_bytes 2>/dev/null) || return
        max=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null) || return
        if [ -n "$max" ] && [ "$max" -gt 0 ] 2>/dev/null; then
            echo "$current $max"
            return
        fi
    fi

    # Cannot determine memory usage
}

# Get current memory usage percentage from cgroups
# Returns: memory usage as integer percentage (0-100), or empty string if unavailable
get_memory_usage_percent() {
    local values current max
    values=$(_read_cgroup_memory) || return
    [ -z "$values" ] && return

    read -r current max <<< "$values"
    echo $((current * 100 / max))
}

# Format bytes to human-readable string
format_bytes() {
    local bytes=$1
    if [ "$bytes" -ge 1073741824 ]; then
        echo "$(( bytes / 1073741824 ))Gi"
    elif [ "$bytes" -ge 1048576 ]; then
        echo "$(( bytes / 1048576 ))Mi"
    elif [ "$bytes" -ge 1024 ]; then
        echo "$(( bytes / 1024 ))Ki"
    else
        echo "${bytes}B"
    fi
}

# Get memory stats for logging
# Returns: "used/limit (XX%)" or "unavailable"
get_memory_stats() {
    local values current max usage
    values=$(_read_cgroup_memory)

    if [ -z "$values" ]; then
        echo "unavailable"
        return
    fi

    read -r current max <<< "$values"
    usage=$((current * 100 / max))
    echo "$(format_bytes "$current")/$(format_bytes "$max") (${usage}%)"
}

# Wait until memory usage is below threshold
# Arguments:
#   $1 - threshold percentage (default: 80)
#   $2 - check interval in seconds (default: 5)
# 
# This function will block until memory usage drops below the threshold.
# If cgroup memory info is unavailable, returns immediately (no blocking).
wait_for_memory() {
    # Suppress xtrace for this function
    { local _xtrace_was_set=false; [[ $- == *x* ]] && _xtrace_was_set=true; set +x; } 2>/dev/null

    local threshold="${1:-80}"
    local interval="${2:-5}"
    local usage
    local waited=false

    # Check if memory monitoring is available
    usage=$(get_memory_usage_percent)
    if [ -z "$usage" ]; then
        if $_xtrace_was_set; then set -x; fi
        # Can't read cgroups - don't block, rely on concurrentLimit only
        return 0
    fi

    while [ "$usage" -ge "$threshold" ]; do
        if [ "$waited" = false ]; then
            echo "Memory throttle: usage above ${threshold}% threshold, pausing new job spawns..."
            waited=true
        fi
        
        echo "  Memory: $(get_memory_stats) - waiting for running jobs to free memory..."
        sleep "$interval"
        
        # Allow background jobs to be reaped while waiting
        wait -n 2>/dev/null || true
        
        usage=$(get_memory_usage_percent)
        if [ -z "$usage" ]; then
            # Lost access to cgroups mid-run (unlikely but handle it)
            break
        fi
    done

    if [ "$waited" = true ]; then
        echo "Memory throttle: usage now at $(get_memory_stats), resuming..."
    fi

    if $_xtrace_was_set; then set -x; fi
}

# Log whether memory-based throttling is available (call once at task start)
log_memory_throttle_status() {
    # Suppress xtrace for this function
    { local _xtrace_was_set=false; [[ $- == *x* ]] && _xtrace_was_set=true; set +x; } 2>/dev/null
    local threshold="${1:-80}"
    local stats
    stats=$(get_memory_stats)
    
    if [ "$stats" = "unavailable" ]; then
        echo "Memory throttle: cgroup memory info not available, using concurrentLimit only"
    else
        echo "Memory throttle: enabled with ${threshold}% threshold, current usage: ${stats}"
    fi

    if $_xtrace_was_set; then set -x; fi
}
