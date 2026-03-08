#!/usr/bin/env bash
# eventbus_functionality_challenge.sh - Validates EventBus module core functionality
# Checks pub/sub types, filtering, middleware chain, and key exported symbols
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
MODULE_NAME="EventBus"

PASS=0
FAIL=0
TOTAL=0

pass() { PASS=$((PASS+1)); TOTAL=$((TOTAL+1)); echo "  PASS: $1"; }
fail() { FAIL=$((FAIL+1)); TOTAL=$((TOTAL+1)); echo "  FAIL: $1"; }

echo "=== ${MODULE_NAME} Functionality Challenge ==="
echo ""

# --- Section 1: Required packages exist ---
echo "Section 1: Required packages"

for pkg in bus event filter middleware; do
    echo "Test: Package pkg/${pkg} exists"
    if [ -d "${MODULE_DIR}/pkg/${pkg}" ]; then
        pass "Package pkg/${pkg} exists"
    else
        fail "Package pkg/${pkg} missing"
    fi
done

# --- Section 2: Key exported types ---
echo ""
echo "Section 2: Key exported types"

echo "Test: EventBus struct exists in bus package"
if grep -q "type EventBus struct" "${MODULE_DIR}/pkg/bus/"*.go 2>/dev/null; then
    pass "EventBus struct exists"
else
    fail "EventBus struct missing"
fi

echo "Test: Event struct exists in event package"
if grep -q "type Event struct" "${MODULE_DIR}/pkg/event/"*.go 2>/dev/null; then
    pass "Event struct exists"
else
    fail "Event struct missing"
fi

echo "Test: Subscription struct exists in event package"
if grep -q "type Subscription struct" "${MODULE_DIR}/pkg/event/"*.go 2>/dev/null; then
    pass "Subscription struct exists"
else
    fail "Subscription struct missing"
fi

echo "Test: Config struct exists in bus package"
if grep -q "type Config struct" "${MODULE_DIR}/pkg/bus/"*.go 2>/dev/null; then
    pass "Config struct in bus package exists"
else
    fail "Config struct in bus package missing"
fi

echo "Test: MetricsCounter struct exists in middleware package"
if grep -q "type MetricsCounter struct" "${MODULE_DIR}/pkg/middleware/"*.go 2>/dev/null; then
    pass "MetricsCounter struct exists"
else
    fail "MetricsCounter struct missing"
fi

echo "Test: Metrics struct exists in bus package"
if grep -q "type Metrics struct" "${MODULE_DIR}/pkg/bus/"*.go 2>/dev/null; then
    pass "Metrics struct in bus package exists"
else
    fail "Metrics struct in bus package missing"
fi

# --- Section 3: Pub/Sub functionality ---
echo ""
echo "Section 3: Pub/Sub functionality"

echo "Test: Publish method exists"
if grep -rq "func.*Publish(" "${MODULE_DIR}/pkg/bus/"*.go 2>/dev/null; then
    pass "Publish method exists"
else
    fail "Publish method missing"
fi

echo "Test: Subscribe method exists"
if grep -rq "func.*Subscribe(" "${MODULE_DIR}/pkg/bus/"*.go 2>/dev/null; then
    pass "Subscribe method exists"
else
    fail "Subscribe method missing"
fi

echo "Test: Unsubscribe method exists"
if grep -rq "func.*Unsubscribe(" "${MODULE_DIR}/pkg/bus/"*.go 2>/dev/null; then
    pass "Unsubscribe method exists"
else
    fail "Unsubscribe method missing"
fi

# --- Section 4: Filter functionality ---
echo ""
echo "Section 4: Filter functionality"

echo "Test: Filter package has Go source files"
if ls "${MODULE_DIR}/pkg/filter/"*.go >/dev/null 2>&1; then
    pass "Filter package has Go source files"
else
    fail "Filter package has no Go source files"
fi

echo "Test: Filter logic is implemented (filter function or type)"
if grep -rqE "(func.*Filter|type.*Filter)" "${MODULE_DIR}/pkg/filter/"*.go 2>/dev/null; then
    pass "Filter logic is implemented"
else
    fail "Filter logic not found"
fi

# --- Section 5: Middleware chain ---
echo ""
echo "Section 5: Middleware chain"

echo "Test: Middleware package has Go source files"
if ls "${MODULE_DIR}/pkg/middleware/"*.go >/dev/null 2>&1; then
    pass "Middleware package has Go source files"
else
    fail "Middleware package has no Go source files"
fi

echo "Test: Middleware function or type exists"
if grep -rqE "(func.*Middleware|type.*Middleware)" "${MODULE_DIR}/pkg/middleware/"*.go 2>/dev/null; then
    pass "Middleware function or type exists"
else
    fail "Middleware function or type missing"
fi

# --- Section 6: Module structure completeness ---
echo ""
echo "Section 6: Module structure"

echo "Test: Each package has at least one non-test Go file"
all_have_source=true
for pkg in bus event filter middleware; do
    non_test=$(find "${MODULE_DIR}/pkg/${pkg}" -name "*.go" ! -name "*_test.go" -type f 2>/dev/null | wc -l)
    if [ "$non_test" -eq 0 ]; then
        fail "Package pkg/${pkg} has no non-test Go files"
        all_have_source=false
    fi
done
if [ "$all_have_source" = true ]; then
    pass "All packages have non-test Go source files"
fi

echo ""
echo "=== Results: ${PASS}/${TOTAL} passed, ${FAIL} failed ==="
[ "${FAIL}" -eq 0 ] && exit 0 || exit 1
