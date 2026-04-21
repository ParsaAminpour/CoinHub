#!/usr/bin/env bash
# =============================================================================
# CoinHub — User Simulation Script
# Simulates 5 distinct user personas for a configurable duration.
#
# Usage:
#   ./scripts/simulate_users.sh [duration_seconds]   (default: 1800 = 30 min)
#
# Personas:
#   active_trader  — frequent limit + market orders, occasional cancels
#   scalper        — rapid-fire limit orders, cancels most of them
#   market_maker   — posts both sides of the book at spread
#   lurker         — mostly browses, trades rarely
#   bad_actor      — malformed payloads, wrong auth, spam
# =============================================================================

BASE="${COINHUB_BASE_URL:-http://localhost:8083}"
DURATION="${1:-1800}"
END=$((SECONDS + DURATION))

PAIRS=("BTC-USDT" "ETH-USDT" "BNB-USDT")
BTC_PRICES=(48000 49000 50000 51000 52000 53000)
ETH_PRICES=(2800 2900 3000 3100 3200)
BNB_PRICES=(380 390 400 410 420)

# ── helpers ───────────────────────────────────────────────────────────────────

rand_int() { echo $(( RANDOM % ($2 - $1 + 1) + $1 )); }

rand_element() {
  local arr=("$@")
  echo "${arr[$(( RANDOM % ${#arr[@]} ))]}"
}

rand_qty() {
  local decimals=("0.001" "0.005" "0.01" "0.02" "0.05" "0.1" "0.25" "0.5")
  rand_element "${decimals[@]}"
}

rand_price_for_pair() {
  case $1 in
    BTC-USDT) rand_element "${BTC_PRICES[@]}" ;;
    ETH-USDT) rand_element "${ETH_PRICES[@]}" ;;
    BNB-USDT) rand_element "${BNB_PRICES[@]}" ;;
  esac
}

rand_side() {
  if (( RANDOM % 2 )); then echo "BUY"; else echo "SELL"; fi
}

ord_id() {
  # alphanumeric + dash, max 36 chars
  echo "$1-$(date +%s%N | tail -c 8)-$(rand_int 1000 9999)"
}

get_token() {
  curl -s -X POST "$BASE/v1/auth/mock/login" \
    | grep -o '"jwt_token":"[^"]*"' \
    | cut -d'"' -f4
}

post() {
  curl -s -o /dev/null -w "%{http_code}" -X POST "$1" \
    -H "Content-Type: application/json" \
    "${@:2}"
}

get() {
  curl -s -o /dev/null -w "%{http_code}" "$1"
}

place_limit() {
  local token=$1 pair=$2 side=$3 price=$4 qty=$5 oid=$6
  post "$BASE/v1/order/limit" \
    -H "Authorization: Bearer $token" \
    -d "{\"user_id\":1,\"client_ord_id\":\"$oid\",\"symbol\":\"$pair\",\"side\":\"$side\",\"ord_type\":\"LIMIT\",\"price\":\"$price\",\"qty\":\"$qty\",\"time_in_force\":\"GTC\"}"
}

place_market() {
  local token=$1 pair=$2 side=$3 qty=$4 oid=$5
  post "$BASE/v1/order/market" \
    -H "Authorization: Bearer $token" \
    -d "{\"user_id\":1,\"client_ord_id\":\"$oid\",\"symbol\":\"$pair\",\"side\":\"$side\",\"ord_type\":\"MARKET\",\"qty\":\"$qty\"}"
}

cancel_order() {
  local token=$1 pair=$2 side=$3 oid=$4
  curl -s -o /dev/null -w "%{http_code}" -X DELETE "$BASE/v1/order/cancel" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $token" \
    -d "{\"order_id\":1,\"client_ord_id\":\"$oid\",\"symbol\":\"$pair\",\"side\":\"$side\"}"
}

register_user() {
  local n=$1
  post "$BASE/v1/auth/register" \
    -d "{\"username\":\"zombie${n}\",\"firstname\":\"Zombie\",\"lastname\":\"User${n}\",\"gmail\":\"zombie${n}@coinhub.test\",\"password\":\"P@ssw0rd${n}\"}"
}

login_fail() {
  post "$BASE/v1/auth/login/username" \
    -d "{\"username\":\"nobody\",\"password\":\"wrongpass\"}"
}

sleep_rand() {
  # sleep between $1 and $2 milliseconds
  local ms=$(rand_int $1 $2)
  sleep "0.$(printf '%03d' $ms)" 2>/dev/null || sleep 1
}

# ── persona: active trader ────────────────────────────────────────────────────
# Logs in, places limit orders frequently, occasional market orders, cancels some.

active_trader() {
  local id=$1
  local token
  token=$(get_token)
  [ -z "$token" ] && return
  echo "[active_trader_$id] started"

  local count=0
  while [ $SECONDS -lt $END ]; do
    local pair; pair=$(rand_element "${PAIRS[@]}")
    local side; side=$(rand_side)
    local price; price=$(rand_price_for_pair "$pair")
    local qty; qty=$(rand_qty)
    local oid; oid=$(ord_id "at${id}")

    local action=$(rand_int 1 10)

    if (( action <= 6 )); then
      place_limit "$token" "$pair" "$side" "$price" "$qty" "$oid" > /dev/null
    elif (( action <= 9 )); then
      place_market "$token" "$pair" "$side" "$qty" "$oid" > /dev/null
    else
      cancel_order "$token" "$pair" "$side" "$oid" > /dev/null
    fi

    count=$((count+1))
    sleep_rand 300 1200
  done
  echo "[active_trader_$id] done — $count actions"
}

# ── persona: scalper ─────────────────────────────────────────────────────────
# Places limit orders quickly and cancels most of them. High frequency.

scalper() {
  local id=$1
  local token
  token=$(get_token)
  [ -z "$token" ] && return
  echo "[scalper_$id] started"

  local count=0
  while [ $SECONDS -lt $END ]; do
    local pair; pair=$(rand_element "${PAIRS[@]}")
    local side; side=$(rand_side)
    local price; price=$(rand_price_for_pair "$pair")
    local qty; qty=$(rand_qty)
    local oid; oid=$(ord_id "sc${id}")

    place_limit "$token" "$pair" "$side" "$price" "$qty" "$oid" > /dev/null
    count=$((count+1))
    sleep_rand 50 200

    # cancel 70% of the time
    if (( RANDOM % 10 < 7 )); then
      cancel_order "$token" "$pair" "$side" "$oid" > /dev/null
      count=$((count+1))
    fi
    sleep_rand 50 300
  done
  echo "[scalper_$id] done — $count actions"
}

# ── persona: market maker ─────────────────────────────────────────────────────
# Posts both buy and sell limit orders at a spread around mid-price.

market_maker() {
  local id=$1
  local token
  token=$(get_token)
  [ -z "$token" ] && return
  echo "[market_maker_$id] started"

  local count=0
  while [ $SECONDS -lt $END ]; do
    local pair; pair=$(rand_element "${PAIRS[@]}")
    local mid; mid=$(rand_price_for_pair "$pair")
    local spread=$(rand_int 5 50)
    local bid=$(( mid - spread ))
    local ask=$(( mid + spread ))
    local qty; qty=$(rand_qty)

    local bid_oid; bid_oid=$(ord_id "mm${id}b")
    local ask_oid; ask_oid=$(ord_id "mm${id}a")

    place_limit "$token" "$pair" "BUY"  "$bid" "$qty" "$bid_oid" > /dev/null
    place_limit "$token" "$pair" "SELL" "$ask" "$qty" "$ask_oid" > /dev/null
    count=$((count+2))

    sleep_rand 800 2000

    # refresh quotes: cancel and repost
    cancel_order "$token" "$pair" "BUY"  "$bid_oid" > /dev/null
    cancel_order "$token" "$pair" "SELL" "$ask_oid" > /dev/null
    count=$((count+2))

    sleep_rand 200 500
  done
  echo "[market_maker_$id] done — $count actions"
}

# ── persona: lurker ───────────────────────────────────────────────────────────
# Mostly hits read/info endpoints, registers, fails login, rarely trades.

lurker() {
  local id=$1
  local token
  token=$(get_token)
  [ -z "$token" ] && return
  echo "[lurker_$id] started"

  register_user "$id" > /dev/null

  local count=0
  while [ $SECONDS -lt $END ]; do
    local action=$(rand_int 1 10)

    if (( action <= 5 )); then
      get "$BASE/v1/ping" > /dev/null
    elif (( action <= 7 )); then
      login_fail > /dev/null
    elif (( action == 8 )); then
      get "$BASE/swagger/index.html" > /dev/null
    elif (( action == 9 )); then
      get "$BASE/metrics" > /dev/null
    else
      # rare trade
      local pair; pair=$(rand_element "${PAIRS[@]}")
      local oid; oid=$(ord_id "lu${id}")
      place_market "$token" "$pair" "$(rand_side)" "$(rand_qty)" "$oid" > /dev/null
    fi

    count=$((count+1))
    sleep_rand 1000 4000
  done
  echo "[lurker_$id] done — $count actions"
}

# ── persona: bad actor ────────────────────────────────────────────────────────
# Malformed requests, missing fields, wrong tokens, route probing.

bad_actor() {
  local id=$1
  echo "[bad_actor_$id] started"

  local count=0
  while [ $SECONDS -lt $END ]; do
    local action=$(rand_int 1 8)

    case $action in
      1) # missing auth
        post "$BASE/v1/order/limit" \
          -d '{"symbol":"BTC-USDT","side":"BUY","ord_type":"LIMIT","price":"50000","qty":"0.01"}' > /dev/null ;;
      2) # invalid token
        post "$BASE/v1/order/market" \
          -H "Authorization: Bearer invalidtoken123" \
          -d '{"symbol":"BTC-USDT","side":"SELL","ord_type":"MARKET","qty":"0.01"}' > /dev/null ;;
      3) # malformed JSON
        curl -s -o /dev/null -X POST "$BASE/v1/auth/register" \
          -H "Content-Type: application/json" \
          -d 'not_json_at_all' ;;
      4) # missing required fields
        post "$BASE/v1/auth/register" \
          -d '{"username":"x"}' > /dev/null ;;
      5) # wrong method
        curl -s -o /dev/null -X GET "$BASE/v1/order/limit" ;;
      6) # unknown route probe
        get "$BASE/v1/admin/$(rand_int 1 999)" > /dev/null ;;
      7) # price not allowed on market
        post "$BASE/v1/order/market" \
          -H "Authorization: Bearer $(get_token)" \
          -d "{\"user_id\":1,\"client_ord_id\":\"bad-$(rand_int 1 9999)\",\"symbol\":\"BTC-USDT\",\"side\":\"BUY\",\"ord_type\":\"MARKET\",\"price\":\"50000\",\"qty\":\"0.01\"}" > /dev/null ;;
      8) # negative qty
        post "$BASE/v1/order/limit" \
          -H "Authorization: Bearer $(get_token)" \
          -d "{\"user_id\":1,\"client_ord_id\":\"neg-$(rand_int 1 9999)\",\"symbol\":\"BTC-USDT\",\"side\":\"BUY\",\"ord_type\":\"LIMIT\",\"price\":\"50000\",\"qty\":\"-0.01\"}" > /dev/null ;;
    esac

    count=$((count+1))
    sleep_rand 200 800
  done
  echo "[bad_actor_$id] done — $count actions"
}

# ── launch all personas ───────────────────────────────────────────────────────

echo "=============================================="
echo "  CoinHub User Simulation"
echo "  Duration : ${DURATION}s (~$(( DURATION / 60 )) min)"
echo "  Target   : $BASE"
echo "=============================================="

# verify app is up
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$BASE/v1/ping")
if [ "$STATUS" != "200" ]; then
  echo "ERROR: app not reachable at $BASE (got $STATUS). Is it running?"
  exit 1
fi
echo "App is up. Spawning personas..."
echo ""

active_trader 1 &
active_trader 2 &
scalper 1 &
scalper 2 &
market_maker 1 &
lurker 1 &
lurker 2 &
lurker 3 &
bad_actor 1 &

ALL_PIDS=$!

# progress ticker
while [ $SECONDS -lt $END ]; do
  remaining=$(( END - SECONDS ))
  echo "[sim] $(( DURATION - remaining ))s elapsed — ${remaining}s remaining"
  sleep 30
done

wait
echo ""
echo "=============================================="
echo "  Simulation complete."
echo "=============================================="
