"""
Simple inputs for the multi-exchange market-making framework.

No environment variables are used by this project. No JSON config file is used.
Edit this file locally before running.

IMPORTANT:
- Use an API wallet private key where possible.
- WALLET_ADDRESS should be the main Hyperliquid account address.
- PRIVATE_KEY can be an approved API wallet private key.
- This is mainnet by default and places real orders.
"""

# =============================================================================
# PROGRAM
# =============================================================================
CONNECTOR = "hyperliquid"
STRATEGY_SCRIPT = "market_making"

# =============================================================================
# HYPERLIQUID ACCOUNT
# =============================================================================
WALLET_ADDRESS = ""
PRIVATE_KEY = ""

# =============================================================================
# MARKETS
# =============================================================================
# Connector market names. You can use UI-style names like BTC-USDC and
# HIP3/builder names like xyz:AMD-USDC; the connector normalizes them to the
# Hyperliquid API coin names BTC and xyz:AMD.
MARKETS = ["xyz:AMD-USDC"]

# Backward-compatible alias for older utilities.
COINS = MARKETS

# =============================================================================
# DATABASES
# =============================================================================
PRIMARY_SQL_URL = "sqlite:///./primary_sql_mirror.sqlite"
SQLITE_URL = "sqlite:///./local_redundant.sqlite"
REPLICATION_SPOOL_FILE = "./replication_failures.jsonl"

# =============================================================================
# ORDER / RISK SETTINGS
# =============================================================================
TARGET_ORDER_NOTIONAL_USD = 10.5
MIN_OPEN_ORDER_NOTIONAL_USD = 10.5
MAX_LONG_INVENTORY_NOTIONAL_USD = 10.5
MAX_SHORT_INVENTORY_NOTIONAL_USD = 10.5
MAX_GROSS_OPEN_ORDER_NOTIONAL_USD = 500.0
ORDERS_PER_SIDE = 1
ORDER_LEVEL_SPACING_BPS = 2.0

# Hyperliquid leverage changes affect required margin, not submitted exposure.
SYNC_MAX_LEVERAGE = False
LEVERAGE_IS_CROSS = True
LEVERAGE_SYNC_INTERVAL_S = 1800.0

# Backward-compatible alias for older scripts.
SET_MAX_LEVERAGE_ON_START = SYNC_MAX_LEVERAGE

# Backward-compatible aliases for older scripts.
MIN_ORDER_USD = MIN_OPEN_ORDER_NOTIONAL_USD
MAX_POSITION_NOTIONAL_USD = max(MAX_LONG_INVENTORY_NOTIONAL_USD, MAX_SHORT_INVENTORY_NOTIONAL_USD)

POST_ONLY_TIF = "Alo"              # Hyperliquid add-liquidity-only / post-only
REDUCE_ONLY_TO_CLOSE_INVENTORY = True

# Backward-compatible alias for older scripts.
REDUCE_ONLY_WHEN_CAPPING = REDUCE_ONLY_TO_CLOSE_INVENTORY
MANAGE_ALL_ORDERS_ON_COINS = True   # Cancels manual orders on COINS. Use a dedicated wallet.
CANCEL_ALL_ON_START = False
CANCEL_ON_SHUTDOWN = True
CANCEL_SYMBOL_ORDERS_ON_FILL = True
CANCEL_WAIT_S = 1.0
CANCEL_POLL_S = 0.25
CANCEL_ON_FILL_GUARD_S = 5.0
CANCEL_ON_FILL_FORCE_REST_REFRESH = False

BULK_EDIT_INTERVAL_S = 1.0
REPRICE_IF_PRICE_MOVES_BPS = 2.0
OPEN_ORDERS_RECONCILE_S = 60.0
INVENTORY_BALANCE_REFRESH_S = 10.0
MAINTENANCE_LOOP_INTERVAL_S = 1.0
LOOP_INTERVAL_S = 0.2
FORCE_REST_REFRESH_AFTER_ORDER_REJECT = False
ACTION_RATE_LIMIT_COOLDOWN_S = 120.0
ACTION_RATE_LIMIT_LOG_S = 10.0
ACTION_RATE_LIMIT_MIN_REMAINING_REQUESTS = 1.0
MAX_SPREAD_BPS = 50.0
MIN_QUOTE_SPREAD_BPS = 1.0

# EV spread pricing chooses the half-spread from real public trade expectancy.
# The strategy estimates fills/hour for each spread from cached websocket trades;
# it does not optimize fills/hour as a free variable.
USE_EV_SPREAD_PRICING = True
EV_MIN_HALF_SPREAD_BPS = 1.0
EV_MAX_HALF_SPREAD_BPS = 50.0
EV_HALF_SPREAD_STEP_BPS = 1.0
EV_MAX_FILLS_PER_HOUR = 200.0
EV_TRADE_LOOKBACK_S = 3600.0
EV_SWEEP_GROUP_MS = 100.0
EV_DEPTH_WITHIN_MID_PCT = 0.001
EV_USE_CURRENT_BOOK_DEPTH = True
EV_MIN_TRADE_SAMPLES = 20
# EV uses half-spread minus a fixed trading fee, so net edge = spread_bps - MAKER_FEE_BPS_PER_SIDE
MAKER_FEE_BPS_PER_SIDE = 3.0
EV_MARKOUT_BPS = 0.0
EV_REQUIRE_READY_BEFORE_OPENING = True
EV_STARTUP_USE_STORED_MARKET_DATA = True
EV_STARTUP_WAIT_TIMEOUT_S = 0.0  # 0 means wait until EV is ready.
EV_STARTUP_FALLBACK_AFTER_TIMEOUT = False
EV_REUSE_LAST_CHOICE_WHEN_NOT_READY = True
EV_CACHED_CHOICE_MAX_AGE_S = 600.0
EV_STARTUP_BACKFILL_RECENT_MARKET_TRADES = False
EV_STARTUP_RECENT_MARKET_TRADES_LIMIT = 2000
USER_FILLS_BACKFILL_LOOKBACK_HOURS = 0.0
USER_FILLS_AGGREGATE_BY_TIME = False

# LOB VWAP fair price uses only book levels close to current mid.
USE_LOB_VWAP_FAIR_PRICE = True
LOB_VWAP_WINDOW_S = 2.0
LOB_VWAP_WITHIN_MID_PCT = 0.001
LOB_VWAP_MAX_LEVELS = 50

# Use websocket L2 snapshots in the quote-edit hot path. REST is a fallback
# while the websocket warms up or if its latest snapshot becomes stale.
USE_WS_MARKET_DATA = True
WS_ORDER_BOOK_STALE_S = 3.0
WS_RECONNECT_DELAY_S = 1.0
WS_PUBLIC_TRADE_CACHE_ITEMS = 200000
WS_PUBLIC_TRADE_CACHE_RETENTION_S = 3600.0
WS_BBO_STREAM_ENABLED = True
# Hyperliquid accepts nSigFigs null, 5, 4, 3, or 2. sig1 is not valid.
# Raw levels remain authoritative. Coarser feeds only extend uncovered depth.
WS_MULTI_RESOLUTION_BOOK_ENABLED = False
WS_BOOK_N_SIG_FIGS = (5, 4, 3, 2)
WS_BOOK_RESOLUTION_PRIORITY = ("raw", "sig5", "sig4", "sig3", "sig2")

# =============================================================================
# MARKET-DATA RECORDING / LOCAL BOOKMAP VIEWER
# =============================================================================
# Record full websocket-backed L2 snapshots and model diagnostics in a
# dedicated SQLite file. This writer runs outside the quote-edit hot path.
MARKET_DATA_RECORDING_ENABLED = True
MARKET_DATA_RECORD_INTERVAL_S = 1.0
MARKET_DATA_DB_PATH = "./market_data.sqlite"
MARKET_DATA_RETENTION_HOURS = 24.0  # Use 0 to keep history indefinitely.
MARKET_DATA_CLEANUP_INTERVAL_S = 60.0
MARKET_DATA_WEB_HOST = "127.0.0.1"
MARKET_DATA_WEB_PORT = 8765
MARKET_DATA_WEB_DEFAULT_MINUTES = 5
MARKET_DATA_WEB_MAX_SNAPSHOTS = 1500
ACCOUNT_EQUITY_RECORDING_ENABLED = False
ACCOUNT_EQUITY_REFRESH_S = 60.0
MARKET_DATA_ACCOUNT_REST_ENABLED = False

# When enabled, keep the closest quote one price step in front of the selected
# LOB depth percentile. Levels farther from the midpoint follow it.
USE_LOB_PERCENTILE_GUARD = True
LOB_PERCENTILE = 0.001
LOB_WITHIN_MID_PCT = 0.05
# Hyperliquid publishes at most 20 levels per side for each l2Book resolution.
# Keep the cap higher so raw and non-overlapping outward aggregate levels fit.
LOB_MAX_LEVELS = 1000
MARKET_DATA_BOOK_LEVEL_LIMIT = 1000
MARKET_DATA_LOB_PERCENTILES = (0.001, 0.005, 0.01, 0.02, 0.05, 0.10, 0.25)

# Backward-compatible aliases for older scripts.
ORDER_REFRESH_S = BULK_EDIT_INTERVAL_S
REPLACE_IF_PRICE_MOVES_BPS = REPRICE_IF_PRICE_MOVES_BPS
FETCH_POSITIONS_S = INVENTORY_BALANCE_REFRESH_S

# =============================================================================
# AVELLANEDA–STOIKOV SETTINGS
# =============================================================================
GAMMA = 0.05
K = 100.0
MIN_HALF_SPREAD_BPS = 1.0
MAX_HALF_SPREAD_BPS = 50.0
HORIZON_SECONDS = 30.0
INVENTORY_SKEW_SCALE = 1.0
VOL_WINDOW = 120
FALLBACK_SIGMA_PER_S = 0.00005

# =============================================================================
# POLLING / LOGGING
# =============================================================================
ORDER_BOOK_DEPTH = 20
FETCH_FILLS_S = 0.0  # 0 disables REST fill polling; websocket order updates remain active.
RECONCILE_DB_S = 60.0
KILL_SWITCH_FILE = "./kill_switch.json"
LOCK_FILE = "./hl_multi_program_mm.lock"
LOG_FILE = "./hl_multi_program_mm.log"
LOG_LEVEL = "INFO"
