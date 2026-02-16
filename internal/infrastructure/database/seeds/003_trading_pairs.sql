-- Seed trading pairs
-- This creates common trading pairs for spot and perpetual trading with EVM-compatible tokens
-- Note: BaseAssetID and QuoteAssetID reference assets from 002_assets.sql

-- ETH/USDT
INSERT INTO trading_pairs (
    id,
    base_asset_id,
    quote_asset_id,
    max_leverage,
    sz_decimal,
    tick_size,
    pair_network_availability,
    status,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000200'::uuid,
    '00000000-0000-0000-0000-000000000100'::uuid, -- ETH
    '00000000-0000-0000-0000-000000000101'::uuid, -- USDT
    100,  -- Max leverage 100x
    6,    -- Size decimal places
    2,    -- Tick size
    '{"testnet": true, "mainnet": true}'::jsonb,
    'active',
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;

-- ETH/USDC
INSERT INTO trading_pairs (
    id,
    base_asset_id,
    quote_asset_id,
    max_leverage,
    sz_decimal,
    tick_size,
    pair_network_availability,
    status,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000201'::uuid,
    '00000000-0000-0000-0000-000000000100'::uuid, -- ETH
    '00000000-0000-0000-0000-000000000102'::uuid, -- USDC
    100,  -- Max leverage 100x
    6,    -- Size decimal places
    2,    -- Tick size
    '{"testnet": true, "mainnet": true}'::jsonb,
    'active',
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;

-- WBTC/USDT
INSERT INTO trading_pairs (
    id,
    base_asset_id,
    quote_asset_id,
    max_leverage,
    sz_decimal,
    tick_size,
    pair_network_availability,
    status,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000202'::uuid,
    '00000000-0000-0000-0000-000000000104'::uuid, -- WBTC
    '00000000-0000-0000-0000-000000000101'::uuid, -- USDT
    125,  -- Max leverage 125x
    8,    -- Size decimal places
    2,    -- Tick size
    '{"testnet": true, "mainnet": true}'::jsonb,
    'active',
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;

-- WBTC/USDC
INSERT INTO trading_pairs (
    id,
    base_asset_id,
    quote_asset_id,
    max_leverage,
    sz_decimal,
    tick_size,
    pair_network_availability,
    status,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000203'::uuid,
    '00000000-0000-0000-0000-000000000104'::uuid, -- WBTC
    '00000000-0000-0000-0000-000000000102'::uuid, -- USDC
    125,  -- Max leverage 125x
    8,    -- Size decimal places
    2,    -- Tick size
    '{"testnet": true, "mainnet": true}'::jsonb,
    'active',
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;

-- LINK/USDT
INSERT INTO trading_pairs (
    id,
    base_asset_id,
    quote_asset_id,
    max_leverage,
    sz_decimal,
    tick_size,
    pair_network_availability,
    status,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000204'::uuid,
    '00000000-0000-0000-0000-000000000105'::uuid, -- LINK
    '00000000-0000-0000-0000-000000000101'::uuid, -- USDT
    50,   -- Max leverage 50x
    6,    -- Size decimal places
    2,    -- Tick size
    '{"testnet": true, "mainnet": true}'::jsonb,
    'active',
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;

-- UNI/USDT
INSERT INTO trading_pairs (
    id,
    base_asset_id,
    quote_asset_id,
    max_leverage,
    sz_decimal,
    tick_size,
    pair_network_availability,
    status,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000205'::uuid,
    '00000000-0000-0000-0000-000000000106'::uuid, -- UNI
    '00000000-0000-0000-0000-000000000101'::uuid, -- USDT
    50,   -- Max leverage 50x
    6,    -- Size decimal places
    2,    -- Tick size
    '{"testnet": true, "mainnet": true}'::jsonb,
    'active',
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;

-- AAVE/USDT
INSERT INTO trading_pairs (
    id,
    base_asset_id,
    quote_asset_id,
    max_leverage,
    sz_decimal,
    tick_size,
    pair_network_availability,
    status,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000206'::uuid,
    '00000000-0000-0000-0000-000000000107'::uuid, -- AAVE
    '00000000-0000-0000-0000-000000000101'::uuid, -- USDT
    50,   -- Max leverage 50x
    6,    -- Size decimal places
    2,    -- Tick size
    '{"testnet": true, "mainnet": true}'::jsonb,
    'active',
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;

-- MATIC/USDT
INSERT INTO trading_pairs (
    id,
    base_asset_id,
    quote_asset_id,
    max_leverage,
    sz_decimal,
    tick_size,
    pair_network_availability,
    status,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000207'::uuid,
    '00000000-0000-0000-0000-000000000108'::uuid, -- MATIC
    '00000000-0000-0000-0000-000000000101'::uuid, -- USDT
    75,   -- Max leverage 75x
    6,    -- Size decimal places
    2,    -- Tick size
    '{"testnet": true, "mainnet": true}'::jsonb,
    'active',
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;
