-- Seed assets
-- This creates EVM-compatible cryptocurrency assets for trading

-- Ethereum (ETH) - Native EVM token
INSERT INTO assets (
    id,
    name,
    symbol,
    max_size,
    network_availability,
    status,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000100'::uuid,
    'Ethereum',
    'ETH',
    10000.0,
    '{"spot_testnet": true, "spot_mainnet": true, "perp_testnet": true, "perp_mainnet": true}'::jsonb,
    'active',
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;

-- Tether (USDT) - ERC-20
INSERT INTO assets (
    id,
    name,
    symbol,
    max_size,
    network_availability,
    status,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000101'::uuid,
    'Tether',
    'USDT',
    1000000.0,
    '{"spot_testnet": true, "spot_mainnet": true, "perp_testnet": true, "perp_mainnet": true}'::jsonb,
    'active',
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;

-- USD Coin (USDC) - ERC-20
INSERT INTO assets (
    id,
    name,
    symbol,
    max_size,
    network_availability,
    status,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000102'::uuid,
    'USD Coin',
    'USDC',
    1000000.0,
    '{"spot_testnet": true, "spot_mainnet": true, "perp_testnet": true, "perp_mainnet": true}'::jsonb,
    'active',
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;

-- Dai Stablecoin (DAI) - ERC-20
INSERT INTO assets (
    id,
    name,
    symbol,
    max_size,
    network_availability,
    status,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000103'::uuid,
    'Dai Stablecoin',
    'DAI',
    1000000.0,
    '{"spot_testnet": true, "spot_mainnet": true, "perp_testnet": true, "perp_mainnet": true}'::jsonb,
    'active',
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;

-- Wrapped Bitcoin (WBTC) - ERC-20
INSERT INTO assets (
    id,
    name,
    symbol,
    max_size,
    network_availability,
    status,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000104'::uuid,
    'Wrapped Bitcoin',
    'WBTC',
    1000.0,
    '{"spot_testnet": true, "spot_mainnet": true, "perp_testnet": true, "perp_mainnet": true}'::jsonb,
    'active',
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;

-- Chainlink (LINK) - ERC-20
INSERT INTO assets (
    id,
    name,
    symbol,
    max_size,
    network_availability,
    status,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000105'::uuid,
    'Chainlink',
    'LINK',
    500000.0,
    '{"spot_testnet": true, "spot_mainnet": true, "perp_testnet": true, "perp_mainnet": true}'::jsonb,
    'active',
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;

-- Uniswap (UNI) - ERC-20
INSERT INTO assets (
    id,
    name,
    symbol,
    max_size,
    network_availability,
    status,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000106'::uuid,
    'Uniswap',
    'UNI',
    1000000.0,
    '{"spot_testnet": true, "spot_mainnet": true, "perp_testnet": true, "perp_mainnet": true}'::jsonb,
    'active',
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;

-- Aave (AAVE) - ERC-20
INSERT INTO assets (
    id,
    name,
    symbol,
    max_size,
    network_availability,
    status,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000107'::uuid,
    'Aave',
    'AAVE',
    100000.0,
    '{"spot_testnet": true, "spot_mainnet": true, "perp_testnet": true, "perp_mainnet": true}'::jsonb,
    'active',
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;

-- Polygon (MATIC) - ERC-20 compatible
INSERT INTO assets (
    id,
    name,
    symbol,
    max_size,
    network_availability,
    status,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000108'::uuid,
    'Polygon',
    'MATIC',
    10000000.0,
    '{"spot_testnet": true, "spot_mainnet": true, "perp_testnet": true, "perp_mainnet": true}'::jsonb,
    'active',
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;
