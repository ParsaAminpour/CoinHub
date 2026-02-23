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
    asset_address,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000100'::uuid,
    'Ethereum',
    'ETH',
    10000.0,
    '{"spot_testnet": true, "spot_mainnet": true, "perp_testnet": true, "perp_mainnet": true}'::jsonb,
    'active',
    '0x0000000000000000000000000000000000000000',
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
    asset_address,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000101'::uuid,
    'Tether',
    'USDT',
    1000000.0,
    '{"spot_testnet": true, "spot_mainnet": true, "perp_testnet": true, "perp_mainnet": true}'::jsonb,
    'active',
    '0xdAC17F958D2ee523a2206206994597C13D831ec7',
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
    asset_address,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000102'::uuid,
    'USD Coin',
    'USDC',
    1000000.0,
    '{"spot_testnet": true, "spot_mainnet": true, "perp_testnet": true, "perp_mainnet": true}'::jsonb,
    'active',
    '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
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
    asset_address,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000103'::uuid,
    'Dai Stablecoin',
    'DAI',
    1000000.0,
    '{"spot_testnet": true, "spot_mainnet": true, "perp_testnet": true, "perp_mainnet": true}'::jsonb,
    'active',
    '0x6B175474E89094C44Da98b954EedeAC495271d0F',
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
    asset_address,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000104'::uuid,
    'Wrapped Bitcoin',
    'WBTC',
    1000.0,
    '{"spot_testnet": true, "spot_mainnet": true, "perp_testnet": true, "perp_mainnet": true}'::jsonb,
    'active',
    '0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599',
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
    asset_address,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000105'::uuid,
    'Chainlink',
    'LINK',
    500000.0,
    '{"spot_testnet": true, "spot_mainnet": true, "perp_testnet": true, "perp_mainnet": true}'::jsonb,
    'active',
    '0x514910771AF9Ca656af840dff83E8264EcF986CA',
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
    asset_address,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000106'::uuid,
    'Uniswap',
    'UNI',
    1000000.0,
    '{"spot_testnet": true, "spot_mainnet": true, "perp_testnet": true, "perp_mainnet": true}'::jsonb,
    'active',
    '0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984',
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
    asset_address,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000107'::uuid,
    'Aave',
    'AAVE',
    100000.0,
    '{"spot_testnet": true, "spot_mainnet": true, "perp_testnet": true, "perp_mainnet": true}'::jsonb,
    'active',
    '0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9',
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
    asset_address,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000108'::uuid,
    'Polygon',
    'MATIC',
    10000000.0,
    '{"spot_testnet": true, "spot_mainnet": true, "perp_testnet": true, "perp_mainnet": true}'::jsonb,
    'active',
    '0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0',
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;
