-- Seed system user
-- This creates a default system user for administrative purposes

INSERT INTO users (
    id,
    username,
    password,
    gmail,
    gmail_verification_status,
    gmail_verified_at,
    status,
    is_verified,
    local_timezone,
    role_id,
    created_at,
    updated_at
) VALUES (
    '00000000-0000-0000-0000-000000000001'::uuid,
    'system_user',
    'randompassowrdgeneratedbyargon',
    'system@coinhub.com',
    'verified',
    NOW(),
    'active',
    true,
    'UTC',
    3, -- system role
    NOW(),
    NOW()
)
ON CONFLICT (id) DO NOTHING;

-- Insert system user profile
INSERT INTO profiles (
    firstname,
    lastname,
    avatar_url,
    bio,
    user_id,
    created_at,
    updated_at
) VALUES (
    'System',
    'Administrator',
    NULL,
    'System administrator account for CoinHub',
    '00000000-0000-0000-0000-000000000001'::uuid,
    NOW(),
    NOW()
)
ON CONFLICT (user_id) DO NOTHING;

