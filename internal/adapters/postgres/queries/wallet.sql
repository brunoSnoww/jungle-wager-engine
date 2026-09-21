-- name: AllocateID :one
SELECT uuidv7()::text AS id;

-- name: GetWalletForUpdate :one
SELECT id::text AS id,player_id::text AS player_id,balance_minor,currency,version,created_at,updated_at
FROM wallet WHERE id=$1 FOR UPDATE;

-- name: GetWallet :one
SELECT id::text AS id,player_id::text AS player_id,balance_minor,currency,version,created_at,updated_at
FROM wallet WHERE id=$1;
