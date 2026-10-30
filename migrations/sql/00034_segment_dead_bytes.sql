-- +goose Up
-- dead_bytes records how much of a shipped catalog segment was already dead
-- when it shipped: the bytes of its CAR frames holding blocks its last
-- op-root no longer reached. NULL means it wasn't measured, as for every
-- segment shipped before this column existed.
ALTER TABLE ingot.segments ADD COLUMN dead_bytes BIGINT;

-- +goose Down
ALTER TABLE ingot.segments DROP COLUMN dead_bytes;
