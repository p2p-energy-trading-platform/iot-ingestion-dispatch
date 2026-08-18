-- +goose Up

CREATE TABLE migration_smoke_test (
    id BIGSERIAL PRIMARY KEY,
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO migration_smoke_test (message)
VALUES ('migration system is working');

-- +goose Down

DROP TABLE IF EXISTS migration_smoke_test;