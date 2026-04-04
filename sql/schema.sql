-- =============================================================================
-- SPX Options Surface Schema
-- PostgreSQL 16+, monthly range partitions on trade_date
--
-- Delta convention: unified put delta 5-95, stored as integers.
--   5  = |Δ_put| = 0.05  (deep OTM put, from OTM put quotes)
--   50 = |Δ_put| = 0.50  (ATM)
--   95 = |Δ_put| = 0.95  (deep ITM put, derived from OTM call quotes)
--
-- Greeks convention: forward greeks (underlying = forward price F).
--   Vega  = dV/dσ  = e^(-rT) * F * N'(d1) * √T
--   Gamma = d²V/dF² = e^(-rT) * N'(d1) / (F * σ * √T)
--   Theta = dV/dt  expressed per calendar day
--   Price = BS put price using forward F and smoothed IV
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Main surface table
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS spx_surface (
    timestamp   TIMESTAMPTZ      NOT NULL,
    trade_date  DATE             NOT NULL,
    dte         SMALLINT         NOT NULL,
    put_delta   SMALLINT         NOT NULL,
    iv          DOUBLE PRECISION NOT NULL,
    price       DOUBLE PRECISION,
    theta       DOUBLE PRECISION,
    vega        DOUBLE PRECISION,
    gamma       DOUBLE PRECISION
) PARTITION BY RANGE (trade_date);

CREATE UNIQUE INDEX IF NOT EXISTS spx_surface_uq
    ON spx_surface (trade_date, timestamp, dte, put_delta);

CREATE INDEX IF NOT EXISTS spx_surface_lookup
    ON spx_surface (timestamp, dte, put_delta);

-- ---------------------------------------------------------------------------
-- ATM table: true ATM point per (timestamp, DTE)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS spx_atm (
    timestamp     TIMESTAMPTZ      NOT NULL,
    trade_date    DATE             NOT NULL,
    dte           SMALLINT         NOT NULL,
    atm_put_delta DOUBLE PRECISION NOT NULL,  -- exact forward put delta at ATM (≈ -0.5)
    atm_strike    DOUBLE PRECISION NOT NULL,
    atm_iv        DOUBLE PRECISION NOT NULL,
    atm_forward   DOUBLE PRECISION NOT NULL
) PARTITION BY RANGE (trade_date);

CREATE UNIQUE INDEX IF NOT EXISTS spx_atm_uq
    ON spx_atm (trade_date, timestamp, dte);

CREATE INDEX IF NOT EXISTS spx_atm_lookup
    ON spx_atm (timestamp, dte);

-- ---------------------------------------------------------------------------
-- Diagnostics: one row per expiry per snapshot
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS spx_surface_diagnostics (
    timestamp           TIMESTAMPTZ      NOT NULL,
    trade_date          DATE             NOT NULL,
    expiry              DATE             NOT NULL,
    expiry_type         CHAR(2)          NOT NULL CHECK (expiry_type IN ('AM', 'PM')),
    dte_actual          DOUBLE PRECISION,           -- continuous DTE at snapshot time
    forward_price       DOUBLE PRECISION,
    risk_free_rate      DOUBLE PRECISION,
    n_strikes_raw       INTEGER,
    n_strikes_clean     INTEGER,
    spline_rmse         DOUBLE PRECISION,
    calendar_arb_flag   BOOLEAN          NOT NULL DEFAULT FALSE,
    butterfly_arb_flag  BOOLEAN          NOT NULL DEFAULT FALSE,
    skipped             BOOLEAN          NOT NULL DEFAULT FALSE,
    skip_reason         TEXT,
    PRIMARY KEY (timestamp, expiry)
);

-- ---------------------------------------------------------------------------
-- Partition helper: create monthly partition for spx_surface if missing
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ensure_surface_partition(p_date DATE)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_name  TEXT;
    v_start DATE;
    v_end   DATE;
BEGIN
    v_start := DATE_TRUNC('month', p_date)::DATE;
    v_end   := (DATE_TRUNC('month', p_date) + INTERVAL '1 month')::DATE;
    v_name  := 'spx_surface_' || TO_CHAR(p_date, 'YYYY_MM');
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = v_name AND n.nspname = current_schema()
    ) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF spx_surface '
            'FOR VALUES FROM (%L) TO (%L)',
            v_name, v_start, v_end
        );
    END IF;
END;
$$;

-- ---------------------------------------------------------------------------
-- Partition helper: create monthly partition for spx_atm if missing
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ensure_atm_partition(p_date DATE)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_name  TEXT;
    v_start DATE;
    v_end   DATE;
BEGIN
    v_start := DATE_TRUNC('month', p_date)::DATE;
    v_end   := (DATE_TRUNC('month', p_date) + INTERVAL '1 month')::DATE;
    v_name  := 'spx_atm_' || TO_CHAR(p_date, 'YYYY_MM');
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = v_name AND n.nspname = current_schema()
    ) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF spx_atm '
            'FOR VALUES FROM (%L) TO (%L)',
            v_name, v_start, v_end
        );
    END IF;
END;
$$;
