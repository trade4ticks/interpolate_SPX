-- Migration 001: replace timestamp TIMESTAMPTZ with trade_date DATE + quote_time TIME
--
-- Since primary keys are changing, the safest approach is to drop and recreate.
-- This is appropriate while still in the testing phase with only 1 day of data.
--
-- Run this ONCE on the VPS before the next pipeline run:
--   psql $SPX_DB_URL -f sql/migrate_001_drop_timestamp.sql
--   python main.py init-db

-- Drop existing tables (child partitions drop automatically)
DROP TABLE IF EXISTS spx_surface             CASCADE;
DROP TABLE IF EXISTS spx_atm                 CASCADE;
DROP TABLE IF EXISTS spx_surface_diagnostics CASCADE;
