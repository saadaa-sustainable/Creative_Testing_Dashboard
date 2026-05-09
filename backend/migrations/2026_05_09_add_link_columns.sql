-- Migration: add preview_link and ad_link to primary_table
-- Date: 2026-05-09
-- Run once against Supabase (psql / Supabase SQL editor). Idempotent.

ALTER TABLE primary_table
  ADD COLUMN IF NOT EXISTS preview_link TEXT,
  ADD COLUMN IF NOT EXISTS ad_link      TEXT;

-- preview_link  → Meta /<ad_id>?fields=preview_shareable_link
-- ad_link       → Meta /<ad_id>?fields=creative{link_url, object_story_spec{link_data{link}}, object_url}
--                  picks the first non-empty in that order
