-- db/schema.sql

CREATE TABLE IF NOT EXISTS listings (
  listing_id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  city TEXT NOT NULL,
  url TEXT NOT NULL,

  title TEXT,
  description TEXT,

  price INT,
  bedrooms REAL,
  bathrooms REAL,
  sqft INT,

  address TEXT,
  neighborhood TEXT,
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION,

  amenities JSONB DEFAULT '[]'::jsonb,
  pet_policy TEXT,
  photo_urls JSONB DEFAULT '[]'::jsonb,

  scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  raw JSONB -- optional: store the full raw parsed record for safety
);

-- Helpful indexes for demo searches
CREATE INDEX IF NOT EXISTS idx_listings_city_price ON listings(city, price);
CREATE INDEX IF NOT EXISTS idx_listings_city_beds ON listings(city, bedrooms);
CREATE INDEX IF NOT EXISTS idx_listings_neighborhood ON listings(neighborhood);
CREATE INDEX IF NOT EXISTS idx_listings_amenities_gin ON listings USING GIN (amenities);
CREATE INDEX IF NOT EXISTS idx_listings_photo_urls_gin ON listings USING GIN (photo_urls);