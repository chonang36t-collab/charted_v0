-- Migration: Add location and site columns to financial_metrics table
-- Run this SQL script on your PostgreSQL database

-- Add location column if it doesn't exist
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'financial_metrics' AND column_name = 'location'
    ) THEN
        ALTER TABLE financial_metrics ADD COLUMN location VARCHAR(100);
        RAISE NOTICE 'Added location column';
    ELSE
        RAISE NOTICE 'location column already exists';
    END IF;
END $$;

-- Add site column if it doesn't exist
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'financial_metrics' AND column_name = 'site'
    ) THEN
        ALTER TABLE financial_metrics ADD COLUMN site VARCHAR(100);
        RAISE NOTICE 'Added site column';
    ELSE
        RAISE NOTICE 'site column already exists';
    END IF;
END $$;

-- Verify the columns were added
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'financial_metrics' 
ORDER BY ordinal_position;
