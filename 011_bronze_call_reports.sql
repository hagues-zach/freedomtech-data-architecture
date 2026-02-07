-- Bronze layer: one table per NCUA text file, raw data preserved as JSONB.
--
-- 18 tables total (1 FOICU + 17 FS220 variants).
-- Each row stores a single credit union's data for one quarter,
-- with the full row from the source file as an untouched JSONB blob.

DO $$
DECLARE
  tables TEXT[] := ARRAY[
    'bronze_foicu',
    'bronze_fs220', 'bronze_fs220a', 'bronze_fs220b', 'bronze_fs220c',
    'bronze_fs220d', 'bronze_fs220g', 'bronze_fs220h', 'bronze_fs220i',
    'bronze_fs220j', 'bronze_fs220k', 'bronze_fs220l', 'bronze_fs220m',
    'bronze_fs220n', 'bronze_fs220p', 'bronze_fs220q', 'bronze_fs220r',
    'bronze_fs220s'
  ];
  t TEXT;
BEGIN
  FOREACH t IN ARRAY tables LOOP
    EXECUTE format('
      CREATE TABLE IF NOT EXISTS %I (
        id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
        cu_number TEXT NOT NULL,
        cycle_date TEXT,
        year INT NOT NULL,
        quarter INT NOT NULL,
        period TEXT NOT NULL,
        raw_data JSONB NOT NULL,
        source_url TEXT,
        imported_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE(cu_number, year, quarter)
      )', t);

    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_cu ON %I(cu_number)', t, t);
    EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_period ON %I(year, quarter)', t, t);

    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);

    EXECUTE format('
      CREATE POLICY IF NOT EXISTS "% readable by authenticated users"
        ON %I FOR SELECT
        TO authenticated
        USING (true)
    ', initcap(replace(t, '_', ' ')), t);
  END LOOP;
END $$;
