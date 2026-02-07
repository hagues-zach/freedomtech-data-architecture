-- Bronze layer: one table per NCUA text file, raw columns preserved as TEXT.
--
-- 18 tables total (1 FOICU + 17 FS220 variants).
-- Each row stores a single credit union's data for one quarter.
-- All source file values are stored as TEXT, preserving exact raw content.
--
-- Only common key columns are defined here. The import script dynamically
-- adds TEXT columns via ALTER TABLE ADD COLUMN for every column header
-- found in each source file, using the original header names as-is.


-- ---------------------------------------------------------------------------
-- Helper RPC functions for dynamic schema management
-- ---------------------------------------------------------------------------

-- Returns all column names for a given table
CREATE OR REPLACE FUNCTION get_column_names(p_table_name TEXT)
RETURNS TEXT[]
LANGUAGE sql
STABLE
AS $$
  SELECT array_agg(column_name::TEXT)
  FROM information_schema.columns
  WHERE table_schema = 'public'
    AND table_name = p_table_name;
$$;

-- Adds a TEXT column to a table (no-op if it already exists)
-- SECURITY DEFINER lets the service role call ALTER TABLE on tables it doesn't own.
CREATE OR REPLACE FUNCTION add_text_column(p_table_name TEXT, p_column_name TEXT)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS %I TEXT', p_table_name, p_column_name);
END;
$$;

-- Tells PostgREST to reload its schema cache (required after dynamic column adds)
CREATE OR REPLACE FUNCTION reload_schema_cache()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  NOTIFY pgrst, 'reload schema';
END;
$$;


-- ---------------------------------------------------------------------------
-- Bronze tables
-- ---------------------------------------------------------------------------

-- 1. bronze_foicu
CREATE TABLE bronze_foicu (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cu_number TEXT NOT NULL,
  cycle_date TEXT,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(cu_number, year, quarter)
);
CREATE INDEX idx_bronze_foicu_cu ON bronze_foicu(cu_number);
CREATE INDEX idx_bronze_foicu_period ON bronze_foicu(year, quarter);
ALTER TABLE bronze_foicu ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Bronze foicu readable by authenticated users"
  ON bronze_foicu FOR SELECT TO authenticated USING (true);


-- 2. bronze_fs220
CREATE TABLE bronze_fs220 (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cu_number TEXT NOT NULL,
  cycle_date TEXT,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(cu_number, year, quarter)
);
CREATE INDEX idx_bronze_fs220_cu ON bronze_fs220(cu_number);
CREATE INDEX idx_bronze_fs220_period ON bronze_fs220(year, quarter);
ALTER TABLE bronze_fs220 ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Bronze fs220 readable by authenticated users"
  ON bronze_fs220 FOR SELECT TO authenticated USING (true);


-- 3. bronze_fs220a
CREATE TABLE bronze_fs220a (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cu_number TEXT NOT NULL,
  cycle_date TEXT,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(cu_number, year, quarter)
);
CREATE INDEX idx_bronze_fs220a_cu ON bronze_fs220a(cu_number);
CREATE INDEX idx_bronze_fs220a_period ON bronze_fs220a(year, quarter);
ALTER TABLE bronze_fs220a ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Bronze fs220a readable by authenticated users"
  ON bronze_fs220a FOR SELECT TO authenticated USING (true);


-- 4. bronze_fs220b
CREATE TABLE bronze_fs220b (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cu_number TEXT NOT NULL,
  cycle_date TEXT,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(cu_number, year, quarter)
);
CREATE INDEX idx_bronze_fs220b_cu ON bronze_fs220b(cu_number);
CREATE INDEX idx_bronze_fs220b_period ON bronze_fs220b(year, quarter);
ALTER TABLE bronze_fs220b ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Bronze fs220b readable by authenticated users"
  ON bronze_fs220b FOR SELECT TO authenticated USING (true);


-- 5. bronze_fs220c
CREATE TABLE bronze_fs220c (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cu_number TEXT NOT NULL,
  cycle_date TEXT,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(cu_number, year, quarter)
);
CREATE INDEX idx_bronze_fs220c_cu ON bronze_fs220c(cu_number);
CREATE INDEX idx_bronze_fs220c_period ON bronze_fs220c(year, quarter);
ALTER TABLE bronze_fs220c ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Bronze fs220c readable by authenticated users"
  ON bronze_fs220c FOR SELECT TO authenticated USING (true);


-- 6. bronze_fs220d
CREATE TABLE bronze_fs220d (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cu_number TEXT NOT NULL,
  cycle_date TEXT,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(cu_number, year, quarter)
);
CREATE INDEX idx_bronze_fs220d_cu ON bronze_fs220d(cu_number);
CREATE INDEX idx_bronze_fs220d_period ON bronze_fs220d(year, quarter);
ALTER TABLE bronze_fs220d ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Bronze fs220d readable by authenticated users"
  ON bronze_fs220d FOR SELECT TO authenticated USING (true);


-- 7. bronze_fs220g
CREATE TABLE bronze_fs220g (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cu_number TEXT NOT NULL,
  cycle_date TEXT,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(cu_number, year, quarter)
);
CREATE INDEX idx_bronze_fs220g_cu ON bronze_fs220g(cu_number);
CREATE INDEX idx_bronze_fs220g_period ON bronze_fs220g(year, quarter);
ALTER TABLE bronze_fs220g ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Bronze fs220g readable by authenticated users"
  ON bronze_fs220g FOR SELECT TO authenticated USING (true);


-- 8. bronze_fs220h
CREATE TABLE bronze_fs220h (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cu_number TEXT NOT NULL,
  cycle_date TEXT,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(cu_number, year, quarter)
);
CREATE INDEX idx_bronze_fs220h_cu ON bronze_fs220h(cu_number);
CREATE INDEX idx_bronze_fs220h_period ON bronze_fs220h(year, quarter);
ALTER TABLE bronze_fs220h ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Bronze fs220h readable by authenticated users"
  ON bronze_fs220h FOR SELECT TO authenticated USING (true);


-- 9. bronze_fs220i
CREATE TABLE bronze_fs220i (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cu_number TEXT NOT NULL,
  cycle_date TEXT,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(cu_number, year, quarter)
);
CREATE INDEX idx_bronze_fs220i_cu ON bronze_fs220i(cu_number);
CREATE INDEX idx_bronze_fs220i_period ON bronze_fs220i(year, quarter);
ALTER TABLE bronze_fs220i ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Bronze fs220i readable by authenticated users"
  ON bronze_fs220i FOR SELECT TO authenticated USING (true);


-- 10. bronze_fs220j
CREATE TABLE bronze_fs220j (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cu_number TEXT NOT NULL,
  cycle_date TEXT,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(cu_number, year, quarter)
);
CREATE INDEX idx_bronze_fs220j_cu ON bronze_fs220j(cu_number);
CREATE INDEX idx_bronze_fs220j_period ON bronze_fs220j(year, quarter);
ALTER TABLE bronze_fs220j ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Bronze fs220j readable by authenticated users"
  ON bronze_fs220j FOR SELECT TO authenticated USING (true);


-- 11. bronze_fs220k
CREATE TABLE bronze_fs220k (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cu_number TEXT NOT NULL,
  cycle_date TEXT,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(cu_number, year, quarter)
);
CREATE INDEX idx_bronze_fs220k_cu ON bronze_fs220k(cu_number);
CREATE INDEX idx_bronze_fs220k_period ON bronze_fs220k(year, quarter);
ALTER TABLE bronze_fs220k ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Bronze fs220k readable by authenticated users"
  ON bronze_fs220k FOR SELECT TO authenticated USING (true);


-- 12. bronze_fs220l
CREATE TABLE bronze_fs220l (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cu_number TEXT NOT NULL,
  cycle_date TEXT,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(cu_number, year, quarter)
);
CREATE INDEX idx_bronze_fs220l_cu ON bronze_fs220l(cu_number);
CREATE INDEX idx_bronze_fs220l_period ON bronze_fs220l(year, quarter);
ALTER TABLE bronze_fs220l ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Bronze fs220l readable by authenticated users"
  ON bronze_fs220l FOR SELECT TO authenticated USING (true);


-- 13. bronze_fs220m
CREATE TABLE bronze_fs220m (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cu_number TEXT NOT NULL,
  cycle_date TEXT,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(cu_number, year, quarter)
);
CREATE INDEX idx_bronze_fs220m_cu ON bronze_fs220m(cu_number);
CREATE INDEX idx_bronze_fs220m_period ON bronze_fs220m(year, quarter);
ALTER TABLE bronze_fs220m ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Bronze fs220m readable by authenticated users"
  ON bronze_fs220m FOR SELECT TO authenticated USING (true);


-- 14. bronze_fs220n
CREATE TABLE bronze_fs220n (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cu_number TEXT NOT NULL,
  cycle_date TEXT,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(cu_number, year, quarter)
);
CREATE INDEX idx_bronze_fs220n_cu ON bronze_fs220n(cu_number);
CREATE INDEX idx_bronze_fs220n_period ON bronze_fs220n(year, quarter);
ALTER TABLE bronze_fs220n ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Bronze fs220n readable by authenticated users"
  ON bronze_fs220n FOR SELECT TO authenticated USING (true);


-- 15. bronze_fs220p
CREATE TABLE bronze_fs220p (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cu_number TEXT NOT NULL,
  cycle_date TEXT,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(cu_number, year, quarter)
);
CREATE INDEX idx_bronze_fs220p_cu ON bronze_fs220p(cu_number);
CREATE INDEX idx_bronze_fs220p_period ON bronze_fs220p(year, quarter);
ALTER TABLE bronze_fs220p ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Bronze fs220p readable by authenticated users"
  ON bronze_fs220p FOR SELECT TO authenticated USING (true);


-- 16. bronze_fs220q
CREATE TABLE bronze_fs220q (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cu_number TEXT NOT NULL,
  cycle_date TEXT,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(cu_number, year, quarter)
);
CREATE INDEX idx_bronze_fs220q_cu ON bronze_fs220q(cu_number);
CREATE INDEX idx_bronze_fs220q_period ON bronze_fs220q(year, quarter);
ALTER TABLE bronze_fs220q ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Bronze fs220q readable by authenticated users"
  ON bronze_fs220q FOR SELECT TO authenticated USING (true);


-- 17. bronze_fs220r
CREATE TABLE bronze_fs220r (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cu_number TEXT NOT NULL,
  cycle_date TEXT,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(cu_number, year, quarter)
);
CREATE INDEX idx_bronze_fs220r_cu ON bronze_fs220r(cu_number);
CREATE INDEX idx_bronze_fs220r_period ON bronze_fs220r(year, quarter);
ALTER TABLE bronze_fs220r ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Bronze fs220r readable by authenticated users"
  ON bronze_fs220r FOR SELECT TO authenticated USING (true);


-- 18. bronze_fs220s
CREATE TABLE bronze_fs220s (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  cu_number TEXT NOT NULL,
  cycle_date TEXT,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(cu_number, year, quarter)
);
CREATE INDEX idx_bronze_fs220s_cu ON bronze_fs220s(cu_number);
CREATE INDEX idx_bronze_fs220s_period ON bronze_fs220s(year, quarter);
ALTER TABLE bronze_fs220s ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Bronze fs220s readable by authenticated users"
  ON bronze_fs220s FOR SELECT TO authenticated USING (true);
