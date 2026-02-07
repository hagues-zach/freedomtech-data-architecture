-- Bronze layer: two tables for clean separation of concerns
--
-- bronze_call_reports: Raw NCUA account codes (~780 numeric values per CU per quarter)
-- bronze_cu_profiles:  Raw FOICU institutional profile data (~24 fields per CU per quarter)
--
-- One row per credit union per quarter in each table (~4,900 rows/quarter each).

-- Financial data (account codes from FS220 files)
CREATE TABLE bronze_call_reports (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  charter_number INT NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  filing_date DATE NOT NULL,
  raw_data JSONB NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(charter_number, year, quarter)
);

CREATE INDEX idx_bronze_reports_charter ON bronze_call_reports(charter_number);
CREATE INDEX idx_bronze_reports_period ON bronze_call_reports(year, quarter);
CREATE INDEX idx_bronze_reports_filing_date ON bronze_call_reports(filing_date);

ALTER TABLE bronze_call_reports ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Bronze call reports readable by authenticated users"
  ON bronze_call_reports FOR SELECT
  TO authenticated
  USING (true);

-- Institutional profile data (from FOICU.txt)
CREATE TABLE bronze_cu_profiles (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  charter_number INT NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  filing_date DATE NOT NULL,
  raw_data JSONB NOT NULL,
  source_url TEXT,
  imported_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(charter_number, year, quarter)
);

CREATE INDEX idx_bronze_profiles_charter ON bronze_cu_profiles(charter_number);
CREATE INDEX idx_bronze_profiles_period ON bronze_cu_profiles(year, quarter);
CREATE INDEX idx_bronze_profiles_filing_date ON bronze_cu_profiles(filing_date);

ALTER TABLE bronze_cu_profiles ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Bronze CU profiles readable by authenticated users"
  ON bronze_cu_profiles FOR SELECT
  TO authenticated
  USING (true);
