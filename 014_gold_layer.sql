-- ============================================================================
-- Gold Layer: Pre-computed CAMEL ratios, dashboard values view, peer RPC
-- ============================================================================
--
-- The Gold layer provides business-ready metrics computed from Silver tables.
-- It replaces the old EAV-pattern tables (call_report_ratios, call_report_values)
-- with:
--   1. gold_ratios        - Wide table: one row per CU per quarter with all ratios
--   2. gold_dashboard_values - View: joins Silver tables for dashboard display values
--   3. get_gold_peer_percentiles() - RPC: peer comparison by asset tier
--
-- Architecture:
--   Bronze (raw JSONB) --> Silver (typed columns) --> Gold (ratios + views)
-- ============================================================================


-- =========================================================================
-- 1. gold_ratios
-- One wide row per CU per quarter with all CAMEL metrics pre-computed.
-- Populated by scripts/compute-gold.js from Silver tables.
-- =========================================================================

CREATE TABLE gold_ratios (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  charter_number INT NOT NULL,
  credit_union_id UUID REFERENCES credit_unions(id),
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  computed_at TIMESTAMPTZ DEFAULT now(),

  -- -----------------------------------------------------------------------
  -- CAPITAL
  -- -----------------------------------------------------------------------
  net_worth_ratio NUMERIC,             -- total_net_worth / total_assets * 100
  net_worth NUMERIC,                   -- total_net_worth (absolute)
  net_worth_growth_yoy NUMERIC,        -- YoY % change in total_net_worth

  -- -----------------------------------------------------------------------
  -- ASSET QUALITY
  -- -----------------------------------------------------------------------
  delinquency_rate NUMERIC,            -- total_delinquent_loans / total_loans * 100
  charge_off_ratio NUMERIC,            -- net_charge_offs / avg_loans * 100, annualized
  net_charge_offs NUMERIC,             -- charge_offs - recoveries (absolute)
  allowance_to_loans NUMERIC,          -- allowance / total_loans * 100
  coverage_ratio NUMERIC,              -- allowance / delinquent_loans * 100

  -- Concentrations
  auto_loan_concentration NUMERIC,     -- (new + used vehicle) / total_loans * 100
  real_estate_concentration NUMERIC,   -- (first + junior + other RE) / total_loans * 100
  commercial_concentration NUMERIC,    -- total_commercial / total_loans * 100
  credit_card_concentration NUMERIC,   -- credit_card / total_loans * 100

  -- -----------------------------------------------------------------------
  -- MANAGEMENT
  -- -----------------------------------------------------------------------
  asset_growth_yoy NUMERIC,            -- YoY % change in total_assets
  loan_growth_yoy NUMERIC,             -- YoY % change in total_loans
  share_growth_yoy NUMERIC,            -- YoY % change in total_shares
  member_growth_yoy NUMERIC,           -- YoY % change in members
  total_members NUMERIC,               -- num_current_members (absolute)
  total_employees NUMERIC,             -- full_time + 0.5 * part_time
  members_per_employee NUMERIC,        -- members / total_employees
  assets_per_employee NUMERIC,         -- total_assets / total_employees

  -- -----------------------------------------------------------------------
  -- EARNINGS
  -- -----------------------------------------------------------------------
  roa NUMERIC,                         -- net_income / total_assets * 100, annualized
  roaa NUMERIC,                        -- net_income / avg_assets * 100, annualized
  roe NUMERIC,                         -- net_income / net_worth * 100, annualized
  efficiency_ratio NUMERIC,            -- non_int_expense / (NII + non_int_income) * 100
  net_interest_margin NUMERIC,         -- (int_income - int_expense) / avg_assets * 100, ann
  yield_on_loans NUMERIC,              -- interest_on_loans / total_loans * 100, annualized
  cost_of_funds NUMERIC,               -- int_expense / total_shares * 100, annualized
  non_interest_income_ratio NUMERIC,   -- non_int_income / (NII + non_int_income) * 100
  gross_income NUMERIC,                -- total_interest_income + total_non_interest_income
  total_assets NUMERIC,                -- silver_assets.total_assets (absolute, for display)
  total_loans NUMERIC,                 -- total_loans_and_leases (absolute)
  total_shares NUMERIC,                -- total_shares_and_deposits (absolute)
  total_borrowings NUMERIC,            -- total_borrowings (absolute)

  -- -----------------------------------------------------------------------
  -- LIQUIDITY
  -- -----------------------------------------------------------------------
  cash_ratio NUMERIC,                  -- total_cash / total_assets * 100
  loan_to_share_ratio NUMERIC,         -- total_loans / total_shares * 100
  loans_to_assets_ratio NUMERIC,       -- total_loans / total_assets * 100
  borrowings_to_assets NUMERIC,        -- total_borrowings / total_assets * 100

  UNIQUE(charter_number, year, quarter)
);

CREATE INDEX idx_gold_ratios_charter ON gold_ratios(charter_number);
CREATE INDEX idx_gold_ratios_cu_id ON gold_ratios(credit_union_id);
CREATE INDEX idx_gold_ratios_period ON gold_ratios(year, quarter);
CREATE INDEX idx_gold_ratios_peer ON gold_ratios(year, quarter, charter_number);
CREATE INDEX idx_gold_ratios_assets ON gold_ratios(total_assets);

ALTER TABLE gold_ratios ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Gold ratios readable by authenticated users"
  ON gold_ratios FOR SELECT
  TO authenticated
  USING (true);


-- =========================================================================
-- 2. gold_dashboard_values
-- SQL view joining Silver tables to surface the ~42 account code values
-- the dashboard needs. Replaces call_report_values queries.
-- =========================================================================

CREATE OR REPLACE VIEW gold_dashboard_values
WITH (security_invoker = true)
AS
SELECT
  a.charter_number,
  cu.id AS credit_union_id,
  a.year,
  a.quarter,
  a.period,

  -- -----------------------------------------------------------------------
  -- Income & Expense (silver_revenue, silver_expenses, silver_net_income)
  -- -----------------------------------------------------------------------
  r.total_interest_income,                          -- 115
  r.interest_on_loans,                              -- 110
  r.income_from_investments,                        -- 120
  r.total_non_interest_income,                      -- 117
  r.fee_income,                                     -- 131
  e.total_interest_expense,                         -- 350
  e.interest_on_borrowed_money,                     -- 340
  e.dividends_on_shares,                            -- 380
  e.interest_on_deposits_scu,                       -- 381
  e.total_non_interest_expense,                     -- 671
  e.provision_for_loan_losses,                      -- 300
  e.cecl_total_credit_loss_expense,                 -- IS0017
  ni.net_income,                                    -- 661A

  -- -----------------------------------------------------------------------
  -- Assets (silver_assets)
  -- -----------------------------------------------------------------------
  a.total_assets,                                   -- 010
  a.total_cash_and_deposits,                        -- AS0009
  a.total_loans_and_leases,                         -- 025B
  a.allowance_for_loan_losses,                      -- 719
  a.land_and_building,                              -- 007
  a.other_fixed_assets,                             -- 008
  a.ncua_share_insurance_deposit,                   -- 794
  a.total_foreclosed_repossessed,                   -- 798A
  a.accrued_interest_on_loans,                      -- 009A
  a.accrued_interest_on_investments,                -- 009B
  a.all_other_assets,                               -- 009C
  a.loans_held_for_sale,                            -- 003

  -- -----------------------------------------------------------------------
  -- Investments (silver_investments)
  -- -----------------------------------------------------------------------
  inv.total_investment_securities,                   -- AS0013
  inv.total_other_investments,                       -- AS0017

  -- -----------------------------------------------------------------------
  -- Capital (silver_capital)
  -- -----------------------------------------------------------------------
  cap.cecl_allowance_loans_leases,                  -- AS0048
  cap.undivided_earnings,                           -- 940
  cap.total_net_worth,                              -- 997
  cap.unrealized_gains_afs_debt_securities,         -- EQ0009

  -- -----------------------------------------------------------------------
  -- Liabilities (silver_liabilities)
  -- -----------------------------------------------------------------------
  l.total_shares_and_deposits,                      -- 018
  l.accounts_payable_and_other_liabilities,         -- 825
  l.accrued_dividends_interest_payable,             -- 820A

  -- -----------------------------------------------------------------------
  -- Liquidity (silver_liquidity)
  -- -----------------------------------------------------------------------
  lq.total_borrowings,                              -- 860C

  -- -----------------------------------------------------------------------
  -- Delinquency (silver_delinquency)
  -- -----------------------------------------------------------------------
  d.total_delinquent_loans,                         -- 041B

  -- -----------------------------------------------------------------------
  -- Charge-offs (silver_charge_offs)
  -- -----------------------------------------------------------------------
  co.total_charge_offs_ytd,                         -- 550
  co.total_recoveries_ytd,                          -- 551

  -- -----------------------------------------------------------------------
  -- Loan Concentrations (silver_loan_composition, silver_commercial_loans)
  -- -----------------------------------------------------------------------
  lc.used_vehicle_loans,                            -- 370
  lc.new_vehicle_loans,                             -- 385
  lc.junior_lien_re_loans,                          -- 386A
  lc.all_other_re_loans,                            -- 386B
  lc.unsecured_credit_card_loans,                   -- 396
  lc.first_lien_re_loans,                           -- 703A
  cl.total_commercial_loans,                        -- 400T1

  -- -----------------------------------------------------------------------
  -- Operations (silver_operations)
  -- -----------------------------------------------------------------------
  o.num_current_members,                            -- 083
  o.num_potential_members,                          -- 084
  o.num_full_time_employees,                        -- 564A
  o.num_part_time_employees,                        -- 564B
  o.num_atm_locations,                              -- 566

  -- -----------------------------------------------------------------------
  -- Loan counts (silver_loan_composition)
  -- -----------------------------------------------------------------------
  lc.num_total_loans_leases,                        -- 025A
  lc.num_loans_granted_ytd                          -- 031A

FROM silver_assets a
JOIN credit_unions cu ON cu.charter_number = a.charter_number
LEFT JOIN silver_revenue r
  ON r.charter_number = a.charter_number AND r.year = a.year AND r.quarter = a.quarter
LEFT JOIN silver_expenses e
  ON e.charter_number = a.charter_number AND e.year = a.year AND e.quarter = a.quarter
LEFT JOIN silver_net_income ni
  ON ni.charter_number = a.charter_number AND ni.year = a.year AND ni.quarter = a.quarter
LEFT JOIN silver_investments inv
  ON inv.charter_number = a.charter_number AND inv.year = a.year AND inv.quarter = a.quarter
LEFT JOIN silver_capital cap
  ON cap.charter_number = a.charter_number AND cap.year = a.year AND cap.quarter = a.quarter
LEFT JOIN silver_liabilities l
  ON l.charter_number = a.charter_number AND l.year = a.year AND l.quarter = a.quarter
LEFT JOIN silver_liquidity lq
  ON lq.charter_number = a.charter_number AND lq.year = a.year AND lq.quarter = a.quarter
LEFT JOIN silver_delinquency d
  ON d.charter_number = a.charter_number AND d.year = a.year AND d.quarter = a.quarter
LEFT JOIN silver_charge_offs co
  ON co.charter_number = a.charter_number AND co.year = a.year AND co.quarter = a.quarter
LEFT JOIN silver_loan_composition lc
  ON lc.charter_number = a.charter_number AND lc.year = a.year AND lc.quarter = a.quarter
LEFT JOIN silver_commercial_loans cl
  ON cl.charter_number = a.charter_number AND cl.year = a.year AND cl.quarter = a.quarter
LEFT JOIN silver_operations o
  ON o.charter_number = a.charter_number AND o.year = a.year AND o.quarter = a.quarter;

REVOKE ALL ON gold_dashboard_values FROM anon, public;
GRANT SELECT ON gold_dashboard_values TO authenticated;


-- =========================================================================
-- 3. get_gold_peer_percentiles()
-- RPC function for peer comparison. Reads from gold_ratios.
-- Accepts credit_union_id (UUID) + asset tier bounds.
-- Returns the same shape as the old get_peer_percentiles().
-- =========================================================================

CREATE OR REPLACE FUNCTION get_gold_peer_percentiles(
  p_credit_union_id UUID,
  p_asset_tier_min BIGINT,
  p_asset_tier_max BIGINT
)
RETURNS TABLE (
  ratio_name TEXT,
  ratio_value NUMERIC,
  peer_median NUMERIC,
  peer_count BIGINT,
  values_below BIGINT,
  values_equal BIGINT,
  category TEXT
)
LANGUAGE sql
STABLE
AS $$
  WITH latest_period AS (
    -- Find the latest quarter that has data for the target CU
    SELECT year, quarter
    FROM gold_ratios
    WHERE credit_union_id = p_credit_union_id
    ORDER BY year DESC, quarter DESC
    LIMIT 1
  ),
  target AS (
    SELECT gr.*
    FROM gold_ratios gr
    JOIN latest_period lp ON gr.year = lp.year AND gr.quarter = lp.quarter
    WHERE gr.credit_union_id = p_credit_union_id
  ),
  peers AS (
    -- Get latest data for each peer CU in the asset tier
    SELECT DISTINCT ON (gr.charter_number) gr.*
    FROM gold_ratios gr
    JOIN latest_period lp ON gr.year = lp.year AND gr.quarter = lp.quarter
    WHERE gr.total_assets >= p_asset_tier_min
      AND gr.total_assets < p_asset_tier_max
      AND gr.credit_union_id IS DISTINCT FROM p_credit_union_id
  ),
  -- Unpivot target into (ratio_name, ratio_value, category) rows
  target_metrics AS (
    SELECT v.ratio_name, v.ratio_value, v.category
    FROM target t,
    LATERAL (VALUES
      -- Capital
      ('net_worth_ratio',            t.net_worth_ratio,            'capital'),
      ('net_worth',                  t.net_worth,                  'capital'),
      ('net_worth_growth_yoy',       t.net_worth_growth_yoy,       'capital'),
      -- Asset Quality
      ('delinquency_rate',           t.delinquency_rate,           'asset_quality'),
      ('charge_off_ratio',           t.charge_off_ratio,           'asset_quality'),
      ('net_charge_offs',            t.net_charge_offs,            'asset_quality'),
      ('allowance_to_loans',         t.allowance_to_loans,         'asset_quality'),
      ('coverage_ratio',             t.coverage_ratio,             'asset_quality'),
      ('auto_loan_concentration',    t.auto_loan_concentration,    'asset_quality'),
      ('real_estate_concentration',  t.real_estate_concentration,  'asset_quality'),
      ('commercial_concentration',   t.commercial_concentration,   'asset_quality'),
      ('credit_card_concentration',  t.credit_card_concentration,  'asset_quality'),
      -- Management
      ('asset_growth_yoy',           t.asset_growth_yoy,           'management'),
      ('loan_growth_yoy',            t.loan_growth_yoy,            'management'),
      ('share_growth_yoy',           t.share_growth_yoy,           'management'),
      ('member_growth_yoy',          t.member_growth_yoy,          'management'),
      ('total_members',              t.total_members,              'management'),
      ('total_employees',            t.total_employees,            'management'),
      ('members_per_employee',       t.members_per_employee,       'management'),
      ('assets_per_employee',        t.assets_per_employee,        'management'),
      -- Earnings
      ('roa',                        t.roa,                        'earnings'),
      ('roaa',                       t.roaa,                       'earnings'),
      ('roe',                        t.roe,                        'earnings'),
      ('efficiency_ratio',           t.efficiency_ratio,           'earnings'),
      ('net_interest_margin',        t.net_interest_margin,        'earnings'),
      ('yield_on_loans',             t.yield_on_loans,             'earnings'),
      ('cost_of_funds',              t.cost_of_funds,              'earnings'),
      ('non_interest_income_ratio',  t.non_interest_income_ratio,  'earnings'),
      ('gross_income',               t.gross_income,               'earnings'),
      ('total_assets',               t.total_assets,               'earnings'),
      ('total_loans',                t.total_loans,                'earnings'),
      ('total_shares',               t.total_shares,               'liquidity'),
      ('total_borrowings',           t.total_borrowings,           'liquidity'),
      -- Liquidity
      ('cash_ratio',                 t.cash_ratio,                 'liquidity'),
      ('loan_to_share_ratio',        t.loan_to_share_ratio,        'liquidity'),
      ('loans_to_assets_ratio',      t.loans_to_assets_ratio,      'liquidity'),
      ('borrowings_to_assets',       t.borrowings_to_assets,       'liquidity')
    ) AS v(ratio_name, ratio_value, category)
    WHERE v.ratio_value IS NOT NULL
  ),
  -- Unpivot peers into (charter_number, ratio_name, ratio_value) rows
  peer_metrics AS (
    SELECT p.charter_number, v.ratio_name, v.ratio_value
    FROM peers p,
    LATERAL (VALUES
      ('net_worth_ratio',            p.net_worth_ratio),
      ('net_worth',                  p.net_worth),
      ('net_worth_growth_yoy',       p.net_worth_growth_yoy),
      ('delinquency_rate',           p.delinquency_rate),
      ('charge_off_ratio',           p.charge_off_ratio),
      ('net_charge_offs',            p.net_charge_offs),
      ('allowance_to_loans',         p.allowance_to_loans),
      ('coverage_ratio',             p.coverage_ratio),
      ('auto_loan_concentration',    p.auto_loan_concentration),
      ('real_estate_concentration',  p.real_estate_concentration),
      ('commercial_concentration',   p.commercial_concentration),
      ('credit_card_concentration',  p.credit_card_concentration),
      ('asset_growth_yoy',           p.asset_growth_yoy),
      ('loan_growth_yoy',            p.loan_growth_yoy),
      ('share_growth_yoy',           p.share_growth_yoy),
      ('member_growth_yoy',          p.member_growth_yoy),
      ('total_members',              p.total_members),
      ('total_employees',            p.total_employees),
      ('members_per_employee',       p.members_per_employee),
      ('assets_per_employee',        p.assets_per_employee),
      ('roa',                        p.roa),
      ('roaa',                       p.roaa),
      ('roe',                        p.roe),
      ('efficiency_ratio',           p.efficiency_ratio),
      ('net_interest_margin',        p.net_interest_margin),
      ('yield_on_loans',             p.yield_on_loans),
      ('cost_of_funds',              p.cost_of_funds),
      ('non_interest_income_ratio',  p.non_interest_income_ratio),
      ('gross_income',               p.gross_income),
      ('total_assets',               p.total_assets),
      ('total_loans',                p.total_loans),
      ('total_shares',               p.total_shares),
      ('total_borrowings',           p.total_borrowings),
      ('cash_ratio',                 p.cash_ratio),
      ('loan_to_share_ratio',        p.loan_to_share_ratio),
      ('loans_to_assets_ratio',      p.loans_to_assets_ratio),
      ('borrowings_to_assets',       p.borrowings_to_assets)
    ) AS v(ratio_name, ratio_value)
    WHERE v.ratio_value IS NOT NULL
  ),
  -- Aggregate peer stats per metric
  peer_agg AS (
    SELECT
      tm.ratio_name,
      tm.ratio_value,
      tm.category,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pm.ratio_value) AS median_val,
      COUNT(pm.*) AS cnt,
      COUNT(*) FILTER (WHERE pm.ratio_value < tm.ratio_value) AS below,
      COUNT(*) FILTER (WHERE pm.ratio_value = tm.ratio_value) AS equal
    FROM target_metrics tm
    JOIN peer_metrics pm ON pm.ratio_name = tm.ratio_name
    GROUP BY tm.ratio_name, tm.ratio_value, tm.category
  )
  SELECT
    pa.ratio_name,
    pa.ratio_value,
    pa.median_val AS peer_median,
    pa.cnt AS peer_count,
    pa.below AS values_below,
    pa.equal AS values_equal,
    pa.category
  FROM peer_agg pa;
$$;
