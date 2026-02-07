-- ============================================================================
-- Silver Layer Tables
-- ============================================================================
--
-- The Silver layer transforms raw Bronze JSONB blobs into typed, columnar tables
-- organized by financial domain. Each table represents one analytical area of
-- the NCUA 5300 Call Report.
--
-- Architecture:
--   Bronze (raw JSONB)  -->  Silver (typed columns)  -->  Gold (ratios/metrics)
--
-- Every table follows the same pattern:
--   - UUID primary key
--   - charter_number / year / quarter / period for identification
--   - UNIQUE constraint on (charter_number, year, quarter)
--   - Indexes on charter_number and (year, quarter)
--   - RLS enabled with authenticated SELECT policy
--   - All financial columns are NUMERIC (no precision constraint)
--   - NCUA account code noted as SQL comment on each column
--
-- Tables (15 total):
--   1.  silver_assets              - Balance sheet: assets, cash, investments summary
--   2.  silver_liabilities         - Shares, deposits, payables, accrued dividends
--   3.  silver_capital             - Net worth, reserves, CECL allowances
--   4.  silver_revenue             - Interest/non-interest income, gains/losses
--   5.  silver_expenses            - Compensation, occupancy, provisions, CECL expense
--   6.  silver_net_income          - Net income + derived aggregates
--   7.  silver_loan_composition    - Loan balances by type, counts, rates
--   8.  silver_delinquency         - Aging buckets (30/60/90/180/360+) by loan type
--   9.  silver_charge_offs         - Charge-offs and recoveries by loan type
--   10. silver_commercial_loans    - MBL breakdowns, originations, participations
--   11. silver_real_estate         - RE loan detail, 1-4 family, rate/term
--   12. silver_investments         - Securities by type, maturity, fair value
--   13. silver_liquidity           - Borrowings, capacity, off-balance sheet
--   14. silver_operations          - Members, employees, derivatives, misc
--   15. silver_risk_based_capital  - Full RBC schedule + CCULR
--
-- Column naming:
--   - 532 codes with NCUA definitions get human-readable snake_case names
--   - ~249 codes without definitions get acct_ prefixed names (e.g., acct_rb0001)
--   - Readable names can be backfilled in a future migration
-- ============================================================================


-- =========================================================================
-- 1. silver_assets  (~35 columns)
-- Balance sheet left side: total assets, cash, investments summary,
-- loans total, fixed assets, foreclosed, other assets
-- =========================================================================

CREATE TABLE silver_assets (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  charter_number INT NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  transformed_at TIMESTAMPTZ DEFAULT now(),

  -- Asset balances
  leases_receivable NUMERIC,                          -- 002
  loans_held_for_sale NUMERIC,                        -- 003
  land_and_building NUMERIC,                          -- 007
  other_fixed_assets NUMERIC,                         -- 008
  accrued_interest_on_loans NUMERIC,                  -- 009A
  accrued_interest_on_investments NUMERIC,             -- 009B
  all_other_assets NUMERIC,                           -- 009C
  goodwill NUMERIC,                                   -- 009D2
  total_assets NUMERIC,                               -- 010
  avg_daily_assets NUMERIC,                           -- 010A
  avg_monthly_assets NUMERIC,                         -- 010B
  avg_quarterly_assets NUMERIC,                       -- 010C
  total_liabilities_shares_equity NUMERIC,             -- 014
  total_loans_and_leases NUMERIC,                     -- 025B
  allowance_for_loan_losses NUMERIC,                  -- 719
  cash_on_hand NUMERIC,                               -- 730A
  cash_on_deposit NUMERIC,                            -- 730B
  cash_on_deposit_corporate_cu NUMERIC,               -- 730B1
  cash_on_deposit_other_fi NUMERIC,                   -- 730B2
  mortgage_servicing_assets NUMERIC,                  -- 779
  ncua_share_insurance_deposit NUMERIC,               -- 794
  total_foreclosed_repossessed NUMERIC,               -- 798A
  split_dollar_life_insurance_collateral NUMERIC,     -- 789E
  split_dollar_life_insurance_endorsement NUMERIC,    -- 789E1
  other_insurance_assets NUMERIC,                     -- 789E2
  other_non_insurance_assets NUMERIC,                 -- 789F
  cash_on_deposit_fed_reserve NUMERIC,                -- AS0003
  coin_and_currency NUMERIC,                          -- AS0004
  cash_items_in_collection NUMERIC,                   -- AS0005
  time_deposits_in_fi NUMERIC,                        -- AS0007
  all_other_deposits NUMERIC,                         -- AS0008
  total_cash_and_deposits NUMERIC,                    -- AS0009
  commercial_foreclosed_repossessed NUMERIC,          -- AS0022
  consumer_re_foreclosed_repossessed NUMERIC,         -- AS0023
  consumer_vehicle_foreclosed_repossessed NUMERIC,    -- AS0024
  consumer_other_foreclosed_repossessed NUMERIC,      -- AS0025
  other_intangible_assets NUMERIC,                    -- AS0032
  total_other_assets NUMERIC,                         -- AS0036

  UNIQUE(charter_number, year, quarter)
);

CREATE INDEX idx_silver_assets_charter ON silver_assets(charter_number);
CREATE INDEX idx_silver_assets_period ON silver_assets(year, quarter);

ALTER TABLE silver_assets ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Silver assets readable by authenticated users"
  ON silver_assets FOR SELECT
  TO authenticated
  USING (true);


-- =========================================================================
-- 2. silver_liabilities  (~50 columns)
-- Member shares/deposits by type, non-member deposits, payables,
-- accrued dividends, share account counts
-- =========================================================================

CREATE TABLE silver_liabilities (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  charter_number INT NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  transformed_at TIMESTAMPTZ DEFAULT now(),

  -- Shares and deposits
  member_shares_all_types NUMERIC,                    -- 013
  total_shares_lt_1yr NUMERIC,                        -- 013A
  total_shares_1_to_3yr NUMERIC,                      -- 013B1
  total_shares_gt_3yr NUMERIC,                        -- 013B2
  total_shares_and_deposits NUMERIC,                  -- 018
  total_shares_deposits_lt_1yr NUMERIC,               -- 018A
  total_shares_deposits_1_to_3yr NUMERIC,             -- 018B1
  total_shares_deposits_gt_3yr NUMERIC,               -- 018B2
  uninsured_member_shares_over_250k NUMERIC,          -- 065A4
  uninsured_nonmember_shares_over_250k NUMERIC,       -- 067A2
  total_uninsured_shares_over_250k NUMERIC,           -- 068A
  total_insured_shares_and_deposits NUMERIC,          -- 069A

  -- Account counts
  num_share_certificates NUMERIC,                     -- 451
  num_share_drafts NUMERIC,                           -- 452
  num_ira_keogh_accounts NUMERIC,                     -- 453
  num_regular_share_accounts NUMERIC,                 -- 454
  num_all_other_shares NUMERIC,                       -- 455
  num_nonmember_deposits NUMERIC,                     -- 457
  num_money_market_accounts NUMERIC,                  -- 458
  num_total_shares_deposits NUMERIC,                  -- 460

  -- Share detail by type
  all_other_shares NUMERIC,                           -- 630
  all_other_shares_lt_1yr NUMERIC,                    -- 630A
  all_other_shares_1_to_3yr NUMERIC,                  -- 630B1
  all_other_shares_gt_3yr NUMERIC,                    -- 630B2
  member_public_unit_accounts NUMERIC,                -- 631
  nonmember_public_unit_accounts NUMERIC,             -- 632
  non_dollar_denominated_deposits NUMERIC,            -- 636
  share_certificates_gte_100k NUMERIC,                -- 638
  ira_keogh_gte_100k NUMERIC,                         -- 639
  share_drafts_swept_to_regular NUMERIC,              -- 641
  commercial_share_accounts NUMERIC,                  -- 643
  negative_shares_in_unsecured_loans NUMERIC,         -- 644
  total_regular_shares NUMERIC,                       -- 657
  regular_shares_lt_1yr NUMERIC,                      -- 657A

  -- Accrued and payables
  accrued_dividends_interest_payable NUMERIC,          -- 820A
  accounts_payable_and_other_liabilities NUMERIC,      -- 825

  -- Non-member deposits
  nonmember_deposits NUMERIC,                         -- 880
  nonmember_deposits_lt_1yr NUMERIC,                  -- 880A
  nonmember_deposits_1_to_3yr NUMERIC,                -- 880B1
  nonmember_deposits_gt_3yr NUMERIC,                  -- 880B2

  -- Share drafts
  total_share_drafts NUMERIC,                         -- 902
  share_drafts_lt_1yr NUMERIC,                        -- 902A

  -- IRA/Keogh
  ira_keogh_lt_1yr NUMERIC,                           -- 906A
  ira_keogh_1_to_3yr NUMERIC,                         -- 906B1
  ira_keogh_gt_3yr NUMERIC,                           -- 906B2
  total_ira_keogh NUMERIC,                            -- 906C

  -- Share certificates
  share_certificates_lt_1yr NUMERIC,                  -- 908A
  share_certificates_1_to_3yr NUMERIC,                -- 908B1
  share_certificates_gt_3yr NUMERIC,                  -- 908B2
  total_share_certificates NUMERIC,                   -- 908C

  -- Money market
  total_money_market_shares NUMERIC,                  -- 911
  money_market_shares_lt_1yr NUMERIC,                 -- 911A

  -- Totals
  num_total_share_accounts NUMERIC,                   -- 966
  total_liabilities NUMERIC,                          -- LI0069
  total_shares NUMERIC,                               -- SH0013
  total_shares_and_deposits_sh NUMERIC,               -- SH0018
  total_nonmember_deposits_sh NUMERIC,                -- SH0880

  UNIQUE(charter_number, year, quarter)
);

CREATE INDEX idx_silver_liabilities_charter ON silver_liabilities(charter_number);
CREATE INDEX idx_silver_liabilities_period ON silver_liabilities(year, quarter);

ALTER TABLE silver_liabilities ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Silver liabilities readable by authenticated users"
  ON silver_liabilities FOR SELECT
  TO authenticated
  USING (true);


-- =========================================================================
-- 3. silver_capital  (~27 columns)
-- Net worth, undivided earnings, reserves, CECL allowances/adoption
-- =========================================================================

CREATE TABLE silver_capital (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  charter_number INT NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  transformed_at TIMESTAMPTZ DEFAULT now(),

  -- Reserves and equity
  other_reserves NUMERIC,                             -- 658
  equity_acquired_in_merger NUMERIC,                  -- 658A
  appropriation_nonconforming_investments NUMERIC,    -- 668
  net_worth_classification NUMERIC,                   -- 700
  net_worth_classification_new_cu NUMERIC,            -- 701
  nonperpetual_capital_account NUMERIC,               -- 769A
  perpetual_contributed_capital NUMERIC,              -- 769B
  subordinated_debt_in_net_worth NUMERIC,             -- 925A
  undivided_earnings NUMERIC,                         -- 940
  unrealized_gains_cash_flow_hedges NUMERIC,          -- 945A
  other_comprehensive_income NUMERIC,                 -- 945B
  unrealized_losses_otti_htm NUMERIC,                 -- 945C
  noncontrolling_interest NUMERIC,                    -- 996
  total_net_worth NUMERIC,                            -- 997
  net_worth_ratio NUMERIC,                            -- 998

  -- Business combination adjustments
  adjusted_retained_earnings_business_combo NUMERIC,          -- 1004
  prior_qtr_adjusted_retained_earnings_combo NUMERIC,         -- 1004A
  adjustments_retained_earnings_combo NUMERIC,                -- 1004B
  adjusted_gain_bargain_purchase_combo NUMERIC,               -- 1004C

  -- Other comprehensive income detail
  unrealized_gains_afs_debt_securities NUMERIC,       -- EQ0009

  -- CECL
  cecl_adoption_indicator NUMERIC,                    -- AS0010
  cecl_allowance_htm_securities NUMERIC,              -- AS0041
  cecl_allowance_afs_securities NUMERIC,              -- AS0042
  cecl_allowance_loans_leases NUMERIC,                -- AS0048
  cecl_allowance_off_balance_sheet NUMERIC,           -- LI0003
  cecl_adoption_date NUMERIC,                         -- NW0001
  cecl_one_time_adjustment NUMERIC,                   -- NW0002
  cecl_transition_provision NUMERIC,                  -- NW0004
  cecl_total_assets_excl_ppp NUMERIC,                 -- NW0010

  UNIQUE(charter_number, year, quarter)
);

CREATE INDEX idx_silver_capital_charter ON silver_capital(charter_number);
CREATE INDEX idx_silver_capital_period ON silver_capital(year, quarter);

ALTER TABLE silver_capital ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Silver capital readable by authenticated users"
  ON silver_capital FOR SELECT
  TO authenticated
  USING (true);


-- =========================================================================
-- 4. silver_revenue  (~21 columns)
-- Interest income by source, non-interest income, gains/losses
-- =========================================================================

CREATE TABLE silver_revenue (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  charter_number INT NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  transformed_at TIMESTAMPTZ DEFAULT now(),

  -- Interest income
  interest_on_loans NUMERIC,                          -- 110
  total_interest_income NUMERIC,                      -- 115
  less_interest_refunded NUMERIC,                     -- 119
  income_from_investments NUMERIC,                    -- 120

  -- Non-interest income
  total_non_interest_income NUMERIC,                  -- 117
  fee_income NUMERIC,                                 -- 131

  -- OTTI / impairment
  total_otti_losses NUMERIC,                          -- 420A
  otti_losses_in_oci NUMERIC,                         -- 420B
  otti_losses_in_earnings NUMERIC,                    -- 420C

  -- Gains and losses
  gain_loss_on_derivatives NUMERIC,                   -- 421
  gain_loss_disposition_fixed_assets NUMERIC,          -- 430
  gain_from_bargain_purchase NUMERIC,                 -- 431
  other_non_operating_income_expense NUMERIC,         -- 440
  net_income_not_in_undivided_earnings NUMERIC,       -- 602

  -- Derived / subtotals
  other_interest_income NUMERIC,                      -- IS0005
  net_interest_income NUMERIC,                        -- IS0010
  other_operating_income NUMERIC,                     -- IS0020
  gain_loss_on_loan_sales NUMERIC,                    -- IS0029
  gain_loss_on_oreo_sales NUMERIC,                    -- IS0030
  gain_loss_equity_trading_securities NUMERIC,        -- IS0046
  gain_loss_other_investments NUMERIC,                -- IS0047

  UNIQUE(charter_number, year, quarter)
);

CREATE INDEX idx_silver_revenue_charter ON silver_revenue(charter_number);
CREATE INDEX idx_silver_revenue_period ON silver_revenue(year, quarter);

ALTER TABLE silver_revenue ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Silver revenue readable by authenticated users"
  ON silver_revenue FOR SELECT
  TO authenticated
  USING (true);


-- =========================================================================
-- 5. silver_expenses  (~21 columns)
-- Compensation, occupancy, provision for losses, CECL credit loss expense
-- =========================================================================

CREATE TABLE silver_expenses (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  charter_number INT NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  transformed_at TIMESTAMPTZ DEFAULT now(),

  -- Operating expenses
  employee_compensation_benefits NUMERIC,             -- 210
  travel_and_conference NUMERIC,                      -- 230
  office_occupancy NUMERIC,                           -- 250
  office_operations NUMERIC,                          -- 260
  educational_promotional NUMERIC,                    -- 270
  loan_servicing NUMERIC,                             -- 280
  professional_outside_services NUMERIC,              -- 290
  provision_for_loan_losses NUMERIC,                  -- 300
  member_insurance NUMERIC,                           -- 310
  operating_fees NUMERIC,                             -- 320
  interest_on_borrowed_money NUMERIC,                 -- 340
  total_interest_expense NUMERIC,                     -- 350
  miscellaneous_operating NUMERIC,                    -- 360

  -- Dividends and interest expense
  dividends_on_shares NUMERIC,                        -- 380
  interest_on_deposits_scu NUMERIC,                   -- 381
  total_non_interest_expense NUMERIC,                 -- 671

  -- CECL credit loss expense
  cecl_loans_leases_credit_loss_expense NUMERIC,              -- IS0011
  cecl_afs_securities_credit_loss_expense NUMERIC,            -- IS0012
  cecl_htm_securities_credit_loss_expense NUMERIC,            -- IS0013
  cecl_off_balance_sheet_credit_loss_expense NUMERIC,         -- IS0016
  cecl_total_credit_loss_expense NUMERIC,                     -- IS0017

  UNIQUE(charter_number, year, quarter)
);

CREATE INDEX idx_silver_expenses_charter ON silver_expenses(charter_number);
CREATE INDEX idx_silver_expenses_period ON silver_expenses(year, quarter);

ALTER TABLE silver_expenses ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Silver expenses readable by authenticated users"
  ON silver_expenses FOR SELECT
  TO authenticated
  USING (true);


-- =========================================================================
-- 6. silver_net_income  (~10 columns)
-- Net income (661A) + derived aggregates computed during transformation
-- =========================================================================

CREATE TABLE silver_net_income (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  charter_number INT NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  transformed_at TIMESTAMPTZ DEFAULT now(),

  -- Raw from Bronze
  net_income NUMERIC,                                 -- 661A

  -- Derived columns (computed in transform-silver.js from revenue + expense codes)
  total_revenue NUMERIC,                              -- computed: interest_income + non_interest_income
  total_expenses NUMERIC,                             -- computed: interest_expense + non_interest_expense
  net_interest_income NUMERIC,                        -- computed: interest_income - interest_expense
  provision_or_credit_loss NUMERIC,                   -- CECL or legacy provision
  pre_provision_net_revenue NUMERIC,                  -- computed: net_interest_income + non_interest_income - non_interest_expense
  non_interest_income NUMERIC,                        -- copied from revenue for convenience
  non_interest_expense NUMERIC,                       -- copied from expenses for convenience

  UNIQUE(charter_number, year, quarter)
);

CREATE INDEX idx_silver_net_income_charter ON silver_net_income(charter_number);
CREATE INDEX idx_silver_net_income_period ON silver_net_income(year, quarter);

ALTER TABLE silver_net_income ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Silver net income readable by authenticated users"
  ON silver_net_income FOR SELECT
  TO authenticated
  USING (true);


-- =========================================================================
-- 7. silver_loan_composition  (~90 columns)
-- Loan balances by type, counts, interest rates, participations
-- =========================================================================

CREATE TABLE silver_loan_composition (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  charter_number INT NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  transformed_at TIMESTAMPTZ DEFAULT now(),

  -- Totals and counts
  num_total_loans_leases NUMERIC,                     -- 025A
  num_total_loans_on_schedule NUMERIC,                -- 025A1
  total_loans_on_schedule NUMERIC,                    -- 025B1
  num_loans_granted_ytd NUMERIC,                      -- 031A
  amount_loans_granted_ytd NUMERIC,                   -- 031B
  num_pals_granted_ytd NUMERIC,                       -- 031C
  amount_pals_granted_ytd NUMERIC,                    -- 031D

  -- Loan balances by type
  used_vehicle_loans NUMERIC,                         -- 370
  new_vehicle_loans NUMERIC,                          -- 385
  junior_lien_re_loans NUMERIC,                       -- 386A
  all_other_re_loans NUMERIC,                         -- 386B
  unsecured_credit_card_loans NUMERIC,                -- 396
  all_other_unsecured_loans NUMERIC,                  -- 397
  payday_alternative_loans NUMERIC,                   -- 397A
  loans_with_rate_over_15pct NUMERIC,                 -- 567
  num_indirect_loans NUMERIC,                         -- 617A
  amount_indirect_loans NUMERIC,                      -- 618A

  -- Participation loans
  purchased_participation_loans_ytd NUMERIC,          -- 690
  participation_loans_sold_ytd NUMERIC,               -- 691
  participation_loans_purchased_outstanding NUMERIC,  -- 691L
  participation_re_loans_purchased NUMERIC,           -- 691L2
  participation_student_loans_purchased NUMERIC,      -- 691L7
  participation_commercial_excl_cd_purchased NUMERIC, -- 691L8
  participation_commercial_cd_purchased NUMERIC,      -- 691L9
  participation_loans_sold_outstanding NUMERIC,       -- 691N
  participation_re_loans_sold NUMERIC,                -- 691N2
  participation_student_loans_sold NUMERIC,           -- 691N7
  participation_commercial_excl_cd_sold NUMERIC,      -- 691N8
  participation_commercial_cd_sold NUMERIC,           -- 691N9

  -- Other loan types
  non_federally_guaranteed_student_loans NUMERIC,     -- 698A
  all_other_secured_non_re_loans NUMERIC,             -- 698C
  first_lien_re_loans NUMERIC,                        -- 703A

  -- Counts by type
  num_leases_receivable NUMERIC,                      -- 954
  loans_to_officials_staff NUMERIC,                   -- 956
  num_new_vehicle_loans NUMERIC,                      -- 958
  num_student_loans NUMERIC,                          -- 963A
  num_other_secured_non_re_loans NUMERIC,             -- 963C
  num_used_vehicle_loans NUMERIC,                     -- 968
  bankruptcy_loan_balances NUMERIC,                   -- 971
  num_unsecured_credit_card_loans NUMERIC,            -- 993
  num_all_other_unsecured_loans NUMERIC,              -- 994
  num_payday_alternative_loans NUMERIC,               -- 994A
  num_loans_to_officials_staff NUMERIC,               -- 995
  num_tdr_loans NUMERIC,                              -- 1000F
  amount_tdr_loans NUMERIC,                           -- 1001F

  -- Indirect loans by type
  num_indirect_vehicle_loans NUMERIC,                 -- IN0001
  amount_indirect_vehicle_loans NUMERIC,              -- IN0002
  num_indirect_re_loans NUMERIC,                      -- IN0003
  amount_indirect_re_loans NUMERIC,                   -- IN0004
  num_indirect_commercial_loans NUMERIC,              -- IN0005
  amount_indirect_commercial_loans NUMERIC,           -- IN0006
  num_indirect_other_loans NUMERIC,                   -- IN0007
  amount_indirect_other_loans NUMERIC,                -- IN0008

  -- Interest rates
  rate_unsecured_credit_card NUMERIC,                 -- 521
  rate_other_unsecured NUMERIC,                       -- 522
  rate_payday_alternative NUMERIC,                    -- 522A
  rate_new_vehicle NUMERIC,                           -- 523
  rate_used_vehicle NUMERIC,                          -- 524
  rate_commercial_re_secured NUMERIC,                 -- 525
  rate_commercial_not_re_secured NUMERIC,             -- 526
  rate_junior_lien_re NUMERIC,                        -- 562A
  rate_all_other_re NUMERIC,                          -- 562B
  rate_first_lien_re NUMERIC,                         -- 563A
  rate_leases_receivable NUMERIC,                     -- 565
  rate_loans_over_15pct_weighted_avg NUMERIC,         -- 568
  rate_student_loans NUMERIC,                         -- 595A
  rate_other_secured_non_re NUMERIC,                  -- 595B

  UNIQUE(charter_number, year, quarter)
);

CREATE INDEX idx_silver_loan_composition_charter ON silver_loan_composition(charter_number);
CREATE INDEX idx_silver_loan_composition_period ON silver_loan_composition(year, quarter);

ALTER TABLE silver_loan_composition ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Silver loan composition readable by authenticated users"
  ON silver_loan_composition FOR SELECT
  TO authenticated
  USING (true);


-- =========================================================================
-- 8. silver_delinquency  (~53 columns)
-- Aging buckets (30/60/90/180/360+) by loan type
-- =========================================================================

CREATE TABLE silver_delinquency (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  charter_number INT NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  transformed_at TIMESTAMPTZ DEFAULT now(),

  -- 30-59 days delinquent
  delinquent_30_59_total NUMERIC,                     -- 020B
  delinquent_30_59_new_vehicle NUMERIC,               -- 020C1
  delinquent_30_59_used_vehicle NUMERIC,              -- 020C2
  delinquent_30_59_leases NUMERIC,                    -- 020D
  delinquent_30_59_student_loans NUMERIC,             -- 020T

  -- 90-179 days delinquent
  delinquent_90_179_total NUMERIC,                    -- 021B
  delinquent_90_179_new_vehicle NUMERIC,              -- 021C1
  delinquent_90_179_used_vehicle NUMERIC,             -- 021C2
  delinquent_90_179_leases NUMERIC,                   -- 021D
  delinquent_90_179_student_loans NUMERIC,            -- 021T

  -- 180-359 days delinquent
  delinquent_180_359_total NUMERIC,                   -- 022B
  delinquent_180_359_new_vehicle NUMERIC,             -- 022C1
  delinquent_180_359_used_vehicle NUMERIC,            -- 022C2
  delinquent_180_359_leases NUMERIC,                  -- 022D
  delinquent_180_359_student_loans NUMERIC,           -- 022T

  -- 360+ days delinquent
  delinquent_360_plus_total NUMERIC,                  -- 023B
  delinquent_360_plus_new_vehicle NUMERIC,            -- 023C1
  delinquent_360_plus_used_vehicle NUMERIC,           -- 023C2
  delinquent_360_plus_leases NUMERIC,                 -- 023D
  delinquent_360_plus_student_loans NUMERIC,          -- 023T

  -- Credit card delinquency
  delinquent_30_59_credit_card NUMERIC,               -- 024B
  delinquent_90_179_credit_card NUMERIC,              -- 026B
  delinquent_180_359_credit_card NUMERIC,             -- 027B
  delinquent_360_plus_credit_card NUMERIC,            -- 028B

  -- Counts by type
  num_delinquent_leases NUMERIC,                      -- 034E
  num_delinquent_new_vehicle NUMERIC,                 -- 035E1
  num_delinquent_used_vehicle NUMERIC,                -- 035E2
  num_total_delinquent NUMERIC,                       -- 041A
  total_delinquent_loans NUMERIC,                     -- 041B
  delinquent_new_vehicle_total NUMERIC,               -- 041C1
  delinquent_used_vehicle_total NUMERIC,              -- 041C2
  delinquent_leases_total NUMERIC,                    -- 041D
  delinquent_indirect_lending NUMERIC,                -- 041E
  delinquent_student_loans_total NUMERIC,             -- 041T
  num_delinquent_credit_card NUMERIC,                 -- 045A
  delinquent_credit_card_total NUMERIC,               -- 045B
  num_delinquent_student_loans NUMERIC,               -- 053E

  -- PALs delinquency
  delinquent_30_59_pals NUMERIC,                      -- 089B
  delinquent_90_179_pals NUMERIC,                     -- 127B
  delinquent_180_359_pals NUMERIC,                    -- 128B
  delinquent_360_plus_pals NUMERIC,                   -- 129B
  num_delinquent_pals NUMERIC,                        -- 130A
  delinquent_pals_total NUMERIC,                      -- 130B

  UNIQUE(charter_number, year, quarter)
);

CREATE INDEX idx_silver_delinquency_charter ON silver_delinquency(charter_number);
CREATE INDEX idx_silver_delinquency_period ON silver_delinquency(year, quarter);

ALTER TABLE silver_delinquency ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Silver delinquency readable by authenticated users"
  ON silver_delinquency FOR SELECT
  TO authenticated
  USING (true);


-- =========================================================================
-- 9. silver_charge_offs  (~45 columns)
-- Charge-offs and recoveries by loan type
-- =========================================================================

CREATE TABLE silver_charge_offs (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  charter_number INT NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  transformed_at TIMESTAMPTZ DEFAULT now(),

  -- Totals
  charge_offs_pals_ytd NUMERIC,                       -- 136
  recoveries_pals_ytd NUMERIC,                        -- 137
  total_charge_offs_ytd NUMERIC,                      -- 550
  charge_offs_new_vehicle_ytd NUMERIC,                -- 550C1
  charge_offs_used_vehicle_ytd NUMERIC,               -- 550C2
  charge_offs_leases_ytd NUMERIC,                     -- 550D
  charge_offs_indirect_ytd NUMERIC,                   -- 550E
  charge_offs_participation_ytd NUMERIC,              -- 550F
  charge_offs_student_loans_ytd NUMERIC,              -- 550T
  total_recoveries_ytd NUMERIC,                       -- 551
  recoveries_new_vehicle_ytd NUMERIC,                 -- 551C1
  recoveries_used_vehicle_ytd NUMERIC,                -- 551C2
  recoveries_leases_ytd NUMERIC,                      -- 551D
  recoveries_indirect_ytd NUMERIC,                    -- 551E
  recoveries_participation_ytd NUMERIC,               -- 551F
  recoveries_student_loans_ytd NUMERIC,               -- 551T
  charge_offs_credit_card_ytd NUMERIC,                -- 680
  recoveries_credit_card_ytd NUMERIC,                 -- 681

  -- By loan type (CH codes)
  charge_offs_other_unsecured_ytd NUMERIC,            -- CH0007
  recoveries_other_unsecured_ytd NUMERIC,             -- CH0008
  charge_offs_other_secured_non_re_ytd NUMERIC,       -- CH0015
  recoveries_other_secured_non_re_ytd NUMERIC,        -- CH0016
  charge_offs_first_lien_re_ytd NUMERIC,              -- CH0017
  recoveries_first_lien_re_ytd NUMERIC,               -- CH0018
  charge_offs_junior_lien_re_ytd NUMERIC,             -- CH0019
  recoveries_junior_lien_re_ytd NUMERIC,              -- CH0020
  charge_offs_other_re_ytd NUMERIC,                   -- CH0021
  recoveries_other_re_ytd NUMERIC,                    -- CH0022
  charge_offs_commercial_cd_ytd NUMERIC,              -- CH0023
  recoveries_commercial_cd_ytd NUMERIC,               -- CH0024
  charge_offs_commercial_farmland_ytd NUMERIC,        -- CH0025
  recoveries_commercial_farmland_ytd NUMERIC,         -- CH0026
  charge_offs_commercial_multifamily_ytd NUMERIC,     -- CH0027
  recoveries_commercial_multifamily_ytd NUMERIC,      -- CH0028
  charge_offs_commercial_owner_occ_ytd NUMERIC,       -- CH0029
  recoveries_commercial_owner_occ_ytd NUMERIC,        -- CH0030
  charge_offs_commercial_non_owner_occ_ytd NUMERIC,   -- CH0031
  recoveries_commercial_non_owner_occ_ytd NUMERIC,    -- CH0032
  charge_offs_commercial_ag_production_ytd NUMERIC,   -- CH0033
  recoveries_commercial_ag_production_ytd NUMERIC,    -- CH0034
  charge_offs_commercial_ci_ytd NUMERIC,              -- CH0035
  recoveries_commercial_ci_ytd NUMERIC,               -- CH0036
  charge_offs_commercial_unsecured_ytd NUMERIC,       -- CH0037
  recoveries_commercial_unsecured_ytd NUMERIC,        -- CH0038
  charge_offs_commercial_unsecured_loc_ytd NUMERIC,   -- CH0039
  recoveries_commercial_unsecured_loc_ytd NUMERIC,    -- CH0040
  charge_offs_purchased_701_23_ytd NUMERIC,           -- CH0047
  recoveries_purchased_701_23_ytd NUMERIC,            -- CH0048

  UNIQUE(charter_number, year, quarter)
);

CREATE INDEX idx_silver_charge_offs_charter ON silver_charge_offs(charter_number);
CREATE INDEX idx_silver_charge_offs_period ON silver_charge_offs(year, quarter);

ALTER TABLE silver_charge_offs ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Silver charge offs readable by authenticated users"
  ON silver_charge_offs FOR SELECT
  TO authenticated
  USING (true);


-- =========================================================================
-- 10. silver_commercial_loans  (~80 columns)
-- MBL breakdowns, originations, commercial participations
-- =========================================================================

CREATE TABLE silver_commercial_loans (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  charter_number INT NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  transformed_at TIMESTAMPTZ DEFAULT now(),

  -- Agriculture
  member_commercial_farmland NUMERIC,                 -- 042A5
  member_commercial_ag_production NUMERIC,            -- 042A6
  nonmember_commercial_farmland NUMERIC,              -- 042A7
  nonmember_commercial_ag_production NUMERIC,         -- 042A8
  total_agricultural_commercial NUMERIC,              -- 042A9

  -- Counts granted YTD
  num_member_commercial_granted_ytd NUMERIC,                  -- 090A1
  num_nonmember_commercial_purchased_ytd NUMERIC,             -- 090B1
  num_member_unsecured_commercial_ytd NUMERIC,                -- 090C5
  num_member_unsecured_commercial_loc_ytd NUMERIC,            -- 090C6
  num_nonmember_unsecured_commercial_ytd NUMERIC,             -- 090C7
  num_nonmember_unsecured_commercial_loc_ytd NUMERIC,         -- 090C8
  num_member_commercial_owner_occ_ytd NUMERIC,                -- 090H2
  num_nonmember_commercial_owner_occ_ytd NUMERIC,             -- 090H3
  num_member_commercial_non_owner_occ_ytd NUMERIC,            -- 090J2
  num_nonmember_commercial_non_owner_occ_ytd NUMERIC,         -- 090J3
  num_member_commercial_re_total_ytd NUMERIC,                 -- 090K2
  num_nonmember_commercial_re_total_ytd NUMERIC,              -- 090K3
  num_member_commercial_ci_ytd NUMERIC,                       -- 090L2
  num_nonmember_commercial_ci_ytd NUMERIC,                    -- 090L3
  num_member_commercial_multifamily_ytd NUMERIC,              -- 090M
  num_nonmember_commercial_multifamily_ytd NUMERIC,           -- 090M1
  num_member_commercial_farmland_ytd NUMERIC,                 -- 099A5
  num_member_commercial_ag_production_ytd NUMERIC,            -- 099A6
  num_nonmember_commercial_farmland_ytd NUMERIC,              -- 099A7
  num_nonmember_commercial_ag_production_ytd NUMERIC,         -- 099A8

  -- Construction & Development
  num_member_commercial_cd NUMERIC,                   -- 143A3
  num_nonmember_commercial_cd NUMERIC,                -- 143A4
  member_commercial_cd_balance NUMERIC,               -- 143B3
  nonmember_commercial_cd_balance NUMERIC,            -- 143B4
  num_member_commercial_cd_ytd NUMERIC,               -- 143C3
  num_nonmember_commercial_cd_ytd NUMERIC,            -- 143C4
  member_commercial_cd_amount_ytd NUMERIC,            -- 143D3
  nonmember_commercial_cd_amount_ytd NUMERIC,         -- 143D4

  -- Balances by type
  net_member_business_loan_balance NUMERIC,            -- 400A
  member_commercial_total NUMERIC,                    -- 400A1
  nonmember_commercial_total NUMERIC,                 -- 400B1
  member_unsecured_commercial NUMERIC,                -- 400C5
  member_unsecured_commercial_loc NUMERIC,            -- 400C6
  nonmember_unsecured_commercial NUMERIC,             -- 400C7
  nonmember_unsecured_commercial_loc NUMERIC,         -- 400C8
  member_commercial_owner_occupied NUMERIC,           -- 400H2
  nonmember_commercial_owner_occupied NUMERIC,        -- 400H3
  member_commercial_non_owner_occupied NUMERIC,       -- 400J2
  nonmember_commercial_non_owner_occupied NUMERIC,    -- 400J3
  member_commercial_ci NUMERIC,                       -- 400L2
  nonmember_commercial_ci NUMERIC,                    -- 400L3
  member_commercial_multifamily NUMERIC,              -- 400M
  nonmember_commercial_multifamily NUMERIC,           -- 400M1
  commercial_not_re_secured NUMERIC,                  -- 400P
  total_commercial_loans NUMERIC,                     -- 400T1

  -- Dollar amounts granted YTD
  member_commercial_farmland_ytd NUMERIC,             -- 463A5
  member_commercial_ag_production_ytd NUMERIC,        -- 463A6
  nonmember_commercial_farmland_ytd NUMERIC,          -- 463A7
  nonmember_commercial_ag_production_ytd NUMERIC,     -- 463A8
  member_commercial_total_ytd NUMERIC,                -- 475A1
  nonmember_commercial_total_ytd NUMERIC,             -- 475B1
  member_unsecured_commercial_ytd NUMERIC,            -- 475C5
  member_unsecured_commercial_loc_ytd NUMERIC,        -- 475C6
  nonmember_unsecured_commercial_ytd NUMERIC,         -- 475C7
  nonmember_unsecured_commercial_loc_ytd NUMERIC,     -- 475C8
  member_commercial_owner_occ_ytd NUMERIC,            -- 475H2
  nonmember_commercial_owner_occ_ytd NUMERIC,         -- 475H3
  member_commercial_non_owner_occ_ytd NUMERIC,        -- 475J2
  nonmember_commercial_non_owner_occ_ytd NUMERIC,     -- 475J3
  member_commercial_re_total_ytd NUMERIC,             -- 475K2
  nonmember_commercial_re_total_ytd NUMERIC,          -- 475K3
  member_commercial_ci_ytd NUMERIC,                   -- 475L2
  nonmember_commercial_ci_ytd NUMERIC,                -- 475L3
  member_commercial_multifamily_ytd NUMERIC,          -- 475M
  nonmember_commercial_multifamily_ytd NUMERIC,       -- 475M1

  -- SBA & government guaranteed
  num_sba_loans NUMERIC,                              -- 691B1
  sba_loans_balance NUMERIC,                          -- 691C1
  sba_loans_guaranteed_portion NUMERIC,               -- 691C2
  num_other_govt_guaranteed_loans NUMERIC,            -- 691P
  other_govt_guaranteed_balance NUMERIC,              -- 691P1
  other_govt_guaranteed_portion NUMERIC,              -- 691P2

  -- RE-secured totals
  member_commercial_re_secured_total NUMERIC,         -- 718A3
  nonmember_commercial_re_secured_total NUMERIC,      -- 718A4
  commercial_re_secured NUMERIC,                      -- 718A5

  -- Counts outstanding
  num_member_commercial_total NUMERIC,                -- 900A1
  num_nonmember_commercial_total NUMERIC,             -- 900B1
  num_member_unsecured_commercial NUMERIC,            -- 900C5
  num_member_unsecured_commercial_loc NUMERIC,        -- 900C6
  num_nonmember_unsecured_commercial NUMERIC,         -- 900C7
  num_nonmember_unsecured_commercial_loc NUMERIC,     -- 900C8
  num_member_commercial_owner_occ NUMERIC,            -- 900H2
  num_nonmember_commercial_owner_occ NUMERIC,         -- 900H3
  num_member_commercial_non_owner_occ NUMERIC,        -- 900J2
  num_nonmember_commercial_non_owner_occ NUMERIC,     -- 900J3
  num_member_commercial_re_total NUMERIC,             -- 900K2
  num_nonmember_commercial_re_total NUMERIC,          -- 900K3
  num_commercial_re_secured NUMERIC,                  -- 900K4
  num_member_commercial_ci NUMERIC,                   -- 900L2
  num_nonmember_commercial_ci NUMERIC,                -- 900L3
  num_member_commercial_multifamily NUMERIC,          -- 900M
  num_nonmember_commercial_multifamily NUMERIC,       -- 900M1
  num_commercial_not_re_secured NUMERIC,              -- 900P
  num_total_commercial_loans NUMERIC,                 -- 900T1
  num_member_commercial_farmland NUMERIC,             -- 961A5
  num_member_commercial_ag_production NUMERIC,        -- 961A6
  num_nonmember_commercial_farmland NUMERIC,          -- 961A7
  num_nonmember_commercial_ag_production NUMERIC,     -- 961A8
  num_total_agricultural_commercial NUMERIC,          -- 961A9

  -- Sold/serviced
  commercial_participation_sold_retained_servicing NUMERIC,       -- 1061
  num_commercial_participation_sold_retained_servicing NUMERIC,   -- 1061A
  commercial_loans_sold_retained_servicing NUMERIC,               -- 1062
  num_commercial_loans_sold_retained_servicing NUMERIC,           -- 1062A
  commercial_loans_sold_no_servicing_ytd NUMERIC,                 -- 1063
  num_commercial_loans_sold_no_servicing_ytd NUMERIC,             -- 1063A

  UNIQUE(charter_number, year, quarter)
);

CREATE INDEX idx_silver_commercial_loans_charter ON silver_commercial_loans(charter_number);
CREATE INDEX idx_silver_commercial_loans_period ON silver_commercial_loans(year, quarter);

ALTER TABLE silver_commercial_loans ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Silver commercial loans readable by authenticated users"
  ON silver_commercial_loans FOR SELECT
  TO authenticated
  USING (true);


-- =========================================================================
-- 11. silver_real_estate  (~13 named columns + 50 acct_rl columns)
-- RE loan detail, 1-4 family breakdowns, rate/term detail
-- RL codes (RL0001-RL0050) lack definitions; use acct_ prefix
-- =========================================================================

CREATE TABLE silver_real_estate (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  charter_number INT NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  transformed_at TIMESTAMPTZ DEFAULT now(),

  -- Named columns from definitions
  residential_construction_first_mtg NUMERIC,         -- 704A2
  interest_only_first_mtg_balance NUMERIC,            -- 704C1
  num_interest_only_first_mtg NUMERIC,                -- 704C2
  interest_only_first_mtg_granted_ytd NUMERIC,        -- 704C3
  first_mtg_sold_secondary_market_ytd NUMERIC,        -- 736
  re_loans_sold_but_serviced NUMERIC,                 -- 779A
  num_first_lien_re_loans NUMERIC,                    -- 959A
  num_junior_lien_re_loans NUMERIC,                   -- 960A
  num_all_other_re_loans NUMERIC,                     -- 960B

  -- RL codes (1-4 Family RE Detail) - no definitions yet
  acct_rl0001 NUMERIC,                                -- RL0001
  acct_rl0002 NUMERIC,                                -- RL0002
  acct_rl0003 NUMERIC,                                -- RL0003
  acct_rl0004 NUMERIC,                                -- RL0004
  acct_rl0005 NUMERIC,                                -- RL0005
  acct_rl0006 NUMERIC,                                -- RL0006
  acct_rl0007 NUMERIC,                                -- RL0007
  acct_rl0008 NUMERIC,                                -- RL0008
  acct_rl0009 NUMERIC,                                -- RL0009
  acct_rl0010 NUMERIC,                                -- RL0010
  acct_rl0011 NUMERIC,                                -- RL0011
  acct_rl0012 NUMERIC,                                -- RL0012
  acct_rl0013 NUMERIC,                                -- RL0013
  acct_rl0014 NUMERIC,                                -- RL0014
  acct_rl0015 NUMERIC,                                -- RL0015
  acct_rl0016 NUMERIC,                                -- RL0016
  acct_rl0017 NUMERIC,                                -- RL0017
  acct_rl0018 NUMERIC,                                -- RL0018
  acct_rl0019 NUMERIC,                                -- RL0019
  acct_rl0020 NUMERIC,                                -- RL0020
  acct_rl0021 NUMERIC,                                -- RL0021
  acct_rl0022 NUMERIC,                                -- RL0022
  acct_rl0023 NUMERIC,                                -- RL0023
  acct_rl0024 NUMERIC,                                -- RL0024
  acct_rl0025 NUMERIC,                                -- RL0025
  acct_rl0026 NUMERIC,                                -- RL0026
  acct_rl0027 NUMERIC,                                -- RL0027
  acct_rl0028 NUMERIC,                                -- RL0028
  acct_rl0029 NUMERIC,                                -- RL0029
  acct_rl0030 NUMERIC,                                -- RL0030
  acct_rl0031 NUMERIC,                                -- RL0031
  acct_rl0032 NUMERIC,                                -- RL0032
  acct_rl0033 NUMERIC,                                -- RL0033
  acct_rl0034 NUMERIC,                                -- RL0034
  acct_rl0035 NUMERIC,                                -- RL0035
  acct_rl0036 NUMERIC,                                -- RL0036
  acct_rl0037 NUMERIC,                                -- RL0037
  acct_rl0038 NUMERIC,                                -- RL0038
  acct_rl0039 NUMERIC,                                -- RL0039
  acct_rl0040 NUMERIC,                                -- RL0040
  acct_rl0041 NUMERIC,                                -- RL0041
  acct_rl0042 NUMERIC,                                -- RL0042
  acct_rl0043 NUMERIC,                                -- RL0043
  acct_rl0044 NUMERIC,                                -- RL0044
  acct_rl0045 NUMERIC,                                -- RL0045
  acct_rl0046 NUMERIC,                                -- RL0046
  acct_rl0047 NUMERIC,                                -- RL0047
  acct_rl0048 NUMERIC,                                -- RL0048
  acct_rl0049 NUMERIC,                                -- RL0049
  acct_rl0050 NUMERIC,                                -- RL0050

  UNIQUE(charter_number, year, quarter)
);

CREATE INDEX idx_silver_real_estate_charter ON silver_real_estate(charter_number);
CREATE INDEX idx_silver_real_estate_period ON silver_real_estate(year, quarter);

ALTER TABLE silver_real_estate ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Silver real estate readable by authenticated users"
  ON silver_real_estate FOR SELECT
  TO authenticated
  USING (true);


-- =========================================================================
-- 12. silver_investments  (~20 named columns + 110 acct_nv columns)
-- Securities by type, maturity, fair value
-- NV codes (NV0001-NV0110) lack definitions; use acct_ prefix
-- =========================================================================

CREATE TABLE silver_investments (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  charter_number INT NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  transformed_at TIMESTAMPTZ DEFAULT now(),

  -- Named columns from definitions
  investments_not_authorized_fcu_scu NUMERIC,          -- 784A
  brokered_certificates NUMERIC,                      -- 788
  securities_employee_benefit_plans NUMERIC,           -- 789C
  other_investments_employee_benefit_plans NUMERIC,    -- 789D
  employee_benefit_plan_701_19c NUMERIC,              -- 789G
  charitable_donation_accounts NUMERIC,               -- 789H
  htm_securities_fair_value NUMERIC,                  -- 801
  total_cuso_investments NUMERIC,                     -- 851
  total_loaned_to_cusos NUMERIC,                      -- 852
  total_cuso_cash_outlay NUMERIC,                     -- 853
  total_investment_securities NUMERIC,                -- AS0013
  all_other_investments NUMERIC,                      -- AS0016
  total_other_investments NUMERIC,                    -- AS0017

  -- NV codes (Fair Value Detail) - no definitions yet
  acct_nv0001 NUMERIC,                                -- NV0001
  acct_nv0002 NUMERIC,                                -- NV0002
  acct_nv0003 NUMERIC,                                -- NV0003
  acct_nv0004 NUMERIC,                                -- NV0004
  acct_nv0005 NUMERIC,                                -- NV0005
  acct_nv0006 NUMERIC,                                -- NV0006
  acct_nv0007 NUMERIC,                                -- NV0007
  acct_nv0008 NUMERIC,                                -- NV0008
  acct_nv0009 NUMERIC,                                -- NV0009
  acct_nv0010 NUMERIC,                                -- NV0010
  acct_nv0011 NUMERIC,                                -- NV0011
  acct_nv0012 NUMERIC,                                -- NV0012
  acct_nv0013 NUMERIC,                                -- NV0013
  acct_nv0014 NUMERIC,                                -- NV0014
  acct_nv0015 NUMERIC,                                -- NV0015
  acct_nv0016 NUMERIC,                                -- NV0016
  acct_nv0017 NUMERIC,                                -- NV0017
  acct_nv0018 NUMERIC,                                -- NV0018
  acct_nv0019 NUMERIC,                                -- NV0019
  acct_nv0020 NUMERIC,                                -- NV0020
  acct_nv0021 NUMERIC,                                -- NV0021
  acct_nv0022 NUMERIC,                                -- NV0022
  acct_nv0023 NUMERIC,                                -- NV0023
  acct_nv0024 NUMERIC,                                -- NV0024
  acct_nv0025 NUMERIC,                                -- NV0025
  acct_nv0026 NUMERIC,                                -- NV0026
  acct_nv0027 NUMERIC,                                -- NV0027
  acct_nv0028 NUMERIC,                                -- NV0028
  acct_nv0029 NUMERIC,                                -- NV0029
  acct_nv0030 NUMERIC,                                -- NV0030
  acct_nv0031 NUMERIC,                                -- NV0031
  acct_nv0032 NUMERIC,                                -- NV0032
  acct_nv0033 NUMERIC,                                -- NV0033
  acct_nv0034 NUMERIC,                                -- NV0034
  acct_nv0035 NUMERIC,                                -- NV0035
  acct_nv0036 NUMERIC,                                -- NV0036
  acct_nv0037 NUMERIC,                                -- NV0037
  acct_nv0038 NUMERIC,                                -- NV0038
  acct_nv0039 NUMERIC,                                -- NV0039
  acct_nv0040 NUMERIC,                                -- NV0040
  acct_nv0041 NUMERIC,                                -- NV0041
  acct_nv0042 NUMERIC,                                -- NV0042
  acct_nv0043 NUMERIC,                                -- NV0043
  acct_nv0044 NUMERIC,                                -- NV0044
  acct_nv0045 NUMERIC,                                -- NV0045
  acct_nv0046 NUMERIC,                                -- NV0046
  acct_nv0047 NUMERIC,                                -- NV0047
  acct_nv0048 NUMERIC,                                -- NV0048
  acct_nv0049 NUMERIC,                                -- NV0049
  acct_nv0050 NUMERIC,                                -- NV0050
  acct_nv0051 NUMERIC,                                -- NV0051
  acct_nv0052 NUMERIC,                                -- NV0052
  acct_nv0053 NUMERIC,                                -- NV0053
  acct_nv0054 NUMERIC,                                -- NV0054
  acct_nv0055 NUMERIC,                                -- NV0055
  acct_nv0056 NUMERIC,                                -- NV0056
  acct_nv0057 NUMERIC,                                -- NV0057
  acct_nv0058 NUMERIC,                                -- NV0058
  acct_nv0059 NUMERIC,                                -- NV0059
  acct_nv0060 NUMERIC,                                -- NV0060
  acct_nv0061 NUMERIC,                                -- NV0061
  acct_nv0062 NUMERIC,                                -- NV0062
  acct_nv0063 NUMERIC,                                -- NV0063
  acct_nv0064 NUMERIC,                                -- NV0064
  acct_nv0065 NUMERIC,                                -- NV0065
  acct_nv0066 NUMERIC,                                -- NV0066
  acct_nv0067 NUMERIC,                                -- NV0067
  acct_nv0068 NUMERIC,                                -- NV0068
  acct_nv0069 NUMERIC,                                -- NV0069
  acct_nv0070 NUMERIC,                                -- NV0070
  acct_nv0071 NUMERIC,                                -- NV0071
  acct_nv0072 NUMERIC,                                -- NV0072
  acct_nv0073 NUMERIC,                                -- NV0073
  acct_nv0074 NUMERIC,                                -- NV0074
  acct_nv0075 NUMERIC,                                -- NV0075
  acct_nv0076 NUMERIC,                                -- NV0076
  acct_nv0077 NUMERIC,                                -- NV0077
  acct_nv0078 NUMERIC,                                -- NV0078
  acct_nv0079 NUMERIC,                                -- NV0079
  acct_nv0080 NUMERIC,                                -- NV0080
  acct_nv0081 NUMERIC,                                -- NV0081
  acct_nv0082 NUMERIC,                                -- NV0082
  acct_nv0083 NUMERIC,                                -- NV0083
  acct_nv0084 NUMERIC,                                -- NV0084
  acct_nv0085 NUMERIC,                                -- NV0085
  acct_nv0086 NUMERIC,                                -- NV0086
  acct_nv0087 NUMERIC,                                -- NV0087
  acct_nv0088 NUMERIC,                                -- NV0088
  acct_nv0089 NUMERIC,                                -- NV0089
  acct_nv0090 NUMERIC,                                -- NV0090
  acct_nv0091 NUMERIC,                                -- NV0091
  acct_nv0092 NUMERIC,                                -- NV0092
  acct_nv0093 NUMERIC,                                -- NV0093
  acct_nv0094 NUMERIC,                                -- NV0094
  acct_nv0095 NUMERIC,                                -- NV0095
  acct_nv0096 NUMERIC,                                -- NV0096
  acct_nv0097 NUMERIC,                                -- NV0097
  acct_nv0098 NUMERIC,                                -- NV0098
  acct_nv0099 NUMERIC,                                -- NV0099
  acct_nv0100 NUMERIC,                                -- NV0100
  acct_nv0101 NUMERIC,                                -- NV0101
  acct_nv0102 NUMERIC,                                -- NV0102
  acct_nv0103 NUMERIC,                                -- NV0103
  acct_nv0104 NUMERIC,                                -- NV0104
  acct_nv0105 NUMERIC,                                -- NV0105
  acct_nv0106 NUMERIC,                                -- NV0106
  acct_nv0107 NUMERIC,                                -- NV0107
  acct_nv0108 NUMERIC,                                -- NV0108
  acct_nv0109 NUMERIC,                                -- NV0109
  acct_nv0110 NUMERIC,                                -- NV0110

  UNIQUE(charter_number, year, quarter)
);

CREATE INDEX idx_silver_investments_charter ON silver_investments(charter_number);
CREATE INDEX idx_silver_investments_period ON silver_investments(year, quarter);

ALTER TABLE silver_investments ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Silver investments readable by authenticated users"
  ON silver_investments FOR SELECT
  TO authenticated
  USING (true);


-- =========================================================================
-- 13. silver_liquidity  (~40 columns)
-- Off-balance sheet, commitments, borrowings detail, capacity
-- =========================================================================

CREATE TABLE silver_liquidity (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  charter_number INT NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  transformed_at TIMESTAMPTZ DEFAULT now(),

  -- Repurchase borrowings
  repurchase_borrowings_lt_1yr NUMERIC,               -- 058A
  repurchase_borrowings_1_to_3yr NUMERIC,             -- 058B1
  repurchase_borrowings_gt_3yr NUMERIC,               -- 058B2
  total_repurchase_borrowings NUMERIC,                -- 058C

  -- Total borrowings
  total_borrowings_lt_1yr NUMERIC,                    -- 860A
  total_borrowings_1_to_3yr NUMERIC,                  -- 860B1
  total_borrowings_gt_3yr NUMERIC,                    -- 860B2
  total_borrowings NUMERIC,                           -- 860C
  borrowings_early_repayment_option NUMERIC,          -- 865A

  -- Subordinated debt
  subordinated_debt_lt_1yr NUMERIC,                   -- 867A
  subordinated_debt_1_to_3yr NUMERIC,                 -- 867B1
  subordinated_debt_gt_3yr NUMERIC,                   -- 867B2
  total_subordinated_debt NUMERIC,                    -- 867C

  -- Pledged assets
  assets_pledged_borrowing_capacity NUMERIC,          -- 878

  -- Borrowing capacity
  total_borrowing_capacity NUMERIC,                   -- 881
  draws_against_capacity_lt_1yr NUMERIC,              -- 883A
  draws_against_capacity_1_to_3yr NUMERIC,            -- 883B1
  draws_against_capacity_gt_3yr NUMERIC,              -- 883B2
  total_draws_against_capacity NUMERIC,               -- 883C
  borrowing_capacity_corporate_cu NUMERIC,            -- 884
  borrowing_capacity_natural_person_cu NUMERIC,       -- 884C
  borrowing_capacity_other_sources NUMERIC,           -- 884D
  draws_corporate_cu NUMERIC,                         -- 885A
  draws_natural_person_cu NUMERIC,                    -- 885A1
  draws_other_sources NUMERIC,                        -- 885A2
  draws_fhlb NUMERIC,                                 -- 885A3
  total_draws_all_sources NUMERIC,                    -- 885A4
  assets_pledged_frb_ppp NUMERIC,                     -- LC0047
  draws_frb_ppp NUMERIC,                              -- LC0085

  -- Off-balance sheet / unfunded commitments
  unfunded_revolving_re_lines NUMERIC,                -- 811D
  unfunded_credit_card_lines NUMERIC,                 -- 812C
  unfunded_commercial_commitments NUMERIC,            -- 814K
  unfunded_share_draft_lines NUMERIC,                 -- 815C
  total_unfunded_commitments NUMERIC,                 -- 816A
  other_unfunded_non_commercial NUMERIC,              -- 816B5
  total_unfunded_non_commercial NUMERIC,              -- 816T
  other_contingent_liabilities NUMERIC,               -- 818A
  loans_transferred_with_recourse NUMERIC,            -- 819
  unfunded_overdraft_protection NUMERIC,              -- 822C

  UNIQUE(charter_number, year, quarter)
);

CREATE INDEX idx_silver_liquidity_charter ON silver_liquidity(charter_number);
CREATE INDEX idx_silver_liquidity_period ON silver_liquidity(year, quarter);

ALTER TABLE silver_liquidity ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Silver liquidity readable by authenticated users"
  ON silver_liquidity FOR SELECT
  TO authenticated
  USING (true);


-- =========================================================================
-- 14. silver_operations  (~25 named columns + 10 acct_pc columns)
-- Members, employees, derivatives, PCD, grants, misc
-- PC codes (PC0001-PC0010) lack definitions; use acct_ prefix
-- =========================================================================

CREATE TABLE silver_operations (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  charter_number INT NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  transformed_at TIMESTAMPTZ DEFAULT now(),

  -- Membership and staffing
  num_current_members NUMERIC,                        -- 083
  num_potential_members NUMERIC,                      -- 084
  num_full_time_employees NUMERIC,                    -- 564A
  num_part_time_employees NUMERIC,                    -- 564B
  num_atm_locations NUMERIC,                          -- 566
  plans_to_add_branches NUMERIC,                      -- 566B

  -- Insurance
  has_other_insurance NUMERIC,                        -- 875
  other_insurance_company NUMERIC,                    -- 876
  other_insurance_amount NUMERIC,                     -- 877

  -- Grants and remittances
  grants_awarded_ytd NUMERIC,                         -- 926
  grants_received_ytd NUMERIC,                        -- 927
  num_international_remittances_ytd NUMERIC,          -- 928

  -- Derivatives totals
  total_derivative_notional NUMERIC,                  -- 1030
  total_derivative_fair_value NUMERIC,                -- 1030C

  -- Derivatives by type (DT codes)
  derivative_ir_purchased_options_notional NUMERIC,   -- DT0001
  derivative_ir_purchased_options_fair_value NUMERIC,  -- DT0002
  derivative_ir_written_options_notional NUMERIC,     -- DT0003
  derivative_ir_written_options_fair_value NUMERIC,   -- DT0004
  derivative_ir_swaps_notional NUMERIC,               -- DT0005
  derivative_ir_swaps_fair_value NUMERIC,             -- DT0006
  derivative_ir_futures_notional NUMERIC,             -- DT0007
  derivative_ir_futures_fair_value NUMERIC,           -- DT0008
  derivative_ir_other_notional NUMERIC,               -- DT0009
  derivative_ir_other_fair_value NUMERIC,             -- DT0010
  derivative_loan_pipeline_notional NUMERIC,          -- DT0011
  derivative_loan_pipeline_fair_value NUMERIC,        -- DT0012
  derivative_equity_call_options_notional NUMERIC,    -- DT0013
  derivative_equity_call_options_fair_value NUMERIC,  -- DT0014
  derivative_other_notional NUMERIC,                  -- DT0015
  derivative_other_fair_value NUMERIC,                -- DT0016

  -- PC codes (Purchased Credit Detail) - no definitions yet
  acct_pc0001 NUMERIC,                                -- PC0001
  acct_pc0002 NUMERIC,                                -- PC0002
  acct_pc0003 NUMERIC,                                -- PC0003
  acct_pc0004 NUMERIC,                                -- PC0004
  acct_pc0005 NUMERIC,                                -- PC0005
  acct_pc0006 NUMERIC,                                -- PC0006
  acct_pc0007 NUMERIC,                                -- PC0007
  acct_pc0008 NUMERIC,                                -- PC0008
  acct_pc0009 NUMERIC,                                -- PC0009
  acct_pc0010 NUMERIC,                                -- PC0010

  UNIQUE(charter_number, year, quarter)
);

CREATE INDEX idx_silver_operations_charter ON silver_operations(charter_number);
CREATE INDEX idx_silver_operations_period ON silver_operations(year, quarter);

ALTER TABLE silver_operations ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Silver operations readable by authenticated users"
  ON silver_operations FOR SELECT
  TO authenticated
  USING (true);


-- =========================================================================
-- 15. silver_risk_based_capital  (~180 columns)
-- Full RBC schedule (RB0001-RB0172) + CCULR (LR0001-LR0008)
-- CCULR codes have definitions; RB codes use acct_ prefix
-- =========================================================================

CREATE TABLE silver_risk_based_capital (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  charter_number INT NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  period TEXT NOT NULL,
  transformed_at TIMESTAMPTZ DEFAULT now(),

  -- CCULR (Complex Credit Union Leverage Ratio)
  cculr_election NUMERIC,                             -- LR0001
  cculr_off_balance_sheet_exposures NUMERIC,          -- LR0002
  cculr_off_balance_sheet_ratio NUMERIC,              -- LR0003
  cculr_trading_assets_liabilities NUMERIC,           -- LR0004
  cculr_trading_assets_liabilities_ratio NUMERIC,     -- LR0005
  cculr_goodwill_intangibles NUMERIC,                 -- LR0006
  cculr_goodwill_intangibles_ratio NUMERIC,           -- LR0007
  cculr_grace_period_indicator NUMERIC,               -- LR0008

  -- RB codes (Risk-Based Capital schedule) - no definitions yet
  acct_rb0001 NUMERIC,                                -- RB0001
  acct_rb0002 NUMERIC,                                -- RB0002
  acct_rb0003 NUMERIC,                                -- RB0003
  acct_rb0004 NUMERIC,                                -- RB0004
  acct_rb0005 NUMERIC,                                -- RB0005
  acct_rb0006 NUMERIC,                                -- RB0006
  acct_rb0007 NUMERIC,                                -- RB0007
  acct_rb0008 NUMERIC,                                -- RB0008
  acct_rb0009 NUMERIC,                                -- RB0009
  acct_rb0010 NUMERIC,                                -- RB0010
  acct_rb0011 NUMERIC,                                -- RB0011
  acct_rb0012 NUMERIC,                                -- RB0012
  acct_rb0013 NUMERIC,                                -- RB0013
  acct_rb0014 NUMERIC,                                -- RB0014
  acct_rb0015 NUMERIC,                                -- RB0015
  acct_rb0016 NUMERIC,                                -- RB0016
  acct_rb0017 NUMERIC,                                -- RB0017
  acct_rb0018 NUMERIC,                                -- RB0018
  acct_rb0019 NUMERIC,                                -- RB0019
  acct_rb0020 NUMERIC,                                -- RB0020
  acct_rb0021 NUMERIC,                                -- RB0021
  acct_rb0022 NUMERIC,                                -- RB0022
  acct_rb0023 NUMERIC,                                -- RB0023
  acct_rb0024 NUMERIC,                                -- RB0024
  acct_rb0025 NUMERIC,                                -- RB0025
  acct_rb0026 NUMERIC,                                -- RB0026
  acct_rb0027 NUMERIC,                                -- RB0027
  acct_rb0028 NUMERIC,                                -- RB0028
  acct_rb0029 NUMERIC,                                -- RB0029
  acct_rb0030 NUMERIC,                                -- RB0030
  acct_rb0031 NUMERIC,                                -- RB0031
  acct_rb0032 NUMERIC,                                -- RB0032
  acct_rb0033 NUMERIC,                                -- RB0033
  acct_rb0034 NUMERIC,                                -- RB0034
  acct_rb0035 NUMERIC,                                -- RB0035
  acct_rb0036 NUMERIC,                                -- RB0036
  acct_rb0037 NUMERIC,                                -- RB0037
  acct_rb0038 NUMERIC,                                -- RB0038
  acct_rb0039 NUMERIC,                                -- RB0039
  acct_rb0040 NUMERIC,                                -- RB0040
  acct_rb0041 NUMERIC,                                -- RB0041
  acct_rb0042 NUMERIC,                                -- RB0042
  acct_rb0043 NUMERIC,                                -- RB0043
  acct_rb0044 NUMERIC,                                -- RB0044
  acct_rb0045 NUMERIC,                                -- RB0045
  acct_rb0046 NUMERIC,                                -- RB0046
  acct_rb0047 NUMERIC,                                -- RB0047
  acct_rb0048 NUMERIC,                                -- RB0048
  acct_rb0049 NUMERIC,                                -- RB0049
  acct_rb0050 NUMERIC,                                -- RB0050
  acct_rb0051 NUMERIC,                                -- RB0051
  acct_rb0052 NUMERIC,                                -- RB0052
  acct_rb0053 NUMERIC,                                -- RB0053
  acct_rb0054 NUMERIC,                                -- RB0054
  acct_rb0055 NUMERIC,                                -- RB0055
  acct_rb0056 NUMERIC,                                -- RB0056
  acct_rb0057 NUMERIC,                                -- RB0057
  acct_rb0058 NUMERIC,                                -- RB0058
  acct_rb0059 NUMERIC,                                -- RB0059
  acct_rb0060 NUMERIC,                                -- RB0060
  acct_rb0061 NUMERIC,                                -- RB0061
  acct_rb0062 NUMERIC,                                -- RB0062
  acct_rb0063 NUMERIC,                                -- RB0063
  acct_rb0064 NUMERIC,                                -- RB0064
  acct_rb0065 NUMERIC,                                -- RB0065
  acct_rb0066 NUMERIC,                                -- RB0066
  acct_rb0067 NUMERIC,                                -- RB0067
  acct_rb0068 NUMERIC,                                -- RB0068
  acct_rb0069 NUMERIC,                                -- RB0069
  acct_rb0070 NUMERIC,                                -- RB0070
  acct_rb0071 NUMERIC,                                -- RB0071
  acct_rb0072 NUMERIC,                                -- RB0072
  acct_rb0073 NUMERIC,                                -- RB0073
  acct_rb0074 NUMERIC,                                -- RB0074
  acct_rb0075 NUMERIC,                                -- RB0075
  acct_rb0076 NUMERIC,                                -- RB0076
  acct_rb0077 NUMERIC,                                -- RB0077
  acct_rb0078 NUMERIC,                                -- RB0078
  acct_rb0079 NUMERIC,                                -- RB0079
  acct_rb0080 NUMERIC,                                -- RB0080
  acct_rb0081 NUMERIC,                                -- RB0081
  acct_rb0082 NUMERIC,                                -- RB0082
  acct_rb0083 NUMERIC,                                -- RB0083
  acct_rb0084 NUMERIC,                                -- RB0084
  acct_rb0085 NUMERIC,                                -- RB0085
  acct_rb0086 NUMERIC,                                -- RB0086
  acct_rb0087 NUMERIC,                                -- RB0087
  acct_rb0088 NUMERIC,                                -- RB0088
  acct_rb0089 NUMERIC,                                -- RB0089
  acct_rb0090 NUMERIC,                                -- RB0090
  acct_rb0091 NUMERIC,                                -- RB0091
  acct_rb0092 NUMERIC,                                -- RB0092
  acct_rb0093 NUMERIC,                                -- RB0093
  acct_rb0094 NUMERIC,                                -- RB0094
  acct_rb0095 NUMERIC,                                -- RB0095
  acct_rb0096 NUMERIC,                                -- RB0096
  acct_rb0097 NUMERIC,                                -- RB0097
  acct_rb0098 NUMERIC,                                -- RB0098
  acct_rb0099 NUMERIC,                                -- RB0099
  acct_rb0100 NUMERIC,                                -- RB0100
  acct_rb0101 NUMERIC,                                -- RB0101
  acct_rb0102 NUMERIC,                                -- RB0102
  acct_rb0103 NUMERIC,                                -- RB0103
  acct_rb0104 NUMERIC,                                -- RB0104
  acct_rb0105 NUMERIC,                                -- RB0105
  acct_rb0106 NUMERIC,                                -- RB0106
  acct_rb0107 NUMERIC,                                -- RB0107
  acct_rb0108 NUMERIC,                                -- RB0108
  acct_rb0109 NUMERIC,                                -- RB0109
  acct_rb0110 NUMERIC,                                -- RB0110
  acct_rb0111 NUMERIC,                                -- RB0111
  acct_rb0112 NUMERIC,                                -- RB0112
  acct_rb0113 NUMERIC,                                -- RB0113
  acct_rb0114 NUMERIC,                                -- RB0114
  acct_rb0115 NUMERIC,                                -- RB0115
  acct_rb0116 NUMERIC,                                -- RB0116
  acct_rb0117 NUMERIC,                                -- RB0117
  acct_rb0118 NUMERIC,                                -- RB0118
  acct_rb0119 NUMERIC,                                -- RB0119
  acct_rb0120 NUMERIC,                                -- RB0120
  acct_rb0121 NUMERIC,                                -- RB0121
  acct_rb0122 NUMERIC,                                -- RB0122
  acct_rb0123 NUMERIC,                                -- RB0123
  acct_rb0124 NUMERIC,                                -- RB0124
  acct_rb0125 NUMERIC,                                -- RB0125
  acct_rb0126 NUMERIC,                                -- RB0126
  acct_rb0127 NUMERIC,                                -- RB0127
  acct_rb0128 NUMERIC,                                -- RB0128
  acct_rb0129 NUMERIC,                                -- RB0129
  acct_rb0130 NUMERIC,                                -- RB0130
  acct_rb0131 NUMERIC,                                -- RB0131
  acct_rb0132 NUMERIC,                                -- RB0132
  acct_rb0133 NUMERIC,                                -- RB0133
  acct_rb0134 NUMERIC,                                -- RB0134
  acct_rb0135 NUMERIC,                                -- RB0135
  acct_rb0136 NUMERIC,                                -- RB0136
  acct_rb0137 NUMERIC,                                -- RB0137
  acct_rb0138 NUMERIC,                                -- RB0138
  acct_rb0139 NUMERIC,                                -- RB0139
  acct_rb0140 NUMERIC,                                -- RB0140
  acct_rb0141 NUMERIC,                                -- RB0141
  acct_rb0142 NUMERIC,                                -- RB0142
  acct_rb0143 NUMERIC,                                -- RB0143
  acct_rb0144 NUMERIC,                                -- RB0144
  acct_rb0145 NUMERIC,                                -- RB0145
  acct_rb0146 NUMERIC,                                -- RB0146
  acct_rb0147 NUMERIC,                                -- RB0147
  acct_rb0148 NUMERIC,                                -- RB0148
  acct_rb0149 NUMERIC,                                -- RB0149
  acct_rb0150 NUMERIC,                                -- RB0150
  acct_rb0151 NUMERIC,                                -- RB0151
  acct_rb0152 NUMERIC,                                -- RB0152
  acct_rb0153 NUMERIC,                                -- RB0153
  acct_rb0154 NUMERIC,                                -- RB0154
  acct_rb0155 NUMERIC,                                -- RB0155
  acct_rb0156 NUMERIC,                                -- RB0156
  acct_rb0157 NUMERIC,                                -- RB0157
  acct_rb0158 NUMERIC,                                -- RB0158
  acct_rb0159 NUMERIC,                                -- RB0159
  acct_rb0160 NUMERIC,                                -- RB0160
  acct_rb0161 NUMERIC,                                -- RB0161
  acct_rb0162 NUMERIC,                                -- RB0162
  acct_rb0163 NUMERIC,                                -- RB0163
  acct_rb0164 NUMERIC,                                -- RB0164
  acct_rb0165 NUMERIC,                                -- RB0165
  acct_rb0166 NUMERIC,                                -- RB0166
  acct_rb0167 NUMERIC,                                -- RB0167
  acct_rb0168 NUMERIC,                                -- RB0168
  acct_rb0169 NUMERIC,                                -- RB0169
  acct_rb0170 NUMERIC,                                -- RB0170
  acct_rb0171 NUMERIC,                                -- RB0171
  acct_rb0172 NUMERIC,                                -- RB0172

  UNIQUE(charter_number, year, quarter)
);

CREATE INDEX idx_silver_risk_based_capital_charter ON silver_risk_based_capital(charter_number);
CREATE INDEX idx_silver_risk_based_capital_period ON silver_risk_based_capital(year, quarter);

ALTER TABLE silver_risk_based_capital ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Silver risk based capital readable by authenticated users"
  ON silver_risk_based_capital FOR SELECT
  TO authenticated
  USING (true);
