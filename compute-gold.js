/**
 * Gold Layer Computation
 *
 * Reads typed Silver tables and computes CAMEL ratios + metrics for each CU
 * per quarter. Results are upserted into the gold_ratios table.
 *
 * Usage:
 *   node scripts/compute-gold.js 2025-Q3              # Single quarter
 *   node scripts/compute-gold.js 2024-Q3 2025-Q3      # Range (inclusive)
 *   node scripts/compute-gold.js --all                 # All quarters in Silver
 *   node scripts/compute-gold.js --latest              # Latest quarter in Silver
 *
 * Options:
 *   --env <file>    Use custom env file (e.g. --env .env.medallion)
 *   --dry-run       Compute and log without writing to gold_ratios
 */

import { createClient } from '@supabase/supabase-js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

// ---------------------------------------------------------------------------
// Environment & Supabase setup
// ---------------------------------------------------------------------------

const projectRoot = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');

const envFlagIndex = process.argv.indexOf('--env');
if (envFlagIndex !== -1 && process.argv[envFlagIndex + 1]) {
  const customEnvPath = path.join(projectRoot, process.argv[envFlagIndex + 1]);
  if (!fs.existsSync(customEnvPath)) {
    console.error(`ERROR: Env file not found: ${customEnvPath}`);
    process.exit(1);
  }
  dotenv.config({ path: customEnvPath });
} else {
  const envLocalPath = path.join(projectRoot, '.env.local');
  const envPath = path.join(projectRoot, '.env');

  if (fs.existsSync(envLocalPath)) {
    dotenv.config({ path: envLocalPath });
  } else {
    dotenv.config({ path: envPath });
  }
}

const supabaseUrl = process.env.VITE_SUPABASE_URL?.replace(/['"]/g, '').trim();
const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY?.replace(/['"]/g, '').trim();

if (!supabaseUrl || !serviceRoleKey) {
  console.error('ERROR: Missing required environment variables.');
  console.error('  - VITE_SUPABASE_URL');
  console.error('  - SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}

// Allowed Supabase project refs (dev + medallion)
const ALLOWED_PROJECTS = ['jhlkpogfkytfedqupytv', 'ckpihayqxwplgxdmijyp'];
const isAllowed = ALLOWED_PROJECTS.some(ref => supabaseUrl.includes(ref));

if (!isAllowed) {
  console.error('\nCOMPUTE BLOCKED — target project not in allow list');
  console.error(`Target: ${supabaseUrl}`);
  console.error(`Allowed: ${ALLOWED_PROJECTS.join(', ')}`);
  process.exit(1);
}

const supabase = createClient(supabaseUrl, serviceRoleKey);

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const BATCH_SIZE = 200;
const PAGE_SIZE = 1000;
const DRY_RUN = process.argv.includes('--dry-run');

// Silver tables we read from
const SILVER_SOURCES = [
  'silver_assets',
  'silver_liabilities',
  'silver_capital',
  'silver_revenue',
  'silver_expenses',
  'silver_net_income',
  'silver_loan_composition',
  'silver_delinquency',
  'silver_charge_offs',
  'silver_commercial_loans',
  'silver_liquidity',
  'silver_operations',
];

// ---------------------------------------------------------------------------
// Quarter utilities (same as transform-silver.js)
// ---------------------------------------------------------------------------

function parseQuarter(str) {
  const match = str.match(/^(\d{4})-Q([1-4])$/);
  if (!match) throw new Error(`Invalid quarter format: "${str}". Expected YYYY-QN`);
  return { year: parseInt(match[1]), quarter: parseInt(match[2]) };
}

function expandQuarterRange(startStr, endStr) {
  const start = parseQuarter(startStr);
  const end = parseQuarter(endStr);
  const quarters = [];

  let y = start.year;
  let q = start.quarter;

  while (y < end.year || (y === end.year && q <= end.quarter)) {
    quarters.push(`${y}-Q${q}`);
    q++;
    if (q > 4) { q = 1; y++; }
  }

  if (quarters.length === 0) throw new Error(`Invalid range: ${startStr} to ${endStr}`);
  return quarters;
}

function parseArgs(argv) {
  const filtered = [];
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--env') { i++; continue; }
    if (argv[i] === '--dry-run') continue;
    filtered.push(argv[i]);
  }

  if (filtered.includes('--all')) return { mode: 'all' };
  if (filtered.includes('--latest')) return { mode: 'latest' };

  const quarters = filtered.filter(a => /^\d{4}-Q[1-4]$/.test(a));

  if (quarters.length === 1) return { mode: 'single', quarters };
  if (quarters.length === 2) return { mode: 'range', quarters: expandQuarterRange(quarters[0], quarters[1]) };

  console.error('Usage:');
  console.error('  node scripts/compute-gold.js 2025-Q3              # Single quarter');
  console.error('  node scripts/compute-gold.js 2024-Q3 2025-Q3      # Range');
  console.error('  node scripts/compute-gold.js --all                 # All quarters in Silver');
  console.error('  node scripts/compute-gold.js --latest              # Latest quarter');
  console.error('');
  console.error('Options:');
  console.error('  --env <file>    Custom env file');
  console.error('  --dry-run       Compute without writing');
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Resolve which quarters to process (from Silver tables)
// ---------------------------------------------------------------------------

async function resolveQuarters(args) {
  if (args.mode === 'single' || args.mode === 'range') return args.quarters;

  // Paginate to avoid the default 1000-row limit
  const seen = new Set();
  const quarters = [];
  let offset = 0;

  while (true) {
    const { data, error } = await supabase
      .from('silver_assets')
      .select('year, quarter')
      .order('year', { ascending: true })
      .order('quarter', { ascending: true })
      .range(offset, offset + PAGE_SIZE - 1);

    if (error) throw new Error(`Failed to query Silver quarters: ${error.message}`);

    for (const row of data) {
      const key = `${row.year}-Q${row.quarter}`;
      if (!seen.has(key)) {
        seen.add(key);
        quarters.push(key);
      }
    }

    if (data.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
  }

  if (quarters.length === 0) throw new Error('No data found in silver_assets');
  if (args.mode === 'latest') return [quarters[quarters.length - 1]];
  return quarters;
}

// ---------------------------------------------------------------------------
// Read paginated Silver data for a single table + quarter
// ---------------------------------------------------------------------------

async function readSilverTable(tableName, year, quarter, selectColumns = '*') {
  const allRecords = [];
  let offset = 0;

  while (true) {
    const { data, error } = await supabase
      .from(tableName)
      .select(selectColumns)
      .eq('year', year)
      .eq('quarter', quarter)
      .range(offset, offset + PAGE_SIZE - 1);

    if (error) throw new Error(`Failed to read ${tableName}: ${error.message}`);
    allRecords.push(...data);
    if (data.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
  }

  return allRecords;
}

// ---------------------------------------------------------------------------
// Build a charter_number -> credit_union_id lookup
// ---------------------------------------------------------------------------

async function buildCUIdLookup() {
  const lookup = {};
  let offset = 0;

  while (true) {
    const { data, error } = await supabase
      .from('credit_unions')
      .select('id, charter_number')
      .range(offset, offset + PAGE_SIZE - 1);

    if (error) throw new Error(`Failed to read credit_unions: ${error.message}`);
    for (const row of data) {
      lookup[row.charter_number] = row.id;
    }
    if (data.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
  }

  return lookup;
}

// ---------------------------------------------------------------------------
// Index Silver data by charter_number for fast lookups
// ---------------------------------------------------------------------------

function indexByCharter(records) {
  const map = {};
  for (const r of records) {
    map[r.charter_number] = r;
  }
  return map;
}

// ---------------------------------------------------------------------------
// Safe division (returns null if denominator is 0 or null)
// ---------------------------------------------------------------------------

function safeDivide(numerator, denominator) {
  if (!denominator || denominator === 0) return null;
  if (numerator == null) return null;
  return numerator / denominator;
}

// Round to 2 decimal places (for ratios/percentages). Returns null if input is null.
function round2(value) {
  if (value == null) return null;
  return Math.round(value * 100) / 100;
}

// ---------------------------------------------------------------------------
// Annualization factor for YTD income/expense items
// NCUA call report income statement values are cumulative YTD.
// To annualize: multiply by (4 / quarter).
// Q1 = *4, Q2 = *2, Q3 = *4/3, Q4 = *1 (already full year)
// ---------------------------------------------------------------------------

function annFactor(quarter) {
  return 4 / quarter;
}

// ---------------------------------------------------------------------------
// Compute Gold ratios for a single CU in a single quarter
// ---------------------------------------------------------------------------

function computeRatios(charter, quarter, silver, priorYear, priorQ4, cuIdLookup) {
  const assets = silver.assets || {};
  const liabilities = silver.liabilities || {};
  const capital = silver.capital || {};
  const revenue = silver.revenue || {};
  const expenses = silver.expenses || {};
  const netIncome = silver.netIncome || {};
  const loans = silver.loanComp || {};
  const delinquency = silver.delinquency || {};
  const chargeOffs = silver.chargeOffs || {};
  const commercial = silver.commercial || {};
  const liquidity = silver.liquidity || {};
  const operations = silver.operations || {};

  const ann = annFactor(quarter);

  // Core values
  const totalAssets = assets.total_assets || 0;
  const totalLoans = assets.total_loans_and_leases || 0;
  const totalShares = liabilities.total_shares_and_deposits || 0;
  const totalBorrowings = liquidity.total_borrowings || 0;
  const totalNetWorth = capital.total_net_worth || 0;
  const netIncomeVal = netIncome.net_income || 0;
  const totalIntIncome = revenue.total_interest_income || 0;
  const totalNonIntIncome = revenue.total_non_interest_income || 0;
  const totalIntExpense = expenses.total_interest_expense || 0;
  const totalNonIntExpense = expenses.total_non_interest_expense || 0;
  const interestOnLoans = revenue.interest_on_loans || 0;
  const members = operations.num_current_members || 0;
  const fullTime = operations.num_full_time_employees || 0;
  const partTime = operations.num_part_time_employees || 0;
  const totalEmployees = fullTime + 0.5 * partTime;

  // Allowance: prefer CECL, fall back to legacy
  const allowance = (capital.cecl_allowance_loans_leases || 0) || (assets.allowance_for_loan_losses || 0);

  // NII
  const nii = totalIntIncome - totalIntExpense;

  // Average assets (using prior year-end Q4 if available)
  const priorYearEndAssets = priorQ4?.assets?.total_assets || 0;
  const avgAssets = priorYearEndAssets > 0 ? (totalAssets + priorYearEndAssets) / 2 : totalAssets;

  // Average loans (for charge-off ratio)
  const priorYearEndLoans = priorQ4?.assets?.total_loans_and_leases || 0;
  const avgLoans = priorYearEndLoans > 0 ? (totalLoans + priorYearEndLoans) / 2 : totalLoans;

  // Delinquent loans
  const delinquentLoans = delinquency.total_delinquent_loans || 0;

  // Net charge-offs (YTD)
  const grossChargeOffs = chargeOffs.total_charge_offs_ytd || 0;
  const recoveries = chargeOffs.total_recoveries_ytd || 0;
  const netChargeOffsVal = grossChargeOffs - recoveries;

  // Loan concentrations
  const newVehicle = loans.new_vehicle_loans || 0;
  const usedVehicle = loans.used_vehicle_loans || 0;
  const firstLien = loans.first_lien_re_loans || 0;
  const juniorLien = loans.junior_lien_re_loans || 0;
  const otherRE = loans.all_other_re_loans || 0;
  const totalCommercial = commercial.total_commercial_loans || 0;
  const creditCard = loans.unsecured_credit_card_loans || 0;

  // Prior year same quarter (for YoY growth)
  const py = priorYear || {};
  const pyAssets = py.assets?.total_assets;
  const pyLoans = py.assets?.total_loans_and_leases;
  const pyShares = py.liabilities?.total_shares_and_deposits;
  const pyMembers = py.operations?.num_current_members;
  const pyNetWorth = py.capital?.total_net_worth;

  function yoyGrowth(current, prior) {
    if (!prior || prior === 0 || current == null) return null;
    return round2(((current - prior) / Math.abs(prior)) * 100);
  }

  // Build the gold_ratios row
  const row = {
    charter_number: charter,
    credit_union_id: cuIdLookup[charter] || null,
    year: assets.year,
    quarter: assets.quarter,
    period: assets.period,

    // Capital
    net_worth_ratio: round2(safeDivide(totalNetWorth, totalAssets) != null
      ? safeDivide(totalNetWorth, totalAssets) * 100 : null),
    net_worth: totalNetWorth || null,
    net_worth_growth_yoy: yoyGrowth(totalNetWorth, pyNetWorth),

    // Asset Quality
    delinquency_rate: round2(safeDivide(delinquentLoans, totalLoans) != null
      ? safeDivide(delinquentLoans, totalLoans) * 100 : null),
    charge_off_ratio: round2(safeDivide(netChargeOffsVal, avgLoans) != null
      ? safeDivide(netChargeOffsVal, avgLoans) * 100 * ann : null),
    net_charge_offs: netChargeOffsVal || null,
    allowance_to_loans: round2(safeDivide(allowance, totalLoans) != null
      ? safeDivide(allowance, totalLoans) * 100 : null),
    coverage_ratio: round2(safeDivide(allowance, delinquentLoans) != null
      ? safeDivide(allowance, delinquentLoans) * 100 : null),

    // Concentrations
    auto_loan_concentration: round2(safeDivide(newVehicle + usedVehicle, totalLoans) != null
      ? safeDivide(newVehicle + usedVehicle, totalLoans) * 100 : null),
    real_estate_concentration: round2(safeDivide(firstLien + juniorLien + otherRE, totalLoans) != null
      ? safeDivide(firstLien + juniorLien + otherRE, totalLoans) * 100 : null),
    commercial_concentration: round2(safeDivide(totalCommercial, totalLoans) != null
      ? safeDivide(totalCommercial, totalLoans) * 100 : null),
    credit_card_concentration: round2(safeDivide(creditCard, totalLoans) != null
      ? safeDivide(creditCard, totalLoans) * 100 : null),

    // Management
    asset_growth_yoy: yoyGrowth(totalAssets, pyAssets),
    loan_growth_yoy: yoyGrowth(totalLoans, pyLoans),
    share_growth_yoy: yoyGrowth(totalShares, pyShares),
    member_growth_yoy: yoyGrowth(members, pyMembers),
    total_members: members || null,
    total_employees: totalEmployees || null,
    members_per_employee: round2(safeDivide(members, totalEmployees)),
    assets_per_employee: Math.round(safeDivide(totalAssets, totalEmployees)) || null,

    // Earnings
    roa: round2(safeDivide(netIncomeVal, totalAssets) != null
      ? safeDivide(netIncomeVal, totalAssets) * 100 * ann : null),
    roaa: round2(safeDivide(netIncomeVal, avgAssets) != null
      ? safeDivide(netIncomeVal, avgAssets) * 100 * ann : null),
    roe: round2(safeDivide(netIncomeVal, totalNetWorth) != null
      ? safeDivide(netIncomeVal, totalNetWorth) * 100 * ann : null),
    efficiency_ratio: round2(safeDivide(totalNonIntExpense, nii + totalNonIntIncome) != null
      ? safeDivide(totalNonIntExpense, nii + totalNonIntIncome) * 100 : null),
    net_interest_margin: round2(safeDivide(nii, avgAssets) != null
      ? safeDivide(nii, avgAssets) * 100 * ann : null),
    yield_on_loans: round2(safeDivide(interestOnLoans, totalLoans) != null
      ? safeDivide(interestOnLoans, totalLoans) * 100 * ann : null),
    cost_of_funds: round2(safeDivide(totalIntExpense, totalShares) != null
      ? safeDivide(totalIntExpense, totalShares) * 100 * ann : null),
    non_interest_income_ratio: round2(safeDivide(totalNonIntIncome, nii + totalNonIntIncome) != null
      ? safeDivide(totalNonIntIncome, nii + totalNonIntIncome) * 100 : null),
    gross_income: (totalIntIncome + totalNonIntIncome) || null,
    total_assets: totalAssets || null,
    total_loans: totalLoans || null,
    total_shares: totalShares || null,
    total_borrowings: totalBorrowings || null,

    // Liquidity
    cash_ratio: round2(safeDivide(assets.total_cash_and_deposits || 0, totalAssets) != null
      ? safeDivide(assets.total_cash_and_deposits || 0, totalAssets) * 100 : null),
    loan_to_share_ratio: round2(safeDivide(totalLoans, totalShares) != null
      ? safeDivide(totalLoans, totalShares) * 100 : null),
    loans_to_assets_ratio: round2(safeDivide(totalLoans, totalAssets) != null
      ? safeDivide(totalLoans, totalAssets) * 100 : null),
    borrowings_to_assets: round2(safeDivide(totalBorrowings, totalAssets) != null
      ? safeDivide(totalBorrowings, totalAssets) * 100 : null),
  };

  return row;
}

// ---------------------------------------------------------------------------
// Process a single quarter
// ---------------------------------------------------------------------------

async function processQuarter(quarterStr, cuIdLookup) {
  const { year, quarter } = parseQuarter(quarterStr);
  console.log(`\n${'='.repeat(60)}`);
  console.log(`Computing Gold ratios for ${quarterStr}`);
  console.log('='.repeat(60));

  // Read current quarter Silver data (select only needed columns)
  console.log('  Reading Silver tables...');

  const [
    assetsData, liabilitiesData, capitalData, revenueData,
    expensesData, netIncomeData, loanCompData, delinquencyData,
    chargeOffsData, commercialData, liquidityData, operationsData,
  ] = await Promise.all(SILVER_SOURCES.map(t => readSilverTable(t, year, quarter)));

  console.log(`  Found ${assetsData.length} CUs in silver_assets`);

  if (assetsData.length === 0) {
    console.log('  No data, skipping.');
    return 0;
  }

  // Index by charter_number
  const assets = indexByCharter(assetsData);
  const liabilities = indexByCharter(liabilitiesData);
  const capital = indexByCharter(capitalData);
  const revenue = indexByCharter(revenueData);
  const expenses = indexByCharter(expensesData);
  const netIncome = indexByCharter(netIncomeData);
  const loanComp = indexByCharter(loanCompData);
  const delinquency = indexByCharter(delinquencyData);
  const chargeOffs = indexByCharter(chargeOffsData);
  const commercial = indexByCharter(commercialData);
  const liquidity = indexByCharter(liquidityData);
  const operations = indexByCharter(operationsData);

  // Read prior year same quarter (for YoY growth)
  const pyYear = year - 1;
  console.log(`  Reading prior year ${pyYear}-Q${quarter} for YoY growth...`);
  const [pyAssets, pyLiabilities, pyCapital, pyOperations] = await Promise.all([
    readSilverTable('silver_assets', pyYear, quarter),
    readSilverTable('silver_liabilities', pyYear, quarter),
    readSilverTable('silver_capital', pyYear, quarter),
    readSilverTable('silver_operations', pyYear, quarter),
  ]);
  console.log(`  Prior year: ${pyAssets.length} CUs found`);

  const pyAssetsMap = indexByCharter(pyAssets);
  const pyLiabilitiesMap = indexByCharter(pyLiabilities);
  const pyCapitalMap = indexByCharter(pyCapital);
  const pyOperationsMap = indexByCharter(pyOperations);

  // Read prior year-end (Q4) for average assets
  const q4Year = year - 1;
  console.log(`  Reading ${q4Year}-Q4 for average assets...`);
  const [q4Assets] = await Promise.all([
    readSilverTable('silver_assets', q4Year, 4),
  ]);
  console.log(`  Prior Q4: ${q4Assets.length} CUs found`);
  const q4AssetsMap = indexByCharter(q4Assets);

  // Compute ratios for each CU
  const goldRows = [];
  const charterNumbers = Object.keys(assets).map(Number);

  for (const charter of charterNumbers) {
    const silver = {
      assets: assets[charter],
      liabilities: liabilities[charter],
      capital: capital[charter],
      revenue: revenue[charter],
      expenses: expenses[charter],
      netIncome: netIncome[charter],
      loanComp: loanComp[charter],
      delinquency: delinquency[charter],
      chargeOffs: chargeOffs[charter],
      commercial: commercial[charter],
      liquidity: liquidity[charter],
      operations: operations[charter],
    };

    const priorYear = {
      assets: pyAssetsMap[charter],
      liabilities: pyLiabilitiesMap[charter],
      capital: pyCapitalMap[charter],
      operations: pyOperationsMap[charter],
    };

    const priorQ4 = {
      assets: q4AssetsMap[charter],
    };

    const row = computeRatios(charter, quarter, silver, priorYear, priorQ4, cuIdLookup);
    goldRows.push(row);
  }

  console.log(`  Computed ${goldRows.length} Gold ratio rows`);

  // Upsert in batches
  if (!DRY_RUN) {
    let successCount = 0;
    for (let i = 0; i < goldRows.length; i += BATCH_SIZE) {
      const batch = goldRows.slice(i, i + BATCH_SIZE);
      const { error } = await supabase
        .from('gold_ratios')
        .upsert(batch, { onConflict: 'charter_number,year,quarter' });

      if (error) {
        console.error(`  ERROR upserting gold_ratios batch: ${error.message}`);
      } else {
        successCount += batch.length;
      }
    }
    console.log(`  Upserted ${successCount}/${goldRows.length} rows`);
  } else {
    console.log(`  (dry run) Would upsert ${goldRows.length} rows`);
  }

  return goldRows.length;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log('Gold Layer Computation');
  console.log(`Target: ${supabaseUrl}`);
  if (DRY_RUN) console.log('MODE: DRY RUN (no writes)');
  console.log('');

  const args = parseArgs(process.argv.slice(2));
  const quarters = await resolveQuarters(args);

  console.log(`Quarters to process: ${quarters.join(', ')}`);

  // Build CU ID lookup once
  console.log('Building charter_number -> credit_union_id lookup...');
  const cuIdLookup = await buildCUIdLookup();
  console.log(`  Found ${Object.keys(cuIdLookup).length} credit unions`);

  const startTime = Date.now();
  let totalRows = 0;

  for (const q of quarters) {
    const count = await processQuarter(q, cuIdLookup);
    totalRows += count;
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log(`\n${'='.repeat(60)}`);
  console.log('GOLD COMPUTATION COMPLETE');
  console.log('='.repeat(60));
  console.log(`  Quarters: ${quarters.length}`);
  console.log(`  Total rows computed: ${totalRows.toLocaleString()}`);
  console.log(`  Elapsed: ${elapsed}s`);
}

main().catch(err => {
  console.error('\nFATAL ERROR:', err.message);
  process.exit(1);
});
