/**
 * Silver Layer Transformation
 *
 * Reads raw JSONB blobs from bronze_call_reports and transforms them into
 * 15 typed, indexed Silver tables.
 *
 * Usage:
 *   node scripts/transform-silver.js 2025-Q3              # Single quarter
 *   node scripts/transform-silver.js 2024-Q3 2025-Q3      # Range (inclusive)
 *   node scripts/transform-silver.js --all                 # All quarters in Bronze
 *   node scripts/transform-silver.js --latest              # Latest quarter in Bronze
 *
 * Options:
 *   --env <file>    Use custom env file (e.g. --env .env.medallion)
 *   --dry-run       Parse and log without writing to Silver tables
 */

import { createClient } from '@supabase/supabase-js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import { ACCOUNT_CODE_MAP, SILVER_TABLES } from './silver-mapping.js';

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
  console.error('\nTRANSFORM BLOCKED — target project not in allow list');
  console.error(`Target: ${supabaseUrl}`);
  console.error(`Allowed: ${ALLOWED_PROJECTS.join(', ')}`);
  process.exit(1);
}

const supabase = createClient(supabaseUrl, serviceRoleKey);

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const BATCH_SIZE = 200;
const PAGE_SIZE = 1000;  // Supabase query limit per page
const DRY_RUN = process.argv.includes('--dry-run');

// Prefixes for unmapped codes that get dynamic acct_ columns
const DYNAMIC_PREFIXES = {
  'RB': 'silver_risk_based_capital',
  'RL': 'silver_real_estate',
  'NV': 'silver_investments',
  'PC': 'silver_operations',
  'DL': 'silver_delinquency',
  'SL': 'silver_loan_composition',
  'LQ': 'silver_liquidity',
  'LN': 'silver_loan_composition',
  'CM': 'silver_commercial_loans',
};

// Build a set of valid acct_ columns that exist in the DDL for each table.
// Codes outside these ranges are skipped (the DDL only created specific ranges).
function buildValidDynamicColumns() {
  const valid = {};
  for (const table of SILVER_TABLES) {
    valid[table] = new Set();
  }

  // RB0001-RB0172
  for (let i = 1; i <= 172; i++) {
    valid['silver_risk_based_capital'].add(`acct_rb${String(i).padStart(4, '0')}`);
  }
  // RL0001-RL0050
  for (let i = 1; i <= 50; i++) {
    valid['silver_real_estate'].add(`acct_rl${String(i).padStart(4, '0')}`);
  }
  // NV0001-NV0110
  for (let i = 1; i <= 110; i++) {
    valid['silver_investments'].add(`acct_nv${String(i).padStart(4, '0')}`);
  }
  // PC0001-PC0010
  for (let i = 1; i <= 10; i++) {
    valid['silver_operations'].add(`acct_pc${String(i).padStart(4, '0')}`);
  }
  // DL, SL, LQ, LN, CM — no pre-created columns in DDL, so these are empty.
  // Dynamic codes for these prefixes will be skipped.

  return valid;
}

const VALID_DYNAMIC_COLUMNS = buildValidDynamicColumns();

// ---------------------------------------------------------------------------
// Quarter utilities
// ---------------------------------------------------------------------------

function parseQuarter(str) {
  const match = str.match(/^(\d{4})-Q([1-4])$/);
  if (!match) throw new Error(`Invalid quarter format: "${str}". Expected YYYY-QN (e.g. 2025-Q3)`);
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

  if (quarters.length === 0) {
    throw new Error(`Invalid range: ${startStr} to ${endStr}`);
  }
  return quarters;
}

// ---------------------------------------------------------------------------
// CLI parsing
// ---------------------------------------------------------------------------

function parseArgs(argv) {
  // Strip --env <file> and --dry-run from args before parsing
  const filtered = [];
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--env') { i++; continue; }
    if (argv[i] === '--dry-run') continue;
    filtered.push(argv[i]);
  }

  if (filtered.includes('--all')) {
    return { mode: 'all' };
  }

  if (filtered.includes('--latest')) {
    return { mode: 'latest' };
  }

  const quarters = filtered.filter(a => /^\d{4}-Q[1-4]$/.test(a));

  if (quarters.length === 1) {
    return { mode: 'single', quarters };
  }

  if (quarters.length === 2) {
    return { mode: 'range', quarters: expandQuarterRange(quarters[0], quarters[1]) };
  }

  console.error('Usage:');
  console.error('  node scripts/transform-silver.js 2025-Q3              # Single quarter');
  console.error('  node scripts/transform-silver.js 2024-Q3 2025-Q3      # Range (inclusive)');
  console.error('  node scripts/transform-silver.js --all                 # All quarters in Bronze');
  console.error('  node scripts/transform-silver.js --latest              # Latest quarter in Bronze');
  console.error('');
  console.error('Options:');
  console.error('  --env <file>    Custom env file (e.g. --env .env.medallion)');
  console.error('  --dry-run       Parse and log without writing');
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Resolve which quarters to process
// ---------------------------------------------------------------------------

async function resolveQuarters(args) {
  if (args.mode === 'single' || args.mode === 'range') {
    return args.quarters;
  }

  // Query Bronze for available quarters, paginating to avoid the default 1000-row limit
  const seen = new Set();
  const quarters = [];
  let offset = 0;

  while (true) {
    const { data, error } = await supabase
      .from('bronze_call_reports')
      .select('year, quarter')
      .order('year', { ascending: true })
      .order('quarter', { ascending: true })
      .range(offset, offset + PAGE_SIZE - 1);

    if (error) throw new Error(`Failed to query Bronze quarters: ${error.message}`);

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

  if (quarters.length === 0) {
    throw new Error('No data found in bronze_call_reports');
  }

  if (args.mode === 'latest') {
    return [quarters[quarters.length - 1]];
  }

  return quarters;  // --all
}

// ---------------------------------------------------------------------------
// Read Bronze data with pagination
// ---------------------------------------------------------------------------

async function readBronzeQuarter(year, quarter) {
  const allRecords = [];
  let offset = 0;

  while (true) {
    const { data, error } = await supabase
      .from('bronze_call_reports')
      .select('charter_number, year, quarter, period, raw_data')
      .eq('year', year)
      .eq('quarter', quarter)
      .range(offset, offset + PAGE_SIZE - 1);

    if (error) throw new Error(`Failed to read Bronze data: ${error.message}`);

    allRecords.push(...data);

    if (data.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
  }

  return allRecords;
}

// ---------------------------------------------------------------------------
// Map a single CU's raw_data to Silver table rows
// ---------------------------------------------------------------------------

function mapToSilverRows(bronzeRecord) {
  const { charter_number, year, quarter, period, raw_data } = bronzeRecord;

  // Initialize row objects for each Silver table
  const rows = {};
  for (const table of SILVER_TABLES) {
    rows[table] = {
      charter_number,
      year,
      quarter,
      period,
    };
  }

  const unmappedCodes = [];

  for (const [code, value] of Object.entries(raw_data)) {
    // Check explicit mapping first
    const mapping = ACCOUNT_CODE_MAP[code];
    if (mapping) {
      rows[mapping.table][mapping.column] = value;
      continue;
    }

    // Check dynamic prefixes for unmapped codes
    let matched = false;
    for (const [prefix, table] of Object.entries(DYNAMIC_PREFIXES)) {
      if (code.startsWith(prefix)) {
        const colName = `acct_${code.toLowerCase()}`;
        // Only include if the column exists in the DDL
        if (VALID_DYNAMIC_COLUMNS[table]?.has(colName)) {
          rows[table][colName] = value;
        }
        // Either way, this code was matched to a prefix (just may not have a column)
        matched = true;
        break;
      }
    }

    if (!matched) {
      unmappedCodes.push(code);
    }
  }

  // Compute derived fields for silver_net_income
  const revenue = rows['silver_revenue'];
  const expenses = rows['silver_expenses'];
  const netIncome = rows['silver_net_income'];

  const interestIncome = revenue.total_interest_income || 0;
  const nonInterestIncome = revenue.total_non_interest_income || 0;
  const interestExpense = expenses.total_interest_expense || 0;
  const nonInterestExpense = expenses.total_non_interest_expense || 0;
  const provision = expenses.provision_for_loan_losses || 0;
  const ceclCreditLoss = expenses.cecl_total_credit_loss_expense || 0;

  netIncome.total_revenue = interestIncome + nonInterestIncome;
  netIncome.total_expenses = interestExpense + nonInterestExpense;
  netIncome.net_interest_income = interestIncome - interestExpense;
  netIncome.provision_or_credit_loss = ceclCreditLoss || provision;
  netIncome.pre_provision_net_revenue =
    (interestIncome + nonInterestIncome) - nonInterestExpense;
  netIncome.non_interest_income = nonInterestIncome;
  netIncome.non_interest_expense = nonInterestExpense;

  return { rows, unmappedCodes };
}

// ---------------------------------------------------------------------------
// Check if a row has any data columns (beyond the common key columns)
// ---------------------------------------------------------------------------

const COMMON_KEYS = new Set(['charter_number', 'year', 'quarter', 'period']);

function hasDataColumns(row) {
  for (const key of Object.keys(row)) {
    if (!COMMON_KEYS.has(key)) return true;
  }
  return false;
}

// ---------------------------------------------------------------------------
// Batch upsert into a Silver table
// ---------------------------------------------------------------------------

async function upsertBatch(tableName, batch) {
  if (DRY_RUN || batch.length === 0) return true;

  const { error } = await supabase
    .from(tableName)
    .upsert(batch, { onConflict: 'charter_number,year,quarter' });

  if (error) {
    console.error(`  ERROR upserting ${tableName}: ${error.message}`);
    return false;
  }
  return true;
}

// ---------------------------------------------------------------------------
// Process a single quarter
// ---------------------------------------------------------------------------

async function processQuarter(quarterStr) {
  const { year, quarter } = parseQuarter(quarterStr);
  console.log(`\n${'='.repeat(60)}`);
  console.log(`Processing ${quarterStr} (year=${year}, quarter=${quarter})`);
  console.log('='.repeat(60));

  // Read all Bronze data for this quarter
  console.log('  Reading bronze_call_reports...');
  const bronzeRecords = await readBronzeQuarter(year, quarter);
  console.log(`  Found ${bronzeRecords.length} credit unions`);

  if (bronzeRecords.length === 0) {
    console.log('  No data to process, skipping.');
    return { cusProcessed: 0, tableRows: {} };
  }

  // Accumulate Silver rows per table
  const tableBatches = {};
  for (const table of SILVER_TABLES) {
    tableBatches[table] = [];
  }

  let totalUnmapped = new Set();

  for (const record of bronzeRecords) {
    const { rows, unmappedCodes } = mapToSilverRows(record);

    for (const code of unmappedCodes) {
      totalUnmapped.add(code);
    }

    // Add rows to batches (only if they have data beyond the key columns)
    for (const table of SILVER_TABLES) {
      if (hasDataColumns(rows[table])) {
        tableBatches[table].push(rows[table]);
      }
    }
  }

  // Upsert each table in batches
  const tableRows = {};
  for (const table of SILVER_TABLES) {
    const rows = tableBatches[table];
    tableRows[table] = rows.length;

    if (rows.length === 0) {
      console.log(`  ${table}: 0 rows (skipped)`);
      continue;
    }

    let successCount = 0;
    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      const ok = await upsertBatch(table, batch);
      if (ok) successCount += batch.length;
    }

    const label = DRY_RUN ? '(dry run)' : '';
    console.log(`  ${table}: ${successCount}/${rows.length} rows ${label}`);
  }

  if (totalUnmapped.size > 0) {
    console.log(`\n  Unmapped codes (${totalUnmapped.size}): ${[...totalUnmapped].sort().slice(0, 20).join(', ')}${totalUnmapped.size > 20 ? '...' : ''}`);
  }

  return { cusProcessed: bronzeRecords.length, tableRows };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log('Silver Layer Transformation');
  console.log(`Target: ${supabaseUrl}`);
  if (DRY_RUN) console.log('MODE: DRY RUN (no writes)');
  console.log('');

  const args = parseArgs(process.argv.slice(2));
  const quarters = await resolveQuarters(args);

  console.log(`Quarters to process: ${quarters.join(', ')}`);

  const startTime = Date.now();
  let totalCUs = 0;
  const grandTotalRows = {};

  for (const q of quarters) {
    const { cusProcessed, tableRows } = await processQuarter(q);
    totalCUs += cusProcessed;

    for (const [table, count] of Object.entries(tableRows)) {
      grandTotalRows[table] = (grandTotalRows[table] || 0) + count;
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  console.log(`\n${'='.repeat(60)}`);
  console.log('TRANSFORMATION COMPLETE');
  console.log('='.repeat(60));
  console.log(`  Quarters: ${quarters.length}`);
  console.log(`  Total CUs processed: ${totalCUs}`);
  console.log(`  Elapsed: ${elapsed}s`);
  console.log('');
  console.log('  Rows per table:');
  for (const table of SILVER_TABLES) {
    const count = grandTotalRows[table] || 0;
    console.log(`    ${table}: ${count.toLocaleString()}`);
  }
}

main().catch(err => {
  console.error('\nFATAL ERROR:', err.message);
  process.exit(1);
});
