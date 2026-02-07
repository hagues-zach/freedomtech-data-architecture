/**
 * Bronze Layer NCUA Call Report Import
 *
 * Auto-downloads NCUA 5300 Call Report ZIPs, stream-parses CSVs, and stores
 * data into two Bronze tables:
 *
 *   bronze_call_reports  — All ~780 account codes as a JSONB blob (financial data)
 *   bronze_cu_profiles   — All ~24 FOICU profile fields as a JSONB blob (institutional data)
 *
 * One row per credit union per quarter in each table.
 *
 * Usage:
 *   node scripts/import-bronze.js 2025-Q3              # Single quarter
 *   node scripts/import-bronze.js 2024-Q3 2025-Q3      # Range (inclusive)
 *   node scripts/import-bronze.js --latest              # Auto-detect latest
 */

import { createClient } from '@supabase/supabase-js';
import { parse } from 'csv-parse';
import AdmZip from 'adm-zip';
import fs from 'fs';
import path from 'path';
import os from 'os';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

// ---------------------------------------------------------------------------
// Environment & Supabase setup
// ---------------------------------------------------------------------------

const projectRoot = path.join(path.dirname(fileURLToPath(import.meta.url)), '..');

// Support --env flag for custom env file (e.g. --env .env.medallion)
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
  console.error('\nIMPORT BLOCKED — target project not in allow list');
  console.error(`Target: ${supabaseUrl}`);
  console.error(`Allowed: ${ALLOWED_PROJECTS.join(', ')}`);
  process.exit(1);
}

const supabase = createClient(supabaseUrl, serviceRoleKey);

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const QUARTER_MONTH_MAP = { 1: '03', 2: '06', 3: '09', 4: '12' };

const NCUA_BASE_URL = 'https://www.ncua.gov/files/publications/analysis';

const FS220_FILES = [
  'FS220.txt', 'FS220A.txt', 'FS220B.txt', 'FS220C.txt', 'FS220D.txt',
  'FS220G.txt', 'FS220H.txt', 'FS220I.txt', 'FS220J.txt', 'FS220K.txt',
  'FS220L.txt', 'FS220M.txt', 'FS220N.txt', 'FS220P.txt', 'FS220Q.txt',
  'FS220R.txt', 'FS220S.txt'
];

// FOICU columns that are join/metadata keys, not profile data
const FOICU_METADATA_KEYS = new Set(['CU_NUMBER', 'CYCLE_DATE', 'JOIN_NUMBER']);

const CU_BATCH_SIZE = 500;
const BRONZE_BATCH_SIZE = 200;
const PROFILE_BATCH_SIZE = 500;

// ---------------------------------------------------------------------------
// Quarter utilities
// ---------------------------------------------------------------------------

function parseQuarter(str) {
  const match = str.match(/^(\d{4})-Q([1-4])$/);
  if (!match) throw new Error(`Invalid quarter format: "${str}". Expected YYYY-QN (e.g. 2025-Q3)`);
  return { year: parseInt(match[1]), quarter: parseInt(match[2]) };
}

function quarterToUrl(quarterStr) {
  const { year, quarter } = parseQuarter(quarterStr);
  return `${NCUA_BASE_URL}/call-report-data-${year}-${QUARTER_MONTH_MAP[quarter]}.zip`;
}

function quarterToFilingDate(quarterStr) {
  const { year, quarter } = parseQuarter(quarterStr);
  const month = QUARTER_MONTH_MAP[quarter];
  const lastDay = { '03': '31', '06': '30', '09': '30', '12': '31' };
  return `${year}-${month}-${lastDay[month]}`;
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

async function detectLatestQuarter() {
  const now = new Date();
  const probe = new Date(now.getTime() - 45 * 24 * 60 * 60 * 1000);

  let year = probe.getFullYear();
  let quarter = Math.ceil((probe.getMonth() + 1) / 3);

  for (let attempt = 0; attempt < 4; attempt++) {
    const qStr = `${year}-Q${quarter}`;
    const url = quarterToUrl(qStr);

    try {
      const response = await fetch(url, { method: 'HEAD' });
      if (response.ok) {
        console.log(`  Latest available quarter: ${qStr}`);
        return qStr;
      }
    } catch {
      // Network error, try previous quarter
    }

    quarter--;
    if (quarter < 1) { quarter = 4; year--; }
  }

  throw new Error('Could not detect latest available quarter from NCUA');
}

// ---------------------------------------------------------------------------
// CLI parsing
// ---------------------------------------------------------------------------

function parseArgs(argv) {
  // Strip --env <file> from args before parsing
  const filtered = [];
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--env') { i++; continue; }  // skip --env and its value
    filtered.push(argv[i]);
  }

  if (filtered.includes('--latest')) {
    return { mode: 'latest', quarters: [] };
  }

  const quarters = filtered.filter(a => /^\d{4}-Q[1-4]$/.test(a));

  if (quarters.length === 1) {
    return { mode: 'single', quarters };
  }

  if (quarters.length === 2) {
    return { mode: 'range', quarters: expandQuarterRange(quarters[0], quarters[1]) };
  }

  console.error('Usage:');
  console.error('  node scripts/import-bronze.js 2025-Q3              # Single quarter');
  console.error('  node scripts/import-bronze.js 2024-Q3 2025-Q3      # Range (inclusive)');
  console.error('  node scripts/import-bronze.js --latest              # Latest available');
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Download & extraction
// ---------------------------------------------------------------------------

async function downloadQuarter(quarterStr) {
  const url = quarterToUrl(quarterStr);
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), `ncua-bronze-${quarterStr}-`));
  const zipPath = path.join(tempDir, `${quarterStr}.zip`);

  console.log(`  Downloading ${url}...`);
  const response = await fetch(url);

  if (!response.ok) {
    cleanupTemp(tempDir);
    throw new Error(`Download failed: ${response.status} ${response.statusText} for ${url}`);
  }

  const buffer = Buffer.from(await response.arrayBuffer());
  fs.writeFileSync(zipPath, buffer);

  console.log(`  Extracting ZIP (${(buffer.length / 1024 / 1024).toFixed(1)} MB)...`);
  const extractDir = path.join(tempDir, 'data');
  fs.mkdirSync(extractDir);

  const zip = new AdmZip(zipPath);
  zip.extractAllTo(extractDir, true);
  fs.unlinkSync(zipPath);

  // Handle case where files are in a subdirectory
  const resolvedDir = resolveDataDir(extractDir);

  return { tempDir, dataDir: resolvedDir, sourceUrl: url };
}

function resolveDataDir(extractDir) {
  if (fs.existsSync(path.join(extractDir, 'FOICU.txt'))) {
    return extractDir;
  }

  const entries = fs.readdirSync(extractDir, { withFileTypes: true });
  for (const entry of entries) {
    if (entry.isDirectory()) {
      const subDir = path.join(extractDir, entry.name);
      if (fs.existsSync(path.join(subDir, 'FOICU.txt'))) {
        return subDir;
      }
    }
  }

  throw new Error(`FOICU.txt not found in extracted data at ${extractDir}`);
}

function cleanupTemp(tempDir) {
  try {
    fs.rmSync(tempDir, { recursive: true, force: true });
  } catch {
    // Best effort cleanup
  }
}

// ---------------------------------------------------------------------------
// Stream CSV parsing
// ---------------------------------------------------------------------------

function streamParseFile(filePath) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(filePath)) {
      resolve(new Map());
      return;
    }

    const index = new Map();
    let skippedCount = 0;
    const fileName = path.basename(filePath);

    const parser = fs.createReadStream(filePath).pipe(
      parse({
        columns: true,
        delimiter: ',',
        skip_empty_lines: true,
        relax_column_count: true,
        relax_quotes: true,
        skip_records_with_error: true,
        trim: true,
        quote: '"'
      })
    );

    parser.on('data', (record) => {
      const cuNumber = record.CU_NUMBER || record.CU_Number;
      if (cuNumber) {
        index.set(cuNumber.toString().trim(), record);
      }
    });

    parser.on('skip', (err) => {
      skippedCount++;
      if (skippedCount <= 5) {
        console.warn(`      Warning: Skipped malformed row in ${fileName} (line ~${err.lines || '?'}): ${err.code || err.message}`);
      }
    });

    parser.on('end', () => {
      if (skippedCount > 0) {
        console.warn(`      ${fileName}: ${skippedCount} malformed row(s) skipped`);
      }
      resolve(index);
    });
    parser.on('error', reject);
  });
}

// ---------------------------------------------------------------------------
// Data assembly
// ---------------------------------------------------------------------------

function normalizeAccountCode(code) {
  if (code.startsWith('ACCT_') || code.startsWith('Acct_')) {
    return code.substring(5);
  }
  return code;
}

function parseNumeric(value) {
  if (value === '' || value === null || value === undefined) return null;
  const num = parseFloat(value);
  return isNaN(num) ? null : num;
}

function extractAccountCodes(record) {
  const codes = {};
  for (const [key, value] of Object.entries(record)) {
    if (key.startsWith('ACCT_') || key.startsWith('Acct_')) {
      const numValue = parseNumeric(value);
      if (numValue !== null) {
        codes[normalizeAccountCode(key)] = numValue;
      }
    }
  }
  return codes;
}

function buildFinancialBlob(cuNumber, fs220Maps) {
  const blob = {};

  for (const fsMap of fs220Maps) {
    const record = fsMap.get(cuNumber);
    if (!record) continue;

    const codes = extractAccountCodes(record);
    Object.assign(blob, codes);
  }

  return blob;
}

function buildProfileBlob(foicuRecord) {
  const blob = {};

  for (const [key, value] of Object.entries(foicuRecord)) {
    if (FOICU_METADATA_KEYS.has(key)) continue;
    if (value === '' || value === null || value === undefined) continue;
    blob[key] = value;
  }

  return blob;
}

function getPeerGroup(totalAssets) {
  if (totalAssets >= 10_000_000_000) return 'E';
  if (totalAssets >= 1_000_000_000) return 'D';
  if (totalAssets >= 250_000_000) return 'C';
  if (totalAssets >= 50_000_000) return 'B';
  return 'A';
}

// ---------------------------------------------------------------------------
// Database operations
// ---------------------------------------------------------------------------

async function upsertCreditUnions(batch) {
  const { error } = await supabase
    .from('credit_unions')
    .upsert(batch, { onConflict: 'charter_number' });

  if (error) {
    console.error(`    Error upserting credit unions batch: ${error.message}`);
    return false;
  }
  return true;
}

async function upsertBronzeReports(batch) {
  const { error } = await supabase
    .from('bronze_call_reports')
    .upsert(batch, { onConflict: 'charter_number,year,quarter' });

  if (error) {
    console.error(`    Error upserting bronze reports batch: ${error.message}`);
    return false;
  }
  return true;
}

async function upsertBronzeProfiles(batch) {
  const { error } = await supabase
    .from('bronze_cu_profiles')
    .upsert(batch, { onConflict: 'charter_number,year,quarter' });

  if (error) {
    console.error(`    Error upserting bronze profiles batch: ${error.message}`);
    return false;
  }
  return true;
}

// ---------------------------------------------------------------------------
// Import a single quarter
// ---------------------------------------------------------------------------

async function importSingleQuarter(quarterStr) {
  const startTime = Date.now();
  const { year, quarter } = parseQuarter(quarterStr);
  const filingDate = quarterToFilingDate(quarterStr);

  console.log(`\n========================================`);
  console.log(`  Bronze Import: ${quarterStr}`);
  console.log(`========================================\n`);

  // Step 1: Download and extract
  console.log('Step 1: Downloading NCUA data...');
  const { tempDir, dataDir, sourceUrl } = await downloadQuarter(quarterStr);

  try {
    // Step 2: Stream-parse all files
    console.log('Step 2: Parsing CSV files...');

    const foicuMap = await streamParseFile(path.join(dataDir, 'FOICU.txt'));
    console.log(`    FOICU.txt: ${foicuMap.size} credit unions`);

    if (foicuMap.size === 0) {
      console.error(`    FOICU.txt is empty. Skipping ${quarterStr}.`);
      return { processed: 0, skipped: 0, errors: 0 };
    }

    const fs220Maps = [];
    for (const fileName of FS220_FILES) {
      const filePath = path.join(dataDir, fileName);
      const map = await streamParseFile(filePath);
      fs220Maps.push(map);
      if (map.size > 0) {
        console.log(`    ${fileName}: ${map.size} records`);
      }
    }

    // Step 3: Build records and upsert
    console.log('Step 3: Building and upserting records...');

    let cuBatch = [];
    let reportBatch = [];
    let profileBatch = [];
    let processed = 0;
    let skipped = 0;
    let errors = 0;

    for (const [cuNumber, foicuRecord] of foicuMap) {
      const charterNumber = parseInt(cuNumber);
      if (!charterNumber) { skipped++; continue; }

      // Build separate JSONB blobs
      const financialData = buildFinancialBlob(cuNumber, fs220Maps);
      const profileData = buildProfileBlob(foicuRecord);

      // Total assets for credit_unions table and skip check
      const totalAssets = financialData['010'] || 0;
      if (totalAssets <= 0) { skipped++; continue; }

      const totalMembers = financialData['083'] || 0;

      // Queue credit_unions upsert
      cuBatch.push({
        charter_number: charterNumber,
        name: (foicuRecord.CU_NAME || `Credit Union ${charterNumber}`).trim(),
        city: (foicuRecord.CITY || 'Unknown').trim(),
        state: (foicuRecord.STATE || 'XX').trim(),
        total_assets: Math.round(totalAssets),
        total_members: Math.round(totalMembers),
        peer_group: getPeerGroup(totalAssets),
        updated_at: new Date().toISOString()
      });

      // Queue bronze_call_reports upsert (financial data)
      reportBatch.push({
        charter_number: charterNumber,
        year,
        quarter,
        period: quarterStr,
        filing_date: filingDate,
        raw_data: financialData,
        source_url: sourceUrl
      });

      // Queue bronze_cu_profiles upsert (profile data)
      profileBatch.push({
        charter_number: charterNumber,
        year,
        quarter,
        period: quarterStr,
        filing_date: filingDate,
        raw_data: profileData,
        source_url: sourceUrl
      });

      // Flush credit_unions batch
      if (cuBatch.length >= CU_BATCH_SIZE) {
        const ok = await upsertCreditUnions(cuBatch);
        if (!ok) errors++;
        cuBatch = [];
      }

      // Flush bronze reports batch
      if (reportBatch.length >= BRONZE_BATCH_SIZE) {
        const ok = await upsertBronzeReports(reportBatch);
        if (!ok) errors++;
        reportBatch = [];
      }

      // Flush bronze profiles batch
      if (profileBatch.length >= PROFILE_BATCH_SIZE) {
        const ok = await upsertBronzeProfiles(profileBatch);
        if (!ok) errors++;
        profileBatch = [];
      }

      processed++;
      if (processed % 1000 === 0) {
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
        console.log(`    Processed ${processed}/${foicuMap.size} (${elapsed}s)`);
      }
    }

    // Flush remaining
    if (cuBatch.length > 0) {
      const ok = await upsertCreditUnions(cuBatch);
      if (!ok) errors++;
    }
    if (reportBatch.length > 0) {
      const ok = await upsertBronzeReports(reportBatch);
      if (!ok) errors++;
    }
    if (profileBatch.length > 0) {
      const ok = await upsertBronzeProfiles(profileBatch);
      if (!ok) errors++;
    }

    const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`\n  Complete: ${processed} CUs imported, ${skipped} skipped, ${errors} batch errors (${totalTime}s)`);

    return { processed, skipped, errors };

  } finally {
    // Step 4: Cleanup
    console.log('Step 4: Cleaning up temp files...');
    cleanupTemp(tempDir);
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log('Bronze Layer NCUA Import');
  console.log(`Target: ${supabaseUrl}\n`);

  const args = parseArgs(process.argv.slice(2));

  let quarters = args.quarters;

  if (args.mode === 'latest') {
    const latest = await detectLatestQuarter();
    quarters = [latest];
  }

  console.log(`Quarters to import: ${quarters.join(', ')}`);

  let totalProcessed = 0;
  let totalErrors = 0;

  for (const q of quarters) {
    try {
      const result = await importSingleQuarter(q);
      totalProcessed += result.processed;
      totalErrors += result.errors;
    } catch (err) {
      console.error(`\n  FAILED: ${q} - ${err.message}`);
      totalErrors++;
    }
  }

  console.log(`\n========================================`);
  console.log(`  All done! ${quarters.length} quarter(s), ${totalProcessed} total CUs, ${totalErrors} errors`);
  console.log(`========================================\n`);
}

main().catch((err) => {
  console.error(`Fatal error: ${err.message}`);
  process.exit(1);
});
