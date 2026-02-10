/**
 * Sensitivity Analysis for V12 Optimized Configuration
 * Uses FLEET OPTIMIZER for each scenario (like browser does)
 * Baseline: $1,999,718 (0 ran dry) with throttle optimization
 *
 * Run with: node sensitivity-v12-optimized.js
 */

const fs = require('fs');
const path = require('path');

// Results capture
let capturedOptimizerResult = null;
const self = {
  postMessage: (msg) => {
    if (msg.type === 'optimizeResult') {
      capturedOptimizerResult = msg.result;
    }
  }
};

// Load route data
console.log('Loading route data...');
const routeDataPath = path.join(__dirname, 'route-data.js');
let routeDataContent = fs.readFileSync(routeDataPath, 'utf-8');
routeDataContent = routeDataContent.replace(/^const /gm, 'var ');
eval(routeDataContent);

console.log('✓ Route data loaded');
console.log('');

// ============================================================================
// BASE CONFIGURATION
// ============================================================================
const BASE_CONFIG = {
  numSmallTugs: 6,
  numSmallTransportBarges: 8,
  numSmallStorageBarges: 12,
  numLargeTransport: 4,
  numLargeStorage: 0,
  TOTAL_LARGE_BARGES: 4,
  LARGE_BARGE_VOLUME: 300000,
  SMALL_BARGE_VOLUME: 80000,
  SMALL_BARGES_PER_TUG: 2,
  LARGE_BARGES_PER_TUG: 1,
  loadedSpeedSmall: 5,
  loadedSpeedLarge: 4,
  emptySpeed: 5,
  switchoutTime: 0.5,
  hookupTime: 0.33,
  pumpRate: 1000,
  DRILLING_START_HOUR: 7,
  DRILLING_END_HOUR: 17,
  sourceConfig: {
    source1: { flowRate: 800, hoursPerDay: 24 },
    source2: { flowRate: 200, hoursPerDay: 24 },
    source3: { flowRate: 400, hoursPerDay: 24 }
  },
  rigs: {
    Blue: { pilot: 7500, ream: 20000, swab: 17000, pull: 14000 },
    Green: { pilot: 7500, ream: 20000, swab: 17000, pull: 14000 },
    Red: { pilot: 7500, ream: 20000, swab: 17000, pull: 14000 }
  },
  COST: {
    tug: { dayRate: 4500, fuelIdleGPH: 10, fuelRunningGPH: 45 },
    barge: { smallDayRate: 250, largeDayRate: 750 },
    fuel: { pricePerGal: 3.00 },
    downtime: { hourlyRate: 6591 },
    mobilization: {
      tugMobCost: 9000,
      tugDemobCost: 9000,
      smallBargeMobCost: 500,
      smallBargeDemobCost: 500,
      largeBargeMobCost: 1500,
      largeBargeDemobCost: 1500
    },
    waterAcquisition: {
      source1: 0.02,
      source2: 0.03,
      source3: 0.03
    }
  },
  hddSchedule: [
    { rig: 'Blue', crossing: 'HDD17', start: '2026-05-14', rigUp: '2026-05-18', pilot: '2026-05-24', ream: '2026-05-27', swab: '2026-05-28', pull: '2026-05-29', rigDown: '2026-06-04', worksSundays: false },
    { rig: 'Green', crossing: 'HDD18', start: '2026-05-27', rigUp: '2026-05-31', pilot: '2026-06-11', ream: '2026-06-22', swab: '2026-06-23', pull: '2026-06-24', rigDown: '2026-06-30', worksSundays: false },
    { rig: 'Blue', crossing: 'HDD20', start: '2026-06-06', rigUp: '2026-06-09', pilot: '2026-06-15', ream: '2026-06-18', swab: '2026-06-19', pull: '2026-06-21', rigDown: '2026-06-26', worksSundays: false },
    { rig: 'Red', crossing: 'HDD23', start: '2026-06-21', rigUp: '2026-06-24', pilot: '2026-07-03', ream: '2026-07-10', swab: '2026-07-12', pull: '2026-07-13', rigDown: '2026-07-19', worksSundays: false },
    { rig: 'Blue', crossing: 'HDD19', start: '2026-06-28', rigUp: '2026-07-01', pilot: '2026-07-08', ream: '2026-07-13', swab: '2026-07-14', pull: '2026-07-15', rigDown: '2026-07-21', worksSundays: false },
    { rig: 'Green', crossing: 'HDD21', start: '2026-07-02', rigUp: '2026-07-05', pilot: '2026-07-19', ream: '2026-07-31', swab: '2026-08-02', pull: '2026-08-03', rigDown: '2026-08-09', worksSundays: false },
    { rig: 'Red', crossing: 'HDD25', start: '2026-07-21', rigUp: '2026-07-23', pilot: '2026-08-03', ream: '2026-08-13', swab: '2026-08-14', pull: '2026-08-16', rigDown: '2026-08-21', worksSundays: false },
    { rig: 'Blue', crossing: 'HDD22', start: '2026-07-23', rigUp: '2026-07-26', pilot: '2026-07-30', ream: '2026-08-02', swab: '2026-08-03', pull: '2026-08-04', rigDown: '2026-08-10', worksSundays: false },
    { rig: 'Blue', crossing: 'HDD24', start: '2026-08-12', rigUp: '2026-08-14', pilot: '2026-08-18', ream: '2026-08-20', swab: '2026-08-21', pull: '2026-08-23', rigDown: '2026-08-28', worksSundays: false },
    { rig: 'Red', crossing: 'HDD26', start: '2026-08-23', rigUp: '2026-08-26', pilot: '2026-08-31', ream: '2026-09-03', swab: '2026-09-04', pull: '2026-09-06', rigDown: '2026-09-11', worksSundays: false }
  ]
};

// Starting fleet for optimizer (browser uses analytical sizing)
const STARTING_FLEET = {
  numSmallTugs: 2,
  numSmallTransportBarges: 2,
  numSmallStorageBarges: 2,
  numLargeTransport: 2,
  numLargeStorage: 2
};

// ============================================================================
// SENSITIVITY TEST SCENARIOS
// ============================================================================

const SENSITIVITY_TESTS = {
  'Water Consumption': [
    { label: '-20%', mult: 0.80 },
    { label: '-10%', mult: 0.90 },
    { label: 'Baseline', mult: 1.00 },
    { label: '+10%', mult: 1.10 },
    { label: '+20%', mult: 1.20 },
    { label: '+30%', mult: 1.30 }
  ],
  'Downtime Cost': [
    { label: '$5,000/hr', value: 5000 },
    { label: '$6,000/hr', value: 6000 },
    { label: '$6,591/hr (base)', value: 6591 },
    { label: '$7,500/hr', value: 7500 },
    { label: '$8,500/hr', value: 8500 },
    { label: '$10,000/hr', value: 10000 }
  ],
  'Tug Day Rate': [
    { label: '$3,500', value: 3500 },
    { label: '$4,000', value: 4000 },
    { label: '$4,500 (base)', value: 4500 },
    { label: '$5,000', value: 5000 },
    { label: '$5,500', value: 5500 },
    { label: '$6,000', value: 6000 }
  ],
  'Large Barge Day Rate': [
    { label: '$600', value: 600 },
    { label: '$750 (base)', value: 750 },
    { label: '$900', value: 900 },
    { label: '$1,000', value: 1000 }
  ],
  'Small Barge Day Rate': [
    { label: '$200', value: 200 },
    { label: '$250 (base)', value: 250 },
    { label: '$300', value: 300 },
    { label: '$350', value: 350 }
  ],
  'Fuel Price': [
    { label: '$2.50/gal', value: 2.50 },
    { label: '$3.00/gal (base)', value: 3.00 },
    { label: '$3.50/gal', value: 3.50 },
    { label: '$4.00/gal', value: 4.00 },
    { label: '$5.00/gal', value: 5.00 }
  ]
};

// ============================================================================
// RUN OPTIMIZER WITH PARAMETER VARIATION
// ============================================================================

function runOptimizerWithVariation(testName, variation) {
  capturedOptimizerResult = null;

  // Create modified config
  const testConfig = JSON.parse(JSON.stringify(BASE_CONFIG));

  // Apply variation
  if (testName === 'Water Consumption') {
    Object.keys(testConfig.rigs).forEach(rig => {
      testConfig.rigs[rig].pilot = Math.round(7500 * variation.mult);
      testConfig.rigs[rig].ream = Math.round(20000 * variation.mult);
      testConfig.rigs[rig].swab = Math.round(17000 * variation.mult);
      testConfig.rigs[rig].pull = Math.round(14000 * variation.mult);
    });
  } else if (testName === 'Downtime Cost') {
    testConfig.COST.downtime.hourlyRate = variation.value;
  } else if (testName === 'Tug Day Rate') {
    testConfig.COST.tug.dayRate = variation.value;
  } else if (testName === 'Large Barge Day Rate') {
    testConfig.COST.barge.largeDayRate = variation.value;
  } else if (testName === 'Small Barge Day Rate') {
    testConfig.COST.barge.smallDayRate = variation.value;
  } else if (testName === 'Fuel Price') {
    testConfig.COST.fuel.pricePerGal = variation.value;
  }

  // Load and execute worker with modified config
  const workerPath = path.join(__dirname, 'simulation-worker.js');
  let workerContent = fs.readFileSync(workerPath, 'utf-8');
  workerContent = workerContent.replace(/importScripts\([^)]+\);?/g, '');
  workerContent = workerContent.replace(/^const /gm, 'var ');
  workerContent = workerContent.replace(/^let /gm, 'var ');

  // Suppress console output
  const originalLog = console.log;
  const originalWarn = console.warn;
  const originalError = console.error;
  console.log = () => {};
  console.warn = () => {};
  console.error = () => {};

  try {
    eval(workerContent);

    // Run fleet optimizer (like browser does)
    self.onmessage({
      data: {
        type: 'runLocalOptimize',
        fleetConfig: STARTING_FLEET,
        userConfig: testConfig
      }
    });

    console.log = originalLog;
    console.warn = originalWarn;
    console.error = originalError;

    if (capturedOptimizerResult && capturedOptimizerResult.optimal) {
      return {
        success: true,
        cost: capturedOptimizerResult.optimal.costs?.grandTotal,
        ranDry: capturedOptimizerResult.optimal.ranDryCount,
        fleet: capturedOptimizerResult.optimal.config,
        iterations: capturedOptimizerResult.totalTested
      };
    } else if (capturedOptimizerResult && capturedOptimizerResult.error) {
      return { success: false, error: capturedOptimizerResult.error };
    }

    return { success: false, error: 'No result captured' };

  } catch (error) {
    console.log = originalLog;
    console.warn = originalWarn;
    console.error = originalError;
    return { success: false, error: error.message };
  }
}

// ============================================================================
// MAIN ANALYSIS
// ============================================================================

console.log('='.repeat(80));
console.log('SENSITIVITY ANALYSIS - V12 Optimized (Using Fleet Optimizer)');
console.log('='.repeat(80));
console.log('Baseline: $1,999,718 (0 ran dry)');
console.log('Optimization: THROTTLE_WATER_LOW=10k + Fleet Optimizer');
console.log('Starting Fleet: 2T, 2ST, 2SS, 2LT, 2LS (optimizer adjusts from here)');
console.log('='.repeat(80));
console.log('');

// Test baseline
console.log('Testing baseline configuration with fleet optimizer...');
const baselineResult = runOptimizerWithVariation('Baseline', {});

if (!baselineResult.success) {
  console.error('ERROR: Baseline test failed:', baselineResult.error);
  process.exit(1);
}

const baselineCost = baselineResult.cost;
const baselineFleet = baselineResult.fleet;

console.log(`✓ Baseline: $${baselineCost.toLocaleString()} (${baselineResult.ranDry} ran dry)`);
console.log(`  Fleet: ${baselineFleet.numSmallTugs}T, ${baselineFleet.numSmallTransportBarges}ST, ${baselineFleet.numSmallStorageBarges}SS, ${baselineFleet.numLargeTransport}LT, ${baselineFleet.numLargeStorage}LS`);
console.log(`  Iterations tested: ${baselineResult.iterations}`);
console.log('');

const allResults = {
  baseline: {
    cost: baselineCost,
    ranDry: baselineResult.ranDry,
    fleet: baselineFleet
  },
  timestamp: new Date().toISOString(),
  tests: {}
};

let totalTests = 0;

// Run all sensitivity tests
Object.entries(SENSITIVITY_TESTS).forEach(([testName, variations]) => {
  console.log('-'.repeat(80));
  console.log(testName);
  console.log('-'.repeat(80));

  allResults.tests[testName] = [];

  variations.forEach((variation) => {
    totalTests++;
    process.stdout.write(`  [${totalTests}] ${variation.label.padEnd(25)}... `);

    const result = runOptimizerWithVariation(testName, variation);

    if (result.success && result.cost) {
      const diff = result.cost - baselineCost;
      const diffPct = (diff / baselineCost * 100);
      const ranDryFlag = result.ranDry > 0 ? ` ⚠️ ${result.ranDry} RAN DRY` : '';

      console.log(
        `$${result.cost.toLocaleString().padStart(12)} ` +
        `(${diffPct >= 0 ? '+' : ''}${diffPct.toFixed(2)}%)${ranDryFlag}`
      );

      allResults.tests[testName].push({
        label: variation.label,
        cost: result.cost,
        ranDry: result.ranDry,
        costDiff: diff,
        costDiffPct: diffPct,
        fleet: result.fleet
      });
    } else {
      console.log(`FAILED: ${result.error || 'Unknown error'}`);
      allResults.tests[testName].push({
        label: variation.label,
        error: result.error || 'Unknown error'
      });
    }
  });

  console.log('');
});

// ============================================================================
// SUMMARY
// ============================================================================

console.log('='.repeat(80));
console.log('SENSITIVITY SUMMARY');
console.log('='.repeat(80));
console.log('');

console.log('Cost Impact Ranking (max absolute change from baseline):');
console.log('-'.repeat(80));

const impacts = [];
Object.entries(allResults.tests).forEach(([testName, results]) => {
  const successful = results.filter(r => r.cost !== undefined && r.ranDry === 0);

  if (successful.length === 0) {
    impacts.push({
      name: testName,
      maxImpactPct: 'N/A (no zero-ranDry results)',
      range: 0
    });
    return;
  }

  const costs = successful.map(r => r.cost);
  const minCost = Math.min(...costs);
  const maxCost = Math.max(...costs);
  const range = maxCost - minCost;
  const rangePct = (range / baselineCost * 100);

  // Max absolute change from baseline
  const maxAbsChange = Math.max(
    Math.abs(minCost - baselineCost),
    Math.abs(maxCost - baselineCost)
  );
  const maxAbsChangePct = (maxAbsChange / baselineCost * 100);

  impacts.push({
    name: testName,
    maxImpactPct: maxAbsChangePct,
    range,
    rangePct,
    minCost,
    maxCost,
    successCount: successful.length,
    totalCount: results.length
  });
});

// Sort by impact
impacts.sort((a, b) => {
  if (typeof a.maxImpactPct === 'string') return 1;
  if (typeof b.maxImpactPct === 'string') return -1;
  return b.maxImpactPct - a.maxImpactPct;
});

impacts.forEach((impact, idx) => {
  if (typeof impact.maxImpactPct === 'string') {
    console.log(`  ${idx + 1}. ${impact.name.padEnd(30)}: ${impact.maxImpactPct}`);
  } else {
    console.log(
      `  ${idx + 1}. ${impact.name.padEnd(30)}: ` +
      `±${impact.maxImpactPct.toFixed(2)}% ` +
      `(range: $${impact.range.toLocaleString()}) ` +
      `[${impact.successCount}/${impact.totalCount} zero-ranDry]`
    );
  }
});

console.log('');
console.log('Key Insights:');
console.log('  - Higher % = greater financial risk/opportunity from parameter changes');
console.log('  - Range shows total cost variation across tested values');
console.log('  - Zero-ranDry count shows how many variations maintained safety');
console.log('');

// ============================================================================
// DETAILED BREAKDOWN
// ============================================================================

console.log('='.repeat(80));
console.log('DETAILED COST BREAKDOWN');
console.log('='.repeat(80));
console.log('');

Object.entries(allResults.tests).forEach(([testName, results]) => {
  console.log(`${testName}:`);

  const successful = results.filter(r => r.cost && r.ranDry === 0);
  const withRanDry = results.filter(r => r.cost && r.ranDry > 0);

  if (successful.length > 0) {
    console.log('  Zero Ran Dry:');
    successful.forEach(r => {
      console.log(`    ${r.label.padEnd(25)}: $${r.cost.toLocaleString().padStart(12)} (${r.costDiffPct >= 0 ? '+' : ''}${r.costDiffPct.toFixed(2)}%)`);
    });
  }

  if (withRanDry.length > 0) {
    console.log('  With Ran Dry Events:');
    withRanDry.forEach(r => {
      console.log(`    ${r.label.padEnd(25)}: $${r.cost.toLocaleString().padStart(12)} ⚠️ ${r.ranDry} ran dry`);
    });
  }

  const failed = results.filter(r => r.error);
  if (failed.length > 0) {
    console.log('  Failed:');
    failed.forEach(r => {
      console.log(`    ${r.label.padEnd(25)}: ${r.error}`);
    });
  }

  console.log('');
});

// ============================================================================
// SAVE RESULTS
// ============================================================================

const outputPath = path.join(__dirname, 'sensitivity-results-v12-optimized.json');
fs.writeFileSync(outputPath, JSON.stringify(allResults, null, 2));

console.log('='.repeat(80));
console.log(`Results saved to: ${path.basename(outputPath)}`);
console.log(`Total tests completed: ${totalTests + 1} (including baseline)`);
console.log('Analysis complete.');
console.log('='.repeat(80));
