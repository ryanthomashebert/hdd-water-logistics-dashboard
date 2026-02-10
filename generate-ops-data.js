// Node.js script to run simulation and extract operations data for a specific date
// Usage: node generate-ops-data.js [date]
// Example: node generate-ops-data.js 2026-07-06

const fs = require('fs');
const path = require('path');
const vm = require('vm');

// Target date from command line or default
const targetDate = process.argv[2] || '2026-07-06';
console.log(`\n=== Generating Operations Data for ${targetDate} ===\n`);

// Mock Web Worker environment
global.importScripts = function(...scripts) {
  scripts.forEach(script => {
    const scriptPath = path.join(__dirname, '..', '..', script);
    const code = fs.readFileSync(scriptPath, 'utf8');
    vm.runInThisContext(code, { filename: script });
  });
};

// Capture simulation result
let simulationResult = null;

global.postMessage = function(data) {
  if (data.type === 'singleResult') {
    simulationResult = data.result;
  } else if (data.type === 'error') {
    console.error('Simulation error:', data.message);
  } else if (data.type === 'progress') {
    // Suppress progress messages
  }
};

global.self = global;

// Load the simulation worker
console.log('Loading simulation worker...');
const workerPath = path.join(__dirname, '..', '..', 'simulation-worker.js');
const workerCode = fs.readFileSync(workerPath, 'utf8');
vm.runInThisContext(workerCode, { filename: 'simulation-worker.js' });

// Run simulation with optimized fleet config from user
console.log('Running simulation...');
const fleetConfig = {
  numSmallTugs: 2,
  numSmallTransportBarges: 2,
  numSmallStorageBarges: 9,
  numLargeTransport: 1,
  numLargeStorage: 2
};

const userConfig = {
  _optimizationMode: false  // We want full data including timeline
};

// Trigger simulation via the message handler (synchronous in Node)
global.self.onmessage({ data: { type: 'runSingle', fleetConfig, userConfig } });

// The simulation runs synchronously in Node
if (!simulationResult) {
  console.error('Simulation did not complete!');
  process.exit(1);
}

console.log(`\nSimulation complete! Success: ${simulationResult.success}`);
console.log(`Total demand: ${Math.round(simulationResult.totalDemand / 1000000)}M gallons`);
console.log(`Ran dry count: ${simulationResult.ranDryCount}`);

// Debug: Print timeline info
if (simulationResult.hddStorageTimeline) {
  console.log(`\nTimeline data available for ${simulationResult.hddStorageTimeline.length} HDDs`);
  simulationResult.hddStorageTimeline.forEach(t => {
    const first = t.hourlyData?.[0];
    const last = t.hourlyData?.[t.hourlyData.length-1];
    console.log(`  ${t.crossing}: ${t.hourlyData?.length || 0} records`);
    if (first) {
      console.log(`    First record keys: ${Object.keys(first).join(', ')}`);
      console.log(`    First record sample: time=${first.time}, hour=${first.hour}, storage=${first.storage}`);
    }
  });
}

// Extract data for target date
const targetDateData = extractDateData(simulationResult, targetDate);

// Write to JSON file
const outputPath = path.join(__dirname, 'ops-data.json');
fs.writeFileSync(outputPath, JSON.stringify(targetDateData, null, 2));
console.log(`\nOperations data written to: ${outputPath}`);

// Also output summary to console
console.log('\n=== Summary for', targetDate, '===');
console.log('Active HDDs:', targetDateData.activeHDDs.map(h => `${h.crossing} (${h.rig} - ${h.phase})`).join(', '));
console.log('Active Tugs:', targetDateData.tugs.length);
console.log('Total Deliveries:', targetDateData.deliveries.length);

function extractDateData(result, dateStr) {
  // Use global CONFIG since result may not include full config
  const hddSchedule = CONFIG.hddSchedule;
  const rigsConfig = CONFIG.rigs;

  const data = {
    date: dateStr,
    displayDate: formatDisplayDate(dateStr),
    projectDay: calculateProjectDay(dateStr, hddSchedule),
    activeHDDs: [],
    tugs: [],
    deliveries: [],
    alerts: [],
    recommendations: [],
    summary: {
      totalDeliveries: 0,
      totalVolume: 0,
      avgStorageLevel: 0
    }
  };

  // Find active HDDs on this date
  hddSchedule.forEach(hdd => {
    const phase = getPhaseForDate(hdd, dateStr);
    if (phase && phase !== 'complete' && phase !== 'pending') {
      // Get storage data from timeline
      const hddTimeline = result.hddStorageTimeline?.find(t => t.crossing === hdd.crossing);
      let storageLevel = 0;
      let storageCapacity = 300000;
      let supplyHours = 0;
      let consumptionRate = 0;

      if (hddTimeline?.hourlyData && hddTimeline.hourlyData.length > 0) {
        // Find data for this date (use noon as representative hour)
        // hourlyData has: date, hour, storageLevel, storageCapacity, stage
        let hourData = hddTimeline.hourlyData.find(h =>
          h.date === dateStr && h.hour === 12
        );

        // If exact match not found, try any hour on that date
        if (!hourData) {
          hourData = hddTimeline.hourlyData.find(h => h.date === dateStr);
        }

        if (hourData) {
          storageLevel = hourData.storageLevel || 0;
          storageCapacity = hourData.storageCapacity || 300000;
          consumptionRate = getConsumptionRate(hdd.rig, phase, rigsConfig);
          supplyHours = consumptionRate > 0 ? storageLevel / consumptionRate : 999;

          // Debug output
          console.log(`  ${hdd.crossing}: date=${hourData.date}, hour=${hourData.hour}, stage=${hourData.stage}, storage=${Math.round(storageLevel/1000)}k/${Math.round(storageCapacity/1000)}k`);
        } else {
          console.log(`  ${hdd.crossing}: No data for date ${dateStr}`);
        }
      } else {
        console.log(`  ${hdd.crossing}: No timeline data available`);
      }

      // Get phase day info
      const phaseInfo = getPhaseInfo(hdd, dateStr, phase);

      data.activeHDDs.push({
        crossing: hdd.crossing,
        rig: hdd.rig,
        phase: phase,
        phaseDay: phaseInfo.day,
        phaseTotalDays: phaseInfo.totalDays,
        consumptionRate: consumptionRate,
        storageLevel: Math.round(storageLevel),
        storageCapacity: storageCapacity,
        storagePercent: Math.round((storageLevel / storageCapacity) * 100),
        supplyHours: Math.round(supplyHours),
        status: getStorageStatus(storageLevel / storageCapacity, supplyHours, phase)
      });
    }
  });

  // Find tug activities for this date from assetLog
  const dayLogs = result.assetLog?.filter(log => log.date === dateStr) || [];
  const tugLogs = dayLogs.filter(log => log.assetType === 'Tug');

  // Get unique tugs and their latest status
  const tugStatuses = new Map();
  tugLogs.forEach(log => {
    const existing = tugStatuses.get(log.assetId);
    if (!existing || log.hour > existing.hour) {
      tugStatuses.set(log.assetId, log);
    }
  });

  tugStatuses.forEach((log, tugId) => {
    data.tugs.push({
      id: tugId,
      name: `T${tugId.replace('tug', '')}`,
      status: mapTugStatus(log.status),
      statusDetail: log.detail || '',
      location: log.location || 'Unknown',
      cargo: log.cargo || '',
      cargoLevel: log.cargoLevel || 0
    });
  });

  // Extract deliveries for this date
  const dayDeliveries = [];
  result.assetLog?.forEach(log => {
    if (log.date === dateStr && log.status === 'pumping' && log.assetType === 'Tug') {
      dayDeliveries.push({
        time: formatHour(log.hour),
        tug: `T${log.assetId.replace('tug', '')}`,
        destination: log.location,
        volume: log.pumpedAmount || 80000,
        status: 'complete'
      });
    }
  });

  // Sort by time
  dayDeliveries.sort((a, b) => a.time.localeCompare(b.time));
  data.deliveries = dayDeliveries;
  data.summary.totalDeliveries = dayDeliveries.length;
  data.summary.totalVolume = dayDeliveries.reduce((sum, d) => sum + d.volume, 0);

  // Calculate avg storage level
  if (data.activeHDDs.length > 0) {
    data.summary.avgStorageLevel = Math.round(
      data.activeHDDs.reduce((sum, h) => sum + h.storagePercent, 0) / data.activeHDDs.length
    );
  }

  // Generate alerts based on storage levels
  data.activeHDDs.forEach(hdd => {
    if (hdd.status === 'critical') {
      data.alerts.push({
        type: 'critical',
        title: `${hdd.crossing} Storage Critical`,
        detail: `Only ${hdd.supplyHours}h supply remaining. Immediate delivery required.`
      });
    } else if (hdd.status === 'warning') {
      data.alerts.push({
        type: 'warning',
        title: `${hdd.crossing} Storage Low`,
        detail: `${hdd.supplyHours}h supply remaining. Priority delivery recommended.`
      });
    } else if (hdd.status === 'standby') {
      data.alerts.push({
        type: 'info',
        title: `${hdd.crossing} Rig-Up in Progress`,
        detail: `${hdd.rig} Rig in ${hdd.phase} phase. Storage: ${hdd.storagePercent}% pre-staged.`
      });
    }
  });

  // Check for upcoming pilot starts
  hddSchedule.forEach(hdd => {
    const pilotDate = new Date(hdd.pilot);
    const targetDateObj = new Date(dateStr);
    const daysUntilPilot = Math.ceil((pilotDate - targetDateObj) / (1000 * 60 * 60 * 24));
    if (daysUntilPilot > 0 && daysUntilPilot <= 3) {
      data.alerts.push({
        type: 'info',
        title: `${hdd.crossing} Pilot Start in ${daysUntilPilot} Day${daysUntilPilot > 1 ? 's' : ''}`,
        detail: `${hdd.rig} Rig scheduled to begin pilot drilling. Verify storage pre-staged.`
      });
    }
  });

  // Generate recommendations
  data.activeHDDs.forEach(hdd => {
    if (hdd.status === 'healthy') {
      data.recommendations.push({
        type: 'info',
        title: `Maintain ${hdd.crossing} Steady State`,
        detail: `${hdd.supplyHours}h supply buffer is healthy. Continue scheduled delivery rotations.`
      });
    } else if (hdd.status === 'warning') {
      data.recommendations.push({
        type: 'warning',
        title: `Prioritize ${hdd.crossing} Deliveries`,
        detail: `Storage at ${hdd.storagePercent}%. Dispatch next available tug.`
      });
    }
  });

  return data;
}

function formatDisplayDate(dateStr) {
  // Parse as UTC to avoid timezone shifts
  const [year, month, day] = dateStr.split('-').map(Number);
  const d = new Date(Date.UTC(year, month - 1, day));
  const options = { weekday: 'long', month: 'long', day: 'numeric', year: 'numeric', timeZone: 'UTC' };
  return d.toLocaleDateString('en-US', options);
}

function calculateProjectDay(dateStr, hddSchedule) {
  if (!hddSchedule || hddSchedule.length === 0) return 1;
  const projectStart = new Date(hddSchedule[0].start);
  const target = new Date(dateStr);
  return Math.ceil((target - projectStart) / (1000 * 60 * 60 * 24)) + 1;
}

function getPhaseForDate(hdd, dateStr) {
  const date = new Date(dateStr);
  const start = new Date(hdd.start);
  const rigUp = new Date(hdd.rigUp);
  const pilot = new Date(hdd.pilot);
  const ream = new Date(hdd.ream);
  const swab = new Date(hdd.swab);
  const pull = new Date(hdd.pull);
  const rigDown = new Date(hdd.rigDown);

  if (date < start) return 'pending';
  if (date >= start && date < rigUp) return 'mobilization';
  if (date >= rigUp && date < pilot) return 'rigUp';
  if (date >= pilot && date < ream) return 'pilot';
  if (date >= ream && date < swab) return 'ream';
  if (date >= swab && date < pull) return 'swab';
  if (date >= pull && date < rigDown) return 'pull';
  if (date >= rigDown) {
    // Check if still within a day after rigDown
    const dayAfterRigDown = new Date(rigDown);
    dayAfterRigDown.setDate(dayAfterRigDown.getDate() + 3);
    if (date <= dayAfterRigDown) return 'rigDown';
    return 'complete';
  }
  return null;
}

function getPhaseInfo(hdd, dateStr, phase) {
  const date = new Date(dateStr);
  let phaseStart, phaseEnd;

  switch(phase) {
    case 'pilot':
      phaseStart = new Date(hdd.pilot);
      phaseEnd = new Date(hdd.ream);
      break;
    case 'ream':
      phaseStart = new Date(hdd.ream);
      phaseEnd = new Date(hdd.swab);
      break;
    case 'swab':
      phaseStart = new Date(hdd.swab);
      phaseEnd = new Date(hdd.pull);
      break;
    case 'pull':
      phaseStart = new Date(hdd.pull);
      phaseEnd = new Date(hdd.rigDown);
      break;
    case 'rigUp':
      phaseStart = new Date(hdd.rigUp);
      phaseEnd = new Date(hdd.pilot);
      break;
    default:
      return { day: 1, totalDays: 1 };
  }

  const day = Math.ceil((date - phaseStart) / (1000 * 60 * 60 * 24)) + 1;
  const totalDays = Math.ceil((phaseEnd - phaseStart) / (1000 * 60 * 60 * 24));

  return { day: Math.max(1, day), totalDays: Math.max(1, totalDays) };
}

function getConsumptionRate(rig, phase, rigsConfig) {
  const rigConfig = rigsConfig[rig];
  if (!rigConfig) return 0;
  return rigConfig[phase] || 0;
}

function dateStrToSimTime(dateStr, hddSchedule) {
  if (!hddSchedule || hddSchedule.length === 0) return 0;
  const projectStart = new Date(hddSchedule[0].start);
  const target = new Date(dateStr);
  return Math.floor((target - projectStart) / (1000 * 60 * 60));
}

function getStorageStatus(percent, supplyHours, phase) {
  // Non-drilling phases (rigUp, rigDown, mobilization) don't need water alerts
  if (['rigUp', 'rigDown', 'mobilization', 'pending', 'complete'].includes(phase)) {
    return 'standby';  // Not yet consuming water
  }
  if (percent < 0.2 || supplyHours < 8) return 'critical';
  if (percent < 0.4 || supplyHours < 16) return 'warning';
  return 'healthy';
}

function mapTugStatus(status) {
  const statusMap = {
    'idle': 'idle',
    'traveling-loaded': 'en-route',
    'traveling-empty': 'returning',
    'filling': 'filling',
    'pumping': 'pumping',
    'waiting': 'waiting',
    'throttled-standby': 'on-site'
  };
  return statusMap[status] || status;
}

function formatHour(hour) {
  const h = Math.floor(hour);
  const m = Math.round((hour - h) * 60);
  const ampm = h >= 12 ? 'PM' : 'AM';
  const displayHour = h % 12 || 12;
  return `${displayHour}:${m.toString().padStart(2, '0')} ${ampm}`;
}
