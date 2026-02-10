// Web Worker for HDD Water Logistics Simulation v11
// Ported from full-simulation-v10.js for browser execution
// v11.1: Same-rig storage barge transfer optimization (2026-01-25)

// Import route data
importScripts('route-data.js');

// ============================================================================
// LOGGING CONTROL (Memory Optimization)
// ============================================================================
// During optimization/iteration, suppress verbose logging to prevent memory exhaustion
let SUPPRESS_VERBOSE_LOGGING = false;

// Wrapper for verbose logging - only logs when not suppressed
function verboseLog(...args) {
  if (!SUPPRESS_VERBOSE_LOGGING) {
    console.log(...args);
  }
}

// Critical logs always print (errors, warnings, key results)
function criticalLog(...args) {
  console.log(...args);
}

// ============================================================================
// DEFAULT CONFIGURATION CONSTANTS
// ============================================================================
let CONFIG = {
  // Fleet limits
  TOTAL_LARGE_BARGES: 4,

  // Equipment volumes - unified barge model (all large barges 300k)
  LARGE_BARGE_VOLUME: 300000,
  SMALL_BARGE_VOLUME: 80000,

  // Tug capacity limits
  SMALL_BARGES_PER_TUG: 2,
  LARGE_BARGES_PER_TUG: 1,

  // Day rates
  LARGE_BARGE_DAY_RATE: 750,
  SMALL_BARGE_DAY_RATE: 250,
  TUG_DAY_RATE: 4500,

  // Speeds (knots)
  loadedSpeedSmall: 5,
  loadedSpeedLarge: 5,
  emptySpeed: 5,

  // Operations timing (hours)
  switchoutTime: 0.5,
  hookupTime: 0.33,
  pumpRate: 1000, // GPM

  // Drilling hours
  DRILLING_START_HOUR: 7,
  DRILLING_END_HOUR: 17,

  // Storage requirements
  MIN_STORAGE_CAPACITY_PER_RIG: 150000,  // Minimum 150k gallons storage at each rig

  // Mobilization parameters
  TUG_DEMOB_THRESHOLD_DAYS: 1.5,
  BARGE_MOB_DEMOB_COST_DAYS: 2,

  // Cost structure
  COST: {
    tug: { dayRate: 4500, fuelIdleGPH: 10, fuelRunningGPH: 45 },
    barge: { smallDayRate: 250, largeDayRate: 750 },
    fuel: { pricePerGal: 3.00 },
    downtime: { hourlyRate: 6591 },
    mobilization: {
      tugMobCost: 4500,
      tugDemobCost: 4500,
      smallBargeMobCost: 500,
      smallBargeDemobCost: 500,
      largeBargeMobCost: 1500,
      largeBargeDemobCost: 1500
    },
    waterAcquisition: {
      source1: 0.02,  // $/gal
      source2: 0.03,  // $/gal
      source3: 0.03   // $/gal
    }
  },

  // Source configuration
  sourceConfig: {
    source1: { flowRate: 800, hoursPerDay: 24 },
    source2: { flowRate: 200, hoursPerDay: 24 },
    source3: { flowRate: 400, hoursPerDay: 24 }
  },

  // Rig consumption rates (gal/hr)
  rigs: {
    Blue: { pilot: 7500, ream: 20000, swab: 17000, pull: 14000 },
    Green: { pilot: 7500, ream: 20000, swab: 17000, pull: 14000 },
    Red: { pilot: 7500, ream: 20000, swab: 17000, pull: 14000 }
  },

  // HDD Schedule
  hddSchedule: [
    { rig: 'Blue', crossing: 'HDD17', start: '2026-05-27', rigUp: '2026-06-01', pilot: '2026-06-04', ream: '2026-06-05', swab: '2026-06-05', pull: '2026-06-05', rigDown: '2026-06-05', worksSundays: false },
    { rig: 'Green', crossing: 'HDD18', start: '2026-05-27', rigUp: '2026-06-01', pilot: '2026-06-12', ream: '2026-06-23', swab: '2026-06-24', pull: '2026-06-25', rigDown: '2026-07-01', worksSundays: false },
    { rig: 'Blue', crossing: 'HDD20', start: '2026-06-06', rigUp: '2026-06-10', pilot: '2026-06-16', ream: '2026-06-19', swab: '2026-06-20', pull: '2026-06-22', rigDown: '2026-06-27', worksSundays: false },
    { rig: 'Red', crossing: 'HDD23', start: '2026-06-21', rigUp: '2026-06-25', pilot: '2026-07-04', ream: '2026-07-11', swab: '2026-07-13', pull: '2026-07-14', rigDown: '2026-07-20', worksSundays: false },
    { rig: 'Blue', crossing: 'HDD19', start: '2026-06-28', rigUp: '2026-07-02', pilot: '2026-07-09', ream: '2026-07-14', swab: '2026-07-15', pull: '2026-07-16', rigDown: '2026-07-22', worksSundays: false },
    { rig: 'Green', crossing: 'HDD21', start: '2026-07-02', rigUp: '2026-07-06', pilot: '2026-07-20', ream: '2026-08-01', swab: '2026-08-03', pull: '2026-08-04', rigDown: '2026-08-10', worksSundays: false },
    { rig: 'Red', crossing: 'HDD25', start: '2026-07-21', rigUp: '2026-07-24', pilot: '2026-08-04', ream: '2026-08-14', swab: '2026-08-15', pull: '2026-08-17', rigDown: '2026-08-22', worksSundays: false },
    { rig: 'Blue', crossing: 'HDD22', start: '2026-07-23', rigUp: '2026-07-27', pilot: '2026-07-31', ream: '2026-08-03', swab: '2026-08-04', pull: '2026-08-05', rigDown: '2026-08-11', worksSundays: false },
    { rig: 'Blue', crossing: 'HDD24', start: '2026-08-12', rigUp: '2026-08-15', pilot: '2026-08-19', ream: '2026-08-21', swab: '2026-08-22', pull: '2026-08-24', rigDown: '2026-08-29', worksSundays: false },
    { rig: 'Red', crossing: 'HDD26', start: '2026-08-23', rigUp: '2026-08-27', pilot: '2026-09-01', ream: '2026-09-04', swab: '2026-09-05', pull: '2026-09-07', rigDown: '2026-09-12', worksSundays: false }
  ]
};

// ============================================================================
// HELPER: Get rig names dynamically from config
// ============================================================================
const getRigNames = (rigsConfig) => {
  if (!rigsConfig || typeof rigsConfig !== 'object') {
    console.warn('[ROBUSTNESS] No rigs config provided, returning empty array');
    return [];
  }
  return Object.keys(rigsConfig);
};

// ============================================================================
// HELPER: Validate critical config values with fallbacks
// ============================================================================
const validateConfig = (config, context = 'simulation') => {
  const warnings = [];
  const validated = { ...config };

  // Validate drilling hours
  if (typeof validated.DRILLING_START_HOUR !== 'number' || validated.DRILLING_START_HOUR < 0 || validated.DRILLING_START_HOUR > 23) {
    warnings.push(`Invalid DRILLING_START_HOUR (${validated.DRILLING_START_HOUR}), using default 7`);
    validated.DRILLING_START_HOUR = 7;
  }
  if (typeof validated.DRILLING_END_HOUR !== 'number' || validated.DRILLING_END_HOUR < 1 || validated.DRILLING_END_HOUR > 24) {
    warnings.push(`Invalid DRILLING_END_HOUR (${validated.DRILLING_END_HOUR}), using default 17`);
    validated.DRILLING_END_HOUR = 17;
  }
  if (validated.DRILLING_START_HOUR >= validated.DRILLING_END_HOUR) {
    warnings.push(`DRILLING_START_HOUR (${validated.DRILLING_START_HOUR}) >= DRILLING_END_HOUR (${validated.DRILLING_END_HOUR}), swapping`);
    [validated.DRILLING_START_HOUR, validated.DRILLING_END_HOUR] = [validated.DRILLING_END_HOUR, validated.DRILLING_START_HOUR];
  }

  // Validate pump rate
  if (typeof validated.pumpRate !== 'number' || validated.pumpRate <= 0) {
    warnings.push(`Invalid pumpRate (${validated.pumpRate}), using default 1000 GPM`);
    validated.pumpRate = 1000;
  }

  // Validate barge volumes
  if (typeof validated.SMALL_BARGE_VOLUME !== 'number' || validated.SMALL_BARGE_VOLUME <= 0) {
    warnings.push(`Invalid SMALL_BARGE_VOLUME (${validated.SMALL_BARGE_VOLUME}), using default 80000`);
    validated.SMALL_BARGE_VOLUME = 80000;
  }
  if (typeof validated.LARGE_BARGE_VOLUME !== 'number' || validated.LARGE_BARGE_VOLUME <= 0) {
    warnings.push(`Invalid LARGE_BARGE_VOLUME (${validated.LARGE_BARGE_VOLUME}), using default 300000`);
    validated.LARGE_BARGE_VOLUME = 300000;
  }

  // Validate speeds
  if (typeof validated.loadedSpeedSmall !== 'number' || validated.loadedSpeedSmall <= 0) {
    warnings.push(`Invalid loadedSpeedSmall (${validated.loadedSpeedSmall}), using default 5 knots`);
    validated.loadedSpeedSmall = 5;
  }
  if (typeof validated.loadedSpeedLarge !== 'number' || validated.loadedSpeedLarge <= 0) {
    warnings.push(`Invalid loadedSpeedLarge (${validated.loadedSpeedLarge}), using default 5 knots`);
    validated.loadedSpeedLarge = 5;
  }
  if (typeof validated.emptySpeed !== 'number' || validated.emptySpeed <= 0) {
    warnings.push(`Invalid emptySpeed (${validated.emptySpeed}), using default 5 knots`);
    validated.emptySpeed = 5;
  }

  // Validate source config flow rates
  if (validated.sourceConfig) {
    Object.keys(validated.sourceConfig).forEach(sourceId => {
      const src = validated.sourceConfig[sourceId];
      if (!src || typeof src.flowRate !== 'number' || src.flowRate <= 0) {
        warnings.push(`Invalid flowRate for ${sourceId} (${src?.flowRate}), using default 200 GPM`);
        validated.sourceConfig[sourceId] = { ...src, flowRate: 200 };
      }
      if (!src || typeof src.hoursPerDay !== 'number' || src.hoursPerDay <= 0 || src.hoursPerDay > 24) {
        warnings.push(`Invalid hoursPerDay for ${sourceId} (${src?.hoursPerDay}), using default 24`);
        validated.sourceConfig[sourceId] = { ...validated.sourceConfig[sourceId], hoursPerDay: 24 };
      }
    });
  }

  // Validate rig consumption rates
  if (validated.rigs) {
    const validStages = ['pilot', 'ream', 'swab', 'pull'];
    Object.keys(validated.rigs).forEach(rigName => {
      const rig = validated.rigs[rigName];
      if (!rig) {
        warnings.push(`Rig ${rigName} has no config, creating default`);
        validated.rigs[rigName] = { pilot: 7500, ream: 20000, swab: 17000, pull: 14000 };
      } else {
        validStages.forEach(stage => {
          if (typeof rig[stage] !== 'number' || rig[stage] < 0) {
            warnings.push(`Invalid ${stage} rate for rig ${rigName} (${rig[stage]}), using default`);
            const defaults = { pilot: 7500, ream: 20000, swab: 17000, pull: 14000 };
            validated.rigs[rigName][stage] = defaults[stage];
          }
        });
      }
    });
  }

  // Log warnings if any
  if (warnings.length > 0) {
    console.warn(`[CONFIG VALIDATION - ${context}] ${warnings.length} issue(s) found:`);
    warnings.forEach(w => console.warn(`  - ${w}`));
  }

  return validated;
};

// ============================================================================
// DISTANCE CALCULATIONS
// ============================================================================
const toRadians = (degrees) => degrees * Math.PI / 180;

const haversineNM = (lat1, lon1, lat2, lon2) => {
  const R = 3440.065;
  const dLat = toRadians(lat2 - lat1);
  const dLon = toRadians(lon2 - lon1);
  const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
            Math.cos(toRadians(lat1)) * Math.cos(toRadians(lat2)) *
            Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
};

const calculateRouteDistance = (route) => {
  if (!route || route.length < 2) return 0;
  let totalDistance = 0;
  for (let i = 0; i < route.length - 1; i++) {
    const [lon1, lat1] = route[i];
    const [lon2, lat2] = route[i + 1];
    totalDistance += haversineNM(lat1, lon1, lat2, lon2);
  }
  return totalDistance;
};

// Build distance matrices
// sourceFlowRates: { source1: 800, source2: 200, source3: 400 } in GPM
function buildDistanceMatrices(hddSchedule, sourceFlowRates = { source1: 800, source2: 200, source3: 400 }) {
  const distanceMatrix = {};
  const hddToHddMatrix = {};
  const allHDDs = new Set();
  const bestSourceForHDD = {};

  hddSchedule.forEach(hdd => allHDDs.add(hdd.crossing));

  Object.entries(MULTI_SOURCE_ROUTES).forEach(([sourceId, sourceData]) => {
    distanceMatrix[sourceId] = {};
    if (sourceData.routes) {
      Object.entries(sourceData.routes).forEach(([hddCrossing, route]) => {
        distanceMatrix[sourceId][hddCrossing] = calculateRouteDistance(route);
      });
    }
  });

  Array.from(allHDDs).forEach(hdd1 => {
    hddToHddMatrix[hdd1] = {};
    Array.from(allHDDs).forEach(hdd2 => {
      if (hdd1 !== hdd2) {
        hddToHddMatrix[hdd1][hdd2] = CENTERLINE_HDD_DISTANCES[hdd1]?.[hdd2] || Infinity;
      }
    });
  });

  // Transport configurations to evaluate
  const transportConfigs = [
    { name: '1-small', volume: 80000 },
    { name: '2-small', volume: 160000 },
    { name: '1-large', volume: 300000 }
  ];
  const LOADED_SPEED = 5; // knots
  const availableSources = Object.keys(distanceMatrix);

  console.log('[DISTANCE MATRIX] Source-to-HDD analysis BY TRANSPORT CONFIG:');
  console.log('  (Best source can change based on barge size - larger barges favor faster fill rates)');

  Array.from(allHDDs).sort().forEach(hdd => {
    const bestByConfig = {};
    const analysisDetails = {};

    transportConfigs.forEach(cfg => {
      let bestSource = availableSources[0] || 'source1';
      let bestCycleTime = Infinity;
      let bestDist = Infinity;

      availableSources.forEach(srcId => {
        const dist = distanceMatrix[srcId]?.[hdd] || Infinity;
        const flowRate = sourceFlowRates[srcId] || 400;
        const fillTimeHrs = cfg.volume / (flowRate * 60);
        const travelTimeHrs = (2 * dist) / LOADED_SPEED;
        const cycleTimeHrs = fillTimeHrs + travelTimeHrs;

        if (cycleTimeHrs < bestCycleTime) {
          bestCycleTime = cycleTimeHrs;
          bestSource = srcId;
          bestDist = dist;
        }
      });

      bestByConfig[cfg.name] = {
        source: bestSource,
        distance: bestDist,
        cycleTime: bestCycleTime
      };
    });

    // Check if best source varies by config
    const sources = Object.values(bestByConfig).map(b => b.source);
    const allSame = sources.every(s => s === sources[0]);
    const configNote = allSame ? '' : ' *** VARIES BY CONFIG ***';

    // Store all config-specific best sources
    bestSourceForHDD[hdd] = {
      // Default (for backward compatibility) - use 2-small as baseline
      source: bestByConfig['2-small'].source,
      distance: bestByConfig['2-small'].distance,
      cycleTime: bestByConfig['2-small'].cycleTime,
      // Per-config best sources (NEW)
      byConfig: bestByConfig,
      variesByConfig: !allSame
    };

    // Log with config breakdown
    const configStrs = transportConfigs.map(cfg => {
      const b = bestByConfig[cfg.name];
      return `${cfg.name}→${b.source.replace('source','src')}`;
    }).join(', ');
    console.log(`  ${hdd}: ${configStrs}${configNote}`);
  });

  return { distanceMatrix, hddToHddMatrix, bestSourceForHDD };
}

// ============================================================================
// DEMAND ANALYSIS
// ============================================================================
function analyzeDailyDemand(hddSchedule, rigs, totalDays, minDate) {
  const DRILLING_START_HOUR = CONFIG.DRILLING_START_HOUR;
  const DRILLING_END_HOUR = CONFIG.DRILLING_END_HOUR;
  const dailyDemand = [];

  for (let d = 0; d < totalDays; d++) {
    const date = new Date(minDate);
    date.setDate(date.getDate() + d);
    const dateStr = date.toISOString().split('T')[0];
    const dayOfWeek = date.getDay();

    let totalDemand = 0;
    let activeRigs = 0;
    const rigActivity = {};

    getRigNames(rigs).forEach(rigName => {
      const rigConfig = rigs[rigName];
      if (!rigConfig) return; // Skip if rig config missing
      const rigSchedule = hddSchedule.filter(h => h.rig === rigName);

      let hourlyRate = 0;
      let stage = 'Idle';
      let hddCrossing = null;

      for (const hdd of rigSchedule) {
        if (dateStr >= hdd.start && dateStr <= hdd.rigDown) {
          if (dateStr <= hdd.rigUp) stage = 'RigUp';
          else if (dateStr <= hdd.pilot) { stage = 'Pilot'; hourlyRate = rigConfig.pilot; }
          else if (dateStr <= hdd.ream) { stage = 'Ream'; hourlyRate = rigConfig.ream; }
          else if (dateStr <= hdd.swab) { stage = 'Swab'; hourlyRate = rigConfig.swab; }
          else if (dateStr <= hdd.pull) { stage = 'Pull'; hourlyRate = rigConfig.pull; }
          else stage = 'RigDown';

          hddCrossing = hdd.crossing;
          if (dayOfWeek === 0 && !hdd.worksSundays) hourlyRate = 0;
          break;
        }
      }

      const drillingHours = DRILLING_END_HOUR - DRILLING_START_HOUR;
      const dailyUsage = hourlyRate * drillingHours;

      if (dailyUsage > 0) {
        activeRigs++;
        totalDemand += dailyUsage;
      }

      rigActivity[rigName] = {
        stage,
        hourlyRate,
        dailyUsage,
        hddCrossing,
        isActive: hourlyRate > 0
      };
    });

    dailyDemand.push({
      dayIndex: d,
      date: dateStr,
      totalDemand,
      activeRigs,
      rigActivity
    });
  }

  return dailyDemand;
}

// ============================================================================
// FLEET REQUIREMENTS
// ============================================================================
function calculateDailyFleetRequirements(dailyDemand, config) {
  const { numSmallTugs, numSmallStorageBarges, numLargeTransport, numLargeStorage } = config;
  const SMALL_BARGE_VOLUME = CONFIG.SMALL_BARGE_VOLUME;
  const SMALL_BARGES_PER_TUG = CONFIG.SMALL_BARGES_PER_TUG;
  const LARGE_BARGE_VOLUME = CONFIG.LARGE_BARGE_VOLUME;

  const fleetRequirements = [];

  dailyDemand.forEach(day => {
    let tugsNeeded = 0;
    let storageBargesNeeded = 0;

    if (day.totalDemand > 0) {
      const tripsPerTugPerDay = 24 / 8;
      const volumePerSmallTug = SMALL_BARGES_PER_TUG * SMALL_BARGE_VOLUME;
      const volumeNeeded = day.totalDemand * 1.2;
      const smallTugsNeeded = Math.ceil(volumeNeeded / (volumePerSmallTug * tripsPerTugPerDay));
      tugsNeeded = Math.min(numSmallTugs + numLargeTransport, smallTugsNeeded);
      storageBargesNeeded = day.activeRigs * Math.floor(numSmallStorageBarges / 3);
    }

    fleetRequirements.push({
      ...day,
      tugsNeeded,
      storageBargesNeeded,
      transportBargesNeeded: tugsNeeded * SMALL_BARGES_PER_TUG,
      largeStorageNeeded: Math.min(numLargeStorage, day.activeRigs)
    });
  });

  return fleetRequirements;
}

// ============================================================================
// BUILD MOBILIZATION SCHEDULE FROM ACTUAL SIMULATION
// ============================================================================
function buildMobilizationScheduleFromSimulation(state, fleetConfig, totalDays, minDate, dailyResults, assetLog) {
  const { numSmallStorageBarges, numLargeStorage } = fleetConfig;
  const COST = CONFIG.COST;

  const schedule = {
    tugs: [],
    barges: []
  };

  let mobDemobCosts = { tugs: 0, smallBarges: 0, largeBarges: 0 };

  // Get first and last demand day
  const dailyResultsArray = Object.values(dailyResults);
  const firstDemandDay = dailyResultsArray.findIndex(d => d.demand > 0);
  const lastDemandDay = dailyResultsArray.length - 1 - [...dailyResultsArray].reverse().findIndex(d => d.demand > 0);

  if (firstDemandDay === -1) {
    return {
      tugs: [],
      barges: [],
      summary: { totalTugDays: 0, totalSmallTransportBargeDays: 0, totalLargeTransportBargeDays: 0, totalSmallStorageBargeDays: 0, totalLargeStorageBargeDays: 0 },
      mobDemobCosts: { tugs: 0, smallBarges: 0, largeBarges: 0, total: 0 }
    };
  }

  const getDateStrFromDay = (dayNum) => {
    const date = new Date(minDate);
    date.setDate(date.getDate() + dayNum);
    return date.toISOString().split('T')[0];
  };

  // Track tugs that were actually used
  state.tugs.forEach(tug => {
    if (!tug.wasEverUsed) return; // Skip tugs that were never used

    // For simplicity, assume tugs are on-site for entire demand period
    // (More sophisticated logic would track first/last dispatch per tug)
    const mobDay = firstDemandDay;
    const demobDay = lastDemandDay;
    const mobDate = getDateStrFromDay(mobDay);
    const demobDate = getDateStrFromDay(demobDay);
    const daysOnSite = demobDay - mobDay + 1;

    mobDemobCosts.tugs += COST.mobilization.tugMobCost;
    mobDemobCosts.tugs += COST.mobilization.tugDemobCost;

    schedule.tugs.push({
      id: `Tug-${tug.id}`,
      assetType: 'tug',
      mobDay, demobDay, mobDate, demobDate, daysOnSite,
      periodNumber: 1,
      totalPeriods: 1
    });
  });

  // Track transport barges (based on individual usage from asset log)
  state.barges.forEach(barge => {
    // Find when this barge was first and last used by checking asset log
    const bargeEvents = assetLog.filter(entry => {
      const bargeSize = barge.volume >= CONFIG.LARGE_BARGE_VOLUME ? 'large' : 'small';
      return entry.assetType === `Barge (${bargeSize})` &&
             entry.assetId === barge.id &&
             ['en-route-loaded', 'at-rig', 'arrived-at-rig'].includes(entry.status);
    });

    // If barge was never used, skip it
    if (bargeEvents.length === 0) return;

    // Find first and last use
    const firstUse = bargeEvents[0];
    const lastUse = bargeEvents[bargeEvents.length - 1];

    const bargeSize = barge.volume >= CONFIG.LARGE_BARGE_VOLUME ? 'large' : 'small';

    // Parse dates from timestamps
    const firstDate = new Date(firstUse.date);
    const lastDate = new Date(lastUse.date);
    const minDateObj = new Date(minDate);

    const mobDay = Math.floor((firstDate - minDateObj) / (1000 * 60 * 60 * 24));
    const demobDay = Math.floor((lastDate - minDateObj) / (1000 * 60 * 60 * 24));
    const mobDate = getDateStrFromDay(mobDay);
    const demobDate = getDateStrFromDay(demobDay);
    const daysOnSite = demobDay - mobDay + 1;

    mobDemobCosts[bargeSize === 'large' ? 'largeBarges' : 'smallBarges'] +=
      bargeSize === 'large' ? COST.mobilization.largeBargeMobCost : COST.mobilization.smallBargeMobCost;
    mobDemobCosts[bargeSize === 'large' ? 'largeBarges' : 'smallBarges'] +=
      bargeSize === 'large' ? COST.mobilization.largeBargeDemobCost : COST.mobilization.smallBargeDemobCost;

    schedule.barges.push({
      id: `B-${barge.id}`,
      assetType: 'barge',
      bargeSize,
      usage: 'transport',
      mobDay, demobDay, mobDate, demobDate, daysOnSite
    });
  });

  // Track storage barges (per rig) - dynamically get rig names from results
  const firstResult = dailyResultsArray.find(d => d.rigDemand);
  const rigNamesFromResults = firstResult ? Object.keys(firstResult.rigDemand) : [];
  const numRigs = rigNamesFromResults.length || 1; // Avoid division by zero
  const storagePerRig = Math.floor(numSmallStorageBarges / numRigs);
  let storageBargeId = 1;

  rigNamesFromResults.forEach(rigName => {
    let firstActive = -1;
    let lastActive = -1;

    for (let d = 0; d < dailyResultsArray.length; d++) {
      const rigDemand = dailyResultsArray[d].rigDemand?.[rigName] || 0;
      if (rigDemand > 0) {
        if (firstActive === -1) firstActive = d;
        lastActive = d;
      }
    }

    if (firstActive >= 0 && storagePerRig > 0) {
      const mobDay = firstActive;
      const demobDay = lastActive;
      const mobDate = getDateStrFromDay(mobDay);
      const demobDate = getDateStrFromDay(demobDay);
      const daysOnSite = demobDay - mobDay + 1;

      for (let b = 0; b < storagePerRig; b++) {
        mobDemobCosts.smallBarges += COST.mobilization.smallBargeMobCost;
        mobDemobCosts.smallBarges += COST.mobilization.smallBargeDemobCost;

        schedule.barges.push({
          id: `SB-${storageBargeId++}`,
          assetType: 'barge',
          bargeSize: 'small',
          usage: 'storage',
          assignedRig: rigName,
          mobDay, demobDay, mobDate, demobDate, daysOnSite
        });
      }
    }
  });

  // Track large storage barges
  if (numLargeStorage > 0) {
    const mobDay = firstDemandDay;
    const demobDay = lastDemandDay;
    const mobDate = getDateStrFromDay(mobDay);
    const demobDate = getDateStrFromDay(demobDay);
    const daysOnSite = demobDay - mobDay + 1;

    for (let lsId = 1; lsId <= numLargeStorage; lsId++) {
      mobDemobCosts.largeBarges += COST.mobilization.largeBargeMobCost;
      mobDemobCosts.largeBarges += COST.mobilization.largeBargeDemobCost;

      schedule.barges.push({
        id: `LSB-${lsId}`,
        assetType: 'barge',
        bargeSize: 'large',
        usage: 'storage',
        assignedRig: 'Dynamic',
        mobDay, demobDay, mobDate, demobDate, daysOnSite
      });
    }
  }

  // Calculate summary
  const totalTugDays = schedule.tugs.reduce((sum, t) => sum + t.daysOnSite, 0);
  const totalSmallTransportBargeDays = schedule.barges.filter(b => b.bargeSize === 'small' && b.usage === 'transport').reduce((sum, b) => sum + b.daysOnSite, 0);
  const totalLargeTransportBargeDays = schedule.barges.filter(b => b.bargeSize === 'large' && b.usage === 'transport').reduce((sum, b) => sum + b.daysOnSite, 0);
  const totalSmallStorageBargeDays = schedule.barges.filter(b => b.bargeSize === 'small' && b.usage === 'storage').reduce((sum, b) => sum + b.daysOnSite, 0);
  const totalLargeStorageBargeDays = schedule.barges.filter(b => b.bargeSize === 'large' && b.usage === 'storage').reduce((sum, b) => sum + b.daysOnSite, 0);

  mobDemobCosts.total = mobDemobCosts.tugs + mobDemobCosts.smallBarges + mobDemobCosts.largeBarges;

  return {
    tugs: schedule.tugs,
    barges: schedule.barges,
    summary: {
      totalTugDays,
      totalSmallTransportBargeDays,
      totalLargeTransportBargeDays,
      totalSmallStorageBargeDays,
      totalLargeStorageBargeDays
    },
    mobDemobCosts
  };
}

// ============================================================================
// ANALYTICAL FLEET SIZING (PRE-SIMULATION)
// ============================================================================
// This function uses deterministic demand knowledge to calculate optimal fleet
// composition and mob/demob timing BEFORE running the simulation.
// ============================================================================

function calculateAnalyticalFleetSchedule(userConfig = {}) {
  const rawConfig = { ...CONFIG, ...userConfig };
  const config = validateConfig(rawConfig, 'analyticalFleetSchedule');
  const hddSchedule = config.hddSchedule || CONFIG.hddSchedule;
  const rigs = config.rigs || CONFIG.rigs;
  const sourceConfig = config.sourceConfig || CONFIG.sourceConfig;
  const COST = config.COST || CONFIG.COST;

  const SMALL_BARGE_VOLUME = config.SMALL_BARGE_VOLUME || CONFIG.SMALL_BARGE_VOLUME;
  const LARGE_BARGE_VOLUME = config.LARGE_BARGE_VOLUME || CONFIG.LARGE_BARGE_VOLUME;
  const DRILLING_START_HOUR = config.DRILLING_START_HOUR || CONFIG.DRILLING_START_HOUR;
  const DRILLING_END_HOUR = config.DRILLING_END_HOUR || CONFIG.DRILLING_END_HOUR;
  const SMALL_BARGES_PER_TUG = config.SMALL_BARGES_PER_TUG || CONFIG.SMALL_BARGES_PER_TUG;
  const LARGE_BARGES_PER_TUG = config.LARGE_BARGES_PER_TUG || CONFIG.LARGE_BARGES_PER_TUG || 1;
  const TOTAL_LARGE_BARGES = config.TOTAL_LARGE_BARGES || CONFIG.TOTAL_LARGE_BARGES || 4;

  // Speeds and timing
  const loadedSpeed = config.loadedSpeedSmall || CONFIG.loadedSpeedSmall || 5;
  const loadedSpeedLarge = config.loadedSpeedLarge || CONFIG.loadedSpeedLarge || 5;
  const emptySpeed = config.emptySpeed || CONFIG.emptySpeed || 5;
  const hookupTime = config.hookupTime || CONFIG.hookupTime || 0.33;
  const pumpRate = config.pumpRate || CONFIG.pumpRate || 1000; // GPM

  // Mob/demob costs
  const tugMobCost = COST.mobilization?.tugMobCost || 9000;
  const tugDemobCost = COST.mobilization?.tugDemobCost || 9000;
  const smallBargeMobCost = COST.mobilization?.smallBargeMobCost || 500;
  const smallBargeDemobCost = COST.mobilization?.smallBargeDemobCost || 500;
  const largeBargeMobCost = COST.mobilization?.largeBargeMobCost || 1500;
  const largeBargeDemobCost = COST.mobilization?.largeBargeDemobCost || 1500;

  // Daily rates
  const tugDayRate = COST.tug?.dayRate || config.TUG_DAY_RATE || 4500;
  const smallBargeDayRate = COST.barge?.smallDayRate || config.SMALL_BARGE_DAY_RATE || 250;
  const largeBargeDayRate = COST.barge?.largeDayRate || config.LARGE_BARGE_DAY_RATE || 750;

  // Demob/remob threshold (days of idle to justify demob/remob)
  const tugDemobThreshold = (tugMobCost + tugDemobCost) / tugDayRate; // Break-even days
  const practicalDemobThreshold = Math.max(tugDemobThreshold + 2, 5); // Add buffer for practical considerations

  console.log('[ANALYTICAL FLEET] Starting pre-simulation fleet analysis...');
  console.log(`[ANALYTICAL FLEET] Tug demob break-even: ${tugDemobThreshold.toFixed(1)} days, practical threshold: ${practicalDemobThreshold.toFixed(0)} days`);

  // =========================================================================
  // STEP 1: Build distance matrix and find best source for each HDD
  // =========================================================================
  // Extract flow rates from sourceConfig for best-source calculation
  const sourceFlowRates = {};
  Object.keys(sourceConfig).forEach(srcId => {
    sourceFlowRates[srcId] = sourceConfig[srcId]?.flowRate || 400;
  });
  const { distanceMatrix, bestSourceForHDD } = buildDistanceMatrices(hddSchedule, sourceFlowRates);

  // =========================================================================
  // STEP 2: Calculate cycle times for each source-to-HDD combination
  // =========================================================================
  const cycleTimeCache = {};

  const calculateCycleTime = (sourceId, hddCrossing) => {
    const cacheKey = `${sourceId}-${hddCrossing}`;
    if (cycleTimeCache[cacheKey]) return cycleTimeCache[cacheKey];

    const distance = distanceMatrix[sourceId]?.[hddCrossing] || 15; // Default 15nm if unknown (consistent fallback)
    const flowRate = sourceConfig[sourceId]?.flowRate || 400; // GPM

    // Calculate efficiency for 1 SMALL barge per tug (single small)
    const single1VolumePerTrip = SMALL_BARGE_VOLUME * 1;
    const single1FillTime = single1VolumePerTrip / flowRate / 60;
    const single1TravelToHDD = distance / loadedSpeed;
    const single1PumpTime = single1VolumePerTrip / pumpRate / 60 + hookupTime * 2;
    const single1ReturnTravel = distance / emptySpeed;
    const single1CycleTime = single1FillTime + single1TravelToHDD + single1PumpTime + single1ReturnTravel;
    const single1GallonsPerHour = single1VolumePerTrip / single1CycleTime;

    // Calculate efficiency for 2 SMALL barges per tug (double small)
    const small2VolumePerTrip = SMALL_BARGE_VOLUME * SMALL_BARGES_PER_TUG;
    const small2FillTime = small2VolumePerTrip / flowRate / 60;
    const small2TravelToHDD = distance / loadedSpeed;
    const small2PumpTime = small2VolumePerTrip / pumpRate / 60 + hookupTime * 2;
    const small2ReturnTravel = distance / emptySpeed;
    const small2CycleTime = small2FillTime + small2TravelToHDD + small2PumpTime + small2ReturnTravel;
    const small2GallonsPerHour = small2VolumePerTrip / small2CycleTime;

    // Calculate efficiency for 1 LARGE barge per tug
    const large1VolumePerTrip = LARGE_BARGE_VOLUME * LARGE_BARGES_PER_TUG;
    const large1FillTime = large1VolumePerTrip / flowRate / 60;
    const large1TravelToHDD = distance / loadedSpeedLarge;
    const large1PumpTime = large1VolumePerTrip / pumpRate / 60 + hookupTime * 2;
    const large1ReturnTravel = distance / emptySpeed;
    const large1CycleTime = large1FillTime + large1TravelToHDD + large1PumpTime + large1ReturnTravel;
    const large1GallonsPerHour = large1VolumePerTrip / large1CycleTime;

    // Legacy aliases for backward compatibility
    const smallCycleTime = small2CycleTime;
    const smallGallonsPerHour = small2GallonsPerHour;
    const smallVolumePerTrip = small2VolumePerTrip;
    const largeCycleTime = large1CycleTime;
    const largeGallonsPerHour = large1GallonsPerHour;
    const largeVolumePerTrip = large1VolumePerTrip;

    // =========================================================================
    // STEP 2B: COST-EFFICIENCY ANALYSIS (per source-HDD combination)
    // =========================================================================

    // STORAGE COST EFFICIENCY (cost per gallon of capacity per day)
    const smallStorageCostPerGal = smallBargeDayRate / SMALL_BARGE_VOLUME;
    const largeStorageCostPerGal = largeBargeDayRate / LARGE_BARGE_VOLUME;
    const storageSavingsPercent = ((smallStorageCostPerGal - largeStorageCostPerGal) / smallStorageCostPerGal) * 100;
    const storagePreference = largeStorageCostPerGal < smallStorageCostPerGal ? 'large' : 'small';

    // TRANSPORT COST EFFICIENCY (cost per gallon/hr throughput)
    // FIXED: Include tug cost in transport efficiency calculation
    // Cost = (tug daily rate + barge daily rate × barges per tug) / 24 hours / throughput
    const single1TransportCostPerThroughput = ((tugDayRate + smallBargeDayRate * 1) / 24) / single1GallonsPerHour;
    const small2TransportCostPerThroughput = ((tugDayRate + smallBargeDayRate * SMALL_BARGES_PER_TUG) / 24) / small2GallonsPerHour;
    const large1TransportCostPerThroughput = ((tugDayRate + largeBargeDayRate * LARGE_BARGES_PER_TUG) / 24) / large1GallonsPerHour;

    // Find best transport option (lowest cost per throughput)
    const transportOptions = [
      { name: '1-small', costPerThroughput: single1TransportCostPerThroughput, cycleTime: single1CycleTime, gallonsPerHour: single1GallonsPerHour, bargesPerTug: 1, bargeType: 'small' },
      { name: '2-small', costPerThroughput: small2TransportCostPerThroughput, cycleTime: small2CycleTime, gallonsPerHour: small2GallonsPerHour, bargesPerTug: 2, bargeType: 'small' },
      { name: '1-large', costPerThroughput: large1TransportCostPerThroughput, cycleTime: large1CycleTime, gallonsPerHour: large1GallonsPerHour, bargesPerTug: 1, bargeType: 'large' }
    ];
    transportOptions.sort((a, b) => a.costPerThroughput - b.costPerThroughput);
    const bestTransportOption = transportOptions[0];
    const worstTransportOption = transportOptions[transportOptions.length - 1];

    const transportPreference = bestTransportOption.bargeType;
    const transportSavingsPercent = ((worstTransportOption.costPerThroughput - bestTransportOption.costPerThroughput) / worstTransportOption.costPerThroughput) * 100;

    // Legacy aliases for backward compatibility
    const smallTransportCostPerThroughput = small2TransportCostPerThroughput;
    const largeTransportCostPerThroughput = large1TransportCostPerThroughput;

    // Throughput efficiency gain (for reference - large vs 2 small)
    const largeBargeEfficiencyGain = (large1GallonsPerHour / small2GallonsPerHour - 1) * 100;

    // Determine best cycle time for transport (cost-optimal)
    const bestTransportCycleTime = bestTransportOption.cycleTime;
    const bestTransportGallonsPerHour = bestTransportOption.gallonsPerHour;
    const bestTransportBargesPerTug = bestTransportOption.bargesPerTug;
    const bestTransportName = bestTransportOption.name;

    // For storage, always use small cycle time as baseline (storage uses small barges initially)
    // Large barge allocation happens in Step 5 based on total volume
    const bestStorageCycleTime = smallCycleTime; // Storage calculation baseline

    cycleTimeCache[cacheKey] = {
      // Cycle time data - all three transport configurations
      single1CycleTime,
      small2CycleTime,
      large1CycleTime,
      single1VolumePerTrip,
      small2VolumePerTrip,
      large1VolumePerTrip,
      distance,
      flowRate,

      // Legacy aliases (2 small = "small", 1 large = "large")
      smallCycleTime,
      largeCycleTime,
      smallVolumePerTrip,
      largeVolumePerTrip,

      // Throughput data - all three configurations
      single1GallonsPerHour,
      small2GallonsPerHour,
      large1GallonsPerHour,
      smallGallonsPerHour,  // Legacy alias for small2
      largeGallonsPerHour,  // Legacy alias for large1
      largeBargeEfficiencyGain,

      // STORAGE COST EFFICIENCY
      smallStorageCostPerGal,
      largeStorageCostPerGal,
      storageSavingsPercent,
      storagePreference,

      // TRANSPORT COST EFFICIENCY (includes tug cost)
      single1TransportCostPerThroughput,
      small2TransportCostPerThroughput,
      large1TransportCostPerThroughput,
      smallTransportCostPerThroughput,  // Legacy alias for small2
      largeTransportCostPerThroughput,  // Legacy alias for large1
      transportOptions,  // Array of all options sorted by cost
      bestTransportOption,
      transportSavingsPercent,
      transportPreference,

      // BEST/RECOMMENDED values for analytical model
      cycleTime: bestTransportCycleTime,
      storageCycleTime: bestStorageCycleTime,
      gallonsPerHour: bestTransportGallonsPerHour,
      bestTransportBargesPerTug,
      bestTransportName
    };

    return cycleTimeCache[cacheKey];
  };

  // Pre-calculate for all source-HDD combinations
  Object.keys(sourceConfig).forEach(sourceId => {
    hddSchedule.forEach(hdd => {
      calculateCycleTime(sourceId, hdd.crossing);
    });
  });

  // Log cost-efficiency analysis summary
  console.log('[ANALYTICAL FLEET] === COST-EFFICIENCY ANALYSIS (Step 2B) ===');
  const storageSmallCostPerGal = smallBargeDayRate / SMALL_BARGE_VOLUME;
  const storageLargeCostPerGal = largeBargeDayRate / LARGE_BARGE_VOLUME;
  console.log(`  Storage: Large barges cost $${storageLargeCostPerGal.toFixed(6)}/gal vs Small $${storageSmallCostPerGal.toFixed(6)}/gal`);
  console.log(`  Transport: Cost per throughput = (tug $${tugDayRate}/day + barge cost) / throughput`);
  console.log('');

  // Show COMPLETE source × config analysis for each HDD
  console.log('[ANALYTICAL FLEET] === PER-HDD SOURCE & CONFIG SELECTION ===');
  console.log('  (Evaluates ALL sources × ALL transport configs to find lowest cost/throughput)');

  hddSchedule.forEach(hdd => {
    const crossing = hdd.crossing;

    // Gather all (source, config) combinations
    const allCombinations = [];
    Object.keys(sourceConfig).forEach(srcId => {
      const cacheKey = `${srcId}-${crossing}`;
      const analysis = cycleTimeCache[cacheKey];
      if (analysis && analysis.transportOptions) {
        analysis.transportOptions.forEach(opt => {
          allCombinations.push({
            source: srcId,
            config: opt.name,
            distance: analysis.distance,
            flowRate: analysis.flowRate,
            costPerThroughput: opt.costPerThroughput,
            cycleTime: opt.cycleTime,
            gallonsPerHour: opt.gallonsPerHour
          });
        });
      }
    });

    // Sort by cost per throughput (best first)
    allCombinations.sort((a, b) => a.costPerThroughput - b.costPerThroughput);
    const best = allCombinations[0];
    const worst = allCombinations[allCombinations.length - 1];

    // Log the best option and comparison
    console.log(`  ${crossing}:`);
    console.log(`    BEST: ${best.source} + ${best.config} (${best.distance.toFixed(1)}nm, ${best.flowRate}gpm) → $${best.costPerThroughput.toFixed(4)}/throughput`);

    // Show top 3 options for comparison
    const top3 = allCombinations.slice(0, 3).map(c =>
      `${c.source.replace('source','src')}+${c.config}=$${c.costPerThroughput.toFixed(4)}`
    ).join(', ');
    console.log(`    Top 3: ${top3}`);

    const savings = ((worst.costPerThroughput - best.costPerThroughput) / worst.costPerThroughput * 100).toFixed(1);
    console.log(`    Savings vs worst: ${savings}%`);
  });

  // =========================================================================
  // STEP 3: Build detailed daily demand profile with HDD-level granularity
  // =========================================================================

  // Find project date range
  const allDates = hddSchedule.flatMap(h => [
    new Date(h.start), new Date(h.rigDown)
  ]);
  const minDate = new Date(Math.min(...allDates));
  const maxDate = new Date(Math.max(...allDates));
  const totalDays = Math.ceil((maxDate - minDate) / (1000 * 60 * 60 * 24)) + 1;

  console.log(`[ANALYTICAL FLEET] Project span: ${minDate.toISOString().split('T')[0]} to ${maxDate.toISOString().split('T')[0]} (${totalDays} days)`);

  const dailyProfile = [];
  const drillingHours = DRILLING_END_HOUR - DRILLING_START_HOUR;

  for (let d = 0; d < totalDays; d++) {
    const currentDate = new Date(minDate);
    currentDate.setDate(currentDate.getDate() + d);
    const dateStr = currentDate.toISOString().split('T')[0];
    const dayOfWeek = currentDate.getDay();

    const activeHDDs = [];
    let totalDailyDemand = 0;

    hddSchedule.forEach(hdd => {
      const { crossing, rig } = hdd;
      const rigConfig = rigs[rig];

      // Check if this HDD is active on this date
      if (dateStr < hdd.start || dateStr > hdd.rigDown) return;

      // Determine stage and hourly rate
      let stage = 'RigDown';
      let hourlyRate = 0;

      if (dateStr <= hdd.rigUp) {
        stage = 'RigUp';
        hourlyRate = 0;
      } else if (dateStr <= hdd.pilot) {
        stage = 'Pilot';
        hourlyRate = rigConfig.pilot;
      } else if (dateStr <= hdd.ream) {
        stage = 'Ream';
        hourlyRate = rigConfig.ream;
      } else if (dateStr <= hdd.swab) {
        stage = 'Swab';
        hourlyRate = rigConfig.swab;
      } else if (dateStr <= hdd.pull) {
        stage = 'Pull';
        hourlyRate = rigConfig.pull;
      } else {
        stage = 'RigDown';
        hourlyRate = 0;
      }

      // No work on Sundays if configured
      if (dayOfWeek === 0 && !hdd.worksSundays) {
        hourlyRate = 0;
      }

      const dailyDemand = hourlyRate * drillingHours;

      if (dateStr >= hdd.start && dateStr <= hdd.rigDown) {
        // Evaluate ALL sources to find best (source, transport config) combination
        const sourceOptions = Object.keys(sourceConfig).map(srcId => {
          const cycleInfo = calculateCycleTime(srcId, crossing);
          return {
            sourceId: srcId,
            distance: distanceMatrix[srcId]?.[crossing] || 15,
            flowRate: cycleInfo.flowRate,
            cycleInfo,
            // Best transport option for THIS source
            bestTransportOption: cycleInfo.bestTransportOption,
            costPerThroughput: cycleInfo.bestTransportOption?.costPerThroughput || Infinity
          };
        });

        // Pick source with lowest cost per throughput (best overall efficiency)
        sourceOptions.sort((a, b) => a.costPerThroughput - b.costPerThroughput);
        const bestOption = sourceOptions[0];
        const cycleInfo = bestOption.cycleInfo;

        activeHDDs.push({
          crossing,
          rig,
          stage,
          hourlyRate,
          dailyDemand,
          needsStorage: stage !== 'RigUp' && stage !== 'RigDown',
          needsWater: dailyDemand > 0,
          // Best source selected based on cost efficiency (considers distance + flow rate + transport config)
          bestSource: bestOption.sourceId,
          distance: bestOption.distance,
          flowRate: bestOption.flowRate,
          cycleTime: cycleInfo.cycleTime,
          gallonsPerTugPerDay: cycleInfo.gallonsPerHour * 24,
          // Per-HDD cost preferences
          storagePreference: cycleInfo.storagePreference,
          transportPreference: cycleInfo.transportPreference,
          bestTransportConfig: cycleInfo.bestTransportName,
          bestTransportBargesPerTug: cycleInfo.bestTransportBargesPerTug,
          // All source options for transparency
          allSourceOptions: sourceOptions.map(s => ({
            source: s.sourceId,
            config: s.bestTransportOption?.name,
            costPerThroughput: s.costPerThroughput?.toFixed(4)
          }))
        });

        totalDailyDemand += dailyDemand;
      }
    });

    // =========================================================================
    // STEP 4: Calculate tug requirements based on PURE THROUGHPUT
    // =========================================================================
    // Tugs can make multi-stop deliveries, so only throughput matters (not # of sites)
    // Average throughput considering multiple HDDs at different distances
    const avgGallonsPerTugPerDay = activeHDDs.length > 0
      ? activeHDDs.reduce((sum, h) => sum + h.gallonsPerTugPerDay, 0) / activeHDDs.length
      : 200000; // Fallback if no active HDDs

    // Tugs needed based on pure throughput
    const tugsNeededRaw = totalDailyDemand > 0
      ? totalDailyDemand / avgGallonsPerTugPerDay
      : 0;

    // Add buffer for storage delivery and contention
    // Storage barges need tugs to deliver them to HDDs (not just transport water)
    // Also need buffer for travel time, wait time, and multi-HDD contention
    const hddsNeedingStorageToday = activeHDDs.filter(h => h.needsStorage).length;
    const storageDeliveryTugs = hddsNeedingStorageToday > 0 ? 1 : 0; // 1 tug for storage delivery
    const contentionBuffer = activeHDDs.length > 1 ? 0.5 : 0; // 50% buffer for multi-HDD

    const tugsNeeded = Math.ceil(tugsNeededRaw * (1 + contentionBuffer) + storageDeliveryTugs);

    // =========================================================================
    // STEP 5-6: COST-OPTIMIZED LARGE BARGE ALLOCATION
    // =========================================================================
    // Evaluates all possible allocations of TOTAL_LARGE_BARGES between storage/transport
    // Picks the allocation that minimizes total daily equipment cost
    // Uses user-configured volumes (LARGE_BARGE_VOLUME, SMALL_BARGE_VOLUME) and limits (TOTAL_LARGE_BARGES)

    const hddsNeedingStorage = activeHDDs.filter(h => h.needsStorage);
    const hddsNeedingWater = activeHDDs.filter(h => h.needsWater);

    // Calculate total STORAGE requirement (buffer = hourly rate × cycle time per HDD)
    let totalStorageNeeded = 0;
    hddsNeedingStorage.forEach(hdd => {
      const cacheKey = `${hdd.bestSource}-${hdd.crossing}`;
      const analysis = cycleTimeCache[cacheKey];
      const cycleTime = analysis?.storageCycleTime || analysis?.cycleTime || 12;
      totalStorageNeeded += hdd.hourlyRate * cycleTime;
    });

    // Calculate total TRANSPORT requirement (volume in transit = hourly throughput × avg cycle time)
    const hourlyThroughput = totalDailyDemand / 24;
    const avgCycleTimeForTransport = hddsNeedingWater.length > 0
      ? hddsNeedingWater.reduce((sum, h) => sum + h.cycleTime, 0) / hddsNeedingWater.length
      : 12;
    const totalTransportNeeded = hourlyThroughput * avgCycleTimeForTransport;

    // Calculate throughput capacity per tug for large barges (from active HDD analysis)
    const largeGallonsPerTugPerDay = hddsNeedingWater.length > 0
      ? hddsNeedingWater.reduce((sum, h) => sum + h.gallonsPerTugPerDay, 0) / hddsNeedingWater.length
      : 400000;

    // Calculate throughput capacity per tug for small barges (2-small config)
    // Small barges have faster cycle time (less fill time) but less volume
    const smallVolumePerTrip = SMALL_BARGE_VOLUME * SMALL_BARGES_PER_TUG;
    const smallCycleTimeApprox = avgCycleTimeForTransport * (smallVolumePerTrip / LARGE_BARGE_VOLUME);
    const smallGallonsPerTugPerDay = smallVolumePerTrip / Math.max(smallCycleTimeApprox, 1) * 24;

    // Evaluate ALL possible large barge allocations (0 to TOTAL_LARGE_BARGES for storage)
    const allocationOptions = [];

    for (let largeForStorage = 0; largeForStorage <= TOTAL_LARGE_BARGES; largeForStorage++) {
      const largeForTransport = TOTAL_LARGE_BARGES - largeForStorage;

      // STORAGE: Large barges cover some, small barges fill the gap
      const largeStorageCap = largeForStorage * LARGE_BARGE_VOLUME;
      const storageGap = Math.max(0, totalStorageNeeded - largeStorageCap);
      const smallStorageNeeded = Math.ceil(storageGap / SMALL_BARGE_VOLUME);

      // TRANSPORT: Large barges provide throughput, small tugs fill the gap
      const largeTugsForTransport = largeForTransport > 0 ? Math.ceil(largeForTransport / LARGE_BARGES_PER_TUG) : 0;
      const largeTransportCap = largeTugsForTransport * largeGallonsPerTugPerDay;
      const transportGap = Math.max(0, totalDailyDemand - largeTransportCap);
      const smallTugsNeeded = transportGap > 0 ? Math.ceil(transportGap / smallGallonsPerTugPerDay) : 0;
      const smallTransportNeeded = smallTugsNeeded * SMALL_BARGES_PER_TUG;

      // Total tugs needed
      const totalTugsNeeded = largeTugsForTransport + smallTugsNeeded;

      // DAILY EQUIPMENT COST (uses configured day rates)
      const dailyCost =
        (totalTugsNeeded * tugDayRate) +
        ((largeForStorage + largeForTransport) * largeBargeDayRate) +
        (smallStorageNeeded * smallBargeDayRate) +
        (smallTransportNeeded * smallBargeDayRate);

      allocationOptions.push({
        largeForStorage,
        largeForTransport,
        smallStorageNeeded,
        smallTugsNeeded,
        smallTransportNeeded,
        largeTugsForTransport,
        totalTugsNeeded,
        dailyCost,
        storageCoverage: totalStorageNeeded > 0 ? (largeStorageCap / totalStorageNeeded * 100) : 100,
        transportCoverage: totalDailyDemand > 0 ? (largeTransportCap / totalDailyDemand * 100) : 100
      });
    }

    // Sort by daily cost (ascending) and pick the minimum cost allocation
    allocationOptions.sort((a, b) => a.dailyCost - b.dailyCost);
    const bestAllocation = allocationOptions[0];

    // Apply the cost-optimal allocation
    const largeStorageBarges = bestAllocation.largeForStorage;
    const largeTransportBarges = bestAllocation.largeForTransport;
    const smallStorageBarges = bestAllocation.smallStorageNeeded;
    const smallTransportBarges = bestAllocation.smallTransportNeeded;
    const totalStorageVolume = totalStorageNeeded;
    const totalTransportVolume = totalTransportNeeded;

    // Combined totals for this day
    const storageBargesNeeded = smallStorageBarges + largeStorageBarges;
    const transportBargesNeeded = smallTransportBarges + largeTransportBarges;

    dailyProfile.push({
      dayIndex: d,
      date: dateStr,
      dayOfWeek,
      activeHDDs,
      totalDailyDemand,
      tugsNeededRaw,
      tugsNeeded,

      // Storage breakdown
      storageBargesNeeded,
      smallStorageBarges,
      largeStorageBarges,

      // Transport breakdown
      transportBargesNeeded,
      smallTransportBarges,
      largeTransportBarges,

      // Totals
      totalSmallBargesNeeded: smallStorageBarges + smallTransportBarges,
      totalLargeBargesNeeded: largeStorageBarges + largeTransportBarges,
      avgGallonsPerTugPerDay
    });
  }

  // =========================================================================
  // END OF ANALYTICAL FLEET SIZING (Steps 1-6)
  // Steps 7-12 (mob/demob schedules, large barge allocation, buffers) DISABLED
  // Return simplified recommendation based on pure throughput/cycle-time analysis
  // =========================================================================

  const peakTugs = Math.max(...dailyProfile.map(d => d.tugsNeeded));

  // Peak requirements by size
  let peakSmallStorage = Math.max(...dailyProfile.map(d => d.smallStorageBarges));
  let peakLargeStorage = Math.max(...dailyProfile.map(d => d.largeStorageBarges));
  let peakSmallTransport = Math.max(...dailyProfile.map(d => d.smallTransportBarges));
  let peakLargeTransport = Math.max(...dailyProfile.map(d => d.largeTransportBarges));

  // CONSTRAINT CHECK: Total large barges cannot exceed TOTAL_LARGE_BARGES
  // If peaks calculated independently exceed the limit, we need to rebalance
  const totalLargePeaks = peakLargeStorage + peakLargeTransport;
  if (totalLargePeaks > TOTAL_LARGE_BARGES) {
    console.log(`[ANALYTICAL FLEET] Large barge constraint violation: ${totalLargePeaks} needed but only ${TOTAL_LARGE_BARGES} available`);

    // Strategy: PROPORTIONAL allocation - large barges are valuable for BOTH transport and storage
    // Allocating all to storage cripples transport throughput (as shown by testing)
    // Split proportionally based on original needs, with minimum 1 for transport if needed
    const originalLargeStorage = peakLargeStorage;
    const originalLargeTransport = peakLargeTransport;
    const totalOriginal = originalLargeStorage + originalLargeTransport;

    // Proportional allocation
    const transportRatio = originalLargeTransport / totalOriginal;
    const storageRatio = originalLargeStorage / totalOriginal;

    // Allocate proportionally, rounding transport up (transport throughput is critical)
    peakLargeTransport = Math.min(
      Math.max(1, Math.ceil(TOTAL_LARGE_BARGES * transportRatio)), // At least 1 for transport
      originalLargeTransport, // Don't exceed original need
      TOTAL_LARGE_BARGES - 1  // Leave at least 1 for storage
    );
    peakLargeStorage = TOTAL_LARGE_BARGES - peakLargeTransport;

    // Compensate with small barges for the deficit
    const largeStorageDeficit = originalLargeStorage - peakLargeStorage;
    const largeTransportDeficit = originalLargeTransport - peakLargeTransport;

    // Convert deficit to small barge equivalents (300k / 80k = 3.75 small per large)
    const smallPerLarge = Math.ceil(LARGE_BARGE_VOLUME / SMALL_BARGE_VOLUME);
    peakSmallStorage += largeStorageDeficit * smallPerLarge;
    peakSmallTransport += largeTransportDeficit * smallPerLarge;

    console.log(`[ANALYTICAL FLEET] Rebalanced: ${peakLargeTransport} large transport + ${peakLargeStorage} large storage = ${TOTAL_LARGE_BARGES} total`);
    console.log(`[ANALYTICAL FLEET] Compensation: +${largeStorageDeficit * smallPerLarge} small storage, +${largeTransportDeficit * smallPerLarge} small transport`);
  }

  const recommendedFleetConfig = {
    numSmallTugs: peakTugs,
    numSmallTransportBarges: peakSmallTransport,
    numSmallStorageBarges: peakSmallStorage,
    numLargeTransport: peakLargeTransport,
    numLargeStorage: peakLargeStorage
  };

  // Determine most common best transport configuration across all HDDs
  const transportConfigCounts = { '1-small': 0, '2-small': 0, '1-large': 0 };
  hddSchedule.forEach(hdd => {
    const bestSource = bestSourceForHDD[hdd.crossing]?.source;
    if (bestSource) {
      const cacheKey = `${bestSource}-${hdd.crossing}`;
      const analysis = cycleTimeCache[cacheKey];
      if (analysis?.bestTransportName) {
        transportConfigCounts[analysis.bestTransportName]++;
      }
    }
  });
  const dominantTransportConfig = Object.entries(transportConfigCounts)
    .sort((a, b) => b[1] - a[1])[0][0];

  const summary = {
    projectDays: totalDays,
    projectStart: minDate.toISOString().split('T')[0],
    projectEnd: maxDate.toISOString().split('T')[0],
    peakTugs,
    peakSmallStorage,
    peakLargeStorage,
    peakSmallTransport,
    peakLargeTransport,
    totalSmallBarges: peakSmallStorage + peakSmallTransport,
    totalLargeBarges: peakLargeStorage + peakLargeTransport,
    method: 'cost-optimal-allocation',
    dominantTransportConfig,
    transportConfigCounts,
    notes: 'Cost-optimal allocation with tug+barge transport costs'
  };

  console.log('[ANALYTICAL FLEET] === COST-OPTIMAL SUMMARY (Steps 1-6) ===');
  console.log(`  Best transport config: ${dominantTransportConfig} (includes tug + barge costs)`);
  console.log(`  Peak tugs needed: ${peakTugs} (pure throughput)`);
  console.log(`  Storage barges: ${peakSmallStorage} small + ${peakLargeStorage} large (cycle-time, cost-optimized)`);
  console.log(`  Transport barges: ${peakSmallTransport} small + ${peakLargeTransport} large (throughput, cost-optimized)`);
  console.log(`  Total: ${peakSmallStorage + peakSmallTransport} small + ${peakLargeStorage + peakLargeTransport} large barges`);

  return {
    success: true,
    dailyProfile,
    summary,
    recommendedFleetConfig,
    cycleTimeAnalysis: cycleTimeCache,
    bestSourceForHDD,
    // Steps 7-12 disabled - no detailed schedules
    tugSchedule: [],
    storageBargeSchedule: [],
    transportBargeSchedule: []
  };

  // =========================================================================
  // STEPS 7-12 BELOW ARE DISABLED BUT PRESERVED FOR FUTURE RE-ENABLE
  // =========================================================================
  // To re-enable: comment out the return above and uncomment the code below
  /* DISABLED - START

  // =========================================================================
  // STEP 7: Derive optimal mob/demob schedule for tugs
  // =========================================================================

  // Find continuous periods where tugs are needed
  const tugPeriods = [];
  let currentTugPeriod = null;

  dailyProfile.forEach((day, idx) => {
    if (day.tugsNeeded > 0) {
      if (!currentTugPeriod) {
        currentTugPeriod = {
          startDay: idx,
          startDate: day.date,
          peakTugs: day.tugsNeeded,
          tugsNeededByDay: [day.tugsNeeded]
        };
      } else {
        currentTugPeriod.peakTugs = Math.max(currentTugPeriod.peakTugs, day.tugsNeeded);
        currentTugPeriod.tugsNeededByDay.push(day.tugsNeeded);
      }
    } else if (currentTugPeriod) {
      // Check if this gap is long enough to justify demob
      let gapDays = 0;
      for (let g = idx; g < dailyProfile.length && dailyProfile[g].tugsNeeded === 0; g++) {
        gapDays++;
      }

      if (gapDays >= practicalDemobThreshold) {
        // End current period
        currentTugPeriod.endDay = idx - 1;
        currentTugPeriod.endDate = dailyProfile[idx - 1].date;
        currentTugPeriod.daysOnSite = currentTugPeriod.endDay - currentTugPeriod.startDay + 1;
        tugPeriods.push(currentTugPeriod);
        currentTugPeriod = null;
      } else {
        // Gap too short, keep tugs on site
        currentTugPeriod.tugsNeededByDay.push(0);
      }
    }
  });

  // Close final period
  if (currentTugPeriod) {
    const lastDemandDay = dailyProfile.findLastIndex(d => d.tugsNeeded > 0);
    currentTugPeriod.endDay = lastDemandDay;
    currentTugPeriod.endDate = dailyProfile[lastDemandDay].date;
    currentTugPeriod.daysOnSite = currentTugPeriod.endDay - currentTugPeriod.startDay + 1;
    tugPeriods.push(currentTugPeriod);
  }

  console.log(`[ANALYTICAL FLEET] Identified ${tugPeriods.length} tug deployment period(s)`);

  // =========================================================================
  // STEP 5: Create individual tug schedules with staggered mob
  // =========================================================================

  const tugSchedule = [];
  let tugIdCounter = 1;

  tugPeriods.forEach((period, periodIdx) => {
    // Find when each tug is first needed within the period
    const tugFirstNeededDay = [];
    let maxTugsInPeriod = 0;

    period.tugsNeededByDay.forEach((needed, dayOffset) => {
      for (let t = maxTugsInPeriod; t < needed; t++) {
        tugFirstNeededDay.push(period.startDay + dayOffset);
      }
      maxTugsInPeriod = Math.max(maxTugsInPeriod, needed);
    });

    // Find when each tug is last needed (reverse)
    const tugLastNeededDay = [];
    let tugsStillNeeded = maxTugsInPeriod;

    for (let dayOffset = period.tugsNeededByDay.length - 1; dayOffset >= 0; dayOffset--) {
      const needed = period.tugsNeededByDay[dayOffset];
      while (tugsStillNeeded > needed) {
        tugLastNeededDay.unshift(period.startDay + dayOffset);
        tugsStillNeeded--;
      }
    }
    // Fill remaining with end of period
    while (tugLastNeededDay.length < maxTugsInPeriod) {
      tugLastNeededDay.unshift(period.endDay);
    }

    // Create schedule for each tug
    for (let t = 0; t < maxTugsInPeriod; t++) {
      const mobDay = Math.max(0, tugFirstNeededDay[t] - 1); // Mob 1 day before first needed
      const demobDay = Math.min(totalDays - 1, tugLastNeededDay[t] + 1); // Demob 1 day after last needed

      tugSchedule.push({
        id: `T${tugIdCounter++}`,
        periodIndex: periodIdx,
        mobDay,
        demobDay,
        mobDate: dailyProfile[mobDay].date,
        demobDate: dailyProfile[demobDay].date,
        daysOnSite: demobDay - mobDay + 1,
        mobCost: tugMobCost,
        demobCost: tugDemobCost,
        dailyRate: tugDayRate,
        totalRentalCost: (demobDay - mobDay + 1) * tugDayRate,
        totalCost: tugMobCost + tugDemobCost + (demobDay - mobDay + 1) * tugDayRate
      });
    }
  });

  console.log(`[ANALYTICAL FLEET] Scheduled ${tugSchedule.length} tugs`);
  tugSchedule.forEach(t => {
    console.log(`  ${t.id}: mob ${t.mobDate}, demob ${t.demobDate} (${t.daysOnSite} days, $${t.totalCost.toLocaleString()})`);
  });

  // =========================================================================
  // STEP 6: Create storage barge schedule (per HDD lifecycle)
  // =========================================================================

  const storageBargeSchedule = [];
  let storageBargeIdCounter = 1;

  // Sort HDDs by pilot date
  const hddsByPilot = [...hddSchedule].sort((a, b) => new Date(a.pilot) - new Date(b.pilot));

  // Track barge availability for reuse
  const bargeAvailability = []; // { bargeId, availableDate }

  hddsByPilot.forEach(hdd => {
    const pilotDate = new Date(hdd.pilot);
    const rigDownDate = new Date(hdd.rigDown);
    const rigConfig = rigs[hdd.rig];

    // Calculate storage needed for this HDD
    const peakHourlyRate = Math.max(
      rigConfig.pilot || 0,
      rigConfig.ream || 0,
      rigConfig.swab || 0,
      rigConfig.pull || 0
    );
    const peakDailyDemand = peakHourlyRate * drillingHours;
    const bufferCapacity = peakDailyDemand * 1.5;
    const bargesNeeded = Math.max(1, Math.ceil(bufferCapacity / SMALL_BARGE_VOLUME));

    // Mob timing: arrive 1 day before pilot
    const mobDate = new Date(pilotDate);
    mobDate.setDate(mobDate.getDate() - 1);

    // Demob timing: 1 day after rigDown
    const demobDate = new Date(rigDownDate);
    demobDate.setDate(demobDate.getDate() + 1);

    const mobDayIndex = Math.max(0, Math.round((mobDate - minDate) / (1000 * 60 * 60 * 24)));
    const demobDayIndex = Math.min(totalDays - 1, Math.round((demobDate - minDate) / (1000 * 60 * 60 * 24)));

    // Check if we can reuse barges from completed HDDs
    const reusableBarges = bargeAvailability.filter(b => new Date(b.availableDate) <= mobDate);

    for (let b = 0; b < bargesNeeded; b++) {
      let isReuse = false;
      let reusedFrom = null;

      if (reusableBarges.length > 0) {
        const reused = reusableBarges.shift();
        isReuse = true;
        reusedFrom = reused.lastHDD;
        // Remove from availability pool
        const idx = bargeAvailability.findIndex(ba => ba.bargeId === reused.bargeId);
        if (idx >= 0) bargeAvailability.splice(idx, 1);
      }

      const bargeId = isReuse ? reusedFrom + '->' : `SB${storageBargeIdCounter++}`;

      storageBargeSchedule.push({
        id: bargeId,
        type: 'small',
        role: 'storage',
        assignedHDD: hdd.crossing,
        assignedRig: hdd.rig,
        mobDay: mobDayIndex,
        demobDay: demobDayIndex,
        mobDate: dailyProfile[mobDayIndex]?.date || mobDate.toISOString().split('T')[0],
        demobDate: dailyProfile[demobDayIndex]?.date || demobDate.toISOString().split('T')[0],
        daysOnSite: demobDayIndex - mobDayIndex + 1,
        isReuse,
        reusedFrom,
        mobCost: isReuse ? 0 : smallBargeMobCost,
        demobCost: smallBargeDemobCost, // Always pay demob at end
        dailyRate: smallBargeDayRate,
        totalRentalCost: (demobDayIndex - mobDayIndex + 1) * smallBargeDayRate,
        totalCost: (isReuse ? 0 : smallBargeMobCost) + smallBargeDemobCost + (demobDayIndex - mobDayIndex + 1) * smallBargeDayRate
      });

      // Add to availability pool after this HDD completes
      bargeAvailability.push({
        bargeId: `SB${storageBargeIdCounter - 1}`,
        availableDate: demobDate.toISOString().split('T')[0],
        lastHDD: hdd.crossing
      });
    }
  });

  console.log(`[ANALYTICAL FLEET] Scheduled ${storageBargeSchedule.length} storage barge assignments`);

  // =========================================================================
  // STEP 7: Create transport barge schedule
  // =========================================================================

  const transportBargeSchedule = [];
  const peakTransportBarges = Math.max(...dailyProfile.map(d => d.transportBargesNeeded));

  // Transport barges follow tug schedule (2 per tug)
  // Find overall mob/demob based on tug schedule
  if (tugSchedule.length > 0) {
    const firstTugMob = Math.min(...tugSchedule.map(t => t.mobDay));
    const lastTugDemob = Math.max(...tugSchedule.map(t => t.demobDay));

    for (let b = 1; b <= peakTransportBarges; b++) {
      transportBargeSchedule.push({
        id: `TB${b}`,
        type: 'small',
        role: 'transport',
        mobDay: firstTugMob,
        demobDay: lastTugDemob,
        mobDate: dailyProfile[firstTugMob]?.date,
        demobDate: dailyProfile[lastTugDemob]?.date,
        daysOnSite: lastTugDemob - firstTugMob + 1,
        mobCost: smallBargeMobCost,
        demobCost: smallBargeDemobCost,
        dailyRate: smallBargeDayRate,
        totalRentalCost: (lastTugDemob - firstTugMob + 1) * smallBargeDayRate,
        totalCost: smallBargeMobCost + smallBargeDemobCost + (lastTugDemob - firstTugMob + 1) * smallBargeDayRate
      });
    }
  }

  console.log(`[ANALYTICAL FLEET] Scheduled ${transportBargeSchedule.length} transport barges`);

  // =========================================================================
  // STEP 8: Calculate total costs
  // =========================================================================

  const totalTugCost = tugSchedule.reduce((sum, t) => sum + t.totalCost, 0);
  const totalStorageBargeCost = storageBargeSchedule.reduce((sum, b) => sum + b.totalCost, 0);
  const totalTransportBargeCost = transportBargeSchedule.reduce((sum, b) => sum + b.totalCost, 0);

  const totalMobCost =
    tugSchedule.reduce((sum, t) => sum + t.mobCost, 0) +
    storageBargeSchedule.reduce((sum, b) => sum + b.mobCost, 0) +
    transportBargeSchedule.reduce((sum, b) => sum + b.mobCost, 0);

  const totalDemobCost =
    tugSchedule.reduce((sum, t) => sum + t.demobCost, 0) +
    storageBargeSchedule.reduce((sum, b) => sum + b.demobCost, 0) +
    transportBargeSchedule.reduce((sum, b) => sum + b.demobCost, 0);

  const totalDailyRentalCost =
    tugSchedule.reduce((sum, t) => sum + t.totalRentalCost, 0) +
    storageBargeSchedule.reduce((sum, b) => sum + b.totalRentalCost, 0) +
    transportBargeSchedule.reduce((sum, b) => sum + b.totalRentalCost, 0);

  const totalEquipmentCost = totalMobCost + totalDemobCost + totalDailyRentalCost;

  // Summary statistics
  const peakTugs = Math.max(...dailyProfile.map(d => d.tugsNeeded));
  const peakStorageBarges = Math.max(...dailyProfile.map(d => d.storageBargesNeeded));
  const totalTugDays = tugSchedule.reduce((sum, t) => sum + t.daysOnSite, 0);
  const uniqueStorageBarges = new Set(storageBargeSchedule.filter(b => !b.isReuse).map(b => b.id)).size;

  const summary = {
    projectDays: totalDays,
    projectStart: minDate.toISOString().split('T')[0],
    projectEnd: maxDate.toISOString().split('T')[0],

    // Fleet sizing
    peakTugs,
    totalTugs: tugSchedule.length,
    peakStorageBarges,
    uniqueStorageBarges,
    peakTransportBarges,
    totalTransportBarges: transportBargeSchedule.length,

    // Days
    totalTugDays,
    totalStorageBargeDays: storageBargeSchedule.reduce((sum, b) => sum + b.daysOnSite, 0),
    totalTransportBargeDays: transportBargeSchedule.reduce((sum, b) => sum + b.daysOnSite, 0),

    // Costs
    totalMobCost,
    totalDemobCost,
    totalDailyRentalCost,
    totalEquipmentCost,

    // Demob analysis
    tugDemobBreakEvenDays: tugDemobThreshold,
    practicalDemobThreshold
  };

  console.log('[ANALYTICAL FLEET] === SUMMARY ===');
  console.log(`  Peak tugs needed: ${peakTugs}`);
  console.log(`  Peak storage barges needed: ${peakStorageBarges} (${uniqueStorageBarges} unique, rest reused)`);
  console.log(`  Peak transport barges needed: ${peakTransportBarges}`);
  console.log(`  Total equipment cost: $${totalEquipmentCost.toLocaleString()}`);

  // =========================================================================
  // STEP 9: Generate recommended fleet configuration for simulation
  // LARGE BARGE ANALYSIS: Check if large barges are more efficient
  // =========================================================================

  // Analyze which HDDs would benefit from large barges
  const largeBargeBenefits = [];
  Object.entries(cycleTimeCache).forEach(([key, analysis]) => {
    if (analysis.useLargeBarges) {
      largeBargeBenefits.push({
        key,
        efficiencyGain: analysis.largeBargeEfficiencyGain,
        smallGPH: analysis.smallGallonsPerHour,
        largeGPH: analysis.largeGallonsPerHour
      });
    }
  });

  // Calculate average efficiency gain from large barges
  const avgEfficiencyGain = largeBargeBenefits.length > 0
    ? largeBargeBenefits.reduce((sum, b) => sum + b.efficiencyGain, 0) / largeBargeBenefits.length
    : 0;

  // =========================================================================
  // LARGE BARGE ALLOCATION: DEMAND-BASED (not "use all available")
  // Only use large barges if demand actually requires their capacity.
  // Large barges are cost-effective BUT we shouldn't over-provision.
  // =========================================================================

  let numLargeTransportRec = 0;
  let numSmallTransportRec = peakTransportBarges;
  let numLargeStorageRec = 0;
  let numSmallStorageRec = uniqueStorageBarges;

  if (TOTAL_LARGE_BARGES > 0) {
    // Calculate total capacity needs
    const totalTransportCapacityNeeded = peakTransportBarges * SMALL_BARGE_VOLUME;
    const totalStorageCapacityNeeded = uniqueStorageBarges * SMALL_BARGE_VOLUME;
    const totalCapacityNeeded = totalTransportCapacityNeeded + totalStorageCapacityNeeded;

    // How many large barges would this capacity require? (ceiling)
    // Only use large barges if they provide meaningful capacity benefit
    const largeBargesForCapacity = Math.ceil(totalCapacityNeeded / LARGE_BARGE_VOLUME);
    const largeBargesActuallyNeeded = Math.min(largeBargesForCapacity, TOTAL_LARGE_BARGES);

    console.log(`[ANALYTICAL FLEET] Demand-based large barge calculation:`);
    console.log(`  Total capacity needed: ${(totalCapacityNeeded/1000).toFixed(0)}K gal`);
    console.log(`  Large barges that would cover this: ${largeBargesForCapacity}`);
    console.log(`  Large barges available: ${TOTAL_LARGE_BARGES}`);
    console.log(`  Large barges actually needed: ${largeBargesActuallyNeeded}`);

    // Split needed large barges between transport and storage based on capacity ratios
    if (largeBargesActuallyNeeded > 0) {
      const transportRatio = totalTransportCapacityNeeded / totalCapacityNeeded;

      // Allocate to transport first (more time-sensitive), then storage
      numLargeTransportRec = Math.min(
        Math.round(largeBargesActuallyNeeded * transportRatio),
        largeBargesActuallyNeeded
      );
      numLargeStorageRec = largeBargesActuallyNeeded - numLargeTransportRec;

      // Ensure we have at least 1 transport if we have tugs and demand
      if (numLargeTransportRec === 0 && largeBargesActuallyNeeded > 0 && peakTugs > 0) {
        numLargeTransportRec = 1;
        numLargeStorageRec = Math.max(0, largeBargesActuallyNeeded - 1);
      }
    }

    // Reduce small barges to account for large barge capacity
    const largeTransportCapacity = numLargeTransportRec * LARGE_BARGE_VOLUME;
    const largeStorageCapacity = numLargeStorageRec * LARGE_BARGE_VOLUME;

    // Small transport: reduce but keep minimum for flexibility
    const smallTransportCapacityNeeded = Math.max(0, totalTransportCapacityNeeded - largeTransportCapacity);
    numSmallTransportRec = Math.max(2, Math.ceil(smallTransportCapacityNeeded / SMALL_BARGE_VOLUME));

    // Small storage: reduce based on large storage capacity
    const smallStorageCapacityNeeded = Math.max(0, totalStorageCapacityNeeded - largeStorageCapacity);
    numSmallStorageRec = Math.max(0, Math.ceil(smallStorageCapacityNeeded / SMALL_BARGE_VOLUME));

    console.log(`[ANALYTICAL FLEET] Large barge allocation (${numLargeTransportRec + numLargeStorageRec} of ${TOTAL_LARGE_BARGES} used):`);
    console.log(`  Large transport: ${numLargeTransportRec} (${(largeTransportCapacity/1000).toFixed(0)}K gal)`);
    console.log(`  Large storage: ${numLargeStorageRec} (${(largeStorageCapacity/1000).toFixed(0)}K gal)`);
    console.log(`  Small transport: ${numSmallTransportRec} (reduced from ${peakTransportBarges})`);
    console.log(`  Small storage: ${numSmallStorageRec} (reduced from ${uniqueStorageBarges})`);
  } else {
    console.log(`[ANALYTICAL FLEET] No large barges available - using small barges only`);
  }

  // =========================================================================
  // STEP 10: Apply 10% buffer to COMBINED barge volume (rounded up to whole barges)
  // This helps with resource contention during overlapping HDD operations
  // =========================================================================
  const BARGE_BUFFER_PERCENT = 0.10;

  // Calculate total current capacity
  const currentSmallCapacity = (numSmallTransportRec + numSmallStorageRec) * SMALL_BARGE_VOLUME;
  const currentLargeCapacity = (numLargeTransportRec + numLargeStorageRec) * LARGE_BARGE_VOLUME;
  const totalCurrentCapacity = currentSmallCapacity + currentLargeCapacity;

  // Calculate buffer needed
  const targetCapacity = totalCurrentCapacity * (1 + BARGE_BUFFER_PERCENT);
  const bufferCapacityNeeded = targetCapacity - totalCurrentCapacity;

  // Add barges to meet buffer - prefer large barges if available (more efficient)
  let bufferedSmallTransport = numSmallTransportRec;
  let bufferedSmallStorage = numSmallStorageRec;
  let bufferedLargeTransport = numLargeTransportRec;
  let bufferedLargeStorage = numLargeStorageRec;

  let remainingBuffer = bufferCapacityNeeded;

  // First, try to add large barges if we have any allocated and buffer is significant
  // Note: We've already allocated all TOTAL_LARGE_BARGES, so buffer goes to small barges
  // But if someone has more large barges available, they could use them here
  if (TOTAL_LARGE_BARGES > 0 && remainingBuffer >= LARGE_BARGE_VOLUME) {
    // Large barges are already fully allocated, so buffer goes to small barges
    // This section is kept for future extensibility if more large barges become available
  }

  // Then add small barges for remaining buffer
  if (remainingBuffer > 0) {
    const smallBargesToAdd = Math.ceil(remainingBuffer / SMALL_BARGE_VOLUME);
    // Add to storage (typically the bottleneck based on HDD22 issue)
    bufferedSmallStorage += smallBargesToAdd;
    remainingBuffer = 0;
  }

  const newTotalCapacity = (bufferedSmallTransport + bufferedSmallStorage) * SMALL_BARGE_VOLUME +
                           (bufferedLargeTransport + bufferedLargeStorage) * LARGE_BARGE_VOLUME;

  console.log(`[ANALYTICAL FLEET] Applying ${BARGE_BUFFER_PERCENT * 100}% combined volume buffer:`);
  console.log(`  Base capacity: ${(totalCurrentCapacity / 1000000).toFixed(2)}M gal`);
  console.log(`  Buffer needed: ${(bufferCapacityNeeded / 1000).toFixed(0)}K gal`);
  console.log(`  New capacity: ${(newTotalCapacity / 1000000).toFixed(2)}M gal`);
  console.log(`  Small storage: ${numSmallStorageRec} → ${bufferedSmallStorage}`);
  if (bufferedLargeTransport !== numLargeTransportRec) {
    console.log(`  Large transport: ${numLargeTransportRec} → ${bufferedLargeTransport}`);
  }

  const recommendedFleetConfig = {
    numSmallTugs: peakTugs,
    numSmallTransportBarges: bufferedSmallTransport,
    numSmallStorageBarges: bufferedSmallStorage,
    numLargeTransport: bufferedLargeTransport,
    numLargeStorage: bufferedLargeStorage
  };

  return {
    success: true,
    dailyProfile,
    tugSchedule,
    storageBargeSchedule,
    transportBargeSchedule,
    summary,
    recommendedFleetConfig,
    cycleTimeAnalysis: cycleTimeCache,
    bestSourceForHDD
  };

  DISABLED - END */
}

// ============================================================================
// MOBILIZATION SCHEDULE (OLD - kept for reference)
// ============================================================================
function generateMobilizationSchedule(fleetRequirements, config) {
  const { numSmallTugs, numSmallTransportBarges, numSmallStorageBarges, numLargeTransport, numLargeStorage } = config;
  const COST = CONFIG.COST;
  const TUG_DEMOB_THRESHOLD_DAYS = CONFIG.TUG_DEMOB_THRESHOLD_DAYS;
  const SMALL_BARGES_PER_TUG = CONFIG.SMALL_BARGES_PER_TUG;

  const firstDemandDay = fleetRequirements.findIndex(d => d.totalDemand > 0);
  const lastDemandDay = fleetRequirements.length - 1 - [...fleetRequirements].reverse().findIndex(d => d.totalDemand > 0);

  if (firstDemandDay === -1) {
    return {
      tugs: [],
      transportBarges: [],
      storageBarges: [],
      largeStorageBarges: [],
      summary: { totalTugDays: 0, totalBargeDays: 0, totalLargeBargeDays: 0 },
      mobDemobCosts: { tugs: 0, smallBarges: 0, largeBarges: 0, total: 0 }
    };
  }

  const tugBreakEvenDays = (COST.mobilization.tugMobCost + COST.mobilization.tugDemobCost) / COST.tug.dayRate;
  const effectiveTugDemobThreshold = Math.max(TUG_DEMOB_THRESHOLD_DAYS, tugBreakEvenDays);

  const schedule = {
    tugs: [],
    barges: [] // All barges tracked together with their usage type
  };

  let mobDemobCosts = { tugs: 0, smallBarges: 0, largeBarges: 0 };

  const dailyTugsNeeded = fleetRequirements.map(d =>
    Math.min(d.tugsNeeded || 0, numSmallTugs + numLargeTransport)
  );

  const totalTugs = numSmallTugs + numLargeTransport;

  // ============================================================================
  // TRACK TUGS AS INDEPENDENT ASSETS
  // ============================================================================
  for (let tugIdx = 0; tugIdx < totalTugs; tugIdx++) {
    const tugId = `Tug-${tugIdx + 1}`;
    const isLargeTransportTug = tugIdx >= numSmallTugs;
    const periods = [];
    let currentPeriodStart = null;

    for (let d = 0; d <= fleetRequirements.length; d++) {
      const tugsNeededToday = d < fleetRequirements.length ? dailyTugsNeeded[d] : 0;
      // Large transport tugs mobilize whenever there's demand (they supplement small tugs)
      const thisTugNeeded = isLargeTransportTug
        ? (tugsNeededToday > 0)
        : (tugsNeededToday > tugIdx);

      if (thisTugNeeded && currentPeriodStart === null) {
        currentPeriodStart = d;
      } else if (!thisTugNeeded && currentPeriodStart !== null) {
        periods.push({ start: currentPeriodStart, end: d - 1 });
        currentPeriodStart = null;
      }
    }

    if (periods.length === 0) continue;

    // Merge periods with small gaps
    const mergedPeriods = [periods[0]];
    for (let i = 1; i < periods.length; i++) {
      const lastPeriod = mergedPeriods[mergedPeriods.length - 1];
      const gap = periods[i].start - lastPeriod.end - 1;

      if (gap <= effectiveTugDemobThreshold) {
        lastPeriod.end = periods[i].end;
      } else {
        mergedPeriods.push(periods[i]);
      }
    }

    mergedPeriods.forEach((period, periodIdx) => {
      const mobDay = period.start;
      const demobDay = period.end;
      const mobDate = fleetRequirements[mobDay].date;
      const demobDate = fleetRequirements[demobDay].date;
      const daysOnSite = demobDay - mobDay + 1;

      mobDemobCosts.tugs += COST.mobilization.tugMobCost;
      mobDemobCosts.tugs += COST.mobilization.tugDemobCost;

      schedule.tugs.push({
        id: tugId + (mergedPeriods.length > 1 ? `-P${periodIdx + 1}` : ''),
        assetType: 'tug',
        mobDay, demobDay, mobDate, demobDate, daysOnSite,
        periodNumber: periodIdx + 1,
        totalPeriods: mergedPeriods.length
      });
    });
  }

  // ============================================================================
  // TRACK SMALL TRANSPORT BARGES - DYNAMIC MOB/DEMOB BASED ON DEMAND
  // ============================================================================
  if (numSmallTransportBarges > 0 && firstDemandDay >= 0) {
    // Calculate barges needed per day based on tug demand
    const dailyBargesNeeded = dailyTugsNeeded.map(tugsNeeded =>
      Math.min(tugsNeeded * SMALL_BARGES_PER_TUG, numSmallTransportBarges)
    );

    for (let bargeIdx = 0; bargeIdx < numSmallTransportBarges; bargeIdx++) {
      const bargeId = `STB-${bargeIdx + 1}`;
      const periods = [];
      let currentPeriodStart = null;

      for (let d = 0; d <= fleetRequirements.length; d++) {
        const bargesNeededToday = d < fleetRequirements.length ? dailyBargesNeeded[d] : 0;
        const thisBargeNeeded = bargesNeededToday > bargeIdx;

        if (thisBargeNeeded && currentPeriodStart === null) {
          currentPeriodStart = d;
        } else if (!thisBargeNeeded && currentPeriodStart !== null) {
          periods.push({ start: currentPeriodStart, end: d - 1 });
          currentPeriodStart = null;
        }
      }

      if (periods.length === 0) continue;

      // Merge periods with small gaps (same threshold as tugs)
      const mergedPeriods = [periods[0]];
      for (let i = 1; i < periods.length; i++) {
        const lastPeriod = mergedPeriods[mergedPeriods.length - 1];
        const gap = periods[i].start - lastPeriod.end - 1;

        if (gap <= effectiveTugDemobThreshold) {
          lastPeriod.end = periods[i].end;
        } else {
          mergedPeriods.push(periods[i]);
        }
      }

      mergedPeriods.forEach((period, periodIdx) => {
        const mobDay = period.start;
        const demobDay = period.end;
        const mobDate = fleetRequirements[mobDay].date;
        const demobDate = fleetRequirements[demobDay].date;
        const daysOnSite = demobDay - mobDay + 1;

        mobDemobCosts.smallBarges += COST.mobilization.smallBargeMobCost;
        mobDemobCosts.smallBarges += COST.mobilization.smallBargeDemobCost;

        schedule.barges.push({
          id: bargeId + (mergedPeriods.length > 1 ? `-P${periodIdx + 1}` : ''),
          assetType: 'barge',
          bargeSize: 'small',
          usage: 'transport',
          mobDay, demobDay, mobDate, demobDate, daysOnSite,
          periodNumber: periodIdx + 1,
          totalPeriods: mergedPeriods.length
        });
      });
    }
  }

  // ============================================================================
  // TRACK LARGE TRANSPORT BARGES - DYNAMIC MOB/DEMOB BASED ON THROUGHPUT DEMAND
  // ============================================================================
  if (numLargeTransport > 0 && firstDemandDay >= 0) {
    // Large transport barges are expensive - only mobilize when throughput REQUIRES large capacity
    // Calculate based on volume throughput needed, not just tug count
    const LARGE_BARGE_VOLUME = CONFIG.LARGE_BARGE_VOLUME || 300000;
    const SMALL_BARGE_VOLUME = CONFIG.SMALL_BARGE_VOLUME || 80000;
    const smallTransportCapacity = numSmallTransportBarges * SMALL_BARGE_VOLUME;

    const dailyLargeBargesNeeded = fleetRequirements.map(d => {
      if (!d.totalDemand || d.totalDemand <= 0) return 0;
      // Calculate if small barges can handle the daily throughput
      // Assume ~2 trips per day per barge for rough throughput estimate
      const tripsPerDay = 2;
      const smallThroughputCapacity = smallTransportCapacity * tripsPerDay;

      // Only need large barges if demand exceeds small barge throughput capacity
      if (d.totalDemand <= smallThroughputCapacity) return 0;

      // Calculate how many large barges needed to cover the shortfall
      const shortfall = d.totalDemand - smallThroughputCapacity;
      const largeThroughputPerBarge = LARGE_BARGE_VOLUME * tripsPerDay;
      const largeBargesNeeded = Math.ceil(shortfall / largeThroughputPerBarge);
      return Math.min(largeBargesNeeded, numLargeTransport);
    });

    // Use shorter merge threshold for large barges - they're expensive, demob aggressively
    const largeBargeDemobThreshold = Math.min(2, effectiveTugDemobThreshold);

    for (let lbIdx = 0; lbIdx < numLargeTransport; lbIdx++) {
      const bargeId = `LTB-${lbIdx + 1}`;
      const periods = [];
      let currentPeriodStart = null;

      for (let d = 0; d <= fleetRequirements.length; d++) {
        const largeBargesNeededToday = d < fleetRequirements.length ? dailyLargeBargesNeeded[d] : 0;
        const thisBargeNeeded = largeBargesNeededToday > lbIdx;

        if (thisBargeNeeded && currentPeriodStart === null) {
          currentPeriodStart = d;
        } else if (!thisBargeNeeded && currentPeriodStart !== null) {
          periods.push({ start: currentPeriodStart, end: d - 1 });
          currentPeriodStart = null;
        }
      }

      if (periods.length === 0) continue;

      // Merge periods with small gaps - use shorter threshold for large barges
      const mergedPeriods = [periods[0]];
      for (let i = 1; i < periods.length; i++) {
        const lastPeriod = mergedPeriods[mergedPeriods.length - 1];
        const gap = periods[i].start - lastPeriod.end - 1;

        if (gap <= largeBargeDemobThreshold) {
          lastPeriod.end = periods[i].end;
        } else {
          mergedPeriods.push(periods[i]);
        }
      }

      mergedPeriods.forEach((period, periodIdx) => {
        const mobDay = period.start;
        const demobDay = period.end;
        const mobDate = fleetRequirements[mobDay].date;
        const demobDate = fleetRequirements[demobDay].date;
        const daysOnSite = demobDay - mobDay + 1;

        mobDemobCosts.largeBarges += COST.mobilization.largeBargeMobCost;
        mobDemobCosts.largeBarges += COST.mobilization.largeBargeDemobCost;

        schedule.barges.push({
          id: bargeId + (mergedPeriods.length > 1 ? `-P${periodIdx + 1}` : ''),
          assetType: 'barge',
          bargeSize: 'large',
          usage: 'transport',
          mobDay, demobDay, mobDate, demobDate, daysOnSite,
          periodNumber: periodIdx + 1,
          totalPeriods: mergedPeriods.length
        });
      });
    }
  }

  // ============================================================================
  // TRACK SMALL STORAGE BARGES AS INDEPENDENT ASSETS
  // ============================================================================
  const rigNamesForStorage = getRigNames(rigs);
  const numRigsForStorage = rigNamesForStorage.length || 1; // Avoid division by zero
  const storagePerRig = Math.floor(numSmallStorageBarges / numRigsForStorage);
  let storageBargeId = 1;

  rigNamesForStorage.forEach(rigName => {
    let firstActive = -1;
    let lastActive = -1;

    for (let d = 0; d < fleetRequirements.length; d++) {
      const rigActivity = fleetRequirements[d].rigActivity?.[rigName];
      if (rigActivity && rigActivity.isActive) {
        if (firstActive === -1) firstActive = d;
        lastActive = d;
      }
    }

    if (firstActive >= 0 && storagePerRig > 0) {
      const mobDay = firstActive;
      const demobDay = lastActive;
      const mobDate = fleetRequirements[mobDay].date;
      const demobDate = fleetRequirements[demobDay].date;
      const daysOnSite = demobDay - mobDay + 1;

      for (let b = 0; b < storagePerRig; b++) {
        mobDemobCosts.smallBarges += COST.mobilization.smallBargeMobCost;
        mobDemobCosts.smallBarges += COST.mobilization.smallBargeDemobCost;

        schedule.barges.push({
          id: `SB-${storageBargeId++}`,
          assetType: 'barge',
          bargeSize: 'small',
          usage: 'storage',
          assignedRig: rigName,
          mobDay, demobDay, mobDate, demobDate, daysOnSite
        });
      }
    }
  });

  // ============================================================================
  // TRACK LARGE STORAGE BARGES - DYNAMIC MOB/DEMOB BASED ON STORAGE CAPACITY NEEDED
  // ============================================================================
  if (numLargeStorage > 0) {
    // Determine when each large storage barge is needed based on STORAGE CAPACITY, not rig count
    // Storage capacity needed = daily demand / working hours * buffer hours
    const LARGE_BARGE_VOLUME = CONFIG.LARGE_BARGE_VOLUME || 300000;
    const BUFFER_HOURS = 6; // Hours of buffer storage to maintain
    const WORKING_HOURS = (CONFIG.DRILLING_END_HOUR || 17) - (CONFIG.DRILLING_START_HOUR || 7);

    // Calculate how many large storage barges are needed each day based on demand
    const dailyLargeStorageNeeded = fleetRequirements.map(d => {
      if (!d.totalDemand || d.totalDemand <= 0) return 0;
      // Storage capacity needed = hourly demand rate * buffer hours
      const hourlyDemand = d.totalDemand / WORKING_HOURS;
      const storageCapacityNeeded = hourlyDemand * BUFFER_HOURS;
      // How many large barges does that require?
      const largeBargesNeeded = Math.ceil(storageCapacityNeeded / LARGE_BARGE_VOLUME);
      // Cap at configured number of large storage barges
      return Math.min(largeBargesNeeded, numLargeStorage);
    });

    for (let lsIdx = 0; lsIdx < numLargeStorage; lsIdx++) {
      const bargeId = `LSB-${lsIdx + 1}`;
      const periods = [];
      let currentPeriodStart = null;

      for (let d = 0; d <= fleetRequirements.length; d++) {
        const largeBargesNeededToday = d < fleetRequirements.length ? dailyLargeStorageNeeded[d] : 0;
        // This barge is needed if its index is less than the number needed today
        const thisBargeNeeded = lsIdx < largeBargesNeededToday;

        if (thisBargeNeeded && currentPeriodStart === null) {
          currentPeriodStart = d;
        } else if (!thisBargeNeeded && currentPeriodStart !== null) {
          periods.push({ start: currentPeriodStart, end: d - 1 });
          currentPeriodStart = null;
        }
      }

      if (periods.length === 0) continue;

      // Merge periods with small gaps
      const mergedPeriods = [periods[0]];
      for (let i = 1; i < periods.length; i++) {
        const lastPeriod = mergedPeriods[mergedPeriods.length - 1];
        const gap = periods[i].start - lastPeriod.end - 1;

        if (gap <= effectiveTugDemobThreshold) {
          lastPeriod.end = periods[i].end;
        } else {
          mergedPeriods.push(periods[i]);
        }
      }

      mergedPeriods.forEach((period, periodIdx) => {
        const mobDay = period.start;
        const demobDay = period.end;
        const mobDate = fleetRequirements[mobDay].date;
        const demobDate = fleetRequirements[demobDay].date;
        const daysOnSite = demobDay - mobDay + 1;

        mobDemobCosts.largeBarges += COST.mobilization.largeBargeMobCost;
        mobDemobCosts.largeBarges += COST.mobilization.largeBargeDemobCost;

        schedule.barges.push({
          id: bargeId + (mergedPeriods.length > 1 ? `-P${periodIdx + 1}` : ''),
          assetType: 'barge',
          bargeSize: 'large',
          usage: 'storage',
          assignedRig: 'Dynamic',
          mobDay, demobDay, mobDate, demobDate, daysOnSite,
          periodNumber: periodIdx + 1,
          totalPeriods: mergedPeriods.length
        });
      });
    }
  }

  const totalTugDays = schedule.tugs.reduce((sum, t) => sum + t.daysOnSite, 0);
  const totalSmallTransportBargeDays = schedule.barges
    .filter(b => b.bargeSize === 'small' && b.usage === 'transport')
    .reduce((sum, b) => sum + b.daysOnSite, 0);
  const totalLargeTransportBargeDays = schedule.barges
    .filter(b => b.bargeSize === 'large' && b.usage === 'transport')
    .reduce((sum, b) => sum + b.daysOnSite, 0);
  const totalSmallStorageBargeDays = schedule.barges
    .filter(b => b.bargeSize === 'small' && b.usage === 'storage')
    .reduce((sum, b) => sum + b.daysOnSite, 0);
  const totalLargeStorageBargeDays = schedule.barges
    .filter(b => b.bargeSize === 'large' && b.usage === 'storage')
    .reduce((sum, b) => sum + b.daysOnSite, 0);

  mobDemobCosts.total = mobDemobCosts.tugs + mobDemobCosts.smallBarges + mobDemobCosts.largeBarges;

  const summary = {
    totalTugDays,
    totalSmallTransportBargeDays,
    totalLargeTransportBargeDays,
    totalSmallStorageBargeDays,
    totalLargeStorageBargeDays,
    firstMobDate: fleetRequirements[firstDemandDay]?.date,
    lastDemobDate: fleetRequirements[lastDemandDay]?.date,
    totalProjectDays: fleetRequirements.length,
    activeDays: lastDemandDay - firstDemandDay + 1,
    tugBreakEvenDays: tugBreakEvenDays.toFixed(1),
    effectiveTugDemobThreshold: effectiveTugDemobThreshold.toFixed(1)
  };

  return { ...schedule, summary, mobDemobCosts };
}

// ============================================================================
// ASSET LOG AGGREGATION FUNCTIONS (Single Source of Truth)
// ============================================================================

// Aggregate asset log into daily summaries
function aggregateAssetLogToDaily(assetLog, totalDays, minDate) {
  const dailyResults = {};

  // Get dynamic rig and source names from global CONFIG
  const rigNamesList = getRigNames(CONFIG.rigs);
  const sourceIdsList = CONFIG.sourceConfig ? Object.keys(CONFIG.sourceConfig) : ['source1', 'source2', 'source3'];

  // Build dynamic initialization objects
  const initRigBool = {};
  const initRigNum = {};
  rigNamesList.forEach(name => {
    initRigBool[name] = false;
    initRigNum[name] = 0;
  });
  const initSourceUtil = {};
  sourceIdsList.forEach(id => { initSourceUtil[id] = 0; });

  // Initialize all days
  for (let d = 0; d < totalDays; d++) {
    const date = new Date(minDate);
    date.setDate(date.getDate() + d);
    const dateStr = date.toISOString().split('T')[0];

    dailyResults[dateStr] = {
      date: dateStr,
      demand: 0,
      usage: 0,
      deficit: 0,
      injected: 0,
      ranDry: { ...initRigBool },
      fuelIdle: 0,
      fuelRunning: 0,
      downtimeHours: { ...initRigNum },
      storageLevel: { ...initRigNum },
      rigDemand: { ...initRigNum },
      rigInjected: { ...initRigNum },
      rigDeficit: { ...initRigNum },
      tugsActive: 0,
      smallBargesActive: 0,
      largeBargesActive: 0,
      sourceUtilization: { ...initSourceUtil }
    };
  }

  // Group events by date and hour
  const eventsByDateHour = {};
  assetLog.forEach(event => {
    const key = `${event.date}_${Math.floor(event.simHour || 0)}`;
    if (!eventsByDateHour[key]) eventsByDateHour[key] = [];
    eventsByDateHour[key].push(event);
  });

  // For each day, count active assets using the correct definition:
  // An asset is "active" on a day if it's mobilized AND has ANY non-idle log entry that day
  Object.keys(dailyResults).forEach(dateStr => {
    const dayResult = dailyResults[dateStr];

    // Get all log entries for this day (across all hours)
    const dayEvents = assetLog.filter(e => e.date === dateStr);

    // Count tugs: mobilized + has any non-idle entry
    const activeTugs = new Set();
    dayEvents.filter(e => e.assetType === 'Tug').forEach(e => {
      if (e.status !== 'idle') {
        activeTugs.add(e.assetId);
      }
    });
    dayResult.tugsActive = activeTugs.size;

    // Count small barges: mobilized + has any non-idle entry
    const activeSmallBarges = new Set();
    dayEvents.filter(e => e.assetType === 'Barge (small)').forEach(e => {
      if (e.status !== 'idle') {
        activeSmallBarges.add(e.assetId);
      }
    });
    dayResult.smallBargesActive = activeSmallBarges.size;

    // Count large barges: mobilized + has any non-idle entry
    const activeLargeBarges = new Set();
    dayEvents.filter(e => e.assetType === 'Barge (large)').forEach(e => {
      if (e.status !== 'idle') {
        activeLargeBarges.add(e.assetId);
      }
    });
    dayResult.largeBargesActive = activeLargeBarges.size;

    // Debug logging for specific date
    if (dateStr === '2026-07-12') {
      console.log(`[ACTIVE COUNT DEBUG] ${dateStr}: ${activeLargeBarges.size} active large barges: ${Array.from(activeLargeBarges).join(', ')}`);
      const largeBargeEvents = dayEvents.filter(e => e.assetType === 'Barge (large)');
      console.log(`[ACTIVE COUNT DEBUG] Total large barge events: ${largeBargeEvents.length}`);
      const byBarge = {};
      largeBargeEvents.forEach(e => {
        if (!byBarge[e.assetId]) byBarge[e.assetId] = [];
        byBarge[e.assetId].push(e.status);
      });
      Object.entries(byBarge).forEach(([id, statuses]) => {
        const hasNonIdle = statuses.some(s => s !== 'idle');
        console.log(`[ACTIVE COUNT DEBUG]   ${id}: ${statuses.join(', ')} => ${hasNonIdle ? 'ACTIVE' : 'NOT ACTIVE'}`);
      });
    }

    // Extract water injection events (note: storage barge 'filling' events don't have waterVolume field, tracked elsewhere)
    const pumpEvents = dayEvents.filter(e => e.status === 'filling' && e.waterVolume);
    pumpEvents.forEach(e => {
      dayResult.injected += e.waterVolume || 0;
      if (e.targetRig && dayResult.rigInjected[e.targetRig] !== undefined) {
        dayResult.rigInjected[e.targetRig] += e.waterVolume || 0;
      }
    });
  });

  return dailyResults;
}

// Build mobilization schedule from asset log only
function buildMobilizationScheduleFromAssetLog(assetLog, fleetConfig, minDate, costConfig) {
  const schedule = { tugs: [], barges: [] };
  const mobDemobCosts = { tugs: 0, smallBarges: 0, largeBarges: 0 };

  // Find last date in asset log for default demob date
  const lastEventDate = assetLog.length > 0
    ? assetLog[assetLog.length - 1].date
    : new Date(minDate).toISOString().split('T')[0];

  // Group events by asset
  const assetEvents = {};
  assetLog.forEach(event => {
    const key = `${event.assetType}_${event.assetId}`;
    if (!assetEvents[key]) assetEvents[key] = [];
    assetEvents[key].push(event);
  });

  console.log(`[MOB SCHEDULE] Building from asset log: ${assetLog.length} total events, ${Object.keys(assetEvents).length} unique assets`);

  // Helper to get date string from day offset
  const getDateStrFromDay = (dayOffset) => {
    const date = new Date(minDate);
    date.setDate(date.getDate() + dayOffset);
    return date.toISOString().split('T')[0];
  };

  // For each asset, find first and last operational event
  Object.entries(assetEvents).forEach(([key, events]) => {
    const [assetTypeRaw, assetId] = key.split('_');

    // Filter for operational events (ACTUAL WORK, not idle at source)
    const operationalEvents = events.filter(e => {
      if (!e.status) return false;

      // Only count as operational if ACTUALLY WORKING:
      // - Moving (en-route-loaded, en-route-empty, en-route-storage)
      // - At a rig/HDD (at-rig, arrived-at-rig, stationed)
      // - Actively filling/loading/unloading
      // - First mobilization event
      //
      // DO NOT count as operational:
      // - 'idle' at source (sitting unused - should demob)
      // - 'queued' (waiting at source - not actively needed)
      const isActualWork = [
        'en-route-loaded', 'en-route-empty', 'en-route-storage',
        'at-rig', 'arrived-at-rig', 'stationed',
        'filling', 'loading', 'unloading',
        'mobilized'
      ].includes(e.status);

      // Special case: 'idle' at an HDD is operational (providing storage capacity)
      // 'idle' at a source is NOT operational (sitting unused)
      if (e.status === 'idle') {
        const location = e.location || '';
        const isAtHDD = location.includes('HDD') || location.includes('crossing');
        return isAtHDD;  // Only count idle if at an HDD
      }

      return isActualWork;
    });

    if (operationalEvents.length === 0) {
      console.log(`[MOB SCHEDULE] Skipping ${key}: no operational events (${events.length} total events)`);
      return;  // Never used
    }

    // Find first and last use
    const firstUse = operationalEvents[0];

    // For demob: use the LAST operational event (ignore premature 'demobilizing' logs)
    // The demobilization check logs 'demobilizing' when a tug is idle for 4+ days,
    // but tugs can be used again after that, so we need the actual last activity
    const lastOperationalEvent = operationalEvents[operationalEvents.length - 1];

    // If last operational event is still relatively recent OR is the only event (mobilization),
    // assume asset stays on-site until end of simulation
    const effectiveLastUse = operationalEvents.length === 1 && operationalEvents[0].status === 'mobilized'
      ? { date: lastEventDate } // Mobilized but never worked - stay until end
      : lastOperationalEvent;

    const mobDay = Math.floor((new Date(firstUse.date) - new Date(minDate)) / (1000 * 60 * 60 * 24));
    // Demob the day AFTER last use (asset works through the last use date)
    const lastUseDay = Math.floor((new Date(effectiveLastUse.date) - new Date(minDate)) / (1000 * 60 * 60 * 24));
    const demobDay = lastUseDay + 1;  // Demobilize the day after last activity
    const daysOnSite = demobDay - mobDay + 1;

    // Add to schedule based on asset type
    if (assetTypeRaw === 'Tug') {
      schedule.tugs.push({
        id: `Tug-${assetId}`,
        assetType: 'tug',
        mobDay,
        demobDay,
        mobDate: getDateStrFromDay(mobDay),
        demobDate: getDateStrFromDay(demobDay),
        daysOnSite
      });
      mobDemobCosts.tugs += (costConfig.tugMobCost || 0) + (costConfig.tugDemobCost || 0);
    } else if (assetTypeRaw.startsWith('Barge')) {
      const bargeSize = assetTypeRaw.includes('large') ? 'large' : 'small';

      // Determine if barge is transport or storage based on location patterns
      // Transport barges move between source and rigs
      // Storage barges stay at one rig
      const locations = new Set(events.map(e => e.location));
      const hasSourceLocation = Array.from(locations).some(loc => loc && (loc.includes('source') || loc.startsWith('at source')));
      const hasRigLocation = Array.from(locations).some(loc => loc && (loc.includes('HDD') || loc.startsWith('at HDD')));

      // If barge visits both sources AND rigs, it's transport
      // If it only visits rigs (or only logs as 'on-site'), it's storage
      const usage = (hasSourceLocation && hasRigLocation) ? 'transport' : 'storage';

      schedule.barges.push({
        id: `Barge-${assetId}`,
        assetType: 'barge',
        bargeSize,
        usage,
        mobDay,
        demobDay,
        mobDate: getDateStrFromDay(mobDay),
        demobDate: getDateStrFromDay(demobDay),
        daysOnSite
      });

      const costKey = bargeSize === 'large' ? 'largeBarges' : 'smallBarges';
      if (bargeSize === 'large') {
        mobDemobCosts[costKey] += (costConfig.largeBargeMobCost || 0) + (costConfig.largeBargeDemobCost || 0);
      } else {
        mobDemobCosts[costKey] += (costConfig.smallBargeMobCost || 0) + (costConfig.smallBargeDemobCost || 0);
      }
    }
  });

  // Calculate totals by type
  const totalTugDays = schedule.tugs.reduce((sum, t) => sum + t.daysOnSite, 0);

  const smallTransportBarges = schedule.barges.filter(b => b.bargeSize === 'small' && b.usage === 'transport');
  const smallStorageBarges = schedule.barges.filter(b => b.bargeSize === 'small' && b.usage === 'storage');
  const largeTransportBarges = schedule.barges.filter(b => b.bargeSize === 'large' && b.usage === 'transport');
  const largeStorageBarges = schedule.barges.filter(b => b.bargeSize === 'large' && b.usage === 'storage');

  const totalSmallTransportBargeDays = smallTransportBarges.reduce((sum, b) => sum + b.daysOnSite, 0);
  const totalSmallStorageBargeDays = smallStorageBarges.reduce((sum, b) => sum + b.daysOnSite, 0);
  const totalLargeTransportBargeDays = largeTransportBarges.reduce((sum, b) => sum + b.daysOnSite, 0);
  const totalLargeStorageBargeDays = largeStorageBarges.reduce((sum, b) => sum + b.daysOnSite, 0);

  // Calculate total mobilization cost
  const totalMobDemobCost = mobDemobCosts.tugs + mobDemobCosts.smallBarges + mobDemobCosts.largeBarges;

  console.log(`[MOB SCHEDULE] Built schedule: ${schedule.tugs.length} tugs, ${schedule.barges.length} barges (${schedule.barges.filter(b => b.bargeSize === 'small').length} small, ${schedule.barges.filter(b => b.bargeSize === 'large').length} large)`);

  return {
    tugs: schedule.tugs,
    barges: schedule.barges,
    summary: {
      totalTugDays,
      totalSmallTransportBargeDays,
      totalSmallStorageBargeDays,
      totalLargeTransportBargeDays,
      totalLargeStorageBargeDays,
      totalSmallBargeDays: totalSmallTransportBargeDays + totalSmallStorageBargeDays,
      totalLargeBargeDays: totalLargeTransportBargeDays + totalLargeStorageBargeDays,
      // Asset counts by role
      numTugs: schedule.tugs.length,
      numSmallTransportBarges: smallTransportBarges.length,
      numSmallStorageBarges: smallStorageBarges.length,
      numLargeTransportBarges: largeTransportBarges.length,
      numLargeStorageBarges: largeStorageBarges.length
    },
    mobDemobCosts: {
      ...mobDemobCosts,
      total: totalMobDemobCost
    }
  };
}

// ============================================================================
// BUILD TUG JOURNEYS FROM ASSET LOG (For Map Animation)
// ============================================================================

function buildTugJourneysFromAssetLog(assetLog) {
  const journeys = [];

  // Group asset log entries by tug
  const tugEvents = {};
  assetLog.forEach(event => {
    if (event.assetType === 'Tug') {
      const tugId = event.assetId;
      if (!tugEvents[tugId]) tugEvents[tugId] = [];
      tugEvents[tugId].push(event);
    }
  });

  // For each tug, build journey segments from status transitions
  Object.entries(tugEvents).forEach(([tugId, events]) => {
    events.sort((a, b) => a.simHour - b.simHour);

    let currentJourney = null;

    events.forEach((event, idx) => {
      const nextEvent = events[idx + 1];

      // Helper to extract crossing from location string
      const extractCrossing = (location) => {
        if (!location) return null;
        const match = location.match(/HDD(\d+)/);
        return match ? `HDD${match[1]}` : null;
      };

      // Helper to extract source from location string
      const extractSource = (location) => {
        if (!location) return null;
        const match = location.match(/source(\d+)/);
        return match ? `source${match[1]}` : null;
      };

      // Helper to find previous source from earlier events
      const findPreviousSource = (upToIdx) => {
        for (let i = upToIdx - 1; i >= 0; i--) {
          const src = extractSource(events[i].location);
          if (src) return src;
        }
        return 'source1'; // Default
      };

      // START traveling-loaded journey (water transport)
      if (event.status === 'en-route-loaded' && !currentJourney) {
        const targetCrossing = event.targetCrossing || extractCrossing(event.location);
        const sourceId = event.sourceId || findPreviousSource(idx);

        currentJourney = {
          tugId: tugId,
          state: 'traveling-loaded',
          startHour: event.simHour,
          startDistance: 0,
          targetRig: event.targetRig || event.details?.targetRig,
          targetCrossing,
          sourceId,
          startDate: event.date,
          route: null,  // Will be populated when we have route data
          bargeIds: event.attachedBargeIds || []
        };
      }

      // START storage delivery journey
      if (event.status === 'en-route-storage' && !currentJourney) {
        const targetCrossing = event.targetCrossing || extractCrossing(event.location);
        const sourceId = event.sourceId || findPreviousSource(idx);

        currentJourney = {
          tugId: tugId,
          state: 'traveling-storage',
          startHour: event.simHour,
          startDistance: 0,
          targetRig: event.targetRig || event.details?.targetRig,
          targetCrossing,
          sourceId,
          startDate: event.date,
          route: null,
          purpose: 'storage-delivery',
          bargeIds: event.attachedBargeIds || []
        };
      }

      // START pickup journey (tug going to another source to pick up barges)
      if (event.status === 'en-route-pickup' && !currentJourney) {
        const targetCrossing = event.targetCrossing || extractCrossing(event.location);
        const sourceId = findPreviousSource(idx);

        currentJourney = {
          tugId: tugId,
          state: 'traveling-pickup',
          startHour: event.simHour,
          startDistance: 0,
          targetCrossing,
          sourceId,
          startDate: event.date,
          route: null,
          purpose: 'barge-pickup'
        };
      }

      // START direct HDD-to-HDD delivery journey (water transport between HDDs)
      if (event.status === 'direct-hdd-delivery' && !currentJourney) {
        const targetCrossing = event.targetCrossing || extractCrossing(event.location);
        const sourceId = event.sourceId;  // Origin HDD

        currentJourney = {
          tugId: tugId,
          state: 'direct-hdd-delivery',
          startHour: event.simHour,
          startDistance: 0,
          targetRig: event.targetRig || event.details?.targetRig,
          targetCrossing,
          sourceId,  // This is the origin HDD, not a water source
          startDate: event.date,
          route: null,  // Will use HDD-to-HDD centerline route
          bargeIds: event.attachedBargeIds || [],
          purpose: 'hdd-to-hdd-direct'
        };
      }

      // END traveling-loaded, START at-rig journey
      if (event.status === 'at-rig' && currentJourney && currentJourney.state === 'traveling-loaded') {
        currentJourney.endHour = event.simHour;
        currentJourney.endDistance = 0;  // Distance calculation done later with route data
        currentJourney.endDate = event.date;
        journeys.push(currentJourney);

        currentJourney = {
          tugId: tugId,
          state: 'at-rig',
          startHour: event.simHour,
          endHour: nextEvent?.simHour || event.simHour + 1,
          startDistance: 0,
          endDistance: 0,
          targetRig: event.targetRig || currentJourney.targetRig,
          targetCrossing: currentJourney.targetCrossing,
          sourceId: currentJourney.sourceId,
          startDate: event.date,
          endDate: nextEvent?.date || event.date
        };
      }

      // END direct-hdd-delivery journey, START at-rig journey (same as traveling-loaded)
      if (event.status === 'at-rig' && currentJourney && currentJourney.state === 'direct-hdd-delivery') {
        currentJourney.endHour = event.simHour;
        currentJourney.endDistance = 0;
        currentJourney.endDate = event.date;
        journeys.push(currentJourney);

        currentJourney = {
          tugId: tugId,
          state: 'at-rig',
          startHour: event.simHour,
          endHour: nextEvent?.simHour || event.simHour + 1,
          startDistance: 0,
          endDistance: 0,
          targetRig: event.targetRig || currentJourney.targetRig,
          targetCrossing: currentJourney.targetCrossing,
          sourceId: currentJourney.sourceId,  // Origin HDD
          startDate: event.date,
          endDate: nextEvent?.date || event.date,
          purpose: 'hdd-to-hdd-direct-arrived'
        };
      }

      // END traveling-storage journey (barges stationed at HDD)
      // Storage journeys end when barges are stationed, tug returns empty
      if ((event.status === 'idle' || event.status === 'en-route-empty') &&
          currentJourney && currentJourney.state === 'traveling-storage') {
        currentJourney.endHour = event.simHour;
        currentJourney.endDistance = 0;
        currentJourney.endDate = event.date;
        journeys.push(currentJourney);

        // If transitioning to en-route-empty, start that journey
        if (event.status === 'en-route-empty') {
          const returnSource = extractSource(event.location) || findPreviousSource(idx);
          currentJourney = {
            tugId: tugId,
            state: 'traveling-empty',
            startHour: event.simHour,
            startDistance: 0,
            targetCrossing: event.targetCrossing,
            sourceId: returnSource,
            startDate: event.date,
            route: null
          };
        } else {
          currentJourney = null;
        }
      }

      // END traveling-pickup journey (arrived at source to pick up barges)
      // Pickup journeys transition to storage delivery
      if (event.status === 'en-route-storage' && currentJourney && currentJourney.state === 'traveling-pickup') {
        currentJourney.endHour = event.simHour;
        currentJourney.endDistance = 0;
        currentJourney.endDate = event.date;
        journeys.push(currentJourney);

        // Now start the storage delivery leg
        const targetCrossing = event.targetCrossing || extractCrossing(event.location);
        const sourceId = extractSource(event.location) || currentJourney.targetCrossing; // The pickup location becomes the source

        currentJourney = {
          tugId: tugId,
          state: 'traveling-storage',
          startHour: event.simHour,
          startDistance: 0,
          targetRig: event.targetRig || event.details?.targetRig,
          targetCrossing,
          sourceId,
          startDate: event.date,
          route: null,
          purpose: 'storage-delivery'
        };
      }

      // END at-rig, START traveling-empty journey (for transport deliveries)
      // Skip if we just handled a storage delivery (currentJourney.state === 'traveling-empty' already set)
      if (event.status === 'en-route-empty' && currentJourney &&
          currentJourney.state !== 'traveling-empty') {
        if (currentJourney.state === 'at-rig') {
          currentJourney.endHour = event.simHour;
          journeys.push(currentJourney);
        }

        const targetCrossing = currentJourney.targetCrossing;
        const sourceId = currentJourney.sourceId;

        currentJourney = {
          tugId: tugId,
          state: 'traveling-empty',
          startHour: event.simHour,
          startDistance: 0,  // Will be filled with route distance
          targetRig: currentJourney.targetRig,
          targetCrossing,
          sourceId,
          startDate: event.date,
          route: null  // Will be reversed route
        };
      }

      // END traveling-empty (arrived at source), START at-source waiting
      if ((event.status === 'idle' && event.location && event.location.includes('source')) &&
          currentJourney && currentJourney.state === 'traveling-empty') {
        currentJourney.endHour = event.simHour;
        currentJourney.endDistance = 0;
        currentJourney.endDate = event.date;
        journeys.push(currentJourney);

        // Start at-source journey (waiting/filling period)
        const sourceId = extractSource(event.location) || currentJourney.sourceId;
        currentJourney = {
          tugId: tugId,
          state: 'at-source',
          startHour: event.simHour,
          endHour: nextEvent?.simHour || event.simHour,
          startDistance: 0,
          endDistance: 0,
          sourceId,
          startDate: event.date,
          endDate: nextEvent?.date || event.date
        };
      }

      // END at-source (tug dispatched), START next journey
      if ((event.status === 'en-route-loaded' || event.status === 'en-route-storage') &&
          currentJourney && currentJourney.state === 'at-source') {
        currentJourney.endHour = event.simHour;
        currentJourney.endDate = event.date;
        journeys.push(currentJourney);
        currentJourney = null; // Will be picked up by the loaded/storage handlers
      }
    });
  });

  return journeys;
}

// ============================================================================
// MAIN SIMULATION FUNCTION
// ============================================================================
function runSimulation(fleetConfig, userConfig = {}) {
  // MEMORY OPTIMIZATION: Suppress verbose logging during optimization
  // This prevents memory exhaustion from millions of log strings
  const originalConsoleLog = console.log;
  if (userConfig._optimizationMode) {
    SUPPRESS_VERBOSE_LOGGING = true;
    // Replace console.log with no-op to prevent string creation
    console.log = function() {};
  }

  // Helper to restore console.log and return result
  const restoreAndReturn = (result) => {
    if (userConfig._optimizationMode) {
      console.log = originalConsoleLog;
    }
    return result;
  };

  // Merge user config with defaults and validate
  const rawCfg = { ...CONFIG, ...userConfig };
  const cfg = validateConfig(rawCfg, 'runSimulation');
  const { numSmallTugs, numSmallTransportBarges, numSmallStorageBarges, numLargeTransport, numLargeStorage } = fleetConfig;

  const hddSchedule = cfg.hddSchedule;
  const sourceConfig = cfg.sourceConfig;
  const rigs = cfg.rigs;
  const COST = cfg.COST;

  const SMALL_BARGE_VOLUME = cfg.SMALL_BARGE_VOLUME;
  const SMALL_BARGES_PER_TUG = cfg.SMALL_BARGES_PER_TUG;
  const LARGE_BARGES_PER_TUG = cfg.LARGE_BARGES_PER_TUG;
  const LARGE_BARGE_VOLUME = cfg.LARGE_BARGE_VOLUME;
  const loadedSpeedSmall = cfg.loadedSpeedSmall;
  const loadedSpeedLarge = cfg.loadedSpeedLarge;
  const emptySpeed = cfg.emptySpeed;
  const switchoutTime = cfg.switchoutTime;
  const hookupTime = cfg.hookupTime;
  const pumpRate = cfg.pumpRate;
  const DRILLING_START_HOUR = cfg.DRILLING_START_HOUR;
  const DRILLING_END_HOUR = cfg.DRILLING_END_HOUR;
  const MIN_STORAGE_CAPACITY_PER_RIG = cfg.MIN_STORAGE_CAPACITY_PER_RIG || 150000;

  const PUMP_TO_STORAGE_RATE = pumpRate * 60;
  const TIME_STEP = 5 / 60;

  // Build distance matrices - pass flow rates so best-source considers fill time, not just distance
  const sourceFlowRates = {};
  Object.keys(sourceConfig).forEach(srcId => {
    sourceFlowRates[srcId] = sourceConfig[srcId]?.flowRate || 400;
  });
  const { distanceMatrix, hddToHddMatrix, bestSourceForHDD } = buildDistanceMatrices(hddSchedule, sourceFlowRates);

  // Date calculations
  // Note: rigDown is when rig-down STARTS, not ends. Add buffer for drilling completion + demob.
  const allDates = hddSchedule.flatMap(h => [h.start, h.rigDown]);
  const maxDate = new Date(Math.max(...allDates.map(d => new Date(d))));
  maxDate.setDate(maxDate.getDate() + 14); // Add 14-day buffer for rig-down completion + demob + final deliveries

  // Calculate minDate based on earliest rigUp START date
  // This gives time to pre-fill storage barges at source before transporting to HDD
  const earliestRigUpStart = new Date(Math.min(...hddSchedule.map(h => new Date(h.start))));
  const minDate = new Date(earliestRigUpStart);
  // No buffer needed - simulation starts when first rig mobilizes
  // This allows storage barges to be filled during rigUp period
  console.log(`[SIM START] Earliest rigUp start: ${earliestRigUpStart.toISOString().split('T')[0]}, simulation minDate: ${minDate.toISOString().split('T')[0]}`);
  const totalDays = Math.ceil((maxDate - minDate) / (1000 * 60 * 60 * 24)) + 1;
  const totalHours = totalDays * 24;

  // Calculate storage capacity per rig - dynamically based on actual rig count
  const rigNames = getRigNames(rigs);
  const numRigs = rigNames.length || 1; // Avoid division by zero
  const smallStoragePerRig = Math.floor(numSmallStorageBarges / numRigs);
  const smallStorageCapacityPerRig = smallStoragePerRig * SMALL_BARGE_VOLUME;

  // Track how many small storage barges to create (will be unified with transport barges later)
  let smallStorageBargeCounter = 1;
  const numSmallStorageBargesTotal = numSmallStorageBarges;

  // Cost tracking - dynamically initialize downtime hours for all rigs
  const downtimeHoursInit = {};
  rigNames.forEach(name => { downtimeHoursInit[name] = 0; });
  // Initialize source tracking dynamically based on configured sources
  const sourceIds = Object.keys(sourceConfig);
  const tripsPerSourceInit = {};
  const waterVolumeBySourceInit = {};
  sourceIds.forEach(id => {
    tripsPerSourceInit[id] = 0;
    waterVolumeBySourceInit[id] = 0;
  });
  const costTracking = {
    fuelIdleGallons: 0, fuelRunningGallons: 0,
    tugIdleHours: 0, tugRunningHours: 0, tugAtRigHours: 0,
    downtimeHours: downtimeHoursInit,
    totalTrips: 0,
    tripsPerSource: tripsPerSourceInit,
    singleStopTrips: 0, multiStopTrips: 0, totalStops: 0,
    directHDDTrips: 0,  // NEW: Track direct HDD-to-HDD deliveries
    directHDDWaterDelivered: 0,  // NEW: Track water delivered via direct routing
    directHDDTimeSaved: 0,  // NEW: Track estimated time saved
    throttledStandbyEvents: 0,  // NEW: Track throttled standby entries
    throttledPumpEvents: 0,  // NEW: Track throttled pump operations
    throttleBreakEvents: 0,  // NEW: Track throttle breaks to urgent deliveries
    waterVolumeBySource: waterVolumeBySourceInit
  };

  // Helper functions
  const getSourceOperatingHours = (sourceId) => {
    const cfg = sourceConfig[sourceId];
    const hours = cfg?.hoursPerDay || 24;
    if (hours >= 24) return { start: 0, end: 24 };
    return { start: 12 - (hours / 2), end: 12 + (hours / 2) };
  };

  const isSourceOperating = (sourceId, hourOfDay) => {
    const { start, end } = getSourceOperatingHours(sourceId);
    return hourOfDay >= start && hourOfDay < end;
  };

  const getDistance = (sourceId, hddCrossing) => distanceMatrix[sourceId]?.[hddCrossing] || 15;

  // Find best source based on THROUGHPUT (cycle time), not just distance
  // Cycle time = fill time + travel time to HDD + travel time back
  // Source with 4x flow rate saves 75% of fill time, often much more than travel difference
  const getBestSourceByThroughput = (hddCrossing, bargeVolume = SMALL_BARGE_VOLUME) => {
    let bestSource = 'source1';
    let bestCycleTime = Infinity;
    let analysis = [];

    Object.keys(sourceConfig).forEach(sourceId => {
      const distance = getDistance(sourceId, hddCrossing);
      const flowRate = sourceConfig[sourceId].flowRate || 400; // GPM

      // Fill time in hours: volume (gal) / (flowRate (GPM) * 60 min/hr)
      const fillTimeHrs = bargeVolume / (flowRate * 60);
      // Round trip travel time in hours: (2 * distance) / speed
      const travelTimeHrs = (2 * distance) / loadedSpeedSmall;
      // Total cycle time
      const cycleTimeHrs = fillTimeHrs + travelTimeHrs;

      analysis.push({
        sourceId,
        distance,
        flowRate,
        fillTimeHrs: fillTimeHrs.toFixed(2),
        travelTimeHrs: travelTimeHrs.toFixed(2),
        cycleTimeHrs: cycleTimeHrs.toFixed(2)
      });

      if (cycleTimeHrs < bestCycleTime) {
        bestCycleTime = cycleTimeHrs;
        bestSource = sourceId;
      }
    });

    return { bestSource, bestCycleTime, analysis };
  };

  const getRigStatus = (rigName, dateStr) => {
    const rigConfig = rigs[rigName];
    const rigSchedule = hddSchedule.filter(h => h.rig === rigName);
    for (const hdd of rigSchedule) {
      if (dateStr >= hdd.start && dateStr <= hdd.rigDown) {
        let stage = 'Idle';
        if (dateStr <= hdd.rigUp) stage = 'RigUp';
        else if (dateStr <= hdd.pilot) stage = 'Pilot';
        else if (dateStr <= hdd.ream) stage = 'Ream';
        else if (dateStr <= hdd.swab) stage = 'Swab';
        else if (dateStr <= hdd.pull) stage = 'Pull';
        else stage = 'RigDown';

        let hourlyRate = 0;
        if (stage === 'Pilot') hourlyRate = rigConfig.pilot;
        else if (stage === 'Ream') hourlyRate = rigConfig.ream;
        else if (stage === 'Swab') hourlyRate = rigConfig.swab;
        else if (stage === 'Pull') hourlyRate = rigConfig.pull;

        if (new Date(dateStr).getDay() === 0 && !hdd.worksSundays) hourlyRate = 0;
        return { stage, hourlyRate, hdd: hdd.crossing, isActive: stage !== 'Idle' };
      }
    }
    return { stage: 'Idle', hourlyRate: 0, hdd: '', isActive: false };
  };

  const getDateStrFromTime = (simTime) => {
    const date = new Date(minDate);
    date.setTime(date.getTime() + simTime * 60 * 60 * 1000);
    return date.toISOString().split('T')[0];
  };

  const getHourOfDayFromTime = (simTime) => Math.floor(simTime % 24);
  const isDrillingHour = (simTime) => {
    const hour = getHourOfDayFromTime(simTime);
    return hour >= DRILLING_START_HOUR && hour < DRILLING_END_HOUR;
  };
  const getTravelTime = (distance, loaded, isLargeBarge) => {
    const speed = loaded ? (isLargeBarge ? loadedSpeedLarge : loadedSpeedSmall) : emptySpeed;
    return distance / speed;
  };

  // Helper: Determine if tug has large barges attached
  const tugHasLargeBarges = (tug) => {
    const attached = kernel.getAttachedBarges(tug.id);
    if (attached.length === 0) return false;
    const firstBarge = state.barges.find(b => b.id === attached[0]);
    return firstBarge && firstBarge.type === 'large';
  };

  // Track how many large storage barges to create (will be unified with transport barges later)
  let largeStorageBargeCounter = 1;
  const numLargeStorageBargesTotal = numLargeStorage;

  // State
  const state = {
    storage: {
      Blue: { smallBargeLevel: 0, smallBargeCapacity: smallStorageCapacityPerRig },
      Green: { smallBargeLevel: 0, smallBargeCapacity: smallStorageCapacityPerRig },
      Red: { smallBargeLevel: 0, smallBargeCapacity: smallStorageCapacityPerRig }
    },
    sources: {},
    barges: [],  // Unified array for ALL barges (transport + storage)
    tugs: [],
    hddStorage: {},  // NEW: Track stationed barges at each HDD crossing
    nextTugId: 1,
    nextSmallBargeId: smallStorageBargeCounter,  // Start counter
    nextLargeBargeId: largeStorageBargeCounter,   // Start counter
    rigQueues: {
      Blue: { pumping: null, queue: [] },
      Green: { pumping: null, queue: [] },
      Red: { pumping: null, queue: [] }
    }
  };

  // Asset log for tracking significant asset actions (must be defined before use)
  const assetLog = [];
  const lastLoggedState = {}; // Track last logged state to avoid duplicates

  // Cost-per-gallon history tracking for each source-destination pair
  const costPerGallonHistory = {};

  // Detailed ran dry event tracking
  const ranDryEvents = [];
  const activeRanDryPeriods = {}; // Track ongoing ran dry periods by rig

  // Dynamic mobilization tracking for post-run optimization
  const dynamicMobilizations = []; // Track each tug/barge added during simulation

  // Helper: Get coordinates for a location (source or HDD crossing)
  const getLocationCoordinates = (location) => {
    if (!location) return null;

    // Handle various location string formats
    let normalizedLocation = location;

    // Strip common prefixes
    if (location.startsWith('at ')) {
      normalizedLocation = location.substring(3);
    } else if (location.startsWith('to ')) {
      normalizedLocation = location.substring(3);
    } else if (location.startsWith('returning to ')) {
      normalizedLocation = location.substring(13); // "returning to " is 13 chars
    }

    // Handle HDD-to-HDD format like "HDD17 -> HDD21" (used in direct-hdd-delivery)
    // Return the origin HDD coordinates (start of journey)
    const hddToHddMatch = normalizedLocation.match(/^(HDD\d+)\s*->\s*(HDD\d+)/i);
    if (hddToHddMatch) {
      normalizedLocation = hddToHddMatch[1]; // Use origin HDD
    }

    // Check if it's a source location
    if (MULTI_SOURCE_ROUTES[normalizedLocation]) {
      return MULTI_SOURCE_ROUTES[normalizedLocation].location;
    }

    // Check if it's an HDD crossing - extract coordinates from route endpoints
    for (const [sourceId, sourceData] of Object.entries(MULTI_SOURCE_ROUTES)) {
      if (sourceData.routes && sourceData.routes[normalizedLocation]) {
        const route = sourceData.routes[normalizedLocation];
        // HDD location is at the end of the route
        if (route && route.length > 0) {
          const endPoint = route[route.length - 1];
          return { lat: endPoint[1], lng: endPoint[0] };
        }
      }
    }

    return null; // Location not found
  };

  // ============================================================================
  // KERNEL MODULE - Single Source of Truth for Tug-Barge Relationships
  // ============================================================================
  //
  // This kernel owns all tug-barge attachment state. The canonical truth is:
  //   barge.assignedTugId - THE source of truth for which tug owns a barge
  //
  // All other views (tug's attached list, source pools) are DERIVED from kernel indexes.
  // Direct mutation of attachment fields outside the kernel is FORBIDDEN.
  //
  const kernel = (() => {
    // Private indexes - maintained ONLY by kernel mutations
    const _attachedByTug = new Map();      // tugId -> Set<bargeId>
    const _freeBySourceAndType = {};       // sourceId -> { small: Set, large: Set }
    const _stationedByHDD = new Map();     // hddId -> Set<bargeId>
    const _fillRequestTimes = new Map();   // bargeId -> simTime (for queue-less FIFO)

    // Initialize indexes from state (call once after barge/tug creation)
    function initializeIndexes(tugs, barges, sourceIds) {
      _attachedByTug.clear();
      _stationedByHDD.clear();
      _fillRequestTimes.clear();

      // Initialize source pools
      sourceIds.forEach(sourceId => {
        _freeBySourceAndType[sourceId] = { small: new Set(), large: new Set() };
      });

      // Initialize tug attachment indexes
      tugs.forEach(tug => {
        _attachedByTug.set(tug.id, new Set(tug.attachedBargeIds || []));
      });

      // Initialize barge location indexes
      let addedToPool = 0;
      barges.forEach(barge => {
        if (barge.status === 'stationed' && barge.assignedHDD) {
          // Stationed at HDD
          if (!_stationedByHDD.has(barge.assignedHDD)) {
            _stationedByHDD.set(barge.assignedHDD, new Set());
          }
          _stationedByHDD.get(barge.assignedHDD).add(barge.id);
        } else if (barge.currentLocation &&
                   barge.currentLocation.startsWith('source') &&
                   !barge.assignedTugId) {
          // At source and not attached to tug -> in free pool
          const sourcePool = _freeBySourceAndType[barge.currentLocation];
          if (sourcePool) {
            sourcePool[barge.type].add(barge.id);
            addedToPool++;
          }
          // Set initial fill request time for ALL barges needing fill (transport AND storage)
          // Storage barges MUST be pre-filled before dispatch to match transport schedule assumptions
          // The transport schedule assumes storage arrives with 80% fill (line 4391-4394)
          if (barge.fillLevel < barge.volume) {
            _fillRequestTimes.set(barge.id, 0); // Time 0 for initial barges
          }
        }
      });
      console.log(`[KERNEL INIT] Added ${addedToPool} barges to free pools`);

      // Log pool sizes
      Object.entries(_freeBySourceAndType).forEach(([src, pools]) => {
        console.log(`[KERNEL INIT] ${src}: small=${pools.small.size}, large=${pools.large.size}`);
      });
    }

    // -------------------------------------------------------------------------
    // READ-ONLY INDEX ACCESSORS
    // -------------------------------------------------------------------------

    function getAttachedBarges(tugId) {
      const set = _attachedByTug.get(tugId);
      return set ? Array.from(set) : [];
    }

    function getAttachedBargeCount(tugId) {
      const set = _attachedByTug.get(tugId);
      return set ? set.size : 0;
    }

    function getFreeBargesAtSource(sourceId, type = null) {
      const pool = _freeBySourceAndType[sourceId];
      if (!pool) return [];
      if (type) return Array.from(pool[type] || []);
      return [...Array.from(pool.small), ...Array.from(pool.large)];
    }

    function getStationedBarges(hddId) {
      const set = _stationedByHDD.get(hddId);
      return set ? Array.from(set) : [];
    }

    function getNextToFill(sourceId, barges) {
      // Queue-less FIFO: find barge at source with earliest fillRequestTime
      const candidates = barges.filter(b =>
        b.currentLocation === sourceId &&
        _fillRequestTimes.has(b.id) &&
        b.fillLevel < b.volume &&
        !b.assignedTugId &&
        b.status !== 'filling' // Not already being filled
      );
      if (candidates.length === 0) return null;

      candidates.sort((a, b) =>
        (_fillRequestTimes.get(a.id) || Infinity) - (_fillRequestTimes.get(b.id) || Infinity)
      );
      return candidates[0];
    }

    function hasFillRequest(bargeId) {
      return _fillRequestTimes.has(bargeId);
    }

    // -------------------------------------------------------------------------
    // MUTATION FUNCTIONS - The ONLY way to change tug-barge relationships
    // -------------------------------------------------------------------------

    function attachBarges(tugId, bargeIds, barges, simTime, context = '') {
      // Ensure tug has an entry in the index
      if (!_attachedByTug.has(tugId)) {
        _attachedByTug.set(tugId, new Set());
      }
      const tugSet = _attachedByTug.get(tugId);

      bargeIds.forEach(bid => {
        const barge = barges.find(b => b.id === bid);
        if (!barge) return;

        // Remove from free pool if present
        if (barge.currentLocation) {
          const sourcePool = _freeBySourceAndType[barge.currentLocation];
          if (sourcePool) {
            sourcePool[barge.type].delete(bid);
          }
        }

        // Remove from stationed index if present
        if (barge.assignedHDD) {
          const hddSet = _stationedByHDD.get(barge.assignedHDD);
          if (hddSet) hddSet.delete(bid);
        }

        // Update barge canonical state
        barge.assignedTugId = tugId;
        barge.currentLocation = null;  // In transit with tug

        // Clear fill request time (no longer waiting for fill)
        _fillRequestTimes.delete(bid);

        // Add to tug attachment index
        tugSet.add(bid);
      });
    }

    function detachBarges(tugId, bargeIds, toLocation, barges, simTime, context = '') {
      const tugSet = _attachedByTug.get(tugId);

      bargeIds.forEach(bid => {
        const barge = barges.find(b => b.id === bid);
        if (!barge) return;

        // Remove from tug attachment index
        if (tugSet) tugSet.delete(bid);

        // Update barge canonical state
        barge.assignedTugId = null;
        barge.currentLocation = toLocation;

        // Add to appropriate index based on destination
        if (toLocation && toLocation.startsWith('source')) {
          const sourcePool = _freeBySourceAndType[toLocation];
          if (sourcePool) {
            sourcePool[barge.type].add(bid);
          }
        } else if (toLocation && toLocation.startsWith('HDD')) {
          // Being stationed at HDD
          if (!_stationedByHDD.has(toLocation)) {
            _stationedByHDD.set(toLocation, new Set());
          }
          _stationedByHDD.get(toLocation).add(bid);
          barge.assignedHDD = toLocation;
        }
      });
    }

    function stationBargeAtHDD(bargeId, hddId, barges) {
      const barge = barges.find(b => b.id === bargeId);
      if (!barge) return;

      // Remove from any previous location
      if (barge.currentLocation) {
        const sourcePool = _freeBySourceAndType[barge.currentLocation];
        if (sourcePool) {
          sourcePool[barge.type].delete(bargeId);
        }
      }

      // Add to HDD stationed index
      if (!_stationedByHDD.has(hddId)) {
        _stationedByHDD.set(hddId, new Set());
      }
      _stationedByHDD.get(hddId).add(bargeId);

      // Update barge state
      barge.currentLocation = hddId;
      barge.assignedHDD = hddId;
      barge.status = 'stationed';
    }

    function unstationBargeFromHDD(bargeId, hddId, barges) {
      const set = _stationedByHDD.get(hddId);
      if (set) set.delete(bargeId);

      const barge = barges.find(b => b.id === bargeId);
      if (barge) {
        barge.assignedHDD = null;
      }
    }

    function requestFill(bargeId, simTime) {
      _fillRequestTimes.set(bargeId, simTime);
    }

    function completeFill(bargeId) {
      _fillRequestTimes.delete(bargeId);
    }

    function addBargeToSourcePool(bargeId, sourceId, bargeType) {
      const pool = _freeBySourceAndType[sourceId];
      if (pool) {
        pool[bargeType].add(bargeId);
      }
    }

    function removeBargeFromSourcePool(bargeId, sourceId, bargeType) {
      const pool = _freeBySourceAndType[sourceId];
      if (pool) {
        pool[bargeType].delete(bargeId);
      }
    }

    // -------------------------------------------------------------------------
    // VALIDATION - Detect inconsistencies for debugging
    // -------------------------------------------------------------------------

    function validateState(tugs, barges) {
      const errors = [];

      // Check: every barge with assignedTugId should be in that tug's index
      barges.forEach(b => {
        if (b.assignedTugId) {
          const tugSet = _attachedByTug.get(b.assignedTugId);
          if (!tugSet || !tugSet.has(b.id)) {
            errors.push(`ORPHAN: Barge ${b.id} has assignedTugId=${b.assignedTugId} but NOT in tug's index`);
          }
        }
      });

      // Check: every barge in a tug's index should have that assignedTugId
      _attachedByTug.forEach((bargeSet, tugId) => {
        bargeSet.forEach(bid => {
          const barge = barges.find(b => b.id === bid);
          if (!barge) {
            errors.push(`GHOST: Tug ${tugId} index contains non-existent barge ${bid}`);
          } else if (barge.assignedTugId !== tugId) {
            errors.push(`MISMATCH: Barge ${bid} in tug ${tugId} index but assignedTugId=${barge.assignedTugId}`);
          }
        });
      });

      // Check: free pool barges should not have assignedTugId
      Object.entries(_freeBySourceAndType).forEach(([sourceId, pools]) => {
        ['small', 'large'].forEach(type => {
          pools[type].forEach(bid => {
            const barge = barges.find(b => b.id === bid);
            if (barge && barge.assignedTugId) {
              errors.push(`POOL_ATTACHED: Barge ${bid} in ${sourceId} pool but has assignedTugId=${barge.assignedTugId}`);
            }
          });
        });
      });

      return errors;
    }

    // Public API
    return {
      initializeIndexes,
      // Read accessors
      getAttachedBarges,
      getAttachedBargeCount,
      getFreeBargesAtSource,
      getStationedBarges,
      getNextToFill,
      hasFillRequest,
      // Mutation functions
      attachBarges,
      detachBarges,
      stationBargeAtHDD,
      unstationBargeFromHDD,
      requestFill,
      completeFill,
      addBargeToSourcePool,
      removeBargeFromSourcePool,
      // Validation
      validateState
    };
  })();
  // ============================================================================
  // END KERNEL MODULE
  // ============================================================================

  const logAssetAction = (simTime, assetType, assetId, status, location, details = {}) => {
    // For draining/filling at HDDs, include fill level in key to capture all changes
    // For other statuses, just use status-location to deduplicate
    const includesFillLevel = (status === 'draining' || status === 'filling') &&
                               location.includes('HDD') &&
                               details.fillLevel !== undefined;

    const key = includesFillLevel
      ? `${assetId}-${status}-${location}-${Math.floor(details.fillLevel / 1000)}` // Round to nearest 1000 gal
      : `${assetId}-${status}-${location}`;

    // Only log state changes (not every timestep)
    if (lastLoggedState[assetId] === key) return;
    lastLoggedState[assetId] = key;

    const dateStr = getDateStrFromTime(simTime);
    const hourOfDay = getHourOfDayFromTime(simTime);
    const coords = getLocationCoordinates(location);

    assetLog.push({
      timestamp: `${dateStr} ${hourOfDay.toFixed(2).padStart(5, '0')}:00`,
      date: dateStr,
      hour: hourOfDay.toFixed(2),
      simHour: simTime,  // NEW: Simulation hour for precise timing
      assetType,
      assetId,
      status,
      location,
      coordinates: coords,  // NEW: Lat/lng for map animation
      ...details
    });
  };

  // Journey event logging with cost tracking
  const logJourneyEvent = (simTime, tugId, eventType, details = {}) => {
    const dateStr = getDateStrFromTime(simTime);
    const hourOfDay = getHourOfDayFromTime(simTime);

    assetLog.push({
      timestamp: `${dateStr} ${hourOfDay.toFixed(2).padStart(5, '0')}:00`,
      date: dateStr,
      hour: hourOfDay.toFixed(2),
      simHour: simTime,
      eventType,  // 'DISPATCH', 'ARRIVE_RIG', 'PUMP_START', 'PUMP_END', 'DEPART_RIG', 'RETURN_SOURCE'
      assetType: 'Journey',
      tugId,
      sourceId: details.sourceId || null,
      targetRig: details.targetRig || null,
      targetCrossing: details.targetCrossing || null,
      distance: details.distance || 0,
      waterVolume: details.waterVolume || 0,
      attachedBargeIds: details.attachedBargeIds || [],
      fromCoords: details.fromCoords || null,
      toCoords: details.toCoords || null,
      route: details.route || null,
      costs: details.costs || null  // Cost breakdown for this journey segment
    });
  };

  // Storage helpers
  // Get HDD crossing for a rig at current time
  // This will be set each timestep in the main loop
  let currentRigStatuses = {};

  const getCurrentHDDForRig = (rigName) => {
    const status = currentRigStatuses[rigName];
    return (status && status.isActive) ? status.hdd : null;
  };

  const getEffectiveStorageCapacity = (rigName) => {
    // Find current HDD crossing for this rig
    const crossing = getCurrentHDDForRig(rigName);
    if (!crossing || !state.hddStorage[crossing]) return 0;

    return state.hddStorage[crossing].totalCapacity;
  };

  // DEBUG: Wrapper to log storage capacity issues
  const debugGetEffectiveStorageCapacity = (rigName, dateStr, hourOfDay) => {
    const crossing = getCurrentHDDForRig(rigName);
    const capacity = getEffectiveStorageCapacity(rigName);
    if (dateStr === '2026-08-16' && rigName === 'Red' && hourOfDay >= 12 && hourOfDay < 13) {
      console.log(`[DEBUG] getEffectiveStorageCapacity(${rigName}): crossing=${crossing}, capacity=${capacity}`);
      console.log(`[DEBUG] currentRigStatuses[Red]:`, currentRigStatuses[rigName]);
      if (crossing) {
        console.log(`[DEBUG] state.hddStorage[${crossing}]:`, state.hddStorage[crossing]);
      }
    }
    return capacity;
  };

  const getEffectiveStorageLevel = (rigName) => {
    // Find current HDD crossing for this rig
    const crossing = getCurrentHDDForRig(rigName);
    if (!crossing || !state.hddStorage[crossing]) return 0;

    return state.hddStorage[crossing].totalLevel;
  };

  const setEffectiveStorageLevel = (rigName, newLevel, simTime = null, crossing = null) => {
    // Find current HDD crossing for this rig
    if (!crossing) crossing = getCurrentHDDForRig(rigName);
    if (!crossing || !state.hddStorage[crossing]) return;

    const hddStorage = state.hddStorage[crossing];
    const capacity = hddStorage.totalCapacity;
    newLevel = Math.max(0, Math.min(newLevel, capacity));

    // Distribute water across stationed barges (fill emptiest first)
    const bargeIds = hddStorage.stationedBarges;
    const barges = bargeIds.map(id => state.barges.find(b => b.id === id)).filter(b => b);

    // Sort by fill level (emptiest first)
    barges.sort((a, b) => (a.fillLevel / a.volume) - (b.fillLevel / b.volume));

    let waterToDistribute = newLevel;
    hddStorage.totalLevel = 0;

    barges.forEach(barge => {
      if (waterToDistribute > 0) {
        barge.fillLevel = Math.min(waterToDistribute, barge.volume);
        waterToDistribute -= barge.fillLevel;
      } else {
        barge.fillLevel = 0;
      }
      hddStorage.totalLevel += barge.fillLevel;
      hddStorage.bargeDetails[barge.id].level = barge.fillLevel;
    });
  };

  const consumeWaterFromRig = (rigName, amount, crossing = null, simTime = null) => {
    // Find current HDD crossing for this rig
    if (!crossing) crossing = getCurrentHDDForRig(rigName);
    if (!crossing || !state.hddStorage[crossing]) return 0;

    const hddStorage = state.hddStorage[crossing];
    const bargeIds = hddStorage.stationedBarges;
    const barges = bargeIds.map(id => state.barges.find(b => b.id === id)).filter(b => b);

    // Sort by fill level (fullest first - consume from fullest)
    barges.sort((a, b) => b.fillLevel - a.fillLevel);

    let remaining = amount;
    barges.forEach(barge => {
      if (remaining > 0 && barge.fillLevel > 0) {
        const beforeLevel = barge.fillLevel;
        const fromBarge = Math.min(barge.fillLevel, remaining);
        barge.fillLevel -= fromBarge;
        remaining -= fromBarge;
        hddStorage.bargeDetails[barge.id].level = barge.fillLevel;

        // Log consumption if simTime provided
        if (simTime && fromBarge > 0) {
          logAssetAction(simTime, `Barge (${barge.type})`, barge.id, 'draining', `at ${crossing}`, {
            usage: 'storage',
            fillLevel: Math.round(barge.fillLevel),
            capacity: barge.volume,
            fillPercent: ((barge.fillLevel / barge.volume) * 100).toFixed(1),
            consumed: Math.round(fromBarge),
            rig: rigName,
            assignedHDD: crossing
          });
        }
      }
    });

    // Update total level
    hddStorage.totalLevel = barges.reduce((sum, b) => sum + b.fillLevel, 0);

    return amount - remaining; // Return actual consumed
  };

  // Initialize sources
  Object.keys(sourceConfig).forEach(sourceId => {
    state.sources[sourceId] = {
      queue: [],
      filling: null,
      dailyHoursUsed: 0,
      flowRate: sourceConfig[sourceId].flowRate,
      availableBarges: []  // NEW: Pool of barge IDs at this source
    };
  });

  // =========================================================================
  // DYNAMIC BARGE MOBILIZATION
  // Barges are NOT placed on site at t=0. They are mobilized just-in-time
  // based on delivery schedules. This prevents early over-filling.
  // =========================================================================

  // Track unmobilized barges (available but not yet on site)
  state.unmobilizedBarges = {
    smallTransport: numSmallTransportBarges,
    largeTransport: numLargeTransport,
    smallStorage: numSmallStorageBargesTotal,
    largeStorage: numLargeStorageBargesTotal
  };

  // Mobilization schedule (populated after delivery schedules are built)
  state.bargeMobilizationSchedule = [];

  console.log(`[DYNAMIC MOB] Barge pool: ${numSmallTransportBarges} small transport, ${numLargeTransport} large transport, ${numSmallStorageBargesTotal} small storage, ${numLargeStorageBargesTotal} large storage`);
  console.log(`[DYNAMIC MOB] Barges will be mobilized just-in-time based on delivery schedules`);

  // Create mixed transport fleet - but DON'T place at sources yet
  const totalFlow = Object.values(sourceConfig).reduce((sum, c) => sum + c.flowRate, 0);

  // NOTE: Barges are created but with currentLocation=null (unmobilized)
  // They will be mobilized to sources when needed

  // Create unmobilized tug pool - tugs will be dynamically mobilized when needed
  // NO TUGS MOBILIZED AT TIME 0 - they mobilize based on delivery needs
  state.unmobilizedTugs = numSmallTugs;

  // =========================================================================
  // DYNAMIC BARGE MOBILIZATION FUNCTIONS
  // Barges are mobilized just-in-time based on delivery schedules
  // =========================================================================

  // Mobilize a barge: create it at a source and start filling
  const mobilizeBarge = (bargeType, role, sourceId, simTime, reason = '') => {
    const isLarge = bargeType === 'large';
    const isStorage = role === 'storage-available';

    // Check if we have unmobilized barges available
    const poolKey = isStorage
      ? (isLarge ? 'largeStorage' : 'smallStorage')
      : (isLarge ? 'largeTransport' : 'smallTransport');

    if (state.unmobilizedBarges[poolKey] <= 0) {
      console.log(`[DYNAMIC MOB] No unmobilized ${poolKey} barges available`);
      return null;
    }

    // Decrement unmobilized pool
    state.unmobilizedBarges[poolKey]--;

    // Create the barge at the source
    const bargeId = isLarge ? 'LB' + state.nextLargeBargeId++ : 'SB' + state.nextSmallBargeId++;
    const volume = isLarge ? LARGE_BARGE_VOLUME : SMALL_BARGE_VOLUME;

    const newBarge = {
      id: bargeId,
      type: bargeType,
      volume: volume,
      fillLevel: 0,  // Arrives empty, needs filling
      currentLocation: sourceId,
      homeSource: sourceId,
      assignedTugId: null,
      role: role,
      assignedHDD: null,
      status: 'queued',  // Ready to fill
      mobilizedAt: simTime
    };

    state.barges.push(newBarge);
    state.sources[sourceId].availableBarges.push(bargeId);
    state.sources[sourceId].queue.push(bargeId);
    kernel.requestFill(bargeId, simTime);

    // Update kernel indexes - add to source pool
    kernel.addBargeToSourcePool(bargeId, sourceId, bargeType);

    console.log(`[DYNAMIC MOB] Mobilized ${bargeId} (${bargeType} ${role}) at ${sourceId}, simTime=${simTime.toFixed(0)}h. Reason: ${reason}. Pool remaining: ${state.unmobilizedBarges[poolKey]}`);

    // Trigger fill if source is not currently filling
    // This ensures newly mobilized barges start filling immediately
    if (!state.sources[sourceId].filling) {
      tryStartNextFill(sourceId, simTime);
    }

    logAssetAction(simTime, `Barge (${bargeType})`, bargeId, 'mobilized', `at ${sourceId}`, {
      role: role,
      reason: reason
    });

    return newBarge;
  };

  // Mobilize a tug from the unmobilized pool
  const mobilizeTug = (sourceId, simTime, reason = '') => {
    if (state.unmobilizedTugs <= 0) {
      console.log(`[DYNAMIC MOB] No unmobilized tugs available`);
      return null;
    }

    state.unmobilizedTugs--;

    const tugId = 'T' + state.nextTugId++;
    const newTug = {
      id: tugId,
      status: 'idle',
      attachedBargeIds: [],
      currentLocation: sourceId,
      sourceId,  // Home source
      smallBargesPerTug: SMALL_BARGES_PER_TUG,
      largeBargesPerTug: LARGE_BARGES_PER_TUG,
      targetRig: null,
      arrivalHour: null,
      dispatchDistance: 0,
      targetCrossing: null,
      pumpPhase: 'none',
      phaseCompleteHour: null,
      deliveryPlan: [],
      currentStopIndex: 0,
      volumeDeliveredThisStop: 0,
      totalVolumeRemaining: 0,
      journeyQueueStart: null,
      journeyFillStart: null,
      mobilizedAt: simTime
    };

    state.tugs.push(newTug);

    console.log(`[DYNAMIC MOB] Mobilized ${tugId} at ${sourceId}, simTime=${simTime.toFixed(0)}h. Reason: ${reason}. Pool remaining: ${state.unmobilizedTugs}`);

    logAssetAction(simTime, 'Tug', tugId, 'mobilized', `at ${sourceId}`, {
      reason: reason
    });

    return newTug;
  };

  // Check if we need to mobilize barges based on upcoming deliveries
  const checkBargeMobilizationNeeds = (simTime) => {
    // Look ahead window for mobilization (hours)
    const MOBILIZATION_LEAD_TIME = 24; // Need 24h lead time for mob + fill

    // Check storage delivery schedule
    // IMPORTANT: Track how many barges are already committed to earlier deliveries at each source
    // This includes BOTH:
    // 1. Deliveries being processed in THIS tick (tracked in bargesCommittedBySource)
    // 2. Deliveries that were mobilized in PREVIOUS ticks but not yet dispatched
    const bargesCommittedBySource = {}; // { source: { small: N, large: N } }

    // FIX: Pre-calculate barges already committed to earlier pending deliveries
    // These are deliveries where bargesMobilized=true but status='pending' (not yet dispatched)
    const preCommittedBySource = {}; // { source: { small: N, large: N } }
    storageDeliverySchedule.forEach(d => {
      if (d.status === 'pending' && d.bargesMobilized) {
        if (!preCommittedBySource[d.source]) {
          preCommittedBySource[d.source] = { small: 0, large: 0 };
        }
        preCommittedBySource[d.source].small += d.smallBarges;
        preCommittedBySource[d.source].large += d.largeBarges;
      }
    });

    storageDeliverySchedule.forEach(delivery => {
      if (delivery.status !== 'pending') return;

      // Calculate when we need barges ready at source
      const totalVolumeNeeded = (delivery.smallBarges * SMALL_BARGE_VOLUME) + (delivery.largeBarges * LARGE_BARGE_VOLUME);
      const fillTimeHrs = totalVolumeNeeded / (sourceConfig[delivery.source]?.flowRate || 400) / 60;
      const needBargesReadyTime = delivery.dispatchTime;
      const needBargesAtSourceTime = needBargesReadyTime - fillTimeHrs;
      const mobilizationTime = needBargesAtSourceTime - MOBILIZATION_LEAD_TIME;

      // Check if it's time to mobilize
      if (simTime >= mobilizationTime && !delivery.bargesMobilized) {
        // Initialize tracking for this source
        if (!bargesCommittedBySource[delivery.source]) {
          bargesCommittedBySource[delivery.source] = { small: 0, large: 0 };
        }
        const committed = bargesCommittedBySource[delivery.source];

        // FIX: Get pre-committed counts from earlier ticks
        const preCommitted = preCommittedBySource[delivery.source] || { small: 0, large: 0 };

        // Count available storage barges at source (separate small and large)
        const smallAvailableAtSource = state.barges.filter(b =>
          b.currentLocation === delivery.source &&
          b.type === 'small' &&
          b.role === 'storage-available' &&
          b.status !== 'stationed' &&
          b.status !== 'en-route-storage'
        ).length;
        const largeAvailableAtSource = state.barges.filter(b =>
          b.currentLocation === delivery.source &&
          b.type === 'large' &&
          b.role === 'storage-available' &&
          b.status !== 'stationed' &&
          b.status !== 'en-route-storage'
        ).length;

        // FIX: Available barges = at source - committed in earlier ticks - committed in this tick
        const totalLargeCommitted = preCommitted.large + committed.large;
        const totalSmallCommitted = preCommitted.small + committed.small;
        const largeAvailable = Math.max(0, largeAvailableAtSource - totalLargeCommitted);
        const smallAvailable = Math.max(0, smallAvailableAtSource - totalSmallCommitted);

        // Mobilize LARGE storage barges first (more cost-effective)
        const largeNeeded = Math.max(0, delivery.largeBarges - largeAvailable);
        if (largeNeeded > 0) {
          console.log(`[DYNAMIC MOB] Storage delivery ${delivery.hdd} needs ${largeNeeded} LARGE storage barges at ${delivery.source} (preCommitted=${preCommitted.large}, thisTickCommitted=${committed.large}, at_source=${largeAvailableAtSource})`);
          for (let i = 0; i < largeNeeded; i++) {
            mobilizeBarge('large', 'storage-available', delivery.source, simTime, `storage for ${delivery.hdd}`);
          }
        }

        // Then mobilize small storage barges for remaining capacity
        const smallNeeded = Math.max(0, delivery.smallBarges - smallAvailable);
        if (smallNeeded > 0) {
          console.log(`[DYNAMIC MOB] Storage delivery ${delivery.hdd} needs ${smallNeeded} small storage barges at ${delivery.source} (preCommitted=${preCommitted.small}, thisTickCommitted=${committed.small}, at_source=${smallAvailableAtSource})`);
          for (let i = 0; i < smallNeeded; i++) {
            mobilizeBarge('small', 'storage-available', delivery.source, simTime, `storage for ${delivery.hdd}`);
          }
        }

        // Mark barges as committed to this delivery
        committed.large += delivery.largeBarges;
        committed.small += delivery.smallBarges;

        delivery.bargesMobilized = true;
      }
    });

    // Check transport delivery schedule - mobilize transport barges based on demand
    if (typeof transportDeliverySchedule !== 'undefined') {
      transportDeliverySchedule.forEach(delivery => {
        if (delivery.status !== 'pending') return;

        const fillTimeHrs = SMALL_BARGE_VOLUME / (sourceConfig[delivery.source]?.flowRate || 400) / 60;
        const needBargesAtSourceTime = delivery.dispatchTime - fillTimeHrs;
        const mobilizationTime = needBargesAtSourceTime - MOBILIZATION_LEAD_TIME;

        if (simTime >= mobilizationTime && !delivery.bargesMobilized) {
          // Count available transport barges at source (both small and large)
          const availableSmallAtSource = state.barges.filter(b =>
            b.currentLocation === delivery.source &&
            b.type === 'small' &&
            b.role === 'available' &&
            ['idle', 'queued', 'filling'].includes(b.status)
          ).length;
          const availableLargeAtSource = state.barges.filter(b =>
            b.currentLocation === delivery.source &&
            b.type === 'large' &&
            b.role === 'available' &&
            ['idle', 'queued', 'filling'].includes(b.status)
          ).length;

          // Calculate capacity needed vs available
          // Large barge = 300k, Small barge = 80k, so 1 large ≈ 3.75 small in capacity
          const availableCapacity = availableLargeAtSource * LARGE_BARGE_VOLUME + availableSmallAtSource * SMALL_BARGE_VOLUME;
          const neededCapacity = delivery.bargeCount * SMALL_BARGE_VOLUME; // bargeCount assumes small barges

          if (availableCapacity < neededCapacity) {
            const capacityDeficit = neededCapacity - availableCapacity;

            // PREFER LARGE BARGES (more cost-effective per gallon)
            // First, mobilize large transport barges if available
            const largeBargesToMob = Math.min(
              Math.ceil(capacityDeficit / LARGE_BARGE_VOLUME),
              state.unmobilizedBarges.largeTransport
            );

            if (largeBargesToMob > 0) {
              console.log(`[DYNAMIC MOB] Transport delivery ${delivery.id} mobilizing ${largeBargesToMob} LARGE barges at ${delivery.source} (more efficient)`);
              for (let i = 0; i < largeBargesToMob; i++) {
                mobilizeBarge('large', 'available', delivery.source, simTime, `transport ${delivery.id}`);
              }
            }

            // Then fill remaining capacity gap with small barges
            const remainingCapacityDeficit = capacityDeficit - (largeBargesToMob * LARGE_BARGE_VOLUME);
            if (remainingCapacityDeficit > 0) {
              const smallBargesToMob = Math.min(
                Math.ceil(remainingCapacityDeficit / SMALL_BARGE_VOLUME),
                state.unmobilizedBarges.smallTransport
              );
              if (smallBargesToMob > 0) {
                console.log(`[DYNAMIC MOB] Transport delivery ${delivery.id} mobilizing ${smallBargesToMob} small barges at ${delivery.source}`);
                for (let i = 0; i < smallBargesToMob; i++) {
                  mobilizeBarge('small', 'available', delivery.source, simTime, `transport ${delivery.id}`);
                }
              }
            }
          }

          // Also mobilize tugs at the same time if needed
          // Account for large barges requiring 1 per tug, small barges 2 per tug
          const totalLargeAtSource = availableLargeAtSource + Math.min(
            Math.ceil((neededCapacity - availableCapacity) / LARGE_BARGE_VOLUME),
            state.unmobilizedBarges.largeTransport
          );
          const totalSmallAtSource = availableSmallAtSource + Math.max(0,
            Math.ceil((neededCapacity - availableCapacity - totalLargeAtSource * LARGE_BARGE_VOLUME) / SMALL_BARGE_VOLUME)
          );
          const idleTugsAtSource = state.tugs.filter(t =>
            t.currentLocation === delivery.source && t.status === 'idle'
          ).length;
          // Large barges need 1 tug each, small barges can share 2 per tug
          const tugsForLarge = totalLargeAtSource;
          const tugsForSmall = Math.ceil(totalSmallAtSource / SMALL_BARGES_PER_TUG);
          const tugsNeeded = Math.max(tugsForLarge, tugsForSmall); // They can share tugs
          const tugDeficit = tugsNeeded - idleTugsAtSource;
          if (tugDeficit > 0 && state.unmobilizedTugs > 0) {
            const tugsToMob = Math.min(tugDeficit, state.unmobilizedTugs);
            console.log(`[DYNAMIC MOB] Transport delivery needs ${tugsToMob} tugs at ${delivery.source}`);
            for (let i = 0; i < tugsToMob; i++) {
              mobilizeTug(delivery.source, simTime, `for transport delivery ${delivery.id}`);
            }
          }

          delivery.bargesMobilized = true;
        }
      });
    }

    // Mobilize tugs for storage deliveries
    // IMPORTANT: Track how many tugs are already committed to earlier deliveries at each source
    // to avoid under-mobilization when multiple deliveries share the same dispatch window
    const tugsCommittedBySource = {};

    storageDeliverySchedule.forEach(delivery => {
      if (delivery.status !== 'pending' || !delivery.bargesMobilized) return;
      if (delivery.tugsMobilized) return;

      // Count idle tugs at this source
      const idleTugsAtSource = state.tugs.filter(t =>
        t.currentLocation === delivery.source && t.status === 'idle'
      ).length;

      // Track how many tugs are already committed to earlier deliveries at this source
      if (!tugsCommittedBySource[delivery.source]) {
        tugsCommittedBySource[delivery.source] = 0;
      }

      // Available tugs = idle tugs - already committed to earlier deliveries
      const availableTugs = Math.max(0, idleTugsAtSource - tugsCommittedBySource[delivery.source]);
      const tugsNeeded = 1; // Storage deliveries typically need 1 tug
      const tugDeficit = tugsNeeded - availableTugs;

      if (tugDeficit > 0 && state.unmobilizedTugs > 0) {
        const tugsToMob = Math.min(tugDeficit, state.unmobilizedTugs);
        console.log(`[DYNAMIC MOB] Storage delivery ${delivery.hdd} needs ${tugsToMob} tug(s) at ${delivery.source} (committed=${tugsCommittedBySource[delivery.source]}, idle=${idleTugsAtSource})`);
        for (let i = 0; i < tugsToMob; i++) {
          mobilizeTug(delivery.source, simTime, `for storage delivery ${delivery.hdd}`);
        }
      }

      // Mark this delivery as having a tug committed
      tugsCommittedBySource[delivery.source]++;
      delivery.tugsMobilized = true;
    });
  };

  // =========================================================================
  // KERNEL INITIALIZATION - Start with empty barge pools
  // Barges will be added as they're mobilized
  // =========================================================================
  kernel.initializeIndexes(state.tugs, state.barges, sourceIds);
  console.log('[KERNEL] Initialized indexes (empty barge pools):', {
    tugs: state.tugs.length,
    barges: state.barges.length,
    sources: sourceIds.length
  });

  // DEBUG: Log pool contents after initialization (should be empty)
  console.log('[KERNEL DEBUG] Free pool contents after init (expecting empty):');
  sourceIds.forEach(srcId => {
    const smallFree = kernel.getFreeBargesAtSource(srcId, 'small');
    const largeFree = kernel.getFreeBargesAtSource(srcId, 'large');
    console.log(`  ${srcId}: small=[${smallFree.join(',')}], large=[${largeFree.join(',')}]`);
  });

  // =========================================================================
  // DYNAMIC MOBILIZATION - Assets mobilize just-in-time based on delivery needs
  // Bootstrap mobilization for early deliveries is done AFTER schedules are built
  // =========================================================================
  console.log('[DYNAMIC MOB] Assets will mobilize just-in-time based on delivery schedules.');
  console.log(`[DYNAMIC MOB] Unmobilized pool: ${state.unmobilizedTugs} tugs, ${state.unmobilizedBarges.smallTransport} small transport, ${state.unmobilizedBarges.largeTransport} large transport, ${state.unmobilizedBarges.smallStorage} small storage, ${state.unmobilizedBarges.largeStorage} large storage`);

  // Daily results - initialize with dynamic rig and source names
  const dailyResults = {};
  const initRanDry = {};
  const initRigZeros = {};
  rigNames.forEach(name => {
    initRanDry[name] = false;
    initRigZeros[name] = 0;
  });
  const initSourceUtil = {};
  sourceIds.forEach(id => {
    initSourceUtil[id] = { fillHours: 0, switchoutHours: 0 };
  });
  for (let d = 0; d < totalDays; d++) {
    const date = new Date(minDate);
    date.setDate(date.getDate() + d);
    const dateStr = date.toISOString().split('T')[0];
    dailyResults[dateStr] = {
      date: dateStr, demand: 0, usage: 0, deficit: 0, injected: 0,
      ranDry: { ...initRanDry },
      fuelIdle: 0, fuelRunning: 0,
      downtimeHours: { ...initRigZeros },
      storageLevel: { ...initRigZeros },
      rigDemand: { ...initRigZeros },
      rigInjected: { ...initRigZeros },
      rigDeficit: { ...initRigZeros },
      tugsActive: 0,
      tugsOnSite: 0,
      smallBargesActive: 0,
      smallBargesActiveTransport: 0,
      smallBargesActiveStorage: 0,
      smallBargesOnSite: 0,
      largeBargesActive: 0,
      largeBargesActiveTransport: 0,
      largeBargesActiveStorage: 0,
      largeBargesOnSite: 0,
      sourceUtilization: JSON.parse(JSON.stringify(initSourceUtil)) // Deep copy
    };
  }

  // Log initial barge setup (both transport and storage)
  state.barges.forEach(barge => {
    const bargeSize = barge.volume >= LARGE_BARGE_VOLUME ? 'large' : 'small';
    // Determine usage based on barge role: available=transport, storage-available=storage
    const usage = barge.role === 'storage-available' ? 'storage' : 'transport';
    logAssetAction(0, `Barge (${bargeSize})`, barge.id, 'queued', `at ${barge.currentLocation}`, {
      fillLevel: Math.round(barge.fillLevel),
      capacity: barge.volume,
      fillPercent: ((barge.fillLevel / barge.volume) * 100).toFixed(1),
      usage: usage,
      assignedTugId: barge.assignedTugId || null
    });
  });

  // Storage barges now start at sources (empty) and will be logged when filled/delivered

  // Barge pool helpers - now using kernel as primary source of truth
  const getAvailableBargesAtSource = (sourceId, bargeType = null, excludeStorage = false) => {
    // Get free barges from kernel (canonical source)
    const freeBargeIds = kernel.getFreeBargesAtSource(sourceId, bargeType);

    // DEBUG: Log kernel vs filter results
    const debugEnabled = false;

    return freeBargeIds.filter(bid => {
      const b = state.barges.find(x => x.id === bid);
      if (!b) return false;

      // IMPORTANT: Exclude storage barges when looking for transport barges
      // UNIFIED MODEL: Use role-based check instead of permanent flag
      const isStorageRole = b.role === 'storage-available' || b.role === 'storage';
      if (excludeStorage && isStorageRole) return false;

      // Barges are ready if:
      // 1. Status is 'idle', 'queued', OR 'filling' (at source, ready or getting ready)
      // 2. Has ACTUAL water (fillLevel > 0) - MUST have water NOW, not "will have soon"
      // 3. Not currently assigned to a tug (kernel already ensures this)
      // CRITICAL FIX: Do NOT dispatch barges with fillLevel=0 even if filling/queued
      const isReadyStatus = ['idle', 'queued', 'filling'].includes(b.status);
      const hasActualWater = b.fillLevel > 0; // MUST have water NOW
      return isReadyStatus && hasActualWater;
    });
  };

  const assignBargesToTug = (tug, bargeIds, sourceId, simTime = null, debugContext = '') => {
    const sourceState = state.sources[sourceId];

    // Use kernel to handle all state changes (canonical source of truth)
    kernel.attachBarges(tug.id, bargeIds, state.barges, simTime, debugContext);

    // Legacy sync: keep tug.attachedBargeIds and source.availableBarges in sync for now
    // TODO: Remove in Phase 6 when we delete tug.attachedBargeIds entirely
    bargeIds.forEach(bid => {
      // Remove from legacy pool
      const idx = sourceState.availableBarges.indexOf(bid);
      if (idx >= 0) {
        sourceState.availableBarges.splice(idx, 1);
      }
      // Add to legacy tug attachment list
      if (!tug.attachedBargeIds.includes(bid)) {
        tug.attachedBargeIds.push(bid);
      }
    });

    // DEBUG: Log on critical date
    if (simTime !== null) {
      const dateStr = getDateStrFromTime(simTime);
      if (dateStr === '2026-08-16') {
        console.log(`[DEBUG ${dateStr}] assignBargesToTug via kernel: ${bargeIds.join(',')} to ${tug.id}. Context: ${debugContext}`);
      }
    }
  };

  const returnBargesToSource = (bargeIds, sourceId, simTime = null, debugContext = '') => {
    const sourceState = state.sources[sourceId];

    bargeIds.forEach(bid => {
      const barge = state.barges.find(b => b.id === bid);
      if (barge) {
        // UNIFIED MODEL: Use role-based check to determine if barge was storage
        const wasStorage = barge.role === 'storage-available' || barge.role === 'storage';

        // Find the tug this barge was attached to (if any) for kernel detach
        const previousTugId = barge.assignedTugId;
        if (previousTugId) {
          // Use kernel to detach (handles index updates)
          kernel.detachBarges(previousTugId, [bid], sourceId, state.barges, simTime, debugContext);
        } else {
          // Barge wasn't attached - just update location via kernel helpers
          kernel.addBargeToSourcePool(bid, sourceId, barge.type);
        }

        // CRITICAL: Preserve storage barge role when returning to source
        // Storage barges should keep role='storage-available', NOT become transport barges
        if (wasStorage) {
          barge.role = 'storage-available';
          barge.status = 'idle'; // Storage barges don't need filling
          console.log(`[STORAGE BARGE POOL] ${bid} returned to ${sourceId} pool with role=storage-available, status=idle. Context: ${debugContext}`);
        } else {
          // Transport barges get queued for fill
          barge.status = 'queued';
          if (barge.fillLevel < barge.volume) {
            kernel.requestFill(bid, simTime || 0);
          }
        }

        // Legacy sync: keep source.availableBarges and source.queue in sync for now
        // TODO: Remove in Phase 4 when we delete source pools entirely
        if (!sourceState.availableBarges.includes(bid)) {
          sourceState.availableBarges.push(bid);
        }
        // Only add transport barges to LEGACY source queue (not the kernel fill queue)
        // Note: Kernel fill queue (via requestFill above) handles ALL barges including storage
        if (!wasStorage && !sourceState.queue.includes(bid)) {
          sourceState.queue.push(bid);
        }

        // Legacy sync: clear tug's attachedBargeIds if tug exists
        // TODO: Remove in Phase 6
        if (previousTugId) {
          const prevTug = state.tugs.find(t => t.id === previousTugId);
          if (prevTug) {
            const tugIdx = prevTug.attachedBargeIds.indexOf(bid);
            if (tugIdx >= 0) prevTug.attachedBargeIds.splice(tugIdx, 1);
          }
        }

        // DEBUG: Log when barges are returned to pool
        if (simTime !== null) {
          const dateStr = getDateStrFromTime(simTime);
          if (dateStr === '2026-08-16') {
            console.log(`[DEBUG ${dateStr}] returnBargesToSource via kernel: ${bid} returned to ${sourceId} pool. Context: ${debugContext}. fillLevel: ${barge.fillLevel}, role: ${barge.role}`);
          }
        }
      }
    });
  };

  const checkUpcomingDemandAtSource = (sourceId, currentTime, hoursAhead) => {
    // Simplified heuristic: Check how many barges are currently at this source
    // and how many tugs are idle here
    const bargesAtSource = state.barges.filter(b =>
      b.currentLocation === sourceId &&
      ['idle', 'queued', 'filling'].includes(b.status)
    ).length;

    const idleTugsAtSource = state.tugs.filter(t =>
      t.currentLocation === sourceId &&
      t.status === 'idle'
    ).length;

    // Simple heuristic: if we have more barges than tugs, some can be dropped
    // Otherwise keep barges attached for quick dispatch
    // Return estimated barges needed (1-2 per tug is typical)
    const bargesNeeded = idleTugsAtSource * 2;

    return bargesNeeded * SMALL_BARGE_VOLUME; // Return as gallons for consistency
  };

  // UNIFIED MODEL: Calculate storage target for a specific stage
  // Returns { targetCapacity, smallBargesNeeded } based on current stage rate
  const calculateStorageTargetForStage = (rigConfig, stage) => {
    const stageRates = {
      'RigUp': 0,
      'Pilot': rigConfig?.pilot || 0,
      'Ream': rigConfig?.ream || 0,
      'Swab': rigConfig?.swab || 0,
      'Pull': rigConfig?.pull || 0,
      'RigDown': 0,
      'Idle': 0
    };

    const hourlyRate = stageRates[stage] || 0;
    const dailyDemand = hourlyRate * (DRILLING_END_HOUR - DRILLING_START_HOUR);
    const targetCapacity = dailyDemand * 1.5;  // 150% buffer
    const smallBargesNeeded = Math.ceil(targetCapacity / SMALL_BARGE_VOLUME);

    return { targetCapacity, smallBargesNeeded, dailyDemand, hourlyRate };
  };

  // Build storage barge delivery schedule based on HDD schedule
  // RESOURCE-AWARE: Tracks barge availability over time, including returns from completed HDDs
  const buildStorageDeliverySchedule = () => {
    const deliveries = [];

    console.log('[STORAGE SCHEDULE] Building resource-aware storage delivery schedule...');
    console.log(`[STORAGE SCHEDULE] Available: ${numSmallStorageBarges} small, ${numLargeStorage} large storage barges`);
    console.log(`[STORAGE SCHEDULE] HDDs to serve: ${hddSchedule.length}`);

    // =========================================================================
    // SAME-RIG TRANSFER PRE-COMPUTATION
    // Identify HDDs that will receive barges via same-rig transfer (support tug)
    // These HDDs don't need storage delivery from us - barges stay with the rig
    // =========================================================================
    const SAME_RIG_TRANSFER_MAX_GAP_DAYS_SCHEDULE = 14; // Same as runtime constant
    const sameRigTransferTargets = new Set(); // HDDs that will receive transferred barges
    const sameRigTransferSources = new Map(); // Map: sourceHDD -> targetHDD

    // Build list of HDDs by rig with their timing
    const hddsByRig = {};
    hddSchedule.forEach(hdd => {
      if (!hddsByRig[hdd.rig]) hddsByRig[hdd.rig] = [];
      const rigUpEndDate = new Date(hdd.rigUp);
      const pilotStartDate = new Date(rigUpEndDate);
      pilotStartDate.setDate(pilotStartDate.getDate() + 1);
      const pilotStartSimTime = (pilotStartDate - minDate) / (1000 * 60 * 60);
      const pullDate = new Date(hdd.pull);
      const pullEndSimTime = (pullDate - minDate) / (1000 * 60 * 60) + 24; // Day after pull
      hddsByRig[hdd.rig].push({ ...hdd, pilotStartSimTime, pullEndSimTime });
    });

    // For each rig, check consecutive HDDs for same-rig transfer eligibility
    Object.entries(hddsByRig).forEach(([rigName, rigHDDs]) => {
      // Sort by pilot start time
      rigHDDs.sort((a, b) => a.pilotStartSimTime - b.pilotStartSimTime);

      for (let i = 0; i < rigHDDs.length - 1; i++) {
        const currentHDD = rigHDDs[i];
        const nextHDD = rigHDDs[i + 1];
        const gapHours = nextHDD.pilotStartSimTime - currentHDD.pullEndSimTime;
        const gapDays = gapHours / 24;

        if (gapDays <= SAME_RIG_TRANSFER_MAX_GAP_DAYS_SCHEDULE && gapDays > 0) {
          // This pair qualifies for same-rig transfer
          sameRigTransferTargets.add(nextHDD.crossing);
          sameRigTransferSources.set(currentHDD.crossing, nextHDD.crossing);
          console.log(`[STORAGE SCHEDULE] Same-rig transfer: ${currentHDD.crossing} -> ${nextHDD.crossing} (${rigName}, gap=${gapDays.toFixed(1)} days)`);
        }
      }
    });

    console.log(`[STORAGE SCHEDULE] Same-rig transfers detected: ${sameRigTransferTargets.size} HDDs will receive barges via transfer`);
    if (sameRigTransferTargets.size > 0) {
      console.log(`[STORAGE SCHEDULE] HDDs receiving transfers: ${[...sameRigTransferTargets].join(', ')}`);
    }

    // =========================================================================
    // STEP 1: Calculate storage requirements for each HDD
    // UNIFIED MODEL: Use PILOT rate for initial storage (not peak rate)
    // Storage can be adjusted at stage transitions if needed
    // =========================================================================
    const hddRequirements = hddSchedule.map(hdd => {
      const { rig, crossing } = hdd;
      const rigConfig = rigs[rig];

      // UNIFIED MODEL: Use pilot rate for initial storage calculation
      // This prevents over-provisioning - storage will be adjusted if needed at stage transitions
      const pilotHourlyRate = rigConfig?.pilot || 0;
      const dailyDemand = pilotHourlyRate * (DRILLING_END_HOUR - DRILLING_START_HOUR);

      // Target: 150% of pilot phase daily demand
      const targetCapacity = dailyDemand * 1.5;
      // PREFER LARGE BARGES for storage (more cost-effective per gallon)
      // 1 large barge (300k) ≈ 3.75 small barges (80k each) but costs only 3x
      // Calculate optimal mix: use large barges first, then fill gap with small
      const smallBargesNeeded = Math.ceil(targetCapacity / SMALL_BARGE_VOLUME);

      // Convert dates to simTime
      // BUG FIX: hdd.pilot is the pilot END date, not START date
      // Pilot STARTS the day AFTER rigUp ends, which is when drilling begins
      const rigUpEndDate = new Date(hdd.rigUp);
      const pilotStartDate = new Date(rigUpEndDate);
      pilotStartDate.setDate(pilotStartDate.getDate() + 1); // Day after rigUp ends
      const pilotEndDate = new Date(hdd.pilot);
      const rigDownDate = new Date(hdd.rigDown);
      const pilotStartSimTime = (pilotStartDate - minDate) / (1000 * 60 * 60);
      const pilotEndSimTime = (pilotEndDate - minDate) / (1000 * 60 * 60);
      const rigDownSimTime = (rigDownDate - minDate) / (1000 * 60 * 60);

      // UNIFIED MODEL: Find best source by THROUGHPUT (cycle time), not just distance
      // A source with 4x flow rate saves far more time than travel distance difference
      const { bestSource: closestSource } = getBestSourceByThroughput(crossing);
      const closestDistance = getDistance(closestSource, crossing);

      // Calculate travel time from best source to HDD
      const travelTimeToHDD = closestDistance / emptySpeed;

      // =========================================================================
      // SAME-RIG TRANSFER: If this HDD is a transfer source, barges don't return
      // until the FINAL HDD in the transfer chain completes
      // =========================================================================
      let effectiveReturnTime = rigDownSimTime + travelTimeToHDD + 2; // +2 hours for demob operations

      // Follow the transfer chain to find the final HDD
      let currentCrossing = crossing;
      while (sameRigTransferSources.has(currentCrossing)) {
        const nextCrossing = sameRigTransferSources.get(currentCrossing);
        const nextHDD = hddSchedule.find(h => h.crossing === nextCrossing);
        if (nextHDD) {
          const nextRigDownDate = new Date(nextHDD.rigDown);
          const nextRigDownSimTime = (nextRigDownDate - minDate) / (1000 * 60 * 60);
          effectiveReturnTime = nextRigDownSimTime + travelTimeToHDD + 2;
          currentCrossing = nextCrossing;
        } else {
          break;
        }
      }

      if (effectiveReturnTime > rigDownSimTime + travelTimeToHDD + 2) {
        console.log(`[STORAGE SCHEDULE] ${crossing}: Transfer source - barges return after ${currentCrossing} completes (${effectiveReturnTime.toFixed(0)}h vs ${(rigDownSimTime + travelTimeToHDD + 2).toFixed(0)}h)`);
      }

      const returnTime = effectiveReturnTime;

      return {
        ...hdd,
        dailyDemand,
        targetCapacity,
        smallBargesNeeded,
        largeBargesNeeded: 0, // Will calculate below
        pilotStartSimTime,  // BUG FIX: Use pilot START time, not end time
        pilotEndSimTime,
        rigDownSimTime,
        returnTime,
        closestSource,  // UNIFIED MODEL: Use closest source, not fixed hub
        travelTimeToHDD
      };
    });

    // Sort by pilot START time (when storage is needed)
    hddRequirements.sort((a, b) => a.pilotStartSimTime - b.pilotStartSimTime);

    // =========================================================================
    // STEP 2: Build barge availability timeline
    // Track when barges become available (initially all available, then returns)
    // =========================================================================
    // Events: { time, type: 'initial' | 'return', smallCount, largeCount, fromHDD }
    const availabilityEvents = [
      { time: 0, type: 'initial', smallCount: numSmallStorageBarges, largeCount: numLargeStorage, fromHDD: null }
    ];

    // Running totals for allocation
    let smallBargesAvailable = numSmallStorageBarges;
    let largeBargesAvailable = numLargeStorage;

    // Track allocations for return scheduling
    const allocations = []; // { hdd, smallCount, largeCount, returnTime }

    console.log(`[STORAGE SCHEDULE] Initial pool: ${smallBargesAvailable} small, ${largeBargesAvailable} large`);

    // =========================================================================
    // STEP 3: Allocate barges to each HDD in chronological order
    // =========================================================================
    hddRequirements.forEach((req, index) => {
      const { crossing, rig, pilotStartSimTime, smallBargesNeeded, closestSource, travelTimeToHDD, returnTime } = req;

      // =========================================================================
      // SAME-RIG TRANSFER: Skip scheduling delivery for HDDs that will receive
      // barges via same-rig transfer (support tug moves barges, not our tugs)
      // =========================================================================
      if (sameRigTransferTargets.has(crossing)) {
        console.log(`[STORAGE SCHEDULE] ${crossing}: SKIPPING - will receive barges via same-rig transfer`);
        return; // Skip to next HDD
      }

      // Target arrival: 12 hours BEFORE pilot STARTS (buffer for delays)
      const targetArrivalTime = pilotStartSimTime - 12;

      // FIX: Calculate fill time - storage barges must be PRE-FILLED before dispatch
      // Fill time = total volume / source flow rate
      const sourceFlowRate = sourceConfig[closestSource]?.flowRate || 400; // GPM
      const totalVolumeToFill = smallBargesNeeded * SMALL_BARGE_VOLUME;
      const fillTimeHours = totalVolumeToFill / sourceFlowRate / 60; // Convert to hours

      // Travel time + hookup
      const travelAndHookup = travelTimeToHDD + 4; // 4 hours hookup/prep

      // Total prep time = fill time + travel + hookup
      // Dispatch can happen after barges are filled, so:
      // - Fill must START at: targetArrival - travelAndHookup - fillTime
      // - Dispatch happens at: targetArrival - travelAndHookup (after fill completes)
      const fillStartTime = Math.max(0, targetArrivalTime - travelAndHookup - fillTimeHours);
      let idealDispatchTime = Math.max(0, targetArrivalTime - travelAndHookup);

      console.log(`[STORAGE SCHEDULE] ${crossing}: fillStart@${fillStartTime.toFixed(0)}h, dispatch@${idealDispatchTime.toFixed(0)}h, arrive@${targetArrivalTime.toFixed(0)}h (fill=${fillTimeHours.toFixed(1)}h, travel=${travelTimeToHDD.toFixed(1)}h)`);

      // Check if we have enough barges at ideal dispatch time
      // Calculate available barges at this time by checking returns
      let smallAtTime = numSmallStorageBarges;
      let largeAtTime = numLargeStorage;

      // Subtract barges currently deployed (not yet returned)
      allocations.forEach(alloc => {
        if (alloc.returnTime > idealDispatchTime) {
          // These barges are still deployed
          smallAtTime -= alloc.smallCount;
          largeAtTime -= alloc.largeCount;
        }
      });

      // Determine how many we can allocate
      // PREFER LARGE BARGES (more cost-effective: $0.0025/gal/day vs $0.00313/gal/day)
      // Allocate large barges first to meet capacity, then small barges for the remainder
      const capacityNeeded = smallBargesNeeded * SMALL_BARGE_VOLUME;

      // FIX: Use Math.ceil() so 1 large barge covers any need up to 300k
      // Previously Math.floor() meant we NEVER allocated large barges unless needing 300k+
      const largeToAllocate = Math.min(
        Math.ceil(capacityNeeded / LARGE_BARGE_VOLUME), // 1 large covers up to 300k
        Math.max(0, largeAtTime) // How many large are available
      );
      const capacityFromLarge = largeToAllocate * LARGE_BARGE_VOLUME;
      const remainingCapacity = Math.max(0, capacityNeeded - capacityFromLarge);
      const smallToAllocate = Math.min(
        Math.ceil(remainingCapacity / SMALL_BARGE_VOLUME),
        Math.max(0, smallAtTime)
      );

      // Calculate total capacity we're actually providing
      const totalCapacityProvided = capacityFromLarge + (smallToAllocate * SMALL_BARGE_VOLUME);

      // If not enough barges, find when some will return
      let actualDispatchTime = idealDispatchTime;
      let waitingForReturns = false;

      // FIX: Check total capacity, not just small barge count
      if (totalCapacityProvided < capacityNeeded) {
        // Need to wait for barges to return
        // Find the earliest return that gives us enough capacity (small OR large)
        const capacityShortfall = capacityNeeded - totalCapacityProvided;

        // Sort allocations by return time
        const pendingReturns = allocations
          .filter(a => a.returnTime > idealDispatchTime)
          .sort((a, b) => a.returnTime - b.returnTime);

        let recoveredCapacity = 0;
        let waitUntil = idealDispatchTime;

        for (const ret of pendingReturns) {
          // Each return provides capacity from both small AND large barges
          recoveredCapacity += ret.smallCount * SMALL_BARGE_VOLUME;
          recoveredCapacity += ret.largeCount * LARGE_BARGE_VOLUME;
          waitUntil = ret.returnTime;
          if (recoveredCapacity >= capacityShortfall) break;
        }

        if (recoveredCapacity >= capacityShortfall) {
          // We can get enough capacity if we wait
          actualDispatchTime = waitUntil + 1; // Wait until after return
          waitingForReturns = true;
          console.log(`[STORAGE SCHEDULE] ${crossing}: Must wait for barge returns. Dispatch delayed from ${idealDispatchTime.toFixed(0)}h to ${actualDispatchTime.toFixed(0)}h`);
        } else {
          // Not enough capacity even after all returns - this is a resource constraint warning
          const totalAvailableCapacity = totalCapacityProvided + recoveredCapacity;
          console.warn(`[STORAGE SCHEDULE WARNING] ${crossing}: Only ${(totalAvailableCapacity/1000).toFixed(0)}k of ${(capacityNeeded/1000).toFixed(0)}k gal capacity available. Proceeding with partial allocation.`);
        }
      }

      // Recalculate available at actual dispatch time (both small AND large)
      let finalSmallAvailable = numSmallStorageBarges;
      let finalLargeAvailable = numLargeStorage;
      allocations.forEach(alloc => {
        if (alloc.returnTime > actualDispatchTime) {
          finalSmallAvailable -= alloc.smallCount;
          finalLargeAvailable -= alloc.largeCount;
        }
      });

      // Recalculate optimal allocation with barges available at dispatch time
      const finalLargeToAllocate = Math.min(
        Math.ceil(capacityNeeded / LARGE_BARGE_VOLUME),
        Math.max(0, finalLargeAvailable)
      );
      const finalCapacityFromLarge = finalLargeToAllocate * LARGE_BARGE_VOLUME;
      const finalRemainingCapacity = Math.max(0, capacityNeeded - finalCapacityFromLarge);
      const finalSmallToAllocate = Math.min(
        Math.ceil(finalRemainingCapacity / SMALL_BARGE_VOLUME),
        Math.max(0, finalSmallAvailable)
      );

      // Record this allocation
      allocations.push({
        hdd: crossing,
        smallCount: finalSmallToAllocate,
        largeCount: finalLargeToAllocate,
        returnTime: returnTime
      });

      // Calculate actual arrival time (dispatch time + travel and hookup)
      const actualArrivalTime = actualDispatchTime + travelAndHookup;

      // Check if arrival is late (after drilling starts at 7 AM on pilot START day)
      const drillingStartTime = pilotStartSimTime + DRILLING_START_HOUR; // 7 AM on pilot START day
      const isLate = actualArrivalTime > drillingStartTime;
      if (isLate) {
        console.warn(`[STORAGE SCHEDULE WARNING] ${crossing}: Storage will arrive AFTER drilling starts! Arrival: ${actualArrivalTime.toFixed(0)}h, Drilling: ${drillingStartTime.toFixed(0)}h`);
      }

      // Check if this HDD is a transfer source (barges will transfer to another HDD)
      const transfersTo = sameRigTransferSources.get(crossing) || null;

      deliveries.push({
        hdd: crossing,
        rig,
        source: closestSource,
        dispatchTime: actualDispatchTime,
        arrivalTime: actualArrivalTime,
        pilotStartTime: pilotStartSimTime,
        smallBarges: finalSmallToAllocate,
        largeBarges: finalLargeToAllocate,
        targetCapacity: req.targetCapacity,
        dailyDemand: req.dailyDemand,
        status: 'pending',
        waitedForReturns: waitingForReturns,
        isLateArrival: isLate,
        transfersTo: transfersTo  // HDD that will receive barges via same-rig transfer
      });

      const finalCapacity = (finalSmallToAllocate * SMALL_BARGE_VOLUME) + (finalLargeToAllocate * LARGE_BARGE_VOLUME);
      const transferNote = transfersTo ? ` [TRANSFERS->${transfersTo}]` : '';
      console.log(`[STORAGE SCHEDULE] ${crossing} (${rig}): dispatch@${actualDispatchTime.toFixed(0)}h, arrive@${actualArrivalTime.toFixed(0)}h, pilot@${pilotStartSimTime.toFixed(0)}h, ` +
        `allocate ${finalSmallToAllocate}S+${finalLargeToAllocate}L (${(finalCapacity/1000).toFixed(0)}k/${(capacityNeeded/1000).toFixed(0)}k gal)${waitingForReturns ? ' [WAITED]' : ''}${isLate ? ' [LATE]' : ''}${transferNote}`);
    });

    // Sort by dispatch time for execution
    deliveries.sort((a, b) => a.dispatchTime - b.dispatchTime);

    // =========================================================================
    // STEP 4: Summary statistics
    // =========================================================================
    const lateCount = deliveries.filter(d => d.isLateArrival).length;
    const waitedCount = deliveries.filter(d => d.waitedForReturns).length;
    const totalSmallAllocated = deliveries.reduce((sum, d) => sum + d.smallBarges, 0);

    const transferSourceCount = deliveries.filter(d => d.transfersTo).length;
    const transferTargetCount = sameRigTransferTargets.size;

    console.log(`[STORAGE SCHEDULE] === SUMMARY ===`);
    console.log(`[STORAGE SCHEDULE] Total deliveries scheduled: ${deliveries.length} (${transferTargetCount} HDDs receive via same-rig transfer)`);
    console.log(`[STORAGE SCHEDULE] Transfer sources: ${transferSourceCount} HDDs will transfer barges to next HDD`);
    console.log(`[STORAGE SCHEDULE] Total small barges allocated: ${totalSmallAllocated} (pool: ${numSmallStorageBarges})`);
    console.log(`[STORAGE SCHEDULE] Deliveries that waited for returns: ${waitedCount}`);
    console.log(`[STORAGE SCHEDULE] Deliveries arriving late (after pilot): ${lateCount}`);
    if (lateCount > 0) {
      console.warn(`[STORAGE SCHEDULE] WARNING: ${lateCount} HDDs will not have storage ready when drilling starts!`);
    }
    if (transferTargetCount > 0) {
      console.log(`[STORAGE SCHEDULE] Same-rig transfers: ${[...sameRigTransferTargets].join(', ')}`);
    }

    return deliveries;
  };

  const storageDeliverySchedule = buildStorageDeliverySchedule();
  console.log(`[STORAGE DELIVERY] Schedule built with ${storageDeliverySchedule.length} deliveries`);

  // Summary: Total storage barges required vs available
  const totalSmallStorageNeeded = storageDeliverySchedule.reduce((sum, d) => sum + d.smallBarges, 0);
  const totalLargeStorageNeeded = storageDeliverySchedule.reduce((sum, d) => sum + d.largeBarges, 0);
  const totalSmallStorageAvailable = state.barges.filter(b => b.role === 'storage-available' && b.type === 'small').length;
  const totalLargeStorageAvailable = state.barges.filter(b => b.role === 'storage-available' && b.type === 'large').length;
  console.log(`[STORAGE SUMMARY] Small barges: ${totalSmallStorageAvailable} available, ${totalSmallStorageNeeded} total needed across ${storageDeliverySchedule.length} HDDs`);
  console.log(`[STORAGE SUMMARY] Large barges: ${totalLargeStorageAvailable} available, ${totalLargeStorageNeeded} total needed across ${storageDeliverySchedule.length} HDDs`);
  if (totalSmallStorageNeeded > totalSmallStorageAvailable) {
    console.warn(`[STORAGE WARNING] Need ${totalSmallStorageNeeded - totalSmallStorageAvailable} more small storage barges! Will need to reuse barges.`);
  }
  console.log(`[STORAGE SUMMARY] Tugs available: ${state.tugs.length}`);
  console.log(`[STORAGE SCHEDULE] First 5 deliveries:`, storageDeliverySchedule.slice(0, 5).map(d => `${d.hdd}@${d.dispatchTime.toFixed(0)}h`).join(', '));

  // ============================================================================
  // PROACTIVE TRANSPORT SCHEDULING - STEP 1: Build Cumulative Demand Curves
  // ============================================================================
  // Feature flag for rollback safety
  const USE_SCHEDULED_DISPATCH = true;

  /**
   * Build cumulative demand curves for each HDD crossing.
   * This pre-computes water consumption for the entire project, giving full visibility
   * into future demand for optimal scheduling decisions.
   *
   * @returns {Object} demandCurves keyed by crossing, each containing:
   *   - crossing, rig, pilotStartSimTime, pilotEndSimTime, pullSimTime, totalDemand
   *   - hourlyDemand: array of {simTime, stage, hourlyRate, cumulativeDemand}
   */
  const buildCumulativeDemandCurve = () => {
    const demandCurves = {};

    hddSchedule.forEach(hdd => {
      const { crossing, rig } = hdd;
      const rigConfig = rigs[rig];

      // Convert phase dates to simTime (hours from minDate)
      // BUG FIX: Calculate pilot START date (day after rigUp ends), not END date
      const rigUpEndDate = new Date(hdd.rigUp);
      const pilotStartDate = new Date(rigUpEndDate);
      pilotStartDate.setDate(pilotStartDate.getDate() + 1); // Day after rigUp ends
      const pilotEndDate = new Date(hdd.pilot);
      const reamDate = new Date(hdd.ream);
      const swabDate = new Date(hdd.swab);
      const pullDate = new Date(hdd.pull);
      const rigDownDate = new Date(hdd.rigDown);

      const pilotStartSimTime = (pilotStartDate - minDate) / (1000 * 60 * 60);
      const pilotEndSimTime = (pilotEndDate - minDate) / (1000 * 60 * 60);
      const reamSimTime = (reamDate - minDate) / (1000 * 60 * 60);
      const swabSimTime = (swabDate - minDate) / (1000 * 60 * 60);
      const pullSimTime = (pullDate - minDate) / (1000 * 60 * 60);
      const rigDownSimTime = (rigDownDate - minDate) / (1000 * 60 * 60);

      // Build hourly demand array from pilot START to rigDown completion
      const hourlyDemand = [];
      let cumulativeDemand = 0;

      // Start from pilot START (when water consumption begins) to rigDown end
      // Water is consumed during drilling hours (7AM-5PM) only
      for (let simTime = pilotStartSimTime; simTime < rigDownSimTime; simTime += 1) {
        const hourOfDay = Math.floor(simTime % 24);
        const dateForSimTime = new Date(minDate.getTime() + simTime * 60 * 60 * 1000);
        const dayOfWeek = dateForSimTime.getDay();

        // Determine stage and hourly rate at this time
        let stage = 'RigDown';
        let hourlyRate = 0;

        const dateStr = dateForSimTime.toISOString().split('T')[0];
        if (dateStr <= hdd.pilot) {
          stage = 'Pilot';
          hourlyRate = rigConfig.pilot;
        } else if (dateStr <= hdd.ream) {
          stage = 'Ream';
          hourlyRate = rigConfig.ream;
        } else if (dateStr <= hdd.swab) {
          stage = 'Swab';
          hourlyRate = rigConfig.swab;
        } else if (dateStr <= hdd.pull) {
          stage = 'Pull';
          hourlyRate = rigConfig.pull;
        }

        // No consumption on Sundays if worksSundays is false
        if (dayOfWeek === 0 && !hdd.worksSundays) {
          hourlyRate = 0;
        }

        // Water only consumed during drilling hours (7AM-5PM)
        const isDrilling = hourOfDay >= DRILLING_START_HOUR && hourOfDay < DRILLING_END_HOUR;
        const consumptionThisHour = isDrilling ? hourlyRate : 0;

        cumulativeDemand += consumptionThisHour;

        hourlyDemand.push({
          simTime,
          hourOfDay,
          stage,
          hourlyRate: consumptionThisHour,
          cumulativeDemand
        });
      }

      demandCurves[crossing] = {
        crossing,
        rig,
        pilotStartSimTime,  // BUG FIX: Use pilot START time
        pilotEndSimTime,
        pullSimTime,
        rigDownSimTime,
        totalDemand: cumulativeDemand,
        peakDailyDemand: Math.max(rigConfig.pilot, rigConfig.ream, rigConfig.swab, rigConfig.pull) * (DRILLING_END_HOUR - DRILLING_START_HOUR),
        hourlyDemand
      };
    });

    return demandCurves;
  };

  const demandCurves = USE_SCHEDULED_DISPATCH ? buildCumulativeDemandCurve() : {};
  if (USE_SCHEDULED_DISPATCH) {
    console.log(`[DEMAND CURVES] Built demand curves for ${Object.keys(demandCurves).length} HDDs`);
    Object.entries(demandCurves).forEach(([crossing, curve]) => {
      console.log(`[DEMAND CURVES] ${crossing}: totalDemand=${Math.round(curve.totalDemand/1000)}K gal, peakDaily=${Math.round(curve.peakDailyDemand/1000)}K, hours=${curve.hourlyDemand.length}`);
    });
  }

  // ============================================================================
  // PROACTIVE TRANSPORT SCHEDULING - STEP 2: Build Transport Delivery Schedule
  // ============================================================================

  /**
   * Look up planned storage capacity for an HDD from storage delivery schedule
   */
  const getPlannedStorageCapacity = (crossing) => {
    const storageDelivery = storageDeliverySchedule.find(d => d.hdd === crossing);
    if (!storageDelivery) return MIN_STORAGE_CAPACITY_PER_RIG;

    const smallCapacity = storageDelivery.smallBarges * SMALL_BARGE_VOLUME;
    const largeCapacity = storageDelivery.largeBarges * LARGE_BARGE_VOLUME;
    return smallCapacity + largeCapacity;
  };

  /**
   * Calculate optimal source and delivery timing for a transport delivery
   * Returns best source based on CYCLE TIME (fill time + travel time), not just distance
   */
  const scheduleOptimalDelivery = (crossing, needTime, volumeNeeded) => {
    // Evaluate all sources
    let bestSource = null;
    let bestCycleTime = Infinity;
    let bestDistance = Infinity;

    Object.keys(sourceConfig).forEach(sourceId => {
      const distance = getDistance(sourceId, crossing);
      const flowRate = sourceConfig[sourceId].flowRate || 400;

      // Calculate cycle time = fill time + round trip travel time
      // This properly values fast sources (800 GPM) over slow but close sources (200 GPM)
      const fillTimeHrs = SMALL_BARGE_VOLUME / (flowRate * 60); // Hours to fill one barge
      const roundTripHrs = (2 * distance) / loadedSpeedSmall; // Round trip travel time
      const cycleTime = fillTimeHrs + roundTripHrs;

      if (cycleTime < bestCycleTime) {
        bestCycleTime = cycleTime;
        bestSource = sourceId;
        bestDistance = distance;
      }
    });

    // Calculate dispatch time by working backward from need time
    // Account for: travel time (loaded) + fill time + hookup time
    const travelTime = bestDistance / loadedSpeedSmall;
    const fillTime = (2 * SMALL_BARGE_VOLUME) / (sourceConfig[bestSource]?.flowRate || 400) / 60; // Convert GPM to hours
    const prepTime = travelTime + fillTime + hookupTime;

    const dispatchTime = Math.max(0, needTime - prepTime);
    const arrivalTime = dispatchTime + travelTime + fillTime;

    return {
      source: bestSource,
      distance: bestDistance,
      dispatchTime,
      arrivalTime,
      travelTime
    };
  };

  /**
   * Build the complete transport delivery schedule for all HDDs
   * Simulates storage levels forward in time and schedules deliveries when needed
   */
  const buildTransportDeliverySchedule = () => {
    const schedule = [];
    let deliveryIdCounter = 1;

    // Process each HDD
    hddSchedule.forEach(hdd => {
      const { crossing, rig } = hdd;
      const curve = demandCurves[crossing];
      if (!curve || curve.hourlyDemand.length === 0) return;

      const storageCapacity = getPlannedStorageCapacity(crossing);
      const peakDailyDemand = curve.peakDailyDemand;

      // Scheduling thresholds
      const targetBuffer = Math.min(peakDailyDemand * 1.5, storageCapacity); // 150% of daily demand
      const triggerThreshold = peakDailyDemand * 0.5; // Schedule when below 50% of daily demand

      // Get storage arrival time from storage delivery schedule
      const storageDelivery = storageDeliverySchedule.find(d => d.hdd === crossing);
      const storageArrivalTime = storageDelivery ? storageDelivery.arrivalTime : curve.pilotStartSimTime - 24;

      // Simulate storage levels forward in time
      let simulatedStorageLevel = 0;
      let lastScheduledDeliveryArrival = -999;

      // Track scheduled deliveries for this HDD to know when water will arrive
      const scheduledForThisHDD = [];

      for (let i = 0; i < curve.hourlyDemand.length; i++) {
        const hourData = curve.hourlyDemand[i];
        const simTime = hourData.simTime;

        // Storage barges arrive with water (pre-filled)
        // Assume storage arrives with 80% fill
        if (simTime >= storageArrivalTime && simulatedStorageLevel === 0) {
          simulatedStorageLevel = storageCapacity * 0.8;
        }

        // Add water from scheduled deliveries that have arrived
        scheduledForThisHDD.forEach(del => {
          if (del.arrivalTime <= simTime && !del.waterAdded) {
            simulatedStorageLevel += del.volumeToDeliver;
            del.waterAdded = true;
          }
        });

        // Subtract consumption
        simulatedStorageLevel = Math.max(0, simulatedStorageLevel - hourData.hourlyRate);

        const timeSinceLastScheduled = simTime - lastScheduledDeliveryArrival;

        // =====================================================================
        // REACTIVE SCHEDULING (backup for scheduled dispatch mode)
        // With continuous ops enabled in simulation, this is less critical
        // =====================================================================
        if (simTime >= storageArrivalTime &&
            simulatedStorageLevel < triggerThreshold &&
            timeSinceLastScheduled >= 4) {

          // Calculate how much water we need
          const deficit = targetBuffer - simulatedStorageLevel;
          const volumeToDeliver = Math.min(deficit, 2 * SMALL_BARGE_VOLUME); // Max 2 barges per delivery

          if (volumeToDeliver >= SMALL_BARGE_VOLUME * 0.5) { // Only schedule if meaningful amount
            // Schedule optimal delivery
            const deliveryPlan = scheduleOptimalDelivery(crossing, simTime, volumeToDeliver);

            const delivery = {
              id: `TD-${String(deliveryIdCounter++).padStart(3, '0')}`,
              hdd: crossing,
              rig,
              source: deliveryPlan.source,
              dispatchTime: deliveryPlan.dispatchTime,
              arrivalTime: deliveryPlan.arrivalTime,
              bargeType: 'small',
              bargeCount: Math.ceil(volumeToDeliver / SMALL_BARGE_VOLUME),
              volumeToDeliver,
              assignedTug: null, // Will be assigned later
              priority: 'scheduled',
              status: 'pending',
              triggerStorageLevel: simulatedStorageLevel,
              triggerTime: simTime
            };

            schedule.push(delivery);
            scheduledForThisHDD.push(delivery);
            lastScheduledDeliveryArrival = deliveryPlan.arrivalTime;

            // Add the water to simulation (optimistic - assumes delivery succeeds)
            // This prevents scheduling too many deliveries
            simulatedStorageLevel += volumeToDeliver;
          }
        }
      }
    });

    // Sort by dispatch time
    schedule.sort((a, b) => a.dispatchTime - b.dispatchTime);

    // Assign tugs to prevent conflicts
    assignTugsToSchedule(schedule);

    return schedule;
  };

  /**
   * Assign tugs to scheduled deliveries to prevent conflicts
   * Uses a simple greedy algorithm: assign each delivery to first available tug
   */
  const assignTugsToSchedule = (schedule) => {
    // Track when each tug will be available
    // Tugs are: T1, T2, T3 (based on numSmallTugs)
    const tugAvailability = {};
    for (let i = 1; i <= numSmallTugs; i++) {
      tugAvailability[`T${i}`] = 0; // Available from time 0
    }

    schedule.forEach(delivery => {
      // Find first available tug
      let assignedTug = null;
      let earliestAvailable = Infinity;

      Object.entries(tugAvailability).forEach(([tugId, availableTime]) => {
        if (availableTime <= delivery.dispatchTime && availableTime < earliestAvailable) {
          assignedTug = tugId;
          earliestAvailable = availableTime;
        }
      });

      if (assignedTug) {
        delivery.assignedTug = assignedTug;
        // Mark tug as busy until delivery completes + return time
        const returnTime = delivery.arrivalTime + (getDistance(delivery.source, delivery.hdd) / emptySpeed) + 2; // +2 hours for pumping
        tugAvailability[assignedTug] = returnTime;
      } else {
        // No tug available at dispatch time - find earliest and defer
        let earliestTug = null;
        let earliestTime = Infinity;
        Object.entries(tugAvailability).forEach(([tugId, availableTime]) => {
          if (availableTime < earliestTime) {
            earliestTug = tugId;
            earliestTime = availableTime;
          }
        });

        if (earliestTug) {
          delivery.assignedTug = earliestTug;
          delivery.originalDispatchTime = delivery.dispatchTime;
          delivery.dispatchTime = earliestTime;
          delivery.arrivalTime = earliestTime + (getDistance(delivery.source, delivery.hdd) / loadedSpeedSmall);
          delivery.priority = 'deferred';

          const returnTime = delivery.arrivalTime + (getDistance(delivery.source, delivery.hdd) / emptySpeed) + 2;
          tugAvailability[earliestTug] = returnTime;
        }
      }
    });
  };

  // Build the transport delivery schedule
  const transportDeliverySchedule = USE_SCHEDULED_DISPATCH ? buildTransportDeliverySchedule() : [];
  if (USE_SCHEDULED_DISPATCH && transportDeliverySchedule.length > 0) {
    console.log(`[TRANSPORT SCHEDULE] Built schedule with ${transportDeliverySchedule.length} deliveries`);
    // Log first 5 deliveries
    transportDeliverySchedule.slice(0, 5).forEach(d => {
      console.log(`[TRANSPORT SCHEDULE] ${d.id}: ${d.hdd} from ${d.source}, dispatch@${d.dispatchTime.toFixed(1)}hrs, arrive@${d.arrivalTime.toFixed(1)}hrs, ${d.bargeCount} barges (${Math.round(d.volumeToDeliver/1000)}K gal), tug=${d.assignedTug}, priority=${d.priority}`);
    });
    if (transportDeliverySchedule.length > 5) {
      console.log(`[TRANSPORT SCHEDULE] ... and ${transportDeliverySchedule.length - 5} more deliveries`);
    }
  }

  // =========================================================================
  // BOOTSTRAP MOBILIZATION FOR EARLY DELIVERIES
  // Now that schedules are built, mobilize assets for very early deliveries
  // Without this, the first HDD (pilot at simTime=0) won't have storage in time
  // =========================================================================
  const BOOTSTRAP_THRESHOLD = 48; // Hours - bootstrap anything needing dispatch within this window

  console.log('[BOOTSTRAP] Checking for early deliveries requiring bootstrap mobilization...');

  let bootstrapCount = 0;

  // Bootstrap storage deliveries that need early dispatch
  storageDeliverySchedule.forEach(delivery => {
    if (delivery.dispatchTime <= BOOTSTRAP_THRESHOLD && delivery.status === 'pending') {
      console.log(`[BOOTSTRAP] Storage delivery ${delivery.hdd} needs early dispatch@${delivery.dispatchTime.toFixed(0)}h - mobilizing assets now`);

      // Mobilize storage barges needed for this delivery
      const largeNeeded = delivery.largeBarges;
      const smallNeeded = delivery.smallBarges;

      // Mobilize large storage barges first
      for (let i = 0; i < largeNeeded && state.unmobilizedBarges.largeStorage > 0; i++) {
        mobilizeBarge('large', 'storage-available', delivery.source, 0, `bootstrap storage for ${delivery.hdd}`);
        bootstrapCount++;
      }

      // Mobilize small storage barges
      for (let i = 0; i < smallNeeded && state.unmobilizedBarges.smallStorage > 0; i++) {
        mobilizeBarge('small', 'storage-available', delivery.source, 0, `bootstrap storage for ${delivery.hdd}`);
        bootstrapCount++;
      }

      // Mobilize at least 1 tug for the delivery (storage needs dedicated tug)
      if (state.unmobilizedTugs > 0) {
        mobilizeTug(delivery.source, 0, `bootstrap for storage delivery ${delivery.hdd}`);
        bootstrapCount++;
      }

      delivery.bargesMobilized = true;
      delivery.tugsMobilized = true;
    }
  });

  // Also bootstrap transport barges and tugs if there's early transport demand
  transportDeliverySchedule.forEach(delivery => {
    if (delivery.dispatchTime <= BOOTSTRAP_THRESHOLD && delivery.status === 'pending') {
      console.log(`[BOOTSTRAP] Transport delivery ${delivery.id} needs early dispatch@${delivery.dispatchTime.toFixed(0)}h - mobilizing assets now`);

      // Mobilize transport barges (prefer large for efficiency)
      const bargesNeeded = delivery.bargeCount || 2;
      const largeToBoot = Math.min(
        Math.ceil(bargesNeeded * SMALL_BARGE_VOLUME / LARGE_BARGE_VOLUME),
        state.unmobilizedBarges.largeTransport
      );

      for (let i = 0; i < largeToBoot; i++) {
        mobilizeBarge('large', 'available', delivery.source, 0, `bootstrap transport ${delivery.id}`);
        bootstrapCount++;
      }

      // If no large available, use small
      if (largeToBoot === 0) {
        for (let i = 0; i < bargesNeeded && state.unmobilizedBarges.smallTransport > 0; i++) {
          mobilizeBarge('small', 'available', delivery.source, 0, `bootstrap transport ${delivery.id}`);
          bootstrapCount++;
        }
      }

      // Mobilize tug if needed
      const idleTugsAtSource = state.tugs.filter(t =>
        t.currentLocation === delivery.source && t.status === 'idle'
      ).length;
      if (idleTugsAtSource === 0 && state.unmobilizedTugs > 0) {
        mobilizeTug(delivery.source, 0, `bootstrap for transport ${delivery.id}`);
        bootstrapCount++;
      }

      delivery.bargesMobilized = true;
    }
  });

  if (bootstrapCount > 0) {
    console.log(`[BOOTSTRAP] Mobilized ${bootstrapCount} assets for early deliveries`);
    // Re-initialize kernel to include bootstrapped assets
    kernel.initializeIndexes(state.tugs, state.barges, sourceIds);
    console.log('[KERNEL] Re-initialized after bootstrap:', {
      tugs: state.tugs.length,
      barges: state.barges.length,
      sources: sourceIds.length
    });
  } else {
    console.log('[BOOTSTRAP] No early deliveries requiring bootstrap mobilization');
  }

  console.log(`[BOOTSTRAP] Unmobilized pool after bootstrap: ${state.unmobilizedTugs} tugs, ${state.unmobilizedBarges.smallTransport} ST, ${state.unmobilizedBarges.largeTransport} LT, ${state.unmobilizedBarges.smallStorage} SS, ${state.unmobilizedBarges.largeStorage} LS`);

  // Check if any HDDs need storage delivered from source (for barges that returned to source)
  const checkForHDDsNeedingStorage = (simTime) => {
    // Find HDDs that need storage, sorted by rigUp time
    const hddsNeedingStorage = [];

    hddSchedule.forEach(hdd => {
      const { crossing, rigUp } = hdd;

      // Check if this HDD doesn't have storage yet
      if (state.hddStorage[crossing] && state.hddStorage[crossing].stationedBarges.length > 0) {
        return; // Already has storage
      }

      // Check if there's a scheduled delivery pending for this HDD
      const scheduledDelivery = storageDeliverySchedule.find(d => d.hdd === crossing);
      if (scheduledDelivery && scheduledDelivery.status === 'pending') {
        return; // Already have a scheduled delivery, don't duplicate
      }

      // Check if there are barges already en-route to this HDD
      const bargesEnRoute = state.barges.filter(b =>
        b.status === 'en-route-storage' && b.assignedHDD === crossing
      );
      if (bargesEnRoute.length > 0) {
        return; // Storage already on the way
      }

      // Check if there are tugs en-route with storage barges to this HDD
      const tugsEnRoute = state.tugs.filter(t =>
        t.status === 'en-route-storage' && t.targetCrossing === crossing
      );
      if (tugsEnRoute.length > 0) {
        return; // Storage delivery already in progress
      }

      // Check if rigUp is coming (within next 30 days / 720 hours)
      const rigUpDate = new Date(rigUp);
      const rigUpSimTime = (rigUpDate - minDate) / (1000 * 60 * 60);
      const hoursUntilRigUp = rigUpSimTime - simTime;

      // Forward-looking: dispatch up to 30 days (720 hours) in advance
      const LOOKAHEAD_HOURS = 720; // 30 days

      if (hoursUntilRigUp >= 0 && hoursUntilRigUp <= LOOKAHEAD_HOURS) {
        hddsNeedingStorage.push({
          hdd,
          crossing,
          rigUpSimTime,
          hoursUntilRigUp
        });
      }
    });

    // Sort by urgency (soonest rigUp first)
    hddsNeedingStorage.sort((a, b) => a.hoursUntilRigUp - b.hoursUntilRigUp);

    // Try to dispatch storage to each HDD that needs it
    for (const { hdd, crossing } of hddsNeedingStorage) {
      // Count available storage barges across ALL sources
      // Storage barges are dispatched EMPTY - they provide capacity, not supply
      const allAvailableSmallStorage = state.barges.filter(b =>
        b.type === 'small' &&
        b.role === 'storage-available' &&
        b.assignedTugId === null &&
        b.status !== 'en-route-storage' && // Not already dispatched
        b.status !== 'stationed' // Not already at an HDD
      );

      const allAvailableLargeStorage = state.barges.filter(b =>
        b.type === 'large' &&
        b.role === 'storage-available' &&
        b.assignedTugId === null &&
        b.status !== 'en-route-storage' &&
        b.status !== 'stationed'
      );

      // Dispatch whatever barges are available (flexible allocation)
      if (allAvailableSmallStorage.length > 0 || allAvailableLargeStorage.length > 0) {
        // Try each source, prefer closest to HDD
        const sourcesByDistance = Object.keys(sourceConfig)
          .map(sourceId => ({ sourceId, dist: getDistance(sourceId, crossing) }))
          .sort((a, b) => a.dist - b.dist);

        for (const { sourceId } of sourcesByDistance) {
          // Find available barges at THIS source
          const smallAtSource = allAvailableSmallStorage.filter(b => b.currentLocation === sourceId);
          const largeAtSource = allAvailableLargeStorage.filter(b => b.currentLocation === sourceId);

          const numSmall = smallAtSource.length;
          const numLarge = largeAtSource.length;

          if (numSmall > 0 || numLarge > 0) {
            // Create delivery task
            const delivery = {
              hdd: crossing,
              rig: hdd.rig,
              source: sourceId,
              smallBarges: numSmall,
              largeBarges: numLarge,
              status: 'pending'
            };

            // Try to dispatch
            const dispatched = dispatchStorageDelivery(delivery, simTime);
            if (dispatched) {
              break; // Successfully dispatched from this source
            }
          }
        }
      }
    }
  };

  // Build chronological HDD list for barge transfers (ANY rig)
  const buildChronologicalHDDList = () => {
    const hddList = [];

    hddSchedule.forEach(hdd => {
      const pullDate = new Date(hdd.pull);
      const pullSimTime = (pullDate - minDate) / (1000 * 60 * 60);
      const rigUpDate = new Date(hdd.rigUp);
      const rigUpSimTime = (rigUpDate - minDate) / (1000 * 60 * 60);

      hddList.push({
        crossing: hdd.crossing,
        rig: hdd.rig,
        pullCompletionTime: pullSimTime,
        rigUpTime: rigUpSimTime,
        start: hdd.start
      });
    });

    // Sort by pull completion time (chronological order)
    hddList.sort((a, b) => a.pullCompletionTime - b.pullCompletionTime);

    return hddList;
  };

  const chronologicalHDDList = buildChronologicalHDDList();

  // Find the next HDD that needs storage (any rig) - returns HDD regardless of timing
  const getNextHDDNeedingStorage = (currentHDD) => {
    const currentIndex = chronologicalHDDList.findIndex(h => h.crossing === currentHDD);

    if (currentIndex === -1) return null;

    // Look for next HDD that hasn't been stationed yet
    for (let i = currentIndex + 1; i < chronologicalHDDList.length; i++) {
      const nextHDD = chronologicalHDDList[i];

      // Check if this HDD already has storage barges
      if (state.hddStorage[nextHDD.crossing] && state.hddStorage[nextHDD.crossing].stationedBarges.length > 0) {
        continue; // Already has storage, skip it
      }

      // Found next HDD that needs storage
      return nextHDD;
    }

    return null; // No more HDDs need storage
  };

  // Storage barge delivery functions
  const dispatchStorageDelivery = (delivery, simTime) => {
    const { hdd, smallBarges: numSmall, largeBarges: numLarge, rig } = delivery;
    console.log(`[STORAGE DISPATCH] Attempting to dispatch storage to ${hdd} at simTime=${simTime.toFixed(1)}: need ${numSmall} small + ${numLarge} large`);

    // Search ALL sources for available barges and tugs (barges may have returned to different source)
    let bestSource = null;
    let bestTug = null;
    let bestSmallBarges = [];
    let bestLargeBarges = [];

    // Check each source, prefer closest to HDD
    const sourcesByDistance = Object.keys(sourceConfig)
      .map(sourceId => ({ sourceId, dist: getDistance(sourceId, hdd) }))
      .sort((a, b) => a.dist - b.dist);

    // Minimum fill percentage required for storage barges before dispatch
    // Transport schedule assumes 80% fill (line 4391-4394), so require at least 50%
    const MIN_STORAGE_FILL_PERCENT = 0.5;

    for (const { sourceId } of sourcesByDistance) {
      // Find available storage barges at this source (role='storage-available')
      // REQUIRE pre-fill to match transport schedule assumptions
      const availableSmallStorage = state.barges.filter(b =>
        b.type === 'small' &&
        b.role === 'storage-available' &&
        b.currentLocation === sourceId &&
        b.assignedTugId === null &&
        b.status !== 'en-route-storage' && // Not already dispatched
        b.status !== 'stationed' && // Not already at an HDD
        b.fillLevel >= b.volume * MIN_STORAGE_FILL_PERCENT // REQUIRE pre-fill
      ).sort((a, b) => b.fillLevel - a.fillLevel); // Prefer fuller barges

      const availableLargeStorage = state.barges.filter(b =>
        b.type === 'large' &&
        b.role === 'storage-available' &&
        b.currentLocation === sourceId &&
        b.assignedTugId === null &&
        b.status !== 'en-route-storage' &&
        b.status !== 'stationed' &&
        b.fillLevel >= b.volume * MIN_STORAGE_FILL_PERCENT // REQUIRE pre-fill
      ).sort((a, b) => b.fillLevel - a.fillLevel); // Prefer fuller barges

      // Check if we have enough barges at this source
      if (availableSmallStorage.length >= numSmall && availableLargeStorage.length >= numLarge) {
        // Find idle tug at this source
        const tug = state.tugs.find(t =>
          t.status === 'idle' &&
          t.currentLocation === sourceId
        );

        if (tug) {
          bestSource = sourceId;
          bestTug = tug;
          bestSmallBarges = availableSmallStorage.slice(0, numSmall);
          bestLargeBarges = availableLargeStorage.slice(0, numLarge);
          break; // Found a valid source, use it
        }
      }
    }

    // FALLBACK: If no source has both tug AND barges, send tug from another source
    if (!bestSource || !bestTug) {
      // Find any source with enough PRE-FILLED storage barges
      let bargeSource = null;
      let bargesAtSource = [];
      const minFillPercent = 0.5; // Same threshold as above

      for (const { sourceId } of sourcesByDistance) {
        const availableSmall = state.barges.filter(b =>
          b.type === 'small' && b.role === 'storage-available' &&
          b.currentLocation === sourceId && !b.assignedTugId &&
          b.status !== 'en-route-storage' && b.status !== 'stationed' &&
          b.fillLevel >= b.volume * minFillPercent // FIX: Must be pre-filled!
        );
        if (availableSmall.length >= numSmall) {
          bargeSource = sourceId;
          bargesAtSource = availableSmall.slice(0, numSmall);
          break;
        }
      }

      // Find any idle tug (anywhere)
      const idleTug = state.tugs.find(t => t.status === 'idle');

      if (bargeSource && idleTug && idleTug.currentLocation !== bargeSource) {
        // Dispatch tug to pick up barges from bargeSource, then deliver to HDD
        const tugSource = idleTug.currentLocation;
        const travelToBargeSource = getDistance(tugSource, bargeSource) / emptySpeed;
        const travelToHDD = getDistance(bargeSource, hdd) / loadedSpeedSmall;

        console.log(`[STORAGE DISPATCH CROSS-SOURCE] Sending ${idleTug.id} from ${tugSource} to ${bargeSource} to pick up barges for ${hdd}`);

        // Set up two-leg journey
        idleTug.status = 'en-route-pickup';
        idleTug.targetCrossing = bargeSource;  // First leg: go to barge source
        idleTug.pickupBargeIds = bargesAtSource.map(b => b.id);
        idleTug.finalDestination = hdd;
        idleTug.finalRig = rig;
        idleTug.arrivalHour = simTime + travelToBargeSource;
        idleTug.storageDelivery = delivery;

        // Reserve the barges
        bargesAtSource.forEach(b => {
          b.status = 'reserved-for-pickup';
          b.reservedByTug = idleTug.id;
        });

        logAssetAction(simTime, 'Tug', idleTug.id, 'en-route-pickup', `to ${bargeSource} for storage pickup`, {
          bargeIds: idleTug.pickupBargeIds,
          finalDestination: hdd
        });

        delivery.status = 'dispatched';
        return true;
      }

      // No solution found - log debug info
      if (!delivery._lastDebugLog || simTime - delivery._lastDebugLog >= 10) {
        delivery._lastDebugLog = simTime;
        console.log(`[STORAGE DISPATCH FAIL] ${hdd}: Need ${numSmall} small + ${numLarge} large barges`);
        Object.keys(sourceConfig).forEach(sourceId => {
          const smallAtSource = state.barges.filter(b =>
            b.type === 'small' && b.role === 'storage-available' &&
            b.currentLocation === sourceId && !b.assignedTugId
          ).length;
          const largeAtSource = state.barges.filter(b =>
            b.type === 'large' && b.role === 'storage-available' &&
            b.currentLocation === sourceId && !b.assignedTugId
          ).length;
          const idleTugsAtSource = state.tugs.filter(t =>
            t.status === 'idle' && t.currentLocation === sourceId
          ).length;
          const allTugsAtSource = state.tugs.filter(t => t.currentLocation === sourceId);
          console.log(`  ${sourceId}: ${smallAtSource} small-storage, ${largeAtSource} large-storage, ${idleTugsAtSource} idle tugs (${allTugsAtSource.map(t => `${t.id}:${t.status}`).join(', ')})`);
        });
      }
      return false; // No solution found
    }

    const source = bestSource;
    const tug = bestTug;

    // Assign barges to tug using kernel
    const bargesToDeliver = [...bestSmallBarges, ...bestLargeBarges];
    const bargeIdsToDeliver = bargesToDeliver.map(b => b.id);

    // Safety check: ensure we have barges to deliver
    if (bargesToDeliver.length === 0) {
      console.warn(`[STORAGE DISPATCH] No barges to deliver to ${hdd} (numSmall=${numSmall}, numLarge=${numLarge})`);
      return false;
    }

    // Use kernel to attach barges (handles indexes and assignedTugId)
    kernel.attachBarges(tug.id, bargeIdsToDeliver, state.barges, simTime, 'dispatchStorageDelivery');

    // Set storage-specific properties on barges
    bargesToDeliver.forEach(b => {
      b.role = 'storage'; // Mark as storage barge
      b.assignedHDD = hdd;
      b.status = 'en-route-storage';

      // Legacy sync: Remove from available pool
      const sourceState = state.sources[source];
      const idx = sourceState.availableBarges.indexOf(b.id);
      if (idx >= 0) {
        sourceState.availableBarges.splice(idx, 1);
      }
    });

    // Setup tug for delivery
    // Legacy sync: keep tug.attachedBargeIds in sync
    tug.attachedBargeIds = kernel.getAttachedBarges(tug.id);
    tug.status = 'en-route-storage';
    tug.targetCrossing = hdd;
    tug.targetRig = rig;

    const distance = getDistance(source, hdd);
    // Determine if carrying large barges (use first barge type as indicator)
    const hasLargeBarges = bargesToDeliver.some(b => b.type === 'large');
    const travelTime = getTravelTime(distance, true, hasLargeBarges);
    tug.arrivalHour = simTime + travelTime;

    // Log dispatch
    logAssetAction(simTime, 'Tug', tug.id, 'en-route-storage', `to ${hdd}`, {
      attachedBargeIds: tug.attachedBargeIds,
      purpose: 'storage-delivery',
      sourceId: source,
      targetCrossing: hdd,
      targetRig: rig
    });

    bargesToDeliver.forEach(b => {
      logAssetAction(simTime, `Barge (${b.type})`, b.id, 'en-route-storage', `to ${hdd}`, {
        fillLevel: b.fillLevel,
        capacity: b.volume,
        usage: 'storage',
        assignedHDD: hdd,
        tugId: tug.id
      });
    });

    delivery.status = 'dispatched';
    console.log(`[STORAGE DISPATCH SUCCESS] ${hdd}: Dispatched ${bargesToDeliver.length} barges from ${source}, arrival@${tug.arrivalHour.toFixed(1)}hrs`);
    return true;
  };

  // ============================================================================
  // PROACTIVE TRANSPORT SCHEDULING - STEP 3: Dispatch Scheduled Transport
  // ============================================================================

  /**
   * Execute a scheduled transport delivery
   * Finds the assigned tug (or fallback to any available), attaches barges, and dispatches
   *
   * @param {Object} delivery - The scheduled delivery object
   * @param {number} simTime - Current simulation time
   * @returns {boolean} - True if dispatch succeeded, false otherwise
   */
  const dispatchScheduledTransport = (delivery, simTime) => {
    const { hdd, rig, source, assignedTug, bargeCount, volumeToDeliver } = delivery;

    console.log(`[SCHEDULED DISPATCH] Attempting ${delivery.id}: ${hdd} from ${source}, assigned tug=${assignedTug}`);

    // Helper: check if a source has available transport barges with water
    const hasAvailableBargesAtSource = (sourceId) => {
      return state.barges.some(b =>
        b.type === 'small' &&
        b.role !== 'storage' &&
        b.role !== 'storage-available' &&
        b.currentLocation === sourceId &&
        b.assignedTugId === null &&
        b.fillLevel > 0 && // MUST have actual water
        ['idle', 'queued', 'filling'].includes(b.status)
      );
    };

    // Find the assigned tug, or fall back to any available tug at the source
    let tug = state.tugs.find(t => t.id === assignedTug && t.status === 'idle' && t.currentLocation === source);

    // Fallback: try any idle tug at the scheduled source
    if (!tug) {
      tug = state.tugs.find(t => t.status === 'idle' && t.currentLocation === source);
    }

    // Fallback: try any idle tug at a source WITH available barges
    if (!tug) {
      tug = state.tugs.find(t => t.status === 'idle' && hasAvailableBargesAtSource(t.currentLocation));
    }

    if (!tug) {
      const tugStates = state.tugs.map(t => `${t.id}:${t.status}@${t.currentLocation}`).join(', ');
      const sourcesWithBarges = Object.keys(sourceConfig).filter(s => hasAvailableBargesAtSource(s));
      console.log(`[SCHEDULE WARNING] ${delivery.id}: No tug available for ${hdd}. Tugs:[${tugStates}] BargesAt:[${sourcesWithBarges.join(',')}]`);
      return false;
    }

    // Get available barges at the tug's current location (must have water or be filling)
    // UNIFIED MODEL: Allow queued/filling barges (they'll have water soon)
    const tugSource = tug.currentLocation;
    const availableBarges = state.barges.filter(b =>
      b.type === 'small' &&
      b.role !== 'storage' &&
      b.role !== 'storage-available' &&
      b.currentLocation === tugSource &&
      b.assignedTugId === null &&
      b.fillLevel > 0 && // MUST have actual water
      ['idle', 'queued', 'filling'].includes(b.status)
    ).sort((a, b) => b.fillLevel - a.fillLevel); // Prefer fuller barges

    if (availableBarges.length === 0) {
      console.log(`[SCHEDULE WARNING] ${delivery.id}: No barges with water at ${tugSource} for delivery to ${hdd}`);
      return false;
    }

    // Select barges (up to bargeCount), prefer fuller ones (already sorted)
    const bargesToAttach = availableBarges.slice(0, Math.min(bargeCount, 2));

    // Calculate actual volume being delivered
    const actualVolume = bargesToAttach.reduce((sum, b) => sum + b.fillLevel, 0);

    // Get the rig's current HDD crossing for delivery
    const rigStatus = getRigStatus(rig, getDateStrFromTime(simTime));
    const targetCrossing = rigStatus.hdd || hdd;

    // Use kernel to attach barges
    const bargeIds = bargesToAttach.map(b => b.id);
    kernel.attachBarges(tug.id, bargeIds, state.barges, simTime, 'dispatchScheduledTransport');

    // Update barge states
    bargesToAttach.forEach(b => {
      b.status = 'en-route-loaded';
      b.assignedTugId = tug.id;

      // Remove from source pool
      const sourceState = state.sources[tugSource];
      if (sourceState) {
        const idx = sourceState.availableBarges.indexOf(b.id);
        if (idx >= 0) sourceState.availableBarges.splice(idx, 1);
      }
    });

    // Update tug state
    tug.attachedBargeIds = kernel.getAttachedBarges(tug.id);
    tug.status = 'en-route-loaded';
    tug.targetCrossing = targetCrossing;
    tug.targetRig = rig;
    tug.totalVolumeRemaining = actualVolume;
    tug.deliveryPlan = [{
      rig,
      crossing: targetCrossing,
      volumeToDeliver: actualVolume,
      urgency: 350 // Scheduled deliveries have moderate priority
    }];
    tug.currentStopIndex = 0;
    tug.pumpPhase = 'none';

    // Calculate arrival time
    const distance = getDistance(tugSource, targetCrossing);
    const isLarge = bargesToAttach.some(b => b.type === 'large');
    const travelTime = getTravelTime(distance, true, isLarge);
    tug.arrivalHour = simTime + travelTime;

    // Log the dispatch
    logAssetAction(simTime, 'Tug', tug.id, 'en-route-loaded', `to ${targetCrossing}`, {
      attachedBargeIds: tug.attachedBargeIds,
      totalVolume: actualVolume,
      purpose: 'scheduled-transport',
      deliveryId: delivery.id,
      sourceId: tugSource,
      targetCrossing: targetCrossing
    });

    bargesToAttach.forEach(b => {
      logAssetAction(simTime, `Barge (${b.type})`, b.id, 'en-route-loaded', `to ${targetCrossing}`, {
        fillLevel: b.fillLevel,
        capacity: b.volume,
        tugId: tug.id,
        deliveryId: delivery.id
      });
    });

    console.log(`[SCHEDULED DISPATCH] SUCCESS ${delivery.id}: ${tug.id} with ${bargesToAttach.length} barges (${Math.round(actualVolume/1000)}K gal) to ${targetCrossing}, ETA=${tug.arrivalHour.toFixed(1)}hrs`);

    return true;
  };

  const stationBargesAtHDD = (tug, simTime) => {
    const { targetCrossing: hdd, targetRig: rig } = tug;
    // Get attached barges from kernel (canonical source)
    const attachedBargeIds = kernel.getAttachedBarges(tug.id);

    // Initialize HDD storage if needed
    if (!state.hddStorage[hdd]) {
      state.hddStorage[hdd] = {
        stationedBarges: [],
        totalCapacity: 0,
        totalLevel: 0,
        bargeDetails: {},
        demobScheduled: false
      };
    }

    const hddStorage = state.hddStorage[hdd];

    // Station each barge at HDD using kernel
    attachedBargeIds.forEach(bargeId => {
      const barge = state.barges.find(b => b.id === bargeId);
      if (!barge) return;

      // Use kernel to station barge at HDD (detaches from tug, updates indexes)
      kernel.stationBargeAtHDD(bargeId, hdd, state.barges);

      // Update barge state for domain-specific properties
      barge.assignedTugId = null;
      barge.currentLocation = hdd;
      barge.status = 'stationed';
      barge.role = 'storage';
      barge.assignedHDD = hdd;

      // Add to HDD storage tracking
      hddStorage.stationedBarges.push(bargeId);
      hddStorage.totalCapacity += barge.volume;
      hddStorage.totalLevel += barge.fillLevel;
      hddStorage.bargeDetails[bargeId] = {
        level: barge.fillLevel,
        capacity: barge.volume,
        type: barge.type
      };

      // Track water arriving with storage barge as an injection event (for chart display)
      if (barge.fillLevel > 0 && injectionEventsByHDD[hdd]) {
        const hourKey = Math.floor(simTime);
        hourlyInjectionByHDD[hdd][hourKey] = (hourlyInjectionByHDD[hdd][hourKey] || 0) + barge.fillLevel;
        injectionEventsByHDD[hdd].push({
          timestamp: simTime,
          hourKey: hourKey,
          volume: barge.fillLevel,
          bargeIds: [bargeId],
          tugId: tug.id,
          type: 'storage-arrival'  // Distinguish from transport pumping
        });
      }

      // Log stationing
      logAssetAction(simTime, `Barge (${barge.type})`, bargeId, 'stationed', `at ${hdd}`, {
        fillLevel: barge.fillLevel,
        capacity: barge.volume,
        usage: 'storage',
        assignedHDD: hdd,
        rig
      });
    });

    // Track storage barge allocation change for chart
    if (storageBargeChangesByHDD[hdd] && attachedBargeIds.length > 0) {
      storageBargeChangesByHDD[hdd].push({
        timestamp: simTime,
        hourKey: Math.floor(simTime),
        bargeIds: [...hddStorage.stationedBarges],
        totalCapacity: hddStorage.totalCapacity,
        action: 'add'
      });
    }

    // Detach all barges from tug via kernel
    kernel.detachBarges(tug.id, attachedBargeIds, hdd, state.barges, simTime, 'stationBargesAtHDD');

    // Release tug - return empty to source
    // Legacy sync: keep tug.attachedBargeIds in sync
    tug.attachedBargeIds = [];
    tug.status = 'en-route-empty';
    tug.targetCrossing = null;
    tug.targetRig = null;

    const returnSource = selectReturnSource(tug.id, hdd);
    tug.currentLocation = returnSource; // Will actually be traveling, but update target
    tug.intendedReturnSource = returnSource;  // Store intended source for arrival handler
    const returnDist = getDistance(returnSource, hdd);
    const returnTime = getTravelTime(returnDist, false, false);
    tug.returnHour = simTime + returnTime;

    logAssetAction(simTime, 'Tug', tug.id, 'en-route-empty', `returning to ${returnSource}`, {
      attachedBargeIds: [],
      afterDelivery: 'storage',
      sourceId: returnSource,
      targetCrossing: hdd
    });
  };

  // Check if there's any future day with water demand across all rigs
  const hasFutureDemand = (currentSimTime, daysAhead = 90) => {
    const hoursAhead = daysAhead * 24;
    const endTime = currentSimTime + hoursAhead;

    // Check each day in the future window
    for (let futureTime = currentSimTime; futureTime < endTime; futureTime += 24) {
      const futureDateStr = getDateStrFromTime(futureTime);

      // Check if ANY rig is active on this day
      for (const rigName of rigNames) {
        const status = getRigStatus(rigName, futureDateStr);
        if (status.isActive && status.hourlyRate > 0) {
          return true; // Found at least one day with demand
        }
      }
    }

    return false; // No future demand found
  };

  // Calculate peak barge requirement in future window
  const getPeakFutureBargeRequirement = (currentSimTime, daysAhead = 90) => {
    const hoursAhead = daysAhead * 24;
    const endTime = Math.min(currentSimTime + hoursAhead, totalHours);

    let peakBarges = 0;

    // Check each day in the future window
    for (let futureTime = currentSimTime; futureTime < endTime; futureTime += 24) {
      const futureDateStr = getDateStrFromTime(futureTime);

      // Count active rigs on this day
      let activeRigs = 0;
      let totalDailyDemand = 0;

      for (const rigName of rigNames) {
        const status = getRigStatus(rigName, futureDateStr);
        if (status.isActive && status.hourlyRate > 0 && !['RigUp', 'RigDown'].includes(status.stage)) {
          activeRigs++;
          totalDailyDemand += status.hourlyRate * 10; // 10 hour drilling day
        }
      }

      // Storage barges needed (per-rig allocation)
      const storageBargesNeeded = activeRigs * Math.floor(numSmallStorageBargesTotal / numRigs);

      // Transport barges needed (estimate based on demand)
      const volumePerBarge = SMALL_BARGE_VOLUME;
      const tripsPerDay = 2; // Conservative estimate
      const transportBargesNeeded = Math.ceil(totalDailyDemand / (volumePerBarge * tripsPerDay));

      const totalBargesNeeded = storageBargesNeeded + transportBargesNeeded;
      peakBarges = Math.max(peakBarges, totalBargesNeeded);
    }

    return peakBarges;
  };

  // Calculate peak tug requirement in future window
  const getPeakFutureTugRequirement = (currentSimTime, daysAhead = 90) => {
    const hoursAhead = daysAhead * 24;
    const endTime = Math.min(currentSimTime + hoursAhead, totalHours);

    let peakTugs = 0;

    // Check each day in the future window
    for (let futureTime = currentSimTime; futureTime < endTime; futureTime += 24) {
      const futureDateStr = getDateStrFromTime(futureTime);

      let totalDailyDemand = 0;

      for (const rigName of rigNames) {
        const status = getRigStatus(rigName, futureDateStr);
        if (status.isActive && status.hourlyRate > 0) {
          totalDailyDemand += status.hourlyRate * 10; // 10 hour drilling day
        }
      }

      // Estimate tugs needed based on demand
      if (totalDailyDemand > 0) {
        const volumePerTug = SMALL_BARGES_PER_TUG * SMALL_BARGE_VOLUME;
        const tripsPerDay = 2;
        const tugsNeeded = Math.ceil(totalDailyDemand / (volumePerTug * tripsPerDay));
        peakTugs = Math.max(peakTugs, tugsNeeded);
      }
    }

    return peakTugs;
  };

  // =========================================================================
  // DEMOBILIZATION LOGIC
  // =========================================================================

  // Track demobilization tasks
  const demobTasks = {
    barges: [], // { bargeId, demobTime, reason }
    tugs: []    // { tugId, demobTime, reason }
  };

  // Check if we should demobilize barges based on future demand
  const checkBargeDemobilization = (simTime) => {
    // Get peak future barge requirement
    const peakFutureBarges = getPeakFutureBargeRequirement(simTime, 90);

    // Count currently mobilized barges (on-site)
    const mobilizedBarges = state.barges.filter(b =>
      b.currentLocation && // Has been mobilized
      !demobTasks.barges.find(t => t.bargeId === b.id) // Not already scheduled for demob
    );

    const currentBargeCount = mobilizedBarges.length;

    // If peak future demand never requires current fleet size, demobilize excess
    if (peakFutureBarges < currentBargeCount) {
      const excessBarges = currentBargeCount - peakFutureBarges;
      console.log(`[DEMOB CHECK] Current barges: ${currentBargeCount}, Peak future need: ${peakFutureBarges}, Excess: ${excessBarges}`);

      // Select barges to demobilize - prefer idle barges at sources
      const bargesToDemob = mobilizedBarges
        .filter(b => b.status === 'idle' && b.currentLocation && b.currentLocation.startsWith('source'))
        .sort((a, b) => a.fillLevel - b.fillLevel) // Prefer empty barges
        .slice(0, excessBarges);

      bargesToDemob.forEach(barge => {
        demobTasks.barges.push({
          bargeId: barge.id,
          demobTime: simTime,
          reason: 'excess capacity - future demand does not require'
        });

        // Log demobilization
        logAssetAction(simTime, `Barge (${barge.type})`, barge.id, 'demobilizing', `from ${barge.currentLocation}`, {
          reason: 'excess capacity',
          fillLevel: barge.fillLevel
        });

        console.log(`[DEMOB] Scheduling ${barge.id} for demobilization - excess capacity`);
      });
    }
  };

  // Check if we should demobilize tugs based on idle time
  const checkTugDemobilization = (simTime) => {
    const DEMOB_THRESHOLD_HOURS = 4 * 24; // 4 days (2 days mob + 2 days demob)

    // Get peak future tug requirement
    const peakFutureTugs = getPeakFutureTugRequirement(simTime, 90);

    // Count currently mobilized tugs
    const mobilizedTugs = state.tugs.filter(t =>
      !demobTasks.tugs.find(task => task.tugId === t.id)
    );

    const currentTugCount = mobilizedTugs.length;

    // For each idle tug, check how long it's been idle
    mobilizedTugs.forEach(tug => {
      if (tug.status !== 'idle') {
        tug.idleStart = null; // Reset if working
        return;
      }

      // Track when tug became idle
      if (!tug.idleStart) {
        tug.idleStart = simTime;
      }

      const idleHours = simTime - tug.idleStart;

      // If tug has been idle longer than threshold AND we have excess capacity
      if (idleHours >= DEMOB_THRESHOLD_HOURS && peakFutureTugs < currentTugCount) {
        demobTasks.tugs.push({
          tugId: tug.id,
          demobTime: simTime,
          reason: `idle for ${(idleHours / 24).toFixed(1)} days, excess capacity`
        });

        // Log demobilization
        logAssetAction(simTime, 'Tug', tug.id, 'demobilizing', `from ${tug.currentLocation}`, {
          reason: 'extended idle period',
          idleHours: idleHours.toFixed(1)
        });

        console.log(`[DEMOB] Scheduling ${tug.id} for demobilization - idle ${(idleHours / 24).toFixed(1)} days`);
      }
    });
  };

  // ============================================================================
  // SAME-RIG TRANSFER OPTIMIZATION
  // When a rig completes one HDD and moves to another within 14 days,
  // storage barges stay stationed and the support tug (outside our model) moves them.
  // This avoids 2 unnecessary tug trips (HDD->source and source->nextHDD).
  // ============================================================================
  const SAME_RIG_TRANSFER_MAX_GAP_DAYS = 14; // Max gap between HDDs for same-rig transfer

  // Find the next HDD for the same rig after a given simTime
  const findNextHDDForRig = (rigName, afterSimTime) => {
    // Get all HDDs for this rig, sorted by pilot start time
    const rigHDDs = hddSchedule
      .filter(h => h.rig === rigName)
      .map(h => {
        const rigUpEndDate = new Date(h.rigUp);
        const pilotStartDate = new Date(rigUpEndDate);
        pilotStartDate.setDate(pilotStartDate.getDate() + 1); // Day after rigUp ends
        const pilotStartSimTime = (pilotStartDate - minDate) / (1000 * 60 * 60);
        return { ...h, pilotStartSimTime };
      })
      .sort((a, b) => a.pilotStartSimTime - b.pilotStartSimTime);

    // Find the next HDD after the given time
    return rigHDDs.find(h => h.pilotStartSimTime > afterSimTime);
  };

  // Check for HDD pull phase completions - SIMPLIFIED: Just return barges to source, keep mobilized if future demand exists
  const checkHDDCompletions = (simTime) => {
    hddSchedule.forEach(hdd => {
      const { crossing, pull, rig } = hdd;

      // Check if HDD just completed PULL phase (can relocate barges now)
      // IMPORTANT: Pull date is the LAST day of pull phase, so we need to wait until
      // the NEXT day (after drilling ends) before relocating barges
      const pullDate = new Date(pull);
      const pullEndSimTime = (pullDate - minDate) / (1000 * 60 * 60) + 24; // Add 24 hours - trigger at START of day AFTER pull

      // FIX: Remove tight time window - demobScheduled flag prevents repeat triggers
      // Previous condition `simTime < pullEndSimTime + TIME_STEP` was too narrow and missed completions
      if (state.hddStorage[crossing] && !state.hddStorage[crossing].demobScheduled &&
          simTime >= pullEndSimTime) {

        console.log(`[HDD COMPLETE] ${crossing}: Pull phase ended, scheduling barge return. pullEndSimTime=${pullEndSimTime.toFixed(0)}h, simTime=${simTime.toFixed(1)}h`);

        // Mark as scheduled
        state.hddStorage[crossing].demobScheduled = true;

        // =========================================================================
        // SAME-RIG TRANSFER CHECK: If same rig has another HDD starting soon,
        // keep barges stationed for support tug transfer (outside our model)
        // =========================================================================
        const nextRigHDD = findNextHDDForRig(rig, simTime);
        if (nextRigHDD) {
          const gapHours = nextRigHDD.pilotStartSimTime - simTime;
          const gapDays = gapHours / 24;

          if (gapDays <= SAME_RIG_TRANSFER_MAX_GAP_DAYS) {
            // Same rig has another HDD starting soon - keep barges for support tug transfer
            console.log(`[STORAGE TRANSFER] ${crossing} -> ${nextRigHDD.crossing}: Same rig (${rig}) move in ${gapDays.toFixed(1)} days. Barges staying stationed for support tug transfer.`);

            // Create sameRigTransferTask - barges stay stationed until next HDD pilot starts
            state.hddStorage[crossing].sameRigTransferTask = {
              fromHDD: crossing,
              toHDD: nextRigHDD.crossing,
              toRig: rig,
              requestTime: simTime,
              nextPilotStartTime: nextRigHDD.pilotStartSimTime,
              gapDays: gapDays,
              status: 'waiting-for-next-hdd'  // Barges stay stationed, waiting for pilot start
            };

            // Log barges being kept for transfer
            const stationedBarges = state.hddStorage[crossing].stationedBarges || [];
            console.log(`[STORAGE TRANSFER] ${crossing}: Keeping ${stationedBarges.length} barges stationed (${stationedBarges.join(', ')}) for transfer to ${nextRigHDD.crossing}`);
            return; // Skip normal return-to-source logic
          } else {
            // Gap too large - return to source instead
            console.log(`[STORAGE RETURN] ${crossing}: Next ${rig} HDD (${nextRigHDD.crossing}) is ${gapDays.toFixed(1)} days away (>${SAME_RIG_TRANSFER_MAX_GAP_DAYS} day threshold). Returning barges to source.`);
          }
        }

        // MASS BALANCE RULE: If ANY single day in the future has demand, keep ALL barges mobilized
        // Only demobilize when project is completely done
        const stillHasWork = hasFutureDemand(simTime, 90); // Look 90 days ahead

        if (stillHasWork) {
          // UNIFIED MODEL: Return to BEST THROUGHPUT source for reuse (not just closest)
          // A source with 4x flow rate saves far more time than travel distance difference
          const { bestSource } = getBestSourceByThroughput(crossing);

          state.hddStorage[crossing].returnToSourceTask = {
            hdd: crossing,
            source: bestSource,  // UNIFIED MODEL: Use best throughput source
            requestTime: simTime,
            status: 'waiting-for-tug'
          };
        } else {
          // UNIFIED MODEL: No future demand - project ending, demobilize to best throughput source
          const { bestSource } = getBestSourceByThroughput(crossing);

          state.hddStorage[crossing].demobTask = {
            hdd: crossing,
            source: bestSource,  // UNIFIED MODEL: Use best throughput source
            requestTime: simTime,
            status: 'waiting-for-tug'
          };
        }
      }
    });
  };

  // ============================================================================
  // EXECUTE SAME-RIG TRANSFER: When next HDD pilot starts, update barge assignments
  // Support tug (outside our model) physically moves barges - we just update state
  // ============================================================================
  const executeSameRigTransfers = (simTime) => {
    Object.entries(state.hddStorage).forEach(([fromHDD, hddStorage]) => {
      if (!hddStorage.sameRigTransferTask) return;
      if (hddStorage.sameRigTransferTask.status !== 'waiting-for-next-hdd') return;

      const task = hddStorage.sameRigTransferTask;
      const { toHDD, toRig, nextPilotStartTime } = task;

      // Transfer at 5 AM on pilot day (2 hours before drilling starts at 7 AM)
      // This matches storage delivery timing
      const transferTime = nextPilotStartTime + 5; // 5 AM on pilot start day

      if (simTime >= transferTime) {
        console.log(`[STORAGE TRANSFER] ${fromHDD} -> ${toHDD}: Executing support tug transfer at simTime=${simTime.toFixed(0)}h (pilot+5h=${transferTime.toFixed(0)}h)`);

        // Get barges to transfer
        const bargeIds = [...(hddStorage.stationedBarges || [])];
        if (bargeIds.length === 0) {
          console.warn(`[STORAGE TRANSFER WARNING] ${fromHDD} -> ${toHDD}: No barges to transfer!`);
          task.status = 'completed-empty';
          return;
        }

        // Initialize destination HDD storage if needed
        if (!state.hddStorage[toHDD]) {
          state.hddStorage[toHDD] = {
            stationedBarges: [],
            totalCapacity: 0,
            totalLevel: 0,
            bargeDetails: {},
            demobScheduled: false,
            demobTask: null,
            transferTask: null,
            sameRigTransferTask: null
          };
        }
        const destStorage = state.hddStorage[toHDD];

        // Calculate total storage level being transferred
        const transferredStorageLevel = bargeIds.reduce((sum, bargeId) => {
          const barge = state.barges.find(b => b.id === bargeId);
          return sum + (barge?.fillLevel || 0);
        }, 0);

        // Track transfer in chart data (removal from source HDD)
        if (storageBargeChangesByHDD[fromHDD]) {
          storageBargeChangesByHDD[fromHDD].push({
            timestamp: simTime,
            hourKey: Math.floor(simTime),
            bargeIds: [],
            previousBargeIds: [...bargeIds],
            totalCapacity: 0,
            transferredStorageLevel: transferredStorageLevel,
            action: 'same-rig-transfer-out',
            destination: toHDD
          });
        }

        // Update each barge's assignment
        bargeIds.forEach(bargeId => {
          const barge = state.barges.find(b => b.id === bargeId);
          if (!barge) return;

          // Unstation from source HDD via kernel
          kernel.unstationBargeFromHDD(bargeId, fromHDD, state.barges);

          // Update barge properties for new HDD
          barge.assignedHDD = toHDD;
          barge.assignedCrossing = toHDD;
          barge.currentLocation = toHDD;
          barge.status = 'stationed';
          barge.role = 'storage';

          // Station at destination HDD via kernel
          kernel.stationBargeAtHDD(bargeId, toHDD, state.barges);

          // Update destination storage tracking
          destStorage.stationedBarges.push(bargeId);
          destStorage.totalCapacity += barge.volume;
          destStorage.totalLevel += barge.fillLevel;
          destStorage.bargeDetails[bargeId] = {
            level: barge.fillLevel,
            capacity: barge.volume,
            type: barge.type
          };

          logAssetAction(simTime, `Barge (${barge.type})`, bargeId, 'stationed', `at ${toHDD} (${toRig})`, {
            fillLevel: barge.fillLevel,
            capacity: barge.volume,
            usage: 'storage',
            arrivedVia: 'same-rig-transfer',
            transferredFrom: fromHDD
          });
        });

        // Track transfer in chart data (arrival at destination HDD)
        if (!storageBargeChangesByHDD[toHDD]) {
          storageBargeChangesByHDD[toHDD] = [];
        }
        storageBargeChangesByHDD[toHDD].push({
          timestamp: simTime,
          hourKey: Math.floor(simTime),
          bargeIds: [...bargeIds],
          previousBargeIds: [],
          totalCapacity: destStorage.totalCapacity,
          transferredStorageLevel: transferredStorageLevel,
          action: 'same-rig-transfer-in',
          source: fromHDD
        });

        // Clear source HDD storage
        hddStorage.stationedBarges = [];
        hddStorage.totalCapacity = 0;
        hddStorage.totalLevel = 0;
        hddStorage.bargeDetails = {};

        // Mark transfer as complete
        task.status = 'completed';

        console.log(`[STORAGE TRANSFER] ${fromHDD} -> ${toHDD}: Transfer complete. ${bargeIds.length} barges (${bargeIds.join(', ')}) now stationed at ${toHDD}`);
      }
    });
  };

  // Dispatch tug to return storage barges to source (keep mobilized)
  const dispatchReturnToSource = (returnTask, simTime) => {
    const { hdd, source, nextHDD } = returnTask;
    const hddStorage = state.hddStorage[hdd];

    // Find idle tug at source
    const tug = state.tugs.find(t => t.status === 'idle' && t.currentLocation === source);
    if (!tug) return false; // No tug available

    // CRITICAL FIX: Drop any attached transport barges back to pool before picking up storage barges
    // This prevents losing transport barges when tug.attachedBargeIds gets overwritten
    const currentlyAttached = kernel.getAttachedBarges(tug.id);
    if (currentlyAttached.length > 0) {
      returnBargesToSource(currentlyAttached, source, simTime, 'dispatchReturnToSource');
      // Legacy sync
      tug.attachedBargeIds = [];
    }

    // Setup tug for pickup
    tug.status = 'en-route-return-to-source';
    tug.targetCrossing = hdd;
    tug.nextHDDForBarges = nextHDD; // Track where barges will go next

    const distance = getDistance(source, hdd);
    const travelTime = getTravelTime(distance, false, false); // Empty trip
    tug.arrivalHour = simTime + travelTime;

    logAssetAction(simTime, 'Tug', tug.id, 'en-route-return-to-source', `to ${hdd}`, {
      purpose: 'return-storage-to-source',
      nextHDD
    });

    returnTask.status = 'tug-dispatched';
    returnTask.tugId = tug.id;
    return true;
  };

  // Dispatch tug to pick up storage barges for demobilization
  const dispatchDemobilization = (demobTask, simTime) => {
    const { hdd, source } = demobTask;
    const hddStorage = state.hddStorage[hdd];

    // Find idle tug at source
    const tug = state.tugs.find(t => t.status === 'idle' && t.currentLocation === source);
    if (!tug) return false; // No tug available

    // CRITICAL FIX: Drop any attached transport barges back to pool before picking up storage barges
    // This prevents losing transport barges when tug.attachedBargeIds gets overwritten
    const currentlyAttached = kernel.getAttachedBarges(tug.id);
    if (currentlyAttached.length > 0) {
      returnBargesToSource(currentlyAttached, source, simTime, 'dispatchDemobilization');
      // Legacy sync
      tug.attachedBargeIds = [];
    }

    // Setup tug for pickup
    tug.status = 'en-route-demob';
    tug.targetCrossing = hdd;

    const distance = getDistance(source, hdd);
    const travelTime = getTravelTime(distance, false, false); // Empty trip
    tug.arrivalHour = simTime + travelTime;

    logAssetAction(simTime, 'Tug', tug.id, 'en-route-demob', `to ${hdd}`, {
      purpose: 'demobilize-storage'
    });

    demobTask.status = 'tug-dispatched';
    demobTask.tugId = tug.id;
    return true;
  };

  // Pick up storage barges and return to source (keep mobilized)
  const pickupBargesReturnToSource = (tug, simTime) => {
    const hdd = tug.targetCrossing;
    const hddStorage = state.hddStorage[hdd];
    const nextHDD = tug.nextHDDForBarges;

    // Get stationed barges and attach to tug using kernel
    const bargeIdsToPickup = [...hddStorage.stationedBarges];

    // Use kernel to attach barges from HDD to tug
    bargeIdsToPickup.forEach(bargeId => {
      const barge = state.barges.find(b => b.id === bargeId);
      if (!barge) return;

      // Unstation from HDD via kernel
      kernel.unstationBargeFromHDD(bargeId, hdd, state.barges);
    });

    // Attach all to tug via kernel
    kernel.attachBarges(tug.id, bargeIdsToPickup, state.barges, simTime, 'pickupBargesReturnToSource');

    // Legacy sync
    tug.attachedBargeIds = kernel.getAttachedBarges(tug.id);

    bargeIdsToPickup.forEach(bargeId => {
      const barge = state.barges.find(b => b.id === bargeId);
      if (!barge) return;

      barge.status = 'returning-to-source';
      barge.role = 'storage-available'; // Keep as storage-available
      barge.assignedHDD = nextHDD; // Track next assignment
      // Keep fillLevel - barges retain their water when relocating

      logAssetAction(simTime, `Barge (${barge.type})`, bargeId, 'returning-to-source', `from ${hdd}`, {
        fillLevel: barge.fillLevel,
        capacity: barge.volume,
        usage: 'storage',
        tugId: tug.id,
        nextHDD
      });
    });

    // Track storage barge removal for chart - include where barges are going
    const previousBargeIds = [...hddStorage.stationedBarges];
    if (storageBargeChangesByHDD[hdd]) {
      storageBargeChangesByHDD[hdd].push({
        timestamp: simTime,
        hourKey: Math.floor(simTime),
        bargeIds: [],
        previousBargeIds: previousBargeIds,
        totalCapacity: 0,
        action: 'move-to-next',
        destination: nextHDD ? nextHDD.crossing : 'source'
      });
    }

    // Clear HDD storage
    hddStorage.stationedBarges = [];
    hddStorage.totalCapacity = 0;
    hddStorage.totalLevel = 0;

    // Return to source
    const returnSource = hddStorage.returnToSourceTask.source;
    tug.status = 'en-route-empty';
    tug.targetCrossing = null;
    tug.returningFromDemob = true;  // Flag for return handler to drop all barges
    tug.intendedReturnSource = returnSource;  // Store intended source for arrival handler

    const returnDist = getDistance(returnSource, hdd);
    const returnTime = getTravelTime(returnDist, false, false);
    tug.returnHour = simTime + returnTime;

    logAssetAction(simTime, 'Tug', tug.id, 'en-route-empty', `returning to ${returnSource}`, {
      attachedBargeIds: tug.attachedBargeIds,
      afterDelivery: 'return-to-source',
      sourceId: returnSource,
      targetCrossing: hdd,
      nextHDD
    });

    console.log(`[STORAGE RETURN] ${hdd}: ${tug.id} picking up ${bargeIdsToPickup.length} storage barges (${bargeIdsToPickup.join(', ')}), returning to ${returnSource}, ETA=${tug.returnHour.toFixed(0)}h`);
  };

  // Pick up storage barges and return to source (demobilize)
  const pickupStorageBarges = (tug, simTime) => {
    const hdd = tug.targetCrossing;
    const hddStorage = state.hddStorage[hdd];

    // Get stationed barges and attach to tug using kernel
    const bargeIdsToPickup = [...hddStorage.stationedBarges];

    // Unstation from HDD and attach to tug via kernel
    bargeIdsToPickup.forEach(bargeId => {
      kernel.unstationBargeFromHDD(bargeId, hdd, state.barges);
    });

    kernel.attachBarges(tug.id, bargeIdsToPickup, state.barges, simTime, 'pickupStorageBarges');

    // Legacy sync
    tug.attachedBargeIds = kernel.getAttachedBarges(tug.id);

    bargeIdsToPickup.forEach(bargeId => {
      const barge = state.barges.find(b => b.id === bargeId);
      if (!barge) return;

      barge.status = 'demobilizing';
      barge.role = 'storage-available'; // Reset to storage-available for reuse
      // Keep fillLevel - barges retain their water when moving

      logAssetAction(simTime, `Barge (${barge.type})`, bargeId, 'demobilizing', `from ${hdd}`, {
        fillLevel: barge.fillLevel,
        capacity: barge.volume,
        usage: 'storage',
        tugId: tug.id
      });
    });

    // Track storage barge removal for chart - demob (returning to source/demobilized)
    const previousBargeIds = [...hddStorage.stationedBarges];
    if (storageBargeChangesByHDD[hdd]) {
      storageBargeChangesByHDD[hdd].push({
        timestamp: simTime,
        hourKey: Math.floor(simTime),
        bargeIds: [],
        previousBargeIds: previousBargeIds,
        totalCapacity: 0,
        action: 'demob',
        destination: 'demobilized'
      });
    }

    // Clear HDD storage
    hddStorage.stationedBarges = [];
    hddStorage.totalCapacity = 0;
    hddStorage.totalLevel = 0;

    // Return to source
    const returnSource = hddStorage.demobTask.source;
    tug.status = 'en-route-empty';
    tug.targetCrossing = null;
    tug.returningFromDemob = true;  // Flag for return handler to drop all storage barges
    tug.intendedReturnSource = returnSource;  // Store intended source for arrival handler

    const returnDist = getDistance(returnSource, hdd);
    const returnTime = getTravelTime(returnDist, false, false);
    tug.returnHour = simTime + returnTime;

    logAssetAction(simTime, 'Tug', tug.id, 'en-route-empty', `returning to ${returnSource}`, {
      attachedBargeIds: tug.attachedBargeIds,
      afterDelivery: 'demobilization',
      sourceId: returnSource,
      targetCrossing: hdd
    });
  };

  // Dispatch tug to pick up storage barges for rig-to-rig transfer
  const dispatchTransfer = (transferTask, simTime) => {
    const { fromHDD, toHDD, toRig } = transferTask;
    const hddStorage = state.hddStorage[fromHDD];

    // Find idle tug at BEST THROUGHPUT source (not just closest)
    // Try to find a tug at the best source first, then fall back to any idle tug
    const { bestSource } = getBestSourceByThroughput(fromHDD);

    let tug = state.tugs.find(t => t.status === 'idle' && t.currentLocation === bestSource);
    let selectedSource = bestSource;

    // Fallback: try any idle tug at any source
    if (!tug) {
      tug = state.tugs.find(t => t.status === 'idle' && Object.keys(sourceConfig).includes(t.currentLocation));
      if (tug) selectedSource = tug.currentLocation;
    }
    if (!tug) return false; // No tug available

    // CRITICAL FIX: Drop any attached transport barges back to pool before picking up storage barges
    // This prevents losing transport barges when tug.attachedBargeIds gets overwritten
    const currentlyAttached = kernel.getAttachedBarges(tug.id);
    if (currentlyAttached.length > 0) {
      returnBargesToSource(currentlyAttached, selectedSource, simTime, 'dispatchTransfer');
      // Legacy sync
      tug.attachedBargeIds = [];
    }

    // Setup tug for pickup
    tug.status = 'en-route-transfer-pickup';
    tug.targetCrossing = fromHDD;
    tug.transferDestination = toHDD;  // Store final destination

    const distance = getDistance(selectedSource, fromHDD);
    const travelTime = getTravelTime(distance, false, false); // Empty trip
    tug.arrivalHour = simTime + travelTime;

    logAssetAction(simTime, 'Tug', tug.id, 'en-route-transfer-pickup', `to ${fromHDD}`, {
      purpose: 'transfer-to-' + toHDD
    });

    transferTask.status = 'tug-dispatched';
    transferTask.tugId = tug.id;
    return true;
  };

  // Pick up storage barges for transfer to another HDD
  const pickupForTransfer = (tug, simTime) => {
    const fromHDD = tug.targetCrossing;
    const toHDD = tug.transferDestination;
    const hddStorage = state.hddStorage[fromHDD];

    // Get stationed barges and attach to tug using kernel
    const bargeIdsToPickup = [...hddStorage.stationedBarges];

    // Unstation from HDD and attach to tug via kernel
    bargeIdsToPickup.forEach(bargeId => {
      kernel.unstationBargeFromHDD(bargeId, fromHDD, state.barges);
    });

    kernel.attachBarges(tug.id, bargeIdsToPickup, state.barges, simTime, 'pickupForTransfer');

    // Legacy sync
    tug.attachedBargeIds = kernel.getAttachedBarges(tug.id);

    bargeIdsToPickup.forEach(bargeId => {
      const barge = state.barges.find(b => b.id === bargeId);
      if (!barge) return;

      barge.status = 'transferring';
      barge.assignedHDD = toHDD;  // Update assigned HDD
      // Keep fillLevel - barges retain their water when transferring

      logAssetAction(simTime, `Barge (${barge.type})`, bargeId, 'transferring', `from ${fromHDD} to ${toHDD}`, {
        fillLevel: barge.fillLevel,
        capacity: barge.volume,
        usage: 'storage',
        tugId: tug.id
      });
    });

    // Track storage barge removal for chart (transfer out)
    // Track storage barge transfer for chart
    const previousBargeIds = [...hddStorage.stationedBarges];
    if (storageBargeChangesByHDD[fromHDD]) {
      storageBargeChangesByHDD[fromHDD].push({
        timestamp: simTime,
        hourKey: Math.floor(simTime),
        bargeIds: [],
        previousBargeIds: previousBargeIds,
        totalCapacity: 0,
        action: 'transfer-out',
        destination: toHDD
      });
    }

    // Clear source HDD storage
    hddStorage.stationedBarges = [];
    hddStorage.totalCapacity = 0;
    hddStorage.totalLevel = 0;

    // Head to destination HDD
    tug.status = 'en-route-transfer-delivery';
    tug.targetCrossing = toHDD;

    const transferDist = getDistance(fromHDD, toHDD);
    const attachedBargeIds = kernel.getAttachedBarges(tug.id);
    const hasLargeBarges = attachedBargeIds.some(bid => {
      const b = state.barges.find(x => x.id === bid);
      return b && b.type === 'large';
    });
    const transferTime = getTravelTime(transferDist, true, hasLargeBarges);
    tug.arrivalHour = simTime + transferTime;

    logAssetAction(simTime, 'Tug', tug.id, 'en-route-transfer-delivery', `to ${toHDD}`, {
      attachedBargeIds: attachedBargeIds,
      purpose: 'deliver-storage'
    });
  };

  // Deliver transferred barges to destination HDD and return tug to source
  const deliverTransferBarges = (tug, simTime) => {
    const toHDD = tug.targetCrossing;
    const toRig = chronologicalHDDList.find(h => h.crossing === toHDD)?.rig || 'Unknown';

    // Initialize storage if not already done
    if (!state.hddStorage[toHDD]) {
      state.hddStorage[toHDD] = {
        stationedBarges: [],
        totalCapacity: 0,
        totalLevel: 0,
        bargeDetails: {},
        demobScheduled: false,
        demobTask: null,
        transferTask: null
      };
    }

    const hddStorage = state.hddStorage[toHDD];

    // Station barges at destination HDD using kernel
    const attachedBargeIds = kernel.getAttachedBarges(tug.id);
    attachedBargeIds.forEach(bargeId => {
      const barge = state.barges.find(b => b.id === bargeId);
      if (!barge) return;

      // Use kernel to station barge at HDD
      kernel.stationBargeAtHDD(bargeId, toHDD, state.barges);

      barge.currentLocation = toHDD;
      barge.assignedTugId = null;
      barge.status = 'stationed';
      barge.role = 'storage';

      hddStorage.stationedBarges.push(bargeId);
      hddStorage.totalCapacity += barge.volume;
      hddStorage.totalLevel += barge.fillLevel;
      hddStorage.bargeDetails[bargeId] = {
        level: barge.fillLevel,
        capacity: barge.volume,
        type: barge.type
      };

      logAssetAction(simTime, `Barge (${barge.type})`, bargeId, 'stationed', `at ${toHDD} (${toRig})`, {
        fillLevel: barge.fillLevel,
        capacity: barge.volume,
        usage: 'storage',
        arrivedVia: 'transfer'
      });
    });

    // Detach all barges from tug via kernel
    kernel.detachBarges(tug.id, attachedBargeIds, toHDD, state.barges, simTime, 'deliverTransferBarges');

    // Clear tug attachments (legacy sync)
    tug.attachedBargeIds = [];
    tug.targetCrossing = null;
    tug.transferDestination = null;

    // Return tug to BEST THROUGHPUT source (not just closest)
    // A source with 4x flow rate saves far more time than travel distance difference
    const { bestSource } = getBestSourceByThroughput(toHDD);

    tug.status = 'en-route-empty';
    tug.currentLocation = toHDD;
    tug.intendedReturnSource = bestSource;  // Store intended source for arrival handler

    const returnDist = getDistance(toHDD, bestSource);
    const returnTime = getTravelTime(returnDist, false, false);
    tug.returnHour = simTime + returnTime;

    logAssetAction(simTime, 'Tug', tug.id, 'en-route-empty', `returning to ${bestSource}`, {
      afterDelivery: 'transfer-complete',
      sourceId: bestSource,
      targetCrossing: toHDD
    });
  };

  // Dispatch helpers
  const findAvailableTug = (sourceId = null, bargeType = null, debug = false, simTime = 0) => {
    // Step 1: Check available barges at source (in pool)
    const sourceState = state.sources[sourceId];
    if (!sourceState) return null;

    // IMPORTANT: Exclude storage barges - they're reserved for storage deliveries only
    const poolBargeIds = getAvailableBargesAtSource(sourceId, bargeType, true);


    // Step 2: Find idle tug at this source (all tugs can pull any barge type)
    for (const t of state.tugs) {
      if (t.status !== 'idle') continue;
      if (sourceId && t.currentLocation !== sourceId) continue;

      // Check if tug has attached barges (kept from previous delivery)
      let availableBargeIds = [...poolBargeIds];
      const tugAttached = kernel.getAttachedBarges(t.id);
      if (tugAttached.length > 0) {
        // Add attached barges that match criteria (exclude storage, match type, has water)
        const attachedAvailable = tugAttached.filter(bid => {
          const b = state.barges.find(x => x.id === bid);
          if (!b) return false;
          if (bargeType && b.type !== bargeType) return false;
          // UNIFIED MODEL: Use role-based check to exclude storage barges
          const isStorageRole = b.role === 'storage-available' || b.role === 'storage';
          if (isStorageRole) return false;
          // KEY FIX: Include queued/filling barges even if fillLevel=0, as they'll have water by dispatch
          return b.fillLevel > 0; // MUST have actual water
        });
        availableBargeIds = [...availableBargeIds, ...attachedAvailable];
      }

      if (availableBargeIds.length === 0) {
        if (debug) {
          console.log(`[DEBUG findAvailableTug] ${sourceId}: ${t.id} is idle but has no available barges (pool=${poolBargeIds.length}, attached=${tugAttached.length})`);
        }
        continue; // No available barges for this tug
      }

      // Tug found - return with available barges info
      return {
        tug: t,
        availableBargeIds,
        availableBargeCount: availableBargeIds.length
      };
    }

    // No idle tug found - try to mobilize one if we have unmobilized tugs
    if (state.unmobilizedTugs > 0 && poolBargeIds.length > 0) {
      console.log(`[DYNAMIC MOB] No idle tug at ${sourceId}, mobilizing one from pool (${state.unmobilizedTugs} available)`);
      const newTug = mobilizeTug(sourceId, simTime, `needed for delivery at ${sourceId}`);
      if (newTug) {
        return {
          tug: newTug,
          availableBargeIds: poolBargeIds,
          availableBargeCount: poolBargeIds.length
        };
      }
    }

    return null;
  };

  // UNIFIED MODEL: Multi-stop tug pickup planning
  // Allows tug to pick up barges from multiple sources on one trip
  // Returns a pickup plan with stops and cumulative travel time
  // Note: This is foundational - full integration requires state machine changes
  const planMultiSourcePickup = (tugId, targetHDD, waterNeeded, simTime) => {
    const tug = state.tugs.find(t => t.id === tugId);
    if (!tug || tug.status !== 'idle') return null;

    const maxSmallBarges = tug.smallBargesPerTug || SMALL_BARGES_PER_TUG;
    const maxLargeBarges = tug.largeBargesPerTug || LARGE_BARGES_PER_TUG;

    const plan = {
      stops: [],
      totalTravelTime: 0,
      totalWaterPickup: 0,
      bargeIds: []
    };

    let currentLocation = tug.currentLocation;
    let smallBargesToPick = 0;
    let largeBargesToPick = 0;

    // Find sources with available barges, sorted by distance from current location
    const sourcesWithBarges = sourceIds
      .map(srcId => {
        const smallAvail = state.barges.filter(b =>
          b.currentLocation === srcId &&
          b.type === 'small' &&
          b.role !== 'storage' && b.role !== 'storage-available' &&
          b.fillLevel > 0 &&
          b.assignedTugId === null
        );
        const largeAvail = state.barges.filter(b =>
          b.currentLocation === srcId &&
          b.type === 'large' &&
          b.role !== 'storage' && b.role !== 'storage-available' &&
          b.fillLevel > 0 &&
          b.assignedTugId === null
        );
        return {
          sourceId: srcId,
          distance: getDistance(currentLocation, srcId),
          smallBarges: smallAvail,
          largeBarges: largeAvail
        };
      })
      .filter(s => s.smallBarges.length > 0 || s.largeBarges.length > 0)
      .sort((a, b) => a.distance - b.distance);

    // Build pickup plan - prefer large barges if available
    for (const source of sourcesWithBarges) {
      if (plan.totalWaterPickup >= waterNeeded) break;
      if (smallBargesToPick >= maxSmallBarges && largeBargesToPick >= maxLargeBarges) break;

      const stopBarges = [];

      // Try to pick large barges first
      if (largeBargesToPick < maxLargeBarges && source.largeBarges.length > 0) {
        const canPickLarge = Math.min(maxLargeBarges - largeBargesToPick, source.largeBarges.length);
        for (let i = 0; i < canPickLarge; i++) {
          const barge = source.largeBarges[i];
          stopBarges.push(barge.id);
          plan.bargeIds.push(barge.id);
          plan.totalWaterPickup += barge.fillLevel;
          largeBargesToPick++;
        }
      }

      // Then pick small barges (only if no large barges picked due to tug capacity constraint)
      if (largeBargesToPick === 0 && smallBargesToPick < maxSmallBarges && source.smallBarges.length > 0) {
        const canPickSmall = Math.min(maxSmallBarges - smallBargesToPick, source.smallBarges.length);
        for (let i = 0; i < canPickSmall; i++) {
          const barge = source.smallBarges[i];
          stopBarges.push(barge.id);
          plan.bargeIds.push(barge.id);
          plan.totalWaterPickup += barge.fillLevel;
          smallBargesToPick++;
        }
      }

      if (stopBarges.length > 0) {
        const travelTime = getTravelTime(source.distance, false, false); // empty speed
        plan.totalTravelTime += travelTime;
        plan.stops.push({
          sourceId: source.sourceId,
          bargeIds: stopBarges,
          travelTime,
          arrivalTime: simTime + plan.totalTravelTime
        });
        currentLocation = source.sourceId;
      }
    }

    // Add final leg to target HDD
    if (plan.stops.length > 0) {
      const finalDistance = getDistance(currentLocation, targetHDD);
      const isLarge = largeBargesToPick > 0;
      const finalTravelTime = getTravelTime(finalDistance, true, isLarge);
      plan.totalTravelTime += finalTravelTime;
      plan.finalLegTime = finalTravelTime;
      plan.targetHDD = targetHDD;
    }

    return plan.stops.length > 0 ? plan : null;
  };

  const selectReturnSource = (tugId, currentHdd) => {
    const candidates = [];
    Object.keys(sourceConfig).forEach(sourceId => {
      const cfg = sourceConfig[sourceId];
      // Count only TRANSPORT barges (not storage) at source that need filling or are ready
      const transportBargesAtSource = state.barges.filter(b =>
        b.currentLocation === sourceId &&
        b.role !== 'storage' && b.role !== 'storage-available' && // Exclude storage barges
        ['queued', 'filling', 'idle'].includes(b.status)
      ).length;
      // Count barges that are READY to go (idle with water)
      const readyBarges = state.barges.filter(b =>
        b.currentLocation === sourceId &&
        b.role !== 'storage' && b.role !== 'storage-available' &&
        b.status === 'idle' &&
        b.fillLevel > 0
      ).length;
      // PRIORITY: Flow rate is king - higher flow rate = faster turnaround
      // Barges will flow to high-capacity sources, so tugs should wait there
      // Only give barge bonus if barges are READY (filled), not just present
      const fillRateScore = (cfg.flowRate / 100) * 2; // 800 GPM = 16 points, 200 GPM = 4 points
      const readyBargeBonus = readyBarges * 5; // Only count ready barges
      const presentBargeBonus = transportBargesAtSource * 1; // Small bonus for present barges
      candidates.push({
        sourceId,
        score: fillRateScore + readyBargeBonus + presentBargeBonus,
        bargeCount: transportBargesAtSource,
        readyBarges,
        flowRate: cfg.flowRate
      });
    });
    // Sort by HIGHEST score (best source first)
    candidates.sort((a, b) => b.score - a.score);
    console.log(`[selectReturnSource] Candidates: ${candidates.map(c => `${c.sourceId}(flow=${c.flowRate},ready=${c.readyBarges},total=${c.bargeCount},score=${c.score.toFixed(1)})`).join(', ')} → ${candidates[0]?.sourceId}`);
    return candidates[0]?.sourceId || 'source1';
  };

  // Calculate estimated delivery cost per gallon from a source to a destination
  const calculateDeliveryCost = (sourceId, hddCrossing, waterNeeded, simTime) => {
    const sourceState = state.sources[sourceId];
    const distance = getDistance(sourceId, hddCrossing);

    // Estimate tug/barge type based on availability
    let available = findAvailableTug(sourceId, 'large', false, simTime);
    const isLarge = !!available;
    if (!available) available = findAvailableTug(sourceId, 'small', false, simTime);
    if (!available) return Infinity; // No tug available

    const { tug } = available;
    const tugDayRate = COST.tug.dayRate;
    // Determine barge type from available barges at this source
    const firstAvailableBarge = state.barges.find(b => available.availableBargeIds.includes(b.id));
    const isLargeBarge = firstAvailableBarge && firstAvailableBarge.type === 'large';
    const bargeDayRate = isLargeBarge ? COST.barge.largeDayRate : COST.barge.smallDayRate;
    const bargeCapacity = isLargeBarge ? LARGE_BARGE_VOLUME : SMALL_BARGE_VOLUME;
    const maxBargesPerTug = isLargeBarge ? tug.largeBargesPerTug : tug.smallBargesPerTug;
    const bargesNeeded = Math.min(Math.ceil(waterNeeded / bargeCapacity), maxBargesPerTug);

    // Estimate queue time based on current source utilization
    const bargesInQueue = sourceState.queue.length;
    const cfg = sourceConfig[sourceId];
    const avgFillTime = bargeCapacity / (cfg.flowRate * 60);
    const estimatedQueueTime = bargesInQueue * (avgFillTime + switchoutTime);

    // Calculate travel time
    const speed = isLarge ? loadedSpeedLarge : loadedSpeedSmall;
    const transitTime = distance / speed;

    // Calculate costs
    const queueCost = (tugDayRate + bargeDayRate * bargesNeeded) * (estimatedQueueTime / 24);
    const fillCost = (tugDayRate + bargeDayRate * bargesNeeded) * (avgFillTime / 24);
    const fuelCost = distance * COST.tug.fuelRunningGPH * (1/speed) * COST.fuel.pricePerGal;
    const transitCost = (tugDayRate + bargeDayRate * bargesNeeded) * (transitTime / 24) + fuelCost;
    const acquisitionCost = waterNeeded * (COST.waterAcquisition[sourceId] || 0);

    const totalCost = queueCost + fillCost + transitCost + acquisitionCost;
    const costPerGallon = waterNeeded > 0 ? totalCost / waterNeeded : Infinity;

    return costPerGallon;
  };

  // =========================================================================
  // HDD-TO-HDD DIRECT ROUTING - Evaluate direct delivery vs return-to-source
  // =========================================================================

  // Find all HDDs that currently need water
  const findHDDsNeedingWater = (simTime) => {
    const dateStr = getDateStrFromTime(simTime);
    const needyHDDs = [];

    rigNames.forEach(otherRig => {
      const otherStatus = getRigStatus(otherRig, dateStr);

      // Skip inactive rigs or those not consuming water
      if (!otherStatus.isActive ||
          ['Idle', 'RigDown', 'RigUp'].includes(otherStatus.stage) ||
          otherStatus.hourlyRate === 0) {
        return;
      }

      const otherCrossing = otherStatus.hdd;
      if (!otherCrossing) return;

      const storageLevel = getEffectiveStorageLevel(otherRig);
      const storageCapacity = getEffectiveStorageCapacity(otherRig);
      const availableCapacity = Math.max(0, storageCapacity - storageLevel);
      const hoursOfSupply = storageLevel / otherStatus.hourlyRate;

      // Calculate water already en route to this HDD
      const waterEnRoute = state.tugs
        .filter(t =>
          (t.targetRig === otherRig || t.targetCrossing === otherCrossing) &&
          ['en-route-loaded', 'traveling-to-next-stop', 'direct-hdd-delivery'].includes(t.status)
        )
        .reduce((sum, t) => sum + (t.totalVolumeRemaining || 0), 0);

      // Effective hours of supply including water en route
      const effectiveSupply = storageLevel + waterEnRoute;
      const effectiveHoursOfSupply = effectiveSupply / otherStatus.hourlyRate;

      needyHDDs.push({
        rig: otherRig,
        crossing: otherCrossing,
        storageLevel,
        storageCapacity,
        availableCapacity,
        hoursOfSupply,
        effectiveHoursOfSupply,
        hourlyRate: otherStatus.hourlyRate,
        waterEnRoute,
        stage: otherStatus.stage
      });
    });

    return needyHDDs;
  };

  // Evaluate whether direct HDD-to-HDD delivery is better than returning to source
  const findBestDirectHDDDelivery = (currentCrossing, currentRig, remainingWater, simTime) => {
    // Minimum water threshold for direct delivery (50k gallons)
    const MIN_WATER_FOR_DIRECT = 50000;

    if (remainingWater < MIN_WATER_FOR_DIRECT) {
      return null;
    }

    const needyHDDs = findHDDsNeedingWater(simTime);

    // Filter out current HDD
    const candidates = needyHDDs.filter(h => h.crossing !== currentCrossing);

    if (candidates.length === 0) {
      return null;
    }

    // Calculate value of returning to source for comparison
    const { bestSource: returnSource, bestCycleTime } = getBestSourceByThroughput(currentCrossing);
    const distToSource = getDistance(returnSource, currentCrossing);
    const returnTime = getTravelTime(distToSource, false, false);  // Empty return

    let bestCandidate = null;
    let bestValue = 0;  // Value = time saved vs return-to-source-then-deliver

    candidates.forEach(candidate => {
      const directDistance = hddToHddMatrix[currentCrossing]?.[candidate.crossing];

      // Skip if no valid distance (HDDs not connected on centerline)
      if (!directDistance || directDistance === Infinity || directDistance > 25) {
        return;  // Max 25nm for direct HDD-to-HDD (reasonable limit)
      }

      // Skip if target doesn't have capacity for meaningful delivery
      if (candidate.availableCapacity < 30000) {
        return;
      }

      // Calculate direct delivery time
      const directTravelTime = getTravelTime(directDistance, true, false);  // Loaded
      const pumpTime = Math.min(remainingWater, candidate.availableCapacity) / PUMP_TO_STORAGE_RATE;
      const directTotalTime = directTravelTime + pumpTime;

      // Calculate alternative: return to source, refill, deliver to this HDD
      const distSourceToTarget = getDistance(returnSource, candidate.crossing);
      const sourceToTargetTime = getTravelTime(distSourceToTarget, true, false);
      const avgFillTime = SMALL_BARGE_VOLUME / (sourceConfig[returnSource].flowRate * 60);
      const alternativeTime = returnTime + avgFillTime + sourceToTargetTime + pumpTime;

      // Time saved by going direct
      const timeSaved = alternativeTime - directTotalTime;

      // Calculate urgency bonus: prioritize HDDs with low supply
      let urgencyBonus = 0;
      if (candidate.effectiveHoursOfSupply < 4) {
        urgencyBonus = 50;  // Very urgent - less than 4 hours supply
      } else if (candidate.effectiveHoursOfSupply < 8) {
        urgencyBonus = 20;  // Moderately urgent - less than 8 hours supply
      }

      // Calculate value score
      // Higher score = better candidate
      // Score considers: time savings, urgency, and water delivery efficiency
      const waterDeliverable = Math.min(remainingWater, candidate.availableCapacity);
      const deliveryEfficiency = waterDeliverable / remainingWater;  // 1.0 = can deliver all water

      const valueScore = (timeSaved * 10) + urgencyBonus + (deliveryEfficiency * 20);

      // Only consider if it saves time AND has positive value
      if (timeSaved > 0 && valueScore > bestValue) {
        bestValue = valueScore;
        bestCandidate = {
          ...candidate,
          distance: directDistance,
          travelTime: directTravelTime,
          timeSaved,
          valueScore,
          waterDeliverable,
          returnSource,
          alternativeTime
        };
      }
    });

    // Only return candidate if value score exceeds threshold
    // This prevents marginal direct deliveries that might not be worth it
    const MIN_VALUE_THRESHOLD = 10;
    if (bestCandidate && bestCandidate.valueScore >= MIN_VALUE_THRESHOLD) {
      return bestCandidate;
    }

    return null;
  };

  const selectBestSourceForHDD = (hddCrossing, simTime = 0, waterNeeded = 80000, debug = false) => {
    let bestSource = null, bestCost = Infinity;
    const sourceDetails = [];

    // Log state snapshot when debug is on and all sources fail
    let noTugsAvailable = true;

    Object.keys(sourceConfig).forEach(sourceId => {
      const costPerGal = calculateDeliveryCost(sourceId, hddCrossing, waterNeeded, simTime);
      const distance = getDistance(sourceId, hddCrossing);
      const available = findAvailableTug(sourceId, 'small', debug, simTime) || findAvailableTug(sourceId, 'large', debug, simTime);
      if (available) noTugsAvailable = false;
      sourceDetails.push({
        sourceId,
        distance: distance.toFixed(1),
        costPerGal: costPerGal === Infinity ? 'N/A' : costPerGal.toFixed(4),
        hasTug: !!available
      });
      if (costPerGal < bestCost) {
        bestCost = costPerGal;
        bestSource = sourceId;
      }
    });

    // Debug: log source selection reasoning
    if (debug) {
      console.log(`[SOURCE SELECT] ${hddCrossing}: ${sourceDetails.map(s =>
        `${s.sourceId}(${s.distance}nm,$${s.costPerGal},tug=${s.hasTug})`
      ).join(' | ')} → ${bestSource || 'NONE'}`);

      // If all sources fail, log tug state snapshot
      if (noTugsAvailable) {
        console.log(`[SOURCE SELECT DEBUG] All tugs busy. Tug states:`);
        state.tugs.forEach(t => {
          console.log(`  ${t.id}: status=${t.status}, loc=${t.currentLocation}, attached=${kernel.getAttachedBarges(t.id).join(',') || 'none'}`);
        });
      }
    }

    return bestSource;
  };

  const tryStartNextFill = (sourceId, simTime) => {
    const sourceState = state.sources[sourceId];
    const cfg = sourceConfig[sourceId];
    if (!cfg) return false;
    const hourOfDay = getHourOfDayFromTime(simTime);
    if (!isSourceOperating(sourceId, hourOfDay)) return false;
    const sourceFillRate = cfg.flowRate * 60;

    // Use kernel to get next barge to fill (FIFO by fillRequestTime)
    // Note: getNextToFill returns the barge object, not just the ID
    const nextBarge = kernel.getNextToFill(sourceId, state.barges);

    if (!sourceState.filling && nextBarge) {
      const nextBargeId = nextBarge.id;

      // Legacy sync: also remove from queue if present
      const queueIdx = sourceState.queue.indexOf(nextBargeId);
      if (queueIdx >= 0) sourceState.queue.splice(queueIdx, 1);

      // Clear fill request in kernel
      kernel.completeFill(nextBargeId);
      const volumeToFill = nextBarge.volume - nextBarge.fillLevel;
      const fillTimeCalc = volumeToFill > 0 ? volumeToFill / sourceFillRate : 0;
      sourceState.filling = {
        bargeId: nextBargeId,
        completeHour: simTime + fillTimeCalc + switchoutTime,
        volumeToFill,
        startHour: simTime,
        fillDuration: fillTimeCalc,
        switchoutDuration: switchoutTime
      };
      nextBarge.status = 'filling';
      nextBarge.sourceId = sourceId;

      // Track fill start time for tug's journey cost calculation
      if (nextBarge.assignedTugId) {
        const assignedTug = state.tugs.find(t => t.id === nextBarge.assignedTugId);
        if (assignedTug) {
          assignedTug.journeyFillStart = simTime;
        }
      }

      const bargeSize = nextBarge.volume >= LARGE_BARGE_VOLUME ? 'large' : 'small';
      const bargeUsage = nextBarge.role === 'storage-available' ? 'storage' : 'transport';
      logAssetAction(simTime, `Barge (${bargeSize})`, nextBarge.id, 'filling', `at ${sourceId}`, {
        fillLevel: Math.round(nextBarge.fillLevel),
        capacity: nextBarge.volume,
        fillPercent: ((nextBarge.fillLevel / nextBarge.volume) * 100).toFixed(1),
        usage: bargeUsage
      });
      return true;
    }
    return false;
  };

  const startPumpingAtStop = (tug, rigName, simTime) => {
    const stop = tug.deliveryPlan[tug.currentStopIndex];
    if (!stop) return;
    tug.status = 'at-rig';
    tug.targetRig = rigName;
    tug.pumpPhase = 'hookup';
    const attachedBargeIds = kernel.getAttachedBarges(tug.id);
    logAssetAction(simTime, 'Tug', tug.id, 'at-rig', `at ${stop.crossing}`, {
      waterVolume: Math.round(tug.totalVolumeRemaining),
      targetRig: rigName,
      pumpPhase: 'hookup',
      attachedBargeIds: attachedBargeIds
    });
    tug.phaseCompleteHour = simTime + hookupTime;
    tug.volumeDeliveredThisStop = 0;

    const effectiveCapacity = getEffectiveStorageCapacity(rigName);
    const effectiveLevel = getEffectiveStorageLevel(rigName);
    const availableCapacity = Math.max(0, effectiveCapacity - effectiveLevel);
    stop.actualVolumeToDeliver = Math.max(0, Math.min(stop.volumeToDeliver, availableCapacity, tug.totalVolumeRemaining));

    attachedBargeIds.forEach(bid => {
      const barge = state.barges.find(b => b.id === bid);
      if (barge) barge.status = 'at-rig';
    });
    state.rigQueues[rigName].pumping = tug.id;
  };

  const tryStartNextPump = (rigName, simTime) => {
    const rigQueue = state.rigQueues[rigName];
    if (!rigQueue.pumping && rigQueue.queue.length > 0) {
      const nextTugId = rigQueue.queue.shift();
      const nextTug = state.tugs.find(t => t.id === nextTugId);
      if (nextTug) {
        startPumpingAtStop(nextTug, rigName, simTime);
        return true;
      }
    }
    return false;
  };

  const processMultiStopPumping = (tug, simTime, dayResult) => {
    if (tug.status !== 'at-rig' || simTime < tug.phaseCompleteHour) return false;
    const rigName = tug.targetRig;
    const stop = tug.deliveryPlan[tug.currentStopIndex];
    if (!stop) return false;

    if (tug.pumpPhase === 'hookup') {
      tug.pumpPhase = 'pumping';
      const volumeToPump = stop.actualVolumeToDeliver || 0;
      tug.phaseCompleteHour = simTime + (volumeToPump > 0 ? volumeToPump / PUMP_TO_STORAGE_RATE : 0);
      return false;
    }

    if (tug.pumpPhase === 'pumping') {
      const volumeDelivered = stop.actualVolumeToDeliver || 0;
      const currentLevel = getEffectiveStorageLevel(rigName);
      const hddCrossing = stop.crossing;

      // Update storage levels (distributes water to individual stationed barges)
      setEffectiveStorageLevel(rigName, currentLevel + volumeDelivered, simTime, hddCrossing);

      // Log stationed barge updates from hddStorage (use "filling" for consistency with source filling)
      // Only log barges that actually have water (don't log "filling at 0")
      if (hddCrossing && state.hddStorage[hddCrossing]) {
        state.hddStorage[hddCrossing].stationedBarges.forEach(bargeId => {
          const barge = state.barges.find(b => b.id === bargeId);
          if (barge && barge.fillLevel > 0) {
            logAssetAction(simTime, `Barge (${barge.type})`, bargeId, 'filling', `at ${hddCrossing}`, {
              fillLevel: Math.round(barge.fillLevel),
              capacity: barge.volume,
              fillPercent: ((barge.fillLevel / barge.volume) * 100).toFixed(1),
              usage: 'storage',
              assignedHDD: hddCrossing
            });
          }
        });
      }

      dayResult.injected += volumeDelivered;
      if (dayResult.rigInjected) dayResult.rigInjected[rigName] += volumeDelivered;
      tug.volumeDeliveredThisStop = volumeDelivered;
      tug.totalVolumeRemaining -= volumeDelivered;

      // Track hourly injection for this HDD crossing
      const crossing = stop.crossing;
      const bargesUsed = kernel.getAttachedBarges(tug.id);
      if (crossing && hourlyInjectionByHDD[crossing]) {
        const hourKey = Math.floor(simTime);
        hourlyInjectionByHDD[crossing][hourKey] = (hourlyInjectionByHDD[crossing][hourKey] || 0) + volumeDelivered;

        // Track injection event with barge info for chart labels
        if (injectionEventsByHDD[crossing] && volumeDelivered > 0) {
          injectionEventsByHDD[crossing].push({
            timestamp: simTime,
            hourKey: hourKey,
            volume: volumeDelivered,
            bargeIds: [...bargesUsed],
            tugId: tug.id
          });
        }
      }
      const totalBargeVolume = bargesUsed.reduce((sum, bid) => {
        const b = state.barges.find(x => x.id === bid);
        return sum + (b?.fillLevel || 0);
      }, 0);

      if (totalBargeVolume > 0) {
        bargesUsed.forEach(bid => {
          const barge = state.barges.find(b => b.id === bid);
          if (barge && barge.fillLevel > 0) {
            const proportion = barge.fillLevel / totalBargeVolume;
            barge.fillLevel = Math.max(0, barge.fillLevel - (volumeDelivered * proportion));
          }
        });
      }

      costTracking.totalStops++;
      tug.pumpPhase = 'disconnect';
      tug.phaseCompleteHour = simTime + hookupTime;
      return false;
    }

    if (tug.pumpPhase === 'disconnect') {
      state.rigQueues[rigName].pumping = null;
      tryStartNextPump(rigName, simTime);
      tug.currentStopIndex++;
      const currentCrossing = stop.crossing;

      // =========================================================================
      // HDD-TO-HDD DIRECT ROUTING - Check if direct delivery to another HDD is better
      // This replaces the old 5nm-only multi-stop logic with a value-based evaluation
      // =========================================================================
      if (tug.totalVolumeRemaining >= 50000) {  // 50k minimum for direct delivery
        const directDelivery = findBestDirectHDDDelivery(
          currentCrossing,
          rigName,
          tug.totalVolumeRemaining,
          simTime
        );

        if (directDelivery) {
          // Direct HDD-to-HDD delivery is worthwhile
          const dateStr = getDateStrFromTime(simTime);
          console.log(`[DIRECT-HDD-DELIVERY] ${tug.id}: ${currentCrossing} -> ${directDelivery.crossing} direct`);
          console.log(`  Remaining water: ${(tug.totalVolumeRemaining/1000).toFixed(0)}k gal`);
          console.log(`  Target ${directDelivery.crossing}: ${directDelivery.effectiveHoursOfSupply.toFixed(1)}h supply, ${(directDelivery.availableCapacity/1000).toFixed(0)}k capacity`);
          console.log(`  Distance: ${directDelivery.distance.toFixed(1)}nm, Travel: ${directDelivery.travelTime.toFixed(1)}h`);

          // Add to delivery plan and dispatch
          tug.deliveryPlan.push({
            rig: directDelivery.rig,
            crossing: directDelivery.crossing,
            volumeToDeliver: Math.min(tug.totalVolumeRemaining, directDelivery.availableCapacity),
            urgency: 300,  // High urgency for direct delivery
            isDirectHDDDelivery: true
          });

          tug.status = 'direct-hdd-delivery';  // New status for animation tracking
          tug.targetCrossing = directDelivery.crossing;
          tug.targetRig = directDelivery.rig;
          tug.arrivalHour = simTime + directDelivery.travelTime;
          tug.pumpPhase = 'none';
          tug.directDeliverySource = currentCrossing;  // Track origin HDD

          // Update barges status
          const attachedBargeIds = kernel.getAttachedBarges(tug.id);
          attachedBargeIds.forEach(bid => {
            const barge = state.barges.find(b => b.id === bid);
            if (barge) {
              barge.status = 'direct-hdd-delivery';
              const bargeSize = barge.volume >= LARGE_BARGE_VOLUME ? 'large' : 'small';
              logAssetAction(simTime, `Barge (${bargeSize})`, barge.id, 'direct-hdd-delivery',
                `${currentCrossing} -> ${directDelivery.crossing}`, {
                  fillLevel: Math.round(barge.fillLevel),
                  capacity: barge.volume,
                  fillPercent: ((barge.fillLevel / barge.volume) * 100).toFixed(1),
                  usage: 'transport',
                  assignedTugId: barge.assignedTugId,
                  sourceId: currentCrossing,  // Origin HDD for animation
                  targetCrossing: directDelivery.crossing
                });
            }
          });

          logAssetAction(simTime, 'Tug', tug.id, 'direct-hdd-delivery',
            `${currentCrossing} -> ${directDelivery.crossing}`, {
              waterVolume: Math.round(tug.totalVolumeRemaining),
              targetHDD: directDelivery.crossing,
              targetRig: directDelivery.rig,
              targetCrossing: directDelivery.crossing,
              sourceId: currentCrossing,  // Origin HDD for animation
              distance: directDelivery.distance,
              timeSaved: directDelivery.timeSaved,
              attachedBargeIds
            });

          // Track statistics
          costTracking.directHDDTrips++;
          costTracking.directHDDTimeSaved += directDelivery.timeSaved;
          costTracking.multiStopTrips++;

          return false;
        }
      }

      // =========================================================================
      // LEGACY: Check planned multi-stop deliveries (if any remain in plan)
      // =========================================================================
      if (tug.currentStopIndex < tug.deliveryPlan.length && tug.totalVolumeRemaining > 0) {
        const nextStop = tug.deliveryPlan[tug.currentStopIndex];
        const distanceToNext = hddToHddMatrix[currentCrossing]?.[nextStop.crossing] || 5;
        tug.status = 'traveling-to-next-stop';
        tug.targetCrossing = nextStop.crossing;
        tug.targetRig = nextStop.rig;
        tug.arrivalHour = simTime + getTravelTime(distanceToNext, true, tugHasLargeBarges(tug));
        tug.pumpPhase = 'none';
        costTracking.multiStopTrips++;
        return false;
      }

      // =========================================================================
      // THROTTLED ON-SITE PUMPING - Stay at HDD if storage full and water remaining
      // =========================================================================
      const effectiveCapacity = getEffectiveStorageCapacity(rigName);
      const effectiveLevel = getEffectiveStorageLevel(rigName);
      const availableCapacity = Math.max(0, effectiveCapacity - effectiveLevel);
      const waterRemaining = tug.totalVolumeRemaining;

      // Conditions for throttled standby:
      // 1. Tug has significant water remaining (>30k gallons)
      // 2. Storage is nearly full (available capacity < 50k)
      // 3. No other HDDs urgently need water
      const THROTTLE_MIN_WATER = 30000;  // 30k gallons minimum
      const THROTTLE_STORAGE_THRESHOLD = 50000;  // Storage nearly full if <50k available

      if (waterRemaining > THROTTLE_MIN_WATER && availableCapacity < THROTTLE_STORAGE_THRESHOLD) {
        // Check if any other HDD urgently needs water
        const needyHDDs = findHDDsNeedingWater(simTime);
        const otherHDDsNeedWater = needyHDDs.some(hdd =>
          hdd.crossing !== currentCrossing && hdd.effectiveHoursOfSupply < 12  // Increased from 8 to 12 hours
        );

        // Count throttled and available tugs
        const otherThrottledTugs = state.tugs.filter(t =>
          t.id !== tug.id &&
          t.status === 'throttled-standby'
        ).length;

        const otherIdleTugs = state.tugs.filter(t =>
          t.id !== tug.id &&
          t.status === 'idle'
        ).length;

        // Count active HDDs (drilling right now, consuming water)
        const activeHDDCount = needyHDDs.length;

        // Count total mobilized tugs
        const totalMobilizedTugs = state.tugs.filter(t =>
          t.status !== 'demobbed'
        ).length;

        // SAFETY RULE: Always keep at least 2 tugs available for dispatch
        // - If we throttle this tug: 1 more will be throttled
        // - Available for dispatch = total - throttled - this one
        const wouldBeThrottled = otherThrottledTugs + 1;
        const wouldBeAvailable = totalMobilizedTugs - wouldBeThrottled;

        // Safe to throttle if we'd still have at least 2 tugs available
        // Exception: If only 1 active HDD, can throttle with fewer available tugs
        const minAvailableNeeded = activeHDDCount <= 1 ? 1 : 2;
        const canThrottle = wouldBeAvailable >= minAvailableNeeded;

        if (!otherHDDsNeedWater && canThrottle) {
          // Enter throttled standby mode - stay at HDD and pump to match consumption
          const rigStatus = getRigStatus(rigName, getDateStrFromTime(simTime));
          const consumptionRate = rigStatus.hourlyRate || 0;

          console.log(`[THROTTLED-STANDBY] ${tug.id} staying at ${currentCrossing}`);
          console.log(`  Water remaining: ${(waterRemaining/1000).toFixed(0)}k gal, Storage available: ${(availableCapacity/1000).toFixed(0)}k gal`);
          console.log(`  Active HDDs: ${activeHDDCount}, Tugs: ${totalMobilizedTugs} total, ${otherThrottledTugs} throttled, ${wouldBeAvailable} would remain available (min needed: ${minAvailableNeeded})`);

          tug.status = 'throttled-standby';
          tug.targetRig = rigName;
          tug.targetCrossing = currentCrossing;
          tug.pumpPhase = 'none';
          tug.throttleConsumptionRate = consumptionRate;
          tug.nextThrottlePumpTime = simTime + 0.5; // Check again in 30 minutes
          tug.currentStopIndex = 0;
          tug.deliveryPlan = [];

          const attachedBargeIds = kernel.getAttachedBarges(tug.id);
          attachedBargeIds.forEach(bid => {
            const barge = state.barges.find(b => b.id === bid);
            if (barge) {
              barge.status = 'throttled-standby';
              barge.currentLocation = currentCrossing;  // Barge is at the HDD, not at source
            }
          });

          logAssetAction(simTime, 'Tug', tug.id, 'throttled-standby', `at ${currentCrossing}`, {
            waterVolume: Math.round(waterRemaining),
            targetRig: rigName,
            targetCrossing: currentCrossing,
            consumptionRate: consumptionRate,
            attachedBargeIds
          });

          // Track this as a throttle event
          if (!costTracking.throttledStandbyEvents) costTracking.throttledStandbyEvents = 0;
          costTracking.throttledStandbyEvents++;

          return false;
        }
      }

      // =========================================================================
      // RETURN TO SOURCE - No direct delivery opportunity found
      // =========================================================================
      costTracking.totalTrips++;
      if (tug.sourceId) costTracking.tripsPerSource[tug.sourceId]++;
      if (tug.deliveryPlan.length === 1) costTracking.singleStopTrips++;

      // Calculate best return source FIRST (before logging barges)
      // Use THROUGHPUT-based selection (cycle time) not just distance
      // A source with 4x flow rate saves far more time than travel distance difference
      const { bestSource: bestReturnSource, bestCycleTime } = getBestSourceByThroughput(stop.crossing);
      const bestReturnDist = getDistance(bestReturnSource, stop.crossing);

      // Update tug status and log return journey
      tug.status = 'en-route-empty';
      tug.returnHour = simTime + getTravelTime(bestReturnDist, false, tugHasLargeBarges(tug));
      tug.deliveryPlan = [];
      tug.intendedReturnSource = bestReturnSource;  // Store intended source for arrival handler
      const attachedBargeIdsForReturn = kernel.getAttachedBarges(tug.id);
      logAssetAction(simTime, 'Tug', tug.id, 'en-route-empty', `returning to ${bestReturnSource}`, {
        attachedBargeIds: attachedBargeIdsForReturn,
        sourceId: bestReturnSource,
        targetCrossing: stop.crossing,
        targetRig: stop.rig
      });
      tug.currentStopIndex = 0;
      tug.pumpPhase = 'none';

      // Now log attached barges with the SAME return source
      attachedBargeIdsForReturn.forEach(bid => {
        const barge = state.barges.find(b => b.id === bid);
        if (barge) {
          barge.status = 'en-route-empty';
          barge.currentLocation = null;  // In transit
          const bargeSize = barge.volume >= LARGE_BARGE_VOLUME ? 'large' : 'small';
          logAssetAction(simTime, `Barge (${bargeSize})`, barge.id, 'en-route-empty', `returning to ${bestReturnSource}`, {
            fillLevel: Math.round(barge.fillLevel),
            capacity: barge.volume,
            fillPercent: ((barge.fillLevel / barge.volume) * 100).toFixed(1),
            usage: 'transport',
            assignedTugId: barge.assignedTugId
          });
        }
      });
      return true;
    }
    return false;
  };

  const planDeliveryRoute = (needs, sourceId, waterAvailable) => {
    if (needs.length === 0) return [];
    const sortedNeeds = [...needs].sort((a, b) => b.urgency - a.urgency);
    return [{
      rig: sortedNeeds[0].rig, crossing: sortedNeeds[0].hddCrossing,
      volumeToDeliver: waterAvailable, urgency: sortedNeeds[0].urgency
    }];
  };

  // Average distance for dispatch planning
  let totalCapacityWeightedDistance = 0, totalWeight = 0;
  hddSchedule.forEach(hdd => {
    const best = bestSourceForHDD[hdd.crossing];
    if (best) { totalCapacityWeightedDistance += best.distance; totalWeight++; }
  });
  const avgBestDistance = totalWeight > 0 ? totalCapacityWeightedDistance / totalWeight : 14;

  // Initialize hourly storage tracking for each HDD
  const hourlyStorageByHDD = {};
  const hourlyInjectionByHDD = {}; // Track water injected per hour per HDD
  const hourlyWithdrawalByHDD = {}; // Track water consumed per hour per HDD
  const injectionEventsByHDD = {}; // Track individual injection events with barge info
  const storageBargeChangesByHDD = {}; // Track storage barge allocation changes
  hddSchedule.forEach(hdd => {
    hourlyStorageByHDD[hdd.crossing] = [];
    hourlyInjectionByHDD[hdd.crossing] = {}; // timestamp -> gallons injected
    hourlyWithdrawalByHDD[hdd.crossing] = {}; // timestamp -> gallons consumed
    injectionEventsByHDD[hdd.crossing] = []; // Array of {timestamp, volume, bargeIds, tugId}
    storageBargeChangesByHDD[hdd.crossing] = []; // Array of {timestamp, bargeIds, totalCapacity}
  });

  // Start initial fills at all sources (barges start empty in unified model)
  Object.keys(sourceConfig).forEach(sourceId => {
    tryStartNextFill(sourceId, 0);
  });

  // ============================================================================
  // MAIN SIMULATION LOOP
  // ============================================================================
  let lastStorageCheckHour = -1; // Track last hour we checked for storage needs
  let lastMobCheckHour = -1;  // Track last hour we checked mobilization needs
  let lastDemobCheckHour = -1;  // Track last hour we checked demobilization needs
  for (let simTime = 0; simTime < totalHours; simTime += TIME_STEP) {
    const dateStr = getDateStrFromTime(simTime);
    const hourOfDay = getHourOfDayFromTime(simTime);
    const dayResult = dailyResults[dateStr];

    // ============================================================================
    // DYNAMIC BARGE MOBILIZATION - Check once per hour
    // ============================================================================
    const currentHour = Math.floor(simTime);
    if (currentHour !== lastMobCheckHour) {
      checkBargeMobilizationNeeds(simTime);
      lastMobCheckHour = currentHour;
    }

    // ============================================================================
    // DEMOBILIZATION CHECKS - Check once per day at midnight
    // ============================================================================
    if (currentHour !== lastDemobCheckHour && hourOfDay === 0) {
      checkBargeDemobilization(simTime);
      checkTugDemobilization(simTime);
      lastDemobCheckHour = currentHour;
    }

    // Check for storage barge deliveries that need dispatch
    // With retry cooldown to prevent infinite loop when resources unavailable
    storageDeliverySchedule.forEach(delivery => {
      if (delivery.status === 'pending' && simTime >= delivery.dispatchTime) {
        // Skip if we're in a cooldown period after a failed attempt
        if (delivery.nextRetryTime && simTime < delivery.nextRetryTime) {
          return; // Skip this delivery until cooldown expires
        }

        const dispatched = dispatchStorageDelivery(delivery, simTime);
        if (!dispatched) {
          // Failed to dispatch - set cooldown before next retry
          delivery.failedAttempts = (delivery.failedAttempts || 0) + 1;
          delivery.nextRetryTime = simTime + 1; // Retry after 1 hour

          // After many failures, log a warning and increase cooldown
          if (delivery.failedAttempts === 5) {
            console.log(`[STORAGE DISPATCH STALLED] ${delivery.hdd}: 5 failed attempts, resources may be constrained`);
          }
          if (delivery.failedAttempts >= 10) {
            delivery.nextRetryTime = simTime + 4; // Retry every 4 hours after 10 failures
          }
        }
      }
    });

    // ============================================================================
    // PROACTIVE TRANSPORT SCHEDULING - STEP 4: Process Scheduled Deliveries
    // ============================================================================
    if (USE_SCHEDULED_DISPATCH) {
      transportDeliverySchedule.forEach(delivery => {
        if (delivery.status === 'pending' &&
            (delivery.priority === 'scheduled' || delivery.priority === 'deferred') &&
            simTime >= delivery.dispatchTime) {

          const dispatched = dispatchScheduledTransport(delivery, simTime);
          if (dispatched) {
            delivery.status = 'dispatched';
          } else {
            // Mark for reactive fallback with urgency boost
            delivery.priority = 'reactive-fallback';
            delivery.originalDispatchTime = delivery.originalDispatchTime || delivery.dispatchTime;
            delivery.failedAttempts = (delivery.failedAttempts || 0) + 1;

            // Don't spam retry - wait at least 1 hour before trying again
            if (delivery.failedAttempts >= 3) {
              delivery.status = 'abandoned'; // Let reactive dispatch handle it
              console.log(`[SCHEDULE WARNING] ${delivery.id}: Abandoned after 3 failures, reactive dispatch will handle ${delivery.hdd}`);
            }
          }
        }
      });
    }

    // DISABLED: This function was causing infinite loops by creating new delivery attempts every call
    // The storageDeliverySchedule is the primary mechanism for storage delivery
    // This function should only be used for edge cases (barges returning early, etc.)
    // For now, rely entirely on scheduled deliveries
    //
    // Check for HDDs that need storage delivered from source (for barges returned to source)
    // Only check once per hour to avoid repeated failed attempts
    // const currentHour = Math.floor(simTime);
    // if (currentHour !== lastStorageCheckHour) {
    //   checkForHDDsNeedingStorage(simTime);
    //   lastStorageCheckHour = currentHour;
    // }

    // Check for tugs arriving at HDDs with storage barges
    state.tugs.forEach(tug => {
      if (tug.status === 'en-route-storage' && simTime >= tug.arrivalHour) {
        stationBargesAtHDD(tug, simTime);
      }
    });

    // Check for tugs arriving at barge source for cross-source pickup
    state.tugs.forEach(tug => {
      if (tug.status === 'en-route-pickup' && simTime >= tug.arrivalHour) {
        const bargeSource = tug.targetCrossing;  // In this case, targetCrossing is the source
        const bargeIds = tug.pickupBargeIds || [];
        const finalDest = tug.finalDestination;
        const finalRig = tug.finalRig;

        console.log(`[STORAGE PICKUP] ${tug.id} arrived at ${bargeSource} to pick up barges ${bargeIds.join(', ')} for ${finalDest}`);

        // Pick up the reserved barges
        const bargesToAttach = state.barges.filter(b => bargeIds.includes(b.id));
        if (bargesToAttach.length === 0) {
          console.log(`[STORAGE PICKUP ERROR] No barges found at ${bargeSource} with IDs: ${bargeIds.join(', ')}`);
          tug.status = 'idle';
          tug.currentLocation = bargeSource;
          return;
        }

        // Attach barges using kernel
        kernel.attachBarges(tug.id, bargeIds, state.barges, simTime, 'crossSourcePickup');

        // Update barge states for storage delivery
        bargesToAttach.forEach(b => {
          b.role = 'storage';
          b.assignedHDD = finalDest;
          b.status = 'en-route-storage';
          b.reservedByTug = null;
        });

        // Continue to final HDD destination
        tug.attachedBargeIds = bargeIds;
        tug.status = 'en-route-storage';
        tug.targetCrossing = finalDest;
        tug.targetRig = finalRig;
        tug.currentLocation = bargeSource;
        const travelToHDD = getDistance(bargeSource, finalDest) / loadedSpeedSmall;
        tug.arrivalHour = simTime + travelToHDD;

        logAssetAction(simTime, 'Tug', tug.id, 'en-route-storage', `to ${finalDest} with ${bargesToAttach.length} storage barges`, {
          bargeIds,
          sourceId: bargeSource,
          targetCrossing: finalDest,
          targetRig: finalRig
        });

        // Clear pickup-specific fields
        tug.pickupBargeIds = null;
        tug.finalDestination = null;
        tug.finalRig = null;
        tug.storageDelivery = null;
      }
    });

    // Check for HDD completions and initiate demobilization
    checkHDDCompletions(simTime);

    // Execute same-rig storage barge transfers (support tug moves - outside our model)
    // This updates barge assignments when next HDD pilot starts
    executeSameRigTransfers(simTime);

    // Check for demobilization tasks waiting for tugs
    Object.values(state.hddStorage).forEach(hddStorage => {
      if (hddStorage.demobTask && hddStorage.demobTask.status === 'waiting-for-tug') {
        dispatchDemobilization(hddStorage.demobTask, simTime);
      }
    });

    // Check for tugs arriving to pick up storage barges for demob
    state.tugs.forEach(tug => {
      if (tug.status === 'en-route-demob' && simTime >= tug.arrivalHour) {
        pickupStorageBarges(tug, simTime);
      }
    });

    // Check for return-to-source tasks waiting for tugs
    Object.values(state.hddStorage).forEach(hddStorage => {
      if (hddStorage.returnToSourceTask && hddStorage.returnToSourceTask.status === 'waiting-for-tug') {
        dispatchReturnToSource(hddStorage.returnToSourceTask, simTime);
      }
    });

    // Check for tugs arriving to pick up barges for return-to-source
    state.tugs.forEach(tug => {
      if (tug.status === 'en-route-return-to-source' && simTime >= tug.arrivalHour) {
        pickupBargesReturnToSource(tug, simTime);
      }
    });

    // Check for transfer tasks waiting for tugs
    Object.values(state.hddStorage).forEach(hddStorage => {
      if (hddStorage.transferTask && hddStorage.transferTask.status === 'waiting-for-tug') {
        dispatchTransfer(hddStorage.transferTask, simTime);
      }
    });

    // Check for tugs arriving to pick up barges for transfer
    state.tugs.forEach(tug => {
      if (tug.status === 'en-route-transfer-pickup' && simTime >= tug.arrivalHour) {
        pickupForTransfer(tug, simTime);
      }
    });

    // Check for tugs arriving to deliver transferred barges
    state.tugs.forEach(tug => {
      if (tug.status === 'en-route-transfer-delivery' && simTime >= tug.arrivalHour) {
        deliverTransferBarges(tug, simTime);
      }
    });

    // Capture storage level at 7:00 AM (start of workday) - only once per day
    const hourInDay = simTime % 24;
    if (hourOfDay === DRILLING_START_HOUR && hourInDay >= DRILLING_START_HOUR && hourInDay < DRILLING_START_HOUR + TIME_STEP) {
      rigNames.forEach(rigName => {
        dayResult.storageLevel[rigName] = getEffectiveStorageLevel(rigName);
      });
    }

    if (!dayResult) continue;

    // Build rigStatuses dynamically from all rig names
    const rigStatuses = {};
    rigNames.forEach(rigName => {
      rigStatuses[rigName] = getRigStatus(rigName, dateStr);
    });

    // Update current rig statuses for storage level functions
    currentRigStatuses = rigStatuses;

    // TODO Phase 4: Update stationed storage barge levels from state.hddStorage
    // Old aggregate tracking removed - will be replaced with individual barge tracking

    // Track storage every hour (24 hours/day) for each HDD from rigUp to rigDown
    // SKIP during optimization to prevent memory crashes (creates 29K+ data points per simulation)
    if (!CONFIG._optimizationMode) {
      const hourFloat = hourOfDay;
      const isWholeHour = Math.abs(hourFloat - Math.floor(hourFloat)) < TIME_STEP;

      if (isWholeHour) {
        // Every hour (all day, not just drilling hours)
        rigNames.forEach(rigName => {
          const rigStatus = rigStatuses[rigName];
          if (rigStatus && rigStatus.hdd && rigStatus.stage !== 'Idle') {
            const storageLevel = getEffectiveStorageLevel(rigName);
            const storageCapacity = getEffectiveStorageCapacity(rigName);
            const isWorkingHour = isDrillingHour(simTime);

            hourlyStorageByHDD[rigStatus.hdd].push({
              timestamp: simTime,
              date: dateStr,
              hour: hourFloat.toFixed(2),
              rigName,
              storageLevel,
              storageCapacity,
              fillPercent: parseFloat(((storageLevel / storageCapacity) * 100).toFixed(1)),
              stage: rigStatus.stage,
              isWorkingHour  // NEW: Flag to shade non-working hours
            });
          }
        });
      }
    }

    // Daily: assign large storage barges to highest-demand active rigs
    if (hourOfDay === 0 && simTime % 24 < TIME_STEP) {
      Object.keys(state.sources).forEach(sourceId => {
        state.sources[sourceId].dailyHoursUsed = 0;
      });

      // TODO Phase 5: Implement large storage barge relocation logic
      // Old daily reassignment removed - will be replaced with physical journeys
    }

    // Fuel tracking
    state.tugs.forEach(tug => {
      if (['en-route-loaded', 'en-route-empty', 'traveling-to-next-stop'].includes(tug.status)) {
        costTracking.fuelRunningGallons += COST.tug.fuelRunningGPH * TIME_STEP;
        costTracking.tugRunningHours += TIME_STEP;
        dayResult.fuelRunning += COST.tug.fuelRunningGPH * TIME_STEP;
      } else {
        costTracking.fuelIdleGallons += COST.tug.fuelIdleGPH * TIME_STEP;
        costTracking.tugIdleHours += TIME_STEP;
        dayResult.fuelIdle += COST.tug.fuelIdleGPH * TIME_STEP;
      }
    });

    // Water consumption
    if (isDrillingHour(simTime)) {
      rigNames.forEach(rigName => {
        const status = rigStatuses[rigName];

        // Close ran dry periods when rig goes idle/RigDown (no longer needs water)
        // This fixes a bug where ran dry periods stayed open across HDD transitions
        if (status.hourlyRate === 0 || ['RigUp', 'RigDown', 'Idle'].includes(status.stage)) {
          if (activeRanDryPeriods[rigName]) {
            const period = activeRanDryPeriods[rigName];
            const duration = simTime - period.startTime;
            ranDryEvents.push({
              rig: rigName,
              startTime: period.startTime,
              endTime: simTime,
              startTimestamp: period.startTimestamp,
              endTimestamp: `${getDateStrFromTime(simTime)} ${getHourOfDayFromTime(simTime).toFixed(2)}:00`,
              durationHours: duration.toFixed(2),
              phase: period.phase,
              hdd: period.hdd
            });
            delete activeRanDryPeriods[rigName];
          }
        }

        if (status.hourlyRate > 0 && !['RigUp', 'RigDown'].includes(status.stage)) {
          const demandThisTick = status.hourlyRate * TIME_STEP;
          const effectiveLevel = getEffectiveStorageLevel(rigName);
          const consumed = consumeWaterFromRig(rigName, Math.min(effectiveLevel, demandThisTick), null, simTime);
          const deficitThisTick = demandThisTick - consumed;

          // Track hourly withdrawal for chart
          const crossing = status.hdd;
          if (crossing && hourlyWithdrawalByHDD[crossing] && consumed > 0) {
            const hourKey = Math.floor(simTime);
            hourlyWithdrawalByHDD[crossing][hourKey] = (hourlyWithdrawalByHDD[crossing][hourKey] || 0) + consumed;
          }

          dayResult.demand += demandThisTick;
          dayResult.usage += consumed;
          if (dayResult.rigDemand) dayResult.rigDemand[rigName] += demandThisTick;

          if (deficitThisTick > 0) {
            dayResult.deficit += deficitThisTick;
            dayResult.rigDeficit[rigName] += deficitThisTick;
            dayResult.ranDry[rigName] = true;
            costTracking.downtimeHours[rigName] += TIME_STEP;
            dayResult.downtimeHours[rigName] += TIME_STEP;

            // Track detailed ran dry events (start/end times)
            if (!activeRanDryPeriods[rigName]) {
              // Start a new ran dry period
              activeRanDryPeriods[rigName] = {
                rig: rigName,
                startTime: simTime,
                startTimestamp: `${getDateStrFromTime(simTime)} ${getHourOfDayFromTime(simTime).toFixed(2)}:00`,
                phase: status.stage,
                hdd: status.hdd
              };
            }
          } else {
            // Rig has water - close any active ran dry period
            if (activeRanDryPeriods[rigName]) {
              const period = activeRanDryPeriods[rigName];
              const duration = simTime - period.startTime;
              ranDryEvents.push({
                rig: rigName,
                startTime: period.startTime,
                endTime: simTime,
                startTimestamp: period.startTimestamp,
                endTimestamp: `${getDateStrFromTime(simTime)} ${getHourOfDayFromTime(simTime).toFixed(2)}:00`,
                durationHours: duration.toFixed(2),
                phase: period.phase,
                hdd: period.hdd
              });
              delete activeRanDryPeriods[rigName];
            }
          }
        }
      });
    }

    // Process filling
    Object.keys(state.sources).forEach(sourceId => {
      const sourceState = state.sources[sourceId];
      if (sourceState.filling && simTime >= sourceState.filling.completeHour) {
        const barge = state.barges.find(b => b.id === sourceState.filling.bargeId);
        if (barge) {
          barge.fillLevel = barge.volume;
          barge.status = 'idle';

          // DEBUG: Log fill completion on Aug 16
          const fillDateStr = getDateStrFromTime(simTime);
          if (fillDateStr === '2026-08-16') {
            console.log(`[DEBUG ${fillDateStr}] Fill complete: ${barge.id} now has ${barge.fillLevel} gal, status=${barge.status}, role=${barge.role}, assignedTugId=${barge.assignedTugId}`);
          }

          // CRITICAL FIX: Ensure barge is properly tracked after filling
          // If barge has assignedTugId, verify the tug actually has it attached
          // If not, clear assignedTugId and add to pool to prevent orphaned barges
          if (barge.assignedTugId) {
            const assignedTug = state.tugs.find(t => t.id === barge.assignedTugId);
            const tugAttachedList = assignedTug ? kernel.getAttachedBarges(assignedTug.id) : [];
            if (!assignedTug || !tugAttachedList.includes(barge.id)) {
              // Tug doesn't have this barge attached - orphaned state
              // Clear assignment and add to pool
              if (fillDateStr === '2026-08-16') {
                console.log(`[DEBUG ${fillDateStr}] Fill complete: ${barge.id} was orphaned (assignedTug=${barge.assignedTugId} doesn't have it). Adding to pool.`);
              }
              barge.assignedTugId = null;
              if (!sourceState.availableBarges.includes(barge.id)) {
                sourceState.availableBarges.push(barge.id);
              }
            }
          } else {
            // No assigned tug - add to pool if not already there
            if (!sourceState.availableBarges.includes(barge.id)) {
              sourceState.availableBarges.push(barge.id);
            }
          }

          // Track water volume from this source (actual amount filled, not barge capacity)
          const volumeFilled = sourceState.filling.volumeToFill || 0;
          costTracking.waterVolumeBySource[sourceId] += volumeFilled;

          const bargeSize = barge.volume >= LARGE_BARGE_VOLUME ? 'large' : 'small';
          const bargeUsage = barge.role === 'storage-available' ? 'storage' : 'transport';
          logAssetAction(simTime, `Barge (${bargeSize})`, barge.id, 'idle', `at ${sourceId}`, {
            fillLevel: Math.round(barge.fillLevel),
            capacity: barge.volume,
            fillPercent: '100.0',
            usage: bargeUsage
          });

          // Track source utilization in daily results
          // Note: Cap at 24h max per day - fills spanning days are attributed to completion day
          const fillInfo = sourceState.filling;
          if (fillInfo.fillDuration && fillInfo.switchoutDuration) {
            // Calculate how much of this fill occurred TODAY vs previous days
            const fillStartHour = fillInfo.startHour || (simTime - fillInfo.fillDuration);
            const dayStartHour = Math.floor(simTime / 24) * 24;
            const hoursInThisDay = Math.min(fillInfo.fillDuration, simTime - Math.max(fillStartHour, dayStartHour));
            dayResult.sourceUtilization[sourceId].fillHours += Math.max(0, hoursInThisDay);
            dayResult.sourceUtilization[sourceId].switchoutHours += fillInfo.switchoutDuration;
          }
        }
        sourceState.filling = null;
        tryStartNextFill(sourceId, simTime);
      }
    });

    // Process tugs
    state.tugs.forEach(tug => {
      if (tug.status === 'en-route-loaded' && simTime >= tug.arrivalHour) {
        tug.status = 'arrived-at-rig';
        const attachedBargeIdsArrival = kernel.getAttachedBarges(tug.id);
        logAssetAction(simTime, 'Tug', tug.id, 'arrived-at-rig', `at ${tug.targetCrossing}`, {
          waterVolume: Math.round(tug.totalVolumeRemaining),
          targetRig: tug.targetRig,
          attachedBargeIds: attachedBargeIdsArrival
        });
        const rigQueue = state.rigQueues[tug.targetRig];
        if (!rigQueue.queue.includes(tug.id)) rigQueue.queue.push(tug.id);
        attachedBargeIdsArrival.forEach(bid => {
          const barge = state.barges.find(b => b.id === bid);
          if (barge) barge.status = 'arrived-at-rig';
        });
        tryStartNextPump(tug.targetRig, simTime);
      }

      if (tug.status === 'traveling-to-next-stop' && simTime >= tug.arrivalHour) {
        tug.status = 'arrived-at-rig';
        const rigQueue = state.rigQueues[tug.targetRig];
        if (!rigQueue.queue.includes(tug.id)) rigQueue.queue.push(tug.id);
        tryStartNextPump(tug.targetRig, simTime);
      }

      // Handle direct HDD-to-HDD delivery arrivals
      if (tug.status === 'direct-hdd-delivery' && simTime >= tug.arrivalHour) {
        const sourceHDD = tug.directDeliverySource;
        const targetHDD = tug.targetCrossing;
        const targetRig = tug.targetRig;

        console.log(`[DIRECT-HDD-ARRIVAL] ${tug.id}: Arrived at ${targetHDD} from ${sourceHDD}`);
        console.log(`  Water remaining: ${(tug.totalVolumeRemaining/1000).toFixed(0)}k gal`);

        // Track water delivered via direct routing
        costTracking.directHDDWaterDelivered += tug.totalVolumeRemaining;

        // Update barge statuses
        const attachedBargeIds = kernel.getAttachedBarges(tug.id);
        attachedBargeIds.forEach(bid => {
          const barge = state.barges.find(b => b.id === bid);
          if (barge) barge.status = 'arrived-at-rig';
        });

        // Clear direct delivery tracking
        tug.directDeliverySource = null;

        // Transition to arrived-at-rig and queue for pumping
        tug.status = 'arrived-at-rig';
        const rigQueue = state.rigQueues[targetRig];
        if (!rigQueue.queue.includes(tug.id)) rigQueue.queue.push(tug.id);

        logAssetAction(simTime, 'Tug', tug.id, 'arrived-at-rig', `at ${targetHDD} (direct from ${sourceHDD})`, {
          waterVolume: Math.round(tug.totalVolumeRemaining),
          targetRig,
          targetCrossing: targetHDD,
          sourceId: sourceHDD,  // Origin HDD for animation tracking
          attachedBargeIds,
          purpose: 'hdd-to-hdd-direct-arrived'
        });

        tryStartNextPump(targetRig, simTime);
      }

      if (tug.status === 'at-rig') {
        processMultiStopPumping(tug, simTime, dayResult);
      }

      // Handle throttled standby - tug stays at HDD and pumps to match consumption
      if (tug.status === 'throttled-standby') {
        const rigName = tug.targetRig;
        const crossing = tug.targetCrossing;

        // Periodically check conditions and pump small amounts
        if (simTime >= tug.nextThrottlePumpTime) {
          const effectiveCapacity = getEffectiveStorageCapacity(rigName);
          const effectiveLevel = getEffectiveStorageLevel(rigName);
          const availableCapacity = Math.max(0, effectiveCapacity - effectiveLevel);
          const waterRemaining = tug.totalVolumeRemaining;

          // Check if other HDDs now need water urgently
          const needyHDDs = findHDDsNeedingWater(simTime);
          const urgentHDD = needyHDDs.find(hdd =>
            hdd.crossing !== crossing && hdd.effectiveHoursOfSupply < 12  // Match entry threshold
          );

          if (urgentHDD && waterRemaining > 50000) {
            // Another HDD needs water - break out and deliver there

            tug.status = 'direct-hdd-delivery';
            tug.targetCrossing = urgentHDD.crossing;
            tug.targetRig = urgentHDD.rig;
            tug.directDeliverySource = crossing;
            const distance = hddToHddMatrix[crossing]?.[urgentHDD.crossing] || 10;
            tug.arrivalHour = simTime + getTravelTime(distance, true, tugHasLargeBarges(tug));
            tug.pumpPhase = 'none';
            tug.deliveryPlan = [{
              rig: urgentHDD.rig,
              crossing: urgentHDD.crossing,
              volumeToDeliver: Math.min(waterRemaining, urgentHDD.availableCapacity),
              urgency: 300
            }];
            tug.currentStopIndex = 0;

            const attachedBargeIds = kernel.getAttachedBarges(tug.id);
            attachedBargeIds.forEach(bid => {
              const barge = state.barges.find(b => b.id === bid);
              if (barge) barge.status = 'direct-hdd-delivery';
            });

            logAssetAction(simTime, 'Tug', tug.id, 'direct-hdd-delivery',
              `${crossing} -> ${urgentHDD.crossing} (throttle-break)`, {
                waterVolume: Math.round(waterRemaining),
                targetCrossing: urgentHDD.crossing,
                targetRig: urgentHDD.rig,
                sourceId: crossing,
                attachedBargeIds
              });

            if (!costTracking.throttleBreakEvents) costTracking.throttleBreakEvents = 0;
            costTracking.throttleBreakEvents++;

          } else if (waterRemaining < 10000) {
            // Water running low - return to source (optimized threshold: 10k gal)

            const { bestSource: bestReturnSource } = getBestSourceByThroughput(crossing);
            const bestReturnDist = getDistance(bestReturnSource, crossing);

            tug.status = 'en-route-empty';
            tug.returnHour = simTime + getTravelTime(bestReturnDist, false, tugHasLargeBarges(tug));
            tug.intendedReturnSource = bestReturnSource;
            tug.targetRig = null;
            tug.targetCrossing = null;
            tug.deliveryPlan = [];
            tug.pumpPhase = 'none';

            const attachedBargeIds = kernel.getAttachedBarges(tug.id);
            attachedBargeIds.forEach(bid => {
              const barge = state.barges.find(b => b.id === bid);
              if (barge) barge.status = 'en-route-empty';
            });

            logAssetAction(simTime, 'Tug', tug.id, 'en-route-empty',
              `returning to ${bestReturnSource} (throttle-end)`, {
                attachedBargeIds,
                sourceId: bestReturnSource,
                targetCrossing: crossing
              });

          } else if (availableCapacity > 10000) {
            // Storage has room - pump a small amount matching hourly consumption
            const rigStatus = getRigStatus(rigName, getDateStrFromTime(simTime));
            const consumptionRate = rigStatus.hourlyRate || 0;
            const isDrilling = rigStatus.isActive && !['Idle', 'RigDown', 'RigUp'].includes(rigStatus.stage);

            if (isDrilling && consumptionRate > 0) {
              // Pump 1-2 hours worth of consumption
              const pumpAmount = Math.min(
                consumptionRate * 1.5,  // 1.5 hours of consumption
                availableCapacity,
                waterRemaining
              );

              if (pumpAmount > 1000) {  // Only pump if meaningful amount

                // Update storage
                setEffectiveStorageLevel(rigName, effectiveLevel + pumpAmount, simTime, crossing);

                // Deduct from barges
                const attachedBargeIds = kernel.getAttachedBarges(tug.id);
                const totalBargeVolume = attachedBargeIds.reduce((sum, bid) => {
                  const b = state.barges.find(x => x.id === bid);
                  return sum + (b?.fillLevel || 0);
                }, 0);

                if (totalBargeVolume > 0) {
                  attachedBargeIds.forEach(bid => {
                    const barge = state.barges.find(b => b.id === bid);
                    if (barge && barge.fillLevel > 0) {
                      const proportion = barge.fillLevel / totalBargeVolume;
                      barge.fillLevel = Math.max(0, barge.fillLevel - (pumpAmount * proportion));
                    }
                  });
                }

                tug.totalVolumeRemaining -= pumpAmount;
                dayResult.injected += pumpAmount;
                if (dayResult.rigInjected) dayResult.rigInjected[rigName] += pumpAmount;

                if (!costTracking.throttledPumpEvents) costTracking.throttledPumpEvents = 0;
                costTracking.throttledPumpEvents++;
              }
            }

            // Schedule next check in 30 minutes
            tug.nextThrottlePumpTime = simTime + 0.5;
          } else {
            // Storage still full, no other HDDs need water - stay in standby
            tug.nextThrottlePumpTime = simTime + 0.5;
          }
        }
      }

      if (tug.status === 'en-route-empty' && simTime >= tug.returnHour) {
        // Use intended return source if set (from storage barge pickup), otherwise calculate
        const returnSourceId = tug.intendedReturnSource || selectReturnSource(tug.id, tug.targetCrossing);
        const sourceState = state.sources[returnSourceId];

        tug.status = 'idle';
        tug.currentLocation = returnSourceId;
        tug.targetRig = null;
        tug.targetCrossing = null;

        // CRITICAL FIX (Jan 22, 2026): Always drop ALL barges to pool when tug returns
        // This prevents the state mismatch bug where barges have assignedTugId set but
        // tug has empty attachedBargeIds, making them unavailable for dispatch.
        // Previous "keep attached" logic caused issues when:
        // 1. Barges were kept attached but had fillLevel=0 after delivery
        // 2. findAvailableTug() filtered out attached barges with no water
        // 3. Pool had no barges (all had assignedTugId set)
        // 4. Result: "No available tug" despite idle tug with barges at source

        // Clear demob flag and intended return source if set
        if (tug.returningFromDemob === true) {
          tug.returningFromDemob = false;
        }
        if (tug.intendedReturnSource) {
          tug.intendedReturnSource = null;
        }

        // Drop ALL attached barges back to pool
        const bargesToDrop = kernel.getAttachedBarges(tug.id);
        if (bargesToDrop.length > 0) {
          // Log storage barge returns (UNIFIED MODEL: role-based check only)
          const storageBarges = bargesToDrop.filter(bid => {
            const b = state.barges.find(x => x.id === bid);
            return b && (b.role === 'storage-available' || b.role === 'storage');
          });
          if (storageBarges.length > 0) {
            console.log(`[STORAGE BARGE RETURN] ${tug.id} dropping ${storageBarges.length} storage barges (${storageBarges.join(', ')}) at ${returnSourceId}, simTime=${simTime.toFixed(0)}h`);
          }
          returnBargesToSource(bargesToDrop, returnSourceId, simTime, 'tugReturnDropAll');
        }

        // Clear tug's attached barges (legacy sync - kernel handles via returnBargesToSource)
        tug.attachedBargeIds = [];

        // Track journey timeline for cost calculation
        tug.journeyQueueStart = simTime; // Tug returned to source, starts waiting for fill
        tug.journeyFillStart = null;  // No barges attached, so no fill time yet

        // DEBUG: Log when tug returns to idle
        console.log(`[TUG RETURN] ${tug.id} now idle at ${returnSourceId}, simTime=${simTime.toFixed(1)}h, dropped ${bargesToDrop.length} barges`);

        logAssetAction(simTime, 'Tug', tug.id, 'idle', `at ${returnSourceId}`, {
          attachedBargeIds: [],
          bargesDropped: bargesToDrop.length,
          bargesKept: 0
        });

        // PRIORITY CHECK: Storage delivery takes precedence over transport fill
        // Storage must arrive BEFORE drilling starts - transport can wait
        let assignedToStorage = false;
        const pendingStorageDeliveries = storageDeliverySchedule.filter(d =>
          d.status === 'pending' && simTime >= d.dispatchTime
        );

        if (pendingStorageDeliveries.length > 0) {
          // Try to dispatch storage delivery with this tug
          for (const delivery of pendingStorageDeliveries) {
            if (dispatchStorageDelivery(delivery, simTime)) {
              console.log(`[STORAGE PRIORITY] ${tug.id} assigned to storage delivery for ${delivery.hdd} (${pendingStorageDeliveries.length} pending)`);
              assignedToStorage = true;
              break;
            }
          }
        }

        // Only start transport fill if not assigned to storage
        if (!assignedToStorage) {
          tryStartNextFill(returnSourceId, simTime);
        }
      }
    });

    // Dispatch logic - PROACTIVE OVERNIGHT BUFFER STRATEGY
    const rigNeeds = [];

    rigNames.forEach(rigName => {
      const status = rigStatuses[rigName];
      if (!status) return; // Skip if rig status not found

      // CRITICAL FIX (Jan 22, 2026): Allow overnight dispatch even when hourlyRate is 0!
      // Previous logic filtered out rigs with hourlyRate===0 (non-drilling hours),
      // preventing any overnight dispatch to build buffer for the next day.
      // Now we only skip truly inactive rigs (Idle/RigDown/RigUp stages).
      if (!status.isActive || ['Idle', 'RigDown', 'RigUp'].includes(status.stage)) return;

      const currentStorage = getEffectiveStorageLevel(rigName);
      const storageCapacity = getEffectiveStorageCapacity(rigName);

      // Calculate water en route to this rig (not just tug count)
      // Note: 'at-rig' and 'arrived-at-rig' mean the tug is ALREADY THERE delivering - don't count as "en route"
      // Only count tugs that are actively traveling TO the rig
      const tugsEnRoute = state.tugs.filter(t =>
        (t.targetRig === rigName || t.deliveryPlan.some(s => s.rig === rigName)) &&
        ['en-route-loaded', 'traveling-to-next-stop'].includes(t.status)
      );

      const waterEnRoute = tugsEnRoute.reduce((sum, tug) => {
        // Calculate water this tug will deliver to this rig
        if (tug.targetRig === rigName) {
          // Single-stop delivery - all water goes to this rig
          return sum + tug.totalVolumeRemaining;
        } else {
          // Multi-stop delivery - only count water allocated to this rig
          const stop = tug.deliveryPlan.find(s => s.rig === rigName);
          return sum + (stop?.volumeToDeliver || 0);
        }
      }, 0);

      const bestDistance = bestSourceForHDD[status.hdd]?.distance || avgBestDistance;
      const travelTime = bestDistance / ((loadedSpeedSmall + loadedSpeedLarge) / 2);
      const pumpTimeEst = (2 * hookupTime) + (2 * SMALL_BARGE_VOLUME / PUMP_TO_STORAGE_RATE) + switchoutTime;
      const timeToDelivery = travelTime + pumpTimeEst;

      // Calculate target storage based on CURRENT PHASE demand (not peak rate)
      // This prevents over-dispatching during low-demand phases like pilot
      const rigConfig = rigs[rigName];

      // Get the current phase's hourly rate (use status.hourlyRate during drilling hours)
      // At night (hourlyRate=0), use the rate for the current stage
      let currentPhaseRate = status.hourlyRate;
      if (currentPhaseRate === 0 && status.stage) {
        // During non-drilling hours, use the rate for current stage
        // Only drilling stages (pilot, ream, swab, pull) have rates defined
        const stageLower = status.stage.toLowerCase();
        if (['pilot', 'ream', 'swab', 'pull'].includes(stageLower)) {
          currentPhaseRate = rigConfig?.[stageLower] || 0;
        }
      }

      // Daily demand based on current phase (not peak)
      // If phase rate is 0 (RigUp, RigDown, Idle), use pilot rate as minimum baseline
      const effectivePhaseRate = currentPhaseRate > 0 ? currentPhaseRate : (rigConfig?.pilot || 7500);
      const dailyDemand = effectivePhaseRate * (DRILLING_END_HOUR - DRILLING_START_HOUR);

      // OVERNIGHT OPTIMIZATION: Night ops are essentially free (just fuel cost)
      // During overnight hours, target FULL storage capacity to build maximum buffer
      // During drilling hours, target 150% of daily demand
      const isOvernightHours = hourOfDay >= DRILLING_END_HOUR || hourOfDay < DRILLING_START_HOUR;
      const targetStorage = isOvernightHours
        ? storageCapacity  // Fill to capacity overnight - it's free!
        : Math.min(dailyDemand * 1.5, storageCapacity);
      const deficit = Math.max(0, targetStorage - currentStorage);

      // HYBRID DISPATCH STRATEGY:
      // 1. ALWAYS check for critically low levels (reactive)
      // 2. ADDITIONALLY check for proactive buffer building during overnight

      const hoursOfSupply = status.hourlyRate > 0 ? currentStorage / status.hourlyRate : 999;
      let shouldDispatch = false;
      let urgency = 300;

      // ============================================================================
      // PROACTIVE SCHEDULING - STEP 5: Adjust Reactive Dispatch Based on Schedule
      // ============================================================================
      // Check if there's a scheduled delivery coming for this rig
      let scheduledDeliveryComingSoon = false;
      let hasOverdueScheduledDelivery = false;
      let overdueHours = 0;

      if (USE_SCHEDULED_DISPATCH) {
        const pendingDeliveriesForRig = transportDeliverySchedule.filter(d =>
          d.rig === rigName &&
          (d.status === 'pending' || d.status === 'dispatched') &&
          d.priority !== 'abandoned'
        );

        pendingDeliveriesForRig.forEach(del => {
          // Check if delivery is coming soon (within 4 hours)
          if (del.arrivalTime > simTime && del.arrivalTime <= simTime + 4) {
            scheduledDeliveryComingSoon = true;
          }
          // Check if delivery is overdue (past dispatch time but not yet dispatched)
          if (del.status === 'pending' && simTime > del.dispatchTime + 1) {
            hasOverdueScheduledDelivery = true;
            overdueHours = Math.max(overdueHours, simTime - del.dispatchTime);
          }
        });
      }

      // =====================================================================
      // CONTINUOUS OPERATIONS MODE (supply-driven, not demand-driven)
      // Keep tugs working 24/7 - dispatch whenever HDD has capacity
      // This allows a smaller fleet to deliver the same amount of water
      // =====================================================================
      const CONTINUOUS_OPS = true;  // Enable continuous operations

      if (CONTINUOUS_OPS) {
        // SUPPLY-DRIVEN: Dispatch if HDD has ANY capacity to accept water
        // Don't wait for storage to be "low" - just keep it topped off
        const hasCapacity = currentStorage < storageCapacity * 0.95;  // Room for at least 5%
        if (hasCapacity) {
          shouldDispatch = true;
          // FIX: Use HOURS OF SUPPLY for urgency, not fill ratio!
          // A rig with small storage barges drains faster than one with large barges
          // Same fill ratio can mean very different hours of supply
          // hoursOfSupply already calculated above: currentStorage / status.hourlyRate
          if (hoursOfSupply < 6) {
            urgency = 500;  // Less than 6 hours supply - high priority
          } else if (hoursOfSupply < 12) {
            urgency = 400;  // Less than 12 hours - medium priority
          } else if (hoursOfSupply < 24) {
            urgency = 300;  // Less than 24 hours - lower priority
          } else {
            urgency = 200;  // Over 24 hours supply - lowest priority (but still dispatch)
          }
        }
      }

      // REACTIVE DISPATCH (emergency override - always active)
      // Dispatch if running critically low, regardless of continuous ops
      if (hoursOfSupply < timeToDelivery) {
        shouldDispatch = true;
        urgency = 600; // CRITICAL - will run dry before delivery arrives
      } else if (hoursOfSupply < timeToDelivery + 1) {
        shouldDispatch = true;
        urgency = Math.max(urgency, 500); // HIGH - less than 1 hour buffer
      } else if (hoursOfSupply < timeToDelivery + 2) {
        shouldDispatch = true;
        urgency = Math.max(urgency, 400); // MEDIUM - less than 2 hours buffer
      }

      // Adjust urgency based on scheduled deliveries
      if (USE_SCHEDULED_DISPATCH) {
        // If scheduled delivery coming in <4 hours, reduce reactive urgency
        // (unless it's a true emergency)
        if (scheduledDeliveryComingSoon && urgency < 500) {
          urgency = Math.max(200, urgency - 100);
        }

        // If scheduled delivery is overdue, boost urgency
        if (hasOverdueScheduledDelivery) {
          const overdueBoost = Math.min(150, Math.floor(overdueHours * 30));
          urgency = Math.min(600, urgency + overdueBoost);
        }
      }

      // PROACTIVE DISPATCH (24/7, not just overnight)
      // ALWAYS dispatch if below target buffer, regardless of time of day
      // The key insight: We need 150% buffer at 7 AM. If we're below that NOW, start dispatching NOW.
      // Don't wait until we can "deliver before drilling starts" - that logic was too restrictive.

      // Calculate how much water we'll consume between now and 7 AM tomorrow
      const hoursUntilDrillingStarts = hourOfDay < DRILLING_START_HOUR
        ? DRILLING_START_HOUR - hourOfDay  // Before 7 AM today
        : (24 - hourOfDay) + DRILLING_START_HOUR; // After 5 PM, calculate to 7 AM tomorrow

      // During drilling hours, we're consuming water - factor that in
      const hoursOfDrillingRemaining = Math.max(0, DRILLING_END_HOUR - hourOfDay);
      const consumptionUntil7AM = isDrillingHour(simTime)
        ? status.hourlyRate * hoursOfDrillingRemaining
        : 0;

      // Adjusted deficit: what we need at 7 AM accounting for consumption between now and then
      const projectedStorageAt7AM = currentStorage - consumptionUntil7AM + waterEnRoute;
      const projectedDeficitAt7AM = Math.max(0, targetStorage - projectedStorageAt7AM);

      // PROACTIVE dispatch - trigger when below target buffer
      // Use projected deficit at 7 AM as the key trigger
      // IMPROVED: Higher base urgency for proactive to avoid waiting until critical
      if (projectedDeficitAt7AM > 0 && !shouldDispatch) {
        shouldDispatch = true;
        // Scale urgency: min 300 (moderate), max 450 (just below reactive HIGH)
        // This ensures proactive dispatch happens before situations become urgent
        urgency = Math.min(450, 300 + Math.floor(projectedDeficitAt7AM / 500));
      }

      // OVERNIGHT BOOST: Night operations are essentially free (just fuel)
      // Boost urgency during overnight hours to maximize buffer building
      if (isOvernightHours && currentStorage < storageCapacity && shouldDispatch) {
        urgency = Math.max(urgency, 400); // Ensure overnight dispatch happens
      }

      // Calculate water still needed
      // CRITICAL FIX: In emergency situations (hoursOfSupply < timeToDelivery),
      // water en route won't arrive in time, so we MUST dispatch additional water
      const capacityGap = Math.max(0, storageCapacity - currentStorage);
      let waterStillNeeded;

      // CRITICAL: If we're going to run dry before any en-route water arrives,
      // we need to dispatch NOW regardless of what's en route
      const isEmergency = hoursOfSupply < timeToDelivery;

      if (isEmergency && urgency >= 500) {
        // Emergency dispatch - ignore water en route (it won't arrive in time!)
        // Dispatch to fill the capacity gap
        waterStillNeeded = capacityGap;
      } else if (deficit > 0) {
        // Reactive: below target, use deficit minus en route
        waterStillNeeded = Math.max(0, deficit - waterEnRoute);
      } else if (shouldDispatch) {
        // Proactive: above target but should dispatch, use capacity gap
        waterStillNeeded = Math.max(0, capacityGap - waterEnRoute);
      } else {
        waterStillNeeded = 0;
      }

      if (shouldDispatch && waterStillNeeded > 0) {
        rigNeeds.push({
          rig: rigName, status, currentStorage, deficit, urgency,
          hddCrossing: status.hdd,
          hoursOfSupply: status.hourlyRate > 0 ? currentStorage / status.hourlyRate : 999,
          timeToDelivery,
          isUrgent: urgency >= 500,
          targetStorage, // Target storage level (150% of daily demand)
          waterEnRoute, // Water currently being delivered
          waterStillNeeded, // Deficit minus water en route
          dailyDemand   // Daily demand for this rig
        });
      }
    });

    if (rigNeeds.length > 0) {
      rigNeeds.sort((a, b) => b.urgency - a.urgency);

      // Track water dispatched this tick to each rig
      const waterDispatchedThisTick = {};

      // MULTI-DISPATCH LOOP: Keep dispatching until no more tugs available or all deficits covered
      // OPTIMIZATION MODE: Disable while loop to prevent memory crashes during exhaustive search
      const useMultiDispatch = !CONFIG._optimizationMode;
      let dispatchedThisTick = true;
      const maxDispatchesPerTick = useMultiDispatch ? 20 : 1;  // Disable during optimization
      let dispatchCount = 0;

      while (dispatchedThisTick && dispatchCount < maxDispatchesPerTick) {
        dispatchedThisTick = false;

        // Re-sort by urgency each iteration (urgency may change as water is dispatched)
        const activeNeeds = rigNeeds.filter(need => {
          const waterAlreadyDispatched = waterDispatchedThisTick[need.rig] || 0;
          return need.waterStillNeeded - waterAlreadyDispatched > 0;
        }).sort((a, b) => b.urgency - a.urgency);

        if (activeNeeds.length === 0) break;

        // Try to dispatch to the highest-priority rig that still needs water
        for (const need of activeNeeds) {
          // Recalculate water still needed accounting for dispatches this tick
          const waterAlreadyDispatched = waterDispatchedThisTick[need.rig] || 0;
          const actualWaterStillNeeded = Math.max(0, need.waterStillNeeded - waterAlreadyDispatched);

          // Stop dispatching to this rig if deficit is covered
          if (actualWaterStillNeeded <= 0) continue;

          // Try ALL sources, not just the "best" one
          let available = null;
          let selectedSource = null;
          const sourceIds = Object.keys(sourceConfig);

          // First try the cost-optimal source
          // Enable debug logging for first 5 days to see source selection reasoning
          const isEarlyDays = simTime < 5 * 24;
          const bestSource = selectBestSourceForHDD(need.hddCrossing, simTime, actualWaterStillNeeded, isEarlyDays);
          if (bestSource) {
            available = findAvailableTug(bestSource, 'large', false, simTime);
            if (!available) available = findAvailableTug(bestSource, 'small', false, simTime);
            if (available) selectedSource = bestSource;
          }

          // If no tug at best source, try other sources
          if (!available) {
            for (const srcId of sourceIds) {
              if (srcId === bestSource) continue;
              available = findAvailableTug(srcId, 'large', false, simTime);
              if (!available) available = findAvailableTug(srcId, 'small', false, simTime);
              if (available) {
                selectedSource = srcId;
                break;
              }
            }
          }

          // No dynamic mobilization - use iterative optimization instead
          // If no tug available, skip this need and let ran dry tracking handle it
          if (!available || !selectedSource) {
            continue;
          }

        const { tug, availableBargeIds } = available;

        // Determine barge type and capacity from available barges
        const firstAvailableBarge = state.barges.find(b => availableBargeIds.includes(b.id));
        const isLargeBarge = firstAvailableBarge && firstAvailableBarge.type === 'large';
        const bargeVolume = isLargeBarge ? LARGE_BARGE_VOLUME : SMALL_BARGE_VOLUME;
        const maxBargesPerTug = isLargeBarge ? tug.largeBargesPerTug : tug.smallBargesPerTug;

        // Calculate how many barges needed for this trip
        // Account for water already dispatched this tick when planning
        const remainingNeeds = rigNeeds.map(n => {
          const dispatched = waterDispatchedThisTick[n.rig] || 0;
          const actualDeficit = Math.max(0, n.waterStillNeeded - dispatched);
          return { ...n, deficit: actualDeficit };
        }).filter(n => n.deficit > 0);

        const totalWaterNeeded = remainingNeeds.reduce((sum, n) => sum + n.deficit, 0);
        const bargesNeeded = Math.ceil(totalWaterNeeded / bargeVolume);

        // Account for barges already attached to tug
        const currentlyAttached = kernel.getAttachedBargeCount(tug.id);
        const spaceAvailable = Math.max(0, maxBargesPerTug - currentlyAttached);
        const bargesToAttach = Math.min(bargesNeeded - currentlyAttached, spaceAvailable, availableBargeIds.length);

        // Assign additional barges from pool to tug (if needed)
        if (bargesToAttach > 0) {
          const assignedBarges = availableBargeIds.slice(0, bargesToAttach);
          assignBargesToTug(tug, assignedBarges, selectedSource, simTime, 'dispatchWaterDelivery');
        }

        // Calculate water from attached barges (sum actual fill levels)
        const waterAvailable = kernel.getAttachedBarges(tug.id).reduce((sum, bid) => {
          const b = state.barges.find(x => x.id === bid);
          return sum + (b ? b.fillLevel : 0);
        }, 0);
        const deliveryPlan = planDeliveryRoute(remainingNeeds, selectedSource, waterAvailable);

        if (deliveryPlan.length > 0) {
          const firstStop = deliveryPlan[0];
          const actualDistance = getDistance(selectedSource, firstStop.crossing);

          tug.status = 'en-route-loaded';
          tug.targetRig = firstStop.rig;
          tug.targetCrossing = firstStop.crossing;
          tug.currentLocation = null;  // In transit

          // Journey cost tracking
          const dispatchTime = simTime;
          const transitTime = getTravelTime(actualDistance, true, tugHasLargeBarges(tug));
          const arrivalTime = dispatchTime + transitTime;

          // Calculate journey costs
          // Defensive: Ensure times are valid and non-negative
          const queueTime = (tug.journeyQueueStart && tug.journeyFillStart && tug.journeyFillStart >= tug.journeyQueueStart)
            ? Math.max(0, tug.journeyFillStart - tug.journeyQueueStart)
            : 0;
          const fillTime = (tug.journeyFillStart && tug.journeyFillStart <= dispatchTime)
            ? Math.max(0, dispatchTime - tug.journeyFillStart)
            : 0;

          const tugDayRate = COST.tug.dayRate;
          const bargeDayRate = tugHasLargeBarges(tug) ? COST.barge.largeDayRate : COST.barge.smallDayRate;
          const attachedForCost = kernel.getAttachedBarges(tug.id);
          const bargeCount = attachedForCost.length;

          const queueCost = (tugDayRate + bargeDayRate * bargeCount) * (queueTime / 24);
          const fillCost = (tugDayRate + bargeDayRate * bargeCount) * (fillTime / 24);
          const fuelCost = actualDistance * (tugHasLargeBarges(tug) ? COST.tug.fuelRunningGPH * (1/loadedSpeedLarge) : COST.tug.fuelRunningGPH * (1/loadedSpeedSmall)) * COST.fuel.pricePerGal;
          const transitCost = (tugDayRate + bargeDayRate * bargeCount) * (transitTime / 24) + fuelCost;

          const sourceAcqRate = COST.waterAcquisition[selectedSource] || 0;
          const acquisitionCost = waterAvailable * sourceAcqRate;

          const totalJourneyCost = queueCost + fillCost + transitCost + acquisitionCost;
          const costPerGallon = waterAvailable > 0 ? totalJourneyCost / waterAvailable : 0;

          logAssetAction(simTime, 'Tug', tug.id, 'en-route-loaded', `to ${firstStop.crossing}`, {
            waterVolume: Math.round(waterAvailable),
            targetRig: firstStop.rig,
            targetCrossing: firstStop.crossing,
            sourceId: selectedSource,
            bargesAttached: attachedForCost.length,
            attachedBargeIds: attachedForCost
          });

          // Log journey with cost breakdown
          logJourneyEvent(simTime, tug.id, 'DISPATCH', {
            sourceId: selectedSource,
            targetRig: firstStop.rig,
            targetCrossing: firstStop.crossing,
            distance: actualDistance,
            waterVolume: waterAvailable,
            attachedBargeIds: attachedForCost,
            fromCoords: getLocationCoordinates(selectedSource),
            toCoords: getLocationCoordinates(firstStop.crossing),
            costs: {
              queueTimeHours: queueTime,
              fillTimeHours: fillTime,
              transitTimeHours: transitTime,
              queueCost: Math.round(queueCost),
              fillCost: Math.round(fillCost),
              transitCost: Math.round(transitCost),
              acquisitionCost: Math.round(acquisitionCost),
              totalCost: Math.round(totalJourneyCost),
              costPerGallon: costPerGallon.toFixed(4)
            }
          });

          // Track cost-per-gallon history for this source-destination pair (skip during optimization)
          if (!CONFIG._optimizationMode) {
            const pairKey = `${selectedSource}-${firstStop.crossing}`;
            if (!costPerGallonHistory[pairKey]) costPerGallonHistory[pairKey] = [];
            costPerGallonHistory[pairKey].push({
              simTime,
              date: getDateStrFromTime(simTime),
              costPerGallon,
              queueTime,
              fillTime,
              transitTime,
              waterVolume: waterAvailable
            });
          }

          // Reset journey tracking for next trip
          tug.journeyQueueStart = null;
          tug.journeyFillStart = null;

          tug.dispatchDistance = actualDistance;
          tug.arrivalHour = arrivalTime;
          tug.sourceId = selectedSource;
          tug.deliveryPlan = deliveryPlan;
          tug.currentStopIndex = 0;
          tug.totalVolumeRemaining = waterAvailable;

          // Track water dispatched to each rig in delivery plan
          for (const stop of deliveryPlan) {
            if (!waterDispatchedThisTick[stop.rig]) waterDispatchedThisTick[stop.rig] = 0;
            waterDispatchedThisTick[stop.rig] += stop.volumeToDeliver;
          }

          // Update status of attached barges
          kernel.getAttachedBarges(tug.id).forEach(bid => {
            const barge = state.barges.find(b => b.id === bid);
            if (barge) {
              barge.status = 'en-route-loaded';
              barge.targetRig = firstStop.rig;
              barge.arrivalHour = tug.arrivalHour;
              const bargeSize = barge.volume >= LARGE_BARGE_VOLUME ? 'large' : 'small';
              logAssetAction(simTime, `Barge (${bargeSize})`, barge.id, 'en-route-loaded', `to ${firstStop.crossing}`, {
                fillLevel: Math.round(barge.fillLevel),
                capacity: barge.volume,
                fillPercent: ((barge.fillLevel / barge.volume) * 100).toFixed(1),
                usage: 'transport',
                assignedTugId: barge.assignedTugId
              });
            }
          });

          // CRITICAL: Mark that we successfully dispatched this tick
          dispatchedThisTick = true;
          dispatchCount++;
          break;  // Exit the for loop to re-evaluate priorities in the while loop
        }
      }
    }  // End of while loop
    }  // End of if (rigNeeds.length > 0)

    // Track active AND on-site tugs/barges - keep max for the day
    // Tugs active = tugs with barges being filled, in transit, pumping, or queued
    const activeTugsList = state.tugs.filter(t => {
      // Tug is active if it's doing any work (not just sitting idle)
      if (['en-route-loaded', 'en-route-empty', 'at-rig', 'arrived-at-rig', 'traveling-to-next-stop'].includes(t.status)) {
        t.wasEverUsed = true; // Mark this tug as having been used
        return true;
      }
      // Also count as active if it has attached barges being filled or queued
      const hasWorkingBarges = kernel.getAttachedBarges(t.id).some(bid => {
        const barge = state.barges.find(b => b.id === bid);
        return barge && ['filling', 'queued'].includes(barge.status);
      });
      if (hasWorkingBarges) t.wasEverUsed = true;
      return hasWorkingBarges;
    });
    const activeTugs = activeTugsList.length;

    // Track the IDs if this is the peak for the day
    if (activeTugs > (dayResult.tugsActive || 0)) {
      dayResult.tugsActive = activeTugs;
      dayResult.activeTugIds = activeTugsList.map(t => t.id);
    }

    // Count active small barges = barges ACTUALLY WORKING (en-route or at-rig)
    // EXCLUDE filling/queued/idle at sources - those are on-site but not active
    // Don't split by role since barges can switch between transport/storage dynamically
    const activeSmallBargesList = state.barges.filter(b =>
      b.volume === SMALL_BARGE_VOLUME &&
      ['en-route-loaded', 'at-rig', 'en-route-empty', 'stationed', 'en-route-storage'].includes(b.status)
    );
    const activeSmallBarges = activeSmallBargesList.length;

    if (activeSmallBarges > (dayResult.smallBargesActive || 0)) {
      dayResult.smallBargesActive = activeSmallBarges;
      dayResult.activeSmallBargeIds = activeSmallBargesList.map(b => b.id);
    }

    // Count active large barges = barges ACTUALLY WORKING (en-route or at-rig)
    // EXCLUDE filling/queued/idle at sources
    // Don't split by role since barges can switch between transport/storage dynamically
    const activeLargeBargesList = state.barges.filter(b =>
      b.type === 'large' &&
      ['en-route-loaded', 'at-rig', 'en-route-empty', 'stationed', 'en-route-storage'].includes(b.status)
    );
    const activeLargeBarges = activeLargeBargesList.length;

    if (activeLargeBarges > (dayResult.largeBargesActive || 0)) {
      dayResult.largeBargesActive = activeLargeBarges;
      dayResult.activeLargeBargeIds = activeLargeBargesList.map(b => b.id);
    }
  }

  // Close any remaining ran dry periods at end of simulation
  Object.entries(activeRanDryPeriods).forEach(([rigName, period]) => {
    const duration = totalHours - period.startTime;
    ranDryEvents.push({
      rig: rigName,
      startTime: period.startTime,
      endTime: totalHours,
      startTimestamp: period.startTimestamp,
      endTimestamp: `${getDateStrFromTime(totalHours)} ${getHourOfDayFromTime(totalHours).toFixed(2)}:00`,
      durationHours: duration.toFixed(2),
      phase: period.phase,
      hdd: period.hdd
    });
  });

  // NOTE: Water flow tracking (demand, usage, injected, deficit) is already complete!
  // - Consumption tracked at lines 1989-2012 during simulation loop
  // - Injection tracked at lines 1747-1748 when tugs deliver water
  // - All data correctly populates dailyResults during simulation
  // - NO aggregation override needed - the tracking already works!

  // Calculate results
  let totalDemand = 0, totalUsage = 0, totalDeficit = 0, daysWithDeficit = 0;
  const deficitDays = [];

  Object.entries(dailyResults).forEach(([dateStr, result]) => {
    totalDemand += result.demand;
    totalUsage += result.usage;
    totalDeficit += result.deficit;
    if (result.deficit > 0) {
      daysWithDeficit++;
      deficitDays.push({
        date: dateStr, demand: Math.round(result.demand),
        usage: Math.round(result.usage), deficit: Math.round(result.deficit),
        rigs: Object.entries(result.ranDry).filter(([_, v]) => v).map(([k]) => k)
      });
    }
  });

  // Build mobilization schedule from asset log (single source of truth)
  const mobSchedule = buildMobilizationScheduleFromAssetLog(assetLog, fleetConfig, minDate, COST.mobilization);

  // Populate on-site counts from mobilization schedule
  Object.values(dailyResults).forEach(dayResult => {
    const currentDateStr = dayResult.date;

    // Count tugs on-site (simple: just check if date is within mob/demob range)
    dayResult.tugsOnSite = (mobSchedule.tugs || []).filter(tug => {
      return currentDateStr >= tug.mobDate && currentDateStr <= tug.demobDate;
    }).length;

    // Count small barges on-site (both transport and storage)
    dayResult.smallBargesOnSite = (mobSchedule.barges || []).filter(b => {
      return b.bargeSize === 'small' && currentDateStr >= b.mobDate && currentDateStr <= b.demobDate;
    }).length;

    // Count large barges on-site (both transport and storage)
    dayResult.largeBargesOnSite = (mobSchedule.barges || []).filter(b => {
      return b.bargeSize === 'large' && currentDateStr >= b.mobDate && currentDateStr <= b.demobDate;
    }).length;

    // REMOVED CONSTRAINT ENFORCEMENT - with dynamic mobilization, active counts are accurate
    // Assets can't be active before mobilization since they're mobilized just-in-time
    // The asset log tracking (lines 1783-1789) accurately captures peak active counts
    // The on-site count from mobilization schedule may not align with intraday activity
    // (e.g., barge used on 6/18 but mobDate=6/18 might not count it as "on-site" that day)
  });

  const totalDowntime = costTracking.downtimeHours.Blue + costTracking.downtimeHours.Green + costTracking.downtimeHours.Red;

  // Calculate fleet totals for summary
  const smallTransportBarges = numSmallTugs * SMALL_BARGES_PER_TUG;
  const totalSmallBarges = smallTransportBarges + numSmallStorageBarges;
  const totalLargeBarges = numLargeTransport + numLargeStorage;

  // Calculate total costs
  const costs = {
    tugRental: mobSchedule.summary.totalTugDays * COST.tug.dayRate,
    smallBargeRental: (mobSchedule.summary.totalSmallTransportBargeDays + mobSchedule.summary.totalSmallStorageBargeDays) * COST.barge.smallDayRate,
    largeBargeRental: (mobSchedule.summary.totalLargeTransportBargeDays + mobSchedule.summary.totalLargeStorageBargeDays) * COST.barge.largeDayRate,
    fuelTotal: (costTracking.fuelIdleGallons + costTracking.fuelRunningGallons) * COST.fuel.pricePerGal,
    downtimeTotal: totalDowntime * COST.downtime.hourlyRate,
    mobilizationCost: mobSchedule.mobDemobCosts.total,
    waterAcquisition: {}
  };
  // Calculate water acquisition costs dynamically for all configured sources
  let waterTotal = 0;
  sourceIds.forEach(srcId => {
    const volume = costTracking.waterVolumeBySource[srcId] || 0;
    const costPerGal = COST.waterAcquisition?.[srcId] || 0.02; // Default $0.02/gal
    costs.waterAcquisition[srcId] = volume * costPerGal;
    waterTotal += costs.waterAcquisition[srcId];
  });
  costs.waterTotal = waterTotal;
  costs.equipmentTotal = costs.tugRental + costs.smallBargeRental + costs.largeBargeRental;
  costs.grandTotal = costs.equipmentTotal + costs.fuelTotal + costs.downtimeTotal + costs.mobilizationCost + costs.waterTotal;
  costs.mobDemobBreakdown = mobSchedule.mobDemobCosts;

  // Count ran dry events dynamically for all configured rigs
  let ranDryCount = 0;
  Object.values(dailyResults).forEach(day => {
    rigNames.forEach(rigName => {
      if (day.ranDry?.[rigName]) ranDryCount++;
    });
  });


  // Build HDD-specific hourly storage timeline (skip during optimization to save memory)
  const hddStorageTimeline = [];
  const buildHDDTimeline = () => {
    if (CONFIG._optimizationMode) return; // Skip timeline building during optimization

    hddSchedule.forEach(hdd => {
      const recordedData = hourlyStorageByHDD[hdd.crossing] || [];
      const injectionEvents = injectionEventsByHDD[hdd.crossing] || [];
      const storageBargeChanges = storageBargeChangesByHDD[hdd.crossing] || [];

      // Build a lookup map from recorded data
      const dataByTimestamp = {};
      recordedData.forEach(d => {
        const key = `${d.date}-${Math.floor(parseFloat(d.hour))}`;
        dataByTimestamp[key] = d;
      });

      // Build lookup for injection events by hour
      const injectionsByHour = {};
      injectionEvents.forEach(evt => {
        if (!injectionsByHour[evt.hourKey]) {
          injectionsByHour[evt.hourKey] = [];
        }
        injectionsByHour[evt.hourKey].push(evt);
      });

      // Build lookup for storage barge changes by hour
      const bargeChangesByHour = {};
      storageBargeChanges.forEach(chg => {
        bargeChangesByHour[chg.hourKey] = chg;
      });

      // Generate complete timeline from start to rigDown - ALL 24 HOURS PER DAY
      const startDate = new Date(hdd.start);
      const rigDownDate = new Date(hdd.rigDown);
      // Note: The while loop uses <= so rigDown day is included without adding +1

      const chartData = [];
      let idx = 0;

      // Helper to determine stage for a given date
      const getStageForDate = (dateStr) => {
        if (dateStr < hdd.start) return 'Pre-Start';
        if (dateStr <= hdd.rigUp) return 'RigUp';
        if (dateStr <= hdd.pilot) return 'Pilot';
        if (dateStr <= hdd.ream) return 'Ream';
        if (dateStr <= hdd.swab) return 'Swab';
        if (dateStr <= hdd.pull) return 'Pull';
        if (dateStr <= hdd.rigDown) return 'RigDown';
        return 'Complete';
      };

      // Track last known storage capacity and barges for capacity line labeling
      let lastCapacity = 0;
      let lastBargeIds = [];

      // Iterate through each day from start to rigDown
      const currentDate = new Date(startDate);
      while (currentDate <= rigDownDate) {
        const dateStr = currentDate.toISOString().split('T')[0];

        // Generate entries for ALL 24 hours
        for (let hourOfDay = 0; hourOfDay < 24; hourOfDay++) {
          const key = `${dateStr}-${hourOfDay}`;
          const recorded = dataByTimestamp[key];
          const stage = getStageForDate(dateStr);
          const isWorkingHour = hourOfDay >= DRILLING_START_HOUR && hourOfDay < DRILLING_END_HOUR;

          // Calculate simTime for this hour
          const simTime = ((currentDate - minDate) / (1000 * 60 * 60)) + hourOfDay;
          const hourKey = Math.floor(simTime);

          // Get injection and withdrawal for this hour
          const injectionThisHour = hourlyInjectionByHDD[hdd.crossing]?.[hourKey] || 0;
          const withdrawalThisHour = hourlyWithdrawalByHDD[hdd.crossing]?.[hourKey] || 0;

          // Get injection events for barge labels
          const injectionEventsThisHour = injectionsByHour[hourKey] || [];
          const injectionBargeLabels = injectionEventsThisHour.map(evt => evt.bargeIds.join(',')).filter(Boolean);

          // Check for storage barge capacity change
          const bargeChange = bargeChangesByHour[hourKey];
          let storageCapacity = recorded?.storageCapacity || lastCapacity;
          let storageBargeIds = lastBargeIds;
          let capacityChanged = false;
          let bargeMovedTo = null;
          let bargeArrivedFrom = null;
          let previousBargeIds = null;
          let transferredStorageLevel = null;

          if (bargeChange) {
            storageCapacity = bargeChange.totalCapacity;
            storageBargeIds = bargeChange.bargeIds;
            bargeMovedTo = bargeChange.destination || null;
            bargeArrivedFrom = bargeChange.source || null; // Track where barges came from
            previousBargeIds = bargeChange.previousBargeIds || null;
            transferredStorageLevel = bargeChange.transferredStorageLevel || null;

            if (storageCapacity !== lastCapacity || JSON.stringify(storageBargeIds) !== JSON.stringify(lastBargeIds)) {
              capacityChanged = true;
              lastCapacity = storageCapacity;
              lastBargeIds = [...storageBargeIds];
            }
          } else if (recorded) {
            storageCapacity = recorded.storageCapacity;
          }

          // Check if this day is a Sunday (for non-working day visualization)
          const isSunday = currentDate.getDay() === 0;

          if (recorded) {
            // Use transferred storage level if this is a transfer-in at the start
            let actualStorageLevel = Math.round(recorded.storageLevel);
            if (bargeArrivedFrom && transferredStorageLevel !== null) {
              // This is the first hour after transfer - use transferred level
              actualStorageLevel = Math.round(transferredStorageLevel);
            }

            chartData.push({
              index: idx,
              timestamp: simTime,
              label: `${dateStr.slice(5)} ${String(hourOfDay).padStart(2, '0')}h`,
              date: dateStr,
              hour: hourOfDay.toString(),
              storageLevel: actualStorageLevel,
              storageCapacity: Math.round(storageCapacity),
              injectionGallons: Math.round(injectionThisHour),
              withdrawalGallons: Math.round(withdrawalThisHour),
              injectionBargeLabels: injectionBargeLabels,
              storageBargeIds: storageBargeIds,
              capacityChanged: capacityChanged,
              bargeMovedTo: bargeMovedTo,
              bargeArrivedFrom: bargeArrivedFrom,
              previousBargeIds: previousBargeIds,
              transferNote: bargeArrivedFrom ? `Storage: ${storageBargeIds.map(id => id.replace('SmallBarge-', 'SB').replace('LargeBarge-', 'LB')).join(',')} from ${bargeArrivedFrom}` :
                           bargeMovedTo ? `Storage: ${previousBargeIds.map(id => id.replace('SmallBarge-', 'SB').replace('LargeBarge-', 'LB')).join(',')} moved to ${bargeMovedTo}` : null,
              stage: stage,
              isWorkingHour: isWorkingHour,
              isSunday: isSunday  // For Sunday hatching on chart
            });
          } else {
            // No data recorded for this hour - show as 0 storage (pre-mobilization or post-demob)
            // UNLESS this is a transfer-in, then use transferred storage level
            let storageLevel = 0;
            if (bargeArrivedFrom && transferredStorageLevel !== null) {
              storageLevel = Math.round(transferredStorageLevel);
            }

            chartData.push({
              index: idx,
              timestamp: simTime,
              label: `${dateStr.slice(5)} ${String(hourOfDay).padStart(2, '0')}h`,
              date: dateStr,
              hour: hourOfDay.toString(),
              storageLevel: storageLevel,
              storageCapacity: storageCapacity,
              injectionGallons: 0,
              withdrawalGallons: 0,
              injectionBargeLabels: [],
              storageBargeIds: storageBargeIds,
              capacityChanged: capacityChanged,
              bargeMovedTo: bargeMovedTo,
              bargeArrivedFrom: bargeArrivedFrom,
              previousBargeIds: previousBargeIds,
              transferNote: bargeArrivedFrom ? `Storage: ${storageBargeIds.map(id => id.replace('SmallBarge-', 'SB').replace('LargeBarge-', 'LB')).join(',')} from ${bargeArrivedFrom}` :
                           bargeMovedTo ? `Storage: ${previousBargeIds.map(id => id.replace('SmallBarge-', 'SB').replace('LargeBarge-', 'LB')).join(',')} moved to ${bargeMovedTo}` : null,
              stage: stage,
              isWorkingHour: isWorkingHour,
              isSunday: isSunday  // For Sunday hatching on chart
            });
          }
          idx++;
        }

        currentDate.setDate(currentDate.getDate() + 1);
      }

      if (chartData.length === 0) return;

      // Build stage regions for background shading (based on hourly index ranges)
      const stageRegions = [];
      let currentStage = null;
      let regionStart = 0;

      chartData.forEach((d, i) => {
        if (d.stage !== currentStage) {
          if (currentStage !== null) {
            stageRegions.push({
              stage: currentStage,
              startIdx: regionStart,
              endIdx: i - 1
            });
          }
          currentStage = d.stage;
          regionStart = i;
        }
      });
      // Close final region
      if (currentStage !== null) {
        stageRegions.push({
          stage: currentStage,
          startIdx: regionStart,
          endIdx: chartData.length - 1
        });
      }

      // Phase markers (for reference lines)
      const phases = [];
      const phaseMarkers = [
        { date: hdd.rigUp, name: 'Rig Up' },
        { date: hdd.pilot, name: 'Pilot' },
        { date: hdd.ream, name: 'Ream' },
        { date: hdd.swab, name: 'Swab' },
        { date: hdd.pull, name: 'Pull' },
        { date: hdd.rigDown, name: 'Rig Down' }
      ];

      phaseMarkers.forEach(phase => {
        const entryIdx = chartData.findIndex(d => d.date >= phase.date);
        if (entryIdx >= 0) {
          phases.push({
            index: entryIdx,
            label: chartData[entryIdx].label,
            name: phase.name
          });
        }
      });

      // Build injection events array for chart annotations
      const injectionAnnotations = injectionEvents.map(evt => ({
        timestamp: evt.timestamp,
        volume: evt.volume,
        bargeLabel: evt.bargeIds.join(', '),
        tugId: evt.tugId
      }));

      hddStorageTimeline.push({
        crossing: hdd.crossing,
        rig: hdd.rig,
        start: hdd.start,
        rigDown: hdd.rigDown,
        worksSundays: hdd.worksSundays,  // Include for Sunday hatching
        hourlyData: chartData,
        phases,
        stageRegions,
        injectionAnnotations  // For labeling injection bars
      });
    });
  };

  buildHDDTimeline();

  // Restore console.log before returning (memory optimization cleanup)
  return restoreAndReturn({
    config: { numSmallTugs, numSmallTransportBarges, numSmallStorageBarges, numLargeTransport, numLargeStorage },
    fleetSummary: {
      tugs: numSmallTugs + numLargeTransport,
      smallTugs: numSmallTugs,
      largeTugs: numLargeTransport,
      smallTransportBarges,
      smallStorageBarges: numSmallStorageBarges,
      totalSmallBarges,
      largeTransportBarges: numLargeTransport,
      largeStorageBarges: numLargeStorage,
      totalLargeBarges,
      storagePerRig: `${Math.floor(numSmallStorageBarges/3) * 80}K small + ${numLargeStorage > 0 ? 'dynamic large' : 'none'}`
    },
    mobilizationSchedule: mobSchedule,
    success: daysWithDeficit === 0,
    // UNIFIED MODEL: Add score for optimizer (lower is better)
    // Score = equipment cost + downtime cost + ran dry penalty
    score: costs.grandTotal + (ranDryCount * 50000), // $50k penalty per ran dry event
    daysWithDeficit,
    ranDryCount,
    totalDemand: Math.round(totalDemand),
    totalUsage: Math.round(totalUsage),
    totalDeficit: Math.round(totalDeficit),
    demandMetPercent: ((totalUsage / totalDemand) * 100).toFixed(1),
    totalTrips: costTracking.totalTrips,
    totalStops: costTracking.totalStops,
    multiStopTrips: costTracking.multiStopTrips,
    directHDDTrips: costTracking.directHDDTrips,
    directHDDWaterDelivered: costTracking.directHDDWaterDelivered,
    directHDDTimeSaved: costTracking.directHDDTimeSaved,
    downtimeHours: totalDowntime,
    costs,
    deficitDays,
    dailyResults: Object.values(dailyResults),
    assetLog,
    hddStorageTimeline,
    costPerGallonHistory,
    ranDryEvents,
    totalDays,
    // Dynamic mobilization analysis
    dynamicMobilizations,
    dynamicMobSummary: {
      tugsAdded: dynamicMobilizations.filter(m => m.type === 'tug').length,
      bargesAdded: dynamicMobilizations.filter(m => m.type === 'barge').length,
      firstMobTime: dynamicMobilizations.length > 0 ? Math.min(...dynamicMobilizations.map(m => m.time)) : null,
      mobByRig: dynamicMobilizations.reduce((acc, m) => {
        acc[m.rig] = (acc[m.rig] || 0) + 1;
        return acc;
      }, {}),
      // Recommended fleet = base config + dynamic adds
      recommendedFleet: {
        numSmallTugs: numSmallTugs + dynamicMobilizations.filter(m => m.type === 'tug').length,
        numSmallTransportBarges,
        numSmallStorageBarges,
        numLargeTransport,
        numLargeStorage,
        note: dynamicMobilizations.length > 0
          ? `Added ${dynamicMobilizations.filter(m => m.type === 'tug').length} tugs dynamically to prevent ran dry`
          : 'Base fleet was sufficient'
      }
    },
    peakTugs: Math.max(...Object.values(dailyResults).map(d => d.tugsActive)),
    peakSmallBarges: Math.max(...Object.values(dailyResults).map(d => d.smallBargesActive)),
    peakLargeBarges: Math.max(...Object.values(dailyResults).map(d => d.largeBargesActive)),
    peakBarges: Math.max(...Object.values(dailyResults).map(d => d.smallBargesActive + d.largeBargesActive)),

    // Total mobilized assets by role (from mobilization schedule)
    // These represent the total count of unique assets mobilized during the project
    peakSmallTransportBarges: mobSchedule.summary.numSmallTransportBarges,
    peakSmallStorageBarges: mobSchedule.summary.numSmallStorageBarges,
    peakLargeTransportBarges: mobSchedule.summary.numLargeTransportBarges,
    peakLargeStorageBarges: mobSchedule.summary.numLargeStorageBarges,

    tugMobes: mobSchedule.tugs.length,
    tugJourneys: buildTugJourneysFromAssetLog(assetLog)  // For map animation
  });
}

// ============================================================================
// LIGHTWEIGHT RESULT HELPER (Memory Optimization)
// ============================================================================
// During optimization, we only need essential fields for comparison.
// Large arrays (assetLog, dailyResults, tugJourneys) are excluded to prevent heap overflow.
function toLightweightResult(result) {
  return {
    config: result.config,
    success: result.success,
    score: result.score,
    ranDryCount: result.ranDryCount,
    daysWithDeficit: result.daysWithDeficit,
    costs: result.costs,
    totalDemand: result.totalDemand,
    totalUsage: result.totalUsage,
    totalDeficit: result.totalDeficit,
    demandMetPercent: result.demandMetPercent,
    downtimeHours: result.downtimeHours,
    totalTrips: result.totalTrips,
    peakTugs: result.peakTugs,
    peakSmallBarges: result.peakSmallBarges,
    peakLargeBarges: result.peakLargeBarges,
    peakBarges: result.peakBarges,
    peakSmallTransportBarges: result.peakSmallTransportBarges,
    peakSmallStorageBarges: result.peakSmallStorageBarges,
    peakLargeTransportBarges: result.peakLargeTransportBarges,
    peakLargeStorageBarges: result.peakLargeStorageBarges,
    // Exclude: assetLog, dailyResults, tugJourneys, hddStorageTimeline,
    //          costPerGallonHistory, ranDryEvents, deficitDays, mobilizationSchedule
    _isLightweight: true
  };
}

// ============================================================================
// OPTIMIZATION FUNCTION
// ============================================================================
function runOptimization(userConfig = {}, progressCallback) {
  const cfg = { ...CONFIG, ...userConfig };
  const TOTAL_LARGE_BARGES = cfg.TOTAL_LARGE_BARGES;
  // UNLIMITED: Full range of options for comprehensive search
  const smallStorageOptions = [0, 3, 6, 9, 12, 15, 18, 21, 24, 30, 36];
  const smallTransportOptions = [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 24];
  const maxSmallTugs = 12; // Increased from 6

  // PRE-SCREENING: Calculate peak demand for mass balance check
  const workingHours = cfg.DRILLING_END_HOUR - cfg.DRILLING_START_HOUR;
  const hddSchedule = cfg.hddSchedule || [];
  const dailyDemand = {};

  hddSchedule.forEach(schedule => {
    let currentDate = new Date(schedule.startDate);
    const phases = [
      { days: schedule.rigUpDays || 0, rate: 0 },
      { days: schedule.pilotDays || 0, rate: cfg.rigs[schedule.rig].pilot },
      { days: schedule.reamDays || 0, rate: cfg.rigs[schedule.rig].ream },
      { days: schedule.swabDays || 0, rate: cfg.rigs[schedule.rig].swab },
      { days: schedule.pullDays || 0, rate: cfg.rigs[schedule.rig].pull },
      { days: schedule.rigDownDays || 0, rate: 0 }
    ];

    phases.forEach(phase => {
      for (let d = 0; d < phase.days; d++) {
        const dateStr = currentDate.toISOString().split('T')[0];
        if (!dailyDemand[dateStr]) dailyDemand[dateStr] = 0;
        dailyDemand[dateStr] += phase.rate * workingHours; // gal/hr × hours = gallons/day
        currentDate.setDate(currentDate.getDate() + 1);
      }
    });
  });

  const peakDailyDemand = Math.max(...Object.values(dailyDemand), 0);

  // Estimate avg cycle time (rough average for pre-screening)
  const avgDistance = 20; // nm
  const avgSpeed = (cfg.loadedSpeedSmall + cfg.loadedSpeedLarge) / 2;
  // Calculate average fill rate dynamically from all configured sources
  const sourceIds = cfg.sourceConfig ? Object.keys(cfg.sourceConfig) : [];
  const avgFillRate = sourceIds.length > 0
    ? sourceIds.reduce((sum, id) => sum + (cfg.sourceConfig[id]?.flowRate || 400), 0) / sourceIds.length
    : 400; // Default 400 GPM if no sources configured
  const avgFillTime = cfg.SMALL_BARGE_VOLUME / (avgFillRate * 60);
  const avgPumpTime = cfg.SMALL_BARGE_VOLUME / (cfg.pumpRate * 60);
  const avgCycleTime = (2 * avgDistance / avgSpeed) + avgFillTime + avgPumpTime;

  const results = [];
  let totalConfigs = 0;
  let testedConfigs = 0;
  let skippedConfigs = 0;

  // Count total configurations
  for (const smallStorage of smallStorageOptions) {
    for (const smallTransport of smallTransportOptions) {
      for (let lgStorage = 0; lgStorage <= TOTAL_LARGE_BARGES; lgStorage++) {
        for (let lgTrans = 0; lgTrans <= TOTAL_LARGE_BARGES - lgStorage; lgTrans++) {
          for (let smallTugs = 0; smallTugs <= maxSmallTugs; smallTugs++) {
            if (smallTugs === 0 && lgTrans === 0) continue;
            if (smallTransport === 0 && lgTrans === 0) continue;
            if (smallStorage === 0 && lgStorage === 0) continue;
            totalConfigs++;
          }
        }
      }
    }
  }

  // Test configurations
  for (const smallStorage of smallStorageOptions) {
    for (const smallTransport of smallTransportOptions) {
      for (let lgStorage = 0; lgStorage <= TOTAL_LARGE_BARGES; lgStorage++) {
        for (let lgTrans = 0; lgTrans <= TOTAL_LARGE_BARGES - lgStorage; lgTrans++) {
          for (let smallTugs = 0; smallTugs <= maxSmallTugs; smallTugs++) {
            if (smallTugs === 0 && lgTrans === 0) continue;
            if (smallTransport === 0 && lgTrans === 0) continue;
            if (smallStorage === 0 && lgStorage === 0) continue;

            const fleetConfig = {
              numSmallTugs: smallTugs,
              numSmallTransportBarges: smallTransport,
              numSmallStorageBarges: smallStorage,
              numLargeTransport: lgTrans,
              numLargeStorage: lgStorage
            };

            // MASS BALANCE PRE-SCREENING: Skip configs that can't meet 100% of peak demand
            // Use ACTUAL barge counts in this config, not max per tug
            const bargesPerSmallTug = Math.min(Math.floor(smallTransport / Math.max(smallTugs, 1)), cfg.SMALL_BARGES_PER_TUG);
            const bargesPerLargeTug = lgTrans > 0 ? Math.min(Math.floor(lgTrans / Math.max(smallTugs, 1)), cfg.LARGE_BARGES_PER_TUG) : 0;

            const smallTugCapacityPer24h = smallTugs * (24 / avgCycleTime) * bargesPerSmallTug * cfg.SMALL_BARGE_VOLUME;
            const largeTugCapacityPer24h = smallTugs * (24 / avgCycleTime) * bargesPerLargeTug * cfg.LARGE_BARGE_VOLUME;
            const totalCapacityPer24h = smallTugCapacityPer24h + largeTugCapacityPer24h;

            if (isFinite(avgCycleTime) && avgCycleTime > 0 && isFinite(peakDailyDemand) && totalCapacityPer24h < peakDailyDemand) {
              skippedConfigs++;
              continue; // Skip this config - mathematically insufficient
            }

            // Set optimization mode flag for faster simulations
            const optimizationUserConfig = { ...userConfig, _optimizationMode: true };
            const result = runSimulation(fleetConfig, optimizationUserConfig);
            // MEMORY FIX: Store only lightweight result to prevent heap overflow
            results.push(toLightweightResult(result));
            testedConfigs++;

            // Report progress more frequently to show it's not frozen
            if (progressCallback && (testedConfigs % 5 === 0 || testedConfigs === 1)) {
              progressCallback({
                type: 'progress',
                tested: testedConfigs,
                total: totalConfigs - skippedConfigs, // Adjusted total
                percent: Math.round((testedConfigs / totalConfigs) * 100),
                currentConfig: `${smallTugs}T, ${smallTransport}ST, ${smallStorage}SS, ${lgTrans}LT, ${lgStorage}LS`,
                success: result.success
              });
            }
          }
        }
      }
    }
  }

  // UNIFIED MODEL: Sort by score (lowest is best)
  // Score includes equipment cost + downtime + ran dry penalty
  // This allows configs with some ran dry events if they're much cheaper
  results.sort((a, b) => a.score - b.score);

  const successfulConfigs = results.filter(r => r.success);
  const top15 = results.slice(0, 15); // Top 15 by score, regardless of success

  return {
    // Don't send allResults - too large to clone for postMessage
    // allResults: results,
    top15,
    optimal: results[0] || null, // Best by score (may have some ran dry)
    bestZeroRanDry: successfulConfigs[0] || null, // Best with zero ran dry
    totalTested: results.length,
    totalGenerated: totalConfigs,
    skippedConfigs,
    successCount: successfulConfigs.length
  };
}

// ============================================================================
// RAN DRY EVENT ANALYSIS (for Smart Fleet Finder)
// ============================================================================
function analyzeRanDryEvents(result, hddSchedule) {
  const events = result.ranDryEvents || [];
  if (events.length === 0) {
    return { bottleneck: 'none', confidence: 1.0, details: {} };
  }

  // Group events by various dimensions
  const byHdd = {};
  const byPhase = {};
  const byRig = {};
  const byHour = {};  // Hour of day (0-23)

  events.forEach(e => {
    // By HDD
    byHdd[e.hdd] = byHdd[e.hdd] || [];
    byHdd[e.hdd].push(e);

    // By phase
    byPhase[e.phase] = byPhase[e.phase] || [];
    byPhase[e.phase].push(e);

    // By rig
    byRig[e.rig] = byRig[e.rig] || [];
    byRig[e.rig].push(e);

    // By hour of day
    const hourOfDay = Math.floor(e.startTime % 24);
    byHour[hourOfDay] = byHour[hourOfDay] || [];
    byHour[hourOfDay].push(e);
  });

  // Calculate total downtime by category
  const totalDowntimeByHdd = {};
  const totalDowntimeByPhase = {};
  Object.entries(byHdd).forEach(([hdd, evts]) => {
    totalDowntimeByHdd[hdd] = evts.reduce((sum, e) => sum + parseFloat(e.durationHours || 0), 0);
  });
  Object.entries(byPhase).forEach(([phase, evts]) => {
    totalDowntimeByPhase[phase] = evts.reduce((sum, e) => sum + parseFloat(e.durationHours || 0), 0);
  });

  // Analyze patterns to diagnose bottleneck
  const diagnosis = {
    bottleneck: 'unknown',
    confidence: 0.5,
    details: {
      totalEvents: events.length,
      byHdd,
      byPhase,
      byRig,
      totalDowntimeByHdd,
      totalDowntimeByPhase
    },
    recommendation: null
  };

  // Check for active rig overlap during ran dry events
  // If ran dry happens when multiple rigs are active, it's likely a capacity issue
  const overlapEvents = events.filter(e => {
    // Check how many rigs were in active drilling phases at this time
    const eventTime = e.startTime;
    let activeCount = 0;
    hddSchedule.forEach(hdd => {
      const pilotTime = hdd._pilotSimTime;
      const rigDownTime = hdd._rigDownSimTime;
      if (pilotTime !== undefined && rigDownTime !== undefined) {
        if (eventTime >= pilotTime && eventTime <= rigDownTime) {
          activeCount++;
        }
      }
    });
    return activeCount >= 2;
  });

  // Check for pilot phase issues (storage arrival timing)
  // Note: Phase names from simulation are capitalized (Pilot, Ream, Swab, Pull)
  const pilotEvents = byPhase['Pilot'] || byPhase['pilot'] || [];
  const pilotDowntime = totalDowntimeByPhase['Pilot'] || totalDowntimeByPhase['pilot'] || 0;

  // Check for ream phase issues (peak demand)
  const reamEvents = byPhase['Ream'] || byPhase['ream'] || [];
  const reamDowntime = totalDowntimeByPhase['Ream'] || totalDowntimeByPhase['ream'] || 0;

  // Determine bottleneck based on patterns
  const totalDowntime = events.reduce((sum, e) => sum + parseFloat(e.durationHours || 0), 0);

  // STORAGE BOTTLENECK indicators:
  // - High proportion of pilot phase issues (storage not there when drilling starts)
  // - Issues concentrated at specific HDDs
  // - Early-day issues (before transport could arrive)
  const pilotProportion = pilotDowntime / (totalDowntime || 1);
  const hddConcentration = Math.max(...Object.values(totalDowntimeByHdd)) / (totalDowntime || 1);

  // TUG/TRANSPORT BOTTLENECK indicators:
  // - High proportion of ream phase issues (high demand period)
  // - Issues during multi-rig overlap periods
  // - Issues spread across multiple HDDs
  const reamProportion = reamDowntime / (totalDowntime || 1);
  const overlapProportion = overlapEvents.length / (events.length || 1);
  const hddsAffected = Object.keys(byHdd).length;

  // Debug: Show what phases we actually have
  const phaseNames = Object.keys(byPhase);
  criticalLog(`[SMART FINDER] Ran dry analysis: ${events.length} events, ${totalDowntime.toFixed(1)}h total downtime`);
  criticalLog(`  Phases found: ${phaseNames.join(', ') || 'none'}`);
  criticalLog(`  Pilot: ${pilotEvents.length} (${(pilotProportion * 100).toFixed(0)}%), Ream: ${reamEvents.length} (${(reamProportion * 100).toFixed(0)}%), HDDs: ${hddsAffected}, Overlap: ${(overlapProportion * 100).toFixed(0)}%`);

  // Decision logic - prioritize storage issues first (they block everything else)
  if (pilotProportion > 0.4) {
    // Significant pilot phase issues = storage not arriving in time
    diagnosis.bottleneck = 'storage';
    diagnosis.confidence = 0.6 + (pilotProportion * 0.3);
    diagnosis.recommendation = 'Add storage barges - issues at drilling start indicate insufficient buffer';
    criticalLog(`[SMART FINDER] Diagnosis: STORAGE bottleneck (pilot=${(pilotProportion*100).toFixed(0)}%)`);
  } else if (reamProportion > 0.3 || overlapProportion > 0.4) {
    // Significant ream phase issues OR issues during multi-rig overlap = capacity issue
    if (overlapProportion > 0.6) {
      // High overlap = need more tugs to handle parallel operations
      diagnosis.bottleneck = 'tug';
      diagnosis.confidence = 0.6 + (overlapProportion * 0.3);
      diagnosis.recommendation = 'Add tugs - demand exceeds transport capacity during overlap periods';
      criticalLog(`[SMART FINDER] Diagnosis: TUG bottleneck (overlap=${(overlapProportion*100).toFixed(0)}%)`);
    } else {
      // Moderate overlap with ream issues = need more transport capacity
      diagnosis.bottleneck = 'transport';
      diagnosis.confidence = 0.6 + (reamProportion * 0.2);
      diagnosis.recommendation = 'Add transport barges - insufficient water volume in transit';
      criticalLog(`[SMART FINDER] Diagnosis: TRANSPORT bottleneck (ream=${(reamProportion*100).toFixed(0)}%)`);
    }
  } else if (hddsAffected >= 5) {
    // Many HDDs affected but no clear pattern = need more tugs for flexibility
    diagnosis.bottleneck = 'tug';
    diagnosis.confidence = 0.5;
    diagnosis.recommendation = 'Add tugs - issues spread across many HDDs';
    criticalLog(`[SMART FINDER] Diagnosis: TUG bottleneck (hdds=${hddsAffected})`);
  } else {
    // Few HDDs with unclear pattern - add storage first (most common root cause)
    diagnosis.bottleneck = 'storage';
    diagnosis.confidence = 0.4;
    diagnosis.recommendation = 'Add storage barges - default for unclear patterns';
    criticalLog(`[SMART FINDER] Diagnosis: Unclear pattern, defaulting to STORAGE`);
  }

  return diagnosis;
}

// ============================================================================
// SMART FLEET FINDER (Targeted Iteration)
// ============================================================================
function findWorkingFleetSmart(userConfig = {}, progressCallback, startingFleet = null) {
  // MEMORY OPTIMIZATION: Suppress verbose logging during iteration
  const previousLoggingState = SUPPRESS_VERBOSE_LOGGING;
  SUPPRESS_VERBOSE_LOGGING = true;

  const cfg = { ...CONFIG, ...userConfig };
  const TOTAL_LARGE_BARGES = cfg.TOTAL_LARGE_BARGES || 6;
  // Reasonable caps - high enough to find solutions, low enough to prevent runaway
  const MAX_SMALL_TUGS = 8;
  const MAX_SMALL_TRANSPORT = 16;
  const MAX_SMALL_STORAGE = 20;
  const MAX_LARGE_TRANSPORT = TOTAL_LARGE_BARGES;
  const MAX_LARGE_STORAGE = TOTAL_LARGE_BARGES;

  // Build HDD schedule with simulation times for overlap detection
  const hddSchedule = (cfg.hddSchedule || []).map(hdd => {
    const projectStart = new Date(cfg.hddSchedule[0]?.start || '2026-05-14');
    const pilotDate = new Date(hdd.pilot);
    const rigDownDate = new Date(hdd.rigDown);
    const pilotSimTime = Math.floor((pilotDate - projectStart) / (1000 * 60 * 60));
    const rigDownSimTime = Math.floor((rigDownDate - projectStart) / (1000 * 60 * 60));
    return { ...hdd, _pilotSimTime: pilotSimTime, _rigDownSimTime: rigDownSimTime };
  });

  // Start from analytical recommendation or provided starting fleet
  const fleetConfig = startingFleet ? { ...startingFleet } : {
    numSmallTugs: 4,
    numSmallTransportBarges: 8,
    numSmallStorageBarges: 12,
    numLargeTransport: 0,
    numLargeStorage: 0
  };

  // Critical logs always print
  criticalLog(`[SMART FINDER] Starting from: T=${fleetConfig.numSmallTugs}, ST=${fleetConfig.numSmallTransportBarges}, SS=${fleetConfig.numSmallStorageBarges}, LT=${fleetConfig.numLargeTransport}, LS=${fleetConfig.numLargeStorage}`);

  let iteration = 0;
  const maxIterations = 50;  // Much lower than the dumb finder
  let lastBottleneck = null;
  let consecutiveSameBottleneck = 0;
  let bestRanDryCount = null;  // Track best ran dry count to detect improvement

  while (iteration < maxIterations) {
    const optimizationUserConfig = { ...userConfig, _optimizationMode: true };
    const result = runSimulation(fleetConfig, optimizationUserConfig);
    iteration++;

    if (progressCallback) {
      progressCallback({
        type: 'progress',
        tested: iteration,
        total: maxIterations,
        percent: Math.min(95, Math.round(iteration / maxIterations * 100)),
        currentConfig: `T=${fleetConfig.numSmallTugs}, ST=${fleetConfig.numSmallTransportBarges}, SS=${fleetConfig.numSmallStorageBarges}, LT=${fleetConfig.numLargeTransport}, LS=${fleetConfig.numLargeStorage}`,
        ranDryCount: result.ranDryCount,
        bottleneck: lastBottleneck
      });
    }

    // Update best ran dry count
    if (bestRanDryCount === null || result.ranDryCount < bestRanDryCount) {
      bestRanDryCount = result.ranDryCount;
    }

    // Success! (Zero ran dry for base model)
    if (result.ranDryCount === 0) {
      criticalLog(`[SMART FINDER] SUCCESS after ${iteration} iterations!`);
      criticalLog(`[SMART FINDER] Final fleet: T=${fleetConfig.numSmallTugs}, ST=${fleetConfig.numSmallTransportBarges}, SS=${fleetConfig.numSmallStorageBarges}, LT=${fleetConfig.numLargeTransport}, LS=${fleetConfig.numLargeStorage}`);
      SUPPRESS_VERBOSE_LOGGING = previousLoggingState;  // Restore logging
      return {
        success: true,
        config: fleetConfig,
        result,
        iterationsTested: iteration,
        method: 'smart'
      };
    }

    // Analyze the ran dry events to determine bottleneck
    const diagnosis = analyzeRanDryEvents(result, hddSchedule);

    // Track consecutive same bottleneck (may need to escalate)
    if (diagnosis.bottleneck === lastBottleneck) {
      consecutiveSameBottleneck++;
    } else {
      consecutiveSameBottleneck = 0;
    }
    lastBottleneck = diagnosis.bottleneck;

    // Track if ran dry count is improving
    const ranDryImproved = bestRanDryCount !== null && result.ranDryCount < bestRanDryCount;

    // If we've tried the same bottleneck 3+ times, try something else
    // KEY INSIGHT: If STORAGE bottleneck but ran dry isn't improving, the issue is likely
    // TUG availability for storage DELIVERY, not storage capacity. Skip transport, go to tugs.
    let effectiveBottleneck = diagnosis.bottleneck;
    if (consecutiveSameBottleneck >= 3) {
      criticalLog(`[SMART FINDER] Same bottleneck ${consecutiveSameBottleneck}x, trying alternative...`);
      if (effectiveBottleneck === 'storage' && !ranDryImproved && consecutiveSameBottleneck >= 3) {
        // Storage bottleneck but no improvement = issue is storage DELIVERY (needs tugs)
        effectiveBottleneck = 'tug';
        criticalLog(`[SMART FINDER] Storage additions not helping - trying tugs for storage delivery`);
      } else if (effectiveBottleneck === 'storage') {
        effectiveBottleneck = 'transport';
      } else if (effectiveBottleneck === 'transport') {
        effectiveBottleneck = 'tug';
      } else {
        effectiveBottleneck = 'storage';
      }
    }

    // Apply targeted fix based on bottleneck
    let added = false;
    switch (effectiveBottleneck) {
      case 'storage':
        // Add storage barges
        if (fleetConfig.numLargeStorage < MAX_LARGE_STORAGE &&
            fleetConfig.numLargeTransport + fleetConfig.numLargeStorage < TOTAL_LARGE_BARGES) {
          fleetConfig.numLargeStorage++;
          added = true;
          criticalLog(`[SMART FINDER] +1 large storage barge (now ${fleetConfig.numLargeStorage})`);
        } else if (fleetConfig.numSmallStorageBarges < MAX_SMALL_STORAGE) {
          fleetConfig.numSmallStorageBarges += 2;
          added = true;
          criticalLog(`[SMART FINDER] +2 small storage barges (now ${fleetConfig.numSmallStorageBarges})`);
        }
        break;

      case 'transport':
        // Add transport barges
        if (fleetConfig.numLargeTransport < MAX_LARGE_TRANSPORT &&
            fleetConfig.numLargeTransport + fleetConfig.numLargeStorage < TOTAL_LARGE_BARGES) {
          fleetConfig.numLargeTransport++;
          added = true;
          criticalLog(`[SMART FINDER] +1 large transport barge (now ${fleetConfig.numLargeTransport})`);
        } else if (fleetConfig.numSmallTransportBarges < MAX_SMALL_TRANSPORT) {
          fleetConfig.numSmallTransportBarges += 2;
          added = true;
          criticalLog(`[SMART FINDER] +2 small transport barges (now ${fleetConfig.numSmallTransportBarges})`);
        }
        break;

      case 'tug':
      default:
        // Add tugs
        if (fleetConfig.numSmallTugs < MAX_SMALL_TUGS) {
          fleetConfig.numSmallTugs++;
          added = true;
          criticalLog(`[SMART FINDER] +1 tug (now ${fleetConfig.numSmallTugs})`);
        }
        break;
    }

    // If we couldn't add the targeted resource, try fallbacks
    if (!added) {
      criticalLog(`[SMART FINDER] Couldn't add ${effectiveBottleneck}, trying fallbacks...`);
      if (fleetConfig.numSmallTugs < MAX_SMALL_TUGS) {
        fleetConfig.numSmallTugs++;
        added = true;
        criticalLog(`[SMART FINDER] Fallback: +1 tug (now ${fleetConfig.numSmallTugs})`);
      } else if (fleetConfig.numSmallStorageBarges < MAX_SMALL_STORAGE) {
        fleetConfig.numSmallStorageBarges += 2;
        added = true;
        criticalLog(`[SMART FINDER] Fallback: +2 storage (now ${fleetConfig.numSmallStorageBarges})`);
      } else if (fleetConfig.numSmallTransportBarges < MAX_SMALL_TRANSPORT) {
        fleetConfig.numSmallTransportBarges += 2;
        added = true;
        criticalLog(`[SMART FINDER] Fallback: +2 transport (now ${fleetConfig.numSmallTransportBarges})`);
      } else {
        // Exhausted all options
        criticalLog(`[SMART FINDER] EXHAUSTED all options!`);
        break;
      }
    }
  }

  // Failed to find working fleet
  criticalLog(`[SMART FINDER] FAILED after ${iteration} iterations`);
  SUPPRESS_VERBOSE_LOGGING = previousLoggingState;  // Restore logging
  return {
    success: false,
    config: fleetConfig,
    result: null,
    iterationsTested: iteration,
    error: 'Could not find fleet configuration that eliminates all ran dry events',
    method: 'smart'
  };
}

// ============================================================================
// INCREMENTAL FLEET FINDER (Legacy - kept for compatibility)
// ============================================================================
function findWorkingFleetIncremental(userConfig = {}, progressCallback, startingFleet = null) {
  const cfg = { ...CONFIG, ...userConfig };
  const TOTAL_LARGE_BARGES = cfg.TOTAL_LARGE_BARGES || 6;
  // UNLIMITED: No artificial caps on large barges
  const MAX_LARGE_TRANSPORT = TOTAL_LARGE_BARGES;
  const MAX_LARGE_STORAGE = TOTAL_LARGE_BARGES;
  // UNLIMITED: Higher caps for small equipment
  const MAX_SMALL_TUGS = 20;
  const MAX_SMALL_TRANSPORT = 30;
  const MAX_SMALL_STORAGE = 48;

  // Start with analytical recommendation if provided, otherwise minimal fleet
  const fleetConfig = startingFleet ? {
    numSmallTugs: startingFleet.numSmallTugs,
    numSmallTransportBarges: startingFleet.numSmallTransportBarges,
    numSmallStorageBarges: startingFleet.numSmallStorageBarges,
    numLargeTransport: startingFleet.numLargeTransport,
    numLargeStorage: startingFleet.numLargeStorage
  } : {
    // Fallback to minimal fleet if no analytical recommendation
    numSmallTugs: 1,
    numSmallTransportBarges: 0,
    numSmallStorageBarges: 6,
    numLargeTransport: 1,
    numLargeStorage: 0
  };

  let iteration = 0;
  const maxIterations = 500; // Increased from 200

  while (iteration < maxIterations) {
    const optimizationUserConfig = { ...userConfig, _optimizationMode: true };
    const result = runSimulation(fleetConfig, optimizationUserConfig);
    iteration++;

    if (progressCallback) {
      progressCallback({
        type: 'progress',
        tested: iteration,
        total: maxIterations,
        percent: Math.min(95, Math.round(iteration / maxIterations * 100)),
        currentConfig: `${fleetConfig.numSmallTugs}T, ${fleetConfig.numSmallTransportBarges}ST, ${fleetConfig.numSmallStorageBarges}SS, ${fleetConfig.numLargeTransport}LT, ${fleetConfig.numLargeStorage}LS`,
        ranDryCount: result.ranDryCount
      });
    }

    // Success - found working fleet (zero ran dry for base model)
    if (result.ranDryCount === 0) {
      return {
        success: true,
        config: fleetConfig,
        result,
        iterationsTested: iteration
      };
    }

    // Strategy depends on whether we started from analytical recommendation
    if (startingFleet) {
      // Started from analytical recommendation - make small adjustments
      // Try adding the most impactful equipment first (prioritize large barges)
      if (fleetConfig.numLargeTransport < MAX_LARGE_TRANSPORT &&
          fleetConfig.numLargeTransport + fleetConfig.numLargeStorage < TOTAL_LARGE_BARGES) {
        fleetConfig.numLargeTransport++;
      } else if (fleetConfig.numLargeStorage < MAX_LARGE_STORAGE &&
                 fleetConfig.numLargeTransport + fleetConfig.numLargeStorage < TOTAL_LARGE_BARGES) {
        fleetConfig.numLargeStorage++;
      } else if (fleetConfig.numSmallTugs < MAX_SMALL_TUGS) {
        fleetConfig.numSmallTugs++;
      } else if (fleetConfig.numSmallStorageBarges < MAX_SMALL_STORAGE) {
        fleetConfig.numSmallStorageBarges += 3;
      } else if (fleetConfig.numSmallTransportBarges < MAX_SMALL_TRANSPORT) {
        fleetConfig.numSmallTransportBarges += 2;
      } else {
        // Exhausted options
        break;
      }
    } else {
      // Started from minimal - use increment strategy prioritizing large barges
      // 1. Large transport barges (most efficient for moving water)
      // 2. Large storage barges (more cost-effective than small)
      // 3. Small tugs (to move the barges)
      // 4. Small storage barges (buffer at rigs)
      // 5. Small transport (for flexibility)
      if (fleetConfig.numLargeTransport < MAX_LARGE_TRANSPORT &&
          fleetConfig.numLargeTransport + fleetConfig.numLargeStorage < TOTAL_LARGE_BARGES) {
        fleetConfig.numLargeTransport++;
      } else if (fleetConfig.numLargeStorage < MAX_LARGE_STORAGE &&
                 fleetConfig.numLargeTransport + fleetConfig.numLargeStorage < TOTAL_LARGE_BARGES) {
        fleetConfig.numLargeStorage++;
      } else if (fleetConfig.numSmallStorageBarges < MAX_SMALL_STORAGE) {
        fleetConfig.numSmallStorageBarges += 3;
      } else if (fleetConfig.numSmallTugs < MAX_SMALL_TUGS) {
        fleetConfig.numSmallTugs++;
      } else if (fleetConfig.numSmallTransportBarges < MAX_SMALL_TRANSPORT) {
        fleetConfig.numSmallTransportBarges += 2;
      } else {
        // Exhausted all options
        break;
      }
    }
  }

  // Failed to find working fleet
  return {
    success: false,
    config: fleetConfig,
    result: null,
    iterationsTested: iteration,
    error: 'Could not find fleet configuration that eliminates all ran dry events'
  };
}

// ============================================================================
// LOCAL OPTIMIZATION
// ============================================================================
function runLocalOptimization(baseConfig, userConfig = {}, progressCallback) {
  // Iterative greedy search: test reductions, keep best, repeat until no improvement
  const cfg = { ...CONFIG, ...userConfig };
  const TOTAL_LARGE_BARGES = cfg.TOTAL_LARGE_BARGES;

  // Verify base config has no ran dry
  const optimizationUserConfig = { ...userConfig, _optimizationMode: true };
  let baseResult = runSimulation(baseConfig, optimizationUserConfig);
  let workingConfig = baseConfig;
  let findFleetIterations = 0;

  // If base config has ran dry, automatically find a working fleet first
  if (baseResult.ranDryCount > 0) {
    console.log(`[OPTIMIZER] Base config has ${baseResult.ranDryCount} ran dry events. Finding working fleet first...`);

    if (progressCallback) {
      progressCallback({
        type: 'progress',
        iteration: 0,
        tested: 1,
        currentBest: baseResult.score,
        testingConfig: 'Finding working fleet...',
        testLabel: 'Phase 1: Find Working Fleet'
      });
    }

    // Use findWorkingFleetSmart to get a working fleet (targeted iteration)
    const workingFleetResult = findWorkingFleetSmart(userConfig, (progress) => {
      if (progressCallback) {
        progressCallback({
          type: 'progress',
          iteration: 0,
          tested: progress.tested,
          currentBest: null,
          testingConfig: progress.currentConfig,
          testLabel: `Smart finder: ${progress.bottleneck || 'analyzing'} (${progress.ranDryCount || '?'} ran dry)`
        });
      }
    }, baseConfig);

    if (!workingFleetResult.success) {
      return {
        error: 'Could not find a working fleet configuration. Try increasing equipment limits.',
        baseResult: baseResult,
        top15: [],
        optimal: null,
        bestZeroRanDry: null,
        totalTested: workingFleetResult.iterationsTested,
        successCount: 0
      };
    }

    // Use the working fleet as the new starting point
    workingConfig = workingFleetResult.config;
    baseResult = workingFleetResult.result;
    findFleetIterations = workingFleetResult.iterationsTested;

    console.log(`[OPTIMIZER] Found working fleet after ${findFleetIterations} iterations: Tugs=${workingConfig.numSmallTugs}, ST=${workingConfig.numSmallTransportBarges}, SS=${workingConfig.numSmallStorageBarges}, LT=${workingConfig.numLargeTransport}, LS=${workingConfig.numLargeStorage}`);
  }

  // Use the WORKING CONFIG as starting point (guaranteed zero ran dry)
  // Peak usage from simulation can be unreliable for storage barges
  console.log(`[OPTIMIZER] Working config: Tugs=${workingConfig.numSmallTugs}, ST=${workingConfig.numSmallTransportBarges}, SS=${workingConfig.numSmallStorageBarges}, LT=${workingConfig.numLargeTransport}, LS=${workingConfig.numLargeStorage}`);

  // Start optimization from working config (not peak usage - it was unreliable)
  let currentConfig = {
    numSmallTugs: workingConfig.numSmallTugs,
    numSmallTransportBarges: workingConfig.numSmallTransportBarges,
    numSmallStorageBarges: workingConfig.numSmallStorageBarges,
    numLargeTransport: workingConfig.numLargeTransport,
    numLargeStorage: workingConfig.numLargeStorage
  };

  // Use baseResult as starting point (already verified zero ran dry)
  const peakResult = baseResult;

  // MEMORY FIX: Store only lightweight results to prevent heap overflow
  const allResults = [toLightweightResult(peakResult)];
  const tested = new Set();
  tested.add(`${currentConfig.numSmallTugs}-${currentConfig.numSmallTransportBarges}-${currentConfig.numSmallStorageBarges}-${currentConfig.numLargeTransport}-${currentConfig.numLargeStorage}`);

  // MEMORY FIX: Use lightweight result for tracking
  let currentBest = toLightweightResult(peakResult);
  let iteration = 0;
  const MAX_ITERATIONS = 50; // Increased from 20

  console.log(`[OPTIMIZER] Starting optimization from peak usage. Peak result cost: $${currentBest.costs.grandTotal.toLocaleString()}, Score: ${currentBest.score.toFixed(0)}, RanDry: ${currentBest.ranDryCount}`);

  while (iteration < MAX_ITERATIONS) {
    iteration++;

    // Define moves - reductions AND rebalancing (shift large between transport/storage)
    const moves = [
      // Reductions (cost savings)
      { label: 'Tug -1', dTugs: -1, dST: 0, dSS: 0, dLT: 0, dLS: 0 },
      { label: 'Tug -2', dTugs: -2, dST: 0, dSS: 0, dLT: 0, dLS: 0 },
      { label: 'SmallTransport -1', dTugs: 0, dST: -1, dSS: 0, dLT: 0, dLS: 0 },
      { label: 'SmallTransport -2', dTugs: 0, dST: -2, dSS: 0, dLT: 0, dLS: 0 },
      { label: 'SmallTransport -3', dTugs: 0, dST: -3, dSS: 0, dLT: 0, dLS: 0 },
      { label: 'SmallStorage -1', dTugs: 0, dST: 0, dSS: -1, dLT: 0, dLS: 0 },
      { label: 'SmallStorage -2', dTugs: 0, dST: 0, dSS: -2, dLT: 0, dLS: 0 },
      { label: 'SmallStorage -3', dTugs: 0, dST: 0, dSS: -3, dLT: 0, dLS: 0 },
      { label: 'SmallStorage -6', dTugs: 0, dST: 0, dSS: -6, dLT: 0, dLS: 0 },
      { label: 'LargeTransport -1', dTugs: 0, dST: 0, dSS: 0, dLT: -1, dLS: 0 },
      { label: 'LargeStorage -1', dTugs: 0, dST: 0, dSS: 0, dLT: 0, dLS: -1 },
      // Rebalancing (shift large between transport and storage - same total cost)
      { label: 'LT→LS (shift 1)', dTugs: 0, dST: 0, dSS: 0, dLT: -1, dLS: +1 },
      { label: 'LS→LT (shift 1)', dTugs: 0, dST: 0, dSS: 0, dLT: +1, dLS: -1 },
      // Combined moves (reduce small, keep large)
      { label: 'SmallTrans-2 SmallStore-3', dTugs: 0, dST: -2, dSS: -3, dLT: 0, dLS: 0 },
      { label: 'Tug-1 SmallStore-3', dTugs: -1, dST: 0, dSS: -3, dLT: 0, dLS: 0 },
    ];
    // Backward compatibility alias
    const reductions = moves;

    let bestImprovement = null;
    let bestImprovementScore = currentBest.score;

    for (const reduction of reductions) {
      const testConfig = {
        numSmallTugs: currentConfig.numSmallTugs + reduction.dTugs,
        numSmallTransportBarges: currentConfig.numSmallTransportBarges + reduction.dST,
        numSmallStorageBarges: currentConfig.numSmallStorageBarges + reduction.dSS,
        numLargeTransport: currentConfig.numLargeTransport + reduction.dLT,
        numLargeStorage: currentConfig.numLargeStorage + reduction.dLS
      };

      // Validate
      if (testConfig.numSmallTugs < 0) continue;
      if (testConfig.numSmallTransportBarges < 0) continue;
      if (testConfig.numSmallStorageBarges < 0) continue;
      if (testConfig.numLargeStorage < 0 || testConfig.numLargeTransport < 0) continue;
      if (testConfig.numLargeStorage + testConfig.numLargeTransport > TOTAL_LARGE_BARGES) continue;
      if (testConfig.numSmallTugs === 0 && testConfig.numLargeTransport === 0) continue;
      if (testConfig.numSmallTransportBarges === 0 && testConfig.numLargeTransport === 0) continue;
      if (testConfig.numSmallStorageBarges === 0 && testConfig.numLargeStorage === 0) continue;

      const key = `${testConfig.numSmallTugs}-${testConfig.numSmallTransportBarges}-${testConfig.numSmallStorageBarges}-${testConfig.numLargeTransport}-${testConfig.numLargeStorage}`;
      if (tested.has(key)) continue;
      tested.add(key);

      const result = runSimulation(testConfig, optimizationUserConfig);
      // MEMORY FIX: Store only lightweight result to prevent heap overflow
      allResults.push(toLightweightResult(result));

      if (progressCallback) {
        progressCallback({
          type: 'progress',
          iteration: iteration,
          tested: allResults.length,
          currentBest: currentBest.score,
          testingConfig: key,
          testLabel: reduction.label
        });
      }

      // Track best improvement (lower score = better)
      // Only consider zero ran dry configs for base model optimization
      if (result.ranDryCount === 0 && result.score < bestImprovementScore) {
        // MEMORY FIX: Store lightweight result
        bestImprovement = toLightweightResult(result);
        bestImprovementScore = result.score;
      }
    }

    // If we found an improvement, make it the new current best and continue
    if (bestImprovement) {
      const oldScore = currentBest.score;
      currentBest = bestImprovement; // Already lightweight
      currentConfig = {
        numSmallTugs: bestImprovement.config.numSmallTugs,
        numSmallTransportBarges: bestImprovement.config.numSmallTransportBarges,
        numSmallStorageBarges: bestImprovement.config.numSmallStorageBarges,
        numLargeTransport: bestImprovement.config.numLargeTransport,
        numLargeStorage: bestImprovement.config.numLargeStorage
      };
      console.log(`[OPTIMIZER] Iteration ${iteration}: Improvement found! Score ${oldScore.toFixed(0)} → ${bestImprovementScore.toFixed(0)} (saved $${(oldScore - bestImprovementScore).toLocaleString()})`);
      console.log(`[OPTIMIZER] New config: Tugs=${currentConfig.numSmallTugs}, ST=${currentConfig.numSmallTransportBarges}, SS=${currentConfig.numSmallStorageBarges}, LT=${currentConfig.numLargeTransport}, LS=${currentConfig.numLargeStorage}`);
    } else {
      console.log(`[OPTIMIZER] Iteration ${iteration}: No improvement found. Stopping.`);
      break;
    }
  }

  // Sort all results by score (lower is better)
  allResults.sort((a, b) => a.score - b.score);

  // Filter configs by ran dry status
  const ACCEPTABLE_DOWNTIME_HOURS = 8;
  const zeroRanDryConfigs = allResults.filter(r => r.ranDryCount === 0);
  const acceptableConfigs = allResults.filter(r =>
    r.ranDryCount === 0 || (r.downtimeHours !== undefined && r.downtimeHours < ACCEPTABLE_DOWNTIME_HOURS)
  );

  // Optimal recommendation: Best zero ran dry config (base model requirement)
  const optimal = zeroRanDryConfigs[0] || allResults[0];

  const totalTestedWithFindFleet = allResults.length + findFleetIterations;
  console.log(`[OPTIMIZER] Complete. Tested ${allResults.length} optimization configs in ${iteration} iterations${findFleetIterations > 0 ? ` (+ ${findFleetIterations} fleet-finding iterations)` : ''}.`);
  console.log(`[OPTIMIZER] Recommended config (zero ran dry): Tugs=${optimal.config.numSmallTugs}, ST=${optimal.config.numSmallTransportBarges}, SS=${optimal.config.numSmallStorageBarges}, LT=${optimal.config.numLargeTransport}, LS=${optimal.config.numLargeStorage}`);
  console.log(`[OPTIMIZER] Best score: ${optimal.score.toFixed(0)} (cost: $${optimal.costs.grandTotal.toLocaleString()}, ranDry: ${optimal.ranDryCount})`);
  console.log(`[OPTIMIZER] Zero ran dry configs: ${zeroRanDryConfigs.length}, Acceptable (< 8h): ${acceptableConfigs.length}, Total: ${allResults.length}`);

  return {
    top15: allResults.slice(0, 15), // Show top 15 by score regardless of ran dry
    optimal: optimal, // Best zero ran dry config (recommended)
    bestZeroRanDry: zeroRanDryConfigs[0] || null,
    acceptableConfigs: acceptableConfigs, // For UI to show without strikethrough
    totalTested: totalTestedWithFindFleet,
    successCount: zeroRanDryConfigs.length,
    iterations: iteration,
    findFleetIterations: findFleetIterations
  };
}

// ============================================================================
// MESSAGE HANDLER
// ============================================================================
self.onmessage = function(e) {
  const { type, fleetConfig, userConfig } = e.data;

  if (type === 'runSingle') {
    // Update global CONFIG with user config
    if (userConfig) {
      CONFIG = { ...CONFIG, ...userConfig };
    }

    try {
      const result = runSimulation(fleetConfig, userConfig);
      self.postMessage({ type: 'singleResult', result, fleetConfig });
    } catch (error) {
      console.error('[WORKER ERROR]', error.message, error.stack);
      self.postMessage({ type: 'error', message: error.message, stack: error.stack });
    }
  }
  else if (type === 'runOptimize') {
    // Update global CONFIG with user config
    if (userConfig) {
      CONFIG = { ...CONFIG, ...userConfig };
    }

    const result = runOptimization(userConfig, (progress) => {
      self.postMessage(progress);
    });

    self.postMessage({ type: 'optimizeResult', result });
  }
  else if (type === 'findWorkingFleet') {
    // Update global CONFIG with user config
    if (userConfig) {
      CONFIG = { ...CONFIG, ...userConfig };
    }

    const startingFleet = e.data.startingFleet || null;
    const useSmart = e.data.useSmart !== false; // Default to smart finder

    let result;
    if (useSmart) {
      console.log('[WORKER] Using SMART fleet finder...');
      result = findWorkingFleetSmart(userConfig, (progress) => {
        self.postMessage(progress);
      }, startingFleet);
    } else {
      console.log('[WORKER] Using legacy incremental fleet finder...');
      result = findWorkingFleetIncremental(userConfig, (progress) => {
        self.postMessage(progress);
      }, startingFleet);
    }

    self.postMessage({ type: 'workingFleetFound', result });
  }
  else if (type === 'findWorkingFleetSmart') {
    // Explicit smart fleet finder (for direct testing)
    if (userConfig) {
      CONFIG = { ...CONFIG, ...userConfig };
    }

    const startingFleet = e.data.startingFleet || null;
    const result = findWorkingFleetSmart(userConfig, (progress) => {
      self.postMessage(progress);
    }, startingFleet);

    self.postMessage({ type: 'workingFleetFound', result });
  }
  else if (type === 'runLocalOptimize') {
    // Update global CONFIG with user config
    if (userConfig) {
      CONFIG = { ...CONFIG, ...userConfig };
    }

    const result = runLocalOptimization(fleetConfig, userConfig, (progress) => {
      self.postMessage(progress);
    });

    self.postMessage({ type: 'optimizeResult', result });
  }
  else if (type === 'analyzeFleet') {
    // Run analytical fleet sizing (pre-simulation)
    if (userConfig) {
      CONFIG = { ...CONFIG, ...userConfig };
    }

    try {
      console.log('[WORKER] Running analytical fleet sizing...');
      const result = calculateAnalyticalFleetSchedule(userConfig);
      self.postMessage({ type: 'analyticalFleetResult', result });
    } catch (error) {
      console.error('[WORKER ERROR] Analytical fleet sizing failed:', error.message, error.stack);
      self.postMessage({ type: 'error', message: error.message, stack: error.stack });
    }
  }
  else if (type === 'updateConfig') {
    // Update global CONFIG
    if (userConfig) {
      CONFIG = { ...CONFIG, ...userConfig };
    }
    self.postMessage({ type: 'configUpdated' });
  }
};
