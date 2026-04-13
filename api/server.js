const fs = require('fs');
const path = require('path');
const express = require('express');
const admin = require('firebase-admin');

const PORT = Number(process.env.PORT || 3010);
const FIREBASE_DB_URL = process.env.FIREBASE_DB_URL || 'URL-FIREBASE-AQUI';
const SERVICE_ACCOUNT_PATH = process.env.FIREBASE_SERVICE_ACCOUNT_PATH
  || path.join(__dirname, '..', 'logs-iops-firebase-adminsdk-fbsvc-9ab3d463b5.json');
const LOCAL_CACHE_DIR = process.env.LOCAL_CACHE_DIR || path.join(__dirname, 'data');
const PROJECT_CONFIGS = {
  ativacoes: {
    key: 'ativacoes',
    label: 'Ativações',
    path: process.env.FIREBASE_DB_PATH_ATIVACOES || 'logs/Ativações',
    cacheFile: 'ativacoes-cache.json'
  },
  conferencia: {
    key: 'conferencia',
    label: 'Conferência de Fluxo - PCV',
    path: process.env.FIREBASE_DB_PATH_CONFERENCIA || 'logs/Conferência de Fluxo - PCV',
    cacheFile: 'conferencia-fluxo-pcv-cache.json'
  }
};

const projectState = {};
const sseClients = new Set();

function loadServiceAccount(filePath) {
  if (!fs.existsSync(filePath)) {
    throw new Error(`Arquivo de service account nao encontrado: ${filePath}`);
  }

  const raw = fs.readFileSync(filePath, 'utf8');
  return JSON.parse(raw);
}

const serviceAccount = loadServiceAccount(SERVICE_ACCOUNT_PATH);

function ensureCacheDir() {
  fs.mkdirSync(LOCAL_CACHE_DIR, { recursive: true });
}

function getProjectState(projectKey) {
  if (!projectState[projectKey]) {
    projectState[projectKey] = {
      cachedData: {},
      hasInitialSync: false,
      lastSyncAt: null,
      lastSyncError: null,
      persistTimer: null
    };
  }
  return projectState[projectKey];
}

function getProjectCacheFile(projectKey) {
  const cfg = PROJECT_CONFIGS[projectKey];
  return path.join(LOCAL_CACHE_DIR, cfg.cacheFile);
}

function loadLocalCache(projectKey) {
  const state = getProjectState(projectKey);
  const cacheFile = getProjectCacheFile(projectKey);

  try {
    ensureCacheDir();
    if (!fs.existsSync(cacheFile)) return;
    const content = fs.readFileSync(cacheFile, 'utf8');
    const parsed = JSON.parse(content);
    state.cachedData = parsed && parsed.data ? parsed.data : {};
    state.lastSyncAt = parsed && parsed.ts ? parsed.ts : null;
  } catch (error) {
    console.error(`Falha ao carregar cache local (${projectKey}):`, error.message);
  }
}

function persistLocalCacheSoon(projectKey) {
  const state = getProjectState(projectKey);
  const cacheFile = getProjectCacheFile(projectKey);

  if (state.persistTimer) clearTimeout(state.persistTimer);

  state.persistTimer = setTimeout(() => {
    try {
      ensureCacheDir();
      fs.writeFileSync(cacheFile, JSON.stringify({
        ts: state.lastSyncAt,
        data: state.cachedData
      }));
    } catch (error) {
      console.error(`Falha ao salvar cache local (${projectKey}):`, error.message);
    }
  }, 400);
}

function broadcastRealtimeSignal(payload) {
  const message = `data: ${JSON.stringify(payload)}\n\n`;
  for (const client of sseClients) {
    client.write(message);
  }
}

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    databaseURL: FIREBASE_DB_URL
  });
}

Object.keys(PROJECT_CONFIGS).forEach(projectKey => {
  const cfg = PROJECT_CONFIGS[projectKey];
  const state = getProjectState(projectKey);
  loadLocalCache(projectKey);

  const dbRef = admin.database().ref(cfg.path);
  dbRef.on('value', snapshot => {
    state.cachedData = snapshot.val() || {};
    state.hasInitialSync = true;
    state.lastSyncAt = Date.now();
    state.lastSyncError = null;
    persistLocalCacheSoon(projectKey);
    broadcastRealtimeSignal({ type: 'sync', project: projectKey, ts: state.lastSyncAt });
  }, error => {
    state.lastSyncError = error.message;
    broadcastRealtimeSignal({ type: 'error', project: projectKey, ts: Date.now(), error: error.message });
  });
});

const app = express();

app.get('/api/health', (_req, res) => {
  const projects = {};
  Object.keys(PROJECT_CONFIGS).forEach(projectKey => {
    const cfg = PROJECT_CONFIGS[projectKey];
    const state = getProjectState(projectKey);
    projects[projectKey] = {
      label: cfg.label,
      path: cfg.path,
      hasInitialSync: state.hasInitialSync,
      lastSyncAt: state.lastSyncAt,
      hasCachedData: Boolean(state.cachedData && Object.keys(state.cachedData).length),
      lastSyncError: state.lastSyncError
    };
  });

  res.json({
    ok: true,
    databaseURL: FIREBASE_DB_URL,
    projects
  });
});

app.get('/api/data/:project', (req, res) => {
  const projectKey = String(req.params.project || '').toLowerCase();
  const cfg = PROJECT_CONFIGS[projectKey];
  if (!cfg) {
    res.status(404).json({ ok: false, error: `Projeto inválido: ${projectKey}` });
    return;
  }

  const state = getProjectState(projectKey);
  res.json({
    ok: true,
    project: projectKey,
    ts: state.lastSyncAt,
    source: 'local-cache',
    hasInitialSync: state.hasInitialSync,
    data: state.cachedData || {}
  });
});

app.get('/api/data/ativacoes', (_req, res) => {
  const state = getProjectState('ativacoes');
  res.json({
    ok: true,
    project: 'ativacoes',
    ts: state.lastSyncAt,
    source: 'local-cache',
    hasInitialSync: state.hasInitialSync,
    data: state.cachedData || {}
  });
});

app.get('/api/realtime', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();

  sseClients.add(res);
  const projects = {};
  Object.keys(PROJECT_CONFIGS).forEach(projectKey => {
    const state = getProjectState(projectKey);
    projects[projectKey] = {
      hasInitialSync: state.hasInitialSync,
      lastSyncAt: state.lastSyncAt,
      lastSyncError: state.lastSyncError
    };
  });

  res.write(`data: ${JSON.stringify({ type: 'connected', ts: Date.now(), projects })}\n\n`);

  const keepAlive = setInterval(() => {
    res.write(': ping\n\n');
  }, 25000);

  req.on('close', () => {
    clearInterval(keepAlive);
    sseClients.delete(res);
    res.end();
  });
});

app.get('/api/realtime/ativacoes', (req, res) => {
  req.url = '/api/realtime';
  app.handle(req, res);
});

app.listen(PORT, () => {
  console.log(`dashboard-firebase api listening on port ${PORT}`);
});
