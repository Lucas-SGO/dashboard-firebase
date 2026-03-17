// ============================================================
// DATA STATE
// ============================================================
let rawData = null;
let allCasos = [];
let filteredCasos = [];
let dateFrom = null;
let dateTo = null;
let availableDates = [];
let realtimeSource = null;
let snapshotRefreshTimer = null;
let snapshotRequestInFlight = false;
let selectedProject = 'ativacoes';
let selectedTheme = 'dark';
const REALTIME_STREAM_URL = '/api/realtime';
const SNAPSHOT_BASE_URL = '/api/data';
const MIN_CASES_ROWS_VISIBLE = 10;
const CASE_ROW_ESTIMATED_HEIGHT = 42;

const PROJECTS = {
  ativacoes: {
    key: 'ativacoes',
    title: 'Ativações',
    subtitle: 'Dashboard de Acompanhamento',
    kpis: {
      totalLabel: 'Total Casos',
      totalSub: 'processados',
      doneLabel: 'Sucessos',
      doneSub: 'finalizados',
      errorLabel: 'Erros',
      errorSub: 'sem resolução',
      fourthLabel: 'Cancelados/Incorretos',
      fourthSub: 'ou incorretos',
      runningLabel: 'Rodando',
      runningSub: 'Em Andamento'
    }
  },
  conferencia: {
    key: 'conferencia',
    title: 'Conferência de Fluxo',
    subtitle: 'Painel de Conferência PCV',
    kpis: {
      totalLabel: 'Total Casos',
      totalSub: 'processados',
      doneLabel: 'Conferidos',
      doneSub: 'processo jurídico concluído',
      errorLabel: 'Erros',
      errorSub: 'falhas de execução',
      fourthLabel: 'Rodagens',
      fourthSub: 'execuções no período',
      runningLabel: 'Em Andamento',
      runningSub: 'casos pendentes'
    }
  }
};

// Active quick-range button tracking
let activeRange = 'today';

// ============================================================
// CONFIG
// ============================================================
function openConfig() {
  document.getElementById('configModal').classList.add('open');
}

function closeConfig() {
  document.getElementById('configModal').classList.remove('open');
}

function applyTheme(theme) {
  const body = document.body;
  const btn = document.getElementById('themeToggleBtn');
  selectedTheme = theme === 'light' ? 'light' : 'dark';

  body.classList.toggle('light-mode', selectedTheme === 'light');
  if (btn) {
    btn.textContent = selectedTheme === 'light' ? '🌙 Modo Escuro' : '☀ Modo Claro';
  }
}

function toggleTheme() {
  const nextTheme = selectedTheme === 'light' ? 'dark' : 'light';
  applyTheme(nextTheme);
  localStorage.setItem('dashboardTheme', selectedTheme);
}

function getProjectConfig() {
  return PROJECTS[selectedProject] || PROJECTS.ativacoes;
}

function applyProjectUI() {
  const config = getProjectConfig();
  document.getElementById('dashboardTitle').textContent = config.title;
  document.getElementById('dashboardSubtitle').textContent = config.subtitle;

  document.getElementById('kpiTotalLabel').textContent = config.kpis.totalLabel;
  document.getElementById('kpiTotalSub').textContent = config.kpis.totalSub;
  document.getElementById('kpiFinalizadosLabel').textContent = config.kpis.doneLabel;
  document.getElementById('kpiFinalizadosSub').textContent = config.kpis.doneSub;
  document.getElementById('kpiErrosLabel').textContent = config.kpis.errorLabel;
  document.getElementById('kpiErrosSub').textContent = config.kpis.errorSub;
  document.getElementById('kpiCanceladosLabel').textContent = config.kpis.fourthLabel;
  document.getElementById('kpiCanceladosSub').textContent = config.kpis.fourthSub;
  document.getElementById('kpiRodagensLabel').textContent = config.kpis.runningLabel;
  document.getElementById('kpiRodagensSub').textContent = config.kpis.runningSub;
}

function onProjectChange(projectKey) {
  if (!PROJECTS[projectKey]) return;
  selectedProject = projectKey;
  localStorage.setItem('dashboardProject', projectKey);
  dateFrom = null;
  dateTo = null;
  activeRange = 'today';
  applyProjectUI();
  connectBackendRealtime();
}

function disconnectRealtime() {
  if (realtimeSource) {
    realtimeSource.close();
    realtimeSource = null;
  }
}

function fetchLocalSnapshot() {
  if (snapshotRequestInFlight) return;
  snapshotRequestInFlight = true;

  const snapshotUrl = `${SNAPSHOT_BASE_URL}/${encodeURIComponent(selectedProject)}`;

  fetch(snapshotUrl, { cache: 'no-store' })
    .then(response => {
      if (!response.ok) {
        throw new Error(`Falha ao carregar snapshot local (${response.status})`);
      }
      return response.json();
    })
    .then(payload => {
      rawData = payload && payload.data ? payload.data : {};
      processData();
      document.getElementById('liveBadge').style.display = 'flex';
    })
    .catch(error => {
      console.error('Falha ao carregar snapshot local:', error);
      document.getElementById('liveBadge').style.display = 'none';
    })
    .finally(() => {
      snapshotRequestInFlight = false;
    });
}

function scheduleSnapshotRefresh(delay = 250) {
  if (snapshotRefreshTimer) return;
  snapshotRefreshTimer = setTimeout(() => {
    snapshotRefreshTimer = null;
    fetchLocalSnapshot();
  }, delay);
}

function connectBackendRealtime() {
  disconnectRealtime();
  showGlobalLoading();
  fetchLocalSnapshot();

  realtimeSource = new EventSource(REALTIME_STREAM_URL);

  realtimeSource.onmessage = event => {
    try {
      const payload = JSON.parse(event.data || '{}');
      if (payload.type === 'connected') {
        scheduleSnapshotRefresh(120);
      }
      if (payload.type === 'sync' && payload.project === selectedProject) {
        scheduleSnapshotRefresh(120);
      }
      if (payload.type === 'error') {
        console.error('Erro no stream realtime:', payload.error);
      }
    } catch (err) {
      console.error('Falha ao processar stream realtime:', err);
    }
  };

  realtimeSource.onerror = () => {
    // EventSource tenta reconectar automaticamente.
    document.getElementById('liveBadge').style.display = 'none';
  };
}

function connectFirebase() {
  closeConfig();
  connectBackendRealtime();
}

function loadFromFile(input) {
  const file = input.files[0];
  if (!file) return;
  disconnectRealtime();
  document.getElementById('liveBadge').style.display = 'none';
  closeConfig();
  showGlobalLoading();
  const reader = new FileReader();
  reader.onload = e => {
    try {
      rawData = JSON.parse(e.target.result);
      processData();
    } catch(err) {
      alert('Arquivo JSON inválido: ' + err.message);
      showGlobalLoading(false);
    }
  };
  reader.readAsText(file);
}

function showGlobalLoading(show = true) {
  // handled by empty states
}

function syncCasesHeightToMachines() {
  const casesPanel = document.querySelector('.main-grid > .panel:first-child');
  const machinesPanel = document.querySelector('.main-grid > .panel:last-child');
  if (!casesPanel || !machinesPanel) return;

  const tableScroll = casesPanel.querySelector('.table-scroll');
  if (!tableScroll) return;

  const panelHeader = casesPanel.querySelector('.panel-header');
  const searchBar = casesPanel.querySelector('.casos-search');

  const fixedHeight = (panelHeader ? panelHeader.offsetHeight : 0)
    + (searchBar ? searchBar.offsetHeight : 0);

  const machineHeight = machinesPanel.offsetHeight;
  const minScrollHeight = MIN_CASES_ROWS_VISIBLE * CASE_ROW_ESTIMATED_HEIGHT;
  const desiredHeight = machineHeight > 0 ? (machineHeight - fixedHeight) : minScrollHeight;
  const finalHeight = Math.max(minScrollHeight, desiredHeight);

  tableScroll.style.maxHeight = `${finalHeight}px`;
  tableScroll.style.overflowY = 'auto';
}

// Auto-load from localStorage
window.addEventListener('load', () => {
  const storedProject = localStorage.getItem('dashboardProject');
  const storedTheme = localStorage.getItem('dashboardTheme');

  if (storedTheme === 'light' || storedTheme === 'dark') {
    selectedTheme = storedTheme;
  }

  applyTheme(selectedTheme);

  if (storedProject && PROJECTS[storedProject]) {
    selectedProject = storedProject;
  }
  const projectSelect = document.getElementById('projectSelect');
  if (projectSelect) {
    projectSelect.value = selectedProject;
  }
  applyProjectUI();
  connectBackendRealtime();
  window.addEventListener('resize', () => {
    window.requestAnimationFrame(syncCasesHeightToMachines);
  });
});

// ============================================================
// DATA PROCESSING
// ============================================================
function processData(silent = false) {
  if (!rawData) return;

  availableDates = Object.keys(rawData).sort();

  // Default to "today" or most recent date on first load
  if (!dateFrom) {
    setRange('today', true);
    return; // setRange calls back into processData
  }

  buildCasos();
  updateKPIs();
  renderMachines();
  renderRecentEvents();
  renderRecentErrors();
  updateLastUpdate();
}

function setRange(preset, init = false) {
  activeRange = preset;
  const allDates = Object.keys(rawData || {}).sort();
  const last = allDates[allDates.length - 1] || toDateStr(new Date());
  const today = toDateStr(new Date());

  if (preset === 'today') {
    dateFrom = dateTo = (rawData && rawData[today]) ? today : last;
  } else if (preset === '7d') {
    dateTo = last;
    dateFrom = toDateStr(new Date(Date.now() - 6 * 86400000));
  } else if (preset === '30d') {
    dateTo = last;
    dateFrom = toDateStr(new Date(Date.now() - 29 * 86400000));
  } else if (preset === 'all') {
    dateFrom = allDates[0] || today;
    dateTo = last;
  }

  document.getElementById('dateFrom').value = dateFrom;
  document.getElementById('dateTo').value = dateTo;
  highlightRangeBtn();

  if (!init) {
    buildCasos();
    updateKPIs();
    renderMachines();
    renderRecentEvents();
    renderRecentErrors();
    updateLastUpdate();
  } else {
    buildCasos();
    updateKPIs();
    renderMachines();
    renderRecentEvents();
    renderRecentErrors();
    updateLastUpdate();
  }
}

function onRangeChange() {
  dateFrom = document.getElementById('dateFrom').value;
  dateTo   = document.getElementById('dateTo').value;
  if (!dateFrom || !dateTo) return;
  if (dateFrom > dateTo) { dateTo = dateFrom; document.getElementById('dateTo').value = dateTo; }
  activeRange = null;
  highlightRangeBtn();
  buildCasos();
  updateKPIs();
  renderMachines();
  renderRecentEvents();
  renderRecentErrors();
  updateLastUpdate();
}

function highlightRangeBtn() {
  document.querySelectorAll('.date-btn').forEach(b => b.classList.remove('active'));
  if (activeRange) {
    const map = { today: 0, '7d': 1, '30d': 2, all: 3 };
    const idx = map[activeRange];
    const btns = document.querySelectorAll('.date-btn');
    if (btns[idx]) btns[idx].classList.add('active');
  }
}

function getSelectedDates() {
  if (!rawData || !dateFrom || !dateTo) return [];
  return Object.keys(rawData).filter(d => d >= dateFrom && d <= dateTo).sort();
}

function toDateStr(d) {
  return d.toISOString().slice(0, 10);
}

function compareVersion(a, b) {
  const pa = String(a || '').split('.').map(part => Number(part) || 0);
  const pb = String(b || '').split('.').map(part => Number(part) || 0);
  const len = Math.max(pa.length, pb.length);

  for (let i = 0; i < len; i += 1) {
    const va = pa[i] || 0;
    const vb = pb[i] || 0;
    if (va > vb) return 1;
    if (va < vb) return -1;
  }
  return 0;
}

function normalizeForMatch(value) {
  return String(value || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '');
}

function isProspectNotFoundEvent(evtKey, evt) {
  const keyNorm = normalizeForMatch(evtKey || '');
  const eventNorm = normalizeForMatch(evt && evt.evento);
  const exceptionNorm = normalizeForMatch(evt && evt.exception);
  const stackNorm = normalizeForMatch(evt && evt.stacktrace);

  const combined = `${keyNorm} ${eventNorm} ${exceptionNorm} ${stackNorm}`;
  return combined.includes('prospect') && combined.includes('nao encontrado');
}

function isCreditStageException(evtKey, evt) {
  const keyNorm = normalizeForMatch(evtKey || '');
  const eventNorm = normalizeForMatch(evt && evt.evento);
  const exceptionNorm = normalizeForMatch(evt && evt.exception);
  const stackNorm = normalizeForMatch(evt && evt.stacktrace);

  const combined = `${keyNorm} ${eventNorm} ${exceptionNorm} ${stackNorm}`;
  return combined.includes('etapa de credito')
    && combined.includes('favor verificar o cot da proposta');
}

function isBrokerMismatchException(evtKey, evt) {
  const keyNorm = normalizeForMatch(evtKey || '');
  const eventNorm = normalizeForMatch(evt && evt.evento);
  const exceptionNorm = normalizeForMatch(evt && evt.exception);
  const stackNorm = normalizeForMatch(evt && evt.stacktrace);

  const combined = `${keyNorm} ${eventNorm} ${exceptionNorm} ${stackNorm}`;
  return combined.includes('corretor vinculado ao prospect nao e o mesmo')
    && combined.includes('cadastrado no pipefy');
}

function isActiveProposalException(evtKey, evt) {
  const keyNorm = normalizeForMatch(evtKey || '');
  const eventNorm = normalizeForMatch(evt && evt.evento);
  const exceptionNorm = normalizeForMatch(evt && evt.exception);
  const stackNorm = normalizeForMatch(evt && evt.stacktrace);

  const combined = `${keyNorm} ${eventNorm} ${exceptionNorm} ${stackNorm}`;
  return combined.includes('quote.validartransacoesdestatus')
    && combined.includes('ja existe uma outra proposta ativa para essa unidade')
    && combined.includes('nao e possivel ativar a proposta em questao');
}

function buildCasos() {
  if (!rawData || !dateFrom || !dateTo) return;
  const isAtivacoes = selectedProject === 'ativacoes';
  const isConferencia = selectedProject === 'conferencia';
  const dates = getSelectedDates();
  allCasos = [];

  for (const date of dates) {
    const day = rawData[date] || {};
    for (const [runKey, run] of Object.entries(day)) {
      if (!run.eventos) continue;
      const machine = run.Maquina || runKey.split(' - ')[0] || '?';
      const user = run.Usuario || '?';

    for (const [casoName, evts] of Object.entries(run.eventos)) {
      if (casoName === 'Geral') continue;

      // Determine status
      let status = 'em-andamento';
      let fase = isConferencia ? 'conferencia' : 'iniciando'; // phase within em-andamento
      let proposta = '';
      let lastTime = '';
      let hasError = false;
      let prospectNotFound = false;
      let creditStageException = false;
      let brokerMismatchException = false;
      let activeProposalException = false;
      let sentToCredit = false;
      let eventList = [];

      for (const [evtKey, evt] of Object.entries(evts)) {
        if (typeof evt !== 'object') continue;
        eventList.push({ key: evtKey, ...evt });
        if (evt.nivel === 'ERRO') hasError = true;
        if (evt.Proposta) proposta = evt.Proposta;
        if (evt.timestamp) lastTime = evt.timestamp;

        const ev = evt.evento || '';
        if (isAtivacoes) {
          // Regra de negocio: "Prospect nao encontrado" entra em Cancelados/Incorretos.
          if (isProspectNotFoundEvent(evtKey, evt)) {
            prospectNotFound = true;
          }
          if (isCreditStageException(evtKey, evt)) {
            creditStageException = true;
          }
          if (isBrokerMismatchException(evtKey, evt)) {
            brokerMismatchException = true;
          }
          if (isActiveProposalException(evtKey, evt)) {
            activeProposalException = true;
          }
        }

        // STATUS (terminal states)
        if (isAtivacoes) {
          if (ev.includes('Enviado para o Crédito') || ev.includes('no Crédito')) {
            sentToCredit = true;
            status = 'credito';
          }
          else if (ev.includes('Proposta Cancelada')) status = 'cancelado';
          else if (ev.includes('Proposta Incorreta')) status = 'cancelado';
          else if (ev.includes('Finalizar Rodagem')) status = 'finalizado';
        } else if (isConferencia) {
          if (ev.includes('Finalizar Processo Jurídico')) status = 'finalizado';
          if (ev.includes('Comparar PV-PCV')) fase = 'comparacao';
          else if (ev.includes('Conferência de Minuta PCV')) fase = 'conferencia';
          else if (ev.includes('Extrair Documentos') || ev.includes('Arquivo encontrado')) fase = 'documentos';
          else if (ev.includes('Anexar Arquivos Pipefy')) fase = 'anexando';
          else if (ev.includes('Finalizar Processo Jurídico')) fase = 'finalizado';
        }

        // FASE (progresses in order, never goes back)
        if (isAtivacoes) {
          if (ev === 'Iniciar Processo de Ativação' || ev === 'Definir url direto para a proposta') {
            if (fase === 'iniciando') fase = 'iniciando';
          } else if (ev === 'Iniciar Processo Simulação') {
            fase = 'simulacao';
          } else if (ev === 'Finalizar Processo Simulação') {
            if (['iniciando','simulacao'].includes(fase)) fase = 'simulacao_ok';
          } else if (ev === 'Iniciar Processo Triagem') {
            fase = 'triagem';
          } else if (ev === 'Finalizar Processo Triagem') {
            if (!['pv','pv_ok','credito_fase'].includes(fase)) fase = 'triagem_ok';
          } else if (ev === 'Iniciar Processo PV - Secretaria') {
            fase = 'pv';
          } else if (ev === 'Finalizar Processo PV - Secretaria') {
            fase = 'pv_ok';
          } else if (ev === 'Distribuir Comissão') {
            fase = 'pv';
          } else if (ev.includes('Enviado para o Crédito') || ev.includes('no Crédito')) {
            fase = 'credito_fase';
          }
        }
      }

      if (isAtivacoes && prospectNotFound) {
        status = 'cancelado';
      }
      if (isAtivacoes && creditStageException) {
        status = 'cancelado';
      }
      if (isAtivacoes && brokerMismatchException) {
        status = 'cancelado';
      }
      if (isAtivacoes && activeProposalException) {
        status = 'cancelado';
      }

      // Regra de negocio: envio para credito sempre conta como sucesso,
      // mesmo se houve erro/incorreto anteriormente no fluxo.
      if (isAtivacoes && sentToCredit) {
        status = 'credito';
      }

      // If has error and not explicitly finalized
      if (hasError && !['credito','cancelado','finalizado'].includes(status)) {
        status = 'erro';
      }

      // Sort events
      eventList.sort((a,b) => (a.timestamp||'').localeCompare(b.timestamp||''));

      // Detect stalled: still "em-andamento" with no update for 15+ minutes → treat as erro
      let stalledMinutes = 0;
      if (status === 'em-andamento' && lastTime) {
        const last = new Date(lastTime.replace(' ', 'T'));
        const now = new Date();
        stalledMinutes = Math.floor((now - last) / 60000);
        if (stalledMinutes >= 15) {
          status = 'erro';
          hasError = true;
        }
      }

      allCasos.push({ casoName, proposta, machine, user, status, fase, lastTime, hasError, isProspectNotFound: prospectNotFound, isCreditStageException: creditStageException, isBrokerMismatchException: brokerMismatchException, isActiveProposalException: activeProposalException, events: eventList, runKey, stalledMinutes });
    } // end casoName loop
    } // end runKey loop
  } // end dates loop

  // Sort by lastTime desc
  allCasos.sort((a,b) => b.lastTime.localeCompare(a.lastTime));
  filteredCasos = [...allCasos];
  renderCasosTable();
}

function filterCasos() {
  const q = document.getElementById('searchInput').value.toLowerCase();
  if (!q) {
    filteredCasos = [...allCasos];
  } else {
    filteredCasos = allCasos.filter(c =>
      c.casoName.toLowerCase().includes(q) ||
      c.proposta.toLowerCase().includes(q) ||
      c.machine.toLowerCase().includes(q) ||
      c.status.includes(q)
    );
  }
  renderCasosTable();
}

function renderCasosTable() {
  const el = document.getElementById('casosContent');
  document.getElementById('casosCount').textContent = `${filteredCasos.length} casos`;

  if (!filteredCasos.length) {
    el.innerHTML = '<div class="empty">Nenhum caso encontrado</div>';
    return;
  }

  el.innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Caso</th>
          <th>Proposta</th>
          <th>Máquina</th>
          <th>Usuário</th>
          <th>Status</th>
          <th>Último evento</th>
        </tr>
      </thead>
      <tbody>
        ${filteredCasos.map((c, i) => `
          <tr onclick="openDetail(${i})">
            <td><div class="caso-name" title="${c.casoName}">${c.casoName}</div></td>
            <td><div class="proposta-code">${c.proposta || '—'}</div></td>
            <td><div class="machine-tag">${c.machine}</div></td>
            <td><div class="machine-tag">${c.user}</div></td>
            <td>${statusBadge(c.status, c.stalledMinutes, c.fase)}</td>
            <td style="font-family:'Space Mono',monospace;font-size:10px;color:var(--muted)">${c.lastTime ? c.lastTime.slice(11,16) : '—'}</td>
          </tr>
        `).join('')}
      </tbody>
    </table>
  `;

  window.requestAnimationFrame(syncCasesHeightToMachines);
}

function faseLabel(fase) {
  const map = {
    'iniciando':    { icon: '⟳', label: 'Iniciando',   color: '#0099ff' },
    'simulacao':    { icon: '◎', label: 'Simulação',   color: '#a855f7' },
    'simulacao_ok': { icon: '◎', label: 'Simulação',   color: '#a855f7' },
    'triagem':      { icon: '◈', label: 'Triagem',     color: '#f59e0b' },
    'triagem_ok':   { icon: '◈', label: 'Triagem',     color: '#f59e0b' },
    'pv':           { icon: '◉', label: 'PV',          color: '#06b6d4' },
    'pv_ok':        { icon: '◉', label: 'PV',          color: '#06b6d4' },
    'credito_fase': { icon: '★', label: 'Finalizado',  color: '#00e5a0' },
    'conferencia':  { icon: '◌', label: 'Conferência', color: '#00b3ff' },
    'comparacao':   { icon: '⇄', label: 'Comparação',  color: '#f59e0b' },
    'documentos':   { icon: '⌁', label: 'Documentos',  color: '#a855f7' },
    'anexando':     { icon: '⬆', label: 'Anexando',    color: '#06b6d4' },
    'finalizado':   { icon: '✓', label: 'Concluído',   color: '#00e5a0' }
  };
  return map[fase] || { icon: '●', label: 'Iniciando', color: '#0099ff' };
}

function statusBadge(s, stalledMinutes, fase) {
  if (s === 'em-andamento' && fase) {
    const f = faseLabel(fase);
    return `<span class="status-badge em-andamento" style="color:${f.color};background:${f.color}18;border:1px solid ${f.color}33">${f.icon} ${f.label}</span>`;
  }
  if (s === 'erro') {
    return `<span class="status-badge erro">✗ Erro</span>`;
  }
  const map = {
    'finalizado': '✓ Finalizado',
    'credito':    '★ Finalizado',
    'cancelado':  '⊘ Cancelado',
  };
  return `<span class="status-badge ${s}">${map[s] || s}</span>`;
}

function updateKPIs() {
  if (!rawData || !dateFrom || !dateTo) return;
  const isAtivacoes = selectedProject === 'ativacoes';
  const dates = getSelectedDates();
  let totalRodagens = 0;
  dates.forEach(date => {
    const day = rawData[date] || {};
    totalRodagens += Object.keys(day).length;
  });

  const rodando = allCasos.filter(c => !['credito', 'finalizado', 'erro', 'cancelado'].includes(c.status)).length;
  const fins = allCasos.filter(c => c.status === 'credito' || c.status === 'finalizado').length;
  const erros = allCasos.filter(c => c.status === 'erro').length;
  const cancelados = allCasos.filter(c => c.status === 'cancelado').length;
  const fourthMetric = isAtivacoes ? cancelados : totalRodagens;

  document.getElementById('kpiRodagens').textContent   = rodando;
  document.getElementById('kpiFinalizados').textContent = fins;
  document.getElementById('kpiErros').textContent       = erros;
  document.getElementById('kpiCancelados').textContent  = fourthMetric;
  document.getElementById('kpiTravados') && (document.getElementById('kpiTravados').textContent = 0);
  document.getElementById('kpiTotal').textContent       = allCasos.length;
}

function renderMachines() {
  if (!rawData || !dateFrom || !dateTo) return;
  const dates = getSelectedDates();
  const machines = {};

  for (const date of dates) {
    const day = rawData[date] || {};
    for (const [runKey, run] of Object.entries(day)) {
      const m = run.Maquina || '?';
      if (!machines[m]) {
        machines[m] = {
          name: m,
          user: run.Usuario || '?',
          casos: 0,
          erros: 0,
          lastEvent: '',
          version: run.Versao || '',
          lastRunStart: run['Inicio Rodagem'] || ''
        };
      }

      const runStart = run['Inicio Rodagem'] || '';
      if (runStart && runStart >= machines[m].lastRunStart) {
        machines[m].lastRunStart = runStart;
        if (run.Versao) machines[m].version = run.Versao;
      }

      if (run.Usuario) machines[m].user = run.Usuario;
      if (!run.eventos) continue;
      for (const [casoName, evts] of Object.entries(run.eventos)) {
        if (casoName === 'Geral') continue;
        machines[m].casos++;
        for (const [, evt] of Object.entries(evts)) {
          if (typeof evt !== 'object') continue;
          if (evt.nivel === 'ERRO') machines[m].erros++;
          if (evt.timestamp && evt.timestamp > machines[m].lastEvent) machines[m].lastEvent = evt.timestamp;
        }
      }
    }
  }

  // Find the most recent event timestamp per machine
  const lastEventByMachine = {};
  Object.values(machines).forEach(m => { lastEventByMachine[m.name] = m.lastEvent; });

  const now = new Date();
  const ACTIVE_THRESHOLD_MS = 15 * 60 * 1000;

  // Only show machines whose last event was within 15 minutes
  const list = Object.values(machines)
    .filter(m => {
      const last = lastEventByMachine[m.name];
      if (!last) return false;
      return (now - new Date(last.replace(' ', 'T'))) < ACTIVE_THRESHOLD_MS;
    })
    .sort((a,b) => b.casos - a.casos);

  let latestVersion = '';
  list.forEach(machine => {
    if (!machine.version) return;
    if (!latestVersion || compareVersion(machine.version, latestVersion) > 0) {
      latestVersion = machine.version;
    }
  });

  const versionMetaEl = document.getElementById('machinesVersionMeta');
  versionMetaEl.textContent = latestVersion
    ? `Versão mais recente: ${latestVersion}`
    : 'Versão mais recente: —';

  document.getElementById('machinesCount').textContent = `${list.length} ativas`;

  const el = document.getElementById('machinesList');
  if (!list.length) { el.innerHTML = '<div class="empty">Nenhuma máquina ativa</div>'; return; }

  el.innerHTML = list.map(m => `
    <div class="machine-item">
      <div class="machine-indicator active"></div>
      <div class="machine-info">
        <div class="machine-name">${m.name}</div>
        <div class="machine-user">${m.user}</div>
        ${m.version
          ? `<div class="machine-version ${latestVersion && compareVersion(m.version, latestVersion) < 0 ? 'outdated' : 'current'}">v${m.version}${latestVersion && compareVersion(m.version, latestVersion) < 0 ? ' · desatualizada' : ''}</div>`
          : '<div class="machine-version unknown">versão não informada</div>'}
      </div>
      <div class="machine-stats">
        <div class="count">${m.casos}</div>
        <div class="label">casos${m.erros > 0 ? ` · <span style="color:var(--danger)">${m.erros} erros</span>` : ''}</div>
      </div>
    </div>
  `).join('');

  window.requestAnimationFrame(syncCasesHeightToMachines);
}

function renderRecentEvents() {
  const events = [];
  allCasos.slice(0, 20).forEach(c => {
    c.events.slice(-2).forEach(e => {
      events.push({ ...e, caso: c.casoName, machine: c.machine });
    });
  });
  events.sort((a,b) => (b.timestamp||'').localeCompare(a.timestamp||''));

  const el = document.getElementById('eventsList');
  if (!events.length) { el.innerHTML = '<div class="empty">Nenhum evento</div>'; return; }

  el.innerHTML = events.slice(0,30).map(e => `
    <div class="event-item">
      <div class="event-time">${(e.timestamp||'').slice(11,16)}</div>
      <div class="event-dot ${e.nivel}"></div>
      <div class="event-content">
        <div class="event-title">${e.evento || e.key}</div>
        <div class="event-meta">${e.caso} · ${e.machine}</div>
      </div>
    </div>
  `).join('');
}

function renderRecentErrors() {
  const isAtivacoes = selectedProject === 'ativacoes';
  const erros = [];
  allCasos.forEach(c => {
    if (isAtivacoes && (c.isProspectNotFound || c.isCreditStageException || c.isBrokerMismatchException)) return;
    c.events.filter(e => e.nivel === 'ERRO').forEach(e => {
      if (isAtivacoes && (isProspectNotFoundEvent(e.key, e) || isCreditStageException(e.key, e) || isBrokerMismatchException(e.key, e))) return;
      erros.push({ ...e, caso: c.casoName, machine: c.machine, casoRef: c });
    });
  });
  erros.sort((a,b) => (b.timestamp||'').localeCompare(a.timestamp||''));

  document.getElementById('errorsCount').textContent = `${erros.length}`;

  const el = document.getElementById('errorsList');
  if (!erros.length) { el.innerHTML = '<div class="empty">Sem erros no período ✓</div>'; return; }

  el.innerHTML = erros.slice(0,20).map((e, i) => `
    <div class="error-item" onclick="openDetailFromError(${i})">
      <div class="error-header">
        <div class="error-exception" title="${e.exception||''}">${e.exception || e.evento}</div>
        <div style="font-family:'Space Mono',monospace;font-size:10px;color:var(--muted);flex-shrink:0">${(e.timestamp||'').slice(11,16)}</div>
      </div>
      <div class="error-caso">${e.caso} · ${e.machine}</div>
    </div>
  `).join('');

  // Store for click handler
  window._errorsList = erros;
}

function openDetailFromError(i) {
  const e = window._errorsList[i];
  openDetailFromCaso(e.casoRef);
}

function openDetail(i) {
  openDetailFromCaso(filteredCasos[i]);
}

function openDetailFromCaso(caso) {
  document.getElementById('modalTitle').textContent = caso.casoName;
  document.getElementById('detailModal').classList.add('open');

  const body = document.getElementById('modalBody');
  body.innerHTML = `
    <div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap">
      ${statusBadge(caso.status, caso.stalledMinutes, caso.fase)}
      <span style="font-size:11px;color:var(--muted);padding:3px 8px;background:var(--surface2);border-radius:4px;font-family:'Space Mono',monospace">${caso.machine}</span>
      ${caso.proposta ? `<span style="font-size:11px;color:var(--muted);padding:3px 8px;background:var(--surface2);border-radius:4px;font-family:'Space Mono',monospace">${caso.proposta}</span>` : ''}
    </div>
    ${caso.status === 'travado' ? `
      <div style="background:rgba(255,120,0,0.08);border:1px solid rgba(255,120,0,0.3);border-radius:8px;padding:12px 16px;margin-bottom:16px;display:flex;gap:10px;align-items:flex-start">
        <span style="font-size:18px">⚠️</span>
        <div>
          <div style="color:#ff7800;font-weight:600;font-size:13px;margin-bottom:4px">Máquina provavelmente desligada</div>
          <div style="color:var(--muted);font-size:12px;line-height:1.5">
            Nenhuma atualização há <strong style="color:#ff7800">${caso.stalledMinutes} minutos</strong>.
            O caso estava <em>em andamento</em> e parou de responder — verifique se a máquina
            <strong style="color:var(--text)">${caso.machine}</strong> está online.
          </div>
        </div>
      </div>
    ` : ''}
    ${caso.events.map(e => `
      <div class="timeline-item">
        <div class="timeline-dot ${e.nivel}"></div>
        <div class="timeline-content">
          <div class="timeline-event">${e.evento || e.key}</div>
          <div class="timeline-time">${e.timestamp || ''}</div>
          ${e['Link Proposta'] ? `<div class="timeline-extra"><a href="${e['Link Proposta']}" target="_blank" style="color:var(--accent2)">🔗 Abrir Proposta no CRM</a></div>` : ''}
          ${e.exception ? `<div class="timeline-extra" style="color:var(--danger)">${e.exception}</div>` : ''}
          ${e.stacktrace ? `<div class="stacktrace">${e.stacktrace}</div>` : ''}
        </div>
      </div>
    `).join('')}
  `;
}

function closeModal(e) {
  if (e.target === document.getElementById('detailModal')) closeDetailModal();
}

function closeDetailModal() {
  document.getElementById('detailModal').classList.remove('open');
}

function updateLastUpdate() {
  const now = new Date().toLocaleTimeString('pt-BR');
  const config = getProjectConfig();
  document.getElementById('lastUpdate').textContent = `${config.title} • atualizado às ${now}`;
}

function formatDateShort(d) {
  const parts = d.split('-');
  if (parts.length !== 3) return d;
  const today = new Date().toISOString().slice(0,10);
  const yesterday = new Date(Date.now()-86400000).toISOString().slice(0,10);
  if (d === today) return 'Hoje';
  if (d === yesterday) return 'Ontem';
  return `${parts[2]}/${parts[1]}`;
}

// ============================================================
// LOAD DEMO DATA (embedded JSON for demonstration)
// ============================================================
// If user has JSON file, they can load via config modal.
// The dashboard is ready to connect to Firebase or load a file.
