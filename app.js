/* global cytoscape, cytoscapeDagre, cytoscapePopper, tippy */
/**
 * Visualizador de linaje de datos (nivel tablas / nivel campos).
 * Consume json/datos-objetivo.json y json/datos-objetivo-campos.json
 * cumpliendo las condiciones descritas en Readme.md (# Renderizar visuales).
 */
(() => {
  cytoscape.use(cytoscapeDagre);
  if (typeof cytoscapePopper === 'function') {
    if (!cytoscape.prototype.popper) {
      cytoscape.use(cytoscapePopper);
    }
  } else {
    console.warn('[viz] cytoscape-popper no disponible; se deshabilitan tooltips flotantes.');
  }

  const state = {
    viewMode: 'tables',
    zone: 'all',
    table: 'all',
    showAll: false,
    hideLz: false,
    layout: 'columns',
    // NUEVO: no renderizar hasta que el usuario elija zona/tabla o "mostrar todas"
    readyToRender: false
  };

  const refs = {
    cyContainer: document.getElementById('cy'),
    overlay: document.querySelector('.zone-overlay'),
    details: document.getElementById('details-content'),
    status: document.getElementById('status-message'),
    viewModeSelect: document.getElementById('view-mode'),
    zoneSelect: document.getElementById('zone-select'),
    tableSelect: document.getElementById('table-select'),
    showAllCheckbox: document.getElementById('show-all-results'),
    hideLzCheckbox: document.getElementById('hide-lz'),
    layoutSelect: document.getElementById('layout-select'),
    resetViewBtn: document.getElementById('reset-view-btn'),
    reloadBtn: document.getElementById('reload-btn')
  };

  const LAYOUTS = {
    /* columns: null,
    dagre: { name: 'dagre', rankDir: 'LR', rankSep: 220, nodeSep: 90, edgeSep: 40, fit: true, padding: 50 },
    breadthfirst: { name: 'breadthfirst', directed: true, circle: false, spacingFactor: 1.2, fit: true, padding: 60 },
    cose: { name: 'cose', fit: true, padding: 80, randomize: false, animate: false, nodeOverlap: 10 }
 */
   columns: null,
   dagre: {
     name: 'dagre',
     rankDir: 'LR',
     rankSep: 220,
     nodeSep: 90,
     edgeSep: 40,
     fit: true,
     padding: 50
   }

  };

  const RESERVED_FIELD_WORDS = new Set(
    [
      'abort','add','add_months','adddate','aggregate','all','alter','analyze','analytic','and','any',
      'appx_median','archive','array','as','asc','authorization','avg','between','bigint','binary',
      'boolean','both','break','bucket','buckets','by','cache','case','cascade','cast','change','char',
      'class','close','cluster','clustered','coalesce','collection','column','columns','comment','compact',
      'compactions','compute','conf','continue','count','create','cross','current','current_date',
      'current_timestamp','cursor','data','database','databases','date','date_add','date_sub','datediff',
      'datetime','day','dayname','dayofmonth','dayofweek','dayofyear','dbproperties','decimal','deferred',
      'delimited','dependency','desc','describe','directories','directory','disable','distinct','distribute',
      'div','double','drop','else','enable','end','escape','escaped','except','exchange','exclusive','exists',
      'explain','extract','extended','external','false','fetch','field','fields','file','fileformat','files',
      'finalize','first','float','floor','following','for','format','from','from_timestamp','from_unixtime',
      'from_utc_timestamp','full','function','functions','grant','group','having','hold','hour','if','ifnull',
      'import','in','incremental','init','initially','inner','inputdriver','inputformat','inpath','insert',
      'int','integer','intersect','interval','into','is','isnull','item','join','key','keys','last','last_day',
      'lateral','left','length','like','limit','lines','load','local','location','lock','locks','log','lower',
      'macro','map','mapjoin','materialized','max','merge','metadata','min','minus','minute','more',
      'months_between','none','nonstrict','not','now','null','nulls','nvl','offset','on','or','order',
      'outer','outputdriver','outputformat','over','overwrite','parquet','partition','partitioned',
      'partitions','percent','power','preceding','primary','procedure','protection','purge','range','read',
      'readonly','real','rebuild','recordreader','recordwriter','recover','regexp_count','regexp_extract',
      'regexp_instr','regexp_like','regexp_replace','regexp_substr','reload','rename','replace','replication',
      'repair','restrict','revoke','rewrite','right','rlike','role','roles','rollback','round','row','rows',
      'schema','schemas','second','select','semi','sequencefile','serde','serdeproperties','server','set',
      'sets','shared','show','skewed','smallint','sort','sqrt','ssl','statistics','stored','streamtable',
      'str_to_timestamp','string','struct','substr','sum','table','tables','tablesample','tblproperties',
      'temporary','terminated','textfile','then','timestamp','timestamp_micros','timestamp_millis',
      'timestamp_seconds','tinyint','to','to_date','to_timestamp','to_unix_timestamp','touch','transform',
      'transaction','transactions','trim','true','trunc','truncate','typeof','unarchive','unbounded','union',
      'unique','unix_timestamp','unlock','unsigned','update','upper','use','using','validate','value','values',
      'variance','varchar','view','views','wait','when','where','while','with','write'
    ].map(w => w.toLowerCase())
  );

  const TABLE_PATTERNS = [
    /^s_bani[^.]*\.[^.]+$/i,
    /^proceso[^.]*\.[^.]+$/i,
    /^proceso\.[^.]+$/i,
    /^resultados[^.]*\.[^.]+$/i
  ];

  let rawData = { tables: [], fields: [] };
  let filteredData = { tables: [], fields: [] };
  let cy;
  let currentZones = new Map();
  let needsFilterSync = true;
  let edgeTooltips = [];

  function setStatus(message) {
    refs.status.textContent = message;
  }

  function updateDetails(content) {
    if (!content) {
      refs.details.innerHTML = '<p>Selecciona una tabla, campo o relacion para ver mas informacion.</p>';
    } else {
      refs.details.innerHTML = content;
    }
  }

  function destroyGraph() {
    edgeTooltips.forEach(t => t.destroy?.());
    edgeTooltips = [];
    if (cy) {
      cy.destroy();
      cy = null;
    }
    refs.cyContainer.style.width = '';
    refs.cyContainer.style.height = '';
    if (refs.overlay) refs.overlay.style.width = '';
  }

  function extractZone(tableName) {
    if (!tableName) return null;
    if (tableName.startsWith('lz.estatico')) return 'lz.estatico';
    if (tableName.startsWith('lz.funcion')) return 'lz.funcion';
    const idx = tableName.indexOf('.');
    if (idx === -1) return tableName;
    return tableName.slice(0, idx);
  }

  function isResultadosTable(tableName) {
    return typeof tableName === 'string' && tableName.startsWith('resultados');
  }

  // --- Helpers inicio/recursión ---
  function startsWithAny(name, prefixes) {
    if (!name) return false;
    const n = name.toLowerCase();
    return prefixes.some(p => n.startsWith(p));
  }
  function shouldRecurseFrom(tableName) {
    return startsWithAny(tableName, ['resultados', 'proceso']);
  }
  function isStartableTable(tableName) {
    return startsWithAny(tableName, ['resultados', 'proceso']);
  }
  // --------------------------------

  function zoneFromTable(tableName) {
    const zone = extractZone(tableName) || tableName || 'desconocido';
    return { zone, type: zoneType(zone) };
  }

  function zoneType(zone) {
    if (!zone) return 'otro';
    if (zone.startsWith('resultados')) return 'resultados';
    if (zone.startsWith('proceso')) return 'proceso';
    if (zone.startsWith('s_bani')) return 's_bani';
    if (zone.startsWith('lz.estatico')) return 'lz-estatico';
    if (zone.startsWith('lz.funcion')) return 'lz-funcion';
    return 'otro';
  }

  function zoneClassFromType(type) {
    switch (type) {
      case 'resultados': return 'zone-resultados';
      case 'proceso': return 'zone-proceso';
      case 's_bani': return 'zone-s_bani';
      case 'lz-estatico': return 'zone-lz-estatico';
      case 'lz-funcion': return 'zone-lz-funcion';
      default: return 'zone-otro';
    }
  }

  function zoneColumnKey(type) {
    switch (type) {
      case 'resultados': return 'right';
      case 's_bani': return 'left';
      case 'lz-estatico':
      case 'lz-funcion':
      case 'proceso':
      default: return 'center';
    }
  }

  function zoneTypeOrder(type) {
    switch (type) {
      case 'resultados': return 0;
      case 'proceso': return 1;
      case 's_bani': return 2;
      case 'lz-estatico': return 3;
      case 'lz-funcion': return 4;
      default: return 5;
    }
  }

  function isLz(tableName) {
    return typeof tableName === 'string' &&
      (tableName.startsWith('lz.estatico') || tableName.startsWith('lz.funcion'));
  }

  function truncate(text, max) {
    if (!text) return '';
    if (text.length <= max) return text;
    const end = Math.max(0, max - 3);
    return `${text.slice(0, end)}...`;
  }

  function escapeHtml(value) {
    return (value || '').replace(/[&<>\"']/g, c => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[c]));
  }

  function firstOrNone(arr) {
    if (!arr || arr.length === 0) return 'copy';
    return arr[0];
  }

  init();

  function init() {
    registerEvents();
    loadData();
    
    Array.from(refs.layoutSelect.options).forEach(opt => {
      if (!['columns', 'dagre'].includes(opt.value)) opt.remove();
    });
  }

  function registerEvents() {
    refs.viewModeSelect.addEventListener('change', () => {
      state.viewMode = refs.viewModeSelect.value;
      // reset selección
      state.zone = 'all';
      state.table = 'all';
      state.showAll = false;
      state.readyToRender = false; // NO render hasta nueva selección
      refs.showAllCheckbox.checked = false;
      needsFilterSync = true;
      renderGraph();
    });

    refs.zoneSelect.addEventListener('change', () => {
      state.zone = refs.zoneSelect.value;
      state.showAll = false;
      refs.showAllCheckbox.checked = false;
      state.readyToRender = (state.zone !== 'all'); // si hay zona, ya podemos renderizar
      needsFilterSync = true;
      renderGraph();
    });

    refs.tableSelect.addEventListener('change', () => {
      state.table = refs.tableSelect.value;
      state.readyToRender = (state.table !== 'all') || (state.zone !== 'all');
      renderGraph();
    });

    refs.showAllCheckbox.addEventListener('change', () => {
      state.showAll = refs.showAllCheckbox.checked;
      if (state.showAll) state.table = 'all';
      // "Mostrar todas" se habilita cuando ya hay zona -> podemos renderizar
      state.readyToRender = state.showAll || (state.table !== 'all') || (state.zone !== 'all');
      needsFilterSync = true;
      renderGraph();
    });

    refs.hideLzCheckbox.addEventListener('change', () => {
      // no altera readyToRender
      state.hideLz = refs.hideLzCheckbox.checked;
      needsFilterSync = true;
      renderGraph();
    });

    refs.layoutSelect.addEventListener('change', () => {
      state.layout = refs.layoutSelect.value;
      /* applyLayout(); */
      
      // Overlay y tema
      const isColumns = (state.layout === 'columns');
      setOverlayVisible(isColumns);       // overlay visible sólo en columnas
      setFreeflowTheme(!isColumns);       // fondo verde sólo en dagre
      applyLayout()
    });

    refs.resetViewBtn.addEventListener('click', () => {
      if (!cy) return;
      if (state.layout === 'columns') {
        applyZoneLayout();
      } else {
        cy.fit(); cy.center();
      }
    });

    refs.reloadBtn.addEventListener('click', () => {
      // no modificamos readyToRender aquí
      loadData(true);
    });

    window.addEventListener('resize', () => {
      if (!cy) return;
      if (state.layout === 'columns') {
        applyZoneLayout();
      } else {
        cy.resize();
      }
    });
  }

  async function loadData(fromReload = false) {
    setStatus('Cargando datos...');
    updateDetails(null);

    try {
      const [tables, fields] = await Promise.all([
        fetchJson('../json/datos-objetivo.json'),
        fetchJson('../json/datos-objetivo-campos.json')
      ]);
      rawData = { tables, fields };
      filteredData = {
        tables: sanitizeRecords(tables, 'tables'),
        fields: sanitizeRecords(fields, 'fields')
      };
      currentZones = new Map();
      needsFilterSync = true;
      refs.layoutSelect.value = state.layout;

      // Poblar combos sin dibujar el grafo
      const baseRecords = getFilteredRecords({ skipZone: true });
      syncFilters(baseRecords);

      setOverlayVisible(state.layout === 'columns');
      setFreeflowTheme(state.layout !== 'columns');

      // Mensaje de espera de selección
      state.readyToRender = false;
      destroyGraph();
      setStatus(fromReload ? 'Datos recargados. Selecciona una zona o una tabla de inicio.' :
                             'Datos cargados. Selecciona una zona o una tabla de inicio.');
      updateDetails(null);

      // IMPORTANTE: NO llamar a renderGraph() aquí.
    } catch (err) {
      console.error('Error cargando datos', err);
      destroyGraph();
      setStatus(`Error al cargar datos: ${err.message || err}`);
      updateDetails(renderErrorDetails(err));
    }
  }

  function fetchJson(url) {
    return fetch(url, { cache: 'no-store' }).then(response => {
      if (!response.ok) throw new Error(`HTTP ${response.status} al obtener ${url}`);
      return response.json();
    });
  }

  function sanitizeRecords(records, level) {
    const filtered = [];
    for (const rec of records) {
      const tablaDestino = normalizeTable(rec.tabla_destino);
      const tablaOrigen = normalizeTable(rec.tabla_origen);
      const campoDestino = normalizeField(rec.campo_destino);
      const campoOrigen = normalizeField(rec.campo_origen);

      if (!isAllowedTable(tablaDestino) || !isAllowedTable(tablaOrigen)) continue;
      if (!campoDestino) continue; // campo destino no puede ser nulo

      if (level === 'fields') {
        if (campoDestino === '*') continue; // nivel tabla se maneja en datos-objetivo.json
        if (!isValidField(campoDestino)) continue;
        if (!campoOrigen || !isValidField(campoOrigen)) continue;
      } else {
        if (!isValidField(campoDestino)) continue;
        if (campoOrigen && !isValidField(campoOrigen)) continue;
      }

      filtered.push({
        ...rec,
        tabla_destino: tablaDestino,
        tabla_origen: tablaOrigen,
        campo_destino: campoDestino,
        campo_origen: campoOrigen
      });
    }
    return filtered;
  }

  function normalizeTable(value) {
    return typeof value === 'string' ? value.trim().toLowerCase() : '';
  }

  function normalizeField(value) {
    if (value === null || value === undefined) return null;
    if (typeof value === 'string') return value.trim().toLowerCase();
    return String(value).trim().toLowerCase();
  }

  function isAllowedTable(tableName) {
    if (!tableName) return false;
    if (tableName.startsWith('lz.estatico') || tableName.startsWith('lz.funcion')) return true;
    return TABLE_PATTERNS.some(rx => rx.test(tableName));
  }

  function isValidField(fieldName) {
    const VALID_FIELD_REGEX = /^[a-z0-9]+(?:_[a-z0-9]+)*$/;
    if (!fieldName) return false;
    if (fieldName === '*') return true; // '*' permitido (nivel tabla)
    if (RESERVED_FIELD_WORDS.has(fieldName)) return false;
    if (/^-?\d+(\.\d+)?$/.test(fieldName)) return false; // descarta números puros
    
    if (fieldName.includes('*')) return false;           // descarta otros usos de '*'
    // campo: empieza con [a-z0-9], puede tener _ en medio, y termina en [a-z0-9]
    if (!/^[a-z0-0-9_]*[a-z0-9]?$/.test(fieldName)) {
      return false;
    }
    if (!VALID_FIELD_REGEX.test(fieldName)) return false;          // snake_case válido

    return true;
  }

  function computeZoneInfo(records) {
    const map = new Map();
    (records || []).forEach(rec => {
      const destination = rec.tabla_destino;
      const zoneName = extractZone(destination);
      if (!zoneName) return;

      if (!map.has(zoneName)) {
        map.set(zoneName, {
          zone: zoneName,
          type: zoneType(zoneName),
          startTables: new Set(),  // tablas de inicio: resultados.* / proceso.*
          destinations: new Set()
        });
      }
      const info = map.get(zoneName);
      info.destinations.add(destination);
      if (isStartableTable(destination)) {
        info.startTables.add(destination);
      }
    });
    return map;
  }

  function populateZoneSelect(zonesMap) {
    const entries = Array.from(zonesMap.values());
    entries.sort((a, b) => {
      const order = zoneTypeOrder(a.type) - zoneTypeOrder(b.type);
      if (order !== 0) return order;
      return a.zone.localeCompare(b.zone);
    });

    refs.zoneSelect.innerHTML = '';
    const allOption = document.createElement('option');
    allOption.value = 'all';
    allOption.textContent = 'Todas las zonas (inicio: resultados/proceso)';
    allOption.title = 'Todas las zonas (inicio: resultados/proceso)';
    refs.zoneSelect.appendChild(allOption);

    entries.forEach(entry => {
      const option = document.createElement('option');
      option.value = entry.zone;
      option.textContent = `${entry.zone} (${entry.startTables.size || entry.destinations.size} tablas)`;
      option.title = entry.zone;
      refs.zoneSelect.appendChild(option);
    });

    let selectedZone;
    if (state.zone !== 'all' && zonesMap.has(state.zone)) {
      selectedZone = state.zone;
    } else if (state.zone === 'all') {
      selectedZone = 'all';
    } else if (entries.length > 0) {
      selectedZone = entries[0].zone;
    } else {
      selectedZone = 'all';
    }
    state.zone = selectedZone;
    refs.zoneSelect.value = selectedZone;
    refs.zoneSelect.title = selectedZone === 'all'
      ? 'Todas las zonas (inicio: resultados/proceso)'
      : selectedZone;
  }

  function updateTableSelect(zonesMap) {
    if (state.zone === 'all' || !zonesMap.has(state.zone)) {
      refs.tableSelect.disabled = true;
      refs.tableSelect.innerHTML = '<option value="all">Selecciona una zona</option>';
      refs.tableSelect.title = 'Selecciona una zona';
      state.table = 'all';
      refs.showAllCheckbox.checked = false;
      refs.showAllCheckbox.disabled = true;
      state.showAll = false;
      return;
    }

    const info = zonesMap.get(state.zone);
    const tables = Array.from(info.startTables).sort();
    const hasTables = tables.length > 0;

    refs.showAllCheckbox.disabled = !hasTables;
    if (!hasTables) {
      refs.showAllCheckbox.checked = false;
      state.showAll = false;
    }

    refs.tableSelect.innerHTML = '';
    if (!hasTables) {
      refs.tableSelect.disabled = true;
      refs.tableSelect.innerHTML = '<option value="all">Sin tablas de inicio</option>';
      refs.tableSelect.title = 'Sin tablas de inicio';
      state.table = 'all';
    } else {
      tables.forEach(table => {
        const option = document.createElement('option');
        option.value = table;
        option.textContent = table;
        option.title = table;
        refs.tableSelect.appendChild(option);
      });
      if (!tables.includes(state.table)) {
        state.table = tables[0];
      }
      refs.tableSelect.disabled = state.showAll;
      refs.tableSelect.title = state.showAll ? 'Mostrando todas las tablas de la zona' : state.table;
    }

    refs.tableSelect.value = state.table;
    if (state.table === 'all') {
      const firstOption = refs.tableSelect.options[0];
      refs.tableSelect.title = firstOption ? firstOption.textContent : 'Tabla destino';
    } else if (!state.showAll) {
      refs.tableSelect.title = state.table;
    }
  }

  function syncFilters(records) {
    const zonesMap = computeZoneInfo(records);
    const zoneNames = Array.from(zonesMap.keys()).sort((a, b) => a.localeCompare(b));
    if (state.zone !== 'all' && !zonesMap.has(state.zone)) {
      state.zone = zoneNames.length ? zoneNames[0] : 'all';
    }

    if (state.zone === 'all') {
      state.table = 'all';
      state.showAll = false;
    } else {
      const zoneInfo = zonesMap.get(state.zone);
      const tables = zoneInfo ? Array.from(zoneInfo.startTables).sort() : [];
      if (state.showAll) {
        state.table = 'all';
      } else if (tables.length === 0) {
        state.table = 'all';
      } else if (!tables.includes(state.table)) {
        state.table = tables[0];
      }
    }

    populateZoneSelect(zonesMap);
    updateTableSelect(zonesMap);
    refs.showAllCheckbox.checked = state.showAll;
    currentZones = new Map(zonesMap);
    needsFilterSync = false;
  }

  function renderGraph() {
    if (!filteredData) return;

    // Base para combos (sin filtros de zona/tabla; respeta hideLz)
    const baseRecords = getFilteredRecords({ skipZone: true });
    if (needsFilterSync) {
      syncFilters(baseRecords);
    }

    // ⛔️ Bloquear render si aún no hay selección del usuario
    const noSelection = (state.zone === 'all' && state.table === 'all' && !state.showAll);
    if (!state.readyToRender || noSelection) {
      destroyGraph();
      setStatus('Selecciona una zona o una tabla de inicio para renderizar.');
      updateDetails(null);
      return;
    }

    const records = getFilteredRecords();
    const total = baseRecords.length;

    if (records.length === 0) {
      destroyGraph();
      setStatus(`Sin relaciones para los filtros seleccionados. (${total} registros disponibles en total)`);
      updateDetails(null);
      return;
    }

    const elements = state.viewMode === 'tables'
      ? buildTableElements(records)
      : buildFieldElements(records);

    drawGraph(elements);
    setStatus(`Mostrando ${records.length} relaciones (${state.viewMode === 'tables' ? 'nivel tabla' : 'nivel campo'}) filtradas de ${total}.`);
  }

  // Cierre upstream con restricción: solo recurre por resultados*/proceso*
  function computeUpstreamClosureConstrained(startTable) {
    const source = state.viewMode === 'tables' ? filteredData.tables : filteredData.fields;

    // Índice: destino -> [relaciones entrantes]
    const incomingByDest = new Map();
    for (const r of source) {
      if (state.hideLz && isLz(r.tabla_origen)) continue; // respeta ocultar LZ
      if (!incomingByDest.has(r.tabla_destino)) incomingByDest.set(r.tabla_destino, []);
      incomingByDest.get(r.tabla_destino).push(r);
    }

    const closure = new Set([startTable]); // tablas a incluir en el grafo
    const stack = [startTable];

    while (stack.length) {
      const dest = stack.pop();
      const incoming = incomingByDest.get(dest) || [];
      for (const edge of incoming) {
        const origin = edge.tabla_origen;
        if (!origin) continue;

        if (!closure.has(origin)) {
          closure.add(origin); // incluir siempre el origen como hoja o intermediaria
          if (shouldRecurseFrom(origin)) {
            stack.push(origin); // seguir recursión solo si es resultados*/proceso*
          }
        }
      }
    }
    return closure;
  }

  function getFilteredRecords(options = {}) {
    const { skipZone = false } = options;
    const source = state.viewMode === 'tables' ? filteredData.tables : filteredData.fields;

    // 1) Base para combos (sin filtros salvo ocultar LZ)
    if (skipZone) {
      return source.filter(rec => {
        if (state.hideLz && isLz(rec.tabla_origen)) return false;
        return true;
      });
    }

    // 2) Si hay tabla concreta elegida (resultados.* o proceso.*): aplicar cierre upstream
    if (state.table !== 'all') {
      const upstreamSet = computeUpstreamClosureConstrained(state.table);
      return source.filter(rec => {
        if (state.hideLz && isLz(rec.tabla_origen)) return false;
        return upstreamSet.has(rec.tabla_destino);
      });
    }

    // 3) Sin tabla concreta: comportamiento general usando "iniciables" (resultados/proceso)
    return source.filter(rec => {
      if (state.hideLz && isLz(rec.tabla_origen)) return false;

      const zoneName = extractZone(rec.tabla_destino);
      if (state.zone !== 'all' && zoneName !== state.zone) return false;

      if (state.zone === 'all') {
        if (state.table !== 'all') return rec.tabla_destino === state.table;
        return isStartableTable(rec.tabla_destino); // resultados/proceso
      }

      if (state.showAll) {
        return isStartableTable(rec.tabla_destino); // todas las iniciables en la zona
      }

      if (state.table !== 'all') {
        return rec.tabla_destino === state.table;
      }

      return isStartableTable(rec.tabla_destino);
    });
  }

  function setOverlayVisible(visible) {
    if (!refs.overlay) return;
    refs.overlay.style.display = visible ? 'grid' : 'none';
  }

  function setFreeflowTheme(isFreeFlow) {
    // Fondo del contenedor del grafo (#cy) transparente para que se vea el fondo de graph-wrapper
    if (refs.cyContainer) {
      refs.cyContainer.style.backgroundColor = isFreeFlow ? 'transparent' : '#ffffff';
    }
    // Ponemos/quitamos una clase en el contenedor para pintar el fondo verde
    const wrapper = document.querySelector('.graph-wrapper');
    if (wrapper) {
      wrapper.classList.toggle('freeflow-bg', isFreeFlow);
    }
  }

  function buildTableElements(records) {
    const nodesMap = new Map();
    const edgesMap = new Map();

    for (const rec of records) {
      const sourceTable = rec.tabla_origen;
      const targetTable = rec.tabla_destino;
      if (!sourceTable || !targetTable) continue;

      const sourceNode = ensureTableNode(nodesMap, sourceTable);
      const targetNode = ensureTableNode(nodesMap, targetTable);

      if (rec.campo_origen && rec.campo_origen !== '*') {
        if (!sourceNode.data.columns.includes(rec.campo_origen)) {
          sourceNode.data.columns.push(rec.campo_origen);
        }
      }
      if (rec.campo_destino) {
        const destinoCol = rec.campo_destino === '*' ? '(*)' : rec.campo_destino;
        if (!targetNode.data.columns.includes(destinoCol)) {
          targetNode.data.columns.push(destinoCol);
        }
      }

      const edgeKey = `${sourceNode.data.id}->${targetNode.data.id}`;
      if (!edgesMap.has(edgeKey)) {
        edgesMap.set(edgeKey, {
          data: {
            id: edgeKey,
            source: sourceNode.data.id,
            target: targetNode.data.id,
            sourceTable,
            targetTable,
            nivel: 'tabla',
            transformaciones: new Set(),
            recomendaciones: new Set(),
            consultas: []
          },
          classes: new Set(['table-edge'])
        });
        if (isLz(sourceTable)) { edgesMap.get(edgeKey).classes.add('lz-edge'); }
      }
      const edge = edgesMap.get(edgeKey);
      if (rec.transformacion_aplicada) { edge.data.transformaciones.add(rec.transformacion_aplicada.trim()); }
      if (rec.recomendaciones) { edge.data.recomendaciones.add(rec.recomendaciones.trim()); }
      //if (rec.consulta) { edge.data.consultas.push(truncate(rec.consulta.trim(), 260)); }
      if (rec.consulta) {edge.data.consultas.push(rec.consulta.trim()); // ← sin truncar
      }
    }

    const edges = Array.from(edgesMap.values()).map(edge => {
      edge.data.transformaciones = Array.from(edge.data.transformaciones);
      edge.data.recomendaciones = Array.from(edge.data.recomendaciones);
      edge.data.consultas = edge.data.consultas.slice(0, 3);
      return { data: edge.data, classes: Array.from(edge.classes).join(' ') };
    });

    return { nodes: Array.from(nodesMap.values()), edges };
  }

  function buildFieldElements(records) {
    const nodesMap = new Map();
    const columnMap = new Map();
    const edgesMap = new Map();

    for (const rec of records) {
      const sourceTable = rec.tabla_origen;
      const targetTable = rec.tabla_destino;
      const sourceField = rec.campo_origen;
      const targetField = rec.campo_destino;
      if (!sourceTable || !targetTable || !sourceField || !targetField) continue;

      const sourceTableNode = ensureTableNode(nodesMap, sourceTable);
      const targetTableNode = ensureTableNode(nodesMap, targetTable);
      const sourceColumnNode = ensureColumnNode(columnMap, sourceField, sourceTableNode);
      const targetColumnNode = ensureColumnNode(columnMap, targetField, targetTableNode);

      const edgeKey = `${sourceColumnNode.data.id}->${targetColumnNode.data.id}`;
      if (!edgesMap.has(edgeKey)) {
        edgesMap.set(edgeKey, {
          data: {
            id: edgeKey,
            source: sourceColumnNode.data.id,
            target: targetColumnNode.data.id,
            sourceTable,
            targetTable,
            sourceField,
            targetField,
            nivel: 'campo',
            transformaciones: new Set(),
            recomendaciones: new Set(),
            consultas: []
          },
          classes: new Set()
        });
        if (isLz(sourceTable)) { edgesMap.get(edgeKey).classes.add('lz-edge'); }
      }
      const edge = edgesMap.get(edgeKey);
      if (rec.transformacion_aplicada) { edge.data.transformaciones.add(rec.transformacion_aplicada.trim()); }
      if (rec.recomendaciones) { edge.data.recomendaciones.add(rec.recomendaciones.trim()); }
      //if (rec.consulta) { edge.data.consultas.push(truncate(rec.consulta.trim(), 220)); }
      if (rec.consulta) {edge.data.consultas.push(rec.consulta.trim()); // ← sin truncar
      }

    }

    const nodes = [
      ...Array.from(nodesMap.values()),
      ...Array.from(columnMap.values())
    ];
    const edges = Array.from(edgesMap.values()).map(edge => {
      edge.data.transformaciones = Array.from(edge.data.transformaciones);
      edge.data.recomendaciones = Array.from(edge.data.recomendaciones);
      edge.data.consultas = edge.data.consultas.slice(0, 3);
      return { data: edge.data, classes: Array.from(edge.classes).join(' ') };
    });

    return { nodes, edges };
  }

  function ensureTableNode(map, tableName) {
    const id = `table:${tableName}`;
    if (map.has(id)) return map.get(id);
    const zoneInfo = zoneFromTable(tableName);
    const node = {
      data: {
        id,
        label: tableName,
        table: tableName,
        type: 'table',
        zone: zoneInfo.zone,
        zoneType: zoneInfo.type,
        columns: []
      },
      classes: `table-node ${zoneClassFromType(zoneInfo.type)}`
    };
    map.set(id, node);
    return node;
  }

  function packFieldsWithinParents() {
    if (!cy) return;

    // ¡Alinea con los valores de applyZoneLayout!
    const headerHeight = 46;
    const rowHeight    = 32;
    const fieldGap     = 12;

    cy.startBatch();

    // group: tabla -> [campos]
    const byTable = new Map();
    cy.nodes('[type = "column"]').forEach(col => {
      const t = col.data('table');
      if (!byTable.has(t)) byTable.set(t, []);
      byTable.get(t).push(col);
    });

    byTable.forEach((cols, tableName) => {
      const tableNode = cy.$id(`table:${tableName}`);
      if (!tableNode.nonempty()) return;

      // Ordena campos por etiqueta para estabilidad
      cols.sort((a, b) => a.data('label').localeCompare(b.data('label')));

      // Centro de la tabla (lo dejó dagre)
      const center = tableNode.position();
      const n = cols.length;

      // Altura total “útil” para calcular el primer y
      // header + gap + n*row + (n-1)*gap + gap final
      const totalFieldsHeight = headerHeight + fieldGap + n * rowHeight + Math.max(0, n - 1) * fieldGap + fieldGap;

      // primer campo al centro superior del bloque de campos
      let currentY = center.y - (totalFieldsHeight / 2) + headerHeight + fieldGap + rowHeight / 2;

      cols.forEach(colNode => {
        colNode.position({ x: center.x, y: currentY });
        colNode.lock();
        currentY += rowHeight + fieldGap;
      });
    });

    cy.endBatch();
  }

  function ensureColumnNode(map, columnName, tableNode) {
    const id = `column:${tableNode.data.table}:${columnName}`;
    if (map.has(id)) return map.get(id);
    const zoneClass = zoneClassFromType(tableNode.data.zoneType);
    const node = {
      data: {
        id,
        label: columnName,
        column: columnName,
        table: tableNode.data.table,
        type: 'column',
        zone: tableNode.data.zone,
        zoneType: tableNode.data.zoneType,
        parent: tableNode.data.id
      },
      classes: `column-node ${zoneClass}`
    };
    if (!tableNode.data.columns.includes(columnName)) {
      tableNode.data.columns.push(columnName);
    }
    map.set(id, node);
    return node;
  }

  function drawGraph(elements) {
    destroyGraph();
    cy = cytoscape({
      container: refs.cyContainer,
      elements: [...elements.nodes, ...elements.edges],
      wheelSensitivity: 0.1,
      boxSelectionEnabled: true,
      style: [
        // nodos
        { selector: 'node', style: {
          'label': 'data(label)', 'font-size': 12, 'font-weight': 500,
          'text-wrap': 'wrap', 'text-max-width': 220, 'color': '#0f172a',
          'background-color': '#ffffff', 'border-color': '#cbd5f5',
          'border-width': 1, 'shape': 'round-rectangle'
        }},
        /* { selector: 'node.table-node', style: {
          'font-size': 13, 'font-weight': 700, 'border-width': 2,
          'padding': '32px 16px 12px 16px', 'text-transform': 'lowercase',
          'text-valign': 'top', 'text-halign': 'center',
          'text-background-opacity': 1, 'text-background-padding': 6,
          'text-background-shape': 'roundrectangle',
          'text-background-color': '#475569', 'color': '#ffffff'
        }}, */

        {
          selector: 'node.table-node',
          style: {
            'shape': 'round-rectangle',
            'background-color': '#ffffff',
            'border-color': '#cbd5f5',
            'border-width': 2,

            // AIRE INTERNO (laterales y arriba/abajo) — ajusta a tu gusto
            'padding': '44px 36px 28px 36px',  // top right bottom left

            // “placa” con el nombre de la tabla
            'label': 'data(label)',
            'font-size': 13,
            'font-weight': 700,
            'text-transform': 'lowercase',
            'text-valign': 'top',
            'text-halign': 'center',
            'text-background-opacity': 1,
            'text-background-padding': 6,
            'text-background-shape': 'roundrectangle',
            'text-background-color': '#475569',
            'color': '#ffffff',

            // el tamaño del padre no se deforma por el label
            'compound-sizing-wrt-labels': 'exclude'
          }
        },

        /* { selector: 'node.column-node', style: {
          'shape': 'round-rectangle', 'font-size': 11, 'padding': '6px 8px',
          'border-width': 1, 'text-transform': 'lowercase', 'background-color': '#ffffff',
          'border-color': '#d4d4d8', 'text-halign': 'left', 'text-valign': 'center',
          'width': 220, 'height': 28
        }}, */
        
        {
          selector: 'node.column-node',
          style: {
            'shape': 'round-rectangle',
            'background-color': '#ffffff',
            'border-color': '#d4d4d8',
            'border-width': 1,

            // ancho/alto del “rectángulo campo”
            'width': 260,     // súbelo a 280/300 si tus nombres lo piden
            'height': 32,     // ¡debe coincidir con rowHeight del layout!
            'padding': '0px',

            // texto dentro del rectángulo (una sola línea)
            'label': 'data(label)',
            'font-size': 11,
            'text-transform': 'lowercase',
            'text-halign': 'center',  // ← texto DENTRO del nodo
            'text-valign': 'center',
            'text-wrap': 'none',
            'text-max-width': 9999,
            'text-margin-x': 0,

            'color': '#0f172a'
          }
        },

        // colores por zona
        { selector: 'node.table-node.zone-resultados', style: { 'border-color': '#65a30d', 'text-background-color': '#65a30d', 'color': '#ffffff' } },
        { selector: 'node.table-node.zone-proceso',    style: { 'border-color': '#be185d', 'text-background-color': '#be185d', 'color': '#ffffff' } },
        { selector: 'node.table-node.zone-s_bani',     style: { 'border-color': '#5b21b6', 'text-background-color': '#5b21b6', 'color': '#f8fafc' } },
        { selector: 'node.table-node.zone-lz-estatico',style: { 'border-color': '#b91c1c', 'text-background-color': '#b91c1c', 'color': '#ffffff' } },
        { selector: 'node.table-node.zone-lz-funcion', style: { 'border-color': '#64748b', 'text-background-color': '#64748b', 'color': '#ffffff' } },
        { selector: 'node.table-node.zone-otro',       style: { 'border-color': '#475569', 'text-background-color': '#475569', 'color': '#ffffff' } },
        { selector: 'node.column-node.zone-resultados',style: { 'border-color': '#a3e635', 'color': '#0f172a' } },
        { selector: 'node.column-node.zone-proceso',   style: { 'border-color': '#fda4af', 'color': '#0f172a' } },
        { selector: 'node.column-node.zone-s_bani',    style: { 'border-color': '#a855f7', 'color': '#0f172a' } },
        { selector: 'node.column-node.zone-lz-estatico',style:{ 'border-color': '#f87171','color': '#0f172a' } },
        { selector: 'node.column-node.zone-lz-funcion',style: { 'border-color': '#94a3b8', 'color': '#0f172a' } },
        { selector: 'node.column-node.zone-otro',      style: { 'border-color': '#94a3b8', 'color': '#0f172a' } },
        // aristas
        { selector: 'edge', style: {
          'curve-style': 'bezier', 'width': 2, 'line-color': '#94a3b8',
          'target-arrow-shape': 'triangle', 'target-arrow-color': '#94a3b8'
        }},
        { selector: 'edge.table-edge', style: { 'width': 3 } },
        { selector: 'edge.lz-edge',    style: { 'line-color': '#ef4444', 'target-arrow-color': '#ef4444', 'width': 3 } }
      ]
    });

    // clases de zona
    cy.nodes().forEach(node => {
      const zoneTypeValue = node.data('zoneType');
      node.addClass(zoneClassFromType(zoneTypeValue));
    });

    applyLayout();
    enableInteractions();
    attachEdgeTooltips(); // on-demand
    cy.autoungrabify(true);
  }

  function applyLayout() {
    if (!cy) return;
    cy.nodes().unlock();

    /* if (state.layout === 'columns') {
      applyZoneLayout();
      return;
    } */

    const isColumns = (state.layout === 'columns');
    setOverlayVisible(isColumns);
    setFreeflowTheme(!isColumns);
    if (isColumns) {
      applyZoneLayout();
      return;
    }

    refs.cyContainer.style.width = '';
    refs.cyContainer.style.height = '';
    if (refs.overlay) refs.overlay.style.width = '';
    const layoutConf = LAYOUTS[state.layout] || LAYOUTS.dagre;
    cy.layout(layoutConf).run();

    // Al terminar dagre, empacamos los campos dentro de su tabla
    if (state.viewMode === 'fields' || state.viewMode === 'fields') {
      packFieldsWithinParents();
    }
    cy.fit(undefined, 80);
  }

  function applyZoneLayout() {
    if (!cy) return;

    cy.startBatch();

    const containerWidth = refs.cyContainer
      ? Math.max(refs.cyContainer.clientWidth, refs.cyContainer.scrollWidth)
      : 0;
    const rawWidth = containerWidth || cy.width() || 1200;
    const width = rawWidth > 0 ? rawWidth : 1200;
    const separator = 6;

    const overlayVisible = refs.overlay && window.getComputedStyle(refs.overlay).display !== 'none';
    let zoneWidth = Math.max(320, Math.max(0, (width - separator * 2) / 3));
    const totalWidth = overlayVisible ? zoneWidth * 3 + separator * 2 : width;
    const offsetX = overlayVisible ? Math.max(0, (totalWidth - width) / 2) : 0;

    const columnsX = {
      left:   offsetX + zoneWidth / 2,
      center: offsetX + zoneWidth + separator + zoneWidth / 2,
      right:  offsetX + zoneWidth * 2 + separator * 2 + zoneWidth / 2
    };

    const startY = 120;
    /* const headerHeight = 46;
    const rowHeight = 34; */    
    const headerHeight = 46;  // altura “visual” para el header de la tabla
    const rowHeight    = 32;  // DEBE coincidir con 'height' del node.column-node
    const fieldGap = 12; // espacio vertical entre campos (mockup)

    const tableGap = 140;
    let maxBottom = startY;

    const tablesByColumn = { left: [], center: [], right: [] };

    const tableNodes = cy.nodes()
      .filter(n => n.data('type') === 'table')
      .sort((a, b) => a.data('label').localeCompare(b.data('label')));

    tableNodes.forEach(node => {
      const columnKey = zoneColumnKey(node.data('zoneType'));
      tablesByColumn[columnKey].push(node);
    });

    const positions = new Map();

    Object.entries(tablesByColumn).forEach(([columnKey, nodes]) => {
      if (!nodes.length) return;
      let cursor = startY;
      nodes.forEach(node => {
        const columnCount = Math.max(1, (node.data('columns') || []).length);
        const tableHeight = headerHeight + columnCount * rowHeight + 32;
        const centerY = cursor + tableHeight / 2;
        node.position({ x: columnsX[columnKey], y: centerY });
        node.lock();
        positions.set(node.data('table'), {
          x: columnsX[columnKey], top: cursor, headerHeight, rowHeight, tableHeight
        });
        cursor += tableHeight + tableGap;
        maxBottom = Math.max(maxBottom, cursor);
      });
    });

    const columnNodesByTable = new Map();
    cy.nodes().filter(n => n.data('type') === 'column').forEach(node => {
      const tableName = node.data('table');
      if (!columnNodesByTable.has(tableName)) columnNodesByTable.set(tableName, []);
      columnNodesByTable.get(tableName).push(node);
    });

    columnNodesByTable.forEach((nodes, tableName) => {
      const metrics = positions.get(tableName);
      if (!metrics) return;
      nodes.sort((a, b) => a.data('label').localeCompare(b.data('label')));
      /* let currentY = metrics.top + metrics.headerHeight + rowHeight / 2; */
      // deja espacio por el top + primer gap
      let currentY = metrics.top + metrics.headerHeight + fieldGap + rowHeight / 2;

      nodes.forEach(node => {
        node.position({ x: metrics.x, y: currentY });
        node.lock();
        /* currentY += rowHeight;
         */
        currentY += rowHeight + fieldGap;  // ⬅️ agrega separación entre filas
      });
      /* maxBottom = Math.max(maxBottom, currentY + rowHeight); */  
      // deja un gap adicional al final para respetar el "fondo rojo"
      maxBottom = Math.max(maxBottom, currentY + fieldGap);

    });

    cy.endBatch();

    const desiredHeight = Math.max(900, maxBottom + 200);
    refs.cyContainer.style.height = `${desiredHeight}px`;

    if (overlayVisible) {
      refs.cyContainer.style.width = `${totalWidth}px`;
      if (refs.overlay) refs.overlay.style.width = `${totalWidth}px`;
    } else {
      refs.cyContainer.style.width = '';
      if (refs.overlay) refs.overlay.style.width = '';
    }

    cy.resize();
    if (overlayVisible) {
      cy.zoom(1);
      cy.pan({ x: 0, y: 0 });
    } else {
      cy.fit(undefined, 80);
    }
  }

  function enableInteractions() {
    if (!cy) return;

    cy.on('tap', evt => {
      if (evt.target === cy) updateDetails(null);
    });

    cy.on('tap', 'node.table-node', evt => {
      updateDetails(renderTableDetails(evt.target));
    });

    cy.on('tap', 'node.column-node', evt => {
      const node = evt.target;
      const incoming = node.incomers('edge');
      const outgoing = node.outgoers('edge');
      updateDetails(renderColumnDetails(node, incoming, outgoing));
    });

    cy.on('tap', 'edge', evt => {
      updateDetails(renderEdgeDetails(evt.target));
    });
  }

  // Tooltips on-demand: se crean al primer hover sobre cada arista
  function attachEdgeTooltips() {
    edgeTooltips.forEach(t => t.destroy?.());
    edgeTooltips = [];

    if (!cy || typeof cytoscapePopper !== 'function' || typeof tippy !== 'function') return;

    cy.edges().forEach(edge => {
      if (typeof edge.popperRef !== 'function') return;

      const showHandler = () => {
        let tip = edge.scratch('_tip');
        if (!tip) {
          const ref = edge.popperRef(); // virtual ref (getBoundingClientRect)
          const content = buildEdgeTooltipContent(edge);
          const dummy = document.createElement('div');
          try {
            tip = tippy(dummy, {
              getReferenceClientRect: ref.getBoundingClientRect,
              appendTo: document.body,
              content,
              trigger: 'manual',
              placement: 'top',
              arrow: true,
              interactive: false,
              hideOnClick: false,
              theme: 'light-border'
            });
            edge.scratch('_tip', tip);
            edgeTooltips.push(tip);
          } catch (e) {
            console.warn('[viz] Tooltip fallo:', e);
            return;
          }
        }
        tip.show();
      };

      const hideHandler = () => {
        const tip = edge.scratch('_tip');
        if (tip) tip.hide();
      };

      edge.on('mouseover', showHandler);
      edge.on('mouseout', hideHandler);
    });
  }

  function buildEdgeTooltipContent(edge) {
    const data = edge.data();
    const div = document.createElement('div');
    div.className = 'cy-tooltip';
    const source = `${escapeHtml(data.sourceTable)}${data.sourceField ? '.' + escapeHtml(data.sourceField) : ''}`;
    const target = `${escapeHtml(data.targetTable)}${data.targetField ? '.' + escapeHtml(data.targetField) : ''}`;
    const transform = data.transformaciones && data.transformaciones.length
      ? escapeHtml(data.transformaciones[0])
      : 'copy';
    div.innerHTML = `
      <strong>${source}</strong><br>
      ➞ <strong>${target}</strong><br>
      <em>${transform}</em>
    `;
    return div;
  }

  function renderTableDetails(node) {
    const zone = node.data('zone');
    const zoneTypeValue = node.data('zoneType');
    const incoming = node.incomers('edge');
    const outgoing = node.outgoers('edge');
    const rawColumns = node.data('columns') || [];
    const columns = Array.isArray(rawColumns) ? [...new Set(rawColumns)].sort() : [];
    return `
      <div class="details-grid">
        <strong>Tabla:</strong><span>${escapeHtml(node.data('table'))}</span>
        <strong>Zona:</strong><span>${escapeHtml(zone)} (${zoneTypeValue})</span>
        <strong>Entradas:</strong><span>${incoming.length}</span>
        <strong>Salidas:</strong><span>${outgoing.length}</span>
        <strong>Campos:</strong>
        <span>${columns.length ? columns.map(escapeHtml).join('<br>') : '-'}</span>
      </div>
    `;
  }

  function renderColumnDetails(node, incomingEdges, outgoingEdges) {
    const origins = incomingEdges.map(edge => {
      const data = edge.data();
      return `${escapeHtml(data.sourceTable)}.${escapeHtml(data.sourceField)} (${escapeHtml(firstOrNone(data.transformaciones))})`;
    });
    const targets = outgoingEdges.map(edge => {
      const data = edge.data();
      return `${escapeHtml(data.targetTable)}.${escapeHtml(data.targetField)} (${escapeHtml(firstOrNone(data.transformaciones))})`;
    });
    return `
      <div class="details-grid">
        <strong>Tabla:</strong><span>${escapeHtml(node.data('table'))}</span>
        <strong>Campo:</strong><span>${escapeHtml(node.data('column'))}</span>
        <strong>Entradas:</strong><span>${origins.length ? origins.join('<br>') : '-'}</span>
        <strong>Salidas:</strong><span>${targets.length ? targets.join('<br>') : '-'}</span>
      </div>
    `;
  }

  function renderEdgeDetails(edge) {
    const data = edge.data();
    const transformaciones = data.transformaciones && data.transformaciones.length
      ? data.transformaciones.map(escapeHtml).join('<br>')
      : 'copy';
    const recomendaciones = data.recomendaciones && data.recomendaciones.length
      ? data.recomendaciones.map(escapeHtml).join('<br>')
      : '-';
    /* const consultas = data.consultas && data.consultas.length
      ? data.consultas.map(escapeHtml).join('<br><br>')
      : '-'; */
    
    const consultas = data.consultas && data.consultas.length
     ? data.consultas
         .map(q => `<pre class="query-block">${escapeHtml(String(q))}</pre>`)
         .join('<hr class="query-sep">')
     : '-';

    return `
      <div class="details-grid">
        <strong>Origen:</strong><span>${escapeHtml(data.sourceTable)}${data.sourceField ? '.' + escapeHtml(data.sourceField) : ''}</span>
        <strong>Destino:</strong><span>${escapeHtml(data.targetTable)}${data.targetField ? '.' + escapeHtml(data.targetField) : ''}</span>
        <strong>Transformación:</strong><span>${transformaciones}</span>
        <strong>Recomendaciones:</strong><span>${recomendaciones}</span>
        <strong>Consulta:</strong><span>${consultas}</span>
      </div>
    `;
  }

  function renderErrorDetails(err) {
    return `
      <div class="details-grid">
        <strong>Error:</strong><span>${escapeHtml(err.message || String(err))}</span>
        <strong>Sugerencia:</strong><span>Verifica que los archivos JSON existan y sirvas la carpeta mediante un servidor local.</span>
      </div>
    `;
  }
})();
