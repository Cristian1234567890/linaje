const cyContainer = document.getElementById('cy');
const detailsEl = document.getElementById('details-content');
const refreshBtn = document.getElementById('refresh-btn');
const layoutSelect = document.getElementById('layout-select');

const LAYOUTS = {
  dagre: {
    name: 'dagre',
    rankDir: 'LR',
    nodeSep: 80,
    rankSep: 160,
    edgeSep: 40,
    fit: true,
    padding: 40
  },
  breadthfirst: {
    name: 'breadthfirst',
    directed: true,
    circle: false,
    spacingFactor: 1.25,
    padding: 30
  },
  cose: {
    name: 'cose',
    padding: 30,
    animate: false
  }
};

let cy;

init();

refreshBtn.addEventListener('click', init);
layoutSelect.addEventListener('change', () => applyLayout(layoutSelect.value));

async function init() {
  try {
    const lineage = await fetchLineage();
    const elements = buildGraph(lineage);
    renderGraph(elements);
    updateDetails();
  } catch (err) {
    console.error(err);
    detailsEl.innerHTML = `<p class="error">No se pudo cargar <code>json/linaje.json</code>. ¿Ejecutaste un servidor local?</p>`;
  }
}

async function fetchLineage() {
  const response = await fetch('../json/linaje.json', {cache: 'no-cache'});
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return response.json();
}

function buildGraph(records) {
  const tables = new Map(); // tabla -> Set columnas
  const nodes = [];
  const edges = [];

  const addColumn = (table, column, extra = {}) => {
    const cleanCol = column || '(sin_campo)';
    if (!tables.has(table)) {
      tables.set(table, new Map());
    }
    const colMap = tables.get(table);
    if (!colMap.has(cleanCol)) {
      const id = `${table}::${cleanCol}`;
      colMap.set(cleanCol, { id, table, column: cleanCol, extra });
    }
    return colMap.get(cleanCol);
  };

  records.forEach((record, idx) => {
    const {
      tabla_origen: sourceTable,
      tabla_destino: targetTable,
      campo_origen: sourceCol,
      campo_destino: targetCol,
      transformacion_aplicada: transform,
      recomendaciones: recomendacion
    } = record;

    if (!targetTable || !targetCol) {
      return; // evitamos nodos sin destino definido
    }

    const sourceInfo = addColumn(sourceTable || 'desconocido', sourceCol, {transformation: transform});
    const targetInfo = addColumn(targetTable, targetCol);

    const edgeId = `e-${idx}-${sourceInfo.id}->${targetInfo.id}`;
    edges.push({
      data: {
        id: edgeId,
        source: sourceInfo.id,
        target: targetInfo.id,
        transformacion: transform || 'copy',
        recomendacion: recomendacion || ''
      },
      classes: sourceTable === 'funciones' ? 'funcion' : ''
    });
  });

  for (const [table, columns] of tables.entries()) {
    const tableId = `table-${table}`;
    nodes.push({
      data: { id: tableId, label: table },
      classes: table === 'funciones' ? 'funciones cy-table' : 'cy-table'
    });

    for (const column of columns.values()) {
      nodes.push({
        data: {
          id: column.id,
          label: column.column,
          parent: tableId,
          table,
          column: column.column,
          transformation: column.extra?.transformation || null
        },
        classes: table === 'funciones' ? 'cy-column funcion' : 'cy-column'
      });
    }
  }

  return { nodes, edges };
}

function renderGraph(elements) {
  if (cy) {
    cy.destroy();
  }

  cy = cytoscape({
    container: cyContainer,
    elements,
    wheelSensitivity: 0.2,
    style: [
      {
        selector: 'node.cy-table',
        style: {
          'shape': 'round-rectangle',
          'background-color': '#22c55e',
          'border-color': '#15803d',
          'border-width': 2,
          'label': 'data(label)',
          'font-size': 12,
          'text-valign': 'center',
          'text-halign': 'center',
          'color': '#ffffff',
          'padding': '10px'
        }
      },
      {
        selector: 'node.cy-table.funciones',
        style: {
          'background-color': '#ef4444',
          'border-color': '#b91c1c'
        }
      },
      {
        selector: 'node.cy-column',
        style: {
          'shape': 'round-rectangle',
          'background-color': '#f0f9ff',
          'border-color': '#bae6fd',
          'border-width': 1,
          'label': 'data(label)',
          'font-size': 11,
          'text-valign': 'center',
          'text-halign': 'center',
          'color': '#0f172a',
          'padding': '3px',
          'width': 'label',
          'height': 'label'
        }
      },
      {
        selector: 'node.cy-column.funcion',
        style: {
          'background-color': '#fee2e2',
          'border-color': '#f87171',
          'color': '#7f1d1d'
        }
      },
      {
        selector: 'edge',
        style: {
          'curve-style': 'bezier',
          'target-arrow-shape': 'triangle',
          'line-color': '#94a3b8',
          'target-arrow-color': '#94a3b8',
          'width': 2
        }
      },
      {
        selector: 'edge.funcion',
        style: {
          'line-color': '#dc2626',
          'target-arrow-color': '#dc2626',
          'width': 3
        }
      }
    ]
  });

  applyLayout(layoutSelect.value);
  enableInteractions();
}

function applyLayout(name) {
  if (!cy) return;
  const layout = cy.layout(LAYOUTS[name] || LAYOUTS.dagre);
  layout.run();
}

function enableInteractions() {
  cy.on('tap', 'node.cy-column', evt => {
    const data = evt.target.data();
    const parent = evt.target.parent();
    updateDetails({
      type: 'column',
      table: parent.data('label'),
      column: data.label,
      transformation: data.transformation
    });
  });

  cy.on('tap', 'node.cy-table', evt => {
    const data = evt.target.data();
    const columns = evt.target.children().map(child => child.data('label'));
    updateDetails({
      type: 'table',
      table: data.label,
      columns
    });
  });

  cy.on('tap', 'edge', evt => {
    const data = evt.target.data();
    const source = cy.getElementById(data.source);
    const target = cy.getElementById(data.target);
    updateDetails({
      type: 'edge',
      source: source.data('label'),
      sourceTable: source.parent().data('label'),
      target: target.data('label'),
      targetTable: target.parent().data('label'),
      transformacion: data.transformacion,
      recomendacion: data.recomendacion
    });
  });

  cy.on('tap', evt => {
    if (evt.target === cy) {
      updateDetails();
    }
  });
}

function updateDetails(info) {
  if (!info) {
    detailsEl.innerHTML = '<p>Haz clic en cualquier columna o conexión para ver la información asociada.</p>';
    return;
  }

  if (info.type === 'table') {
    detailsEl.innerHTML = `
      <div class="details-grid">
        <strong>Tabla:</strong><span>${escapeHtml(info.table)}</span>
        <strong>Columnas:</strong>
        <span>${info.columns.map(escapeHtml).join('<br>')}</span>
      </div>`;
  } else if (info.type === 'column') {
    detailsEl.innerHTML = `
      <div class="details-grid">
        <strong>Tabla:</strong><span>${escapeHtml(info.table)}</span>
        <strong>Columna:</strong><span>${escapeHtml(info.column)}</span>
        <strong>Transformación:</strong><span>${escapeHtml(info.transformation || 'copy')}</span>
      </div>`;
  } else if (info.type === 'edge') {
    detailsEl.innerHTML = `
      <div class="details-grid">
        <strong>Origen:</strong><span>${escapeHtml(info.sourceTable)} / ${escapeHtml(info.source)}</span>
        <strong>Destino:</strong><span>${escapeHtml(info.targetTable)} / ${escapeHtml(info.target)}</span>
        <strong>Transformación:</strong><span>${escapeHtml(info.transformacion)}</span>
        <strong>Recomendación:</strong><span>${escapeHtml(info.recomendacion || '-')}</span>
      </div>`;
  }
}

function escapeHtml(str) {
  return (str || '').replace(/[&<>"]+/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
}
