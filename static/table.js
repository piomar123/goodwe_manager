// Shared helper for rendering a plain HTML table from column names + row
// objects. Used by the history page (server-paginated raw data) and the
// forecast page (hourly PV production breakdown).
function renderTable(theadId, tbodyId, emptyId, columns, rows) {
  const thead = document.getElementById(theadId);
  const tbody = document.getElementById(tbodyId);
  const empty = document.getElementById(emptyId);
  thead.innerHTML = '<tr>' + columns.map(c => `<th>${c}</th>`).join('') + '</tr>';
  if (rows.length === 0) {
    tbody.innerHTML = '';
    empty.classList.remove('d-none');
    return;
  }
  empty.classList.add('d-none');
  tbody.innerHTML = rows.map(row =>
    '<tr>' + columns.map(c => `<td>${row[c] === null || row[c] === undefined ? '' : row[c]}</td>`).join('') + '</tr>'
  ).join('');
}
