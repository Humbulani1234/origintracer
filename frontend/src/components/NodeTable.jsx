// src/components/NodeTable.jsx
export const SVC_COLORS = {
  gunicorn: "#5a8a5a", uvicorn: "#4a7fa5", django: "#e8a020",
  asyncio:  "#8060c0", celery:  "#c06040", redis: "#409060",
  nginx: "#507090", db: "#705080",
};

export function svcColor(svc) { return SVC_COLORS[svc] || "#666"; }

export function SvcPill({ svc }) {
  const c = svcColor(svc);
  return (
    <span style={{ background:`${c}22`, color:c, border:`1px solid ${c}44`,
      borderRadius:2, padding:"1px 7px", fontSize:10, fontFamily:"monospace" }}>
      {svc}
    </span>
  );
}

export function fmtMs(ns) {
  if (!ns && ns !== 0) return "—";
  const ms = ns / 1e6;
  return ms < 1 ? ms.toFixed(2)+"ms" : ms.toFixed(1)+"ms";
}

export function short(s, n=38) { return s.length > n ? s.slice(0,n-1)+"…" : s; }

export default function NodeTable({ nodes }) {
  return (
    <table className="data-table">
      <thead><tr><th>service</th><th>node</th><th>calls</th><th>avg</th></tr></thead>
      <tbody>
        {nodes.map(n => {
          const name = n.id.split("::")[1] || n.id;
          return (
            <tr key={n.id}>
              <td><SvcPill svc={n.service} /></td>
              <td className="name-cell" title={name}>{short(name)}</td>
              <td className="count">{n.call_count}</td>
              <td className="muted">{fmtMs(n.avg_duration_ns)}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}