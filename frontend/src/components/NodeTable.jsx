// src/components/NodeTable.jsx
export const SVC_COLORS = {
  gunicorn: '#5a8a5a', uvicorn: '#4a7fa5', django: '#e8a020',
  asyncio:  '#8060c0', celery:  '#c06040', redis:  '#409060',
  nginx:    '#507090', db:      '#705080',
};

export function svcColor(svc) { return SVC_COLORS[svc] || '#666'; }

export function SvcPill({ svc }) {
  const c = svcColor(svc);
  return (
    <span style={{ background:`${c}22`, color:c, border:`1px solid ${c}44`,
      borderRadius:2, padding:'1px 7px', fontSize:10, fontFamily:'var(--mono)' }}>
      {svc}
    </span>
  );
}

export function fmtMs(ns) {
  if (!ns && ns !== 0) return '—';
  const ms = ns / 1e6;
  return ms < 1 ? ms.toFixed(2)+'ms' : ms.toFixed(1)+'ms';
}

export function short(s, n=38) { return s.length > n ? s.slice(0,n-1)+'…' : s; }

export default function NodeTable({ nodes }) {
  return (
    <table className="data-table">
      <thead><tr><th>service</th><th>node</th><th>calls</th><th>avg</th></tr></thead>
      <tbody>
        {nodes.map(n => {
          const name = n.id.split('::')[1] || n.id;
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

// ─────────────────────────────────────────────────────
// src/components/EdgeTable.jsx  (export this separately in real project)
const EDGE_COLORS = {
  spawned:'#5a8a5a', ran:'#c06040', calls:'#555',
  handled:'#4a7fa5', dispatched:'#8060c0',
};

export function EdgeTable({ edges }) {
  return (
    <table className="data-table">
      <thead><tr>
        <th>from</th><th>node</th><th style={{textAlign:'center'}}>edge</th>
        <th>to</th><th>node</th><th>calls</th>
      </tr></thead>
      <tbody>
        {edges.map((e, i) => {
          const sc = e.source.split('::')[1] || e.source;
          const tc = e.target.split('::')[1] || e.target;
          const ss = e.source.split('::')[0];
          const ts = e.target.split('::')[0];
          const ec = EDGE_COLORS[e.type] || '#555';
          return (
            <tr key={i}>
              <td><SvcPill svc={ss} /></td>
              <td className="name-cell" title={sc}>{short(sc, 22)}</td>
              <td style={{textAlign:'center'}}>
                <span style={{ color:ec, fontSize:10, padding:'1px 5px',
                  background:`${ec}18`, borderRadius:2 }}>{e.type}</span>
              </td>
              <td><SvcPill svc={ts} /></td>
              <td className="name-cell" title={tc}>{short(tc, 22)}</td>
              <td className="count">{e.call_count}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

// ─────────────────────────────────────────────────────
// src/components/TraceTimeline.jsx
export function TraceTimeline({ trace }) {
  if (!trace) {
    return (
      <div style={{ padding:'32px 14px', fontFamily:'var(--mono)', fontSize:11,
        color:'var(--muted)', textAlign:'center' }}>
        run <span style={{color:'var(--amber)'}}>\ stitch &lt;trace_id&gt;</span> to view a trace
      </div>
    );
  }

  const { id, stages } = trace;
  const maxDur = Math.max(...stages.filter(s => s.duration_ms).map(s => s.duration_ms), 1);
  const totalMs = stages.reduce((a, s) => a + (s.duration_ms || 0), 0);

  return (
    <div style={{ padding:14 }}>
      <div style={{ fontFamily:'var(--mono)', fontSize:10, color:'var(--muted)',
        marginBottom:12, letterSpacing:'0.06em' }}>
        trace <span style={{color:'var(--amber)'}}>{id.slice(0,8)}…</span>
      </div>

      {stages.map((s, i) => {
        const barW = s.duration_ms ? Math.max(2, (s.duration_ms / maxDur) * 100) : 0;
        const c = svcColor(s.service);
        const nameShort = short(s.name, 28);
        return (
          <div key={i} style={{ display:'grid', gridTemplateColumns:'64px 1fr 220px',
            gap:10, alignItems:'center', padding:'3px 0',
            borderBottom:'1px solid rgba(42,42,42,0.4)' }}>
            <div style={{ textAlign:'right', fontFamily:'var(--mono)', fontSize:10,
              color: s.duration_ms ? 'var(--amber)' : 'var(--muted)' }}>
              {s.duration_ms ? s.duration_ms.toFixed(1)+'ms' : '—'}
            </div>
            <div style={{ height:8, background:'var(--bg3)', borderRadius:1 }}>
              <div style={{ height:'100%', width:`${barW}%`, background:c,
                borderRadius:1, minWidth:2 }} />
            </div>
            <div style={{ fontFamily:'var(--mono)', fontSize:10, color:'var(--muted)',
              overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>
              <span style={{color:'var(--text)'}}>{s.probe}</span>
              <span style={{color:'#555', fontSize:9, marginLeft:4}}>
                {s.service}::{nameShort}
              </span>
            </div>
          </div>
        );
      })}

      <div style={{ marginTop:12, fontFamily:'var(--mono)', fontSize:10, color:'var(--muted)' }}>
        end-to-end <span style={{color:'var(--amber)'}}>{totalMs.toFixed(1)}ms</span>
        {' · '}
        <span style={{color:'var(--amber)'}}>{stages.length}</span> stages
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────
// src/components/EventLog.jsx
export function EventLog({ events }) {
  return (
    <div>
      {events.map((e, i) => {
        const c = svcColor(e.service);
        const nameShort = short(e.name || '', 32);
        return (
          <div key={i} style={{ display:'grid', gridTemplateColumns:'80px 120px 1fr auto',
            gap:8, alignItems:'center', padding:'5px 14px',
            borderBottom:'1px solid rgba(42,42,42,0.4)',
            fontFamily:'var(--mono)', fontSize:10 }}>
            <span style={{color:'var(--muted)'}}>{e.wall_time || e.timestamp}</span>
            <span style={{color:c}}>{e.probe}</span>
            <span className="muted" title={e.name}>{nameShort}</span>
            <span style={{color:'var(--amber)'}}>
              {e.duration_ms ? e.duration_ms.toFixed(1)+'ms' : '—'}
            </span>
          </div>
        );
      })}
    </div>
  );
}

// ─────────────────────────────────────────────────────
// src/components/StatusBar.jsx
export function StatusBar({ nodes, edges, events, status }) {
  return (
    <div style={{ display:'flex', gap:20, alignItems:'center', padding:'5px 14px',
      borderTop:'1px solid var(--border)', background:'var(--bg2)',
      fontFamily:'var(--mono)', fontSize:10, color:'var(--muted)' }}>
      <span>
        <span style={{display:'inline-block', width:5, height:5, borderRadius:'50%',
          background:'#3c9', marginRight:4, verticalAlign:'middle'}} />
        live
      </span>
      <span><span style={{color:'var(--amber)'}}>{nodes.length}</span> nodes</span>
      <span><span style={{color:'var(--amber)'}}>{edges.length}</span> edges</span>
      <span><span style={{color:'var(--amber)'}}>{events.length}</span> events</span>
      {status && (
        <span><span style={{color:'var(--amber)'}}>{status.sockets}</span> sockets</span>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────
// src/components/QueryBar.jsx
export function QueryBar({ onRun, loading }) {
  const [val, setVal] = useState('');

  const run = () => { if (val.trim()) { onRun(val); setVal(''); } };

  return (
    <div style={{ display:'flex', alignItems:'center', gap:8, padding:'7px 14px',
      borderBottom:'1px solid var(--border)', background:'var(--bg)' }}>
      <span style={{fontFamily:'var(--mono)', fontSize:11, color:'var(--amber)', flexShrink:0}}>›</span>
      <input
        value={val}
        onChange={e => setVal(e.target.value)}
        onKeyDown={e => e.key === 'Enter' && run()}
        placeholder="SHOW nodes  ·  SHOW edges  ·  \stitch <trace_id>"
        style={{ flex:1, background:'none', border:'none', outline:'none',
          fontFamily:'var(--mono)', fontSize:11, color:'var(--text)',
          caretColor:'var(--amber)' }}
      />
      <button onClick={run} disabled={loading}
        style={{ fontFamily:'var(--mono)', fontSize:10, color:'var(--amber)',
          background:'rgba(232,160,32,0.08)', border:'1px solid rgba(232,160,32,0.2)',
          borderRadius:3, padding:'2px 8px', cursor:'pointer' }}>
        {loading ? '…' : 'run ↵'}
      </button>
    </div>
  );
}

// NOTE: In a real project split each component into its own file.
// They are combined here for clarity. The imports in App.jsx reference
// them as separate files.

import { useState } from "react";