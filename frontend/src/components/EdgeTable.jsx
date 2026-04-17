import { SvcPill, short } from "./NodeTable";

const EDGE_COLORS = {
  spawned: "#5a8a5a", ran: "#c06040", calls: "#555",
  handled: "#4a7fa5", dispatched: "#8060c0",
};

export default function EdgeTable({ edges }) {
  return (
    <table className="data-table">
      <thead><tr>
        <th>from</th><th>node</th><th style={{textAlign:"center"}}>edge</th>
        <th>to</th><th>node</th><th>calls</th>
      </tr></thead>
      <tbody>
        {edges.map((e, i) => {
          const sc = e.source.split("::")[1] || e.source;
          const tc = e.target.split("::")[1] || e.target;
          const ss = e.source.split("::")[0];
          const ts = e.target.split("::")[0];
          const ec = EDGE_COLORS[e.type] || "#555";
          return (
            <tr key={i}>
              <td><SvcPill svc={ss} /></td>
              <td className="name-cell" title={sc}>{short(sc, 22)}</td>
              <td style={{textAlign:"center"}}>
                <span style={{ color:ec, fontSize:10, padding:"1px 5px",
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