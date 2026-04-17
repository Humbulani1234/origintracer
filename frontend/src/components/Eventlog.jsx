import { svcColor, short } from "./NodeTable";

function fmtTime(wall_time) {
  if (!wall_time) return "—";
  return new Date(wall_time * 1000).toLocaleTimeString([], {
    hour: "2-digit", minute: "2-digit", second: "2-digit",
  });
}

function fmtDuration(ns) {
  if (ns == null) return "—";
  if (ns < 1_000_000) return (ns / 1_000).toFixed(1) + "µs";
  if (ns < 1_000_000_000) return (ns / 1_000_000).toFixed(1) + "ms";
  return (ns / 1_000_000_000).toFixed(2) + "s";
}

const COL = {
  time: { width: 80 },
  service: { width: 90 },
  probe: { width: 180 },
  name: { }, // flex
  duration: { width: 72, textAlign: "right" },
  trace: { width: 110 },
};

const TH = {
  padding: "6px 10px", fontSize: 10, fontWeight: 500,
  color: "var(--color-text-tertiary)", letterSpacing: "0.06em",
  textAlign: "left", whiteSpace: "nowrap",
};

const TD = {
  padding: "5px 10px", fontSize: 11, fontFamily: "monospace",
  borderBottom: "0.5px solid var(--color-border-tertiary)",
  whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis",
  verticalAlign: "middle",
};

export default function EventLog({ events }) {
  if (!events?.length) {
    return (
      <div style={{ padding: "32px 14px", fontFamily: "monospace",
          fontSize: 11, color: "var(--color-text-tertiary)", textAlign: "center" }}>
        No events yet.
      </div>
    );
  }

  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse",
          tableLayout: "fixed", minWidth: 600 }}>
        <thead>
          <tr style={{ borderBottom: "0.5px solid var(--color-border-secondary)" }}>
            <th style={{ ...TH, width: COL.time.width }}>time</th>
            <th style={{ ...TH, width: COL.service.width }}>service</th>
            <th style={{ ...TH, width: COL.probe.width }}>probe</th>
            <th style={{ ...TH }}>name</th>
            <th style={{ ...TH, width: COL.duration.width, textAlign: "right" }}>duration</th>
            <th style={{ ...TH, width: COL.trace.width }}>trace id</th>
          </tr>
        </thead>
        <tbody>
          {events.map((e, i) => {
            const c = svcColor(e.service);
            return (
              <tr key={i} style={{ background: "transparent" }}
                onMouseEnter={ev => ev.currentTarget.style.background = "var(--color-background-secondary)"}
                onMouseLeave={ev => ev.currentTarget.style.background = "transparent"}>
                <td style={{ ...TD, color: "var(--color-text-secondary)" }}>
                  {fmtTime(e.wall_time)}
                </td>
                <td style={TD}>
                  <span style={{
                    display: "inline-block", padding: "1px 6px",
                    borderRadius: 4, fontSize: 10, fontWeight: 500,
                    background: c + "22", color: c,
                    border: `0.5px solid ${c}55`,
                  }}>
                    {short(e.service || "", 10)}
                  </span>
                </td>
                <td style={{ ...TD, color: "var(--color-text-primary)" }}>
                  {short(e.probe || "", 28)}
                </td>
                <td style={{ ...TD, color: "var(--color-text-secondary)" }}
                  title={e.name}>
                  {short(e.name || "", 40)}
                </td>
                <td style={{ ...TD, textAlign: "right",
                    color: e.duration_ns ? "var(--color-text-warning)" : "var(--color-text-tertiary)" }}>
                  {fmtDuration(e.duration_ns)}
                </td>
                <td style={{ ...TD, color: "var(--color-text-tertiary)",
                    userSelect: "all", cursor: "text", whiteSpace: "nowrap" }}
                    title="click to select">
                  {e.trace_id ?? "—"}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}