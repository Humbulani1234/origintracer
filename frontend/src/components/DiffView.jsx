import { svcColor } from "./NodeTable";

const ADDED   = "#3c9";
const REMOVED = "#c06040";

function DiffSection({ title, items, color }) {
    if (!items?.length) return null;
    return (
        <div style={{ marginBottom: 16 }}>
            <div style={{ fontFamily:"monospace", fontSize:10,
                color:"var(--muted)", letterSpacing:"0.06em",
                marginBottom: 6 }}>
                {title} ({items.length})
            </div>
            {items.map((item, i) => {
                const svc = item.split("::")[0];
                const name = item.split("::")[1] || item;
                const c = color === ADDED ? svcColor(svc) : color;
                return (
                    <div key={i} style={{ display:"flex", alignItems:"center",
                        gap: 8, padding:"3px 0",
                        borderBottom:"1px solid rgba(42,42,42,0.4)",
                        fontFamily:"monospace", fontSize:11 }}>
                        <span style={{ color, flexShrink:0, width:12 }}>
                            {color === ADDED ? "+" : "−"}
                        </span>
                        <span style={{ color: c }}>{svc}</span>
                        <span style={{ color:"var(--muted)", fontSize:9 }}>::</span>
                        <span style={{ color:"var(--text)" }}>
                            {name?.length > 50 ? name.slice(0,48)+"…" : name}
                        </span>
                    </div>
                );
            })}
        </div>
    );
}

export default function DiffView({ diff }) {
    const isEmpty = !diff ||
        (!diff.added_nodes?.length &&
         !diff.removed_nodes?.length &&
         !diff.added_edges?.length &&
         !diff.removed_edges?.length);

    if (isEmpty) {
        return (
            <div style={{ padding:"32px 14px", fontFamily:"monospace",
                fontSize:11, color:"var(--muted)", textAlign:"center" }}>
                No changes detected since last snapshot.
            </div>
        );
    }

    const totalAdded = (diff.added_node_ids?.length   || 0) +
                         (diff.added_edge_keys?.length   || 0);
    const totalRemoved = (diff.removed_node_ids?.length || 0) +
                         (diff.removed_edge_keys?.length || 0);

    return (
        <div style={{ padding: 14 }}>
            <div style={{ fontFamily:"monospace", fontSize:10,
                color:"var(--muted)", marginBottom:16,
                letterSpacing:"0.06em" }}>
                since deployment{" "}
                {diff.label && (
                    <span style={{color:"var(--amber)"}}>{diff.label}</span>
                )}
                {"  ·  "}
                <span style={{color: ADDED}}>+{totalAdded}</span>
                {"  "}
                <span style={{color: REMOVED}}>−{totalRemoved}</span>
            </div>

            <DiffSection
                title="new nodes"
                items={diff.added_nodes}
                color={ADDED}
            />
            <DiffSection
                title="removed nodes"
                items={diff.removed_nodes}
                color={REMOVED}
            />
            <DiffSection
                title="new edges"
                items={diff.added_edges}
                color={ADDED}
            />
            <DiffSection
                title="removed edges"
                items={diff.removed_edges}
                color={REMOVED}
            />

            {totalAdded === 0 && totalRemoved === 0 && (
                <div style={{ fontFamily:"monospace", fontSize:11,
                    color:"var(--muted)", textAlign:"center",
                    paddingTop: 24 }}>
                    no structural changes since last deployment
                </div>
            )}
        </div>
    );
}