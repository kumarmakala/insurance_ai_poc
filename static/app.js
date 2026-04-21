/* ==================================================================
 * IRYSCLOUD — Data Migration Console
 * Single-file React SPA, no build step.
 * Sections:
 *   1. Utilities & API
 *   2. Icons
 *   3. Toast system
 *   4. Design primitives
 *   5. Confidence ring
 *   6. Command palette
 *   7. App shell (sidebar, topbar)
 *   8. Panel: Ingest
 *   9. Panel: Mapping
 *  10. Panel: Dedup
 *  11. Panel: Graph
 *  12. Panel: Review
 *  13. Panel: DAG
 *  14. Panel: Audit
 *  15. Panel: Agent
 *  16. Root
 * ================================================================ */

const { useState, useEffect, useRef, useMemo, useCallback, createContext, useContext } = React;

/* ======================== 1. Utilities & API ==================== */

const api = async (path, opts = {}) => {
    const r = await fetch(path, opts);
    if (!r.ok) throw new Error(`${r.status} ${path}`);
    return r.json();
};

/** Consume an SSE response stream from fetch(); calls onEvent({event,data}) per frame.
 *  If onEvent returns "stop" (or any truthy value), the reader is cancelled and the
 *  function returns cleanly — useful when a terminal event like `end` arrives but the
 *  server hasn't closed the HTTP body yet (uvicorn can keepalive for a bit). */
async function streamSSE(path, opts, onEvent) {
    const r = await fetch(path, opts);
    if (!r.ok || !r.body) throw new Error(`${r.status} ${path}`);
    const reader = r.body.getReader();
    const decoder = new TextDecoder();
    let buf = "";
    const stop = async () => { try { await reader.cancel(); } catch {} };
    for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        let idx;
        while ((idx = buf.indexOf("\n\n")) !== -1) {
            const raw = buf.slice(0, idx);
            buf = buf.slice(idx + 2);
            let ev = "message", dataLines = [];
            for (const line of raw.split("\n")) {
                if (line.startsWith("event:")) ev = line.slice(6).trim();
                else if (line.startsWith("data:")) dataLines.push(line.slice(5).trim());
            }
            if (!dataLines.length) continue;
            let payload;
            try { payload = JSON.parse(dataLines.join("\n")); }
            catch { payload = dataLines.join("\n"); }
            const verdict = onEvent({ event: ev, data: payload });
            if (verdict) { await stop(); return; }
        }
    }
}

const PII_FIELDS = new Set([
    "ssn", "fein", "license_number", "licenseNumber",
    "email", "emailAddress", "primary_phone", "primaryPhone_number",
    "date_of_birth", "dateOfBirth",
]);

const maskValue = (field, v) => {
    if (v == null || v === "") return v;
    const s = String(v);
    const f = field.toLowerCase();
    if (f.includes("ssn")) return "•••-••-" + s.slice(-4);
    if (f.includes("fein")) return "••-•••" + s.slice(-3);
    if (f.includes("email")) { const [u, d] = s.split("@"); return d ? `${u[0]}•••@${d}` : "•••"; }
    if (f.includes("phone")) return "•••-•••-" + s.slice(-4);
    if (f.includes("birth")) return s.slice(0, 4) + "-••-••";
    if (f.includes("license")) return s.slice(0, 2) + "••••••";
    return "••••";
};

const fmtNum = (n) => n == null ? "—" : Number(n).toLocaleString("en-US");
const fmtMoney = (n) => n == null ? "—" : `$${Number(n).toLocaleString("en-US")}`;

const useCountUp = (target, ms = 650) => {
    const [v, setV] = useState(0);
    useEffect(() => {
        if (target == null) return;
        const start = performance.now();
        const from = 0;
        let raf = 0;
        const tick = (t) => {
            const p = Math.min(1, (t - start) / ms);
            const eased = 1 - Math.pow(1 - p, 3);
            setV(Math.round(from + (target - from) * eased));
            if (p < 1) raf = requestAnimationFrame(tick);
        };
        raf = requestAnimationFrame(tick);
        return () => cancelAnimationFrame(raf);
    }, [target, ms]);
    return v;
};

const useKeyboard = (combo, handler, deps = []) => {
    useEffect(() => {
        const onKey = (e) => {
            const cmd = e.metaKey || e.ctrlKey;
            const k = e.key.toLowerCase();
            if (combo === "cmd+k" && cmd && k === "k") { e.preventDefault(); handler(e); }
            if (combo === "escape" && k === "escape") handler(e);
            if (combo === "j" && k === "j" && !e.metaKey) handler(e);
            if (combo === "k" && k === "k" && !e.metaKey) handler(e);
            if (combo === "y" && k === "y") handler(e);
            if (combo === "n" && k === "n") handler(e);
        };
        window.addEventListener("keydown", onKey);
        return () => window.removeEventListener("keydown", onKey);
    }, deps);
};

/* ======================== 2. Icons ============================== */

/* Lucide-compatible SVG paths, stroke=1.75 for crispness at 16px */
const ICON_PATHS = {
    upload: '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/>',
    sparkle: '<path d="M12 3v3"/><path d="M12 18v3"/><path d="m5 5 2 2"/><path d="m17 17 2 2"/><path d="M3 12h3"/><path d="M18 12h3"/><path d="m5 19 2-2"/><path d="m17 7 2-2"/><circle cx="12" cy="12" r="3"/>',
    map: '<polygon points="3 6 9 3 15 6 21 3 21 18 15 21 9 18 3 21 3 6"/><line x1="9" y1="3" x2="9" y2="18"/><line x1="15" y1="6" x2="15" y2="21"/>',
    copy: '<rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>',
    share: '<circle cx="18" cy="5" r="3"/><circle cx="6" cy="12" r="3"/><circle cx="18" cy="19" r="3"/><line x1="8.59" y1="13.51" x2="15.42" y2="17.49"/><line x1="15.41" y1="6.51" x2="8.59" y2="10.49"/>',
    users: '<path d="M16 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="8.5" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/>',
    user: '<path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/>',
    building: '<rect x="4" y="2" width="16" height="20" rx="2"/><path d="M9 22v-4h6v4"/><path d="M8 6h.01"/><path d="M16 6h.01"/><path d="M8 10h.01"/><path d="M16 10h.01"/><path d="M8 14h.01"/><path d="M16 14h.01"/>',
    clock: '<circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/>',
    shield: '<path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>',
    bot: '<rect x="3" y="11" width="18" height="10" rx="2"/><circle cx="12" cy="5" r="2"/><path d="M12 7v4"/><line x1="8" y1="16" x2="8" y2="16"/><line x1="16" y1="16" x2="16" y2="16"/>',
    search: '<circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/>',
    check: '<polyline points="20 6 9 17 4 12"/>',
    x: '<line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>',
    chev_right: '<polyline points="9 18 15 12 9 6"/>',
    chev_down: '<polyline points="6 9 12 15 18 9"/>',
    info: '<circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/>',
    refresh: '<polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10"/><path d="M20.49 15a9 9 0 0 1-14.85 3.36L1 14"/>',
    play: '<polygon points="5 3 19 12 5 21 5 3"/>',
    pause: '<rect x="6" y="4" width="4" height="16"/><rect x="14" y="4" width="4" height="16"/>',
    alert: '<path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/>',
    file: '<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/>',
    file_spread: '<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="8" y1="13" x2="16" y2="13"/><line x1="8" y1="17" x2="13" y2="17"/>',
    lock: '<rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/>',
    eye: '<path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/>',
    git_merge: '<circle cx="18" cy="18" r="3"/><circle cx="6" cy="6" r="3"/><path d="M6 21V9a9 9 0 0 0 9 9"/>',
    layers: '<polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/>',
    activity: '<polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>',
    terminal: '<polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/>',
    corner: '<polyline points="9 10 4 15 9 20"/><path d="M20 4v7a4 4 0 0 1-4 4H4"/>',
    zap: '<polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>',
    plug: '<path d="M12 22v-5"/><path d="M9 8V2"/><path d="M15 8V2"/><path d="M18 8v4a4 4 0 0 1-4 4h-4a4 4 0 0 1-4-4V8Z"/>',
    target: '<circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/>',
    download: '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/>',
    command: '<path d="M18 3a3 3 0 0 0-3 3v12a3 3 0 0 0 3 3 3 3 0 0 0 3-3 3 3 0 0 0-3-3H6a3 3 0 0 0-3 3 3 3 0 0 0 3 3 3 3 0 0 0 3-3V6a3 3 0 0 0-3-3 3 3 0 0 0-3 3 3 3 0 0 0 3 3h12a3 3 0 0 0 3-3 3 3 0 0 0-3-3z"/>',
    send: '<line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/>',
    git_branch: '<line x1="6" y1="3" x2="6" y2="15"/><circle cx="18" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/>',
    arrow_up: '<line x1="12" y1="19" x2="12" y2="5"/><polyline points="5 12 12 5 19 12"/>',
    arrow_down: '<line x1="12" y1="5" x2="12" y2="19"/><polyline points="19 12 12 19 5 12"/>',
};

function Icon({ name, size = 16, className = "" }) {
    const p = ICON_PATHS[name];
    if (!p) return null;
    return (
        <svg
            width={size} height={size} viewBox="0 0 24 24"
            fill="none" stroke="currentColor" strokeWidth="1.75"
            strokeLinecap="round" strokeLinejoin="round"
            className={className}
            dangerouslySetInnerHTML={{ __html: p }}
        />
    );
}

/* ======================== 3. Toasts ============================= */

const ToastCtx = createContext({ push: () => {} });

function ToastProvider({ children }) {
    const [toasts, setToasts] = useState([]);
    const push = useCallback((msg, kind = "success", sub = null) => {
        const id = Math.random().toString(36).slice(2);
        setToasts(t => [...t, { id, msg, kind, sub }]);
        setTimeout(() => setToasts(t => t.filter(x => x.id !== id)), 3800);
    }, []);
    return (
        <ToastCtx.Provider value={{ push }}>
            {children}
            <div className="toast-layer">
                {toasts.map(t => (
                    <div key={t.id} className={`toast toast-${t.kind}`}>
                        <span className="toast-dot" />
                        <div>
                            <div className="font-medium">{t.msg}</div>
                            {t.sub && <div className="text-ink-400 text-[11.5px] mt-0.5">{t.sub}</div>}
                        </div>
                    </div>
                ))}
            </div>
        </ToastCtx.Provider>
    );
}
const useToast = () => useContext(ToastCtx);

/* ======================== 4. Design primitives ================== */

function Button({ children, variant = "ghost", size = "md", icon, iconRight, disabled, onClick, className = "", type = "button" }) {
    const sizes = { sm: "h-8 px-3 text-[13px]", md: "h-9 px-3.5 text-[13.5px]", lg: "h-10 px-4 text-sm" };
    const variants = {
        primary:  "btn-grad",
        secondary:"bg-white text-ink-700 border border-ink-200 hover:border-ink-300 hover:bg-ink-50",
        ghost:    "text-ink-600 hover:text-ink-900 hover:bg-ink-100",
        danger:   "bg-red-600 text-white hover:bg-red-700",
        soft:     "bg-brand-50 text-brand-700 hover:bg-brand-100",
    };
    return (
        <button type={type} disabled={disabled} onClick={onClick}
            className={`${sizes[size]} ${variants[variant]} inline-flex items-center gap-1.5 rounded-lg font-medium transition-colors disabled:opacity-50 disabled:cursor-not-allowed focus-ring ${className}`}>
            {icon && <Icon name={icon} size={14} />}
            {children}
            {iconRight && <Icon name={iconRight} size={14} />}
        </button>
    );
}

const Card = ({ className = "", children, padding = "p-5" }) => (
    <div className={`bg-surface rounded-xl shadow-card ${padding} ${className}`}>{children}</div>
);

const Chip = ({ children, kind = "neutral", icon }) => (
    <span className={`chip chip-${kind}`}>
        {icon && <Icon name={icon} size={11} />}
        {children}
    </span>
);

const Kbd = ({ children }) => <kbd className="kbd">{children}</kbd>;

function Skeleton({ className = "", lines = 1, height = "h-4" }) {
    return (
        <div className={className}>
            {Array.from({ length: lines }).map((_, i) => (
                <div key={i} className={`skeleton ${height} mb-2 last:mb-0`} style={{ width: `${70 + (i * 7) % 30}%` }} />
            ))}
        </div>
    );
}

function EmptyState({ icon = "sparkle", title, body, cta }) {
    return (
        <div className="flex flex-col items-center justify-center py-16 text-center">
            <div className="w-12 h-12 rounded-2xl bg-ink-100 flex items-center justify-center text-ink-400 mb-4">
                <Icon name={icon} size={22} />
            </div>
            <div className="font-semibold text-ink-900">{title}</div>
            {body && <div className="text-ink-500 text-sm mt-1 max-w-md">{body}</div>}
            {cta && <div className="mt-5">{cta}</div>}
        </div>
    );
}

function Drawer({ open, onClose, title, subtitle, children, width = 560 }) {
    useKeyboard("escape", () => open && onClose(), [open]);
    if (!open) return null;
    return (
        <>
            <div className="drawer-backdrop" onClick={onClose} />
            <aside className="drawer" style={{ width }}>
                <div className="flex items-start justify-between mb-6">
                    <div>
                        <div className="text-[11px] uppercase tracking-widest text-ink-500 font-semibold">{subtitle}</div>
                        <div className="text-lg font-semibold text-ink-900 mt-0.5">{title}</div>
                    </div>
                    <button className="text-ink-400 hover:text-ink-700 p-1 -mt-1 -mr-1" onClick={onClose}><Icon name="x" size={20} /></button>
                </div>
                {children}
            </aside>
        </>
    );
}

function PII({ field, value, onReveal }) {
    const [shown, setShown] = useState(false);
    const toast = useToast();
    if (value == null || value === "") return <span className="text-ink-300">—</span>;
    if (!PII_FIELDS.has(field)) return <span>{String(value)}</span>;
    if (shown) return (
        <span className="pii-revealed">
            <Icon name="eye" size={11} />
            <span>{String(value)}</span>
            <button className="pii-reveal" onClick={() => setShown(false)}>hide</button>
        </span>
    );
    return (
        <span className="pii-masked">
            <Icon name="lock" size={11} />
            <span>{maskValue(field, value)}</span>
            <button className="pii-reveal" onClick={async () => {
                const reason = prompt("Reason for revealing this PII?\n(Audited to immutable log.)", "Underwriting review");
                if (!reason) return;
                await api("/api/audit/reveal", {
                    method: "POST",
                    headers: { "content-type": "application/json" },
                    body: JSON.stringify({ field, target: String(value), reason }),
                });
                setShown(true);
                toast.push("PII revealed", "warn", `Logged to audit: ${field}`);
                onReveal && onReveal();
            }}>reveal</button>
        </span>
    );
}

/* ======================== 5. Confidence ring ==================== */

function ConfidenceRing({ value = 0, size = 56 }) {
    const r = (size - 8) / 2;
    const c = 2 * Math.PI * r;
    const pct = Math.max(0, Math.min(1, value));
    const offset = c - pct * c;
    const color = pct >= 0.85 ? "#10b981" : pct >= 0.70 ? "#f59e0b" : "#ef4444";
    return (
        <div className="ring-wrap" style={{ width: size, height: size }}>
            <svg width={size} height={size}>
                <circle cx={size/2} cy={size/2} r={r} fill="none" stroke="#e2e8f0" strokeWidth="4" />
                <circle cx={size/2} cy={size/2} r={r} fill="none" stroke={color} strokeWidth="4" strokeLinecap="round"
                    strokeDasharray={c} strokeDashoffset={offset} style={{ transition: "stroke-dashoffset .6s cubic-bezier(.2,.9,.3,1)" }} />
            </svg>
            <div className="ring-label tabular">{(pct * 100).toFixed(0)}%</div>
        </div>
    );
}

/* ======================== 6. Command palette =================== */

function CommandPalette({ open, onClose, onNavigate }) {
    const [q, setQ] = useState("");
    const [sel, setSel] = useState(0);
    const inputRef = useRef();
    const commands = useMemo(() => ([
        { id: "nav:ingest",   label: "Go to Ingest",   icon: "upload",    kind: "Navigate" },
        { id: "nav:mapping",  label: "Go to Mapping",  icon: "map",       kind: "Navigate" },
        { id: "nav:dedup",    label: "Go to Dedup",    icon: "git_merge", kind: "Navigate" },
        { id: "nav:graph",    label: "Go to Graph",    icon: "share",     kind: "Navigate" },
        { id: "nav:review",   label: "Go to Review Queue", icon: "users", kind: "Navigate" },
        { id: "nav:dag",      label: "Go to DAG Runner",   icon: "activity", kind: "Navigate" },
        { id: "nav:audit",    label: "Go to Audit",    icon: "shield",    kind: "Navigate" },
        { id: "nav:agent",    label: "Go to Agent",    icon: "bot",       kind: "Navigate" },
        { id: "act:run-dag",  label: "Run ingestion DAG",  icon: "play",  kind: "Action" },
        { id: "act:reset",    label: "Reset workspace",    icon: "refresh", kind: "Action" },
    ]), []);
    const filtered = useMemo(() =>
        !q.trim() ? commands : commands.filter(c => c.label.toLowerCase().includes(q.toLowerCase()))
    , [q, commands]);

    useEffect(() => { if (open) setTimeout(() => inputRef.current?.focus(), 30); }, [open]);
    useEffect(() => { setSel(0); }, [q]);

    const run = (cmd) => {
        onClose();
        if (cmd.id.startsWith("nav:")) onNavigate(cmd.id.slice(4), { action: null });
        else onNavigate(null, { action: cmd.id.slice(4) });
    };

    useKeyboard("escape", () => open && onClose(), [open]);

    if (!open) return null;
    return (
        <div className="palette-root" onClick={onClose}>
            <div className="palette" onClick={e => e.stopPropagation()}>
                <div className="flex items-center gap-2 px-4 border-b border-ink-200">
                    <Icon name="search" size={16} className="text-ink-400" />
                    <input ref={inputRef} value={q} onChange={e => setQ(e.target.value)}
                        placeholder="Search commands, pages, actions…"
                        onKeyDown={e => {
                            if (e.key === "ArrowDown") { setSel(s => Math.min(filtered.length - 1, s + 1)); e.preventDefault(); }
                            if (e.key === "ArrowUp")   { setSel(s => Math.max(0, s - 1)); e.preventDefault(); }
                            if (e.key === "Enter" && filtered[sel]) run(filtered[sel]);
                        }}
                        className="flex-1"
                    />
                    <Kbd>esc</Kbd>
                </div>
                <div className="palette-results">
                    {filtered.length === 0 && <div className="palette-empty">No matches for "{q}"</div>}
                    {filtered.map((c, i) => (
                        <div key={c.id} className={`palette-item ${i === sel ? "selected" : ""}`} onClick={() => run(c)} onMouseEnter={() => setSel(i)}>
                            <div className="w-7 h-7 rounded-md bg-ink-100 text-ink-500 flex items-center justify-center"><Icon name={c.icon} size={14} /></div>
                            <div>
                                <div className="text-sm text-ink-900">{c.label}</div>
                                <div className="text-[11px] text-ink-400">{c.kind}</div>
                            </div>
                            <div className="palette-kbd">{i === sel ? "⏎" : ""}</div>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
}

/* ======================== 7. App shell ========================== */

const NAV = [
    { group: "Ingestion",     items: [
        { id: "ingest",  label: "Ingest",     icon: "upload",    desc: "Drop files → pipeline" },
        { id: "dag",     label: "DAG Runner", icon: "activity",  desc: "Airflow-style runs" },
    ]},
    { group: "Data Quality",  items: [
        { id: "mapping", label: "Schema Mapping", icon: "map",       desc: "Column + enum checks" },
        { id: "dedup",   label: "Deduplication",  icon: "git_merge", desc: "Entity/person clusters" },
        { id: "graph",   label: "Relationships",  icon: "share",     desc: "Graph of who's who" },
    ]},
    { group: "Humans in loop", items: [
        { id: "review",  label: "Review Queue", icon: "users",  desc: "HITL decisions" },
        { id: "audit",   label: "Audit Log",    icon: "shield", desc: "Every action, PII reveals" },
    ]},
    { group: "AI co-pilot",   items: [
        { id: "agent",   label: "Submission Agent", icon: "bot", desc: "Build a quote package" },
    ]},
];

function Sidebar({ active, onSelect, stats }) {
    const badges = { mapping: stats?.mapping_issues, dedup: stats?.dedup_clusters, review: stats?.open_reviews };
    return (
        <aside className="w-60 bg-ink-950 text-ink-100 flex flex-col min-h-screen sticky top-0">
            <div className="flex items-center gap-2.5 px-5 pt-5 pb-4 border-b border-ink-800">
                <img src="/static/assets/irys_logo.png" alt="IRYSCLOUD" className="w-9 h-9 rounded-xl object-contain shadow-lg" />
                <div>
                    <div className="text-sm font-semibold tracking-tight text-white">IRYSCLOUD</div>
                    <div className="text-[10px] text-ink-400 uppercase tracking-[.14em]">Migration Console</div>
                </div>
            </div>
            <nav className="flex-1 overflow-y-auto px-3 py-3">
                {NAV.map(section => (
                    <div key={section.group}>
                        <div className="nav-section">{section.group}</div>
                        {section.items.map(item => (
                            <div key={item.id} className={`nav-item ${active === item.id ? "active" : ""}`} onClick={() => onSelect(item.id)}>
                                <Icon name={item.icon} size={15} />
                                <span>{item.label}</span>
                                {badges[item.id] ? <span className="nav-badge">{badges[item.id]}</span> : null}
                            </div>
                        ))}
                    </div>
                ))}
            </nav>
            <div className="border-t border-ink-800 px-4 py-3 text-[11px] text-ink-500 flex items-center gap-2">
                <div className="w-6 h-6 rounded-full bg-ink-800 flex items-center justify-center text-ink-300 font-semibold text-[11px]">VS</div>
                <div className="flex-1">
                    <div className="text-ink-200">VidyaSri</div>
                    <div className="text-ink-500">vidya@iryscloud.com</div>
                </div>
            </div>
        </aside>
    );
}

const SECTION_META = Object.fromEntries(NAV.flatMap(g => g.items.map(i => [i.id, { ...i, group: g.group }])));

function Topbar({ active, onReset, llmMode, onCmd, stats }) {
    const meta = SECTION_META[active];
    const [pulse, setPulse] = useState(false);
    const lastRef = useRef(null);
    useEffect(() => {
        const snap = stats ? `${stats.entities}|${stats.humans}|${stats.relationships}|${stats.mapping_issues}|${stats.dedup_clusters}|${stats.open_reviews}` : null;
        if (snap && lastRef.current && snap !== lastRef.current) {
            setPulse(true);
            const id = setTimeout(() => setPulse(false), 900);
            lastRef.current = snap;
            return () => clearTimeout(id);
        }
        lastRef.current = snap;
    }, [stats]);
    return (
        <header className="h-14 border-b border-ink-200 topbar-frosted flex items-center px-6 gap-3 sticky top-0 z-40">
            <div className="flex items-center gap-2.5">
                <span className="brand-mark w-7 h-7 text-[11.5px]">IC</span>
                <div className="flex items-center gap-2 text-[13px] text-ink-500">
                    <span className="text-ink-900 font-semibold tracking-tight">Harbor Risk Partners</span>
                    <span className="hidden md:inline-flex items-center gap-1.5 px-2 py-0.5 rounded-md bg-ink-50 border border-ink-200 text-[10.5px] font-semibold uppercase tracking-wider text-ink-500">
                        <span className="w-1.5 h-1.5 rounded-full bg-emerald-500" /> demo · us-east-1
                    </span>
                    <Icon name="chev_right" size={12} />
                    <span className="text-ink-400">{meta?.group}</span>
                    <Icon name="chev_right" size={12} />
                    <span className="text-ink-900 font-medium">{meta?.label}</span>
                </div>
            </div>
            {stats && (
                <div className={`ml-4 hidden md:flex items-center gap-3 text-[11.5px] text-ink-500 tabular px-2.5 py-1 rounded-full border ${pulse ? "border-brand-300 bg-brand-50" : "border-ink-200 bg-white"} transition-colors`}>
                    <span className={`dot ${pulse ? "dot-brand pulse-dot" : "dot-success"}`} style={{margin: 0}} />
                    <span><b className="text-ink-900">{stats.entities}</b> ent</span>
                    <span><b className="text-ink-900">{stats.humans}</b> hum</span>
                    <span><b className="text-ink-900">{stats.relationships}</b> edge</span>
                    <span><b className="text-ink-900">{stats.mapping_issues}</b> map</span>
                    <span><b className="text-ink-900">{stats.open_reviews}</b> HITL</span>
                </div>
            )}
            <div className="ml-auto flex items-center gap-2">
                <button onClick={onCmd} className="inline-flex items-center gap-2 h-8 px-2.5 rounded-lg border border-ink-200 text-ink-500 text-[12.5px] hover:bg-ink-50">
                    <Icon name="search" size={12} />
                    <span>Quick search</span>
                    <span className="ml-4 flex items-center gap-1"><Kbd>⌘</Kbd><Kbd>K</Kbd></span>
                </button>
                <div className={`inline-flex items-center gap-1.5 text-[11.5px] px-2 py-1 rounded-full border ${llmMode === "live" ? "bg-emerald-50 border-emerald-200 text-emerald-700" : "bg-ink-50 border-ink-200 text-ink-600"}`}>
                    <span className={`dot ${llmMode === "live" ? "dot-success" : "dot-success"}`} style={{margin: 0}} />
                    {llmMode === "live" ? "Claude Sonnet 4.5 · live" : "IRYSCLOUD Agent · ready"}
                </div>
                <Button variant="secondary" size="sm" icon="refresh" onClick={onReset}>Reset workspace</Button>
            </div>
        </header>
    );
}

function PageHeader({ title, subtitle, actions, eyebrow }) {
    return (
        <div className="flex items-start justify-between gap-6 mb-7">
            <div className="min-w-0">
                {eyebrow && <div className="hero-eyebrow">{eyebrow}</div>}
                <h1 className="hero-title">{title}</h1>
                {subtitle && <p className="text-[14px] text-ink-500 mt-2.5 max-w-2xl leading-relaxed">{subtitle}</p>}
            </div>
            {actions && <div className="flex items-center gap-2 shrink-0">{actions}</div>}
        </div>
    );
}

/* ======================== 8. Panel: Ingest ====================== */

const CONNECTORS = [
    { id: "xlsx", label: "Excel upload", kind: "File", state: "active" },
    { id: "pdf",  label: "PDF upload",   kind: "File", state: "active" },
    { id: "csv",  label: "CSV upload",   kind: "File", state: "active" },
];

const PIPE_STEPS = [
    { id: "parse",    label: "Parse",    icon: "file" },
    { id: "classify", label: "Classify", icon: "target" },
    { id: "map",      label: "Map",      icon: "map" },
    { id: "dedupe",   label: "Dedupe",   icon: "git_merge" },
    { id: "graph",    label: "Graph",    icon: "share" },
    { id: "validate", label: "Validate", icon: "check" },
    { id: "load",     label: "Load",     icon: "download" },
];

function StatCard({ label, value, icon, delta }) {
    const animated = useCountUp(value);
    // Track the first non-null value we ever see for this stat so we can
    // display an auto-delta ("+14 since boot") when the caller doesn't pass one.
    const baselineRef = useRef(null);
    useEffect(() => {
        if (baselineRef.current == null && typeof value === "number") baselineRef.current = value;
    }, [value]);
    let deltaNum = typeof delta === "number" ? delta : null;
    if (deltaNum == null && baselineRef.current != null && typeof value === "number") {
        deltaNum = value - baselineRef.current;
    }
    const deltaKind = deltaNum == null ? "flat" : deltaNum > 0 ? "" : deltaNum < 0 ? "neg" : "flat";
    const deltaLabel = deltaNum == null
        ? (typeof delta === "string" ? delta : null)
        : deltaNum > 0 ? `+${deltaNum.toLocaleString()} since boot`
        : deltaNum < 0 ? `${deltaNum.toLocaleString()} since boot`
        : "steady";
    return (
        <Card className="stat-card">
            <div className="flex items-start justify-between mb-3">
                <div className="text-[11px] uppercase tracking-widest text-ink-500 font-semibold">{label}</div>
                <div className="stat-icon-grad"><Icon name={icon} size={15} /></div>
            </div>
            <div className="text-[34px] font-semibold text-ink-900 tabular tracking-tight leading-none">{animated?.toLocaleString() || 0}</div>
            {deltaLabel && (
                <div className="mt-3 flex items-center gap-1.5">
                    <span className={`stat-delta ${deltaKind}`}>
                        {deltaNum != null && deltaNum > 0 && <Icon name="arrow_up" size={10} />}
                        {deltaNum != null && deltaNum < 0 && <Icon name="arrow_down" size={10} />}
                        {deltaLabel}
                    </span>
                </div>
            )}
        </Card>
    );
}

const inferFileType = (name) => {
    const lower = (name || "").toLowerCase();
    if (lower.endsWith(".pdf")) return "pdf";
    if (lower.endsWith(".xlsx")) return "xlsx";
    if (lower.endsWith(".csv")) return "csv";
    return "other";
};

const humanSize = (bytes) => {
    if (bytes == null) return "";
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
};

const fmtSecs = (ms) => {
    const s = Math.max(0, ms / 1000);
    if (s < 60) return `${s.toFixed(s < 10 ? 1 : 0)}s`;
    const m = Math.floor(s / 60);
    const r = Math.round(s - m * 60);
    return `${m}m ${r}s`;
};

function SheetCard({ sh }) {
    const [hover, setHover] = useState(false);
    const headers = Array.isArray(sh.headers) ? sh.headers.filter(Boolean) : [];
    const sample = Array.isArray(sh.sample) ? sh.sample : [];
    const hasPreview = headers.length > 0;
    const MAX_COLS = 12;
    const shownCols = headers.slice(0, MAX_COLS);
    const moreCols = Math.max(0, headers.length - MAX_COLS);
    return (
        <Card padding="p-4">
            <div className="flex items-start justify-between">
                <div>
                    <div className="text-[10.5px] uppercase tracking-widest text-ink-500 font-semibold">{(sh.kind || "unknown").replace(/_/g, " ")}</div>
                    <div className="font-semibold text-ink-900 text-[13.5px] mt-0.5">{sh.name}</div>
                </div>
                <div
                    className="relative"
                    onMouseEnter={() => setHover(true)}
                    onMouseLeave={() => setHover(false)}
                >
                    <Icon
                        name="file_spread"
                        size={16}
                        className={`cursor-help transition-colors ${hasPreview ? (hover ? "text-brand-600" : "text-ink-400") : "text-ink-300"}`}
                    />
                    {hover && hasPreview && (
                        <div className="absolute right-0 top-6 z-40 w-80 max-w-[22rem] rounded-lg bg-white shadow-pop border border-ink-200 p-3 animate-slide-up">
                            <div className="text-[10.5px] uppercase tracking-widest text-ink-500 font-semibold">Preview</div>
                            <div className="mt-1 text-[12px] font-semibold text-ink-900 truncate">{sh.name}</div>
                            <div className="text-[11px] text-ink-500 tabular mt-0.5">{sh.rows} rows · {sh.cols} cols</div>
                            <div className="mt-2.5">
                                <div className="text-[10.5px] uppercase tracking-widest text-ink-500 font-semibold mb-1">Columns</div>
                                <div className="flex flex-wrap gap-1">
                                    {shownCols.map((h, i) => (
                                        <span key={i} className="inline-flex items-center px-1.5 py-0.5 rounded bg-ink-100 text-ink-700 text-[10.5px] font-medium">{h}</span>
                                    ))}
                                    {moreCols > 0 && (
                                        <span className="inline-flex items-center px-1.5 py-0.5 rounded bg-ink-50 text-ink-500 text-[10.5px] font-medium">+{moreCols} more</span>
                                    )}
                                </div>
                            </div>
                            {sample.length > 0 && (
                                <div className="mt-2.5">
                                    <div className="text-[10.5px] uppercase tracking-widest text-ink-500 font-semibold mb-1">Sample</div>
                                    <div className="space-y-1">
                                        {sample.slice(0, 2).map((row, ri) => (
                                            <div key={ri} className="text-[11px] text-ink-700 tabular truncate" title={row.filter(Boolean).join(" · ")}>
                                                {row.slice(0, 4).map((c, ci) => (
                                                    <span key={ci} className="inline-block mr-1.5">
                                                        <span className="text-ink-400">{shownCols[ci] || ""}:</span> <span className="text-ink-800">{String(c || "—").slice(0, 24)}</span>
                                                    </span>
                                                ))}
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            )}
                        </div>
                    )}
                </div>
            </div>
            <div className="text-[11.5px] text-ink-500 mt-2 tabular">{sh.rows} rows · {sh.cols} cols</div>
        </Card>
    );
}

function IngestPanel({ onIngested, onNavigate }) {
    const [dragging, setDragging] = useState(false);
    const [sheets, setSheets] = useState([]);
    const [stepState, setStepState] = useState({});
    const [stepNotes, setStepNotes] = useState({});
    const [stepDurations, setStepDurations] = useState({});
    const [summary, setSummary] = useState(null);
    const [err, setErr] = useState(null);
    const [stats, setStats] = useState(null);
    // queue of files in the current batch:
    // [{ filename, size, type, status, counts, err, startedAt, finishedAt }]
    const [files, setFiles] = useState([]);
    const [activeIdx, setActiveIdx] = useState(null);
    const [running, setRunning] = useState(false);
    const [windowDragging, setWindowDragging] = useState(false);
    const [, setTick] = useState(0);
    const fileRef = useRef();
    const toast = useToast();

    // Refs that the async queue loop and global drop handler read from.
    const queueRef = useRef([]);          // File objects, index-aligned with `files` state
    const processingRef = useRef(false);  // true while processQueue is looping
    const resetNeededRef = useRef(true);  // server truncate pending until a file commits
    const batchStartRef = useRef(null);   // performance.now() of current batch
    const filesRef = useRef([]);          // mirror of `files` for use in async contexts
    const handleFilesRef = useRef(() => {}); // stable pointer for window drop handler

    useEffect(() => { filesRef.current = files; }, [files]);

    // Live ticker so per-file "running for Xs" and batch ETA update each second.
    useEffect(() => {
        if (!running) return;
        const id = setInterval(() => setTick(t => t + 1), 1000);
        return () => clearInterval(id);
    }, [running]);

    const refreshAll = async () => {
        try {
            const [s, st] = await Promise.all([api("/api/sheets"), api("/api/stats")]);
            setSheets(s.sheets || []); setStats(st);
        } catch (e) {}
    };
    useEffect(() => { refreshAll(); }, []);

    const patchFile = (idx, patch) => {
        setFiles(prev => prev.map((f, i) => i === idx ? { ...f, ...patch } : f));
    };

    const friendlyError = (msg) => {
        const reason = (msg || "").toLowerCase();
        if (reason.includes("413") || reason.includes("too large")) return "File is too large to ingest.";
        if (reason.includes("only .xlsx") || reason.includes("only .xlsx / .pdf") || reason.includes("only .xlsx / .pdf / .csv"))
            return "Only Excel (.xlsx), policy PDFs, or CSV files can be ingested today.";
        if (reason.includes("failed to fetch") || reason.includes("network"))
            return "Lost connection to IRYSCLOUD. Check the server is running.";
        return "IRYSCLOUD couldn't read that file. Open it in Excel to verify, then retry.";
    };

    // Process a single file within a batch. `reset=true` wipes the DB before this file;
    // subsequent files in the batch pass `reset=false` so their rows append.
    const ingestOne = async (file, idx, reset) => {
        setActiveIdx(idx);
        setStepState({}); setStepNotes({}); setStepDurations({});
        const startedAt = performance.now();
        patchFile(idx, { status: "running", err: null, counts: null, startedAt, finishedAt: null });

        const form = new FormData();
        form.append("file", file);
        const qs = reset ? "" : "?reset=false";
        let finalCounts = null;
        try {
            let streamErr = null;
            await streamSSE(`/api/ingest/stream${qs}`, { method: "POST", body: form }, ({ event, data }) => {
                if (event === "step") {
                    setStepState(p => ({ ...p, [data.id]: data.status }));
                    if (data.note) setStepNotes(p => ({ ...p, [data.id]: data.note }));
                    if (data.duration_ms != null) setStepDurations(p => ({ ...p, [data.id]: data.duration_ms }));
                } else if (event === "end") {
                    finalCounts = {
                        entities: data.entities, humans: data.humans,
                        contacts: data.contacts, relationships: data.relationships,
                        markets: data.markets,
                    };
                    // Finalize immediately — don't wait for the HTTP body to close.
                    return "stop";
                } else if (event === "error") {
                    streamErr = new Error(data.message || "stream error");
                    return "stop";
                }
            });
            if (streamErr) throw streamErr;
            patchFile(idx, { status: "success", counts: finalCounts, finishedAt: performance.now() });
            return { ok: true, counts: finalCounts };
        } catch (e) {
            // Fallback: non-streaming endpoint so an older server still works
            try {
                const form2 = new FormData();
                form2.append("file", file);
                const r = await api(`/api/ingest${qs}`, { method: "POST", body: form2 });
                for (const s of PIPE_STEPS) setStepState(p => ({ ...p, [s.id]: "success" }));
                const counts = r.counts || null;
                patchFile(idx, { status: "success", counts, finishedAt: performance.now() });
                return { ok: true, counts };
            } catch (e2) {
                const friendly = friendlyError(e2.message || String(e2));
                patchFile(idx, { status: "error", err: friendly, finishedAt: performance.now() });
                return { ok: false, err: friendly };
            }
        }
    };

    // Walks the `files` state, picking up any pending rows and running them serially.
    // Safe to call repeatedly — guarded by processingRef so only one loop runs at a time.
    const processQueue = async () => {
        if (processingRef.current) return;
        processingRef.current = true;
        setRunning(true);
        let lastCounts = null;
        let firstErr = null;
        for (;;) {
            const snapshot = filesRef.current;
            const idx = snapshot.findIndex(f => f.status === "pending");
            if (idx === -1) break;
            const fileObj = queueRef.current[idx];
            if (!fileObj) {
                patchFile(idx, { status: "error", err: "queue desync — re-upload this file" });
                continue;
            }
            const res = await ingestOne(fileObj, idx, resetNeededRef.current);
            if (res.ok) {
                lastCounts = res.counts || lastCounts;
                resetNeededRef.current = false;
            } else if (!firstErr) {
                firstErr = res.err;
            }
        }
        setActiveIdx(null);
        setRunning(false);
        processingRef.current = false;

        if (lastCounts) setSummary({ processed: true, counts: lastCounts });
        const finalFiles = filesRef.current;
        const total = finalFiles.length;
        const okN = finalFiles.filter(f => f.status === "success").length;
        const failN = finalFiles.filter(f => f.status === "error").length;
        if (firstErr && okN === 0) setErr(firstErr);
        if (okN > 0) {
            const label = total === 1 ? "Ingestion complete" : `Batch complete — ${okN}/${total} files`;
            const detail = lastCounts
                ? `${lastCounts.entities} entities · ${lastCounts.humans} humans · ${lastCounts.contacts} contacts`
                : undefined;
            toast.push(label, failN ? "info" : "success", detail);
            refreshAll();
            onIngested && onIngested();
        } else if (firstErr) {
            toast.push("Ingestion failed", "error", firstErr);
        }
    };

    const handleFiles = (fileList) => {
        const list = Array.from(fileList || []).filter(Boolean);
        if (!list.length) return;
        const entries = list.map(f => ({
            filename: f.name, size: f.size, type: inferFileType(f.name),
            status: "pending", counts: null, err: null, startedAt: null, finishedAt: null,
        }));
        if (!processingRef.current) {
            // Fresh batch — clear prior batch artefacts and reset the DB on the first file.
            setErr(null); setSummary(null);
            setStepState({}); setStepNotes({}); setStepDurations({});
            setActiveIdx(null);
            resetNeededRef.current = true;
            batchStartRef.current = performance.now();
            queueRef.current = [...list];
            // Keep filesRef synchronized so processQueue (below) sees pending rows
            // on its first synchronous pass, before React commits setFiles.
            filesRef.current = entries;
            setFiles(entries);
            toast.push(
                list.length === 1 ? "File received" : `${list.length} files queued`,
                "info",
                list.length === 1
                    ? `${list[0].name} · ${humanSize(list[0].size)}`
                    : "Processing sequentially — first wipes, rest append",
            );
        } else {
            // Append to the live batch; the running loop will pick them up in order.
            queueRef.current = [...queueRef.current, ...list];
            filesRef.current = [...filesRef.current, ...entries];
            setFiles(prev => [...prev, ...entries]);
            toast.push(
                `${list.length} file${list.length === 1 ? "" : "s"} added`,
                "info",
                "Will process after the current queue drains",
            );
        }
        processQueue();
    };

    const removeFile = (idx) => {
        const entry = filesRef.current[idx];
        if (!entry || entry.status !== "pending") return;
        setFiles(prev => prev.filter((_, i) => i !== idx));
        queueRef.current = queueRef.current.filter((_, i) => i !== idx);
    };

    const retryFile = (idx) => {
        const entry = filesRef.current[idx];
        if (!entry || entry.status !== "error") return;
        const reset = { status: "pending", err: null, counts: null, startedAt: null, finishedAt: null };
        // Sync the ref so processQueue's first pass sees the pending row immediately.
        filesRef.current = filesRef.current.map((f, i) => i === idx ? { ...f, ...reset } : f);
        patchFile(idx, reset);
        processQueue();
    };

    // Keep a stable pointer for the window-level drop handler so it never sees a
    // stale closure even as handleFiles gets re-created each render.
    handleFilesRef.current = handleFiles;

    // Window-level drag/drop handling: full-page overlay + drop-anywhere behaviour.
    // Also prevents the browser from opening files when dropped outside the dropzone.
    useEffect(() => {
        let dragDepth = 0;
        const hasFiles = (e) => {
            try {
                const types = e.dataTransfer?.types;
                if (!types) return false;
                for (let i = 0; i < types.length; i++) if (types[i] === "Files") return true;
                return false;
            } catch { return false; }
        };
        const onEnter = (e) => {
            if (!hasFiles(e)) return;
            e.preventDefault();
            dragDepth++;
            setWindowDragging(true);
        };
        const onLeave = (e) => {
            e.preventDefault();
            dragDepth = Math.max(0, dragDepth - 1);
            if (dragDepth === 0) setWindowDragging(false);
        };
        const onOver = (e) => { e.preventDefault(); };
        const onDrop = (e) => {
            e.preventDefault();
            dragDepth = 0;
            setWindowDragging(false);
            const fs = Array.from(e.dataTransfer?.files || []);
            if (fs.length) handleFilesRef.current?.(fs);
        };
        window.addEventListener("dragenter", onEnter);
        window.addEventListener("dragleave", onLeave);
        window.addEventListener("dragover", onOver);
        window.addEventListener("drop", onDrop);
        return () => {
            window.removeEventListener("dragenter", onEnter);
            window.removeEventListener("dragleave", onLeave);
            window.removeEventListener("dragover", onOver);
            window.removeEventListener("drop", onDrop);
        };
    }, []);

    const openChooser = () => {
        const el = fileRef.current;
        if (!el) { toast.push("File input not ready — reload the page", "error"); return; }
        toast.push(
            running ? "Add more files" : "Opening file chooser…",
            "info",
            running ? "They'll be queued behind the current batch" : "Pick one or more .xlsx workbooks, policy PDFs, or .csv files",
        );
        try { el.click(); }
        catch (e) { toast.push("Couldn't open chooser", "error", String(e.message || e)); }
    };

    const runSample = async () => {
        if (running) return;
        try {
            toast.push("Fetching sample workbook…", "info");
            const r = await fetch("/static/assets/sample.xlsx");
            if (!r.ok) throw new Error(`sample fetch ${r.status}`);
            const buf = await r.arrayBuffer();
            const file = new File([buf], "IRYSCLOUD_Sample_Export.xlsx", { type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" });
            await handleFiles([file]);
        } catch (e) {
            setErr(`Could not load the sample workbook: ${e.message || e}`);
            toast.push("Sample failed", "error", String(e.message || e));
        }
    };

    return (
        <div>
            {windowDragging && (
                <div className="fixed inset-0 z-50 flex items-center justify-center bg-brand-600/15 backdrop-blur-sm pointer-events-none animate-fade-in">
                    <div className="rounded-3xl border-2 border-dashed border-brand-500 bg-white/95 px-10 py-8 shadow-pop flex items-center gap-5 max-w-md">
                        <div className="w-16 h-16 rounded-2xl bg-brand-600 text-white flex items-center justify-center">
                            <Icon name="upload" size={28} />
                        </div>
                        <div>
                            <div className="text-lg font-semibold text-ink-900">
                                {processingRef.current ? "Drop to add to queue" : "Drop to start ingestion"}
                            </div>
                            <div className="text-sm text-ink-500 mt-0.5">
                                .xlsx workbooks, .pdf policy declarations, or .csv slices — mix is supported
                            </div>
                        </div>
                    </div>
                </div>
            )}
            <PageHeader
                eyebrow="Migration console"
                title="Ingest a vendor export"
                subtitle="Drop one or more Excel (.xlsx) workbooks, policy PDFs, or CSV slices (entities/humans/contacts/relationships). IRYSCLOUD classifies each file, maps columns to the canonical schema, dedupes, builds relationships, validates, and loads to IRYSCLOUD — all visible, all audited."
                actions={<>
                    {running ? (
                        <span className="live-ticker">
                            <span className="live-dot" />
                            <span>Migrating · <b className="text-ink-900">{activeIdx != null && files[activeIdx] ? `${Math.min(activeIdx + 1, files.length)}/${files.length}` : "…"}</b></span>
                        </span>
                    ) : stats && (stats.entities + stats.humans + stats.contacts) > 0 ? (
                        <span className="live-ticker">
                            <span className="live-dot" style={{ background: "#10b981", animation: "none" }} />
                            <span><b className="text-ink-900 tabular">{(stats.entities + stats.humans + stats.contacts).toLocaleString()}</b> records migrated</span>
                        </span>
                    ) : null}
                    <a href="/static/assets/sample.xlsx" download="IRYSCLOUD_Ingestion_Template.xlsx" className="inline-block">
                        <Button variant="secondary" size="sm" icon="download">Download template</Button>
                    </a>
                    <Button variant="soft" size="sm" icon="zap" disabled={running} onClick={runSample}>
                        Run sample
                    </Button>
                    <label
                        htmlFor="iris-xlsx-input"
                        onClick={() => toast.push(running ? "Add more files" : "Opening file chooser…", "info")}
                        className="h-8 px-3 text-[13px] inline-flex items-center gap-1.5 rounded-lg font-medium transition-colors focus-ring bg-brand-600 hover:bg-brand-700 text-white cursor-pointer shadow-[0_1px_0_rgba(255,255,255,.15)_inset,0_1px_2px_rgba(15,23,42,.15)]"
                    >
                        <Icon name="upload" size={14} />
                        {running ? "Add more" : "Upload files"}
                    </label>
                </>}
            />

            {stats && (
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
                    <StatCard label="Entities" value={stats.entities} icon="building" />
                    <StatCard label="Humans" value={stats.humans} icon="users" />
                    <StatCard label="Relationships" value={stats.relationships} icon="share" />
                    <StatCard label="Markets active" value={stats.markets} icon="plug" />
                </div>
            )}

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                <div className="lg:col-span-2">
                    <div className={`dropzone ${dragging || windowDragging ? "is-drag" : ""} rounded-2xl py-14 text-center`}
                         role="button"
                         tabIndex={0}
                         aria-label="Upload one or more Excel workbooks or policy PDFs to ingest"
                         onKeyDown={e => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); openChooser(); } }}
                         onDragOver={e => { e.preventDefault(); setDragging(true); }}
                         onDragLeave={() => setDragging(false)}
                         onDrop={e => { e.preventDefault(); setDragging(false); /* window-level handler ingests the files */ }}>
                        <div className={`w-14 h-14 mx-auto mb-4 rounded-2xl flex items-center justify-center transition-colors ${running ? "bg-brand-600 text-white" : "bg-brand-50 text-brand-600"}`}>
                            <Icon name="upload" size={24} />
                        </div>
                        <div className="font-semibold text-ink-900 text-[15px]">
                            {running
                                ? (files.length > 1 ? `Ingesting ${Math.min((activeIdx ?? 0) + 1, files.length)} of ${files.length}…` : "Ingestion running…")
                                : "Drop .xlsx workbooks, policy PDFs, or .csv files anywhere"}
                        </div>
                        <div className="text-sm text-ink-500 mt-1">
                            {running
                                ? "Keep dragging files onto the page to queue them behind the current batch."
                                : "Batch-ingest a mix of workbooks and policy PDFs — first file wipes, the rest append."}
                        </div>
                        <div className="mt-5 flex items-center justify-center gap-2">
                            <label
                                htmlFor="iris-xlsx-input"
                                onClick={() => toast.push(running ? "Add more files" : "Opening file chooser…", "info")}
                                className="h-8 px-3 text-[13px] inline-flex items-center gap-1.5 rounded-lg font-medium transition-colors focus-ring bg-brand-600 hover:bg-brand-700 text-white cursor-pointer shadow-[0_1px_0_rgba(255,255,255,.15)_inset,0_1px_2px_rgba(15,23,42,.15)]"
                            >
                                <Icon name="file_spread" size={14} />
                                {running ? "Add more…" : "Choose files…"}
                            </label>
                            <Button variant="soft" size="sm" icon="zap" disabled={running} onClick={runSample}>
                                Run sample
                            </Button>
                        </div>
                        <div className="mt-4 flex items-center justify-center gap-3 text-[11px] text-ink-500">
                            <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-md bg-emerald-50 text-emerald-700 font-medium">
                                <span className="w-1.5 h-1.5 rounded-full bg-emerald-500" /> XLSX
                            </span>
                            <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-md bg-rose-50 text-rose-700 font-medium">
                                <span className="w-1.5 h-1.5 rounded-full bg-rose-500" /> PDF
                            </span>
                            <span className="text-ink-400">supported · mix freely</span>
                        </div>
                        <input
                            id="iris-xlsx-input"
                            ref={fileRef}
                            type="file"
                            multiple
                            accept=".xlsx,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,.pdf,application/pdf,.csv,text/csv"
                            className="absolute w-px h-px p-0 -m-px overflow-hidden whitespace-nowrap border-0 opacity-0"
                            tabIndex={-1}
                            aria-hidden="true"
                            onClick={e => { e.target.value = ""; }}
                            onChange={e => { const fs = Array.from(e.target.files || []); if (fs.length) handleFiles(fs); }}
                        />
                    </div>

                    <div className="mt-6">
                        <div className="flex items-center gap-2 mb-3">
                            <div className="text-[11px] uppercase tracking-widest text-ink-500 font-semibold">Pipeline</div>
                            {activeIdx != null && files[activeIdx] && (
                                <div className="text-[11px] text-ink-500 truncate max-w-[60%]">
                                    <span className="text-ink-400">·</span> {files[activeIdx].filename}
                                    {files.length > 1 && <span className="text-ink-400 ml-1">({activeIdx + 1}/{files.length})</span>}
                                </div>
                            )}
                            <div className="h-px flex-1 bg-ink-200" />
                        </div>
                        <div className="flex items-center gap-0 overflow-x-auto pb-2">
                            {PIPE_STEPS.map((s, i) => {
                                const st = stepState[s.id] || "pending";
                                const note = stepNotes[s.id];
                                const dur = stepDurations[s.id];
                                return (
                                    <React.Fragment key={s.id}>
                                        <div className={`pipe-node pipe-${st} min-w-[150px]`}>
                                            <div className="flex items-center gap-2">
                                                <Icon name={s.icon} size={13} />
                                                <div className="font-semibold text-[13px] text-ink-900">{s.label}</div>
                                            </div>
                                            <div className={`pipe-status ${st === "running" ? "pulse-dot" : ""}`}>{st}</div>
                                            {dur != null && <div className="text-[10.5px] text-ink-500 mt-1 tabular">{dur} ms</div>}
                                            {note && <div className="text-[10.5px] text-ink-600 mt-1 leading-tight line-clamp-2">{note}</div>}
                                        </div>
                                        {i < PIPE_STEPS.length - 1 && <span className="pipe-arrow" />}
                                    </React.Fragment>
                                );
                            })}
                        </div>
                    </div>

                    {files.length > 0 && (() => {
                        const total = files.length;
                        const okN = files.filter(f => f.status === "success").length;
                        const failN = files.filter(f => f.status === "error").length;
                        const pendingN = files.filter(f => f.status === "pending").length;
                        const settledN = okN + failN;
                        const pct = total ? (settledN / total) * 100 : 0;
                        const elapsedMs = batchStartRef.current ? performance.now() - batchStartRef.current : 0;
                        const avgMs = okN > 0 ? elapsedMs / okN : null;
                        const etaMs = avgMs ? avgMs * (total - settledN) : null;
                        const now = performance.now();
                        return (
                            <div className="mt-6">
                                <div className="flex items-center gap-2 mb-3">
                                    <div className="text-[11px] uppercase tracking-widest text-ink-500 font-semibold">Batch queue</div>
                                    <div className="h-px flex-1 bg-ink-200" />
                                    <div className="text-[11px] text-ink-500 tabular">
                                        {settledN}/{total} done
                                        {failN > 0 && <span className="text-red-600"> · {failN} failed</span>}
                                        {pendingN > 0 && <span> · {pendingN} queued</span>}
                                    </div>
                                </div>
                                <Card padding="p-0">
                                    <div className="px-4 pt-4 pb-3">
                                        <div className="flex items-center justify-between text-[11px] text-ink-500 tabular mb-2">
                                            <div>
                                                <span className="text-ink-900 font-semibold">{Math.round(pct)}%</span>
                                                <span className="ml-2">{settledN} of {total} files</span>
                                            </div>
                                            <div className="flex items-center gap-2">
                                                <span>{fmtSecs(elapsedMs)} elapsed</span>
                                                {running && etaMs != null && etaMs > 0 && <span className="text-ink-400">· ~{fmtSecs(etaMs)} left</span>}
                                                {!running && okN > 0 && avgMs != null && <span className="text-ink-400">· {fmtSecs(avgMs)} avg</span>}
                                            </div>
                                        </div>
                                        <div className="h-1.5 rounded-full bg-ink-100 overflow-hidden">
                                            <div
                                                className={`h-full transition-all duration-300 ${failN && !running ? "bg-amber-500" : "bg-brand-500"}`}
                                                style={{ width: `${pct}%` }}
                                            />
                                        </div>
                                    </div>
                                    <ul className="divide-y divide-ink-100 border-t border-ink-100">
                                        {files.map((f, i) => {
                                            const isActive = i === activeIdx;
                                            const typeColor = f.type === "pdf"
                                                ? "bg-rose-50 text-rose-600"
                                                : f.type === "xlsx"
                                                ? "bg-emerald-50 text-emerald-600"
                                                : "bg-ink-100 text-ink-500";
                                            const typeIcon = f.type === "pdf" ? "file" : "file_spread";
                                            const elapsed =
                                                f.startedAt && f.finishedAt ? fmtSecs(f.finishedAt - f.startedAt)
                                                : f.startedAt ? `${fmtSecs(now - f.startedAt)}…`
                                                : null;
                                            return (
                                                <li
                                                    key={i}
                                                    className={`flex items-center gap-3 px-4 py-3 transition-colors ${isActive ? "bg-brand-50/60" : ""}`}
                                                >
                                                    <div className={`w-9 h-9 rounded-lg flex items-center justify-center shrink-0 ${typeColor}`}>
                                                        <Icon name={typeIcon} size={16} />
                                                    </div>
                                                    <div className="min-w-0 flex-1">
                                                        <div className="flex items-center gap-2">
                                                            <div className="text-[13px] font-medium text-ink-900 truncate" title={f.filename}>
                                                                {f.filename}
                                                            </div>
                                                            <span className="text-[9.5px] uppercase tracking-wider font-bold text-ink-400 shrink-0">
                                                                {f.type === "other" ? "?" : f.type}
                                                            </span>
                                                        </div>
                                                        <div className="text-[11px] text-ink-500 tabular mt-0.5">
                                                            {humanSize(f.size)}
                                                            {f.counts && (
                                                                <span> · +{f.counts.entities} ent · +{f.counts.humans} hum · +{f.counts.contacts} con</span>
                                                            )}
                                                            {elapsed && <span className="text-ink-400"> · {elapsed}</span>}
                                                            {f.err && <span className="text-red-600 block sm:inline"> · {f.err}</span>}
                                                        </div>
                                                    </div>
                                                    <div className="flex items-center gap-2 shrink-0">
                                                        {f.status === "pending" && <Chip kind="neutral">Queued</Chip>}
                                                        {f.status === "running" && <Chip kind="brand">Running</Chip>}
                                                        {f.status === "success" && <Chip kind="success" icon="check">Done</Chip>}
                                                        {f.status === "error"   && <Chip kind="danger" icon="alert">Failed</Chip>}
                                                        {f.status === "pending" && !running && (
                                                            <button
                                                                type="button"
                                                                onClick={() => removeFile(i)}
                                                                className="text-[11px] text-ink-400 hover:text-red-600 px-1.5 py-0.5 rounded transition-colors focus-ring"
                                                                aria-label={`Remove ${f.filename} from queue`}
                                                            >
                                                                Remove
                                                            </button>
                                                        )}
                                                        {f.status === "error" && !running && (
                                                            <button
                                                                type="button"
                                                                onClick={() => retryFile(i)}
                                                                className="text-[11px] text-brand-600 hover:text-brand-700 hover:underline px-1.5 py-0.5 rounded transition-colors focus-ring"
                                                                aria-label={`Retry ${f.filename}`}
                                                            >
                                                                Retry
                                                            </button>
                                                        )}
                                                    </div>
                                                </li>
                                            );
                                        })}
                                    </ul>
                                </Card>
                            </div>
                        );
                    })()}

                    {err && (
                        <Card className="mt-6 border-red-300 bg-red-50">
                            <div className="flex items-start gap-2">
                                <Icon name="alert" size={16} className="text-red-600 mt-0.5" />
                                <div>
                                    <div className="text-red-800 font-semibold text-[13.5px]">Ingestion failed</div>
                                    <div className="text-red-700 text-[12.5px] mt-1 break-words">{err}</div>
                                    <div className="text-red-600 text-[11.5px] mt-2">If the app was updated, hard-refresh the page (⌘⇧R) and try again.</div>
                                </div>
                            </div>
                        </Card>
                    )}
                    {summary && (
                        <Card className="mt-6 border-emerald-200 bg-emerald-50">
                            <div className="flex items-center gap-2 text-emerald-800 font-semibold mb-1">
                                <Icon name="check" size={16} /> Ingestion complete
                            </div>
                            <div className="text-sm text-emerald-700">
                                <b>{summary.counts?.entities}</b> entities · <b>{summary.counts?.humans}</b> humans · <b>{summary.counts?.contacts}</b> contacts · <b>{summary.counts?.relationships}</b> relationships · <b>{summary.counts?.markets}</b> markets loaded.
                            </div>
                            <div className="mt-3 flex gap-2">
                                <Button variant="primary" size="sm" icon="map" onClick={() => onNavigate("mapping")}>Review mapping</Button>
                                <Button variant="secondary" size="sm" icon="git_merge" onClick={() => onNavigate("dedup")}>See dedupes</Button>
                            </div>
                        </Card>
                    )}

                    {sheets.length > 0 && (
                        <div className="mt-8">
                            <div className="flex items-center gap-2 mb-3">
                                <div className="text-[11px] uppercase tracking-widest text-ink-500 font-semibold">Detected sheets</div>
                                <div className="h-px flex-1 bg-ink-200" />
                            </div>
                            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                                {sheets.map(sh => <SheetCard key={sh.name} sh={sh} />)}
                            </div>
                        </div>
                    )}
                </div>

                <div>
                    <Card padding="p-5">
                        <div className="flex items-center gap-2 mb-3">
                            <div className="text-[11px] uppercase tracking-widest text-ink-500 font-semibold">Source connectors</div>
                        </div>
                        <div className="space-y-2.5">
                            {CONNECTORS.map(c => (
                                <div key={c.id} className="flex items-center justify-between py-1">
                                    <div className="flex items-center gap-2.5">
                                        <div className="w-7 h-7 rounded-md bg-ink-100 text-ink-500 flex items-center justify-center"><Icon name={c.kind === "AMS" ? "plug" : "file"} size={13} /></div>
                                        <div>
                                            <div className="text-[13px] text-ink-900 font-medium">{c.label}</div>
                                            <div className="text-[11px] text-ink-500">{c.kind}</div>
                                        </div>
                                    </div>
                                    {c.state === "active" ? <Chip kind="success" icon="check">Active</Chip> : <Chip kind="neutral">Coming soon</Chip>}
                                </div>
                            ))}
                        </div>
                    </Card>
                </div>
            </div>
        </div>
    );
}

/* ======================== 9. Panel: Mapping ===================== */

function MappingPanel() {
    const [rows, setRows] = useState(null);
    const [filter, setFilter] = useState("all");
    const [q, setQ] = useState("");
    const [selected, setSelected] = useState(null);
    const [explain, setExplain] = useState(null);
    const toast = useToast();

    useEffect(() => { api("/api/mapping").then(r => setRows(r.rows)); }, []);

    const filtered = useMemo(() => {
        if (!rows) return [];
        let out = rows;
        if (filter !== "all") out = out.filter(r => r.status === filter);
        if (q.trim()) out = out.filter(r => (r.source_field || "").toLowerCase().includes(q.toLowerCase()) || (r.sample_value || "").toLowerCase().includes(q.toLowerCase()));
        return out;
    }, [rows, filter, q]);

    const counts = useMemo(() => {
        if (!rows) return { all: 0, ok: 0, warn: 0, error: 0 };
        return {
            all: rows.length,
            ok: rows.filter(r => r.status === "ok").length,
            warn: rows.filter(r => r.status === "warn").length,
            error: rows.filter(r => r.status === "error").length,
        };
    }, [rows]);

    const openRow = async (row) => {
        setSelected(row);
        setExplain({ text: "Generating explanation…", source: "loading" });
        try {
            const r = await api(`/api/mapping/${row.issue_id}/explain`);
            setExplain(r);
        } catch { setExplain({ text: "Unable to fetch explanation.", source: "error" }); }
    };

    if (!rows) return <Skeleton lines={10} height="h-6" className="space-y-2" />;

    const FilterBtn = ({ v, children, k }) => (
        <button onClick={() => setFilter(v)} className={`px-3 h-8 text-[13px] rounded-lg border ${filter === v ? "bg-ink-900 border-ink-900 text-white" : "border-ink-200 bg-white text-ink-600 hover:bg-ink-50"}`}>
            {children} <span className={`ml-1.5 tabular ${filter === v ? "text-ink-300" : "text-ink-400"}`}>{k}</span>
        </button>
    );

    return (
        <div>
            <PageHeader
                eyebrow="Data quality"
                title="AI schema mapping"
                subtitle={`${counts.all} fields mapped · ${counts.error} errors · ${counts.warn} warnings. The AI suggests a fix for every violation of the canonical schema.`}
                actions={<>
                    <Button variant="secondary" size="sm" icon="download">Export report</Button>
                    <Button variant="primary" size="sm" icon="sparkle" onClick={() => toast.push("Bulk fix queued", "info", `${counts.error} enum fixes sent to the review queue`)}>Accept all AI suggestions</Button>
                </>}
            />

            <div className="flex items-center gap-2 mb-4 flex-wrap">
                <FilterBtn v="all" k={counts.all}>All</FilterBtn>
                <FilterBtn v="ok" k={counts.ok}>OK</FilterBtn>
                <FilterBtn v="warn" k={counts.warn}>Warnings</FilterBtn>
                <FilterBtn v="error" k={counts.error}>Errors</FilterBtn>
                <div className="relative ml-auto">
                    <Icon name="search" size={14} className="text-ink-400 absolute left-3 top-1/2 -translate-y-1/2" />
                    <input value={q} onChange={e => setQ(e.target.value)} placeholder="Search fields or values…" className="h-8 pl-8 pr-3 w-64 border border-ink-200 rounded-lg text-[13px] bg-white focus:outline-none focus:ring-2 focus:ring-brand-500 focus:border-transparent" />
                </div>
            </div>

            <Card padding="p-0" className="overflow-hidden">
                <table className="data-table">
                    <thead>
                        <tr>
                            <th>Source field</th>
                            <th>IRYSCLOUD canonical</th>
                            <th>Sample</th>
                            <th>Status</th>
                            <th>AI note</th>
                            <th className="text-right">Confidence</th>
                        </tr>
                    </thead>
                    <tbody>
                        {filtered.map(r => (
                            <tr key={r.issue_id} className="cursor-pointer" onClick={() => openRow(r)}>
                                <td className="mono text-[12.5px] text-ink-800">{r.source_field}</td>
                                <td className="mono text-[12.5px] text-ink-600">{r.canonical_field || <span className="text-ink-300">—</span>}</td>
                                <td className="text-ink-700">{r.sample_value || <span className="text-ink-300">—</span>}</td>
                                <td>
                                    {r.status === "ok" && <Chip kind="success" icon="check">OK</Chip>}
                                    {r.status === "warn" && <Chip kind="warn" icon="alert">Warning</Chip>}
                                    {r.status === "error" && <Chip kind="danger" icon="alert">Error</Chip>}
                                </td>
                                <td className="text-ink-600 text-[12.5px]">
                                    {r.suggested_fix
                                        ? <span><span className="mono text-red-600">{r.sample_value}</span> <Icon name="chev_right" size={11} className="inline text-ink-400" /> <span className="mono text-emerald-700">{r.suggested_fix}</span> <span className="text-ink-400">· {r.record_ref}</span></span>
                                        : r.note}
                                </td>
                                <td className="text-right mono text-[12.5px] tabular text-ink-700">{((r.confidence || 0) * 100).toFixed(0)}%</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
                {filtered.length === 0 && <EmptyState icon="check" title="No issues match" body="Adjust the filter or the search query." />}
            </Card>

            <Drawer open={!!selected} onClose={() => setSelected(null)} subtitle="Mapping detail" title={selected?.source_field || ""}>
                {selected && (
                    <>
                        <div className="grid grid-cols-2 gap-3 mb-6">
                            <DetailTile label="Canonical" value={selected.canonical_field || "—"} mono />
                            <DetailTile label="Confidence" value={<span className="tabular">{((selected.confidence||0)*100).toFixed(0)}%</span>} />
                            <DetailTile label="Sample value" value={selected.sample_value || "—"} />
                            <DetailTile label="Record" value={selected.record_ref || "—"} />
                        </div>

                        {selected.suggested_fix && (
                            <Card className="mb-6 bg-gradient-to-br from-brand-50 to-transparent border border-brand-100">
                                <div className="text-[11px] uppercase tracking-widest text-brand-700 font-semibold mb-2">Suggested fix</div>
                                <div className="flex items-center gap-3 text-[14px]">
                                    <code className="mono px-2 py-1 bg-white border border-red-200 text-red-700 rounded">{selected.sample_value}</code>
                                    <Icon name="chev_right" size={14} className="text-ink-400" />
                                    <code className="mono px-2 py-1 bg-white border border-emerald-200 text-emerald-700 rounded">{selected.suggested_fix}</code>
                                </div>
                                <div className="flex gap-2 mt-4">
                                    <Button variant="primary" size="sm" icon="check" onClick={() => toast.push("Fix applied", "success", "Canonical value updated and audited")}>Apply fix</Button>
                                    <Button variant="secondary" size="sm" icon="corner" onClick={() => toast.push("Sent to HITL", "info", "A reviewer will decide")}>Send to review</Button>
                                </div>
                            </Card>
                        )}

                        <div>
                            <div className="flex items-center gap-2 mb-2">
                                <div className="text-[11px] uppercase tracking-widest text-ink-500 font-semibold">AI explanation</div>
                                {explain?.source && <Chip kind="brand" icon="sparkle">{explain.source}</Chip>}
                            </div>
                            <Card padding="p-4" className="text-[13.5px] leading-relaxed">
                                <div className="md" dangerouslySetInnerHTML={{ __html: explain ? marked.parse(explain.text || "") : "" }} />
                            </Card>
                        </div>
                    </>
                )}
            </Drawer>
        </div>
    );
}

const DetailTile = ({ label, value, mono }) => (
    <div className="rounded-lg border border-ink-200 bg-ink-50/40 p-3">
        <div className="text-[10.5px] uppercase tracking-widest text-ink-500 font-semibold">{label}</div>
        <div className={`mt-1 text-[13.5px] text-ink-900 ${mono ? "mono" : ""}`}>{value}</div>
    </div>
);

/* ======================== 10. Panel: Dedup ====================== */

function signalIcon(sig) {
    if (/SSN|ssn/.test(sig)) return "lock";
    if (/FEIN|fein/.test(sig)) return "lock";
    if (/DOB|birth/i.test(sig)) return "clock";
    if (/relationship|employer|shared/i.test(sig)) return "share";
    if (/TF-IDF|tfidf/i.test(sig)) return "sparkle";
    if (/name/i.test(sig)) return "user";
    return "check";
}

function DedupPanel({ onChange }) {
    const [clusters, setClusters] = useState(null);
    const [filter, setFilter] = useState("all");
    const toast = useToast();
    const load = () => api("/api/dedup").then(r => setClusters(r.clusters));
    useEffect(() => { load(); }, []);
    if (!clusters) return <Skeleton lines={8} height="h-14" />;

    const filtered = clusters.filter(c =>
        filter === "all" ? true :
        filter === "auto" ? c.auto_merged :
        filter === "hitl" ? c.status === "hitl" : true
    );

    const override = async (id) => {
        await api(`/api/dedup/${id}/override`, { method: "POST" });
        await load();
        toast.push("Routed to HITL", "info", `Cluster #${id} sent for human review`);
        onChange && onChange();
    };

    return (
        <div>
            <PageHeader
                eyebrow="Data quality"
                title="Entity & human deduplication"
                subtitle={`${clusters.length} clusters · ${clusters.filter(c => c.auto_merged).length} auto-merged above threshold · ${clusters.filter(c => c.status === "hitl").length} routed to HITL. Every merge is reversible.`}
            />

            <div className="flex items-center gap-2 mb-4">
                {[["all","All"],["auto","Auto-merged"],["hitl","Needs review"]].map(([k, l]) => (
                    <button key={k} onClick={() => setFilter(k)} className={`px-3 h-8 text-[13px] rounded-lg border ${filter === k ? "bg-ink-900 border-ink-900 text-white" : "border-ink-200 bg-white text-ink-600 hover:bg-ink-50"}`}>{l}</button>
                ))}
            </div>

            <div className="space-y-4">
                {filtered.map(c => (
                    <Card key={c.cluster_id} padding="p-5">
                        <div className="flex items-start gap-5">
                            <ConfidenceRing value={c.confidence} />
                            <div className="flex-1 min-w-0">
                                <div className="flex items-center gap-2 flex-wrap">
                                    <div className="text-[11px] uppercase tracking-widest text-ink-500 font-semibold">
                                        {c.kind === "entity" ? "Entity cluster" : "Human cluster"} · #{c.cluster_id}
                                    </div>
                                    {c.auto_merged ? <Chip kind="success" icon="check">Auto-merged</Chip> : <Chip kind="warn" icon="alert">HITL review</Chip>}
                                </div>
                                <div className="text-[15px] font-semibold text-ink-900 mt-0.5">
                                    Winning record: <span className="mono text-brand-700">{c.winner_ref}</span>
                                </div>
                                <div className="mt-2 flex flex-wrap gap-1.5">
                                    {c.signals.slice(0, 8).map((s, i) => <Chip key={i} kind="brand" icon={signalIcon(s)}>{s}</Chip>)}
                                </div>
                            </div>
                            <div className="flex items-center gap-2">
                                {c.status !== "hitl" && <Button variant="secondary" size="sm" icon="corner" onClick={() => override(c.cluster_id)}>Override</Button>}
                                <Button variant="ghost" size="sm" icon="eye">Preview merge</Button>
                            </div>
                        </div>

                        <div className="mt-5 grid grid-cols-1 md:grid-cols-3 gap-3">
                            {c.members.map((m, i) => (
                                <div key={i} className={`rounded-lg border border-ink-200 p-3.5 text-[12.5px] ${i === 0 ? "bg-emerald-50/40 border-emerald-200" : "bg-ink-50/40"}`}>
                                    <div className="flex items-center justify-between mb-1.5">
                                        <div className="mono text-[10.5px] text-ink-500">{m.entityIdentifier || m.humanIdentifier}</div>
                                        {i === 0 && <Chip kind="success">Winner</Chip>}
                                    </div>
                                    <div className="font-semibold text-ink-900 truncate">
                                        {m.name || `${m.firstName || ""} ${m.lastName || ""}`.trim()}
                                    </div>
                                    {m.doingBusinessAs && <div className="text-ink-600 mt-0.5">DBA: {m.doingBusinessAs}</div>}
                                    {m.fein && <div className="text-ink-600 mt-0.5 flex items-center gap-1">FEIN: <PII field="fein" value={m.fein} /></div>}
                                    {m.dateOfBirth && <div className="text-ink-600 mt-0.5 flex items-center gap-1">DOB: <PII field="dateOfBirth" value={m.dateOfBirth} /></div>}
                                    {m.occupation && <div className="text-ink-600 mt-0.5">{m.occupation}</div>}
                                </div>
                            ))}
                        </div>
                    </Card>
                ))}
            </div>

            {filtered.length === 0 && <EmptyState icon="git_merge" title="No clusters match" body="Everything's clean at this filter level." />}
        </div>
    );
}

/* ======================== 11. Panel: Graph ====================== */

function GraphPanel() {
    const hostRef = useRef();
    const [net, setNet] = useState(null);
    const [selected, setSelected] = useState(null);
    const [loading, setLoading] = useState(true);
    const dataRef = useRef(null);

    useEffect(() => {
        let n;
        api("/api/graph").then(data => {
            setLoading(false);
            dataRef.current = data;
            n = new vis.Network(hostRef.current, data, {
                physics: { solver: "forceAtlas2Based", forceAtlas2Based: { gravitationalConstant: -55, centralGravity: 0.012, springLength: 110, springConstant: 0.08 }, stabilization: { iterations: 220 } },
                interaction: { hover: true, tooltipDelay: 160 },
                nodes: { borderWidth: 1, shadow: { enabled: true, size: 6, color: "rgba(15,23,42,.08)" }, font: { size: 12, face: "Inter var, Inter, sans-serif" } },
                edges: { smooth: { type: "continuous" }, font: { align: "middle", size: 10, face: "Inter var, Inter, sans-serif", color: "#64748b" }, width: 1.2 },
            });
            setNet(n);
            n.on("selectNode", p => {
                const id = p.nodes[0];
                const node = data.nodes.find(nn => nn.id === id);
                const ego = data.edges.filter(e => e.from === id || e.to === id);
                setSelected({ node, edges: ego });
            });
            n.on("deselectNode", () => setSelected(null));
        });
        return () => n && n.destroy();
    }, []);

    const focus = (label) => {
        if (!net || !dataRef.current) return;
        const match = dataRef.current.nodes.find(n => (n.label || "").toLowerCase().includes(label.toLowerCase()));
        if (match) { net.selectNodes([match.id]); net.focus(match.id, { scale: 1.15, animation: true });
            setSelected({ node: match, edges: dataRef.current.edges.filter(e => e.from === match.id || e.to === match.id) });
        }
    };

    const LEGEND = [
        { label: "Entity",      color: "#1e293b", shape: "box" },
        { label: "Human",       color: "#6366f1", shape: "ellipse" },
        { label: "Spouse of",   color: "#be185d" },
        { label: "Employee of", color: "#0ea5e9" },
        { label: "Co-Worker",   color: "#16a34a" },
        { label: "Owner of",    color: "#ef4444" },
        { label: "Board",       color: "#f59e0b" },
    ];

    return (
        <div>
            <PageHeader
                eyebrow="Data quality"
                title="Relationship graph"
                subtitle="Every person and business IRYSCLOUD discovered, and the edges between them. Click any node to see its ego network."
                actions={<>
                    <Button variant="secondary" size="sm" icon="refresh" onClick={() => net && net.fit()}>Reset view</Button>
                    <Button variant="primary" size="sm" icon="download">Export PNG</Button>
                </>}
            />

            <div className="flex items-center gap-2 mb-3 flex-wrap">
                <Button variant="secondary" size="sm" icon="target" onClick={() => focus("Laura Kline")}>Laura Kline</Button>
                <Button variant="secondary" size="sm" icon="target" onClick={() => focus("Chen")}>Chen household</Button>
                <Button variant="secondary" size="sm" icon="target" onClick={() => focus("Annapolis Parks")}>Annapolis Parks</Button>
                <div className="ml-auto flex items-center gap-2.5 flex-wrap">
                    {LEGEND.map(l => <span key={l.label} className="inline-flex items-center gap-1.5 text-[11.5px] text-ink-600"><span className="dot" style={{ background: l.color }} />{l.label}</span>)}
                </div>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-[1fr_320px] gap-4">
                <div>
                    {loading && <Skeleton className="h-80" />}
                    <div className="graph-canvas" ref={hostRef} />
                </div>
                <div>
                    {selected ? (
                        <Card padding="p-5">
                            <div className="text-[11px] uppercase tracking-widest text-ink-500 font-semibold mb-1">Selected</div>
                            <div className="text-lg font-semibold text-ink-900">{selected.node?.label}</div>
                            <div className="text-[12.5px] text-ink-500 mt-0.5">{selected.node?.title}</div>
                            <div className="text-[10.5px] uppercase tracking-widest text-ink-500 font-semibold mt-4 mb-2">Ego network · {selected.edges.length} edges</div>
                            <ul className="space-y-1.5 text-[13px] max-h-[460px] overflow-y-auto">
                                {selected.edges.map((e, i) => (
                                    <li key={i} className="flex items-center gap-1.5 text-ink-700">
                                        <Icon name="git_branch" size={12} className="text-ink-400 flex-shrink-0" />
                                        <span className="mono text-[12px] text-ink-600 truncate">{e.from.split("::")[1]}</span>
                                        <span className="text-ink-400">→</span>
                                        <Chip kind="brand">{e.label}</Chip>
                                        <span className="text-ink-400">→</span>
                                        <span className="mono text-[12px] text-ink-600 truncate">{e.to.split("::")[1]}</span>
                                    </li>
                                ))}
                            </ul>
                        </Card>
                    ) : (
                        <Card padding="p-5"><EmptyState icon="share" title="Nothing selected" body="Click any node in the graph to inspect its relationships." /></Card>
                    )}
                </div>
            </div>
        </div>
    );
}

/* ======================== 12. Panel: Review ===================== */

function ReviewPanel({ onChange }) {
    const [items, setItems] = useState(null);
    const [sel, setSel] = useState(0);
    const [filter, setFilter] = useState("open");
    const toast = useToast();
    const load = () => api("/api/review").then(r => setItems(r.items));
    useEffect(() => { load(); }, []);

    const filtered = useMemo(() => (items || []).filter(i => filter === "all" ? true : i.status === filter), [items, filter]);
    const active = filtered[Math.min(sel, filtered.length - 1)];

    useKeyboard("j", () => setSel(s => Math.min((filtered.length || 1) - 1, s + 1)), [filtered.length]);
    useKeyboard("k", () => setSel(s => Math.max(0, s - 1)), []);
    useKeyboard("y", () => active && active.status === "open" && decide(active.review_id, "accept"), [active]);
    useKeyboard("n", () => active && active.status === "open" && decide(active.review_id, "reject"), [active]);

    const decide = async (id, decision) => {
        const reason = prompt("Reason? (required for audit)", decision === "accept" ? "Approved after review" : "Rejected — needs additional verification");
        if (!reason) return;
        await api(`/api/review/${id}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ decision, reason }),
        });
        await load();
        toast.push(`Decision: ${decision}`, decision === "accept" ? "success" : "warn", `Review #${id} · logged to audit`);
        onChange && onChange();
    };

    if (!items) return <Skeleton lines={6} height="h-12" />;

    const kindLabel = (k) => ({ enum_fix: "Enum fix", dedupe_low_conf: "Dedupe (low confidence)", dedupe_override: "Dedupe override", ocr_failure: "OCR failure" }[k] || k);

    return (
        <div>
            <PageHeader
                eyebrow="Humans in the loop"
                title="Review queue"
                subtitle={`${items.filter(i => i.status === "open").length} open decisions · ${items.filter(i => i.status === "closed").length} closed. Use J/K to navigate, Y/N to decide.`}
                actions={<>
                    <Button variant="secondary" size="sm" icon="download">Export</Button>
                </>}
            />

            <div className="flex items-center gap-2 mb-4">
                {[["open","Open"],["closed","Closed"],["all","All"]].map(([k, l]) => (
                    <button key={k} onClick={() => { setFilter(k); setSel(0); }} className={`px-3 h-8 text-[13px] rounded-lg border ${filter === k ? "bg-ink-900 border-ink-900 text-white" : "border-ink-200 bg-white text-ink-600 hover:bg-ink-50"}`}>{l}</button>
                ))}
                <div className="ml-auto flex items-center gap-1.5 text-[11.5px] text-ink-500">
                    <Kbd>J</Kbd> / <Kbd>K</Kbd> navigate · <Kbd>Y</Kbd> accept · <Kbd>N</Kbd> reject
                </div>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-[360px_1fr] gap-4">
                <Card padding="p-0" className="overflow-hidden max-h-[72vh] overflow-y-auto">
                    {filtered.length === 0 && <EmptyState icon="check" title="No items" body="Nothing waiting." />}
                    {filtered.map((it, i) => (
                        <div key={it.review_id} onClick={() => setSel(i)} className={`px-4 py-3 border-b border-ink-100 cursor-pointer ${i === sel ? "bg-brand-50/60 border-l-2 border-l-brand-600" : "hover:bg-ink-50"}`}>
                            <div className="flex items-center gap-2 mb-1">
                                <div className="text-[10.5px] uppercase tracking-widest text-ink-500 font-semibold">{kindLabel(it.kind)}</div>
                                <span className={`chip chip-${it.status === "open" ? "warn" : "neutral"} ml-auto`}>{it.status}</span>
                            </div>
                            <div className="text-[13px] text-ink-900 font-medium leading-snug">{it.title}</div>
                            <div className="text-[11px] text-ink-400 mt-1 tabular">{it.created_ts}</div>
                        </div>
                    ))}
                </Card>

                <Card padding="p-5">
                    {active ? <>
                        <div className="flex items-start justify-between mb-4">
                            <div>
                                <div className="text-[11px] uppercase tracking-widest text-brand-700 font-semibold">{kindLabel(active.kind)}</div>
                                <div className="text-[18px] font-semibold text-ink-900 mt-1">{active.title}</div>
                                <div className="text-[12px] text-ink-500 mt-1">Review #{active.review_id} · created {active.created_ts}</div>
                            </div>
                            <Chip kind={active.status === "open" ? "warn" : "neutral"}>{active.status}</Chip>
                        </div>

                        {active.kind === "enum_fix" && active.payload?.suggested_fix && (
                            <div className="mb-4 p-4 rounded-lg bg-gradient-to-br from-brand-50 to-transparent border border-brand-100">
                                <div className="text-[11px] uppercase tracking-widest text-brand-700 font-semibold mb-2">Suggested fix</div>
                                <div className="flex items-center gap-3 text-[14px]">
                                    <code className="mono px-2 py-1 bg-white border border-red-200 text-red-700 rounded">{active.payload.sample_value}</code>
                                    <Icon name="chev_right" size={14} className="text-ink-400" />
                                    <code className="mono px-2 py-1 bg-white border border-emerald-200 text-emerald-700 rounded">{active.payload.suggested_fix}</code>
                                    <span className="ml-2 text-[12px] text-ink-500">conf {(active.payload.confidence*100).toFixed(0)}%</span>
                                </div>
                                <div className="mt-2 text-[12.5px] text-ink-600">Affected: <b>{active.payload.record_ref}</b></div>
                                <div className="mt-1 text-[12px] text-ink-500">{active.payload.note}</div>
                            </div>
                        )}

                        {active.kind === "dedupe_low_conf" && active.payload?.members && (
                            <div className="mb-4">
                                <div className="text-[11px] uppercase tracking-widest text-ink-500 font-semibold mb-2">Candidates</div>
                                <div className="grid grid-cols-2 gap-3">
                                    {active.payload.members.map((m, i) => (
                                        <div key={i} className={`rounded-lg p-3 text-[12.5px] border ${i === 0 ? "diff-left" : "diff-right"}`}>
                                            <div className="mono text-[10.5px] text-ink-600">{m.humanIdentifier}</div>
                                            <div className="font-semibold mt-0.5">{m.firstName} {m.lastName}</div>
                                            <div className="text-ink-600 mt-0.5">DOB: <PII field="dateOfBirth" value={m.dateOfBirth} /></div>
                                            {m.occupation && <div className="text-ink-600 mt-0.5">{m.occupation}</div>}
                                        </div>
                                    ))}
                                </div>
                                <div className="mt-3 text-[12px] text-ink-500">Confidence: <b>{(active.payload.confidence*100).toFixed(0)}%</b></div>
                                <div className="mt-1 flex flex-wrap gap-1.5">{active.payload.signals?.map((s, i) => <Chip key={i} kind="brand" icon={signalIcon(s)}>{s}</Chip>)}</div>
                            </div>
                        )}

                        {active.kind === "ocr_failure" && (
                            <div className="mb-4 p-4 rounded-lg bg-red-50 border border-red-200">
                                <div className="flex items-center gap-2 text-red-700 font-medium mb-1"><Icon name="alert" size={14} />{active.payload.file}</div>
                                <div className="text-[12.5px] text-red-800">Page {active.payload.page} · OCR confidence {(active.payload.confidence * 100).toFixed(0)}%</div>
                                <div className="text-[12.5px] text-red-700 mt-1">{active.payload.reason}</div>
                            </div>
                        )}

                        {active.status === "open" ? (
                            <div className="flex gap-2">
                                <Button variant="primary" size="md" icon="check" onClick={() => decide(active.review_id, "accept")}>Accept <Kbd>Y</Kbd></Button>
                                <Button variant="secondary" size="md" icon="x" onClick={() => decide(active.review_id, "reject")}>Reject <Kbd>N</Kbd></Button>
                                <Button variant="ghost" size="md" icon="refresh" onClick={() => decide(active.review_id, "rollback")}>Rollback</Button>
                            </div>
                        ) : (
                            <Card padding="p-3" className="bg-ink-50">
                                <div className="text-[12.5px]"><span className="text-ink-500">Decision: </span><b>{active.decision}</b> — {active.reason}</div>
                                <div className="text-[11px] text-ink-400 mt-1">Closed {active.decided_ts}</div>
                            </Card>
                        )}
                    </> : <EmptyState icon="users" title="Select an item" body="Pick a review on the left to see details." />}
                </Card>
            </div>
        </div>
    );
}

/* ======================== 13. Panel: DAG ========================= */

const DAG_STEPS = [
    { id: "parse",    label: "parse",    icon: "file" },
    { id: "classify", label: "classify", icon: "target" },
    { id: "map",      label: "map",      icon: "map" },
    { id: "dedupe",   label: "dedupe",   icon: "git_merge" },
    { id: "graph",    label: "graph",    icon: "share" },
    { id: "validate", label: "validate", icon: "check" },
    { id: "load",     label: "load",     icon: "download" },
];

function DagPanel() {
    const [steps, setSteps] = useState([]);
    const [running, setRunning] = useState(false);
    const [total, setTotal] = useState(null);
    const [history, setHistory] = useState([]);

    const run = () => {
        setSteps([]); setTotal(null); setRunning(true);
        const es = new EventSource("/api/dag/stream");
        es.addEventListener("step", e => {
            const d = JSON.parse(e.data);
            setSteps(prev => {
                const other = prev.filter(s => s.id !== d.id);
                return [...other, d].sort((a, b) => DAG_STEPS.findIndex(x => x.id === a.id) - DAG_STEPS.findIndex(x => x.id === b.id));
            });
        });
        es.addEventListener("end", e => {
            const t = JSON.parse(e.data).wall_ms;
            setTotal(t); setRunning(false); es.close();
            setHistory(h => [{ ts: new Date().toLocaleTimeString(), duration: t, status: "success" }, ...h].slice(0, 6));
        });
        es.onerror = () => { setRunning(false); es.close(); };
    };

    return (
        <div>
            <PageHeader
                eyebrow="Ingestion"
                title="DAG runner"
                subtitle="Every ingestion is a DAG. Watch it run step-by-step, with duration and record counts streamed via Server-Sent Events."
                actions={<>
                    <Button variant="primary" size="md" icon={running ? "pause" : "play"} disabled={running} onClick={run}>{running ? "Running…" : "Run DAG"}</Button>
                </>}
            />

            <div className="grid grid-cols-1 lg:grid-cols-[1fr_280px] gap-4">
                <div>
                    <Card padding="p-6">
                        <div className="flex items-center gap-1 overflow-x-auto pb-3">
                            {DAG_STEPS.map((s, i) => {
                                const st = steps.find(x => x.id === s.id);
                                const status = st?.status || "pending";
                                return (
                                    <React.Fragment key={s.id}>
                                        <div className={`pipe-node pipe-${status} flex-shrink-0 min-w-[170px]`}>
                                            <div className="flex items-center gap-2 mb-1">
                                                <Icon name={s.icon} size={14} />
                                                <div className="font-semibold text-[13px] text-ink-900">{s.label}</div>
                                            </div>
                                            <div className={`pipe-status ${status === "running" ? "pulse-dot" : ""}`}>{status}</div>
                                            {st?.duration_ms != null && <div className="text-[11px] text-ink-500 mt-1 tabular">{st.duration_ms} ms</div>}
                                            {st?.note && <div className="text-[10.5px] text-ink-600 mt-1 leading-tight line-clamp-2">{st.note}</div>}
                                        </div>
                                        {i < DAG_STEPS.length - 1 && <span className="pipe-arrow" />}
                                    </React.Fragment>
                                );
                            })}
                        </div>

                        {total != null && (
                            <div className="mt-4 pt-4 border-t border-ink-200 flex items-center gap-5">
                                <div><div className="text-[10.5px] uppercase tracking-widest text-ink-500 font-semibold">Total</div><div className="text-[22px] font-semibold tabular">{total} ms</div></div>
                                <div><div className="text-[10.5px] uppercase tracking-widest text-ink-500 font-semibold">Steps</div><div className="text-[22px] font-semibold tabular">{steps.filter(s => s.status === "success").length}/{DAG_STEPS.length}</div></div>
                                <div><div className="text-[10.5px] uppercase tracking-widest text-ink-500 font-semibold">Status</div><div className="text-[22px] font-semibold text-emerald-600">Success</div></div>
                            </div>
                        )}
                    </Card>

                    <Card padding="p-5" className="mt-4">
                        <div className="text-[11px] uppercase tracking-widest text-ink-500 font-semibold mb-2">Per-step log</div>
                        <div className="space-y-2">
                            {steps.filter(s => s.status === "success").map(s => (
                                <div key={s.id} className="flex items-start gap-3 text-[12.5px]">
                                    <div className="mono text-[11px] text-ink-500 w-16 text-right tabular">{s.duration_ms}ms</div>
                                    <div className="dot dot-success mt-1.5" />
                                    <div>
                                        <div className="text-ink-900 font-medium">{s.title}</div>
                                        <div className="text-ink-600">{s.note}</div>
                                    </div>
                                </div>
                            ))}
                            {steps.length === 0 && <div className="text-ink-400 text-[12.5px]">Run the DAG to see per-step logs stream in.</div>}
                        </div>
                    </Card>
                </div>

                <div>
                    <Card padding="p-5">
                        <div className="text-[11px] uppercase tracking-widest text-ink-500 font-semibold mb-3">Recent runs</div>
                        {history.length === 0 && <div className="text-[12.5px] text-ink-400">Your runs will appear here.</div>}
                        {history.map((h, i) => (
                            <div key={i} className="flex items-center gap-2.5 py-2 border-b last:border-0 border-ink-100">
                                <div className="dot dot-success" />
                                <div className="text-[12.5px] flex-1">
                                    <div className="text-ink-900 font-medium">{h.ts}</div>
                                    <div className="text-ink-500 tabular">{h.duration} ms</div>
                                </div>
                                <Icon name="chev_right" size={12} className="text-ink-300" />
                            </div>
                        ))}
                    </Card>
                </div>
            </div>
        </div>
    );
}

/* ======================== 14. Panel: Audit ======================= */

function AuditPanel() {
    const [rows, setRows] = useState(null);
    const [q, setQ] = useState("");
    const [kind, setKind] = useState("all");
    useEffect(() => { api("/api/audit").then(r => setRows(r.rows)); }, []);

    if (!rows) return <Skeleton lines={10} height="h-8" />;
    const kinds = ["all", ...Array.from(new Set(rows.map(r => r.action)))];
    const filtered = rows.filter(r => (kind === "all" || r.action === kind) &&
        (q.trim() === "" || JSON.stringify(r).toLowerCase().includes(q.toLowerCase())));

    return (
        <div>
            <PageHeader
                eyebrow="Humans in the loop"
                title="Audit log"
                subtitle={`${rows.length} events · every ingestion, PII reveal, and HITL decision. Immutable, append-only.`}
                actions={<Button variant="secondary" size="sm" icon="download">Export CSV</Button>}
            />

            <div className="flex items-center gap-2 mb-4 flex-wrap">
                <select value={kind} onChange={e => setKind(e.target.value)} className="h-8 px-3 rounded-lg border border-ink-200 text-[13px] bg-white focus:ring-2 focus:ring-brand-500 focus:border-transparent focus:outline-none">
                    {kinds.map(k => <option key={k} value={k}>{k === "all" ? "All actions" : k}</option>)}
                </select>
                <div className="relative">
                    <Icon name="search" size={14} className="text-ink-400 absolute left-3 top-1/2 -translate-y-1/2" />
                    <input value={q} onChange={e => setQ(e.target.value)} placeholder="Search any field…" className="h-8 pl-8 pr-3 w-64 border border-ink-200 rounded-lg text-[13px] bg-white focus:outline-none focus:ring-2 focus:ring-brand-500 focus:border-transparent" />
                </div>
                <span className="text-[12px] text-ink-500 ml-2 tabular">{filtered.length} / {rows.length} events</span>
            </div>

            <Card padding="p-0" className="overflow-hidden">
                <table className="data-table">
                    <thead>
                        <tr>
                            <th>When</th>
                            <th>Actor</th>
                            <th>Action</th>
                            <th>Field</th>
                            <th>Target</th>
                            <th>Reason</th>
                        </tr>
                    </thead>
                    <tbody>
                        {filtered.map(r => (
                            <tr key={r.audit_id}>
                                <td className="mono text-[11.5px] text-ink-500 tabular">{r.ts}</td>
                                <td className="text-ink-800">{r.actor}</td>
                                <td><Chip kind={r.action.includes("REVEAL") ? "warn" : r.action.includes("ACCEPT") ? "success" : r.action.includes("REJECT") ? "danger" : "neutral"}>{r.action}</Chip></td>
                                <td className="mono text-[12px] text-ink-600">{r.field || <span className="text-ink-300">—</span>}</td>
                                <td className="text-ink-700">{r.target || <span className="text-ink-300">—</span>}</td>
                                <td className="text-ink-600 text-[12.5px]">{r.reason || <span className="text-ink-300">—</span>}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
                {filtered.length === 0 && <EmptyState icon="shield" title="No audit events match" body="Clear the filter or broaden the search." />}
            </Card>
        </div>
    );
}

/* ======================== 15. Panel: Agent ======================= */

const PROMPT_GROUPS = [
    { label: "Submission & quoting", items: [
        "Prepare a commercial liability quote package for Coastal Grille & Burgers. Prefer carriers we have active appointments with.",
        "Draft the underwriter cover email for the Stonefield Confections renewal — cite only facts present in our migrated records. No speculation.",
        "Which of our active carriers best fits a $48M Stonefield Confections submission? Justify from appointment history, not hunches.",
    ]},
    { label: "Portfolio intelligence", items: [
        "Which active carrier appointments are we under-utilizing relative to their allocated capacity? Rank the top 3 and suggest classes of business to redirect next quarter.",
        "Roll up every policy by household (shared address + surname) and surface the top 10 by total premium — private-client cross-sell candidates only.",
        "Identify commercial insureds missing cyber or EPL coverage that profile-match peers who carry both. Flag as cross-sell, do not invent.",
    ]},
    { label: "Risk & relationship graph", items: [
        "Find entities that share a board member, officer, or mailing address. Flag any aggregated exposure above $25M as concentration risk.",
        "Walk from 'Coastal Grille & Burgers' across the graph — every linked human, sibling entity, and carrier — one sentence of context per hop.",
        "Summarize all board-level relationships for Annapolis Parks Department and surface any director also tied to another insured in our book.",
        "List every human linked to a Chesapeake Ave address and note any that appear on more than one entity.",
    ]},
    { label: "Renewal & retention", items: [
        "List policies renewing in the next 90 days, ranked by premium-at-risk and days since last human contact. Flag anything with zero touches in 60+ days.",
        "Which active accounts have had no human contact in 180+ days? Prioritize by premium size — these are retention hot-spots.",
    ]},
    { label: "Data quality & defensibility", items: [
        "Produce a one-page migration defensibility brief: record counts, dedupe clusters auto-merged above 0.85 confidence, HITL overrides, and any open enum violations.",
        "In plain English for the CEO: what are the five most dangerous things we migrated from the vendor export, and what would go wrong if we didn't catch them?",
    ]},
];
const PROMPT_CHIPS = PROMPT_GROUPS.flatMap(g => g.items);

function downloadAgentResponseAsPDF({ prompt, markdown, context, source }) {
    const html = marked.parse(markdown || "");
    const escape = (s) => String(s ?? "").replace(/[&<>"']/g, c => ({ "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;" }[c]));
    const tags = [];
    if (source) tags.push(source === "llm" ? "Claude Sonnet 4.5" : "grounded");
    if (context?.entity_name) tags.push(escape(context.entity_name));
    if (context?.markets_shortlisted != null) tags.push(`${context.markets_shortlisted} carriers`);
    const stamp = new Date().toLocaleString();
    const doc = `<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>IRYSCLOUD Agent — Submission</title>
<style>
  @page { margin: 0.6in; }
  html, body { background: #fff; color: #0f172a; }
  body { font: 13.5px/1.55 -apple-system, "Inter", "Segoe UI", Roboto, sans-serif; margin: 0; padding: 32px; max-width: 780px; }
  header { border-bottom: 1px solid #e2e8f0; padding-bottom: 14px; margin-bottom: 20px; }
  .brand { font-size: 11px; letter-spacing: .14em; text-transform: uppercase; color: #4f46e5; font-weight: 600; }
  h1 { font-size: 20px; margin: 4px 0 6px; }
  .meta { font-size: 11.5px; color: #64748b; display: flex; gap: 10px; flex-wrap: wrap; }
  .tags { margin-top: 8px; display: flex; gap: 6px; flex-wrap: wrap; }
  .tag { font-size: 10.5px; background: #eef2ff; color: #4338ca; padding: 2px 8px; border-radius: 999px; border: 1px solid #c7d2fe; }
  section.prompt { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 10px; padding: 12px 14px; margin-bottom: 18px; }
  section.prompt .label { font-size: 10.5px; letter-spacing: .12em; text-transform: uppercase; color: #64748b; font-weight: 600; margin-bottom: 4px; }
  section.prompt .text { font-size: 13px; color: #1e293b; white-space: pre-wrap; }
  .body h1, .body h2, .body h3 { color: #0f172a; margin-top: 1.2em; }
  .body h2 { font-size: 16px; border-bottom: 1px solid #e2e8f0; padding-bottom: 4px; }
  .body h3 { font-size: 14px; }
  .body p { margin: 0.55em 0; }
  .body ul, .body ol { padding-left: 1.3em; }
  .body li { margin: 0.25em 0; }
  .body code { background: #f1f5f9; padding: 1px 5px; border-radius: 4px; font-size: 12px; }
  .body pre { background: #0f172a; color: #e2e8f0; padding: 12px; border-radius: 8px; overflow: auto; font-size: 12px; }
  .body blockquote { border-left: 3px solid #c7d2fe; margin: 0.5em 0; padding: 2px 12px; color: #475569; background: #f8fafc; }
  .body table { border-collapse: collapse; width: 100%; font-size: 12.5px; margin: 0.6em 0; }
  .body th, .body td { border: 1px solid #e2e8f0; padding: 6px 8px; text-align: left; }
  .body th { background: #f1f5f9; }
  footer { margin-top: 28px; padding-top: 12px; border-top: 1px solid #e2e8f0; font-size: 10.5px; color: #94a3b8; display: flex; justify-content: space-between; }
</style></head>
<body>
  <header>
    <div class="brand">IRYSCLOUD · Submission Agent</div>
    <h1>Submission response</h1>
    <div class="meta"><span>Generated ${escape(stamp)}</span></div>
    ${tags.length ? `<div class="tags">${tags.map(t => `<span class="tag">${t}</span>`).join("")}</div>` : ""}
  </header>
  <section class="prompt">
    <div class="label">Prompt</div>
    <div class="text">${escape(prompt || "")}</div>
  </section>
  <div class="body">${html}</div>
  <footer><span>Grounded in migrated records</span><span>IRYSCLOUD AI Data Migration</span></footer>
</body></html>`;

    const iframe = document.createElement("iframe");
    iframe.setAttribute("aria-hidden", "true");
    iframe.style.position = "fixed";
    iframe.style.right = "0";
    iframe.style.bottom = "0";
    iframe.style.width = "0";
    iframe.style.height = "0";
    iframe.style.border = "0";
    iframe.style.opacity = "0";
    iframe.style.pointerEvents = "none";

    const cleanup = () => {
        try { iframe.parentNode && iframe.parentNode.removeChild(iframe); } catch (e) {}
    };

    iframe.onload = () => {
        try {
            const win = iframe.contentWindow;
            win.focus();
            win.print();
        } catch (e) {
            cleanup();
            return;
        }
        // Remove after the print dialog has had time to snapshot the DOM.
        setTimeout(cleanup, 1500);
    };

    document.body.appendChild(iframe);
    const idoc = iframe.contentDocument || iframe.contentWindow?.document;
    if (!idoc) { cleanup(); return false; }
    idoc.open();
    idoc.write(doc);
    idoc.close();
    return true;
}

const EXEC_DIRECTIVE = "Respond in CEO/executive brief style. Start with a 3-bullet TL;DR (lead with the decision or recommendation), then the full details below. Keep it tight, no preamble. ";

function AgentPanel() {
    const [messages, setMessages] = useState([]);
    const [input, setInput] = useState(PROMPT_CHIPS[0]);
    const [loading, setLoading] = useState(false);
    const [execMode, setExecMode] = useState(false);
    const scrollRef = useRef();
    const toast = useToast();

    useEffect(() => { scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: "smooth" }); }, [messages, loading]);

    const send = async () => {
        if (!input.trim() || loading) return;
        const userMsg = { role: "user", text: input, exec: execMode };
        const aiIdx = 1; // placeholder, real index computed after setMessages
        setMessages(m => [...m, userMsg, { role: "ai", markdown: "", source: null, context: null, streaming: true, exec: execMode }]);
        const p = input;
        const wirePrompt = execMode ? `${EXEC_DIRECTIVE}\n\nQuestion: ${p}` : p;
        setInput("");
        setLoading(true);
        try {
            await streamSSE("/api/agent/stream", {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ prompt: wirePrompt }),
            }, ({ event, data }) => {
                if (event === "context") {
                    setMessages(m => m.map((msg, i) => i === m.length - 1 ? { ...msg, context: data } : msg));
                } else if (event === "source") {
                    setMessages(m => m.map((msg, i) => i === m.length - 1 ? { ...msg, source: data.source } : msg));
                } else if (event === "delta") {
                    setMessages(m => m.map((msg, i) => i === m.length - 1 ? { ...msg, markdown: (msg.markdown || "") + (data.text || "") } : msg));
                } else if (event === "done") {
                    setMessages(m => m.map((msg, i) => i === m.length - 1 ? { ...msg, streaming: false, source: data.source || msg.source } : msg));
                }
            });
        } catch (e) {
            setMessages(m => m.map((msg, i) => i === m.length - 1 ? { ...msg, markdown: `**Error:** ${e.message}`, source: "error", streaming: false } : msg));
        } finally { setLoading(false); }
    };

    const onKey = (e) => { if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) { e.preventDefault(); send(); } };

    return (
        <div className="max-w-5xl">
            <PageHeader
                eyebrow="AI co-pilot"
                title="Submission agent"
                subtitle="Ask IRYSCLOUD to build a carrier submission from the migrated data. It grounds every fact in the canonical records."
                actions={<Chip kind="brand" icon="sparkle">Streaming</Chip>}
            />

            <div className="grid grid-cols-1 lg:grid-cols-[1fr_260px] gap-4">
                <Card padding="p-0" className={`flex flex-col ${loading ? "agent-card-stream" : ""}`}>
                    <div className="px-5 py-3 border-b border-ink-200 flex items-center gap-2 text-[12.5px] text-ink-600">
                        <div className="brand-mark w-8 h-8"><Icon name="bot" size={15} /></div>
                        <div className="flex-1">
                            <div className="text-ink-900 font-medium">IRYSCLOUD Agent</div>
                            <div className="text-[11px] text-ink-500">Sees your migrated data. Doesn't guess.</div>
                        </div>
                        {loading && (
                            <span className="live-ticker" style={{ padding: "4px 10px", fontSize: 11 }}>
                                <span className="live-dot" />
                                <span>Claude Sonnet · streaming</span>
                            </span>
                        )}
                    </div>

                    <div ref={scrollRef} className="px-5 py-5 space-y-4 min-h-[360px] max-h-[64vh] overflow-y-auto scroll-fade-bottom">
                        {messages.length === 0 && (
                            <div className="text-center py-10">
                                <div className="w-12 h-12 mx-auto rounded-2xl bg-brand-50 text-brand-600 flex items-center justify-center mb-3"><Icon name="sparkle" size={20} /></div>
                                <div className="font-semibold text-ink-900">Ready when you are.</div>
                                <div className="text-[13px] text-ink-500 max-w-md mx-auto mt-1">Ask a question or pick a suggested prompt — the agent pulls from the exact records you just migrated.</div>
                            </div>
                        )}
                        {messages.map((m, i) => {
                            if (m.role === "user") {
                                return (
                                    <div key={i} className="flex justify-end">
                                        <div className="chat-bubble-user rounded-2xl rounded-tr-sm px-4 py-3 max-w-[80%] text-[13.5px]">{m.text}</div>
                                    </div>
                                );
                            }
                            const priorPrompt = messages[i - 1]?.role === "user" ? messages[i - 1].text : "";
                            const canDownload = !m.streaming && (m.markdown || "").trim().length > 0;
                            return (
                                <div key={i} className="flex justify-start">
                                    <div className="chat-bubble-ai rounded-2xl rounded-tl-sm p-4 max-w-[90%] text-[13.5px] shadow-card">
                                        {(m.source || m.context || m.exec) && (
                                            <div className="flex items-center gap-1.5 mb-2 flex-wrap">
                                                {m.streaming && <Chip kind="brand" icon="sparkle">streaming…</Chip>}
                                                {!m.streaming && m.source && <Chip kind={m.source === "llm" ? "success" : "brand"} icon={m.source === "llm" ? "sparkle" : "shield"}>{m.source === "llm" ? "Claude Sonnet 4.5" : "grounded"}</Chip>}
                                                {m.exec && <Chip kind="brand" icon="sparkle">Exec brief</Chip>}
                                                {m.context?.entity_name && <Chip kind="brand" icon="building">{m.context.entity_name}</Chip>}
                                                {m.context?.markets_shortlisted != null && <Chip kind="brand" icon="plug">{m.context.markets_shortlisted} carriers</Chip>}
                                            </div>
                                        )}
                                        {(m.markdown || "").length === 0 && m.streaming ? (
                                            <div className="flex items-center gap-1.5 text-ink-500 text-[12.5px]"><span className="pulse-dot">●</span> grounding in migrated records…</div>
                                        ) : (
                                            <div className="md" dangerouslySetInnerHTML={{ __html: marked.parse(m.markdown || "") + (m.streaming ? '<span class="stream-caret">▌</span>' : '') }} />
                                        )}
                                        {canDownload && (
                                            <div className="mt-3 pt-2.5 border-t border-ink-200/70 flex justify-end gap-2">
                                                <Button
                                                    variant="ghost"
                                                    size="sm"
                                                    icon="copy"
                                                    onClick={async () => {
                                                        try {
                                                            await navigator.clipboard.writeText(m.markdown || "");
                                                            toast.push("Copied to clipboard", "success", "Paste into email or Slack");
                                                        } catch (e) {
                                                            toast.push("Copy failed", "error", "Clipboard unavailable");
                                                        }
                                                    }}
                                                    title="Copy the response as markdown"
                                                >
                                                    Copy
                                                </Button>
                                                <Button
                                                    variant="secondary"
                                                    size="sm"
                                                    icon="download"
                                                    onClick={() => {
                                                        const ok = downloadAgentResponseAsPDF({
                                                            prompt: priorPrompt,
                                                            markdown: m.markdown,
                                                            context: m.context,
                                                            source: m.source,
                                                        });
                                                        if (!ok) toast.push("Pop-up blocked", "error", "Allow pop-ups to download as PDF");
                                                    }}
                                                    title="Open a print-ready view — use your browser's Save as PDF"
                                                >
                                                    Download PDF
                                                </Button>
                                            </div>
                                        )}
                                    </div>
                                </div>
                            );
                        })}
                    </div>

                    <div className="p-3 border-t border-ink-200">
                        <div className="flex items-end gap-2 bg-ink-50 rounded-xl p-1.5 border border-ink-200 focus-within:ring-2 focus-within:ring-brand-500 focus-within:border-transparent">
                            <textarea value={input} onChange={e => setInput(e.target.value)} onKeyDown={onKey} rows={2}
                                placeholder="Ask the agent…"
                                className="flex-1 bg-transparent resize-none text-[14px] px-2.5 py-1.5 outline-none placeholder:text-ink-400" />
                            <Button variant="primary" size="sm" icon="send" onClick={send} disabled={loading || !input.trim()}>Send <Kbd>⌘⏎</Kbd></Button>
                        </div>
                        <div className="mt-2 flex items-center justify-between text-[11.5px]">
                            <button
                                type="button"
                                onClick={() => setExecMode(v => !v)}
                                title="Prepend the CEO brief directive: 3-bullet TL;DR, no preamble"
                                className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 border transition-colors ${execMode ? "bg-brand-50 border-brand-200 text-brand-700" : "bg-white border-ink-200 text-ink-500 hover:text-ink-700 hover:border-ink-300"}`}
                            >
                                <Icon name="sparkle" size={11} />
                                Exec brief {execMode ? "· on" : "· off"}
                            </button>
                            <div className="text-ink-400">⌘⏎ to send</div>
                        </div>
                    </div>
                </Card>

                <div className="space-y-4">
                    <Card padding="p-5">
                        <div className="flex items-center justify-between mb-2">
                            <div className="text-[11px] uppercase tracking-widest text-ink-500 font-semibold">Prompt library</div>
                            <div className="text-[10.5px] text-ink-400 tabular">{PROMPT_CHIPS.length}</div>
                        </div>
                        <div className="max-h-[60vh] overflow-y-auto pr-1 -mr-1 space-y-3">
                            {PROMPT_GROUPS.map(group => (
                                <div key={group.label}>
                                    <div className="text-[10px] uppercase tracking-widest text-ink-400 font-semibold mb-1.5">{group.label}</div>
                                    <div className="space-y-1.5">
                                        {group.items.map((p, i) => (
                                            <button
                                                key={i}
                                                onClick={() => setInput(p)}
                                                title="Click to load into the composer"
                                                className="w-full text-left p-2.5 rounded-lg border border-ink-200 hover:border-brand-300 hover:bg-brand-50/50 text-[12.5px] leading-snug text-ink-700 transition-colors"
                                            >
                                                {p}
                                            </button>
                                        ))}
                                    </div>
                                </div>
                            ))}
                        </div>
                    </Card>
                    <Card padding="p-5">
                        <div className="text-[11px] uppercase tracking-widest text-ink-500 font-semibold mb-2">What the agent can see</div>
                        <ul className="text-[12.5px] text-ink-700 space-y-1.5">
                            <li className="flex items-center gap-2"><Icon name="building" size={12} className="text-ink-400" /> All migrated entities</li>
                            <li className="flex items-center gap-2"><Icon name="users" size={12} className="text-ink-400" /> All migrated humans</li>
                            <li className="flex items-center gap-2"><Icon name="share" size={12} className="text-ink-400" /> Relationship graph</li>
                            <li className="flex items-center gap-2"><Icon name="plug" size={12} className="text-ink-400" /> Active carrier appointments</li>
                            <li className="flex items-center gap-2"><Icon name="map" size={12} className="text-ink-400" /> Address + household cross-refs</li>
                        </ul>
                    </Card>
                </div>
            </div>
        </div>
    );
}

/* ======================== 16. Root ============================== */

function App() {
    const [active, setActive] = useState("ingest");
    const [stats, setStats] = useState(null);
    const [health, setHealth] = useState({ llm: "fallback" });
    const [paletteOpen, setPaletteOpen] = useState(false);
    const toast = useToast();

    const refresh = useCallback(() => { api("/api/stats").then(setStats).catch(() => {}); }, []);
    useEffect(() => { refresh(); api("/api/health").then(setHealth).catch(() => {}); }, [refresh]);
    useEffect(() => {
        const id = setInterval(() => { api("/api/stats").then(setStats).catch(() => {}); }, 4000);
        return () => clearInterval(id);
    }, []);

    useKeyboard("cmd+k", (e) => { e.preventDefault(); setPaletteOpen(v => !v); }, []);

    const onReset = async () => {
        if (!confirm("Reset workspace? This clears SQLite and re-ingests the source file.")) return;
        await api("/api/reset", { method: "POST" });
        await refresh();
        setActive("ingest");
        toast.push("Workspace reset", "success", "Fresh data loaded from source file");
    };

    const onPaletteNav = (id, { action } = {}) => {
        if (id) setActive(id);
        if (action === "reset") onReset();
        if (action === "run-dag") setActive("dag");
    };

    return (
        <div className="flex min-h-screen">
            <Sidebar active={active} onSelect={setActive} stats={stats} />
            <main className="flex-1 min-w-0 flex flex-col">
                <Topbar active={active} onReset={onReset} llmMode={health.llm} onCmd={() => setPaletteOpen(true)} stats={stats} />
                <div className="flex-1 overflow-auto flex flex-col">
                    <div className="max-w-[1280px] w-full mx-auto px-8 py-8 animate-fade-in flex-1" key={active}>
                        {active === "ingest"  && <IngestPanel  onIngested={refresh} onNavigate={setActive} />}
                        {active === "mapping" && <MappingPanel />}
                        {active === "dedup"   && <DedupPanel   onChange={refresh} />}
                        {active === "graph"   && <GraphPanel />}
                        {active === "review"  && <ReviewPanel  onChange={refresh} />}
                        {active === "dag"     && <DagPanel />}
                        {active === "audit"   && <AuditPanel />}
                        {active === "agent"   && <AgentPanel />}
                    </div>
                    <footer className="trust-strip">
                        <span className="trust-item"><span className="trust-dot" /><b>SOC 2</b> Type II</span>
                        <span className="trust-item"><span className="trust-dot" /><b>AES-256</b> at rest</span>
                        <span className="trust-item"><span className="trust-dot" /><b>TLS 1.3</b> in transit</span>
                        <span className="trust-item"><span className="trust-dot" /><b>HIPAA</b>-ready</span>
                        <span className="trust-item"><span className="trust-dot" />Every action <b>audit-logged</b></span>
                    </footer>
                </div>
            </main>
            <CommandPalette open={paletteOpen} onClose={() => setPaletteOpen(false)} onNavigate={onPaletteNav} />
        </div>
    );
}

function Root() {
    return <ToastProvider><App /></ToastProvider>;
}

ReactDOM.createRoot(document.getElementById("root")).render(<Root />);
