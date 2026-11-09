"use client";

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { Cpu, ChevronDown } from "lucide-react";

type Worker = { pid: string; worker_instance_id: string };
type WorkerContextValue = {
  workers: Worker[];
  workerPid: string;
  setWorkerPid: (pid: string) => void;
  loading: boolean;
  error: string;
  refresh: () => Promise<void>;
};

const WorkerContext = createContext<WorkerContextValue | null>(null);

export function WorkerProvider({ children }: { children: ReactNode }) {
  const [workers, setWorkers] = useState<Worker[]>([]);
  const [workerPid, setWorkerPidState] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  const refresh = useCallback(async () => {
    setLoading(true);
    try {
      const response = await fetch("/api/workers", { cache: "no-store" });
      const body = await response.json();
      if (!response.ok) {
        throw new Error(body?.detail ?? "Could not load live workers.");
      }

      const nextWorkers = Array.isArray(body?.data)
      ? body.data.filter(
          (worker: Worker & { online?: boolean }) => worker.online === true,
        )
      : [];
      setWorkers(nextWorkers);
      setError("");
      setWorkerPidState((current) => {
        const saved =
          typeof window === "undefined"
            ? current
            : (window.localStorage.getItem("origintracer.worker_pid") ??
              current);
        const nextPid = nextWorkers.some(
          (worker: Worker) => worker.pid === saved,
        )
          ? saved
          : (nextWorkers[0]?.pid ?? "");
        if (typeof window !== "undefined" && nextPid) {
          window.localStorage.setItem("origintracer.worker_pid", nextPid);
        }
        return nextPid;
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not load workers.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const setWorkerPid = useCallback((pid: string) => {
    setWorkerPidState(pid);
    if (typeof window !== "undefined") {
      window.localStorage.setItem("origintracer.worker_pid", pid);
    }
  }, []);

  const value = useMemo(
    () => ({ workers, workerPid, setWorkerPid, loading, error, refresh }),
    [workers, workerPid, setWorkerPid, loading, error, refresh],
  );

  return (
    <WorkerContext.Provider value={value}>{children}</WorkerContext.Provider>
  );
}

export function useWorker() {
  const context = useContext(WorkerContext);
  if (!context) throw new Error("useWorker must be used within WorkerProvider");
  return context;
}

export function WorkerSelector() {
  const { workers, workerPid, setWorkerPid, loading, error } = useWorker();

  return (
    <label
      className="hover:bg-sidebar-accent focus-within:ring-ring relative flex h-10 w-full cursor-pointer items-center gap-3 rounded-lg px-3 text-sm focus-within:ring-2"
      title={error || undefined}
    >
      <Cpu className="text-muted-foreground pointer-events-none size-4 shrink-0" />
      <span className="pointer-events-none min-w-0 flex-1 truncate">
        Worker
      </span>
      <span className="text-muted-foreground pointer-events-none max-w-36 truncate text-right text-xs">
        {loading ? "Finding…" : workerPid ? `PID ${workerPid}` : "None"}
      </span>
      <ChevronDown className="text-muted-foreground pointer-events-none size-3.5 shrink-0" />
      <select
        aria-label="Selected worker"
        className="absolute inset-0 z-10 h-full w-full cursor-pointer opacity-0 disabled:cursor-not-allowed"
        value={workerPid}
        onChange={(event) => setWorkerPid(event.target.value)}
        disabled={loading || workers.length === 0}
      >
        {loading && <option value="">Finding workers…</option>}
        {!loading && workers.length === 0 && (
          <option value="">No live workers</option>
        )}
        {workers.map((worker) => (
          <option
            key={worker.worker_instance_id}
            value={worker.pid}
          >
            PID {worker.pid}
          </option>
        ))}
      </select>
    </label>
  );
}
