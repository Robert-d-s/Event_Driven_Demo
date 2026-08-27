import { useEffect, useState } from "react";

export interface SagaLogEntry {
  order_id: number;
  at: string;
  kind: "state" | "command" | "reply" | "timeout";
  detail: string;
}

export interface SagaSnapshot {
  by_state: Record<string, number>;
  log: SagaLogEntry[];
}

/** Polls observer's /saga — the orchestrator's state machine + audit log. */
export function useSaga(intervalMs = 1000) {
  const [saga, setSaga] = useState<SagaSnapshot | null>(null);

  useEffect(() => {
    let stopped = false;
    const tick = async () => {
      try {
        const resp = await fetch("/api/observer/saga");
        if (resp.ok && !stopped) setSaga(await resp.json());
      } catch {
        /* observer down — keep last */
      }
    };
    tick();
    const id = setInterval(tick, intervalMs);
    return () => {
      stopped = true;
      clearInterval(id);
    };
  }, [intervalMs]);

  return saga;
}
