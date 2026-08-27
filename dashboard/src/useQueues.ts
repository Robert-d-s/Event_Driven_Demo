import { useEffect, useState } from "react";
import type { QueueStat } from "./types";

/** Polls observer's /queues endpoint every second for queue depths. */
export function useQueues(intervalMs = 1000) {
  const [queues, setQueues] = useState<QueueStat[]>([]);

  useEffect(() => {
    let stopped = false;

    const tick = async () => {
      try {
        const resp = await fetch("/api/observer/queues");
        if (resp.ok && !stopped) {
          setQueues(await resp.json());
        }
      } catch {
        /* observer down — keep last known values */
      }
    };

    tick();
    const id = setInterval(tick, intervalMs);
    return () => {
      stopped = true;
      clearInterval(id);
    };
  }, [intervalMs]);

  return queues;
}
