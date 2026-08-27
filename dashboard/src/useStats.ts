import { useEffect, useState } from "react";
import type { Stats } from "./types";

/** Polls observer's /stats for the cross-service consistency snapshot. */
export function useStats(intervalMs = 1000) {
  const [stats, setStats] = useState<Stats | null>(null);

  useEffect(() => {
    let stopped = false;
    const tick = async () => {
      try {
        const resp = await fetch("/api/observer/stats");
        if (resp.ok && !stopped) setStats(await resp.json());
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

  return stats;
}
