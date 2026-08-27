import { useEffect, useRef, useState } from "react";
import type { BusEvent } from "./types";

/**
 * Connects to observer's WebSocket and accumulates the event stream.
 *
 * Auto-reconnects: observer restarts (or isn't up yet when you open the page),
 * and the dashboard should just wait and retry rather than sit dead.
 */
export function useEventStream(max = 200) {
  const [events, setEvents] = useState<BusEvent[]>([]);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    let stopped = false;
    let retry: ReturnType<typeof setTimeout>;

    const connect = () => {
      if (stopped) return;
      const proto = location.protocol === "https:" ? "wss" : "ws";
      const ws = new WebSocket(`${proto}://${location.host}/api/observer/ws`);
      wsRef.current = ws;

      ws.onopen = () => setConnected(true);
      ws.onclose = () => {
        setConnected(false);
        if (!stopped) retry = setTimeout(connect, 2000);
      };
      ws.onerror = () => ws.close();
      ws.onmessage = (ev) => {
        const parsed = JSON.parse(ev.data) as BusEvent;
        setEvents((prev) => [parsed, ...prev].slice(0, max));
      };
    };

    connect();
    return () => {
      stopped = true;
      clearTimeout(retry);
      wsRef.current?.close();
    };
  }, [max]);

  return { events, connected };
}
