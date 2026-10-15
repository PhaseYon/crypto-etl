import { useEffect, useState, useSyncExternalStore } from "react";

export type ThemePreference = "system" | "light" | "dark";

const STORAGE_KEY = "crypto-etl-theme";
const CHANGE_EVENT = "crypto-etl-themechange";

export function readPreference(): ThemePreference {
  try {
    const stored = window.localStorage.getItem(STORAGE_KEY);
    if (stored === "light" || stored === "dark" || stored === "system") return stored;
  } catch {
    // storage blocked (private mode, sandbox): fall back to the system setting
  }
  return "system";
}

export function applyPreference(preference: ThemePreference): void {
  const root = document.documentElement;
  if (preference === "system") delete root.dataset.theme;
  else root.dataset.theme = preference;
  try {
    window.localStorage.setItem(STORAGE_KEY, preference);
  } catch {
    // not persisted; still applied for this session
  }
  window.dispatchEvent(new Event(CHANGE_EVENT));
}

function subscribe(callback: () => void): () => void {
  const media = window.matchMedia("(prefers-color-scheme: dark)");
  media.addEventListener("change", callback);
  window.addEventListener(CHANGE_EVENT, callback);
  return () => {
    media.removeEventListener("change", callback);
    window.removeEventListener(CHANGE_EVENT, callback);
  };
}

function resolvedTheme(): "light" | "dark" {
  const explicit = document.documentElement.dataset.theme;
  if (explicit === "light" || explicit === "dark") return explicit;
  return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

/** The theme actually in effect ("system" resolved), updating on toggle or OS change. */
export function useResolvedTheme(): "light" | "dark" {
  return useSyncExternalStore(subscribe, resolvedTheme, () => "light");
}

export function useThemePreference(): [ThemePreference, (next: ThemePreference) => void] {
  const [preference, setPreference] = useState<ThemePreference>(readPreference);
  useEffect(() => {
    applyPreference(preference);
  }, [preference]);
  return [preference, setPreference];
}

export type ChartColors = {
  surface: string;
  text: string;
  muted: string;
  grid: string;
  axis: string;
  up: string;
  down: string;
  volume: string;
  series: [string, string, string];
};

function cssVar(name: string, fallback: string): string {
  const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return value || fallback;
}

const colorCache: Partial<Record<"light" | "dark", ChartColors>> = {};

/** Canvas charts can't read CSS variables, so resolve the tokens for the active theme. */
export function useChartColors(): ChartColors {
  const theme = useResolvedTheme();
  return (colorCache[theme] ??= readChartColors());
}

function readChartColors(): ChartColors {
  return {
    surface: cssVar("--surface-1", "#fcfcfb"),
    text: cssVar("--text-secondary", "#52514e"),
    muted: cssVar("--text-muted", "#898781"),
    grid: cssVar("--grid", "#e1e0d9"),
    axis: cssVar("--axis", "#c3c2b7"),
    up: cssVar("--up", "#2a78d6"),
    down: cssVar("--down", "#e34948"),
    volume: cssVar("--volume", "#c3c2b7"),
    series: [
      cssVar("--series-1", "#2a78d6"),
      cssVar("--series-2", "#eb6834"),
      cssVar("--series-3", "#1baf7a"),
    ],
  };
}
