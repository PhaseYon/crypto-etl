import { useEffect, useState, type ComponentType } from "react";

import { useMeta } from "./api/queries";
import { useThemePreference, type ThemePreference } from "./lib/theme";
import { MarketView } from "./views/market/MarketView";
import { MlView } from "./views/ml/MlView";
import { PipelineView } from "./views/pipeline/PipelineView";
import { QualityView } from "./views/quality/QualityView";

type TabId = string;

type Tab = { id: TabId; label: string; view: ComponentType };

const TABS: Tab[] = [
  { id: "pipeline", label: "Pipeline", view: PipelineView },
  { id: "market", label: "Market", view: MarketView },
  { id: "quality", label: "Data quality", view: QualityView },
  { id: "ml", label: "ML", view: MlView },
];

function tabFromHash(hash: string): TabId {
  const id = hash.replace(/^#\/?/, "");
  return TABS.find((t) => t.id === id)?.id ?? "pipeline";
}

export function App() {
  const [tab, setTab] = useState<TabId>(() => tabFromHash(window.location.hash));
  const meta = useMeta();

  useEffect(() => {
    const onHash = () => {
      setTab(tabFromHash(window.location.hash));
    };
    window.addEventListener("hashchange", onHash);
    return () => {
      window.removeEventListener("hashchange", onHash);
    };
  }, []);

  const select = (id: TabId) => {
    window.history.pushState(null, "", `#/${id}`);
    setTab(id);
  };

  const active = TABS.find((t) => t.id === tab) ?? TABS[0];
  const View = active?.view ?? PipelineView;

  return (
    <div className="app">
      <header className="app-header">
        <div className="app-title">
          <h1>crypto-etl</h1>
          <span className="app-subtitle">Coinbase trades → Kafka → Delta lake → ML</span>
          {meta.data?.data_source === "mock" ? (
            <span className="chip" title="The API is serving generated demo data (DATA_SOURCE=mock)">
              Mock data
            </span>
          ) : null}
        </div>
        <div className="header-actions">
          <ThemeToggle />
        </div>
      </header>
      <nav className="tabs" role="tablist" aria-label="Dashboard sections">
        {TABS.map((t) => (
          <button
            key={t.id}
            type="button"
            role="tab"
            id={`tab-${t.id}`}
            aria-selected={t.id === tab}
            aria-controls={`panel-${t.id}`}
            className="tab"
            onClick={() => {
              select(t.id);
            }}
          >
            {t.label}
          </button>
        ))}
      </nav>
      <main role="tabpanel" id={`panel-${tab}`} aria-labelledby={`tab-${tab}`}>
        <View />
      </main>
    </div>
  );
}

const NEXT: Record<ThemePreference, ThemePreference> = { system: "light", light: "dark", dark: "system" };
const THEME_LABEL: Record<ThemePreference, string> = {
  system: "Theme: system",
  light: "Theme: light",
  dark: "Theme: dark",
};

function ThemeToggle() {
  const [preference, setPreference] = useThemePreference();
  return (
    <button
      type="button"
      className="button"
      onClick={() => {
        setPreference(NEXT[preference]);
      }}
      title="Switch between system, light and dark"
    >
      {THEME_LABEL[preference]}
    </button>
  );
}
