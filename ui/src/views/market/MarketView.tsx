import { useState } from "react";

import { useMarket, useMeta } from "../../api/queries";
import type { Candle, MarketResponse } from "../../api/types";
import { Panel } from "../../components/Panel";
import { StatTile } from "../../components/StatTile";
import { formatCount, formatPrice, formatSignedPercent, formatTime, formatVolume } from "../../lib/format";
import { CandleChart } from "./CandleChart";
import { RANGES, summarize } from "./candles";

const TABLE_ROWS = 120;

export function MarketView() {
  const meta = useMeta();
  const products = meta.data?.products ?? [];
  const [selected, setSelected] = useState<string | null>(null);
  const [minutes, setMinutes] = useState<number>(360);
  const [showTable, setShowTable] = useState(false);
  const product = selected ?? products[0];
  const query = useMarket(product, minutes);
  const rangeLabel = RANGES.find((r) => r.minutes === minutes)?.label ?? `${minutes}m`;

  return (
    <div className="stack">
      {/* One filter row scopes everything below it */}
      <div className="filters" role="group" aria-label="Market filters">
        <div className="segmented" role="group" aria-label="Time range">
          {RANGES.map((r) => (
            <button
              key={r.minutes}
              type="button"
              aria-pressed={r.minutes === minutes}
              onClick={() => {
                setMinutes(r.minutes);
              }}
            >
              {r.label}
            </button>
          ))}
        </div>
        <label className="field">
          Product
          <select
            value={product ?? ""}
            disabled={products.length === 0}
            onChange={(e) => {
              setSelected(e.target.value);
            }}
          >
            {products.map((p) => (
              <option key={p} value={p}>
                {p}
              </option>
            ))}
          </select>
        </label>
      </div>
      <Panel
        title={product ? `${product} · 1-minute candles` : "Market"}
        subtitle={`gold/ohlcv_1m · last ${rangeLabel} · times in UTC`}
        query={query}
        isEmpty={(data) => emptyMessage(data)}
        actions={
          <button
            type="button"
            className="button"
            aria-pressed={showTable}
            onClick={() => {
              setShowTable((v) => !v);
            }}
          >
            {showTable ? "Show chart" : "Show table"}
          </button>
        }
      >
        {(data) => (
          <div className="stack">
            <MarketTiles candles={data.candles} rangeLabel={rangeLabel} />
            {showTable ? (
              <CandleTable candles={data.candles} />
            ) : (
              <CandleChart candles={data.candles} productId={data.product_id} />
            )}
          </div>
        )}
      </Panel>
    </div>
  );
}

function emptyMessage(data: MarketResponse): string | null {
  if (data.status === "inactive") return "No candles yet: the gold table hasn't been created.";
  if (data.status === "down") return `Couldn't read candles: ${data.error ?? "unknown error"}`;
  if (data.candles.length === 0) return "No trades in this range.";
  return null;
}

function MarketTiles({ candles, rangeLabel }: { candles: Candle[]; rangeLabel: string }) {
  const s = summarize(candles);
  if (!s) return null;
  return (
    <div className="tiles">
      <StatTile label="Last price" value={formatPrice(s.last)} />
      <StatTile
        label={`Change (${rangeLabel})`}
        value={formatSignedPercent(s.change)}
        note="first open → last close"
      />
      <StatTile
        label={`High / low (${rangeLabel})`}
        value={formatPrice(s.high)}
        note={`low ${formatPrice(s.low)}`}
      />
      <StatTile
        label={`Volume (${rangeLabel})`}
        value={formatVolume(s.volume)}
        note={`${formatCount(s.trades)} trades`}
      />
    </div>
  );
}

/** The chart's accessible twin: every value is readable without hovering. */
function CandleTable({ candles }: { candles: Candle[] }) {
  const rows = candles.slice(-TABLE_ROWS).reverse();
  return (
    <div className="table-wrap">
      <table className="data">
        <caption className="sr-only">Candles, newest first</caption>
        <thead>
          <tr>
            <th scope="col">Minute</th>
            {["Open", "High", "Low", "Close", "Volume", "VWAP", "Trades"].map((h) => (
              <th key={h} scope="col" className="num">
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((c) => (
            <tr key={c.time}>
              <td>{formatTime(c.time)}</td>
              <td className="num">{formatPrice(c.open)}</td>
              <td className="num">{formatPrice(c.high)}</td>
              <td className="num">{formatPrice(c.low)}</td>
              <td className="num">{formatPrice(c.close)}</td>
              <td className="num">{formatVolume(c.volume)}</td>
              <td className="num">{formatPrice(c.vwap)}</td>
              <td className="num">{formatCount(c.trade_count)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {candles.length > TABLE_ROWS ? (
        <p className="footer">
          Showing the newest {TABLE_ROWS} of {formatCount(candles.length)} minutes.
        </p>
      ) : null}
    </div>
  );
}
