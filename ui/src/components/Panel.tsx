import type { UseQueryResult } from "@tanstack/react-query";
import type { ReactNode } from "react";

import { formatAgo } from "../lib/format";

type PanelProps<T> = {
  title: string;
  subtitle?: ReactNode;
  actions?: ReactNode;
  query: Pick<UseQueryResult<T>, "data" | "error" | "isPending" | "isError" | "isFetching" | "dataUpdatedAt">;
  /** Return a message when the data is empty; the panel then shows it instead of children. */
  isEmpty?: (data: T) => ReactNode | null;
  children: (data: T) => ReactNode;
  className?: string;
};

/** Card with consistent loading, error, empty and stale-while-failing states. */
export function Panel<T>({ title, subtitle, actions, query, isEmpty, children, className }: PanelProps<T>) {
  const { data, error, isPending, isError } = query;
  return (
    <section className={`panel ${className ?? ""}`} aria-label={title}>
      <header className="panel-header">
        <div>
          <h2 className="panel-title">{title}</h2>
          {subtitle ? <p className="panel-subtitle">{subtitle}</p> : null}
        </div>
        {actions}
      </header>
      {isPending ? (
        <div className="state" role="status" aria-live="polite">
          <span className="sr-only">Loading {title}</span>
          <div className="skeleton" />
          <div className="skeleton" style={{ width: "40%" }} />
        </div>
      ) : data === undefined ? (
        <div className="state" role="alert">
          <span className="state-title">Couldn't load {title.toLowerCase()}</span>
          <span>{error?.message ?? "Unknown error"}</span>
          <span className="muted">Retrying automatically…</span>
        </div>
      ) : (
        <>
          {isError ? (
            <div className="notice" role="status">
              <span aria-hidden="true">⚠</span>
              <span>
                Showing data from {formatAgo(new Date(query.dataUpdatedAt).toISOString())}; refresh failing (
                {error?.message}). Retrying with backoff.
              </span>
            </div>
          ) : null}
          <PanelBody data={data} isEmpty={isEmpty} stale={isError}>
            {children}
          </PanelBody>
        </>
      )}
    </section>
  );
}

function PanelBody<T>({
  data,
  isEmpty,
  stale,
  children,
}: {
  data: T;
  isEmpty: ((data: T) => ReactNode | null) | undefined;
  stale: boolean;
  children: (data: T) => ReactNode;
}) {
  const empty = isEmpty?.(data);
  if (empty) {
    return (
      <div className="state">
        <span>{empty}</span>
      </div>
    );
  }
  // Hold the previous render at reduced opacity while refreshes fail: no layout jump
  return (
    <div className="panel-body" data-refreshing={stale}>
      {children(data)}
    </div>
  );
}
