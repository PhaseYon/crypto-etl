import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { App } from "./App";
import { applyPreference, readPreference } from "./lib/theme";
import "./styles/app.css";

applyPreference(readPreference());

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Polling handles recovery (with backoff); one quick retry smooths over blips
      retry: 1,
      retryDelay: 1_000,
      refetchOnWindowFocus: true,
    },
  },
});

const root = document.getElementById("root");
if (!root) throw new Error("#root element missing");

createRoot(root).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <App />
    </QueryClientProvider>
  </StrictMode>,
);
