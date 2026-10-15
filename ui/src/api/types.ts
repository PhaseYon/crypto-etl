// Aliases over the generated OpenAPI types (schema.ts). Never edit schema.ts by hand: change the
// API's pydantic models, then run `python api/scripts/export_openapi.py` and `npm run gen:api`.
import type { components } from "./schema";

type Schemas = components["schemas"];

export type Health = Schemas["Stage"]["status"];
export type Stage = Schemas["Stage"];
export type Metric = Schemas["Metric"];
export type MetricUnit = Metric["unit"];
export type PipelineResponse = Schemas["PipelineResponse"];
export type KafkaDetail = Schemas["KafkaDetail"];
export type LagDetail = Schemas["LagDetail"];
export type TableStatus = Schemas["TableStatus"];
export type MarketResponse = Schemas["MarketResponse"];
export type Candle = Schemas["Candle"];
export type QualityResponse = Schemas["QualityResponse"];
export type QuarantineSummary = Schemas["QuarantineSummary"];
export type GapsSummary = Schemas["GapsSummary"];
export type BackfillShare = Schemas["BackfillShare"];
export type PredictionsResponse = Schemas["PredictionsResponse"];
export type Prediction = Schemas["Prediction"];
export type MetaResponse = Schemas["MetaResponse"];
