# Ingot — observability

How Ingot's traces, metrics and logs reach on-call, and what a change that adds or alters
something operational should add so that a failure shows. The OTLP environment variables are
described under [Running it](../README.md#running-it) in the README, the metrics Ingot reports
(local blob storage, multipart acceptance, the catalog journal) are listed in its
[Local disk](../README.md#local-disk) tables, and the task queues' `taskrunner.*` metrics in
[`taskrunner/README.md`](../taskrunner/README.md#metrics); this page does not repeat them.

---

## Where signals go

Ingot runs on the appliances, where it pushes traces and metrics over OTLP to Alloy (compose sets
`OTEL_EXPORTER_OTLP_ENDPOINT`) and its container logs ship too; infra-central's alert rules and
`forge-regions` dashboard read `ingot_local_blobs_*`, `ingot_multipart_*` and
`ingot_catalog_journal_*`. Nothing there reads `taskrunner_*`.

## Adding a signal

- **Metrics.** Create instruments through an injectable `MeterProvider` that defaults to the
  global one, as the `ingot.*` instruments do (`s3frontend.Deps.MeterProvider`,
  `Layered.CountBlobReads`) and the runner's do (`taskrunner.Options.MeterProvider`). Prefer a
  signal that answers "is this working?", such as a failure count or a stalled amount, over one
  that only measures volume, and keep attributes low-cardinality: no keys, CIDs or bucket names.
  The exception is a per-bucket storage gauge such as `ingot.catalog.*`. It may carry the
  bucket's space DID as its only label (never the bucket's name), which bounds it at one series
  per bucket, and it reports a snapshot a sweep refreshes rather than querying Postgres at each
  collection. List each new metric in a metrics table in the README, beside the local blob,
  multipart and catalog journal ones.
- **Traces.** Request work runs inside the request's span, and so does a sync task, which runs in
  its caller's context. An async task's handler runs with no span, so a span it starts with
  `tracing.Start` is the root of a trace of its own (`blobs.conclude` in the accept UploadPart queues);
  the local blob sweeper starts one per run (`sweep.local_blobs`), and so does the catalog usage
  sweep (`sweep.catalog_usage`). Postgres queries are traced only under a recording span. A new
  outbound HTTP client uses `tracing.NewHTTPClient()`.
- **Alerts and dashboard panels** live in fil-forge/infra-central, under `terraform/envs/grafana/`:
  the Forge Regions rule groups (the main Forge Regions group holds the Ingot rules) and the
  `forge-regions` dashboard. Its `docs/observability.md` ("Adding an alert or panel") says how
  to add them. Open a companion PR there, or say in this PR that one is needed.
