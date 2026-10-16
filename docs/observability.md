# Ingot — observability

How Ingot's traces, metrics and logs reach on-call, and what a change that adds or alters
something operational should add so that a failure shows. The OTLP environment variables are
described under [Running it](../README.md#running-it) in the README, and the metrics local blob
storage reports are listed in its [Local disk](../README.md#local-disk) table; this page does not
repeat either.

---

## Where signals go

Ingot runs on the appliances, where it pushes traces and metrics over OTLP to Alloy (compose sets
`OTEL_EXPORTER_OTLP_ENDPOINT`) and its container logs ship too; infra-central's alert rules and
`forge-regions` dashboard already read `ingot_local_blobs_*`.

## Adding a signal

- **Metrics.** Create instruments through an injectable `MeterProvider` that defaults to the
  global one, as the `ingot.local_blobs.*` instruments do (`s3frontend.Deps.MeterProvider`,
  `Layered.CountBlobReads`). Prefer a signal that answers "is this working?", such as a failure
  count or a stalled amount, over one that only measures volume, and keep attributes
  low-cardinality: no keys, CIDs or bucket names. List each new metric in a metrics table in the
  README: the local blob one, or a new one beside it.
- **Traces.** New work runs inside the request's span or a sweep's root span; a new outbound HTTP
  client uses `tracing.NewHTTPClient()`.
- **Alerts and dashboard panels** live in fil-forge/infra-central, under `terraform/envs/grafana/`:
  the Forge Regions rule groups (the main Forge Regions group holds the two Ingot rules today) and
  the `forge-regions` dashboard. Its `docs/observability.md` ("Adding an alert or panel") says how to add them. Open a companion PR there,
  or say in this PR that one is needed.
