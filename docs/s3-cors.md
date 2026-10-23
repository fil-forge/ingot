# S3 CORS in Ingot

How the gateway answers browser cross-origin requests: a gateway-wide
document the operator configures, which a bucket's own document replaces
once `PutBucketCors` has stored one.

## 1. How CORS works here

versitygw drives every piece of its CORS behaviour off one backend call,
`GetBucketCors`. Its `ApplyBucketCORS` middleware runs on every bucket and
object route for a request carrying an `Origin` header, and its preflight
controller answers an unsigned `OPTIONS` from the same document. Both fall
through untouched when the call answers `NoSuchCORSConfiguration`. So
serving a document is all CORS support takes, and which document a bucket
answers with is the whole design.

A bucket answers with, in order:

1. its own document, when `PutBucketCors` has stored one;
2. the gateway-wide document rendered from `cors_allowed_origins`
   (`internal/cors`), when the operator configured one;
3. `NoSuchCORSConfiguration`, which leaves the request without CORS headers.

A bucket's document replaces the gateway's rather than merging with it, as a
bucket's configuration does on S3: the stored document means what it says
on its own, and an operator who wants the console's origin on every bucket
keeps it in the gateway list. The trade is that a tenant who stores a list
without the console's origin locks the console out of that bucket's objects
until they add it, which is also how a bucket on S3 behaves.

## 2. The backend contract

Three methods in `s3frontend/bucketcors.go`, all one registry call:

| Method | Behaviour |
| --- | --- |
| `GetBucketCors` | The bucket's own document, else the gateway's, else `NoSuchCORSConfiguration`. `NoSuchBucket` outranks all three. |
| `PutBucketCors` | Stores the body verbatim. versitygw's controller has already parsed it and run `CORSConfiguration.Validate`, which checks each rule but not their number, so the backend parses it again and refuses more than S3's hundred rules (`MalformedXML`, the schema's own refusal); a body over 64 KiB is refused before the parse (`EntityTooLarge`) to bound the row. A refused document leaves the stored one in place. |
| `DeleteBucketCors` | Clears the document, so the bucket answers with the gateway's again. Idempotent, as on S3. |

The document is the `<CORSConfiguration>` XML the controller validated,
stored as versitygw's `ParseCORSOutput` reads it back. That is the pattern
the object-lock configuration and the bucket tag set follow: the controller
owns the schema, the backend owns the bytes.

## 3. Where it lives

`ingot.buckets.cors_config` (migration 00031), a nullable `bytea` read on
the same row as everything else `registry.Get` returns, so the per-request
read costs nothing extra. `registry.State.CORSConfig` carries it, nil when
the bucket has none of its own, and `SetCORSConfig` writes it, nil clearing.

The gateway-wide document is still marshalled once at construction
(`Deps.CORS`); `cors_allowed_origins` in the config is now the default for
buckets without a document of their own, and its comment says so.

## 4. Authorization

Hilt classifies `GET /{bucket}?cors` as `GetBucketCors` under
`s3:GetBucketCORS`, a policy-grantable read, and `PUT` and `DELETE ?cors`
as `PutBucketCors` and `DeleteBucketCors` under `s3:PutBucketCORS`, held by
service keys only like the other bucket configuration writes. A delete
needs the write permission, as on S3, which has no separate delete action.
None of the three needs a Forge command: the document lives in the
registry.

## 5. Tests

- `s3frontend/bucketcors_test.go`: the default, the replacement for one
  bucket alone, the delete round-trip, the size cap, and the missing
  bucket outranking everything. `s3frontend/cors_test.go` keeps the round
  trip of the gateway-wide document through versitygw's parser.
- `itest`: `TestForgeScenarios/CORS/per_bucket` sets a document on one
  bucket and checks the preflights on it and on a sibling, then deletes it
  and sees the default return; `TestForgeRoutePermissions` checks the
  writes are refused to a key holding only the read. Both run against a
  hilt override or with `INGOT_ITEST_CORS_WRITES=1`, until the published
  hilt image authorizes the writes.
