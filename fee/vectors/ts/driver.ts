// driver.ts drives the real (pinned) foc-encryption reference implementation to
// (re)generate and verify the FEE cross-implementation fixtures under
// ../testdata. It is invoked by ../pull-foc-encryption.sh, which vendors the
// pinned source into ./vendor/foc-encryption and installs cborg. Run with bun:
//
//   bun driver.ts [generate|verify|all]   (default: all)
//
// generate — encrypt a multi-chunk file with foc-encryption and write the
//            `multi-chunk-ts` fixture (AC2: TS seals, Go decrypts).
// verify   — decrypt every committed fixture with foc-encryption and check the
//            recovered plaintext (AC1/AC3: TS decrypts the Go-sealed blobs, and
//            the reference parses their recipient descriptors). Exits non-zero on
//            any mismatch.
import { CoseAlgorithm, decrypt, encrypt, parseEnvelope } from './vendor/foc-encryption/src/index.ts'
import { createHash } from 'node:crypto'
import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'

const HERE = import.meta.dir
const TESTDATA = join(HERE, '..', 'testdata')

// Must match the Go side (fee/vectors/helpers_test.go) and the reference
// (packages/foc-encryption/src/cose/headers.ts).
const FEE_TYP = 'application/vnd.foc-envelope+cose'
const CHUNK_SIZE = 4096

function sha256(label: string, n = 32): Uint8Array {
  return new Uint8Array(createHash('sha256').update(label).digest()).subarray(0, n)
}
const toHex = (u: Uint8Array): string => Buffer.from(u).toString('hex')
const fromHex = (h: string): Uint8Array => new Uint8Array(Buffer.from(h, 'hex'))

async function generateMultiChunkTS(): Promise<void> {
  const name = 'multi-chunk-ts'
  const cek = sha256('fil-473-fee-cek-ts-v1')

  // Deterministic ~15 KiB plaintext so the STREAM spans several 4 KiB chunks.
  const unit = new TextEncoder().encode('multi-chunk-ts/FIL-473 ')
  const bytes: number[] = []
  while (bytes.length < 15000) for (const b of unit) bytes.push(b)
  const plaintext = new Uint8Array(bytes)

  const blob = await encrypt(plaintext, cek, {
    algorithm: CoseAlgorithm.CHUNKED_AES_256_GCM_STREAM,
    chunkSize: CHUNK_SIZE,
  })
  const meta = parseEnvelope(blob)

  const dir = join(TESTDATA, name)
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true })
  writeFileSync(join(dir, 'blob.bin'), blob)
  writeFileSync(join(dir, 'plaintext.bin'), plaintext)
  writeFileSync(
    join(dir, 'meta.json'),
    JSON.stringify(
      {
        name,
        producer: 'ts',
        description: 'AC2: multi-chunk file encrypted in foc-encryption (TS); decrypts in Go.',
        tag: 16,
        algorithm: CoseAlgorithm.CHUNKED_AES_256_GCM_STREAM,
        typ: FEE_TYP,
        chunk_size: meta.chunkSize ?? CHUNK_SIZE,
        chunk_count: meta.chunkCount ?? 0,
        cek_hex: toHex(cek),
      },
      null,
      2,
    ) + '\n',
  )
  console.log(`generated ${name}: blob=${blob.length}B chunks=${meta.chunkCount}`)
}

async function verifyAll(): Promise<number> {
  let failures = 0
  let checked = 0
  for (const entry of readdirSync(TESTDATA, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue
    const dir = join(TESTDATA, entry.name)
    const metaPath = join(dir, 'meta.json')
    if (!existsSync(metaPath)) continue

    const meta = JSON.parse(readFileSync(metaPath, 'utf8'))
    const blob = new Uint8Array(readFileSync(join(dir, 'blob.bin')))
    const expected = new Uint8Array(readFileSync(join(dir, 'plaintext.bin')))
    checked++
    try {
      const got = await decrypt(blob, fromHex(meta.cek_hex))
      if (Buffer.compare(Buffer.from(got), Buffer.from(expected)) === 0) {
        console.log(`PASS ${meta.name} (producer=${meta.producer}, tag=${meta.tag})`)
      } else {
        console.error(`FAIL ${meta.name}: recovered plaintext does not match`)
        failures++
      }
    } catch (err) {
      console.error(`FAIL ${meta.name}: ${(err as Error).message}`)
      failures++
    }
  }
  console.log(`verified ${checked} fixture(s) with foc-encryption; ${failures} failure(s)`)
  return failures
}

const mode = process.argv[2] ?? 'all'
if (mode === 'generate' || mode === 'all') await generateMultiChunkTS()
let failures = 0
if (mode === 'verify' || mode === 'all') failures = await verifyAll()
if (failures > 0) {
  console.error(`cross-implementation check FAILED: ${failures} fixture(s)`)
  process.exit(1)
}
console.log('cross-implementation check OK')
