# z3s

[日本語](README.ja.md)

z3s is a CLI that backs up ZFS snapshots to S3 as full or incremental streams and restores them. It is inspired by [presslabs/z3](https://github.com/presslabs/z3), but its data format is not compatible with z3.

- Two send modes: raw (`zfs send -w`) and plain. Plain mode supports gzip / zstd / xz / bzip2 compression and age encryption (X25519 recipients).
- Every backup object starts with a plaintext CBOR header that describes how to restore it. The index is a cache that can be rebuilt from these headers.
- z3s does not create or destroy snapshots, and does not delete old backups from S3.

## Build

Requires Java 21 or later and sbt 2.

```
sbt assembly
java -jar target/out/jvm/scala-3.10.0/z3s/z3s.jar --help
```

On Java 24 and later, the JVM prints a warning about the Scala runtime calling `sun.misc.Unsafe`. It does not affect behavior.

## Usage

Common options go after the subcommand. Command-line arguments take precedence over environment variables. AWS credentials are taken from the SDK's default credential provider chain.

| Option | Environment variable | Meaning |
| --- | --- | --- |
| `--bucket` | `Z3S_BUCKET` | Destination bucket. Required |
| `--prefix` | `Z3S_PREFIX` | Prefix of object keys. Defaults to an empty string |
| `--dataset` | `Z3S_DATASET` | Target dataset. Required |
| `--snapshot-prefix` | `Z3S_SNAPSHOT_PREFIX` | Only consider snapshots whose name (the part after `@`) starts with this |
| `--region` | `AWS_REGION` | Region |
| `--endpoint` | `Z3S_ENDPOINT` | Endpoint of S3-compatible storage. When set, z3s uses path-style access and sends checksums only when required |
| `--concurrency` | | Number of parallel S3 requests. Defaults to 4 |
| `--dry-run` | | Print the commands and the plan without writing to S3 or running `zfs recv` |

```
# Send a full stream first
z3s backup --bucket my-backups --dataset tank/data --full

# Then send incrementals from the latest snapshot on S3
z3s backup --bucket my-backups --dataset tank/data --snapshot-prefix daily-

# Compress with zstd:9 and encrypt with age
z3s backup --bucket my-backups --dataset tank/data --compress zstd:9 \
  --encrypt age --recipient age1... --recipient age1...

# Send a natively encrypted dataset as is
z3s backup --bucket my-backups --dataset tank/secret --raw --full

# Restore
z3s restore daily-2026-10-23 --bucket my-backups --dataset tank/data --target tank/restored \
  --identity ~/.config/z3s/key.txt

z3s status --bucket my-backups --dataset tank/data [--refresh] [--json]
z3s reindex --bucket my-backups --dataset tank/data
```

- Without `--snapshot`, `backup` sends the latest local snapshot matching `--snapshot-prefix`. For an incremental backup, z3s walks back through local snapshots in createtxg order, picks the latest one that exists on S3 with the same guid as the base, and sends each consecutive pair from the base to the target as a separate object.
- `--compress` defaults to `zstd:3`. Accepted values are `none`, `gzip[:1-9]`, `zstd[:1-19]`, `xz[:0-9]` and `bzip2[:1-9]`.
- z3s refuses an incremental whose parent was sent in a different send mode (raw or plain). Compression and encryption may differ between objects.
- `restore` starts receiving after the latest snapshot in the chain that the target already has (matched by guid). `--force` uses `zfs recv -F`.

Exit codes: 0 (success), 1 (runtime error), 2 (usage error), 3 (aborted by a health check).

## Cleaning up incomplete multipart uploads

If the process is killed, an incomplete multipart upload remains in the bucket. Configure a lifecycle rule to remove it.

```json
{
  "Rules": [
    {
      "ID": "abort-incomplete-multipart-upload",
      "Status": "Enabled",
      "Filter": { "Prefix": "" },
      "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 7 }
    }
  ]
}
```

```
aws s3api put-bucket-lifecycle-configuration --bucket my-backups --lifecycle-configuration file://lifecycle.json
```

## Object format

- Backup objects are stored at `{prefix}{dataset}@{snapshot}`, and the index at `{prefix}{dataset}/.z3s-index.cbor`.
- An object starts with the magic `5a 33 53 42 55 50 00 00` ("Z3SBUP\0\0"), followed by the format version (u16 BE, currently 1), the header length (u32 BE, at most 65,536) and the CBOR header. The payload follows.
- In raw mode the payload is the output of `zfs send -w`. In plain mode it is the compressed output of `zfs send`. With age encryption, z3s encrypts "SHA-256 of the header (32 bytes) + compressed stream", and checks those 32 bytes against the plaintext header on restore.
- The header is a CBOR map with string keys. Unknown keys are ignored when decoding.
- The user metadata `z3s-format` and `z3s-parent` are informational only.

## Tests

```
sbt test
```

Integration tests against an S3-compatible server run only when the environment variables below are set. sbt 2 passes the environment of its server process to the test JVM, so stop any running server with `sbt shutdown` first.

```
sbt shutdown
Z3S_IT_ENDPOINT=http://127.0.0.1:9000 Z3S_IT_ACCESS_KEY=... Z3S_IT_SECRET_KEY=... sbt "testOnly dev.capslock.z3s.S3IntegrationSuite"
```

Interoperability tests with the reference age implementation use `AGE_BIN` and `AGE_KEYGEN_BIN`, or `age` and `age-keygen` on `PATH`. They are skipped if neither is found.

There are no integration tests against a real ZFS pool yet.
