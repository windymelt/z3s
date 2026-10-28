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
| `--endpoint` | `Z3S_ENDPOINT` | Endpoint of S3-compatible storage. When set, z3s uses path-style access and sends checksums only when required. Content-MD5 is sent with every part in either case |
| `--concurrency` | | Number of parallel S3 requests. Defaults to 4 |
| `--progress` | `Z3S_PROGRESS` | `auto`, `bar`, `log` or `none`. Defaults to `auto` |
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
z3s verify --bucket my-backups --dataset tank/data [--snapshot NAME] [--sample N]
```

- Without `--snapshot`, `backup` sends the latest local snapshot matching `--snapshot-prefix`. For an incremental backup, z3s walks back through local snapshots in createtxg order, picks the latest one that exists on S3 with the same guid as the base, and sends each consecutive pair from the base to the target as a separate object.
- `--compress` defaults to `zstd:3`. Accepted values are `none`, `gzip[:1-9]`, `zstd[:1-19]`, `xz[:0-9]` and `bzip2[:1-9]`.
- z3s refuses an incremental whose parent was sent in a different send mode (raw or plain). Compression and encryption may differ between objects.
- `--recursive` (`-r`) also backs up the descendant datasets, from parents to children, each with its own objects and index. The snapshot name is chosen on the dataset given by `--dataset` and the same name is used for every descendant, so snapshots taken with `zfs snapshot -r` stay together; descendants without that snapshot are skipped with a warning. Datasets with ZFS native encryption are sent raw, and the others use the given mode. A dataset with no backup on S3 is sent full even without `--full`. If a dataset fails, the others are still backed up, and the failures are listed at the end with a non-zero exit code. `status --recursive` shows the same tree, including datasets that only remain on S3.
- `restore` starts receiving after the latest snapshot in the chain that the target already has (matched by guid). `--force` (or `-F`) uses `zfs recv -F`.
- `restore --recursive` restores every dataset under `--dataset` that has backups on S3, from parents to children. Each one is received at the same relative path under `--target`, so `tank/data/db` is received into `tank/restored/db` with `--target tank/restored`. The snapshot name is the same for every dataset, and datasets without a backup of that snapshot are skipped with a warning. Whether `--identity` is needed is checked for the whole tree before anything is received. If a dataset fails, its descendants are skipped because `zfs recv` cannot create them without the parent; the other datasets are still restored, and the failures are listed at the end with a non-zero exit code.

Progress is printed to standard error. `backup` counts the bytes that `zfs send` has written against the estimate from `zfs send -nvP`, before compression and encryption. `restore` counts the bytes downloaded from S3 against the object size. `bar` rewrites a single line, and `log` prints one line at every 10% step or every 60 seconds. `auto` uses `bar` when the console is a terminal and `log` otherwise. If the stream grows beyond the estimate, the percentage stops at 99.9% and the final line shows the actual size.

Exit codes: 0 (success), 1 (runtime error), 2 (usage error), 3 (aborted by a health check).

## Verifying backups

`verify` reads the backup objects in the index and checks them against the ETags on S3. It streams the objects and writes nothing to disk. Without `--snapshot` it checks every backup of the dataset, and `--recursive` (`-r`) also checks the descendant datasets.

- By default, `verify` reads each object to the end, computes the MD5 of each part, and compares MD5(concatenated part MD5s)-(number of parts) with the ETag. Every byte is downloaded, so the transfer and the time grow with the size of the backups. When the part MD5 list (see [Object format](#object-format)) matches the ETag, a mismatch is reported with the numbers of the damaged parts.
- `--sample N` reads only N randomly chosen parts of each object with ranged GETs, and compares each with the part MD5 list. It needs a list that matches the ETag, so objects backed up before z3s saved the list cannot be checked this way; run `verify` without `--sample` for them.
- An object whose ETag is not of the form `<MD5>-<number of parts>`, or whose content matches the part MD5 list but not the ETag, is reported as unverifiable and is not counted as a failure. On Amazon S3, objects encrypted with SSE-KMS or SSE-C have such ETags.
- Without the part MD5 list, a computed ETag that differs from the one on S3 is reported as a mismatch. This means the object is damaged, or the storage does not compute ETags from MD5s.
- If any object is damaged, mismatched, or listed in the index but missing on S3, `verify` lists them at the end and exits with 1.

## Resuming an interrupted backup

Raw backups and plain backups without encryption can be resumed. If such a backup stops before it completes, for example because it was interrupted or the network failed, z3s keeps the multipart upload and its state. Running the same `backup` again resumes it. `backup` prints where the state is kept when it starts.

- The state is kept in `--state-dir` (`Z3S_STATE_DIR`), which defaults to `$XDG_STATE_HOME/z3s` or `~/.local/state/z3s`. It holds the upload ID, the part size, the object header and the MD5 of each uploaded part. It contains no secrets.
- On resume, z3s runs `zfs send` from the beginning again and skips the parts that are already on S3. It compares each regenerated part with the recorded MD5. If they differ, z3s aborts the upload and the next run starts over.
- `--no-resume` aborts the interrupted upload and starts over. A different snapshot or mode also starts over.
- Age-encrypted backups are not resumed, because resuming would require storing the file key. They abort the upload when they fail.
- An upload removed by the lifecycle rule below cannot be resumed; the next run starts over.

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
- zstd frames carry a checksum (XXH64) of the decompressed content, which is checked on restore. Objects written without it can still be restored.
- The header is a CBOR map with string keys. Unknown keys are ignored when decoding. `partSize` is the part size of the multipart upload. Objects uploaded before this key was added do not have it.
- The MD5 of each part is stored at `{prefix}{dataset}/.z3s-parts/{snapshot}.cbor`, a CBOR map with `schemaVersion` (1), `partSize`, `size` (the object size) and `md5` (an array of 16-byte strings, in part order). Amazon S3, MinIO and Sakura Object Storage compute the ETag of a multipart object as MD5(concatenated part MD5s)-(number of parts), so the list can be checked against the ETag. This does not hold for objects encrypted with SSE-KMS or SSE-C on Amazon S3. If saving the list fails, z3s prints a warning and the backup still succeeds. When you delete a backup object, delete its list too.
- The user metadata `z3s-format` and `z3s-parent` are informational only.

## Tests

```
sbt testFull
```

In sbt 2, `test` only reruns tests affected by changes. Use `testFull` to run every test.

Integration tests against an S3-compatible server run only when the environment variables below are set. sbt 2 passes the environment of its server process to the test JVM, so stop any running server with `sbt shutdown` first.

```
sbt shutdown
Z3S_IT_ENDPOINT=http://127.0.0.1:9000 Z3S_IT_ACCESS_KEY=... Z3S_IT_SECRET_KEY=... sbt "testOnly dev.capslock.z3s.S3IntegrationSuite"
```

Interoperability tests with the reference age implementation use `AGE_BIN` and `AGE_KEYGEN_BIN`, or `age` and `age-keygen` on `PATH`. They are skipped if neither is found.

There are no integration tests against a real ZFS pool yet.
