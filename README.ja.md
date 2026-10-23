# z3s

[English](README.md)

ZFS のスナップショットを S3 へ full/incremental でバックアップし、復元する CLI です。[presslabs/z3](https://github.com/presslabs/z3) の機能を参考にしていますが、データ形式の互換性はありません。

- send mode は raw (`zfs send -w`) と plain の 2 種類です。plain では gzip / zstd / xz / bzip2 による圧縮と、age 形式 (X25519 recipient) の暗号化を指定できます。
- 各バックアップオブジェクトの先頭に平文ヘッダ (CBOR) を置きます。インデックスはヘッダから再構築できるキャッシュです。
- スナップショットの作成・削除と、S3 上の古いバックアップの削除は行いません。

## ビルド

Java 21 以降と sbt 2 が必要です。

```
sbt assembly
java -jar target/out/jvm/scala-3.10.0/z3s/z3s.jar --help
```

Java 24 以降では Scala ランタイムが `sun.misc.Unsafe` を使うことについての警告が出ます。動作には影響しません。

## 使い方

共通オプションは各サブコマンドの後ろに書きます。コマンドライン引数が環境変数より優先されます。AWS の認証情報は SDK の標準の credential provider chain で取得します。

| オプション | 環境変数 | 意味 |
| --- | --- | --- |
| `--bucket` | `Z3S_BUCKET` | 保存先のバケット。必須 |
| `--prefix` | `Z3S_PREFIX` | オブジェクトキーの prefix。既定は空文字列 |
| `--dataset` | `Z3S_DATASET` | 対象の dataset。必須 |
| `--snapshot-prefix` | `Z3S_SNAPSHOT_PREFIX` | 対象とするスナップショット名 (`@` の後ろ) の prefix |
| `--region` | `AWS_REGION` | リージョン |
| `--endpoint` | `Z3S_ENDPOINT` | S3 互換ストレージの endpoint。指定すると path-style でアクセスし、チェックサムの付与を必要時のみに切り替えます |
| `--concurrency` | | S3 への並列リクエスト数。既定は 4 |
| `--progress` | `Z3S_PROGRESS` | `auto`、`bar`、`log`、`none` のいずれか。既定は `auto` |
| `--dry-run` | | 実行するコマンドと計画を表示し、S3 への書き込みと `zfs recv` を行いません |

```
# 初回は full で送る
z3s backup --bucket my-backups --dataset tank/data --full

# 以降は S3 上の最新スナップショットから incremental で送る
z3s backup --bucket my-backups --dataset tank/data --snapshot-prefix daily-

# zstd:9 で圧縮し、age で暗号化する
z3s backup --bucket my-backups --dataset tank/data --compress zstd:9 \
  --encrypt age --recipient age1... --recipient age1...

# ZFS ネイティブ暗号化された dataset を raw で送る
z3s backup --bucket my-backups --dataset tank/secret --raw --full

# 復元する
z3s restore daily-2026-10-23 --bucket my-backups --dataset tank/data --target tank/restored \
  --identity ~/.config/z3s/key.txt

z3s status --bucket my-backups --dataset tank/data [--refresh] [--json]
z3s reindex --bucket my-backups --dataset tank/data
```

- `backup` で `--snapshot` を省略すると、`--snapshot-prefix` に一致する最新のローカルスナップショットを送ります。incremental では、ローカルのスナップショットを createtxg の順に遡り、S3 上に同じ guid で存在する最新のものを起点にして、そこから対象までの各ペアを個別のオブジェクトとして送ります。
- `--compress` の既定値は `zstd:3` です。指定できる値は `none`、`gzip[:1-9]`、`zstd[:1-19]`、`xz[:0-9]`、`bzip2[:1-9]` です。
- incremental の親子で send mode (raw / plain) が異なる場合は中断します。圧縮方式と暗号化方式はオブジェクトごとに異なっていても構いません。
- `restore` は、復元先にすでにある連鎖中のスナップショット (guid で判定) の次から受信します。`--force` を付けると `zfs recv -F` を使います。

進捗は標準エラー出力に表示します。`backup` は、`zfs send` が出力したバイト数を `zfs send -nvP` の見積もりに対する割合で表示します。圧縮と暗号化の前のバイト数です。`restore` は、S3 からダウンロードしたバイト数をオブジェクトサイズに対する割合で表示します。`bar` は 1 行を書き換え、`log` は 10% 進むごとか 60 秒ごとに 1 行を出します。`auto` は、コンソールが端末なら `bar`、そうでなければ `log` を使います。実際のサイズが見積もりを超えた場合、割合は 99.9% で止まり、最後の行に実際のサイズを表示します。

終了コードは 0 (成功)、1 (実行時エラー)、2 (引数エラー)、3 (健全性チェックによる中断) です。

## 未完了の multipart upload の削除

プロセスが強制終了されると、未完了の multipart upload がバケットに残ります。バケットに lifecycle ルールを設定して削除してください。

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

## S3 オブジェクトの形式

- バックアップオブジェクトのキーは `{prefix}{dataset}@{snapshot}`、インデックスのキーは `{prefix}{dataset}/.z3s-index.cbor` です。
- オブジェクトの先頭は magic `5a 33 53 42 55 50 00 00` ("Z3SBUP\0\0")、format version (u16 BE、現在は 1)、ヘッダ長 (u32 BE、65,536 以下)、ヘッダ (CBOR) の順に並び、その後ろが payload です。
- payload は、raw では `zfs send -w` の出力そのまま、plain では `zfs send` の出力を圧縮したものです。age で暗号化する場合は「ヘッダの SHA-256 (32 バイト) + 圧縮済みストリーム」を age 形式で暗号化します。復元時にはこの 32 バイトを平文ヘッダと照合します。
- ヘッダは文字列キーの CBOR map で、未知のキーは無視してデコードします。
- 補助情報として user metadata の `z3s-format` と `z3s-parent` を付けます。処理の判断には使いません。

## テスト

```
sbt testFull
```

sbt 2の`test`は、変更の影響を受けるテストだけを実行します。すべてのテストを実行するには`testFull`を使ってください。

S3 互換サーバに対する結合テストは、環境変数を設定したときだけ実行されます。sbt 2 はサーバプロセスの環境変数をテスト JVM に引き継ぐため、先に `sbt shutdown` で既存のサーバを止めてください。

```
sbt shutdown
Z3S_IT_ENDPOINT=http://127.0.0.1:9000 Z3S_IT_ACCESS_KEY=... Z3S_IT_SECRET_KEY=... sbt "testOnly dev.capslock.z3s.S3IntegrationSuite"
```

公式の age との相互運用テストは、`AGE_BIN` と `AGE_KEYGEN_BIN`、または `PATH` 上の `age` と `age-keygen` を使います。見つからない場合は skip します。

ZFS のプールを使う結合テストはまだありません。
