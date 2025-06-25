## Dedup Index Backfill

First, build the binary:
```bash
cargo build --release
```

Download the shared bigquery dump json file, say `dump.json`

Import the dump by:
```bash
./target/release/dedup-backfill import dump.json
```

Run a check to get some stats before starting backfill:
```bash
./target/release/dedup-backfill check --cutoff 2025-06-24T17:29:57Z
```

Once the numbers are confirmed, run the backfill
```bash
./target/release/dedup-backfill insert --cutoff 2025-06-24T17:29:57Z --token $STDB_ACCESS_TOKEN
```
