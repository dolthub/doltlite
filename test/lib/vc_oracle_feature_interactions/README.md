# Feature-interaction oracle families

`test/vc_oracle_feature_interaction_test.sh` owns the harness and sources these
files in a fixed order. The fragments are not standalone test programs; they
share the runner's temporary directory, `oracle` helper, and result counters.

Place each new case with the feature whose behavior it primarily exercises:

- `merge.sh`: row merge behavior and general merge interactions
- `history.sh`: branches, commits, tags, reset, revert, cherry-pick, and logs
- `schema.sh`: DDL, indexes, keys, constraints, views, and generated columns
- `query.sh`: SQL expressions and query behavior after version-control changes
- `stress.sh`: deep histories, broad schemas, large batches, and fan-in graphs

Keep every case self-contained. The runner gives each oracle a separate
DoltLite database and Dolt repository based on its unique test name.
