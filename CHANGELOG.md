# 0.14.0

- Bump peer dependencies.

# 0.13.0

- Omit nested fields for `update` operations.

# 0.12.0

- Fix `Document` type.

# 0.11.0

- Prepend `pipeline` with omit pipeline if `omit` is passed to `initSync`.

# 0.10.0

- Renamed `Options` to `SyncOptions`.

# 0.9.0

- Moved `omit` to `initSync` so that it can be used in the `processChangeStream` pipeline.

# 0.8.0

- Don't delete the last scan id key when initial scan is completed.
- Allow an initial scan to resume after completion by calling `clearCompletedOn` first.
- Export `getKeys` fn.
- Changed Redis key prefix from to `mongoChangeStream`.

# 0.7.0

- `omit` option for excluding undesired fields.

# 0.6.0

- `sortField` option for overriding the default sorting field of `_id`.

# 0.5.0

- Pass `QueueOptions` to `runInitialScan`.

# 0.4.0

- Bumped `prom-utils` to latest.

# 0.3.0

- Batch `runInitialScan`.

# 0.2.0

- Separate out event stream handling into the `processChangeStream` method.

# 0.1.0

- Initial release.
