# 0.25.0

- Latest `prom-utils` that includes `batchBytes` option.

# 0.24.0

- Use `eventemitter3`.

# 0.23.0

- `JSONSchema` type.

# 0.22.0

- Option to strip metadata from a JSON schema for `detectSchemaChange`.
- You can now await `changeStream.start` which resolves after the initial
change detection has completed.

# 0.21.0

- Ensure that you can call `start` after calling `stop`.

# 0.20.0

- Bump `mongodb` peer dep.

# 0.19.1

- Fix issue with `runInitialScan` where calling `stop` before the scan had finished
would incorrectly set the scan completed key in Redis. Also, `stop` now awaits flushing
the queue.

# 0.19.0

- BREAKING CHANGE: Changed API for `runInitialScan`. You must explicitly call `start` now.
The change stream can be stopped by calling `stop`.
- It is now possible to cleanly stop `runInitialScan` and `processChangeStream`, allowing
for a smooth restarting behavior if a schema change is detected.

# 0.18.0

- Await processing of event when calling `stop`.

# 0.17.0

- BREAKING CHANGE: `initSync` now takes `collection`.
- NEW: `detectSchemaChange`.

# 0.16.0

- BREAKING CHANGE: Changed API for `processChangeStream`. You must explicitly call `start` now.
The change stream can be stopped by calling `stop`.

# 0.15.0

- Export `getCollectionKey` util.

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
