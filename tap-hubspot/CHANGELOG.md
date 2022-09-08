# Changelog

## 2.6.4
  * When making `deals` requests, only attach `properties` if selected [#102](https://github.com/singer-io/tap-hubspot/pull/102)

## 2.6.3
  * Use the metadata library better

## 2.6.2
  * Revert the revert. Go back to v2.6.0.

## 2.6.1
  * Revert v2.6.0 to v.2.5.2

## 2.6.0
  * Replaced `annotated_schema` with Singer `metadata`
  * Added integration tests to CircleCI

## 2.5.2
  * Companies and Engagements have a new pattern to catch records that are updated during a long-running sync. Rather than using a lookback window, the bookmark value will be limited to the `min(current_sync_start, max_bk_seen)` [#98](https://github.com/singer-io/tap-hubspot/pull/98)

## 2.4.0
  * The owners stream can optionally fetch "inactive owners" [#92](https://github.com/singer-io/tap-hubspot/pull/92)

## 2.3.0
  * Engagements will now track how long the stream takes to sync, and look back on the next run by that amount to cover potentially missed updates due to asynchronous updates during the previous sync [#91](https://github.com/singer-io/tap-hubspot/pull/91)

## 2.2.8
  * When resuming an interrupted sync, will now attempt all streams before exiting [#90](https://github.com/singer-io/tap-hubspot/pull/90)

## 2.2.7
  * Add `delivered`, `forward`, `print`, `reply`, `spamreport` to `campaigns.counters`

## 2.2.6
  * Change a loop over `dict.items()` to `dict.values()` because the keys returned were not being used [#82](https://github.com/singer-io/tap-hubspot/pull/82)

## 2.2.5
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## 2.2.4
  * Ensure that deal associations are being retrieved if `associations` are selected in the catalog [#79](https://github.com/singer-io/tap-hubspot/pull/79)

## 2.2.3
  * Scrub the access token from error messages Hubspot returns when there are insufficient permissions [#75](https://github.com/singer-io/tap-hubspot/pull/75)

## 2.2.2
  * Fix a bug with the 'engagements' stream which requires the 'engagement' field to have automatic inclusion [#74](https://github.com/singer-io/tap-hubspot/pull/74)

## 2.2.1
  * Fix a bug with the 'inclusion' metadata for replication_key fields [#72](https://github.com/singer-io/tap-hubspot/pull/72)

## 2.2.0
  * Adds property selection to the tap [#67](https://github.com/singer-io/tap-hubspot/pull/67)
  * Removed the keywords stream as it is deprecated [#68](https://github.com/singer-io/tap-hubspot/pull/68)
  * Schema updates [#69](https://github.com/singer-io/tap-hubspot/pull/69) [#70](https://github.com/singer-io/tap-hubspot/pull/70)
