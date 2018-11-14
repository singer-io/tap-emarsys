# Changelog

## 0.4.3
  * Update version of `requests` to `2.20.0` in response to CVE 2018-18074

## 0.4.2
  * Adds the field name deduplication logic to the sync mode of `contacts` to be able to ensure that fields are available in the same way they were discovered. [#8](https://github.com/singer-io/tap-emarsys/pull/8)

## 0.4.1
  * Fixes a bug where a `noramlize_field` would share names with another. The field's id is now appended to the end to ensure uniqueness [#7](https://github.com/singer-io/tap-emarsys/pull/7)

## 0.3.1
  * Fixes a memory issue when syncing contacts [#4](https://github.com/singer-io/tap-emarsys/pull/4)
