# Reddit scraper

## Scraper

Example usage:

<!--
```bash
npm start -- --subreddit ShitMomGroupsSay
```

or:
-->

```bash
node reddit-scraper.mjs --subreddit ShitMomGroupsSay
```


## Collator

Notes:

* Consider creating (and scraping to) ndjson files, rather than raw files, for more efficient append and seek to last entry.
* Outputs to CSV files, based on submission id.
* Process subreddit's submissions all to a single file.
* Process comments to an individual file, one per submission (with submission details included at the top)
  * How does submission content fit into the first comment row?
  * Warn if a comment's submission is not found (it should be if submissions are processed first, but may not be if the submission was deleted).  Append to a warning log file.
* Batch run, incremental updates based on file last-modified date (adding any new submissions, and appending any missing comments)
  * Locate file to append
  * If exist, detect last created_utc; otherwise create new file.
  * Append content
  * Keep multiple files open and flush/close if oldest when trying to open more than a fixed limit, or when application ending.


<!--

## Notes

Pushshift: https://pushshift.io/api-parameters/
...notes: https://www.reddit.com/r/pushshift/comments/bcxguf/new_to_pushshift_read_this_faq/

-->

