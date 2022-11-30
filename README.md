# Reddit scraper

Produces an offline archive of submissions and comments for a subreddit.

There are three steps:

1. **Scraper:** (Is not actually a web scraper!) Uses the [Pushshift API](https://pushshift.io) to download a subreddit's entire submission and comment archive.  These files are grouped purely by creation date, and not sumbission.

2. **Collator:** Rearranges the downloaded data to be per-submission.  This process can be run after a new scrape to add the latest data (assuming it is always run to completion).

3. **Reporter:** Takes the raw, per-submission data, adds some additional context (such as comment order, nested comment depth, etc), and generates "report" files.  This process can be 

These processes are designed so that they be run (in order) periodically to efficiently "top up" a local archive with the latest data, efficently collate new submission comments, and efficiently regenerate reports for submissions with new comments.


## Installing and running this software

1. If you do not already have *Node.js* installed, please install the current *LTS* version from: [nodejs.org/en/download](https://nodejs.org/en/download/).

2. Open a *Terminal* / *Command Prompt* / *Power Shell* window.

3. If you have not already installed the software:

   * If you are on *Mac OS*, *Linux* or *WSL*, copy and paste the following command, and press <kbd>Return</kbd>:

      ```bash
      curl -L https://github.com/digitalinteraction/reddit-scraper/archive/refs/heads/main.zip -o reddit-scraper-main.zip && unzip reddit-scraper-main.zip && rm reddit-scraper-main.zip && mv reddit-scraper-main reddit-scraper
      ```
      <!--
      mkdir reddit-scraper && curl -L https://github.com/digitalinteraction/reddit-scraper/archive/refs/heads/main.zip -o reddit-scraper/main.zip && unzip reddit-scraper/main.zip -d reddit-scraper && cp -r reddit-scraper/reddit-scraper-main/* reddit-scraper && rm reddit-scraper/main.zip && rm -r reddit-scraper/reddit-scraper-main
      -->
   
      * Or, if you are on *Windows*, copy and paste the following command, and press <kbd>Enter</kbd>:

      ```batch
      powershell -Command "& {Invoke-WebRequest https://github.com/digitalinteraction/reddit-scraper/archive/refs/heads/main.zip -o reddit-scraper.zip ; Expand-Archive reddit-scraper.zip ; del reddit-scraper.zip ; copy -r reddit-scraper/reddit-scraper-main/* reddit-scraper ; del -r reddit-scraper/reddit-scraper-main }"
      ```

4. Change the working directory to the software location: 

    ```bash
    cd reddit-scraper
    ```

5. If you have not previously installed the software, install the library dependencies:

    ```bash
    npm install
    ```

6. You can now run the *Scraper*/*Collator* commands below.  To open the current directory in your computer's file browser:

   * On *Mac OS*:
   
      ```bash
      open .
      ```

   * On *Windows*:
   
      ```batch
      start .
      ```
      
   * On *WSL*:
   
      ```bash
      cmd.exe /c start .
      ```
      
   * On *Linux*:
   
      ```bash
      xdg-open .
      ```

## Scraper

This uses [Pushshift API](https://pushshift.io) to download the entire submission and comment archive for a specific subreddit.

Example usage:

<!--
```bash
npm start -- --subreddit reddit
```

or:
-->

```bash
node reddit-scraper.mjs --subreddit reddit
```

By default, the output will be in a directory `data/$SUBREDDIT.reddit`, where `$SUBREDDIT` is the subreddit you specified.

This process can be interrupted, run again to resume, and repeated to fetch the most recent data.


## Collator

This process takes the scraped raw submissions and comments and, collates them to a `data/$SUBREDDIT.collated-ndjson` directory as:

  * a single `submissions-index.ndjson` file, one line for each submission.
  
  * a file `comments-$SUBMISSIONID.ndjson` file, where `$SUBMISSIONID` is the ID of the submission, with one line for each comment. 

```bash
node reddit-collator.mjs --subreddit reddit
```

As there can be many comment files, these are stored in subdirectories of the first couple of characters of the submission ID.

This process must be allowed to run to completion, but may then be re-run if you have newly-scraped data, and it will efficiently add it to the collations.  If it does not run to completion, re-runs may be incomplete, and you must use the `--purge` flag to remove the existing partial collations and regenerate the entire collation.


## Reporter

This process takes the collated submissions and comments and produces a *report* file for each submission, with the submission content, and all comments (correctly nested) below.

This process can be repeated after a collation and will add any missing reports (e.g. new submissions) and recreate any reports where the collation has been modified (e.g. with newly-scraped comments).

The reports are written to a `data/$SUBREDDIT.report` directory as:

  * a single `submissions.csv` file, one row for each submission.
  
  * a file `submission-$DATE-$SUBMISSIONID.csv` file, where `$DATE` is the created date of the submission, and `$SUBMISSIONID` is the ID of the submission, and containing one row for each comment.  Nesting is identified by the number of asterisks in the initial column.  These files are placed in subdirectories -- one for each year and month (`YYYY-MM` format).

If preferred, you can change the output to machine-readable `.json` with `--output json`.


<!--

## Notes

Pushshift: https://pushshift.io/api-parameters/
...notes: https://www.reddit.com/r/pushshift/comments/bcxguf/new_to_pushshift_read_this_faq/

-->

