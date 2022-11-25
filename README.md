# Reddit scraper

Produces an offline archive for a specific subreddit.


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

5. If you have not previously installed the software, install the `node` library dependencies:

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

This process takes the scraped raw submissions and comments and collates them into:

  * a single `submissions-all.csv` file, one row for each submission.
  
  * a file `comments-$SUBMISSIONID.csv` file, where `$SUBMISSIONID` is the ID of the submission, with one row for each comment. 

```bash
node reddit-collator.mjs --subreddit reddit
```

By default, the output will be in a directory `data/$SUBREDDIT.collated-csv`, where `$SUBREDDIT` is the subreddit you specified.  As there can be many comment files, these are stored in subdirectories of the first couple of characters of the submission ID.

This process must be allowed to run to completion, but may then be re-run if you have newly-scraped data, and it will efficiently add it to the collations.  If it does not run to completion, re-runs may be incomplete, and you should use the `--purge` flag to remove the existing partial collations.


<!--

## Notes

Pushshift: https://pushshift.io/api-parameters/
...notes: https://www.reddit.com/r/pushshift/comments/bcxguf/new_to_pushshift_read_this_faq/

-->

