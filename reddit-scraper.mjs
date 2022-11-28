import url from 'url';
import fs from 'fs';
import path from 'path';
import glob from 'glob';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import fetch from 'node-fetch';
import { sleep, timestampToString, timestampToFilename, stringToTimestamp } from './reddit-util.mjs';

console.log('NOTE: Starting...');

// Scrape submissions or comments
async function scrape(subreddit, type, options) {

    // Output directory
    const dataDir = path.join(options.data, `${subreddit}${options.scrapeDirectoryExtension}`);
    console.log(`--- SCRAPE: ${subreddit}/${type} --> ${dataDir}`);

    // Check for existing data to resume from
    const existingFiles = glob.sync(path.join(dataDir, `${type}${options.filenameSeparator}*${options.filenameExtension}`), { nodir: true });
    let mostRecentTimestamp = null;
    for (const filename of existingFiles) {
        // Extract timestamp from filename
        const timestamp = stringToTimestamp(path.basename(filename).slice(`${type}${options.filenameSeparator}`.length, -(options.filenameExtension.length)));

        // Confirm filename round-trips (i.e. is a valid time and formatted as expected)
        const expectedFilename = `${type}${options.filenameSeparator}${timestampToFilename(timestamp)}${options.filenameExtension}`;
        if (path.basename(filename) != expectedFilename) {
            console.log(`WARNING: Filename was not correctly parsed, ignoring: ${path.basename(filename)} -- parsed as ${timestamp} = ${timestampToString(timestamp, null)} -- expected filename: ${expectedFilename}`);
            continue;
        }

        // Update most recent timestamp
        if (timestamp != null && timestamp > mostRecentTimestamp) {
            mostRecentTimestamp = timestamp;
        }
    }
    
    if (mostRecentTimestamp == null) {
        console.log('NOTE: Performing initial scraping (not resuming as there is no stored data).');
    } else {
        console.log(`NOTE: Resuming scraping from most recent file time: ${mostRecentTimestamp} -- ${timestampToString(mostRecentTimestamp, null)}`);
    }

    // Loop to scrape next batch from most recent timestamp and save to files with the appropriate last date for the set
    let requestCount = 0;
    let resultCount = 0;
    let errors = 0;
    for (;;) {
        // Maximum count
        if (options.maxRequests != null && requestCount >= options.maxRequests) {
            console.log(`NOTE: Maximum number of requests reached (${requestCount}) -- stopping.`);
            break;
        }

        // Wait
        console.log(`... ${options.requestDelay} ms`);
        await sleep(options.requestDelay);

        // API URL
        let url = `${options.baseUrl}${options.searchUrl[type]}?subreddit=${subreddit}&sort=asc&sort_type=created_utc&size=${options.maxSize}`;
        if (mostRecentTimestamp != null) {
            url += `&after=${Math.floor(mostRecentTimestamp / 1000)}`;
        }

        // Request
        requestCount++;
        console.log(`<<< #${requestCount} ${url}`);
        const response = await fetch(url);
        if (!response.ok) {
            errors++;
            console.log(`ERROR: #${errors} Failed to fetch data: ${response.status} ${response.statusText}`);
            if (errors > options.maxErrors) {
                console.log(`ERROR: Maximum error count reached -- stopping.`);
                break;
            }
            continue;
        }
        errors = 0;

        // Parse response
        const json = await response.json();
        console.log(`=== ${json.data.length} ${type}`);

        // Sense check results
        if (json.data.length == 0) {
            console.log('NOTE: No more results, stopping.');
            break;
        }
        const resultFirstCreated = json.data[0].created_utc * 1000;
        const resultLastCreated = json.data[json.data.length - 1].created_utc * 1000;
        if (resultFirstCreated > resultLastCreated) {
            console.log(`ERROR: Data sense check failed, first result is newer than last result (${resultFirstCreated} > ${resultLastCreated}) -- stopping.`);
            break;
        }
        if (resultFirstCreated < mostRecentTimestamp || resultLastCreated < mostRecentTimestamp) {
            console.log(`ERROR: Data sense check failed, results are older than earliest requested (${resultFirstCreated} < ${mostRecentTimestamp} || ${resultLastCreated} < ${mostRecentTimestamp}) -- stopping.`);
            break;
        }

        // Update
        mostRecentTimestamp = resultLastCreated;
        resultCount += json.data.length;
        const filename = path.join(dataDir, `${type}${options.filenameSeparator}${timestampToFilename(mostRecentTimestamp)}${options.filenameExtension}`);
        console.log(`>>> #${json.data.length}/${resultCount} ${filename}`);

        // Ensure the directory exists at write time (at write time to prevent directory creation for invalid subreddits)
        fs.mkdirSync(dataDir, { recursive: true });

        // Write JSON to file
        fs.writeFileSync(filename, JSON.stringify(json.data));
    }

    console.log(`------ ${resultCount} ${type} from ${requestCount} requests --> ${dataDir}`);

}

// Run the scraper
async function run(options) {

    // If no subreddits specified, find any in the data directory
    if (options.subreddit.length == 0) {
        const globSpec = `${options.data}/*${options.scrapeDirectoryExtension}/`;
        const existingDirectories = glob.sync(globSpec);
        options.subreddit = existingDirectories.map(dir => path.basename(dir).slice(0, -(options.scrapeDirectoryExtension.length)));

        if (options.subreddit.length == 0) {
            console.log(`WARNING: Nothing to do -- no subreddits specified, and no existing ones were found in the data directory: ${globSpec}`);
        } else {
            console.log(`NOTE: Subreddits not specified -- using ${options.subreddit.length} subreddit(s) found in data directory: ${options.subreddit.join(', ')}`);
        }
    } else {
        console.log(`NOTE: Scraping ${options.subreddit.length} subreddit(s) specified: ${options.subreddit.join(', ')}`);
    }

    // Scrape each subreddit/type
    for (const subreddit of options.subreddit) {
        if (options.submissions) {
            await scrape(subreddit, 'submissions', options);
        }
        if (options.comments) {
            await scrape(subreddit, 'comments', options);
        }
    }
}


// Handle command-line arguments and run the scraper
function main(argv, defaultOptions) {
    // Command-line options
    const args = yargs(hideBin(process.argv))
        .scriptName('reddit-scraper')
        .option('subreddit', {
            default: [],
            array: true,
            describe: 'Subreddit to scrape (if none specified, existing scrapes are continued)',
            type: 'string',
        })
        .option('data', {
            default: defaultOptions.data,
            describe: 'Data folder (defaults to data folder with script)',
            type: 'string'
        })
        .option('submissions', {
            default: defaultOptions.submissions,
            describe: 'Scrape submissions (default, use --no-submissions to disable)',
            type: 'boolean'
        })
        .option('comments', {
            default: defaultOptions.comments,
            describe: 'Scrape comments (default, use --no-comments to disable)',
            type: 'boolean'
        })
        .argv;

    const options = Object.assign({}, defaultOptions, args);
    run(options);
}


// Default options
const dirname = path.dirname(url.fileURLToPath(import.meta.url));
const defaultOptions = {
    subreddits: null,
    baseUrl: 'https://api.pushshift.io',
    searchUrl: {
        'subreddits': '/reddit/subreddit/search',
        'submissions': '/reddit/submission/search',
        'comments': '/reddit/comment/search',
    },
    submissions: true,
    comments: true,
    data: path.join(dirname, 'data'),
    filenameSeparator: '-',
    filenameExtension: '.json',
    scrapeDirectoryExtension: '.reddit',
    maxSize: 500,
    maxRequests: null,
    maxErrors: 3,
    requestDelay: 500,
};


// Run
main(process.argv, defaultOptions);
console.log('NOTE: Finishing...');
