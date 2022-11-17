import url from 'url';
import fs from 'fs';
import path from 'path';
import glob from 'glob';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import fetch from 'node-fetch';

console.log('NOTE: Starting...');

// Converts a timestamp into a human-readable string
function timestampString(timestamp) {
    if (timestamp == null) return '';
    if (!(timestamp instanceof Date) && typeof timestamp !== 'string') timestamp = new Date(timestamp);
    if (typeof timestamp !== 'string') timestamp = timestamp.toISOString();
    return timestamp.replace(/[T]/g, ' ').replace(/Z/g, '');
}

// Converts a millisecond timestamp into a filename string
function toFilenameTimestamp(timestamp) {
    if (timestamp == null) return '';
    if (!(timestamp instanceof Date) && typeof timestamp !== 'string') timestamp = new Date(timestamp);
    if (typeof timestamp !== 'string') timestamp = timestamp.toISOString();
    return timestamp.replace(/[T:\.]/g, '-').replace(/Z/g, '');
}

// Parses a filename string to a millisecond timestamp
function parseFilenameTimestamp(ts) {
    if (ts == null || ts == '') return null;
    const separators = '--T::.';
    const dateString = ts.split(/[-T :Z\.]/g).map((part, index) => {
        if (index > separators.length) return '';
        if (index > 0) {
            return separators[index - 1] + part;
        } else {
            return part;
        }
    }).join('');
    return (new Date(dateString)).getTime();
}


async function scrape(options, type) {

    // Ensure output directory exists
    const dataDir = path.join(options.data, options.subreddit);
    fs.mkdirSync(dataDir, { recursive: true });

    // Check for existing data to resume from
    const existingFiles = glob.sync(`${dataDir}/${type}-*.json`);
    let mostRecentTimestamp = null;
    let mostRecentFile = null;
    for (const filename of existingFiles) {
        const timestamp = parseFilenameTimestamp(path.basename(filename).replace(`${type}-`, '').replace('.json', ''));
        if (timestamp != null && timestamp > mostRecentTimestamp) {
            mostRecentTimestamp = timestamp;
            mostRecentFile = filename;
            const expectedFilename = `${type}-${toFilenameTimestamp(mostRecentTimestamp)}.json`;
            if (path.basename(filename) != expectedFilename) {
                console.log(`WARNING: File ${filename} was not correctly parsed (${expectedFilename})`);
            }
        }
    }

    console.log(`NOTE: Resuming from most recent file: ${mostRecentFile} -- ${timestampString(mostRecentTimestamp)}`);

    // TODO: Loop to scrape next 500 from most recent timestamp and save to files with the appropriate last date for the set

}


async function run(options) {
    if (options.scrapeSubmissions) {
        scrape(options, 'submissions');
    }
    if (options.scrapeComments) {
        scrape(options, 'comments');
    }
}

function main(argv, defaultOptions) {
    // Command-line options
    const args = yargs(hideBin(process.argv))
        .scriptName('reddit-scraper')
        .option('subreddit', {
            //alias: 's',
            demandOption: true,
            describe: 'Subreddit to scrape',
            type: 'string'
        })
        .option('data', {
            default: defaultOptions.data,
            describe: 'Data folder (defaults to data folder with script)',
            type: 'string'
        })
        .option('scrape-submissions', {
            default: defaultOptions.scrapeSubmissions,
            describe: 'Scrape submissions (default, use --no-scrape-submissions to disable)',
            type: 'boolean'
        })
        .option('scrape-comments', {
            default: defaultOptions.scrapeComments,
            describe: 'Scrape comments (default, use --no-scrape-comments to disable)',
            type: 'boolean'
        })
        .argv;

    const options = Object.assign({}, defaultOptions, args);
    run(options);
}

const dirname = path.dirname(url.fileURLToPath(import.meta.url));
const defaultOptions = {
    subreddit: null,
    baseUrl: 'https://api.pushshift.io',
    searchUrl: {
        'subreddits': '/reddit/subreddit/search',
        'submissions': '/reddit/submission/search',
        'comments': '/reddit/comment/search',
    },
    scrapeSubmissions: true,
    scrapeComments: true,
    data: path.join(dirname, 'data'),
};

main(process.argv, defaultOptions);
console.log('NOTE: Finishing...');
