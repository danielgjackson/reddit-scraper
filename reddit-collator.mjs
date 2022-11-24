import url from 'url';
import fs from 'fs';
import path from 'path';
import glob from 'glob';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import { getSystemErrorMap } from 'util';

console.log('NOTE: Starting...');

// Asynchronously wait for a specified time
async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Converts a millisecond timestamp into a human-readable date/time string
// separator: undefined (ISO 8601), null (human-readable "YYYY-MM-DD HH:mm:ss.fff"), '-' (suitable for filenames "YYYY-MM-DD-HH-mm-ss-fff")
function timestampToString(timestamp, separator) {
    if (timestamp == null) return '';
    if (!(timestamp instanceof Date) && typeof timestamp !== 'string') timestamp = new Date(timestamp);
    if (typeof timestamp !== 'string') timestamp = timestamp.toISOString();
    if (separator === null) {
        timestamp = timestamp.replace(/Z/g, '').replace(/[T]/g, ' ');
    } else if (separator != null) {
        timestamp = timestamp.replace(/Z/g, '').replace(/[-T: \.]/g, separator);
    }
    return timestamp;
}

// Converts a millisecond timestamp into a filename-compatible date/time string
function timestampToFilename(timestamp) {
    return timestampToString(timestamp, '-').slice(0, -4);  // Remove milliseconds
}

// Parses a string date/time to a millisecond timestamp
function stringToTimestamp(ts) {
    if (ts == null || ts == '') return null;
    const separators = '--T::.';
    const dateString = ts.split(/[-T :Z\.]/g).map((part, index) => {
        if (index > separators.length) return '';
        if (index > 0) {
            return separators[index - 1] + part;
        } else {
            return part;
        }
    }).join('') + 'Z';
    return (new Date(dateString)).getTime();
}

// Takes an ID string, e.g. 'abcdefg' and returns a path string, e.g. 'ab/cd/ef/g'.
// Assuming base-36, each directory will contain fewer than (36^2=) 1296 files.
function idToSubdirectory(id) {
    return id.replace(/(..)(?=[^$])/g, '$1\t').split('\t').join('/')
}

// Collate submissions or comments
async function collate(subreddit, type, options) {

    // Input and output directories
    const scrapeDataDir = path.join(options.data, `${subreddit}${options.scrapeDirectoryExtension}`);
    const collatedDataDir = path.join(options.data, `${subreddit}${options.collatedDirectoryExtension}`);
    console.log(`--- COLLATE: ${subreddit}/${type} -- ${scrapeDataDir} --> ${collatedDataDir}`);

    // Source data directory must exist
    if (!fs.existsSync(scrapeDataDir) || !fs.lstatSync(scrapeDataDir).isDirectory()) {
        console.log(`ERROR: No scrape data directory: ${scrapeDataDir} -- skipping collation.`);
        return;
    }

    // Find existing collated files
    const existingCollatedFiles = glob.sync(path.join(collatedDataDir, `**/${type}${options.filenameSeparator}*${options.collatedExtension}`), { nodir: true });

    // Purge
    if (options.purge) {
        throw new Error('Purge not implemented yet');
// TODO: Do not allow non-specified subreddit purge

    }

    // Check existing collated file timestamps
    let lastModified = null;
    for (const filename of existingCollatedFiles) {
        const modified = fs.lstatSync(filename).mtime.getTime();
        if (modified > lastModified) lastModified = modified;
    }

    // Find newer data files
    const globDataFiles = path.join(scrapeDataDir, `${type}${options.filenameSeparator}*${options.filenameExtension}`);
    const allDataFiles = glob.sync(globDataFiles, { nodir: true }).sort();
    let dataFiles = allDataFiles;
    if (lastModified) {
        dataFiles = dataFiles.filter(filename => {
            const modified = fs.lstatSync(filename).mtime.getTime();
            return modified > lastModified;
        });
    }

    // Indicate to user what we're going to do
    if (lastModified == null) {
        if (dataFiles.length == 0) {
            console.log('NOTE: No data files to collate.');
        } else {
            console.log(`NOTE: Performing full collation of ${dataFiles.length} files from ${allDataFiles.length} files of stored data at: ${globDataFiles}`);
        }
    } else {
        if (dataFiles.length == 0) {
            console.log(`NOTE: No new data files were found to resume collation modified since: ${lastModified} -- ${timestampToString(lastModified, null)}`);
        } else {
            console.log(`NOTE: Resuming collation from ${dataFiles.length} files that were modified since: ${lastModified} -- ${timestampToString(lastModified, null)}`);
        }
    }

    // Cache of open files (filename to stream) -- ensure closed even if an error occurs
    const streams = {};
    let fileCount = 0;
    let recordCount = 0;
    try {
        // For each new data file, open to determine records, collate into relevant output files.
        for (const dataFile of dataFiles) {
            const contents = fs.readFileSync(dataFile, 'utf8');
            const records = JSON.parse(contents);
            for (const record of records) {
                let subdirectory;
                let filename;
                if (type == 'submissions') {
                    subdirectory = collatedDataDir;
                    filename = `all-submissions.${options.collatedExtension}`;
                } else if (type == 'comments') {
                    const submission = record.link_id.replace(/^t3_/, '');
                    subdirectory = path.join(collatedDataDir, idToSubdirectory(submission));
                    filename = `submission${options.filenameSeparator}${submission}.${options.collatedExtension}`;
                } else {
                    throw new Error(`Invalid type: ${type}`);
                }

                const collatedFilename = path.join(subdirectory, filename);

                // If stream not already open
                if (!streams[filename]) {
                    // If too many streams open, close the oldest one
                    while (Object.keys(streams).length > 0 && Object.keys(streams).length >= options.maxOpenFiles) {
                        let oldestLastWritten = -1;
                        let oldestFilename = null;
                        for (const filename in streams) {
                            if (streams[filename].lastWritten > oldestLastWritten || oldestLastWritten < 0) {
                                oldestLastWritten = streams[filename].lastWritten;
                                oldestFilename = filename;
                            }
                        }
                        if (!oldestFilename) break;
                        //console.log(`--- ${oldestFilename}`);
                        streams[oldestFilename].end();
                        delete streams[oldestFilename];
                    }

                    // Ensure the directory exists
                    fs.mkdirSync(subdirectory, { recursive: true });

                    // Open file as append
                    streams[filename] = fs.createWriteStream(collatedFilename, { flags: 'a' });

                    streams[filename].fullFilename = collatedFilename;
    
                    //console.log(`+++ ${collatedFilename}`);
                } else {
                    //console.log(`=== ${collatedFilename}`);
                }

                // Tag stream with last written serial number for LRU cache
                streams[filename].lastWritten = recordCount;

                // Write record to stream
                streams[filename].write(`${JSON.stringify(record)}\n`);
            }
            fileCount++;
        }

    } finally {
        // Close all open streams
        for (const filename in streams) {
            //console.log(`---- ${filename}`);
            streams[filename].end();
        }
        streams.length = 0;
    }

    console.log(`------ ${recordCount} ${type} from ${fileCount} files --> ${collatedDataDir}`);
}

// Run the collator
async function run(options) {

    // If no subreddits specified, find any existing collations in the data directory
    if (options.subreddit.length == 0) {
        const globSpecCollated = `${options.data}/*${options.collatedDirectoryExtension}/`;
        const existingDirectories = glob.sync(globSpecCollated);
        options.subreddit = existingDirectories.map(dir => path.basename(dir).slice(0, -(options.collatedDirectoryExtension.length)));
        if (options.subreddit.length == 0) {
            // If no subreddits specified and no existing collations, find any existing scraped subreddits
            const globSpecData = `${options.data}/*${options.scrapeDirectoryExtension}/`;
            const existingDirectories = glob.sync(globSpecData);
            options.subreddit = existingDirectories.map(dir => path.basename(dir).slice(0, -(options.scrapeDirectoryExtension.length)));
            if (options.subreddit.length == 0) {
                console.log(`WARNING: Nothing to do -- no subreddits specified, and no existing collations were found at ${globSpecCollated} -- and no existing scraped data was found at ${globSpecData}`);
            } else {
                console.log(`NOTE: Subreddits not specified, no existing collations were found at ${globSpecCollated}, but using ${options.subreddit.length} scraped subreddit(s) found in data directory: ${options.subreddit.join(', ')}`);
            }
        } else {
            console.log(`NOTE: Subreddits not specified -- using ${options.subreddit.length} subreddit collation(s) found in data directory: ${options.subreddit.join(', ')}`);
        }
    } else {
        console.log(`NOTE: Scraping ${options.subreddit.length} subreddit(s) specified: ${options.subreddit.join(', ')}`);
    }

    // Collate each subreddit/type
    for (const subreddit of options.subreddit) {
        if (options.submissions) {
            await collate(subreddit, 'submissions', options);
        }
        if (options.comments) {
            await collate(subreddit, 'comments', options);
        }
    }
}


// Handle command-line arguments and run the collator
function main(argv, defaultOptions) {
    // Command-line options
    const args = yargs(hideBin(process.argv))
        .scriptName('reddit-collator')
        .option('subreddit', {
            default: [],
            array: true,
            describe: 'Subreddit to collate (if none specified, existing collations are updated)',
            type: 'string',
        })
        .option('data', {
            default: defaultOptions.data,
            describe: 'Data folder (defaults to data folder with script)',
            type: 'string'
        })
        .option('submissions', {
            default: defaultOptions.submissions,
            describe: 'Collate submissions (default, use --no-submissions to disable)',
            type: 'boolean'
        })
        .option('comments', {
            default: defaultOptions.comments,
            describe: 'Collate comments (default, use --no-comments to disable)',
            type: 'boolean'
        })
        .option('purge', {
            default: defaultOptions.purge,
            describe: 'Remove existing collated data (subreddits must be explicitly specified)',
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
    submissions: true,
    comments: true,
    data: path.join(dirname, 'data'),
    filenameSeparator: '-',
    filenameExtension: '.json',
    scrapeDirectoryExtension: '.reddit',
    collatedDirectoryExtension: '.collated',
    maxOpenFiles: 256,
    purge: false,
    collatedExtension: 'ndjson',
};


// Run
main(process.argv, defaultOptions);
console.log('NOTE: Finishing...');
