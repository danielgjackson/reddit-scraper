import url from 'url';
import fs from 'fs';
import path from 'path';
import glob from 'glob';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import { timestampToString, removeEmptyDirectoryRecursive, idToSubdirectory, csvEscape } from './reddit-util.mjs';

console.log('NOTE: Starting...');


// Collate submissions or comments
function collateType(subreddit, type, options) {

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
    const existingCollatedFiles = glob.sync(path.join(collatedDataDir, `**/${type}${options.filenameSeparator}*${options.collatedExtension}`).replaceAll(path.sep, '/'), { nodir: true });

    // Check existing collated file timestamps
    let lastModified = null;
    for (const filename of existingCollatedFiles) {
        const modified = fs.lstatSync(filename).mtime.getTime();
        if (modified > lastModified) lastModified = modified;
    }

    // Find newer data files
    const globDataFiles = path.join(scrapeDataDir, `${type}${options.filenameSeparator}*${options.filenameExtension}`);
    const allDataFiles = glob.sync(globDataFiles.replaceAll(path.sep, '/'), { nodir: true }).sort();
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
        let lastUpdate = null;
        for (const dataFile of dataFiles) {
            fileCount++;
            if (lastUpdate == null || Date.now() - lastUpdate > 1000 || dataFile === dataFiles[dataFiles.length - 1]) {
                console.log(`...collating file ${fileCount}/${dataFiles.length} (${(100 * fileCount / dataFiles.length).toFixed(0)}%) - ${recordCount} ${type}`);
                lastUpdate = Date.now();
            }
            const contents = fs.readFileSync(dataFile, 'utf8');
            const records = JSON.parse(contents);
            for (const record of records) {
                let subdirectory;
                let filename;
                if (type == 'submissions') {
                    subdirectory = collatedDataDir;
                    filename = `${type}${options.filenameSeparator}${options.indexFilename}${options.collatedExtension}`;
                } else if (type == 'comments') {
                    const submission = record.link_id.replace(/^t3_/, '');
                    subdirectory = path.join(collatedDataDir, idToSubdirectory(submission));
                    filename = `${type}${options.filenameSeparator}${submission}${options.collatedExtension}`;
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
                            if (streams[filename].lastWritten < oldestLastWritten || oldestLastWritten < 0) {
                                oldestLastWritten = streams[filename].lastWritten;
                                oldestFilename = filename;
                            }
                        }
                        if (!oldestFilename) break;
                        //console.log(`--- ${oldestFilename} (${Object.keys(streams).length} @ ${oldestLastWritten})`);
                        streams[oldestFilename].end();
                        delete streams[oldestFilename];
                    }

                    // Ensure the directory exists
                    fs.mkdirSync(subdirectory, { recursive: true });

                    // Open file as append
                    let stream;
                    if (fs.existsSync(collatedFilename)) {
                        stream = fs.createWriteStream(collatedFilename, { flags: 'a' });
                    } else {
                        stream = fs.createWriteStream(collatedFilename);
                        // Write header if the file format requires
                        ;
                    }
                    
                    // Track open streams
                    streams[filename] = stream;
                    streams[filename].fullFilename = collatedFilename;
    
                    //console.log(`+++ ${collatedFilename}`);
                } else {
                    //console.log(`=== ${collatedFilename}`);
                }

                // Tag stream with last written serial number for LRU cache
                streams[filename].lastWritten = recordCount;

                // Write record to stream
                let outputString = null;
                outputString = `${JSON.stringify(record)}\n`
                streams[filename].write(outputString);

                recordCount++;
            }
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

// Collate a specific subreddit
function collate(subreddit, options) {
    // Purge
    if (options.purge) {
        const collatedDataDir = path.join(options.data, `${subreddit}${options.collatedDirectoryExtension}`);
        console.log(`PURGE: Begin: ${collatedDataDir}`);

        if (options.source != 'specified') {
            console.log(`ERROR: --purge option will only work for explicitly specified subreddits, use option --subreddit`);
            return;
        }

        // Purge each type of file
        for (const type of ['submissions', 'comments']) {
            // Find existing collated files
            const existingCollatedFiles = glob.sync(path.join(collatedDataDir, `**/${type}${options.filenameSeparator}*${options.collatedExtension}`).replaceAll(path.sep, '/'), { nodir: true });

            if (existingCollatedFiles.length <= 0) {
                console.log(`PURGE: No existing ${type} files to delete.`);
            } else {
                console.log(`PURGE: Deleting ${existingCollatedFiles.length} existing collated ${type} file(s)...`);
                for (const existingCollatedFile of existingCollatedFiles) {
                    fs.unlinkSync(existingCollatedFile);
                }
            }
        }

        if (!fs.existsSync(collatedDataDir) || !fs.statSync(collatedDataDir).isDirectory()) {
            console.log('PURGE: No data directory to delete.');
        } else if (removeEmptyDirectoryRecursive(collatedDataDir)) {
            console.log('PURGE: Data directory successfully removed.');
        } else {
            console.log('PURGE: Data directory not entirely removed (tree may contain be other files).');
        }
        console.log('PURGE: Completed');
    }

    // Collate
    if (options.submissions) {
        collateType(subreddit, 'submissions', options);
    }
    if (options.comments) {
        collateType(subreddit, 'comments', options);
    }
}

// Run the collator
function run(options) {
    // If no subreddits specified, find any existing collations in the data directory
    if (options.subreddit.length == 0) {
        const globSpecCollated = `${options.data}${path.sep}*${options.collatedDirectoryExtension}${path.sep}`;
        const existingDirectories = glob.sync(globSpecCollated.replaceAll(path.sep, '/'));
        options.subreddit = existingDirectories.map(dir => path.basename(dir).slice(0, -(options.collatedDirectoryExtension.length + 1)));
        if (options.subreddit.length == 0) {
            // If no subreddits specified and no existing collations, find any existing scraped subreddits
            const globSpecData = `${options.data}${path.sep}*${options.scrapeDirectoryExtension}${path.sep}`;
            const existingDirectories = glob.sync(globSpecData.replaceAll(path.sep, '/'));
            options.subreddit = existingDirectories.map(dir => path.basename(dir).slice(0, -(options.scrapeDirectoryExtension.length)));
            if (options.subreddit.length == 0) {
                console.log(`WARNING: Nothing to do -- no subreddits specified, and no existing collations were found at ${globSpecCollated} -- and no existing scraped data was found at ${globSpecData}`);
                options.source = 'none';
            } else {
                console.log(`NOTE: Subreddits not specified, no existing collations were found at ${globSpecCollated}, but using ${options.subreddit.length} scraped subreddit(s) found in data directory: ${options.subreddit.join(', ')}`);
                options.source = 'data';
            }
        } else {
            console.log(`NOTE: Subreddits not specified -- using ${options.subreddit.length} subreddit collation(s) found in data directory: ${options.subreddit.join(', ')}`);
            options.source = 'collations';
        }
    } else {
        console.log(`NOTE: Scraping ${options.subreddit.length} subreddit(s) specified: ${options.subreddit.join(', ')}`);
        options.source = 'specified';
    }

    // Collate each subreddit/type
    for (const subreddit of options.subreddit) {
        collate(subreddit, options);
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
    collatedExtension: '.ndjson',
    scrapeDirectoryExtension: '.reddit',
    collatedDirectoryExtension: '.collated',
    permalinkPrefix: 'https://www.reddit.com',
    purge: false,
    source: null, // none/data/collations/specified
    indexFilename: 'index'
};


// Run
main(process.argv, defaultOptions);
console.log('NOTE: Finishing...');
