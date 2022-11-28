import url from 'url';
import fs from 'fs';
import path from 'path';
import glob from 'glob';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import { timestampToString, removeEmptyDirectoryRecursive, idToSubdirectory, csvEscape } from './reddit-util.mjs';

console.log('NOTE: Starting...');

// Generate report of submissions and comments
function generateReport(subreddit, options) {

    // Input and output directories
    const collatedDataDir = path.join(options.data, `${subreddit}${options.collatedDirectoryExtension}-ndjson`);
    const reportDataDir = path.join(options.data, `${subreddit}${options.reportDirectoryExtension}-${options.output}`);

    console.log(`--- REPORT: ${subreddit}/ -- ${collatedDataDir} --> ${reportDataDir}`);

    // Source collated data directory must exist
    if (!fs.existsSync(collatedDataDir) || !fs.lstatSync(collatedDataDir).isDirectory()) {
        console.log(`ERROR: No collated data directory: ${collatedDataDir} -- skipping report generation.`);
        return;
    }

    // Check collated index and any existed report index
    const collatedIndex = path.join(collatedDataDir, `${options.collatedIndexFilename}.ndjson`);
    const reportIndex = path.join(reportDataDir, `${options.reportIndexFilename}.${options.output}`);
    if (!fs.existsSync(collatedIndex)) {
        console.log(`ERROR: No collated submissions index: ${collatedIndex} -- skipping report generation.`);
        return;
    }

// TODO: Read and sort the collatedIndex
throw new Error("Not implemented")

    // Regenerate if the report is missing, or the collated index is newer
    let generatedIndex = false;
    if (!fs.existsSync(reportIndex) || fs.statSync(collatedIndex).mtimeMs > fs.statSync(reportIndex).mtimeMs) {
        generatedIndex = true;
        console.log(`NOTE: (Re)generating report index...`);
// TODO: Write the reportIndex
throw new Error("Not implemented")
    } else {
        console.log(`NOTE: Report index skipped (collated index is not newer).`);
    }

    let fileCount = 0;

// TODO: For each submission, check if the collated comments file is newer than the report, regenerate if required (reports include calculating the order and nesting of comments)
throw new Error("Not implemented")

    console.log(`------ ${fileCount}+${generatedIndex ? '1' : '0'} file(s) --> ${reportDataDir}`);
}

// Generate a report for a specific subreddit
function report(subreddit, options) {
    // Purge
    if (options.purge) {
        const reportDataDir = path.join(options.data, `${subreddit}${options.reportDirectoryExtension}-${options.output}`);
        console.log(`PURGE: Begin: ${reportDataDir}`);

        if (options.source != 'specified') {
            console.log(`ERROR: --purge option will only work for explicitly specified subreddits, use option --subreddit`);
            return;
        }

        // Find existing report files
        const existingReportFiles = glob.sync(path.join(reportDataDir, `**/submission-*.${options.output}`), { nodir: true });
        const reportIndex = path.join(reportDataDir, `${options.reportIndexFilename}.${options.output}`)
        if (fs.existsSync(reportIndex)) { existingReportFiles.push(reportIndex); }
        if (existingReportFiles.length <= 0) {
            console.log(`PURGE: No existing report files to delete.`);
        } else {
            console.log(`PURGE: Deleting ${existingReportFiles.length} existing report file(s)...`);
            for (const existingReportFile of existingReportFiles) {
                fs.unlinkSync(existingReportFile);
            }
        }

        if (!fs.existsSync(reportDataDir) || !fs.statSync(reportDataDir).isDirectory()) {
            console.log('PURGE: No report data directory to delete.');
        } else if (removeEmptyDirectoryRecursive(reportDataDir)) {
            console.log('PURGE: Report data directory successfully removed.');
        } else {
            console.log('PURGE: Report data directory not entirely removed (tree may contain be other files).');
        }
        console.log('PURGE: Completed');
    }

    // Report generation
    generateReport(subreddit, options);
}

// Run the collator
function run(options) {
    if (!['csv'].includes(options.output)) {
        console.log(`ERROR: Unknown output type (expected 'csv'): ${options.output}`);
        return;
    }

    // If no subreddits specified, find any existing collations in the data directory
    if (options.subreddit.length == 0) {
        const globSpecReport = `${options.data}/*${options.reportDirectoryExtension}-${options.output}/`;
        const existingDirectories = glob.sync(globSpecReport);
        options.subreddit = existingDirectories.map(dir => path.basename(dir).slice(0, -(options.reportDirectoryExtension.length + 1 + options.output.length)));
        if (options.subreddit.length == 0) {
            console.log(`WARNING: Nothing to do -- no subreddits specified, and no existing reports were found at ${globSpecReport}`);
            options.source = 'none';
        } else {
            console.log(`NOTE: Subreddits not specified, but using ${options.subreddit.length} subreddit(s) found in report directory: ${options.subreddit.join(', ')}`);
            options.source = 'data';
        }
    } else {
        console.log(`NOTE: Scraping ${options.subreddit.length} subreddit(s) specified: ${options.subreddit.join(', ')}`);
        options.source = 'specified';
    }

    // Generate a report for each subreddit
    for (const subreddit of options.subreddit) {
        report(subreddit, options);
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
            describe: 'Subreddit to generate reports for (if none specified, existing reports are updated)',
            type: 'string',
        })
        .option('data', {
            default: defaultOptions.data,
            describe: 'Data folder (defaults to data folder with script)',
            type: 'string'
        })
        .option('output', {
            default: defaultOptions.output,
            describe: `Output type ('csv').`,
            type: 'string'
        })
        .option('purge', {
            default: defaultOptions.purge,
            describe: 'Remove existing reports (subreddits must be explicitly specified)',
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
    collatedDirectoryExtension: '.collated',
    reportDirectoryExtension: '.report',
    maxOpenFiles: 400,
    permalinkPrefix: 'https://www.reddit.com',
    purge: false,
    source: null, // none/data/collations/specified
    collatedIndexFilename: 'index',
    reportIndexFilename: 'submissions',
    output: 'csv', // ndjson/csv
};


// Run
main(process.argv, defaultOptions);
console.log('NOTE: Finishing...');
