import url from 'url';
import fs from 'fs';
import path from 'path';
import glob from 'glob';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import { timestampToString, removeEmptyDirectoryRecursive, idToSubdirectory, csvEscape, timestampToFilename, timestampToYearMonthSubdirectory } from './reddit-util.mjs';

console.log('NOTE: Starting...');

// Generate report of submissions and comments
function generateReport(subreddit, options) {

    // Input and output directories
    const collatedDataDir = path.join(options.data, `${subreddit}${options.collatedDirectoryExtension}`);
    const reportDataDir = path.join(options.data, `${subreddit}${options.reportDirectoryExtension}-${options.output}`);
    
    console.log(`--- REPORT: ${subreddit} -- ${collatedDataDir} --> ${reportDataDir}`);

    // Source collated data directory must exist
    if (!fs.existsSync(collatedDataDir) || !fs.lstatSync(collatedDataDir).isDirectory()) {
        console.log(`ERROR: No collated data directory: ${collatedDataDir} -- skipping report generation.`);
        return;
    }

    // Read collated index
    const collatedIndex = path.join(collatedDataDir, `submissions-${options.collatedIndexFilename}${options.collatedExtension}`);
    if (!fs.existsSync(collatedIndex)) {
        console.log(`ERROR: No collated submissions index: ${collatedIndex} -- skipping report generation.`);
        return;
    }

    // Read collated index
    const collatedIndexData = fs.readFileSync(collatedIndex, 'utf8').split('\n').filter(line => line.length > 0).map(line => JSON.parse(line));

    // Process: Ensure sorted by timestamp (should be scraped by timestamp)
    collatedIndexData.sort((a, b) => a.created_utc - b.created_utc);
    collatedIndexData.forEach((submission, index) => {
        submission.used_url = (submission.url != submission.full_link) ? submission.url : '';     // Only use posted url if it's different from full_link
        submission.full_permalink = options.permalinkPrefix + submission.permalink;
        submission.filename = timestampToYearMonthSubdirectory(submission.created_utc * 1000) + '/' + 'submission' + '-' + timestampToFilename(submission.created_utc * 1000) + '-' + submission.id + '.' + options.output;
    });

    // Regenerate report index if missing, or the collated index is newer
    const reportIndex = path.join(reportDataDir, `${options.reportIndexFilename}.${options.output}`);
    let generatedIndex = false;
    if (!fs.existsSync(reportIndex) || fs.statSync(collatedIndex).mtimeMs > fs.statSync(reportIndex).mtimeMs) {
        generatedIndex = true;
        console.log(`NOTE: (Re)generating report index...`);

        // Write report index
        fs.mkdirSync(reportDataDir, { recursive: true });
        if (options.output == 'csv') {
            const rows = [];

            // UTF-8 BOM, so Excel opens as code page 65001 (UTF-8)
            rows.push('\ufeff' + 'SubmissionId,Created,Author,Title,Text,URL,Link\n');

            // Data rows
            for (const submission of collatedIndexData) {
                // .id -- submission id
                // .created_utc -- created time seconds since epoch
                // .author -- author username
                // .title -- submission title
                // .selftext -- submission text
                // .url -- posted URL
                // .full_link -- link to Reddit submission
                rows.push(`${csvEscape(submission.id)},${timestampToString(submission.created_utc * 1000, null).slice(0, -4)},${csvEscape(submission.author)},${csvEscape(submission.title)},${csvEscape(submission.selftext)},${csvEscape(submission.used_url, false)},${csvEscape(submission.full_link, false)}\n`);
            }

            // Write rows to file
            fs.writeFileSync(reportIndex, rows.join(''));

        } else if (options.output == 'json') {
            // Write object as JSON to file
            //fs.writeFileSync(reportIndex, JSON.stringify(collatedIndexData));

            // Write records, one per line, as valid JSON
            fs.writeFileSync(reportIndex, '[' + collatedIndexData.map(submission => JSON.stringify(submission)).join('\n,') + '\n]');

        } else {
            console.log(`ERROR: Unknown output type: ${options.output}`);
            return;
        }
    } else {
        console.log(`NOTE: Report index skipped (collated index is not newer).`);
    }

    // For each submission, check if the collated comments file is newer than the report, regenerate if required (reports include calculating the order and nesting of comments)
    let submissionCount = 0;
    let fileCount = 0;
    let lastUpdate = null;
    for (const submission of collatedIndexData) {
        submissionCount++;
        if (lastUpdate == null || Date.now() - lastUpdate > 1000 || submission === collatedIndexData[collatedIndexData.length - 1]) {
            console.log(`...reporting file ${submissionCount}/${collatedIndexData.length} (${(100 * submissionCount / (collatedIndexData.length - 1)).toFixed(0)}%)`);
            lastUpdate = Date.now();
        }

        const collatedSubdirectory = path.join(collatedDataDir, idToSubdirectory(submission.id));
        const collatedCommentsFile = path.join(collatedSubdirectory, `comments-${submission.id}${options.collatedExtension}`);

        const reportFile = path.join(reportDataDir, path.normalize(submission.filename));    // Normalize for '/'->'\' on Windows

        // Create report if it is missing, or if the collated comments file is newer
        const hasComments = fs.existsSync(collatedCommentsFile);
        if (!fs.existsSync(reportFile) || (hasComments && fs.statSync(collatedCommentsFile).mtimeMs > fs.statSync(reportFile).mtimeMs)) {
            // Read collated comments
            const sourceComments = !hasComments ? [] : fs.readFileSync(collatedCommentsFile, 'utf8').split('\n').filter(line => line.length > 0).map(line => JSON.parse(line));

            // Process comments to add additional fields
            sourceComments.sort((a, b) => a.created_utc - b.created_utc);
            sourceComments.forEach((comment) => {
                comment.full_permalink = options.permalinkPrefix + comment.permalink;
                comment.parent = (comment.parent_id == comment.link_id) ? null : comment.parent_id.replace(/^t[13]_/, '');
                if (comment.nest_level === undefined) {
                    comment.nest_level = null;
                }
            });
            
            // Create map of unsorted comments
            const childCommentsForId = {};
            for (const comment of sourceComments) {
                const parentId = comment.parent;
                if (!childCommentsForId[parentId]) {
                    childCommentsForId[parentId] = [];
                }
                childCommentsForId[parentId].push(comment);
            }

            // Visit comment hierarchy
            const comments = [];
            function visitComments(id, depth = 0) {
                const children = childCommentsForId[id];
                if (children) {
                    for (const child of children) {
                        child.depth = depth;
                        comments.push(child);
                        visitComments(child.id, depth + 1);
                    }
                }
            }
            // Add comments under the parent submission (and their children, in turn)
            //visitComments(submission.id);
            // Add unassigned comments (possibly from deleted parents?)            
            visitComments(null);

            // Verify no comments are missing
            if (comments.length != sourceComments.length) {
                console.log(`ERROR: No all comments were sorted correctly, only ${comments.length} != ${sourceComments.length}`);
                return;
            }

            // Output
            fs.mkdirSync(path.dirname(reportFile), { recursive: true });
            if (options.output == 'csv') {
                const rows = [];

                // UTF-8 BOM, so Excel opens as code page 65001 (UTF-8)
                rows.push('\ufeff' + 'Depth,CommentId,ParentId,Created,Author,Title,Body,Url,Link\n');
    
                //  Add submission as a first row (appear as a comment with no parent, with title and possible URL)
                // (depth -- empty for submission)
                // .id -- submission id
                // (parent_id not set for submission)
                // .created_utc -- created time seconds since epoch
                // .author -- author username
                // .title -- submission title
                // .selftext -- submission text
                // .url -- posted URL (if set)
                // .full_link -- link to Reddit submission                
                rows.push(`,${csvEscape(submission.id)},,${timestampToString(submission.created_utc * 1000, null).slice(0, -4)},${csvEscape(submission.author)},${csvEscape(submission.title)},${csvEscape(submission.selftext)},${csvEscape(submission.url, false)},${csvEscape(submission.full_permalink, false)}\n`);

                // Data rows
                for (const comment of comments) {
                    // .depth (calculated); .nest_level (original)
                    // .id -- comment id
                    // .parent_id ('t1_...') -- parent comment id
                    // .created_utc -- created time seconds since epoch
                    // .author -- author username
                    // (title -- empty for comments, only set for submission)
                    // .body -- comment text
                    // (url -- empty for comments, only set for submission)
                    // link
                    rows.push(`${comment.depth},${csvEscape(comment.id)},${csvEscape(comment.parent)},${timestampToString(comment.created_utc * 1000, null).slice(0, -4)},${csvEscape(comment.author)},,${csvEscape(comment.body)},,${csvEscape(comment.full_permalink, false)}\n`);
                }
    
                // Write rows to file
                fs.writeFileSync(reportFile, rows.join(''));

            } else if (options.output == 'json') {
                // Write object as JSON to file
                //fs.writeFileSync(reportFile, JSON.stringify(comments));

                // Write records, one per line, as valid JSON
                fs.writeFileSync(reportFile, '[' + comments.map(comment => JSON.stringify(comment)).join('\n,') + '\n]');

            } else {
                console.log(`ERROR: Unknown output type: ${options.output}`);
                return;
            }

            fileCount++;
        }
    }

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
        const existingReportFiles = glob.sync(path.join(reportDataDir, `**/submission-*.${options.output}`).replaceAll(path.sep, '/'), { nodir: true });
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
    if (!['csv', 'json'].includes(options.output)) {
        console.log(`ERROR: Unknown output type (expected 'csv' or 'json'): ${options.output}`);
        return;
    }

    // If no subreddits specified, find any existing collations in the data directory
    if (options.subreddit.length == 0) {
        const globSpecReport = `${options.data}${path.sep}*${options.reportDirectoryExtension}-${options.output}${path.sep}`;
        const existingDirectories = glob.sync(globSpecReport.replaceAll(path.sep, '/'));
        options.subreddit = existingDirectories.map(dir => path.basename(dir).slice(0, -(options.reportDirectoryExtension.length + 1 + options.output.length)));
        if (options.subreddit.length == 0) {
            // If no subreddits specified and no existing reports, find any existing collations
            const globSpecCollated = `${options.data}${path.sep}*${options.collatedDirectoryExtension}${path.sep}`;
            const existingDirectories = glob.sync(globSpecCollated.replaceAll(path.sep, '/'));
            options.subreddit = existingDirectories.map(dir => path.basename(dir).slice(0, -(options.collatedDirectoryExtension.length)));
            if (options.subreddit.length == 0) {
                console.log(`WARNING: Nothing to do -- no subreddits specified, and no existing reports were found at ${globSpecReport} -- and no existing collations were found at ${globSpecCollated}`);
                options.source = 'none';
            } else {
                console.log(`NOTE: Subreddits not specified, no existing reports were found at ${globSpecReport}, but using ${options.subreddit.length} collated subreddit(s) found in data directory: ${options.subreddit.join(', ')}`);
                options.source = 'collations';
            }
        } else {
            console.log(`NOTE: Subreddits not specified, but using ${options.subreddit.length} subreddit report(s) found in data directory: ${options.subreddit.join(', ')}`);
            options.source = 'reports';
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
            describe: `Output type ('csv' or 'json').`,
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
    data: path.join(dirname, 'data'),
    filenameSeparator: '-',
    filenameExtension: '.json',
    collatedExtension: '.ndjson',
    collatedDirectoryExtension: '.collated',
    reportDirectoryExtension: '.report',
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
