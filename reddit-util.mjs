
// Asynchronously wait for a specified time
export async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Converts a millisecond timestamp into a human-readable date/time string
// separator: undefined (ISO 8601), null (human-readable "YYYY-MM-DD HH:mm:ss.fff"), '-' (suitable for filenames "YYYY-MM-DD-HH-mm-ss-fff")
export function timestampToString(timestamp, separator) {
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
export function timestampToFilename(timestamp) {
    return timestampToString(timestamp, '-').slice(0, -4);  // Remove milliseconds
}

// Parses a string date/time to a millisecond timestamp
export function stringToTimestamp(ts) {
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

// Recursively remove a directory tree containing no files
export function removeEmptyDirectoryRecursive(directory) {
    if (!fs.statSync(directory).isDirectory()) return false;
    const files = fs.readdirSync(directory);
    let removed = 0;
    //console.log(`RMDIR-INFO: ${files.length}: ${directory}/{${files.join(',')}}`);
    for (const file of files) {
        if (removeEmptyDirectoryRecursive(path.join(directory, file))) removed++;
    }
    if (files.length - removed != 0) {
        return false;
    }
    fs.rmdirSync(directory);
    return true;
}

// Takes an ID string, e.g. 'abcdef' and returns a path string, e.g. 'ab/cd/ef'.
// For two-character splits in base-36, each directory will contain fewer than (36^2=) 1296 files.
export function idToSubdirectory(id) {
    //return id.replace(/(..)(?=[^$])/g, '$1\t').split('\t').join('/')
    const maxLetters = 4;
    const splitLetters = 2;
    const parts = [];
    while (id.length > maxLetters) {
        parts.push(id.substr(0, splitLetters));
        id = id.slice(splitLetters);
    }
    //if (id.length > 0) parts.push(id);
    return parts.join('/');
}

// Escape a CSV string value
export function csvEscape(value, force = true) {
    // All values as strings
    if (value == null) return '';   // null is empty string in CSV
    if (typeof value !== 'string') value = value.toString();
    if (value == '') return '""';   // always output empty string as quoted to differentiate from null
    // Double quotes must be escaped with a second double quote
    if (value.includes('"')) value = value.replace(/"/g, '""');
    // Strings containing commas, double quotes, or newlines must be wrapped in double quotes
    if (force || value.includes(',') || value.includes('"') || value.includes('\n')) value = `"${value}"`;
    return value;
}
