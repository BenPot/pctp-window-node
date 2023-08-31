import storage from "node-persist";

class StorageService {
    private location: string = './storage';
    public createStorage(path: string): storage.LocalStorage {
        return storage.create({
            dir: `${this.location}/path`,
            stringify: JSON.stringify,
            parse: JSON.parse,
            encoding: 'utf8',
            // can also be custom logging function
            logging: false,
            // every 2 minutes the process will clean-up the expired cache
            expiredInterval: 2 * 60 * 1000,
            // in some cases, you (or some other service) might add non-valid storage files to your
            // storage dir, i.e. Google Drive, make this true if you'd like to ignore these files and not throw an error
            forgiveParseErrors: false,
            // how often to check for pending writes, don't worry if you feel like 1s is a lot, it actually tries to process every time you setItem as well
            // writeQueueIntervalMs: 1000,
            // if you setItem() multiple times to the same key, only the last one would be set, BUT the others would still resolve with the results of the last one, if you turn this to false, each one will execute, but might slow down the writing process.
            // writeQueueWriteOnlyLast: true
        });
    }
}

export default (new StorageService()) as StorageService;