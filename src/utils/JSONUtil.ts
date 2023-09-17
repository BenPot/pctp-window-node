import { readFileSync, writeFileSync  } from 'fs';

class JSONUtil {
    /**
     * tryParse
     */
    public tryParse(jsonStr: string): unknown | boolean {
        try {
            const o = JSON.parse(jsonStr);
            if (Object.keys(o).length && typeof o === 'object') return o
        } catch (error) {
            console.log(error);
        }
        return false;
    }

    public parseObjectFromFile<T>(filePath: string): T | null {
        try {
            const data = readFileSync(filePath);
            return JSON.parse(data.toString()) as T;
        } catch (error) {
            console.log(error);
        }
        return null;
    }

    public writeObjectToFile(filePath: string, object: any): boolean {
        try {
            writeFileSync(filePath, JSON.stringify(object));
            return true;
        } catch (error) {
            console.log(error);
        }
        return false;
    }
}

export default (new JSONUtil()) as JSONUtil;