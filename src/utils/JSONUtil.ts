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
}

export default (new JSONUtil()) as JSONUtil;