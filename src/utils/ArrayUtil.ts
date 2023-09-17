class ArrayUtil {
    /**
     * removeElement
     */
    public removeElement(item: any, arr: any[]) {
        const index = arr.indexOf(item);
        if (index > -1) { // only splice array when item is found
            arr.splice(index, 1); // 2nd parameter means remove one item only
        }
    }

    public getArrayChunks<T>(arr: T[], chunkSize: number): [] | [T[]] {
        const retArr: any = [];
        while (true) {
            if (!!arr && arr.length > chunkSize) {
                retArr.push(arr.splice(0, chunkSize));
            } else {
                retArr.push(arr);
                break;
            }
        }
        return retArr;
    }
}

export default (new ArrayUtil()) as ArrayUtil;