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
}

export default (new ArrayUtil()) as ArrayUtil;