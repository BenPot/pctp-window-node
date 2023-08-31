class TimeUtil {
    /**
     * sleep
     */
    public timeout(ms: number): Promise<unknown> {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

export default (new TimeUtil()) as TimeUtil;