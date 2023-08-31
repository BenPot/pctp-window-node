class DateUtil {
    /**
     * getTimeSerial
     */
    public getTimeSerial(): number {
        return (new Date()).getTime();
    }
}

export default (new DateUtil()) as DateUtil;