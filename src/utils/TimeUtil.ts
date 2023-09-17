export class Timer {
    private startTime: Date | null = null;
    constructor() {
        this.startTime = null;
    }
    isStarted() {
        return this.startTime !== null;
    }
    reset() {
        this.startTime = new Date();
    }
    stop(inMinutes = false) {
        const endTime = this.getElapsedTime(inMinutes);
        this.startTime = null;
        return endTime;
    }
    public getElapsedTime(inMinutes = false): number | null {
        if (!!!this.startTime) return null;
        const currentTime = new Date();
        const timeDiffInMin = inMinutes ? ((currentTime.getTime() - this.startTime.getTime()) / 1000) / 60 : null;
        const timeDiffInSec = inMinutes ? null : ((currentTime.getTime() - this.startTime.getTime()) / 1000);
        return inMinutes ? timeDiffInMin : timeDiffInSec;
    }
    stopWithInfo(doIncludeSeconds = false): string | null {
        if (!!!this.startTime) return null;
        const endTime = new Date();
        const timeDiffInMin = ((endTime.getTime() - this.startTime.getTime()) / 1000) / 60;
        const timeDiffInSec = ((endTime.getTime() - this.startTime.getTime()) / 1000);
        this.startTime = null;
        return Math.round(timeDiffInMin) === 0 ? `${Math.round(timeDiffInSec)} sec`
            : `${Math.round(timeDiffInMin)} min ${doIncludeSeconds ? `${Math.round(timeDiffInSec)%60} sec` : ''}`
    }
}

class TimeUtil {
    /**
     * sleep
     */
    public timeout(ms: number): Promise<unknown> {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

export default (new TimeUtil()) as TimeUtil;