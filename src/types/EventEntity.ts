import { Response } from "express";

export type EventId = {
    id: string,
    serial: string,
}

export type Subscriber = {
    id: string,
    response: Response,
}