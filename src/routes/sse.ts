import { Router, NextFunction, Request, Response } from "express";
import SSEController from "../controllers/SSEController";

export const sse = Router();

sse.get('/id-to-refresh', async (req: Request, res: Response, next: NextFunction) => {
    try {
        SSEController.register(req, res);
    } catch (e) {
        next(e);
    }
});