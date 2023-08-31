import { Router, NextFunction, Request, Response } from "express";
import TabController from "../controllers/TabController";

export const tab = Router();

tab.get('/rows', async (req: Request, res: Response, next: NextFunction) => {
    try {
        res.status(201).json('test');
    } catch (e) {
        next(e);
    }
});