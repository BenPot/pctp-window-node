import bodyParser from 'body-parser';
import errorhandler from 'strong-error-handler';
import express, { Express, NextFunction, Request, Response } from 'express';
import { tab } from '../routes/tab';
import dotenv from 'dotenv';

dotenv.config();

export const app: Express = express();

// middleware for parsing application/x-www-form-urlencoded
app.use(bodyParser.urlencoded({ extended: true }));

// middleware for json body parsing
app.use(bodyParser.json({ limit: '5mb' }));

app.use((req: Request, res: Response, next: NextFunction) => {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Expose-Headers", "x-total-count");
    res.header("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,PATCH");
    res.header("Access-Control-Allow-Headers", "Content-Type,authorization");
    next();
});

app.use('/tab', tab);

app.use(errorhandler({
    debug: process.env.ENV !== 'prod',
    log: true,
}));