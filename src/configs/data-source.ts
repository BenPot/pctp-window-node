import sql from 'mssql';

const liveConfig: string | sql.config = {
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    database: process.env.DB_NAME,
    server: <string>process.env.DB_SERVER,
    port: parseInt(process.env.DB_PORT || '', 10),
    pool: {
        max: 10,
        min: 0,
        idleTimeoutMillis: 30000
    },
    requestTimeout: 130000,
    options: {
        trustServerCertificate: true // change to true for local dev / self-signed certs
    }
}

export const livePool: sql.ConnectionPool = new sql.ConnectionPool(liveConfig);