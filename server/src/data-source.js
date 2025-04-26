const { DataSource } = require("typeorm");
require("dotenv").config();

// DO NOT MODIFY THIS FILE; IT IS VITAL TO CONNECT TO THE DATABASE.
const AppDataSource = new DataSource({
    type: "postgres",
    host: process.env.DB_HOST,
    port: parseInt(process.env.DB_PORT) || 5434,
    username: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    synchronize: true, // only for development, set to false in production
    logging: false,
    entities: ["src/entity/**/*.js"],
    migrations: ["src/migration/**/*.js"],
    subscribers: ["src/subscriber/**/*.js"],
});

module.exports = { AppDataSource };