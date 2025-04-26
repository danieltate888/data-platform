// ==================== Global Dependencies ====================
require("reflect-metadata");
require('dotenv').config();

const express = require('express');
const cors = require('cors');
const app = express();
const PORT = process.env.PORT || 3000;

// ==================== Database Initialization ====================
const { AppDataSource } = require("./src/data-source");

// ==================== Routes =====================================
const routes = require('./src/routes');

// ==================== Global Middleware Setup ====================
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ==================== Start App after DB Connection ====================
console.log("Initializing database connection...");
AppDataSource.initialize()
    .then(() => {
        console.log("database connection successful!");

        // Register all API routes
        app.use('/api', routes);

        // Start the server
        app.listen(PORT, () => {
            console.log(`Server is running at http://localhost:${PORT} in ${process.env.NODE_ENV} mode`);
        });
    })
    .catch((error) => {
        console.log("database connection error:", error);
    });