const express = require('express');
const { exec } = require('child_process');
const router = express.Router();

router.post('/start', (req, res) => {
    exec('python3 backend/etl/etl_pipeline1.py', (error, stdout, stderr) => {
        if (error) {
            console.error(`ETL error: ${error.message}`);
            return res.status(500).json({ message: 'ETL process failed' });
        }
        if (stderr) {
            console.error(`ETL stderr: ${stderr}`);
        }
        console.log(`ETL stdout: ${stdout}`);
        res.json({ message: 'ETL process started' });
    });
});

module.exports = router;