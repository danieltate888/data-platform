const express = require('express');
const horseRouter = require('./horse.route');
const etlRouter = require('./etl');

const router = express.Router();

router.use('/horses', horseRouter);
router.use('/etl', etlRouter);

module.exports = router;