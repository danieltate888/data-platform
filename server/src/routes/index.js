const express = require('express');
const horseRouter = require('./horse.route');
const etlRouter = require('./etl.route');
const predictRouter = require('./predict.route');

const router = express.Router();

router.use('/horses', horseRouter);
router.use('/etl', etlRouter);
router.use('/predict', predictRouter);

module.exports = router;