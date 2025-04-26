const express = require('express');
const horseController = require('../controllers/horse.controller');

const router = express.Router();

// Add a new horse
router.post('/', horseController.createHorse);

// Get all horses
router.get('/', horseController.getAllHorses);

// Get a horse by ID
router.get('/:id', horseController.getHorseById);

module.exports = router;