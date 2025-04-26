const { AppDataSource } = require('../data-source');
const Horse = require('../entity/Horse');

// Create a new horse
const createHorse = async (req, res) => {
    try {
        const horseRepo = AppDataSource.getRepository('Horse');
        const horse = horseRepo.create(req.body);
        const result = await horseRepo.save(horse);
        res.status(201).json(result);
    } catch (error) {
        console.error('Error creating horse:', error);
        res.status(500).json({ message: 'Internal Server Error' });
    }
};

// Get all horses
const getAllHorses = async (req, res) => {
    try {
        const horseRepo = AppDataSource.getRepository('Horse');
        const horses = await horseRepo.find();
        res.json(horses);
    } catch (error) {
        console.error('Error fetching horses:', error);
        res.status(500).json({ message: 'Internal Server Error' });
    }
};

// Get horse by ID
const getHorseById = async (req, res) => {
    try {
        const horseRepo = AppDataSource.getRepository('Horse');
        const horse = await horseRepo.findOneBy({ id: parseInt(req.params.id) });
        if (!horse) {
            return res.status(404).json({ message: 'Horse not found' });
        }
        res.json(horse);
    } catch (error) {
        console.error('Error fetching horse:', error);
        res.status(500).json({ message: 'Internal Server Error' });
    }
};

module.exports = {
    createHorse,
    getAllHorses,
    getHorseById,
};