const { EntitySchema } = require("typeorm");

module.exports = new EntitySchema({
    name: "Horse",
    tableName: "horses",
    columns: {
        id: {
            primary: true,
            type: "int",
            generated: true,
        },
        name: {
            type: "varchar",
        },
        gender: {
            type: "varchar",
        },
        birth_year: {
            type: "int",
        },
        sire_name: {
            type: "varchar",
        },
        dam_name: {
            type: "varchar",
        },
    },
});