const { EntitySchema } = require("typeorm");

// module.exports = new EntitySchema({
//     name: "Horse",
//     tableName: "horses",
//     columns: {
//         id: {
//             primary: true,
//             type: "int",
//             generated: true,
//         },
//         name: {
//             type: "varchar",
//         },
//         gender: {
//             type: "varchar",
//         },
//         birth_year: {
//             type: "int",
//         },
//         sire_name: {
//             type: "varchar",
//         },
//         dam_name: {
//             type: "varchar",
//         },
//     },
// });

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
        is_champion: {
            type: "boolean",
            default: false,
        },
        inbreeding_score: {
            type: "float",
            nullable: true,
        },
        defect_risk_score: {
            type: "float",
            nullable: true,
        },
    },
});