// src/index.js

const parseQuery = require('./queryParser');
const readCSV = require('./csvReader');

async function executeSELECTQuery(query) {
    const { fields, table, whereClauses } = parseQuery(query);
    const data = await readCSV(`${table}.csv`);
    
    const filteredData = whereClauses.length > 0
        ? data.filter(row => whereClauses.every(clause => {
            // You can expand this to handle different operators
            return row[clause.field] === clause.value;
        }))
        : data;
    // Filter the fields based on the query
    return filteredData.map(row => {
        const filteredRow = {};
        fields.forEach(field => {
            filteredRow[field] = row[field];
        });
        return filteredRow;
    });
}
function parseWhereClause(whereString) {
    const conditions = whereString.split(/ AND | OR /i);
    return conditions.map(condition => {
        const [field, operator, value] = condition.split(/\s+/);
        return { field, operator, value };
    });
}


module.exports = executeSELECTQuery;