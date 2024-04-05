// src/index.js

const parseQuery = require('./queryParser');
const readCSV = require('./csvReader');

async function executeSELECTQuery(query) {
    const { fields, table, whereClause } = parseQuery(query);
    const data = await readCSV(`${table}.csv`);
    
    const filteredData = whereClause
        ? data.filter(row => {
            const [field, value] = whereClause.split('=').map(s => s.trim());
            return row[field] === value;
        })
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

module.exports = executeSELECTQuery;