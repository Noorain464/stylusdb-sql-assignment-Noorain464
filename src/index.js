const {parseQuery} = require('./queryParser');
const readCSV = require('./csvReader');
async function performInnerJoin(data, joinData, joinCondition, fields, table) {
    if (joinTable && joinCondition) {
        joinData = await readCSV(`${joinTable}.csv`);
        data = data.flatMap(mainRow => {
            return joinData
                .filter(joinRow => {
                    const mainValue = mainRow[joinCondition.left.split('.')[1]];
                    const joinValue = joinRow[joinCondition.right.split('.')[1]];
                    return mainValue === joinValue;
                })
                .map(joinRow => {
                    return fields.reduce((acc, field) => {
                        const [tableName, fieldName] = field.split('.');
                        acc[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                        return acc;
                    }, {});
                });
        });
    }
}

async function performLeftJoin(data, joinData, joinCondition, fields, table) {
    if (joinTable && joinCondition) {
        joinData = await readCSV(`${joinTable}.csv`);
        data = data.flatMap(mainRow => {
            const matchingRows = joinData.filter(joinRow => {
                const mainValue = mainRow[joinCondition.left.split('.')[1]];
                const joinValue = joinRow[joinCondition.right.split('.')[1]];
                return mainValue === joinValue;
            });
    
            if (matchingRows.length === 0) {
                // If no matching row found in joinData, create a new row with null values for joinData fields
                const newRow = {};
                fields.forEach(field => {
                    const [tableName, fieldName] = field.split('.');
                    newRow[field] = tableName === table ? mainRow[fieldName] : null;
                });
                return [newRow];
            }
    
            return matchingRows.map(joinRow => {
                return fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split('.');
                    acc[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                    return acc;
                }, {});
            });
        });
    }
      
}

async function performRightJoin(data, joinData, joinCondition, fields, table) {
    if (joinTable && joinCondition) {
        joinData = await readCSV(`${joinTable}.csv`);
        data = data.flatMap(mainRow => {
            const matchingRows = joinData.filter(joinRow => {
                const mainValue = mainRow[joinCondition.left.split('.')[1]];
                const joinValue = joinRow[joinCondition.right.split('.')[1]];
                return mainValue === joinValue;
            });
    
            if (matchingRows.length === 0) {
                // If no matching row found in joinData, create a new row with null values for joinData fields
                const newRow = {};
                fields.forEach(field => {
                    const [tableName, fieldName] = field.split('.');
                    newRow[field] = tableName === table ? mainRow[fieldName] : null;
                });
                return [newRow];
            }
    
            return matchingRows.map(joinRow => {
                return fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split('.');
                    acc[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                    return acc;
                }, {});
            });
        });
    }
    
    
}
async function executeSELECTQuery(query) {
    const { fields, table, whereClauses, joinType, joinTable, joinCondition } = parseQuery(query);
    let data = await readCSV(`${table}.csv`);

    // Logic for applying JOINs
    if (joinTable && joinCondition) {
        const joinData = await readCSV(`${joinTable}.csv`);
        switch (joinType.toUpperCase()) {
            case 'INNER':
                data = performInnerJoin(data, joinData, joinCondition, fields, table);
                break;
            case 'LEFT':
                data = performLeftJoin(data, joinData, joinCondition, fields, table);
                break;
            case 'RIGHT':
                data = performRightJoin(data, joinData, joinCondition, fields, table);
                break;
            // Handle default case or unsupported JOIN types
        }
    }
    
    const filteredData = whereClauses.length > 0
        ? data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause)))
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
function evaluateCondition(row, clause) {
    const { field, operator, value } = clause;
    switch (operator) {
        case '=': return row[field] === value;
        case '!=': return row[field] !== value;
        case '>': return row[field] > value;
        case '<': return row[field] < value;
        case '>=': return row[field] >= value;
        case '<=': return row[field] <= value;
        default: throw new Error(`Unsupported operator: ${operator}`);
    }
}


module.exports = executeSELECTQuery;