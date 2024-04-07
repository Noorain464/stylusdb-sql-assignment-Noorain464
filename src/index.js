const {parseQuery} = require('./queryParser');
const readCSV = require('./csvReader');
async function performInnerJoin(data, joinData, joinCondition, fields, table) {
    // Logic for INNER JOIN
    // ...
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
    if (joinData && joinCondition) {
        const joinTableData = await readCSV(`${joinData}.csv`);
        return data.map(mainRow => {
            const matchingJoinRows = joinTableData.filter(joinRow => mainRow[joinCondition.left.split('.')[1]] === joinRow[joinCondition.right.split('.')[1]]);
            if (matchingJoinRows.length > 0) {
                return matchingJoinRows.map(joinRow => {
                    const newRow = {};
                    fields.forEach(field => {
                        const [tableName, fieldName] = field.split('.');
                        newRow[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                    });
                    return newRow;
                });
            } else {
                const newRow = {};
                fields.forEach(field => {
                    const [tableName, fieldName] = field.split('.');
                    newRow[field] = tableName === table ? mainRow[fieldName] : null;
                });
                return newRow;
            }
        }).flat();
    }
    return data;
}

async function performRightJoin(data, joinData, joinCondition, fields, table) {
    if (joinData && joinCondition) {
        const joinTableData = await readCSV(`${joinData}.csv`);
        return joinTableData.map(joinRow => {
            const matchingMainRows = data.filter(mainRow => mainRow[joinCondition.left.split('.')[1]] === joinRow[joinCondition.right.split('.')[1]]);
            if (matchingMainRows.length > 0) {
                return matchingMainRows.map(mainRow => {
                    const newRow = {};
                    fields.forEach(field => {
                        const [tableName, fieldName] = field.split('.');
                        newRow[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                    });
                    return newRow;
                });
            } else {
                const newRow = {};
                fields.forEach(field => {
                    const [tableName, fieldName] = field.split('.');
                    newRow[field] = tableName === table ? null : joinRow[fieldName];
                });
                return newRow;
            }
        }).flat();
    }
    return data;
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
            default:
                throw new Error('Unsupported JOIN type.');
        }
    }
    // Apply WHERE clause filtering after JOIN (or on the original data if no join)
    const filteredData = whereClauses.length > 0
        ? data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause)))
        : data;

    // Select the specified fields
    return filteredData.map(row => {
        const selectedRow = {};
        fields.forEach(field => {
            // Assuming 'field' is just the column name without table prefix
            selectedRow[field] = row[field];
        });
        return selectedRow;
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