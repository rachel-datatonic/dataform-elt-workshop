/* Generic functions used in the codebase

These functions have been created as they tend to be a 
common requirement across Dataform projects.

These are quite flexible depending on bespoke usecases.
*/


/**
 * Generate a surrogate key by using a Farm fingerprint hashing
 * function over a concatentated list of fields
 * @param [table_name] Name of the table / CTE
 * @param [fields] List of fields
 * @param [surrogate_key_name] Name of surrogate key
 */
function generateSurrogateKey(table_name, fields, surrogate_key_name) {
    const ifNullFields = fields.map((field) => {
        return (field !== "calendar_date") ? `IFNULL(CAST(${field} AS STRING), 'NULL')` : `${field}`;
    }).join(", ', ', ");

    return `
        ${table_name}_generate_id AS (
            SELECT
                CAST(
                    FARM_FINGERPRINT(
                        CONCAT(${ifNullFields})
                    )
                AS STRING) AS ${surrogate_key_name},
                *
            FROM ${table_name}
        )
    `;
}

/**
 * Union tables together
 * @param [tables] List of tables/CTEs
 */
function unionTables(tables) {
    return tables.map(t => `SELECT * FROM ${t}`).join(" UNION ALL ");
}

/**
 * Add an insertion timestamp field
 * @param [table] Name of table
 */
const addInsertionTimestamp = (table) => `SELECT *, CURRENT_TIMESTAMP() AS insertion_timestamp FROM ${table}`;

/**
 * Generate schema based on layer and environment value
 * Useful when promoting between dev and prod environments
 * @param [layer] Name of layer. Eg."staging"
 */
const generateSchema = (layer) => `${layer}_${dataform.projectConfig.vars.env}`

module.exports = {
    generateSurrogateKey,
    addInsertionTimestamp,
    unionTables,
    generateSchema
};
