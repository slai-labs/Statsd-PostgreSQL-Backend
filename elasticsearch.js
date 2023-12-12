/*
THIS IS TEMPORARY UNTIL WE UPDATE OUR WORKER'S EXPORTER
*/


module.exports = (function () {
    let fetch = null
    const env = require("dotenv").config({ path: process.env.CONFIG_PATH || "../statsd-postgres-backend/.env" }).parsed

    const sendBulkMetrics = async function (body) {
        if (!fetch) {
            fetch = await import("node-fetch").then(fetchMethod => fetchMethod.default);
        }

        const response = await fetch(
            `${env.ELASTICSEARCH_ENDPOINT}/${env.ELASTICSEARCH_DATASTREAM_NAME}/_bulk`,
            {
                method: "PUT",
                headers: {
                    "Content-Type": "application/json",
                    "Authorization": `Basic ${Buffer.from(`${env.ELASTICSEARCH_USER}:${env.ELASTICSEARCH_PASS}`).toString("base64")}`
                },
                body,
            }
        )


        if (!response.ok) {
            console.error("Error sending metrics to Elasticsearch: ", response.statusText)
        }
    }

    const indexName = function () {
        const date = new Date()
        const year = date.getUTCFullYear()
        const month = date.getUTCMonth()
        const day = date.getUTCDate()
        
        return `${env.ELASTICSEARCH_DATASTREAM_NAME}-${year}.${(month + 1).toString().padStart(2, "0")}.${day.toString().padStart(2, "0")}`
    }

    const compileMetrics = function (metrics) {
        const metrics_copy = (metrics || []).slice(0);
        // const _index = indexName();
        let insertBuffer = []

        if (metrics_copy.length === 0) {
            return;
        }

        for (const index in metrics_copy) {
            const data = {
                "@timestamp": metrics_copy[index].collected,
                "topic": metrics_copy[index].topic,
                "category": metrics_copy[index].category,
                "subcategory": metrics_copy[index].subcategory,
                "identity": metrics_copy[index].identity,
                "metric": metrics_copy[index].metric,
                "type": metrics_copy[index].type,
                "value": metrics_copy[index].value,
                "tags": metrics_copy[index].tags,
            }

            insertBuffer.push(JSON.stringify({ create: {} }))
            insertBuffer.push(JSON.stringify(data))
        }

        insertBuffer = insertBuffer.join("\n") + "\n"

        return insertBuffer
    }

    const sendMetricsToElasticSearch = async function (metrics) {
        const body = compileMetrics(metrics)
        if (!body) return
        await sendBulkMetrics(body)
    }

    return {
        sendMetricsToElasticSearch,
    }
}())