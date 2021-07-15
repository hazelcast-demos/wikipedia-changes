package org.hazelcast.wikipedia

import com.hazelcast.core.Hazelcast
import com.hazelcast.function.FunctionEx
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.pipeline.ServiceFactories
import com.hazelcast.org.json.JSONArray
import com.hazelcast.org.json.JSONObject

fun main() {
    val pipeline = Pipeline.create().apply {
        readFrom(wikipedia)
            .withTimestamps({ it.getLong("timestamp") }, 100)
            .map(RemoveFieldIfArray("log_params"))
            .mapUsingService(
                ServiceFactories.sharedService(databaseReaderSupplier),
                enrichWithLocation
            ).peek()
            .writeTo(elasticsearch)
    }
    Hazelcast.bootstrappedInstance().jet.newJob(pipeline)
}

class RemoveFieldIfArray(private val fieldName: String) : FunctionEx<JSONObject, JSONObject> {
    override fun applyEx(json: JSONObject) = json.apply {
        if (json.has(fieldName) && json.get(fieldName) is JSONArray)
            remove(fieldName)
    }
}

