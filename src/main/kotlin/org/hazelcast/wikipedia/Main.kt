package org.hazelcast.wikipedia

import com.hazelcast.core.Hazelcast
import com.hazelcast.function.FunctionEx
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.pipeline.StreamStage
import com.hazelcast.org.json.JSONArray
import com.hazelcast.org.json.JSONObject

fun main() {
    val pipeline = Pipeline.create().apply {
        readFrom(wikipedia)
            .withTimestamps({ it.getLong("timestamp") }, 100)
            .apply(RemoveFieldIfArray("log_params"))
            .peek()
            .writeTo(elasticsearch)
    }
    Hazelcast.bootstrappedInstance().jet.newJob(pipeline)
}

class RemoveFieldIfArray(private val fieldName: String) : FunctionEx<StreamStage<JSONObject>, StreamStage<JSONObject>> {
    override fun applyEx(stage: StreamStage<JSONObject>) = stage
        .setName("remove-log-params-if-array")
        .map { json ->
            json.apply {
                if (json.has(fieldName) && json.get(fieldName) is JSONArray)
                    remove(fieldName)
            }
        }
}
