package org.hazelcast.wikipedia

import com.hazelcast.core.Hazelcast
import com.hazelcast.function.FunctionEx
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.org.json.JSONArray
import com.hazelcast.org.json.JSONObject

fun main() {
    val pipeline = Pipeline.create().apply {
        readFrom(wikipedia)
            .withTimestamps({ it.getLong("timestamp") }, 100)
            .map(MakeFieldObjectIfArray("log_params"))
            .peek()
            .writeTo(elasticsearch)
    }
    Hazelcast.bootstrappedInstance().jet.newJob(pipeline)
}

class MakeFieldObjectIfArray(private val fieldName: String) : FunctionEx<JSONObject, JSONObject> {
    override fun applyEx(json: JSONObject) = json.apply {
        if (json.has(fieldName) && json.get(fieldName) is JSONArray)
            put(fieldName, JSONObject())
    }
}
