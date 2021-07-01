package org.hazelcast.wikipedia

import com.hazelcast.core.Hazelcast
import com.hazelcast.jet.pipeline.Pipeline
import com.hazelcast.jet.pipeline.Sinks

fun main() {
    val pipeline = Pipeline.create().apply {
        readFrom(wikipedia)
            .withTimestamps({ it.getLong("timestamp") }, 100)
            .writeTo(Sinks.logger())
    }
    Hazelcast.bootstrappedInstance().jet.newJob(pipeline)
}
