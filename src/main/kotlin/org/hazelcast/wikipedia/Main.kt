package org.hazelcast.wikipedia

import com.hazelcast.core.Hazelcast
import com.hazelcast.jet.pipeline.Pipeline

fun main() {
    val pipeline = Pipeline.create().apply {
        readFrom(wikipedia)
            .withTimestamps({ it.getLong("timestamp") }, 100)
            .writeTo(elasticsearch)
    }
    Hazelcast.bootstrappedInstance().jet.newJob(pipeline)
}
