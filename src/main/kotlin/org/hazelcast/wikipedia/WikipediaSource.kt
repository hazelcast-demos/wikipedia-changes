package org.hazelcast.wikipedia

import com.hazelcast.jet.core.Processor
import com.hazelcast.jet.pipeline.SourceBuilder
import com.hazelcast.org.json.JSONObject
import com.launchdarkly.eventsource.EventSource
import okhttp3.HttpUrl.Companion.toHttpUrl


private val context = { _: Processor.Context ->
    val eventHandler = WikipediaChangeEventHandler()
    val url = "https://stream.wikimedia.org/v2/stream/recentchange".toHttpUrl()
    val eventSource = EventSource.Builder(eventHandler, url)
        .build()
        .apply { start() }
    eventHandler to eventSource
}

private val call = { context: Pair<WikipediaChangeEventHandler, EventSource>,
                     buffer: SourceBuilder.SourceBuffer<JSONObject> ->
    with(context.first.queue) {
        var i = 0
        while (isNotEmpty() && i < 100) {
            buffer.add(JSONObject(remove()))
            i++
        }
    }
}

val wikipedia = SourceBuilder
    .stream("wikipedia", context)
    .fillBufferFn(call)
    .destroyFn { it.second.close() }
    .build()
