package org.hazelcast.wikipedia

import com.hazelcast.jet.core.Processor
import com.hazelcast.jet.pipeline.SourceBuilder
import com.hazelcast.org.json.JSONObject
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.Disposable


private val context = { _: Processor.Context ->
    val eventStream = WebClient
        .create("https://stream.wikimedia.org/v2/stream/recentchange")
        .get()
        .retrieve()
        .bodyToFlux(String::class.java)
    val eventHandler = WikipediaChangeEventHandler()
    val disposable = eventStream.subscribe(eventHandler)
    eventHandler to disposable
}

private val call = { context: Pair<WikipediaChangeEventHandler, Disposable>,
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
    .destroyFn { it.second.dispose() }
    .build()
