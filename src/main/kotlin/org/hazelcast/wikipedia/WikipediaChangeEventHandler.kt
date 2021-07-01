package org.hazelcast.wikipedia

import com.launchdarkly.eventsource.EventHandler
import com.launchdarkly.eventsource.MessageEvent
import java.util.concurrent.LinkedBlockingQueue

class WikipediaChangeEventHandler : EventHandler {

    val queue = LinkedBlockingQueue<String>()

    override fun onMessage(event: String, message: MessageEvent) {
        queue.add(message.data)
    }

    override fun onError(t: Throwable) {
        t.printStackTrace()
    }

    override fun onOpen() = Unit

    override fun onClosed() = Unit

    override fun onComment(comment: String) = Unit
}
