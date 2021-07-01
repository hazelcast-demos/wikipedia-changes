package org.hazelcast.wikipedia

import java.util.concurrent.LinkedBlockingQueue
import java.util.function.Consumer

class WikipediaChangeEventHandler : Consumer<String> {

    val queue = LinkedBlockingQueue<String>()

    override fun accept(message: String) {
        queue.add(message)
    }
}
