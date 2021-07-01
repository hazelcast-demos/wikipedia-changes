package org.hazelcast.wikipedia

import com.hazelcast.jet.elastic.ElasticClients
import com.hazelcast.jet.elastic.ElasticSinks
import com.hazelcast.org.json.JSONObject
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.xcontent.XContentType


private val clientBuilder = {
    val env = System.getenv()
    val user = env.getOrDefault("ELASTICSEARCH_USERNAME", "elastic")
    val password = env.getOrDefault("ELASTICSEARCH_PASSWORD", "changeme")
    val host = env.getOrDefault("ELASTICSEARCH_HOST", "localhost")
    val port = env.getOrDefault("ELASTICSEARCH_PORT", "9200").toInt()
    ElasticClients.client(user, password, host, port)
}

val elasticsearch = ElasticSinks.elastic<JSONObject>(clientBuilder) {
    IndexRequest("wikipedia").source(it.toString(), XContentType.JSON)
}