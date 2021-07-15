package org.hazelcast.wikipedia

import com.hazelcast.jet.core.ProcessorSupplier
import com.hazelcast.jet.pipeline.ServiceFactories
import com.hazelcast.jet.pipeline.StreamStage
import com.hazelcast.org.json.JSONObject
import com.maxmind.geoip2.DatabaseReader
import org.apache.commons.validator.routines.InetAddressValidator
import java.net.InetAddress

val enrichWithLocation = { stage: StreamStage<JSONObject> ->
    stage.setName("enrich-with-location")
        .mapUsingService(ServiceFactories.sharedService(databaseReaderSupplier)) { reader: DatabaseReader, json: JSONObject ->
            json.apply {
                if (!json.optBoolean("bot") && json.has("user")) {
                    val user = json.getString("user")
                    if (validator.isValid(user)) {
                        reader.tryCity(InetAddress.getByName(user))
                            .ifPresent { json.put("location", JSONObject(it.toJson())) }
                    }
                }
            }
        }
}

private val databaseReaderSupplier = { _: ProcessorSupplier.Context ->
    val database = WikipediaChangeEventHandler::class.java.classLoader.getResourceAsStream("data/GeoLite2-City.mmdb")
    DatabaseReader.Builder(database).build()
}

private val validator = InetAddressValidator.getInstance()
