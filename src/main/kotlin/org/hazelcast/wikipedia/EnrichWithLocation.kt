package org.hazelcast.wikipedia

import com.hazelcast.function.BiFunctionEx
import com.hazelcast.jet.core.ProcessorSupplier
import com.hazelcast.org.json.JSONObject
import com.maxmind.geoip2.DatabaseReader
import org.apache.commons.validator.routines.InetAddressValidator
import java.net.InetAddress

val databaseReaderSupplier = { _: ProcessorSupplier.Context ->
    val database = WikipediaChangeEventHandler::class.java.classLoader.getResourceAsStream("data/GeoLite2-City.mmdb")
    DatabaseReader.Builder(database).build()
}

val enrichWithLocation = object : BiFunctionEx<DatabaseReader, JSONObject, JSONObject> {
    private val validator = InetAddressValidator.getInstance()
    override fun applyEx(reader: DatabaseReader, json: JSONObject) = json.apply {
        if (!json.optBoolean("bot") && json.has("user")) {
            val user = json.getString("user")
            if (validator.isValid(user)) {
                reader.tryCity(InetAddress.getByName(user))
                    .ifPresent { json.put("location", JSONObject(it.toJson())) }
            }
        }
    }
}