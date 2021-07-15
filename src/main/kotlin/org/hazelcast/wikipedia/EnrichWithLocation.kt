package org.hazelcast.wikipedia

import com.hazelcast.function.BiFunctionEx
import com.hazelcast.jet.core.ProcessorSupplier
import com.hazelcast.org.json.JSONArray
import com.hazelcast.org.json.JSONObject
import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.model.CityResponse
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
                    .ifPresent { json.withLocationFrom(it) }
            }
        }
    }
}

private fun JSONObject.withLocationFrom(response: CityResponse) {
    val country = JSONObject()
        .put("iso", response.country.isoCode)
        .put("name", response.country.name)
    val coordinates = JSONArray()
        .put(response.location.latitude)
        .put(response.location.longitude)
    val location = JSONObject()
        .put("country", country)
        .put("coordinates", coordinates)
        .put("city", response.city.name)
        .put("timezone", response.location.timeZone)
        .put("accuracy-radius", response.location.accuracyRadius)
    put("location", location)
}
