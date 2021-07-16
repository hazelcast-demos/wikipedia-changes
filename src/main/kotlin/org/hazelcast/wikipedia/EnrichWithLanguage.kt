package org.hazelcast.wikipedia

import com.github.pemistahl.lingua.api.*
import com.hazelcast.jet.core.ProcessorSupplier
import com.hazelcast.org.json.JSONObject

val languageDetectorSupplier = { _: ProcessorSupplier.Context ->
    LanguageDetectorBuilder
        .fromAllSpokenLanguages()
        .build()
}

val enrichWithLanguage = { detector: LanguageDetector, json: JSONObject ->
    json.apply {
        val comment = json.optString("parsedcomment")
        if (comment.isNotEmpty()) {
            val language = detector.detectLanguageOf(comment)
            if (language != Language.UNKNOWN) {
                json.put(
                    "language", JSONObject()
                        .put("code2", language.isoCode639_1)
                        .put("code3", language.isoCode639_3)
                        .put("name", language.name)
                )
            }
        }
    }
}
