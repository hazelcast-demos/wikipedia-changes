package org.hazelcast.wikipedia

import com.github.pemistahl.lingua.api.*
import com.hazelcast.jet.core.ProcessorSupplier
import com.hazelcast.jet.pipeline.ServiceFactories
import com.hazelcast.jet.pipeline.StreamStage
import com.hazelcast.org.json.JSONObject

val languageDetectorSupplier = { _: ProcessorSupplier.Context ->
    LanguageDetectorBuilder
        .fromAllSpokenLanguages()
        .build()
}

val enrichWithLanguage = { stage: StreamStage<JSONObject> ->
    stage.setName("enrich-with-language")
        .mapUsingService(ServiceFactories.sharedService(languageDetectorSupplier)) { detector: LanguageDetector, json: JSONObject ->
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
}
