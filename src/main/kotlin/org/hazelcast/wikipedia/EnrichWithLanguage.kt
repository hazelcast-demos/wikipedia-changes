package org.hazelcast.wikipedia

import com.github.pemistahl.lingua.api.LanguageDetector
import com.github.pemistahl.lingua.api.LanguageDetectorBuilder
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
                val comment = json.optString("comment")
                if (comment.isNotEmpty()) {
                    val languagesWithConfidence = detector.computeLanguageConfidenceValues(comment)
                    if (languagesWithConfidence.isNotEmpty()) {
                        val mostLikelyLanguage = languagesWithConfidence.firstKey()
                        val secondMostLikelyConfidence = languagesWithConfidence.filterNot { it.key == mostLikelyLanguage }.maxBy { it.value }?.value ?: 0.0
                        json.put(
                            "language", JSONObject()
                                .put("code2", mostLikelyLanguage.isoCode639_1)
                                .put("code3", mostLikelyLanguage.isoCode639_3)
                                .put("name", mostLikelyLanguage.name)
                                .put("confidence", 1.0 - secondMostLikelyConfidence)
                        )
                    }
                }
            }
        }
}
