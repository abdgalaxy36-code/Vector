package org.matrix.vector.manager.data.repository

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONArray
import org.matrix.vector.manager.logW
import org.matrix.vector.ui.store.OnlineModule
import org.matrix.vector.ui.store.Release

/**
 * Best-effort machine translation of Store payloads into English.
 *
 * The catalogue is written by hundreds of third-party authors in whatever language they like; this
 * layer exists so an English UI can show something readable without waiting on per-module work at
 * the UI layer. Every call is fail-open: on any network, parse, or timeout failure the original
 * text is returned unchanged, so a flaky connection degrades into "some rows stay untranslated"
 * and never into a broken Store. Results are cached in-memory for the process lifetime, keyed on
 * the exact source string, so pulling to refresh or revisiting a module reuses prior work.
 */
object RepoTranslate {
    /** Endpoints that need a User-Agent to answer; gtx is the free, key-less Google Translate API. */
    private const val GTX_HOST = "https://translate.googleapis.com/translate_a/single"

    /** Concurrent in-flight translations. High enough to feel fast, low enough not to look like abuse. */
    private const val MAX_PARALLEL = 6

    private val cache = ConcurrentHashMap<String, String>()

    private val client =
        OkHttpClient.Builder()
            .connectTimeout(8, TimeUnit.SECONDS)
            .readTimeout(8, TimeUnit.SECONDS)
            .build()

    /**
     * Cheap filter: a string that is already Latin/Cyrillic-free of CJK, Hangul, Kana, Arabic,
     * Hebrew, Thai, Devanagari etc is overwhelmingly likely to be English or a package name, and
     * sending it wastes a round-trip. Package names and scopes never reach here (they are filtered
     * by callers), but the check also skips already-English summaries for free.
     */
    private val NON_ENGLISH =
        Regex("[\\u0370-\\u1FFF\\u0590-\\u08FF\\u0900-\\u0DFF\\u0E00-\\u0F7F\\u1000-\\u109F\\u1100-\\u11FF\\u3040-\\u30FF\\u3400-\\u4DBF\\u4E00-\\u9FFF\\uAC00-\\uD7AF\\uF900-\\uFAFF\\uFE70-\\uFEFF]")

    fun needsTranslation(s: String?): Boolean =
        !s.isNullOrBlank() && s.length >= 2 && NON_ENGLISH.containsMatchIn(s)

    /** Translates one plain string; returns [s] unchanged on any failure or if already Latin. */
    fun text(s: String?): String? {
        if (s == null) return null
        if (!needsTranslation(s)) return s
        cache[s]?.let {
            return it
        }
        val out =
            runCatching { callGtx(s) }
                .onFailure { e -> logW("translate: failed for \"${s.take(40)}\"", e) }
                .getOrDefault(s)
        if (out != s) cache[s] = out
        return out
    }

    /**
     * Translates the human-readable text nodes of an HTML fragment (README, release notes), leaving
     * tags and pre/code blocks intact. Splits on tags so the translation request never carries
     * markup the engine might mangle; code fences are skipped so snippets stay verbatim.
     */
    fun html(raw: String?): String? {
        if (raw == null) return null
        if (!needsTranslation(raw)) return raw
        cache[raw]?.let {
            return it
        }
        val out =
            runCatching { translateHtmlSegments(raw) }
                .onFailure { e -> logW("translate: html failed", e) }
                .getOrDefault(raw)
        if (out != raw) cache[raw] = out
        return out
    }

    private fun translateHtmlSegments(raw: String): String {
        val sb = StringBuilder(raw.length + 64)
        // Split into tag / text segments; track pre/code depth so their contents pass through.
        var preCodeDepth = 0
        val tagRegex = Regex("<[^>]+>")
        var last = 0
        for (m in tagRegex.findAll(raw)) {
            if (m.range.first > last) {
                val textSeg = raw.substring(last, m.range.first)
                sb.append(if (preCodeDepth > 0) textSeg else (text(textSeg) ?: textSeg))
            }
            val tag = m.value
            sb.append(tag)
            val lower = tag.lowercase()
            if (lower.startsWith("<pre") || lower.startsWith("<code") ||
                lower.startsWith("<script") || lower.startsWith("<style")
            ) {
                if (!lower.startsWith("</")) preCodeDepth++
            }
            if (lower.startsWith("</pre") || lower.startsWith("</code") ||
                lower.startsWith("</script") || lower.startsWith("</style")
            ) {
                if (preCodeDepth > 0) preCodeDepth--
            }
            last = m.range.last + 1
        }
        if (last < raw.length) {
            val textSeg = raw.substring(last)
            sb.append(if (preCodeDepth > 0) textSeg else (text(textSeg) ?: textSeg))
        }
        return sb.toString()
    }

    /**
     * Translates a whole catalogue in the background, in parallel, with a small in-flight cap.
     *
     * Mutates nothing: returns a new list whose entries are copies with only the text fields
     * changed. Runs off the caller's dispatcher so the Store's own refresh is not held up by
     * hundreds of small HTTP calls; the caller decides when to swap the list in.
     */
    suspend fun catalog(modules: List<OnlineModule>): List<OnlineModule> =
        withContext(Dispatchers.IO) {
            val semaphore = Semaphore(MAX_PARALLEL)
            modules
                .map { m ->
                    async {
                        semaphore.withPermit { module(m, alsoReleases = false) }
                    }
                }
                .awaitAll()
        }

    /** Translates one module's own fields, and optionally its releases (the detail payload). */
    suspend fun module(m: OnlineModule, alsoReleases: Boolean): OnlineModule =
        withContext(Dispatchers.IO) {
            val releases =
                if (alsoReleases) m.releases?.map { release(it) } ?: m.releases else m.releases
            m.copy(
                name = text(m.name) ?: m.name,
                description = text(m.description),
                summary = text(m.summary),
                readmeHTML = html(m.readmeHTML),
                releases = releases,
                betaReleases =
                    if (alsoReleases) m.betaReleases?.map { release(it) } ?: m.betaReleases
                    else m.betaReleases,
            )
        }

    private fun release(r: Release): Release =
        r.copy(name = text(r.name), descriptionHTML = html(r.descriptionHTML))

    /** One gtx call. Returns the joined translation, or throws for the caller to swallow. */
    private fun callGtx(s: String): String {
        val url =
            GTX_HOST.toHttpUrl()
                .newBuilder()
                .addQueryParameter("client", "gtx")
                .addQueryParameter("sl", "auto")
                .addQueryParameter("tl", "en")
                .addQueryParameter("dt", "t")
                .addQueryParameter("q", s)
                .build()
        val request = Request.Builder().url(url).get().build()
        client.newCall(request).execute().use { resp ->
            if (!resp.isSuccessful) error("HTTP ${resp.code}")
            val body = resp.body.string()
            // Response is [[["translated","original",...],...],...]; take the first sentence of
            // each chunk and concatenate, which matches how the gtx endpoint splits long input.
            val outer = JSONArray(body)
            val sb = StringBuilder()
            val chunks = outer.getJSONArray(0)
            for (i in 0 until chunks.length()) {
                sb.append(chunks.getJSONArray(i).getString(0))
            }
            return sb.toString().ifBlank { s }
        }
    }
}
