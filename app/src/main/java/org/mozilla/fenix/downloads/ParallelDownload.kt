/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.mozilla.fenix.downloads

import java.io.IOException
import java.io.InputStream
import java.util.concurrent.Callable
import java.util.concurrent.CancellationException
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import mozilla.components.concept.fetch.Client
import mozilla.components.concept.fetch.Headers
import mozilla.components.concept.fetch.MutableHeaders
import mozilla.components.concept.fetch.Request
import mozilla.components.concept.fetch.Response

/**
 * Downloads large, stable HTTP resources with four concurrent requests while exposing an ordered stream to the existing
 * download service. Small files, encoded bodies, and servers without byte ranges or a strong ETag retain the original
 * response. Resumes retain the service's existing Range handling.
 */
internal class ParallelDownload(private val client: Client) {
    fun prepare(request: Request, response: Response): Response {
        val length = response.headers[Headers.Names.CONTENT_LENGTH]?.toLongOrNull() ?: return response
        val etag = response.headers[Headers.Names.E_TAG] ?: return response
        if (
            request.method != Request.Method.GET ||
                request.body != null ||
                request.headers?.contains(Headers.Names.RANGE) == true ||
                !(request.url.startsWith("https://") || request.url.startsWith("http://")) ||
                response.status != Response.SUCCESS ||
                length < MINIMUM_SIZE ||
                !etag.startsWith('"') ||
                !etag.endsWith('"') ||
                !response.headers["Accept-Ranges"].equals("bytes", ignoreCase = true) ||
                !response.hasIdentityEncoding()
        ) {
            return response
        }

        // Keep the original body untouched until an actual range request has succeeded. Some
        // servers advertise byte ranges but ignore Range, reject extra requests, or return HTML.
        val first =
            try {
                readRange(request, 0, CHUNK_SIZE - 1L, length, etag, response.url)
            } catch (_: IOException) {
                return response
            }
        response.close()
        return response.copy(
            body =
                Response.Body(
                    RangeInputStream(request, length, etag, response.url, first),
                    response.headers[Headers.Names.CONTENT_TYPE],
                )
        )
    }

    private fun readRange(
        request: Request,
        start: Long,
        end: Long,
        total: Long,
        etag: String,
        expectedUrl: String,
        register: (Response) -> Unit = {},
        unregister: (Response) -> Unit = {},
    ): ByteArray {
        val headers =
            MutableHeaders(request.headers?.toList().orEmpty()).apply {
                set(Headers.Names.RANGE, "bytes=$start-$end")
                set("If-Range", etag)
                set("Accept-Encoding", "identity")
            }
        val rangeRequest =
            request
                .copy(
                    headers = headers,
                    useCaches = false,
                    connectTimeout = request.connectTimeout ?: (15L to TimeUnit.SECONDS),
                    readTimeout = request.readTimeout ?: (30L to TimeUnit.SECONDS),
                )
                .also {
                    // These fields are outside Request's primary constructor and copy() omits them.
                    it.referrerUrl = request.referrerUrl
                    it.conservative = request.conservative
                }
        client.fetch(rangeRequest).use { range ->
            register(range)
            try {
                if (
                    range.status != PARTIAL_CONTENT ||
                        range.url != expectedUrl ||
                        range.headers[Headers.Names.CONTENT_RANGE] != "bytes $start-$end/$total" ||
                        range.headers[Headers.Names.E_TAG] != etag ||
                        !range.hasIdentityEncoding()
                ) {
                    throw IOException("Server did not return the requested download range")
                }
                return range.body.useStream { input ->
                    val bytes = ByteArray((end - start + 1).toInt())
                    var offset = 0
                    while (offset < bytes.size) {
                        if (Thread.currentThread().isInterrupted) throw IOException("Download cancelled")
                        val count = input.read(bytes, offset, bytes.size - offset)
                        if (count < 0) throw IOException("Incomplete download range")
                        if (count == 0) continue
                        offset += count
                    }
                    if (input.read() != -1) throw IOException("Download range exceeds requested size")
                    bytes
                }
            } finally {
                unregister(range)
            }
        }
    }

    private inner class RangeInputStream(
        private val request: Request,
        private val length: Long,
        private val etag: String,
        private val expectedUrl: String,
        first: ByteArray,
    ) : InputStream() {
        private val executor =
            Executors.newFixedThreadPool(CONNECTIONS) { task ->
                Thread(task, "iceraven-download-range").apply { isDaemon = true }
            }
        private val pending = ConcurrentLinkedQueue<Future<ByteArray>>()
        private val active = mutableSetOf<Response>()
        @Volatile private var awaiting: Future<ByteArray>? = null
        private var chunk = first
        private var offset = 0
        private var nextStart = first.size.toLong()
        @Volatile private var closed = false

        init {
            repeat(CONNECTIONS) { enqueue() }
        }

        private fun enqueue() {
            synchronized(active) {
                if (nextStart >= length || closed) return
                val start = nextStart
                val end = start + minOf(CHUNK_SIZE.toLong(), length - start) - 1
                nextStart = end + 1
                pending.add(
                    executor.submit(
                        Callable {
                            readRange(request, start, end, length, etag, expectedUrl, ::register, ::unregister)
                        }
                    )
                )
            }
        }

        private fun register(response: Response) {
            synchronized(active) {
                if (closed) {
                    response.close()
                    throw IOException("Download cancelled")
                }
                active.add(response)
            }
        }

        private fun unregister(response: Response) {
            synchronized(active) { active.remove(response) }
        }

        override fun read(): Int {
            val byte = ByteArray(1)
            return if (read(byte, 0, 1) == -1) -1 else byte[0].toInt() and 0xff
        }

        override fun read(bytes: ByteArray, start: Int, count: Int): Int {
            if (start < 0 || count < 0 || start > bytes.size - count) throw IndexOutOfBoundsException()
            if (closed) throw IOException("Download stream closed")
            if (count == 0) return 0
            if (offset == chunk.size) {
                val next = pending.poll()
                if (next == null) {
                    executor.shutdown()
                    return -1
                }
                awaiting = next
                try {
                    if (closed) throw IOException("Download stream closed")
                    chunk = next.get()
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    close()
                    throw IOException("Download interrupted", e)
                } catch (e: ExecutionException) {
                    close()
                    throw IOException("Could not download range", e.cause)
                } catch (e: CancellationException) {
                    close()
                    throw IOException("Download cancelled", e)
                } finally {
                    awaiting = null
                }
                offset = 0
                enqueue()
            }
            val copied = minOf(count, chunk.size - offset)
            chunk.copyInto(bytes, start, offset, offset + copied)
            offset += copied
            return copied
        }

        override fun close() {
            synchronized(active) {
                if (closed) return
                closed = true
            }
            awaiting?.cancel(true)
            pending.forEach { it.cancel(true) }
            pending.clear()
            executor.shutdownNow()
            val responses = synchronized(active) { active.toList().also { active.clear() } }
            responses.forEach { it.close() }
        }
    }

    private fun Response.hasIdentityEncoding(): Boolean {
        val encoding = headers["Content-Encoding"]
        return encoding == null || encoding.equals("identity", ignoreCase = true)
    }

    private companion object {
        const val CONNECTIONS = 4
        const val CHUNK_SIZE = 1024 * 1024
        const val MINIMUM_SIZE = 8L * CHUNK_SIZE
        const val PARTIAL_CONTENT = 206
    }
}
