/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package org.mozilla.fenix.downloads

import com.sun.net.httpserver.HttpServer
import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.InputStream
import java.net.HttpURLConnection
import java.net.InetSocketAddress
import java.net.URI
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import mozilla.components.concept.fetch.Client
import mozilla.components.concept.fetch.Header
import mozilla.components.concept.fetch.Headers
import mozilla.components.concept.fetch.MutableHeaders
import mozilla.components.concept.fetch.Request
import mozilla.components.concept.fetch.Response
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotSame
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test

class ParallelDownloadTest {
    private val data = ByteArray(8 * 1024 * 1024 + 71) { (it % 251).toByte() }
    private val request =
        Request("https://example.com/file", private = true).apply {
            referrerUrl = "https://example.com/page"
            conservative = true
        }

    @Test
    fun `downloads ranges concurrently and assembles exact bytes in order`() {
        val entered = CountDownLatch(4)
        val active = AtomicInteger()
        val maximum = AtomicInteger()
        val client = RangeClient { rangeRequest, response ->
            val start = rangeRequest.headers!![Headers.Names.RANGE]!!.substringAfter('=').substringBefore('-').toLong()
            if (start in CHUNK.toLong()..(4L * CHUNK)) {
                val connections = active.incrementAndGet()
                maximum.accumulateAndGet(connections, ::maxOf)
                entered.countDown()
                assertTrue(
                    "Four independent range requests should overlap",
                    entered.await(5, TimeUnit.SECONDS),
                )
                active.decrementAndGet()
            }
            response
        }
        val original = originalResponse()
        val prepared = ParallelDownload(client).prepare(request, original)
        assertNotSame(original, prepared)
        assertArrayEquals(data, prepared.body.useStream { it.readBytes() })
        assertEquals(4, maximum.get())
        assertEquals(9, client.requests.size)
        client.requests.forEach {
            assertEquals(ETAG, it.headers!!["If-Range"])
            assertEquals("identity", it.headers["Accept-Encoding"])
            assertTrue(it.private)
            assertEquals(request.referrerUrl, it.referrerUrl)
            assertTrue(it.conservative)
            assertEquals(request.url, it.url)
            assertFalse(it.useCaches)
        }
    }

    @Test
    fun `downloads from an HTTP range server using the original browser response`() {
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        val executor = Executors.newFixedThreadPool(6)
        val concurrent = CountDownLatch(4)
        server.executor = executor
        server.createContext("/download") { exchange ->
            val range = exchange.requestHeaders.getFirst("Range")
            val start = range?.substringAfter('=')?.substringBefore('-')?.toInt() ?: 0
            val end = range?.substringAfter('-')?.toInt() ?: data.lastIndex
            exchange.responseHeaders.set("ETag", ETAG)
            exchange.responseHeaders.set("Accept-Ranges", "bytes")
            if (range != null) {
                assertEquals(ETAG, exchange.requestHeaders.getFirst("If-Range"))
                exchange.responseHeaders.set("Content-Range", "bytes $start-$end/${data.size}")
                if (start in CHUNK..(4 * CHUNK)) {
                    concurrent.countDown()
                    assertTrue(concurrent.await(5, TimeUnit.SECONDS))
                }
            }
            try {
                exchange.sendResponseHeaders(
                    if (range == null) 200 else 206,
                    (end - start + 1).toLong(),
                )
                exchange.responseBody.use { it.write(data, start, end - start + 1) }
            } catch (_: IOException) {
                // The initial full response is deliberately closed after the range probe succeeds.
            } finally {
                exchange.close()
            }
        }
        val client =
            object : Client() {
                override fun fetch(request: Request): Response {
                    val connection = URI.create(request.url).toURL().openConnection() as HttpURLConnection
                    request.headers?.forEach { connection.setRequestProperty(it.name, it.value) }
                    connection.connectTimeout = 5000
                    connection.readTimeout = 5000
                    val headers =
                        MutableHeaders(
                            connection.headerFields.flatMap { (name, values) ->
                                if (name == null) emptyList() else values.map { Header(name, it) }
                            }
                        )
                    return Response(
                        request.url,
                        connection.responseCode,
                        headers,
                        object : Response.Body(connection.inputStream) {
                            override fun close() {
                                super.close()
                                connection.disconnect()
                            }
                        },
                    )
                }
            }
        server.start()
        try {
            val request = Request("http://127.0.0.1:${server.address.port}/download")
            val original = client.fetch(request)
            val prepared = ParallelDownload(client).prepare(request, original)
            assertNotSame(original, prepared)
            assertArrayEquals(data, prepared.body.useStream { it.readBytes() })
            assertEquals(0, concurrent.count)
        } finally {
            server.stop(0)
            executor.shutdownNow()
        }
    }

    @Test
    fun `small unknown encoded and unvalidated files keep the original stream`() {
        val client = RangeClient()
        val download = ParallelDownload(client)
        val responses =
            listOf(
                originalResponse(length = CHUNK.toString()),
                originalResponse(length = "invalid"),
                originalResponse(length = null),
                originalResponse(etag = null),
                originalResponse(etag = "W/$ETAG"),
                originalResponse(ranges = "none"),
                originalResponse(ranges = null),
                originalResponse(encoding = "gzip"),
                originalResponse(status = 404),
            )
        responses.forEach { response ->
            assertSame(response, download.prepare(request, response))
            response.close()
        }
        assertTrue(client.requests.isEmpty())
    }

    @Test
    fun `resumes posts and non HTTP downloads keep the original stream`() {
        val client = RangeClient()
        val download = ParallelDownload(client)
        listOf(
                request.copy(headers = MutableHeaders(Headers.Names.RANGE to "bytes=1024-")),
                request.copy(method = Request.Method.POST),
                request.copy(body = Request.Body.fromString("body")),
                request.copy(url = "blob:https://example.com/file"),
            )
            .forEach { current ->
                originalResponse().use { assertSame(it, download.prepare(current, it)) }
            }
        assertTrue(client.requests.isEmpty())
    }

    @Test
    fun `a server ignoring Range falls back without consuming or closing the original body`() {
        var originalClosed = false
        val original =
            originalResponse(
                body =
                    object : ByteArrayInputStream(data) {
                        override fun close() {
                            originalClosed = true
                        }
                    }
            )
        val client = RangeClient { _, response -> response.copy(status = 200) }
        val prepared = ParallelDownload(client).prepare(request, original)
        assertSame(original, prepared)
        assertFalse(originalClosed)
        assertArrayEquals(data, prepared.body.useStream { it.readBytes() })
        assertTrue(originalClosed)
        assertEquals(1, client.requests.size)
    }

    @Test
    fun `a failed probe falls back to the existing response`() {
        val client =
            object : Client() {
                override fun fetch(request: Request): Response = throw IOException("offline")
            }
        originalResponse().use { original ->
            assertSame(original, ParallelDownload(client).prepare(request, original))
            assertArrayEquals(data, original.body.useStream { it.readBytes() })
        }
    }

    @Test
    fun `a redirect to a different resource cannot reuse the initial resources ETag`() {
        val client = RangeClient { _, response -> response.copy(url = "https://example.com/other") }
        originalResponse().use { original ->
            assertSame(original, ParallelDownload(client).prepare(request, original))
            assertArrayEquals(data, original.body.useStream { it.readBytes() })
        }
    }

    @Test
    fun `incorrect range changed ETag and encoded probes are rejected`() {
        listOf(
                MutableHeaders(
                    Headers.Names.CONTENT_RANGE to "bytes 1-$CHUNK/${data.size}",
                    Headers.Names.E_TAG to ETAG,
                ),
                MutableHeaders(
                    Headers.Names.CONTENT_RANGE to "bytes 0-${CHUNK - 1}/${data.size}",
                    Headers.Names.E_TAG to "\"changed\"",
                ),
                MutableHeaders(
                    Headers.Names.CONTENT_RANGE to "bytes 0-${CHUNK - 1}/${data.size}",
                    Headers.Names.E_TAG to ETAG,
                    "Content-Encoding" to "gzip",
                ),
            )
            .forEach { headers ->
                val client = RangeClient { _, response -> response.copy(headers = headers) }
                originalResponse().use { original ->
                    assertSame(original, ParallelDownload(client).prepare(request, original))
                }
            }
    }

    @Test
    fun `short and oversized probe bodies fall back instead of producing corrupt files`() {
        listOf(CHUNK - 1, CHUNK + 1).forEach { size ->
            val client = RangeClient { _, response ->
                response.copy(body = Response.Body(ByteArrayInputStream(ByteArray(size))))
            }
            originalResponse().use { original ->
                assertSame(original, ParallelDownload(client).prepare(request, original))
            }
        }
    }

    @Test
    fun `a resource changing after the first chunk fails without returning mismatched bytes`() {
        val client = RangeClient { rangeRequest, response ->
            if (rangeRequest.headers!![Headers.Names.RANGE] == "bytes=0-${CHUNK - 1}") response
            else response.copy(status = 200)
        }
        ParallelDownload(client).prepare(request, originalResponse()).body.useStream { stream ->
            val first = ByteArray(CHUNK)
            assertEquals(CHUNK, stream.read(first))
            assertArrayEquals(data.copyOf(CHUNK), first)
            try {
                stream.read()
                fail("A changed resource must not be appended to the file")
            } catch (_: IOException) {
                /* Expected: the existing service marks the download failed. */
            }
        }
    }

    @Test
    fun `closing the body releases all active range responses`() {
        val started = CountDownLatch(4)
        val closed = CountDownLatch(4)
        val client = RangeClient { rangeRequest, response ->
            if (rangeRequest.headers!![Headers.Names.RANGE] == "bytes=0-${CHUNK - 1}") response
            else
                response.copy(
                    body =
                        Response.Body(
                            object : InputStream() {
                                override fun read(): Int {
                                    started.countDown()
                                    try {
                                        assertTrue(closed.await(5, TimeUnit.SECONDS))
                                    } catch (_: InterruptedException) {
                                        Thread.currentThread().interrupt()
                                    }
                                    throw IOException("closed")
                                }

                                override fun close() {
                                    closed.countDown()
                                }
                            }
                        )
                )
        }
        val response = ParallelDownload(client).prepare(request, originalResponse())
        assertTrue(started.await(5, TimeUnit.SECONDS))
        response.close()
        assertTrue(closed.await(5, TimeUnit.SECONDS))
    }

    @Test
    fun `stream respects zero length reads bounds and end of file`() {
        ParallelDownload(RangeClient()).prepare(request, originalResponse()).body.useStream { stream ->
            assertEquals(0, stream.read(ByteArray(0)))
            try {
                stream.read(ByteArray(1), 1, 1)
                fail("Invalid read bounds")
            } catch (_: IndexOutOfBoundsException) {
                /* InputStream contract. */
            }
            assertArrayEquals(data, stream.readBytes())
            assertEquals(-1, stream.read())
            assertEquals(0, stream.read(ByteArray(0)))
        }
    }

    private fun originalResponse(
        length: String? = data.size.toString(),
        etag: String? = ETAG,
        ranges: String? = "bytes",
        encoding: String? = null,
        status: Int = 200,
        body: InputStream = ByteArrayInputStream(data),
    ): Response {
        val headers = MutableHeaders()
        length?.let { headers.set(Headers.Names.CONTENT_LENGTH, it) }
        etag?.let { headers.set(Headers.Names.E_TAG, it) }
        ranges?.let { headers.set("Accept-Ranges", it) }
        encoding?.let { headers.set("Content-Encoding", it) }
        return Response(request.url, status, headers, Response.Body(body))
    }

    private inner class RangeClient(
        private val transform: (Request, Response) -> Response = { _, response -> response }
    ) : Client() {
        val requests: MutableList<Request> = Collections.synchronizedList(mutableListOf())

        override fun fetch(request: Request): Response {
            requests.add(request)
            val range = request.headers!![Headers.Names.RANGE]!!.substringAfter('=').split('-')
            val start = range[0].toInt()
            val end = range[1].toInt()
            return transform(
                request,
                Response(
                    request.url,
                    206,
                    MutableHeaders(
                        Headers.Names.CONTENT_RANGE to "bytes $start-$end/${data.size}",
                        Headers.Names.E_TAG to ETAG,
                    ),
                    Response.Body(ByteArrayInputStream(data.copyOfRange(start, end + 1))),
                ),
            )
        }
    }

    private companion object {
        const val ETAG = "\"stable-file\""
        const val CHUNK = 1024 * 1024
    }
}
