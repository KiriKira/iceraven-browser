# Parallel downloads

Iceraven automatically uses up to four concurrent HTTP requests for files of at least 8 MiB
when the server advertises byte ranges and supplies a strong ETag. Each request transfers a 1 MiB
chunk. The downloader buffers only a bounded number of chunks in memory and delivers them in file
order to the existing download service. No additional executable or storage permission is needed.

Every chunk uses `Range`, `If-Range`, and identity encoding. Its status, final URL, content range, ETag, and
actual byte count must match before its bytes enter the output file. The initial range probe leaves
the original response available for fallback. Small files, unknown lengths, compressed responses,
weak or absent ETags, and servers rejecting or ignoring ranges continue through the original
single-connection download path. A resource changing during parallel transfer fails rather than
mixing versions of a file.

Notifications, progress, scoped storage, pause, cancellation, and resume continue to use the existing
download service. Closing the stream cancels queued work and closes active range responses. Resumed
downloads use the upstream service's existing range and restart handling, with one connection.

The implementation is in `app/src/main/java/org/mozilla/fenix/downloads/ParallelDownload.kt`.
`automation/iceraven/patches/parallel_downloads.patch` adds a response hook to Android Components
for both browser-provided responses and requests made by the download service. The existing CI
preparation script applies that patch; no separately published submodule commit is required.

`ParallelDownloadTest` covers concurrent transfer, byte-for-byte ordering, compatibility fallback,
changed resources, short/oversized responses, resource cleanup, stream contracts, private request
metadata, and transfer against a local HTTP range server. The standard application test command is:

```sh
./gradlew :app:testDebugUnitTest --tests org.mozilla.fenix.downloads.ParallelDownloadTest
```
