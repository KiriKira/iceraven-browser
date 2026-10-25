基于上游 Iceraven **2.51.0 / Firefox 158.0**，加入默认并发下载。

- 对至少 8 MiB、支持 HTTP Range 且具有强 ETag 的文件，默认使用 4 路并发、每段 1 MiB。
- 复用最终下载 URL，支持 GitHub Release 等签名 CDN 下载地址。
- 校验最终 URL、分段范围、ETag 和实际字节数，避免拼接错误文件。
- 不满足并发条件时保留普通下载；暂停、取消和原有单连接断点续传流程继续可用。
- 不需要安装 aria2，也不增加存储权限。

验证：12 项下载测试通过，包含本地 HTTP 分段服务的实际并发下载；调试 APK 构建通过。
GitHub Release 附件的最终地址已实测返回有效 Range / If-Range 的 206 响应。

附件提供 arm64-v8a、armeabi-v7a、x86_64 的正式构建，以及 SHA256SUMS 校验文件。
APK 沿用本仓库 GitHub Actions 配置的签名密钥。
