# YouTubeVideoInfoScraper

根据 video id 使用 YouTube Data API v3 获取视频信息，可配置 Proxy。

[Proxy 配置思路](https://suniven.github.io/posts/58e6e58f/)

## config.ini 配置

```ini
[youtube]
api_service_name = youtube
api_version = v3
API_KEY = [YOUR_GOOGLE_API_KEY]

[proxy]
host = [YOUR_HOST]
port = [YOUR_PORT]
```