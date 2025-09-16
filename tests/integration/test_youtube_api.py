from web.youtube_channel_etl import YouTubeChannelETL


class DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class DummySession:
    def __init__(self):
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params, timeout))
        return DummyResponse({"items": [{"snippet": {"channelId": "UC123"}}]})


def test_resolve_channel_id_uses_custom_base_url():
    session = DummySession()
    etl = YouTubeChannelETL(
        api_key="key",
        db_host="localhost",
        db_port=3306,
        db_user="user",
        db_pass="pass",
        db_name="yt",
        session=session,
        api_base_url="https://example.test/api",
    )

    channel_id = etl.resolve_channel_id("https://youtube.com/@artist")

    assert channel_id == "UC123"
    url, params, timeout = session.calls[0]
    assert url == "https://example.test/api/search"
    assert params["q"] == "@artist"
    assert timeout == 30
