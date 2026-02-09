DOMAIN = "youtube_recently_added"
CONF_API_KEY = "api_key"
CONF_CLIENT_ID = "client_id"
CONF_CLIENT_SECRET = "client_secret"
CONF_REFRESH_TOKEN = "refresh_token"
CONF_CHANNEL_IDS = "channel_ids"
CONF_MAX_REGULAR_VIDEOS = "max_regular_videos"
CONF_MAX_SHORT_VIDEOS = "max_short_videos"
DEFAULT_MAX_REGULAR_VIDEOS = 16
DEFAULT_MAX_SHORT_VIDEOS = 16
CONF_FAVORITE_CHANNELS = "favorite_channels"
DEFAULT_FAVORITE_CHANNELS = ""
CONF_FILTER_CHANNELS = "filter_channels"
DEFAULT_FILTER_CHANNELS = ""
WEBHOOK_ID = "youtube_recently_added"
PUBSUB_HUB = "https://pubsubhubbub.appspot.com/subscribe"
CALLBACK_PATH = "/api/webhook/youtube_recently_added"
LOGGER_NAME = "youtube_recently_added"
CONF_USE_COMMENTS = "USE_COMMENTS_AS_SUMMARY"
DEFAULT_USE_COMMENTS = False

_use_comments_as_summary = DEFAULT_USE_COMMENTS

def get_USE_COMMENTS_AS_SUMMARY():
    global _use_comments_as_summary
    return _use_comments_as_summary

def set_USE_COMMENTS_AS_SUMMARY(value: bool):
    global _use_comments_as_summary
    _use_comments_as_summary = value
