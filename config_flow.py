import logging
from homeassistant import config_entries
from homeassistant.core import callback
import voluptuous as vol
import aiohttp
from .const import DOMAIN, CONF_API_KEY, CONF_CLIENT_ID, CONF_CLIENT_SECRET, CONF_REFRESH_TOKEN, CONF_CHANNEL_IDS, CONF_MAX_REGULAR_VIDEOS, CONF_MAX_SHORT_VIDEOS, DEFAULT_MAX_REGULAR_VIDEOS, DEFAULT_MAX_SHORT_VIDEOS, CONF_FAVORITE_CHANNELS, DEFAULT_FAVORITE_CHANNELS, CONF_USE_COMMENTS, DEFAULT_USE_COMMENTS
from .youtube_api import YouTubeAPI

_LOGGER = logging.getLogger(__name__)

class YouTubeRecentlyAddedConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    VERSION = 1
    CONNECTION_CLASS = config_entries.CONN_CLASS_CLOUD_POLL

    def __init__(self):
        self.youtube_api = None
        self.auth_url = None

    async def async_step_user(self, user_input=None):
        errors = {}
        if user_input is not None:
            try:
                self.youtube_api = YouTubeAPI(
                    user_input[CONF_API_KEY],
                    user_input[CONF_CLIENT_ID],
                    user_input[CONF_CLIENT_SECRET],
                    None,  # refresh_token
                    None,  # channel_ids
                    self.hass
                )
                self.auth_url = await self.youtube_api.get_authorization_url()

                if not self.auth_url:
                    _LOGGER.error("Authorization URL is None. There was a problem generating it.")
                    errors["base"] = "auth_url_none"

                return await self.async_step_auth()
            except aiohttp.ClientError as err:
                _LOGGER.error("Network error during initial setup: %s", str(err))
                errors["base"] = "cannot_connect"
            except ValueError as e:
                _LOGGER.error("Invalid input error during initial setup: %s", str(e))
                errors["base"] = "invalid_auth"
            except Exception as e:
                _LOGGER.error("Unexpected error during initial setup: %s", str(e), exc_info=True)
                errors["base"] = "auth_error"

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema({
                vol.Required(CONF_API_KEY): str,
                vol.Required(CONF_CLIENT_ID): str,
                vol.Required(CONF_CLIENT_SECRET): str,
            }),
            errors=errors,
        )

    async def async_step_auth(self, user_input=None):
        if user_input is None:
            schema = vol.Schema({
                vol.Required("code"): str,
                vol.Required(CONF_MAX_REGULAR_VIDEOS, default=DEFAULT_MAX_REGULAR_VIDEOS): int,
                vol.Required(CONF_MAX_SHORT_VIDEOS, default=DEFAULT_MAX_SHORT_VIDEOS): int,
                "Optional Sensor: Include only your most favorite channels": "",
                vol.Optional(CONF_FAVORITE_CHANNELS, 
                            description="Favorite Channels (comma-delimited list)",
                            default=DEFAULT_FAVORITE_CHANNELS): str,
                vol.Required(CONF_USE_COMMENTS, default=DEFAULT_USE_COMMENTS): bool,
            })
            return self.async_show_form(
                step_id="auth",
                data_schema=schema,
                description_placeholders={"auth_url": self.auth_url},
            )

        try:
            refresh_token = await self.youtube_api.perform_oauth2_flow(user_input["code"])
            
            channel_list = await self.youtube_api.fetch_user_channels(refresh_token)
            if not channel_list:
                raise ValueError("No channels fetched. Please check your YouTube subscriptions.")
            self.channel_ids = channel_list  
            self.youtube_api.channel_ids = channel_list  

            from .const import set_USE_COMMENTS_AS_SUMMARY
            set_USE_COMMENTS_AS_SUMMARY(user_input[CONF_USE_COMMENTS])

            entry = self.async_create_entry(
                title="YouTube Recently Added",
                data={
                    CONF_API_KEY: self.youtube_api.api_key,
                    CONF_CLIENT_ID: self.youtube_api.client_id,
                    CONF_CLIENT_SECRET: self.youtube_api.client_secret,
                    CONF_REFRESH_TOKEN: refresh_token,
                    CONF_CHANNEL_IDS: self.channel_ids,
                    CONF_MAX_REGULAR_VIDEOS: user_input[CONF_MAX_REGULAR_VIDEOS],
                    CONF_MAX_SHORT_VIDEOS: user_input[CONF_MAX_SHORT_VIDEOS],
                    CONF_FAVORITE_CHANNELS: user_input[CONF_FAVORITE_CHANNELS],
                    CONF_USE_COMMENTS: user_input[CONF_USE_COMMENTS],
                },
            )
            self.youtube_api.config_entry = entry
            return entry
        except Exception as e:
            _LOGGER.error("Error during authentication in async_step_auth: %s", str(e))
            schema = vol.Schema({
                vol.Required("code"): str,
                vol.Required(CONF_MAX_REGULAR_VIDEOS, default=DEFAULT_MAX_REGULAR_VIDEOS): int,
                vol.Required(CONF_MAX_SHORT_VIDEOS, default=DEFAULT_MAX_SHORT_VIDEOS): int,
                "Optional Sensor: Include only your most favorite channels": "",
                vol.Optional(CONF_FAVORITE_CHANNELS, 
                            description="Favorite Channels (comma-delimited list)",
                            default=DEFAULT_FAVORITE_CHANNELS): str,
                vol.Required(CONF_USE_COMMENTS, 
                            description="Top 5 comments as video summary instead of video description",
                            default=DEFAULT_USE_COMMENTS): bool,
            })
            return self.async_show_form(
                step_id="auth",
                data_schema=schema,
                errors={"base": "auth_error"},
                description_placeholders={"auth_url": self.auth_url},
            )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):
        return YouTubeRecentlyAddedOptionsFlowHandler(config_entry)

class YouTubeRecentlyAddedOptionsFlowHandler(config_entries.OptionsFlow):
    def __init__(self, config_entry):
        self.config_entry = config_entry
        self.youtube_api = None
        self.auth_url = None
        self.options = dict(config_entry.data)

    async def async_step_init(self, user_input=None):
        errors = {}
        if user_input is not None:
            from .const import set_USE_COMMENTS_AS_SUMMARY
            old_use_comments = self.config_entry.options.get(CONF_USE_COMMENTS, DEFAULT_USE_COMMENTS)
            new_use_comments = user_input.get(CONF_USE_COMMENTS, DEFAULT_USE_COMMENTS)
            
            # Update the setting
            set_USE_COMMENTS_AS_SUMMARY(new_use_comments)
            self.options.update(user_input)

            # If the USE_COMMENTS setting changed, trigger an immediate statistics and comments update
            if old_use_comments != new_use_comments:
                coordinator = self.hass.data[DOMAIN].get(f"{self.config_entry.entry_id}_coordinator")
                if coordinator:
                    all_videos = []
                    if isinstance(coordinator.data.get('data'), list):
                        all_videos.extend(coordinator.data['data'])
                    if isinstance(coordinator.data.get('shorts_data'), list):
                        all_videos.extend(coordinator.data['shorts_data'])
                    if isinstance(coordinator.data.get('favorite_channels'), list):
                        all_videos.extend(coordinator.data['favorite_channels'])
                    
                    video_ids = list({v['id'] for v in all_videos if isinstance(v, dict) and 'id' in v})
                    if video_ids:
                        stats_and_comments = await coordinator.youtube.batch_update_video_statistics_and_comments(video_ids)
                        if stats_and_comments:
                            for videos in [coordinator.data.get('data', []), coordinator.data.get('shorts_data', []), coordinator.data.get('favorite_channels', [])]:
                                for video in videos:
                                    if isinstance(video, dict) and video.get('id') in stats_and_comments:
                                        if 'statistics' not in video:
                                            video['statistics'] = {}
                                        stats = stats_and_comments[video['id']]
                                        video['statistics'].update({
                                            'viewCount': stats.get('viewCount', video['statistics'].get('viewCount', '0')),
                                            'likeCount': stats.get('likeCount', video['statistics'].get('likeCount', '0'))
                                        })
                                        if 'live_status' in stats:
                                            video['live_status'] = stats['live_status']
                                        if 'comments' in stats:
                                            video['summary'] = stats['comments']
                            coordinator.async_set_updated_data(coordinator.data)

            return self.async_create_entry(title="", data=self.options)

        schema = vol.Schema({
            vol.Required(CONF_MAX_REGULAR_VIDEOS, 
                        default=self.config_entry.options.get(CONF_MAX_REGULAR_VIDEOS, DEFAULT_MAX_REGULAR_VIDEOS)): int,
            vol.Required(CONF_MAX_SHORT_VIDEOS, 
                        default=self.config_entry.options.get(CONF_MAX_SHORT_VIDEOS, DEFAULT_MAX_SHORT_VIDEOS)): int,
            "Optional Sensor: Include only your most favorite channels": "",
            vol.Optional(CONF_FAVORITE_CHANNELS, 
                        description="Favorite Channels (comma-delimited list)",
                        default=self.config_entry.options.get(CONF_FAVORITE_CHANNELS, DEFAULT_FAVORITE_CHANNELS)): str,
            vol.Required(CONF_USE_COMMENTS,
                        default=self.config_entry.options.get(CONF_USE_COMMENTS, DEFAULT_USE_COMMENTS)): bool,
        })

        return self.async_show_form(
            step_id="init",
            data_schema=schema,
            errors=errors,
        )

    async def async_step_auth(self, user_input=None):
        if user_input is None:
            return self.async_show_form(
                step_id="auth",
                data_schema=vol.Schema({
                    vol.Required("code"): str,
                }),
                description_placeholders={
                    "auth_url": self.auth_url,
                },
            )

        try:
            token = await self.youtube_api.perform_oauth2_flow(user_input["code"])
            self.options[CONF_REFRESH_TOKEN] = token

            # Fetch the user's channels using the YouTube API
            channel_list = await self.youtube_api.fetch_user_channels(token)
            if not channel_list:
                raise ValueError("No channels fetched. Please check your YouTube subscriptions.")
            
            self.options[CONF_CHANNEL_IDS] = channel_list
            
            # Update the entry with the new token and channels
            self.hass.config_entries.async_update_entry(
                self.config_entry,
                data=self.options,
            )
            
            return self.async_create_entry(title="", data=self.options)
        except Exception as error:
            _LOGGER.error("OAuth2 error: %s", str(error))
            return self.async_show_form(
                step_id="auth",
                data_schema=vol.Schema({
                    vol.Required("code"): str,
                }),
                errors={"base": "auth_error"},
                description_placeholders={
                    "auth_url": self.auth_url,
                },
            )
