"""Config flow for YouTube Recently Added integration."""
import logging
from typing import Any, Mapping

from homeassistant import config_entries
from homeassistant.core import callback
from homeassistant.helpers import config_entry_oauth2_flow
import voluptuous as vol

from .const import (
    DOMAIN,
    CONF_API_KEY,
    CONF_CLIENT_ID,
    CONF_CLIENT_SECRET,
    CONF_REFRESH_TOKEN,
    CONF_CHANNEL_IDS,
    CONF_MAX_REGULAR_VIDEOS,
    CONF_MAX_SHORT_VIDEOS,
    DEFAULT_MAX_REGULAR_VIDEOS,
    DEFAULT_MAX_SHORT_VIDEOS,
    CONF_FAVORITE_CHANNELS,
    DEFAULT_FAVORITE_CHANNELS,
    CONF_FILTER_CHANNELS,
    DEFAULT_FILTER_CHANNELS,
    CONF_USE_COMMENTS,
    DEFAULT_USE_COMMENTS,
)

# NOTE: YouTubeAPI is imported lazily inside methods that need it,
# NOT at module level.  This avoids pulling in heavy transitive
# dependencies (numpy, PIL, aiofiles …) during config-flow class
# loading, which was the most likely cause of the silent 500 error.

_LOGGER = logging.getLogger(__name__)

YOUTUBE_SCOPE = "https://www.googleapis.com/auth/youtube.force-ssl"


class YouTubeRecentlyAddedConfigFlow(
    config_entry_oauth2_flow.AbstractOAuth2FlowHandler,
    domain=DOMAIN,
):
    """Handle a config flow for YouTube Recently Added."""

    DOMAIN = DOMAIN
    VERSION = 1

    def __init__(self) -> None:
        """Initialize the config flow."""
        super().__init__()
        self._oauth_data: dict[str, Any] | None = None
        self._reauth_entry: config_entries.ConfigEntry | None = None

    @property
    def logger(self) -> logging.Logger:
        """Return logger."""
        return _LOGGER

    @property
    def extra_authorize_data(self) -> dict:
        """Extra data to append to the authorize URL."""
        return {
            "scope": YOUTUBE_SCOPE,
            "access_type": "offline",
            "prompt": "consent",
        }

    # ── Step: User (entry point) ──────────────────────────────────────

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Handle the initial user step."""
        # Skip uniqueness check during reauth
        if not self._reauth_entry:
            await self.async_set_unique_id(DOMAIN)
            self._abort_if_unique_id_configured()
        return await super().async_step_user(user_input)

    # ── OAuth entry creation (called after token exchange) ────────────

    async def async_oauth_create_entry(
        self, data: dict[str, Any]
    ) -> config_entries.ConfigFlowResult:
        """Handle the creation of an entry after OAuth completes.

        For reauth: update existing entry immediately (no extra steps).
        For new setup: redirect to the API key / options step.
        """
        if self._reauth_entry:
            return await self._finish_reauth(data)

        # Store OAuth data and move to API key collection
        self._oauth_data = data
        return await self.async_step_api_key()

    # ── Step: API Key + options (new setup only) ──────────────────────

    async def async_step_api_key(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Collect YouTube Data API v3 key and sensor options."""
        errors: dict[str, str] = {}

        if user_input is not None:
            # ── Safely extract OAuth state ────────────────────────────────
            # These accesses are outside the channel-fetch try/except so
            # failures here would previously escape as unhandled exceptions,
            # causing HA to show the generic "Unknown error occurred".
            try:
                api_key = user_input[CONF_API_KEY].strip()
                client_id = self.flow_impl.client_id
                client_secret = self.flow_impl.client_secret
                token_data = self._oauth_data.get("token", {})
                refresh_token = token_data.get("refresh_token")
            except Exception as exc:
                _LOGGER.error(
                    "Failed to read OAuth state in api_key step: %s",
                    exc,
                    exc_info=True,
                )
                errors["base"] = "auth_error"
                refresh_token = None  # fall through to show form with error

            if not errors and not refresh_token:
                errors["base"] = "no_refresh_token"

            # ── Fetch user channels (network call) ────────────────────────
            channel_list = None
            if not errors:
                try:
                    # Lazy import — keeps module-level loading lightweight
                    from .youtube_api import YouTubeAPI

                    youtube = YouTubeAPI(
                        api_key,
                        client_id,
                        client_secret,
                        refresh_token,
                        None,  # channel_ids
                        self.hass,
                    )
                    channel_list = await youtube.fetch_user_channels(refresh_token)
                    if not channel_list:
                        errors["base"] = "channel_fetch_failed"
                except Exception as exc:
                    error_msg = str(exc)
                    if "quota exceeded" in error_msg.lower():
                        _LOGGER.error(
                            "YouTube API quota exceeded during setup: %s", error_msg
                        )
                    else:
                        _LOGGER.error(
                            "Error during channel fetch in api_key step: %s",
                            error_msg,
                            exc_info=True,
                        )
                    errors["base"] = "cannot_connect"

            # ── Create config entry (OUTSIDE try/except) ──────────────────
            # This must NOT be inside the channel-fetch try/except.
            # If async_create_entry raises (e.g. async_setup_entry fails),
            # the entry may already be created.  Catching that exception
            # would re-show the form with an error while the entry
            # silently exists — which is the "Unknown error" bug.
            if not errors and channel_list:
                from .const import set_USE_COMMENTS_AS_SUMMARY

                use_comments = user_input.get(
                    CONF_USE_COMMENTS, DEFAULT_USE_COMMENTS
                )
                set_USE_COMMENTS_AS_SUMMARY(use_comments)

                _LOGGER.debug(
                    "Creating config entry with %d channels", len(channel_list)
                )
                return self.async_create_entry(
                    title="YouTube Recently Added",
                    data={
                        CONF_API_KEY: api_key,
                        CONF_CLIENT_ID: client_id,
                        CONF_CLIENT_SECRET: client_secret,
                        CONF_REFRESH_TOKEN: refresh_token,
                        CONF_CHANNEL_IDS: channel_list,
                        "token": token_data,
                        "auth_implementation": self._oauth_data.get(
                            "auth_implementation"
                        ),
                    },
                    options={
                        CONF_MAX_REGULAR_VIDEOS: user_input.get(
                            CONF_MAX_REGULAR_VIDEOS,
                            DEFAULT_MAX_REGULAR_VIDEOS,
                        ),
                        CONF_MAX_SHORT_VIDEOS: user_input.get(
                            CONF_MAX_SHORT_VIDEOS,
                            DEFAULT_MAX_SHORT_VIDEOS,
                        ),
                        CONF_FAVORITE_CHANNELS: user_input.get(
                            CONF_FAVORITE_CHANNELS,
                            DEFAULT_FAVORITE_CHANNELS,
                        ),
                        CONF_FILTER_CHANNELS: user_input.get(
                            CONF_FILTER_CHANNELS, DEFAULT_FILTER_CHANNELS
                        ),
                        CONF_USE_COMMENTS: use_comments,
                    },
                )

        # FIX: Removed bare string keys that were used as section headers.
        # voluptuous schemas only accept vol.Required/vol.Optional markers
        # or actual schema keys — plain strings cause serialization crashes.
        schema = vol.Schema(
            {
                vol.Required(CONF_API_KEY): str,
                vol.Required(
                    CONF_MAX_REGULAR_VIDEOS, default=DEFAULT_MAX_REGULAR_VIDEOS
                ): int,
                vol.Required(
                    CONF_MAX_SHORT_VIDEOS, default=DEFAULT_MAX_SHORT_VIDEOS
                ): int,
                vol.Optional(
                    CONF_FAVORITE_CHANNELS,
                    default=DEFAULT_FAVORITE_CHANNELS,
                ): str,
                vol.Optional(
                    CONF_FILTER_CHANNELS,
                    default=DEFAULT_FILTER_CHANNELS,
                ): str,
                vol.Required(CONF_USE_COMMENTS, default=DEFAULT_USE_COMMENTS): bool,
            }
        )

        return self.async_show_form(
            step_id="api_key",
            data_schema=schema,
            errors=errors,
        )

    # ── Reauth flow ──────────────────────────────────────────────────

    async def async_step_reauth(
        self, entry_data: Mapping[str, Any]
    ) -> config_entries.ConfigFlowResult:
        """Handle reauth flow when token is invalid."""
        self._reauth_entry = self.hass.config_entries.async_get_entry(
            self.context["entry_id"]
        )
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Handle reauth confirmation."""
        if user_input is None:
            return self.async_show_form(
                step_id="reauth_confirm",
                data_schema=vol.Schema({}),
            )
        # Begin OAuth flow (goes to pick_implementation → external auth)
        return await self.async_step_user()

    async def _finish_reauth(
        self, data: dict[str, Any]
    ) -> config_entries.ConfigFlowResult:
        """Finish reauth by updating the existing config entry."""
        token_data = data.get("token", {})
        refresh_token = token_data.get("refresh_token")

        if not refresh_token:
            _LOGGER.error("No refresh token received during reauth")
            return self.async_abort(reason="oauth_error")

        client_id = self.flow_impl.client_id
        client_secret = self.flow_impl.client_secret
        existing_api_key = self._reauth_entry.data.get(CONF_API_KEY, "")

        # Try to fetch fresh channels; fall back to existing ones
        channel_list = self._reauth_entry.data.get(CONF_CHANNEL_IDS, [])
        try:
            from .youtube_api import YouTubeAPI

            youtube = YouTubeAPI(
                existing_api_key,
                client_id,
                client_secret,
                refresh_token,
                None,
                self.hass,
            )
            fetched = await youtube.fetch_user_channels(refresh_token)
            if fetched:
                channel_list = fetched
        except Exception as exc:
            _LOGGER.warning(
                "Channel fetch failed during reauth; keeping existing channels: %s",
                exc,
            )

        self.hass.config_entries.async_update_entry(
            self._reauth_entry,
            data={
                **self._reauth_entry.data,
                CONF_CLIENT_ID: client_id,
                CONF_CLIENT_SECRET: client_secret,
                CONF_REFRESH_TOKEN: refresh_token,
                CONF_CHANNEL_IDS: channel_list,
                "token": token_data,
                "auth_implementation": data.get("auth_implementation"),
            },
            options=self._reauth_entry.options,  # preserve existing options
        )
        await self.hass.config_entries.async_reload(self._reauth_entry.entry_id)
        return self.async_abort(reason="reauth_successful")

    # ── Options flow registration ─────────────────────────────────────

    @staticmethod
    @callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> config_entries.OptionsFlow:
        """Get the options flow handler."""
        return YouTubeRecentlyAddedOptionsFlowHandler(config_entry)


# ══════════════════════════════════════════════════════════════════════
#  Options Flow (unchanged logic, cleaned up schema)
# ══════════════════════════════════════════════════════════════════════


class YouTubeRecentlyAddedOptionsFlowHandler(config_entries.OptionsFlow):
    """Handle options for YouTube Recently Added."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        """Initialize options flow."""
        self.options = dict(config_entry.data)

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Manage the options."""
        errors: dict[str, str] = {}

        if user_input is not None:
            from .const import set_USE_COMMENTS_AS_SUMMARY

            old_use_comments = self.config_entry.options.get(
                CONF_USE_COMMENTS, DEFAULT_USE_COMMENTS
            )
            new_use_comments = user_input.get(CONF_USE_COMMENTS, DEFAULT_USE_COMMENTS)

            set_USE_COMMENTS_AS_SUMMARY(new_use_comments)
            self.options.update(user_input)

            # If the USE_COMMENTS setting changed, trigger an immediate update
            if old_use_comments != new_use_comments:
                coordinator = self.hass.data[DOMAIN].get(
                    f"{self.config_entry.entry_id}_coordinator"
                )
                if coordinator:
                    all_videos = []
                    if isinstance(coordinator.data.get("data"), list):
                        all_videos.extend(coordinator.data["data"])
                    if isinstance(coordinator.data.get("shorts_data"), list):
                        all_videos.extend(coordinator.data["shorts_data"])
                    if isinstance(coordinator.data.get("favorite_channels"), list):
                        all_videos.extend(coordinator.data["favorite_channels"])

                    video_ids = list(
                        {
                            v["id"]
                            for v in all_videos
                            if isinstance(v, dict) and "id" in v
                        }
                    )
                    if video_ids:
                        stats_and_comments = await coordinator.youtube.batch_update_video_statistics_and_comments(
                            video_ids
                        )
                        if stats_and_comments:
                            for videos in [
                                coordinator.data.get("data", []),
                                coordinator.data.get("shorts_data", []),
                                coordinator.data.get("favorite_channels", []),
                            ]:
                                for video in videos:
                                    if (
                                        isinstance(video, dict)
                                        and video.get("id") in stats_and_comments
                                    ):
                                        if "statistics" not in video:
                                            video["statistics"] = {}
                                        stats = stats_and_comments[video["id"]]
                                        video["statistics"].update(
                                            {
                                                "viewCount": stats.get(
                                                    "viewCount",
                                                    video["statistics"].get(
                                                        "viewCount", "0"
                                                    ),
                                                ),
                                                "likeCount": stats.get(
                                                    "likeCount",
                                                    video["statistics"].get(
                                                        "likeCount", "0"
                                                    ),
                                                ),
                                            }
                                        )
                                        if "live_status" in stats:
                                            video["live_status"] = stats["live_status"]
                                        if "comments" in stats:
                                            video["summary"] = stats["comments"]
                            coordinator.async_set_updated_data(coordinator.data)

            return self.async_create_entry(title="", data=self.options)

        # FIX: Removed bare string keys from schema (same fix as api_key step)
        schema = vol.Schema(
            {
                vol.Required(
                    CONF_MAX_REGULAR_VIDEOS,
                    default=self.config_entry.options.get(
                        CONF_MAX_REGULAR_VIDEOS, DEFAULT_MAX_REGULAR_VIDEOS
                    ),
                ): int,
                vol.Required(
                    CONF_MAX_SHORT_VIDEOS,
                    default=self.config_entry.options.get(
                        CONF_MAX_SHORT_VIDEOS, DEFAULT_MAX_SHORT_VIDEOS
                    ),
                ): int,
                vol.Optional(
                    CONF_FAVORITE_CHANNELS,
                    description={
                        "suggested_value": self.config_entry.options.get(
                            CONF_FAVORITE_CHANNELS, DEFAULT_FAVORITE_CHANNELS
                        ),
                    },
                ): str,
                vol.Optional(
                    CONF_FILTER_CHANNELS,
                    description={
                        "suggested_value": self.config_entry.options.get(
                            CONF_FILTER_CHANNELS, DEFAULT_FILTER_CHANNELS
                        ),
                    },
                ): str,
                vol.Required(
                    CONF_USE_COMMENTS,
                    default=self.config_entry.options.get(
                        CONF_USE_COMMENTS, DEFAULT_USE_COMMENTS
                    ),
                ): bool,
            }
        )

        return self.async_show_form(
            step_id="init",
            data_schema=schema,
            errors=errors,
        )
