import logging
import os
import json
import io
import re
import html
from datetime import datetime, timedelta

from isodate import parse_duration
import numpy as np
from PIL import Image

import aiohttp
import aiofiles
import asyncio
import async_timeout
from zoneinfo import ZoneInfo

from homeassistant.helpers.storage import Store
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.components import persistent_notification
from homeassistant.exceptions import ConfigEntryNotReady, ConfigEntryAuthFailed

from .const import (DOMAIN, CONF_MAX_REGULAR_VIDEOS, CONF_MAX_SHORT_VIDEOS, DEFAULT_MAX_REGULAR_VIDEOS, DEFAULT_MAX_SHORT_VIDEOS, WEBHOOK_ID, CONF_FAVORITE_CHANNELS, DEFAULT_FAVORITE_CHANNELS, CONF_REFRESH_TOKEN, get_USE_COMMENTS_AS_SUMMARY)

_LOGGER = logging.getLogger(__name__)

class QuotaExceededException(Exception):
    pass

def filter_live_stream_title(title: str, live_status: str = None) -> str:
    LIVE_TITLE_FILTER_ENABLED = True
    if not LIVE_TITLE_FILTER_ENABLED:
        return title
    
    original_title = title

    if "🔴" in title:
        title = title.replace("🔴", "")
        
        # Also remove "LIVE" if it appears at the beginning of the title
        if title.strip().upper().startswith("LIVE"):
            live_text = "LIVE"
            live_pos = title.upper().find(live_text)
            if live_pos != -1:
                title = title[live_pos + len(live_text):].strip()

        title = title.strip()
        _LOGGER.debug(f"Filtered live prefix from title: '{original_title}' -> '{title}'")
    
    return title

class YouTubeAPI:
    def __init__(self, api_key, client_id, client_secret, refresh_token=None, channel_ids=None, hass=None, config_entry=None):
        self.api_key = api_key
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.channel_ids = list(channel_ids) if channel_ids else []
        self.initial_channel_ids = self.channel_ids.copy()
        self.hass = hass
        self.config_entry = config_entry
        self._store = Store(hass, 1, f"{DOMAIN}.youtube_data")
        self.session = None
        self.access_token = None
        self.token_expiration = None
        self.backoff_time = 15
        self.max_backoff_time = 3600
        self.quota_reset_time = None
        self.current_quota = 0
        self.fetched_video_ids = set()
        self.last_sync_time = None
        self.last_fetch_time = None
        self.subscriptions = None
        self.last_subscription_update = None
        self.channel_playlist_mapping = {}
        self.quota_reset_task = None
        self.subscription_renewal_task = None
        self.last_subscription_renewal_time = None
        self.playlist_cache = {}  
        self.channel_subscription_times = {}
        self.fetched_video_ids_lock = asyncio.Lock()
        self.video_processing_semaphore = asyncio.Semaphore(10)  # Limit concurrent video processing
        self.quota_lock = asyncio.Lock()
        self.token_refresh_lock = asyncio.Lock()
        self.token_refresh_failures = 0
        self.permanent_token_failure = False
        self.last_fetch_time = None

        _LOGGER.debug(f"Initializing YouTubeAPI with current_quota: {self.current_quota}, quota_reset_time: {self.quota_reset_time}")
        # _LOGGER.critical(f"YouTubeAPI initialized with channel_ids: {self.channel_ids}")
        # _LOGGER.critical(f"YouTubeAPI initial quota: {self.current_quota}, initial reset time: {self.quota_reset_time}")
        # _LOGGER.critical(f"YouTubeAPI initialized with channel_ids: {self.channel_ids}, channel_subscription_times: {self.channel_subscription_times}")

    def _process_channel_ids(self, channel_ids):
        if isinstance(channel_ids, list):
            return [ch.strip() for ch in channel_ids if isinstance(ch, str) and ch.strip()]
        elif isinstance(channel_ids, str):
            return [ch.strip() for ch in channel_ids.split(',') if ch.strip()]
        else:
            return []

    async def _load_persistent_data(self):
        _LOGGER.debug("Loading persistent data")
        stored_data = await self._store.async_load()
        if stored_data:
            self.current_quota = stored_data.get("current_quota", 0)
            quota_reset_str = stored_data.get("quota_reset_time")
            if quota_reset_str:
                parsed_quota_reset_time = datetime.fromisoformat(quota_reset_str)
                if parsed_quota_reset_time.tzinfo is None:
                    self.quota_reset_time = parsed_quota_reset_time.replace(tzinfo=ZoneInfo("UTC"))
                else:
                    self.quota_reset_time = parsed_quota_reset_time.astimezone(ZoneInfo("UTC"))
                
                current_time = datetime.now(ZoneInfo("UTC"))
                _LOGGER.debug(f"Loaded quota_reset_time (UTC): {self.quota_reset_time}, Current time (UTC): {current_time}")
                if current_time >= self.quota_reset_time:
                    _LOGGER.info("Stored quota reset time has passed. Resetting quota.")
                    self.current_quota = 0
                    self.quota_reset_time = self._calculate_quota_reset_time()
            else:
                _LOGGER.debug("No quota_reset_time found in stored data. Calculating new reset time.")
                self.quota_reset_time = self._calculate_quota_reset_time()
            self.fetched_video_ids = set(stored_data.get("fetched_video_ids", []))
            # Only use stored refresh_token if we don't have one from config
            if not self.refresh_token:
                self.refresh_token = stored_data.get("refresh_token", self.refresh_token)
            self.access_token = stored_data.get("access_token")
            self.subscriptions = stored_data.get("subscriptions", [])
            last_sub_update = stored_data.get("last_subscription_update")
            self.last_subscription_update = datetime.fromisoformat(last_sub_update).astimezone(ZoneInfo("UTC")) if last_sub_update else None
            last_sync = stored_data.get("last_sync_time")
            self.last_sync_time = datetime.fromisoformat(last_sync).astimezone(ZoneInfo("UTC")) if last_sync else None
            self.channel_playlist_mapping = stored_data.get("channel_playlist_mapping", {})
            self.channel_ids = stored_data.get("channel_ids", [])
            self.playlist_cache = stored_data.get("playlist_cache", {})
            token_expiration = stored_data.get("token_expiration")
            self.token_expiration = datetime.fromisoformat(token_expiration).astimezone(ZoneInfo("UTC")) if token_expiration else None
            self.channel_subscription_times = {k: datetime.fromisoformat(v).replace(tzinfo=ZoneInfo("UTC")) 
                                            for k, v in stored_data.get("channel_subscription_times", {}).items()}
            self.last_subscription_renewal_time = datetime.fromisoformat(stored_data.get("last_subscription_renewal_time")).astimezone(ZoneInfo("UTC")) if stored_data.get("last_subscription_renewal_time") else None
            self.last_fetch_time = datetime.fromisoformat(stored_data.get("last_fetch_time")).replace(tzinfo=ZoneInfo("UTC")) if stored_data.get("last_fetch_time") else None

            if self.config_entry:
                coordinator_data = stored_data.get("coordinator_data")
                if coordinator_data:
                    coordinator = self.hass.data.get(DOMAIN, {}).get(f"{self.config_entry.entry_id}_coordinator")
                    if coordinator:
                        coordinator.data = coordinator_data
                        coordinator.favorite_channels = [channel.strip() for channel in 
                            self.config_entry.options.get(CONF_FAVORITE_CHANNELS, DEFAULT_FAVORITE_CHANNELS).split(',') 
                            if channel.strip()]
                    else:
                        _LOGGER.error(f"Coordinator not found for entry {self.config_entry.entry_id}.")
                        return False
        else:
            _LOGGER.debug("No persistent data found. Starting with default values.")
            self.current_quota = 0
            self.quota_reset_time = None
            self.playlist_cache = {}
            self.channel_subscription_times = {}
            self.last_subscription_renewal_time = None
            self.subscriptions = []
            self.fetched_video_ids = set()
            self.channel_ids = []
            self.channel_playlist_mapping = {}
            self.access_token = None

        _LOGGER.debug(f"Final current_quota after loading persistent data: {self.current_quota}")

        self.subscriptions = self.subscriptions or []
        self.channel_ids = self.channel_ids or []
        self.fetched_video_ids = self.fetched_video_ids or set()
        self.channel_playlist_mapping = self.channel_playlist_mapping or {}
        self.playlist_cache = self.playlist_cache or {}
        self.channel_subscription_times = self.channel_subscription_times or {}

        _LOGGER.debug(f"Loaded subscriptions count: {len(self.subscriptions)}")
        _LOGGER.debug(f"Loaded channel_ids count: {len(self.channel_ids)}")
        _LOGGER.debug(f"Loaded fetched_video_ids count: {len(self.fetched_video_ids)}")
        _LOGGER.debug(f"Loaded channel_playlist_mapping count: {len(self.channel_playlist_mapping)}")

    async def _save_persistent_data(self, force=False):
        data = {
            "current_quota": self.current_quota,
            "quota_reset_time": self.quota_reset_time.isoformat() if self.quota_reset_time else None,
            "fetched_video_ids": list(self.fetched_video_ids),
            "refresh_token": self.refresh_token,
            "channel_ids": self.channel_ids,
            "subscriptions": self.subscriptions,
            "last_subscription_update": self.last_subscription_update.isoformat() if self.last_subscription_update else None,
            "last_sync_time": self.last_sync_time.isoformat() if self.last_sync_time else None,
            "channel_playlist_mapping": self.channel_playlist_mapping,
            "playlist_cache": self.playlist_cache,
            "token_expiration": self.token_expiration.isoformat() if self.token_expiration else None,
            "channel_subscription_times": {k: v.isoformat() for k, v in self.channel_subscription_times.items()},
            "last_subscription_renewal_time": self.last_subscription_renewal_time.isoformat() if self.last_subscription_renewal_time else None,
            "webhook_id": WEBHOOK_ID,
            "last_webhook_time": self.last_webhook_time.isoformat() if hasattr(self, 'last_webhook_time') else None,
            "access_token": self.access_token,
            "last_fetch_time": self.last_fetch_time.isoformat() if self.last_fetch_time else None
        }

        if self.config_entry and hasattr(self.config_entry, 'entry_id'):
            coordinator = self.hass.data.get(DOMAIN, {}).get(f"{self.config_entry.entry_id}_coordinator")
            if coordinator and hasattr(coordinator, 'data'):
                coordinator_data = coordinator.data.copy()
                coordinator_data['favorite_channels'] = self.config_entry.options.get(CONF_FAVORITE_CHANNELS, DEFAULT_FAVORITE_CHANNELS)
                data.update(coordinator_data)
            else:
                data.update({
                    "max_regular_videos": self.config_entry.options.get(CONF_MAX_REGULAR_VIDEOS, DEFAULT_MAX_REGULAR_VIDEOS),
                    "max_short_videos": self.config_entry.options.get(CONF_MAX_SHORT_VIDEOS, DEFAULT_MAX_SHORT_VIDEOS),
                    "favorite_channels": self.config_entry.options.get(CONF_FAVORITE_CHANNELS, DEFAULT_FAVORITE_CHANNELS),
                })

        try:
            await self._store.async_save(data)
        except Exception as e:
            _LOGGER.error(f"Failed to save persistent data: {e}")
            raise

    def _calculate_quota_reset_time(self):
        pacific = ZoneInfo("America/Los_Angeles")
        now_pacific = datetime.now(pacific)
        next_reset_pacific = now_pacific.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        quota_reset_time_utc = next_reset_pacific.astimezone(ZoneInfo("UTC"))
        _LOGGER.debug(f"Calculated quota reset time (PST): {next_reset_pacific}, in UTC: {quota_reset_time_utc}")
        return quota_reset_time_utc

    async def schedule_quota_reset(self):
        _LOGGER.debug("Entering schedule_quota_reset method")
        if self.quota_reset_task and not self.quota_reset_task.done():
            _LOGGER.debug("Quota reset task is already scheduled.")
            return

        self.quota_reset_time = self._calculate_quota_reset_time()
        await self._save_persistent_data()
        
        _LOGGER.info(f"Scheduling quota reset at {self.quota_reset_time} (UTC)")

        async def reset_quota():
            while True:
                try:
                    now = datetime.now(ZoneInfo("UTC"))
                    sleep_time = (self.quota_reset_time - now).total_seconds()
                    if sleep_time < 0:
                        self.quota_reset_time = self._calculate_quota_reset_time()
                        sleep_time = (self.quota_reset_time - now).total_seconds()
                    
                    _LOGGER.debug(f"Waiting {sleep_time} seconds until quota reset at {self.quota_reset_time}")
                    await asyncio.sleep(sleep_time)
                    
                    self.current_quota = 0
                    _LOGGER.info(f"Quota reset to 0 at {datetime.now(ZoneInfo('UTC'))}")
                    await self._save_persistent_data()
                    
                    self.hass.bus.async_fire("youtube_quota_reset")
                    _LOGGER.debug("Fired 'youtube_quota_reset' event")
                    
                    self.quota_reset_time = self._calculate_quota_reset_time()
                    _LOGGER.info(f"Next quota reset scheduled for {self.quota_reset_time}")
                except asyncio.CancelledError:
                    _LOGGER.warning("Quota reset task was cancelled.")
                    break
                except Exception as e:
                    _LOGGER.error(f"Error during quota reset: {e}", exc_info=True)
                    await asyncio.sleep(60)  # Retry after a short delay

        self.quota_reset_task = asyncio.create_task(reset_quota())
        _LOGGER.debug("Quota reset task has been created and scheduled.")

    async def schedule_subscription_renewal(self, callback_url):
        _LOGGER.debug("Entering schedule_subscription_renewal method")
        if self.subscription_renewal_task and not self.subscription_renewal_task.done():
            _LOGGER.debug("Subscription renewal task is already scheduled.")
            return

        async def renew_subscription():
            while True:
                try:
                    now = datetime.now(ZoneInfo("UTC"))
                    if self.last_subscription_renewal_time:
                        time_since_last_renewal = now - self.last_subscription_renewal_time
                        sleep_time = max(0, (timedelta(hours=23, minutes=50) - time_since_last_renewal).total_seconds())
                    else:
                        sleep_time = 0  # Renew immediately if never renewed before

                    _LOGGER.debug(f"Sleeping for {sleep_time} seconds until next subscription renewal.")
                    await asyncio.sleep(sleep_time)

                    # Renew subscription
                    _LOGGER.debug("Renewing PubSubHubbub subscription")
                    await self.subscribe_to_pubsub(callback_url)
                    self.last_subscription_renewal_time = datetime.now(ZoneInfo("UTC"))
                    await self._save_persistent_data()
                    _LOGGER.info(f"Subscription renewed at {self.last_subscription_renewal_time}")

                except asyncio.CancelledError:
                    _LOGGER.warning("Subscription renewal task was cancelled.")
                    break
                except Exception as e:
                    _LOGGER.error(f"Error during subscription renewal: {e}")
                    await asyncio.sleep(300)  # Retry after 5 minutes

        if not self.last_subscription_renewal_time or (datetime.now(ZoneInfo("UTC")) - self.last_subscription_renewal_time) > timedelta(hours=23, minutes=50):
            _LOGGER.info("Initiating immediate subscription renewal")
            await self.subscribe_to_pubsub(callback_url)
            self.last_subscription_renewal_time = datetime.now(ZoneInfo("UTC"))
            await self._save_persistent_data()

        self.subscription_renewal_task = asyncio.create_task(renew_subscription())
        _LOGGER.debug("Subscription renewal task has been created and scheduled.")

    async def initialize(self):
        try:
            _LOGGER.debug(f"Initializing YouTubeAPI. Starting quota: {self.current_quota}")
            # _LOGGER.critical(f"Starting initialize with channel_ids: {len(self.channel_ids)}")
            
            # Store initial channels before loading persistent data
            initial_channels = self.channel_ids.copy() if self.channel_ids else []
            # _LOGGER.critical(f"Stored initial channels: {len(initial_channels)} channels")
            
            # Load persistent data
            stored_data = await self._store.async_load()
            if stored_data:
                # Load everything except channel_ids
                self.current_quota = stored_data.get("current_quota", 0)
                quota_reset_str = stored_data.get("quota_reset_time")
                if quota_reset_str:
                    self.quota_reset_time = datetime.fromisoformat(quota_reset_str).replace(tzinfo=ZoneInfo("UTC"))
                self.fetched_video_ids = set(stored_data.get("fetched_video_ids", []))
                self.refresh_token = stored_data.get("refresh_token", self.refresh_token)
                self.access_token = stored_data.get("access_token")
                token_expiration = stored_data.get("token_expiration")
                self.token_expiration = datetime.fromisoformat(token_expiration).astimezone(ZoneInfo("UTC")) if token_expiration else None
                self.subscriptions = stored_data.get("subscriptions", [])
                last_sub_update = stored_data.get("last_subscription_update")
                self.last_subscription_update = datetime.fromisoformat(last_sub_update).astimezone(ZoneInfo("UTC")) if last_sub_update else None
                last_sync = stored_data.get("last_sync_time")
                self.last_sync_time = datetime.fromisoformat(last_sync).astimezone(ZoneInfo("UTC")) if last_sync else None
                self.channel_playlist_mapping = stored_data.get("channel_playlist_mapping", {})
                self.playlist_cache = stored_data.get("playlist_cache", {})
                self.channel_subscription_times = {k: datetime.fromisoformat(v).replace(tzinfo=ZoneInfo("UTC")) 
                                                for k, v in stored_data.get("channel_subscription_times", {}).items()}
                self.last_subscription_renewal_time = datetime.fromisoformat(stored_data.get("last_subscription_renewal_time")).astimezone(ZoneInfo("UTC")) if stored_data.get("last_subscription_renewal_time") else None
                
            # Always restore initial channels if they exist
            if initial_channels:
                _LOGGER.debug(f"Restoring initial channels: {len(initial_channels)} channels")
                self.channel_ids = initial_channels
            elif stored_data and stored_data.get("channel_ids"):
                self.channel_ids = stored_data.get("channel_ids", [])
            else:
                self.channel_ids = []
            
            if self.channel_ids:
                await self._save_persistent_data(force=True)
                
            # _LOGGER.critical(f"Channel IDs after initialization: {len(self.channel_ids)} channels")
            
            if self.current_quota is None:
                self.current_quota = 0
                _LOGGER.debug("Setting current_quota to 0 as it was None")

            _LOGGER.debug(f"After loading persistent data - Quota: {self.current_quota}, Reset time: {self.quota_reset_time}")
            _LOGGER.debug(f"Processed channel_ids: {self.channel_ids}")
            
            # Check if quota reset time has passed
            current_time = datetime.now(ZoneInfo("UTC"))
            if self.quota_reset_time and current_time >= self.quota_reset_time:
                _LOGGER.info("Quota reset time has passed. Resetting quota.")
                self.current_quota = 0
                self.quota_reset_time = self._calculate_quota_reset_time()
                await self._save_persistent_data(force=True)
            
            # Reset quota if it's above 10000
            if self.current_quota > 10000:
                _LOGGER.warning(f"Stored quota ({self.current_quota}) exceeds maximum. Resetting to 0.")
                self.current_quota = 0
                await self._save_persistent_data()
            
            current_time = datetime.now(ZoneInfo("UTC"))
            if (self.access_token and self.token_expiration and 
                current_time < self.token_expiration - timedelta(minutes=5)):
                _LOGGER.debug("Using stored valid access token")
                valid_token = self.access_token
            else:
                _LOGGER.debug("Ensuring valid OAuth token")
                valid_token = await self.ensure_valid_token()
                if not valid_token:
                    _LOGGER.error("Failed to ensure valid OAuth token during initialization")
                    persistent_notification.async_create(
                        self.hass,
                        f"Please reconfigure [YouTube Recently Added](/config/integrations) integration. OAuth token is invalid or revoked.",
                        title="YouTube Integration Authentication Failed",
                        notification_id="youtube_auth_failed"
                    )
                    return False

            self.access_token = valid_token
            _LOGGER.debug("OAuth token validated successfully")

            # Schedule quota reset
            asyncio.create_task(self.schedule_quota_reset())
            _LOGGER.debug("Quota reset task scheduled")

            return True

        except Exception as e:
            _LOGGER.error(f"Unexpected error during initialization: {e}", exc_info=True)
            return False

    # NOTE: get_authorization_url() and perform_oauth2_flow() have been removed.
    # OAuth authorization and token exchange are now handled by Home Assistant's
    # built-in OAuth2 framework (AbstractOAuth2FlowHandler + Application Credentials).
    # Token refresh is still handled by refresh_oauth_token() below.

    async def get_recent_videos(self):
        # _LOGGER.critical("Entering get_recent_videos method.")
        
        current_time = datetime.now(ZoneInfo("UTC"))
        if self.quota_reset_time and current_time >= self.quota_reset_time:
            self.current_quota = 0
            self.quota_reset_time = self._calculate_quota_reset_time()
            await self._save_persistent_data()
            _LOGGER.info(f"Quota reset to 0. Next reset time: {self.quota_reset_time}")
        
        if self.current_quota >= 10000:
            # _LOGGER.critical(f"Quota exceeded; skipping API calls until reset time at {self.quota_reset_time}.")
            message = f"API quota exceeded. Further attempts will resume after the quota resets at {self.quota_reset_time}"
            persistent_notification.async_create(
                self.hass,
                message,
                title="YouTube API Quota Exceeded",
                notification_id="youtube_quota_exceeded"
            )
            raise QuotaExceededException(message)

        valid_token = await self.ensure_valid_token()
        if not valid_token:
            _LOGGER.error("Failed to ensure valid access token; aborting API calls.")
            return {"data": [], "shorts_data": []}

        self.access_token = valid_token

        videos = {"data": [], "shorts_data": []}
        try:
            async with aiohttp.ClientSession() as session:
                headers = {"Authorization": f"Bearer {self.access_token}"}
                _LOGGER.debug("Fetching subscriptions")
                subscriptions = await self._fetch_subscriptions(session, headers)
                _LOGGER.debug(f"Fetched {len(subscriptions)} subscriptions")

                if subscriptions:
                    _LOGGER.debug("Fetching uploads playlists")
                    uploads_playlists = await self._fetch_uploads_playlists(session, headers, subscriptions)
                    _LOGGER.debug(f"Fetched {len(uploads_playlists)} uploads playlists")
                    _LOGGER.debug("Fetching activities")
                    videos = await self._fetch_activities(session, headers, uploads_playlists)
                    _LOGGER.debug(f"Fetched {len(videos.get('data', []))} regular videos and {len(videos.get('shorts_data', []))} shorts")
                else:
                    _LOGGER.debug("No subscriptions found")

            # _LOGGER.critical(f"Fetched a total of {len(videos.get('data', []))} regular videos and {len(videos.get('shorts_data', []))} shorts across all subscriptions")
            # _LOGGER.debug(f"Videos returned from get_recent_videos: {videos}")
            return videos
        except QuotaExceededException as qee:
            _LOGGER.warning(f"Quota exceeded during video fetch: {qee}")
            raise
        except aiohttp.ClientError as err:
            _LOGGER.error(f"Network error fetching subscribed videos: {err}")
            return {"data": [], "shorts_data": []}
        except Exception as e:
            _LOGGER.error(f"Unexpected error in get_recent_videos: {e}", exc_info=True)
            return {"data": [], "shorts_data": []}

    async def get_video_by_id(self, video_id):
        # Log the initiation of the get_video_by_id method with the given video ID
        # _LOGGER.critical(f"get_video_by_id called for video ID: {video_id}")
        
        # Log the current setting for using comments as summary
        # _LOGGER.critical(f"USE_COMMENTS_AS_SUMMARY is set to: {get_USE_COMMENTS_AS_SUMMARY()}")

        # Check if the current API quota has been exceeded
        if self.current_quota >= 9997:
            message = f"API quota exceeded. Further attempts will resume after the quota resets at {self.quota_reset_time}"
            # Log a warning about the API quota being exceeded
            _LOGGER.warning(message)
            # Raise an exception to indicate quota has been exceeded
            raise QuotaExceededException(message)
        
        # Ensure the OAuth token is valid before making the API request
        await self.ensure_valid_token()
        
        # Construct the YouTube API URL for fetching video details
        url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,contentDetails,statistics,liveStreamingDetails&id={video_id}&key={self.api_key}"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        
        try:
            # Create an asynchronous HTTP session
            async with aiohttp.ClientSession() as session:
                # Make the GET request to the YouTube API
                async with session.get(url, headers=headers, timeout=10) as response:
                    # Read the response text
                    response_text = await response.text()
                    
                    if response.status == 200:
                        # Parse the JSON response
                        data = json.loads(response_text)
                        items = data.get("items", [])
                        
                        if items:
                            # Load stored data asynchronously
                            stored_data = await self._store.async_load()
                            oldest_video_date = None
                            
                            if stored_data:
                                # Combine regular and shorts video data
                                all_videos = stored_data.get('data', []) + stored_data.get('shorts_data', [])
                                publish_dates = [video.get('snippet', {}).get('publishedAt') for video in all_videos if video.get('snippet')]
                                
                                if publish_dates:
                                    # Determine the oldest video publication date
                                    oldest_video_date = min(
                                        datetime.fromisoformat(date.rstrip('Z')).replace(tzinfo=ZoneInfo("UTC")) 
                                        for date in publish_dates if date
                                    )
                            
                            # Select the first item from the response as the video data
                            item = items[0]

                            # Secondary check for obviously invalid or missing fields
                            snippet_ok = bool(item.get('snippet') and item['snippet'].get('publishedAt'))
                            content_ok = bool(item.get('contentDetails') and 'duration' in item['contentDetails'])

                            if not snippet_ok or not content_ok:
                                # Log a warning if snippet or content details are invalid/incomplete
                                # _LOGGER.warning(f"Video ID {video_id} has an invalid or incomplete snippet. Returning None.")
                                return None

                            # Parse the video's published date and time
                            video_published_at_str = item['snippet']['publishedAt']
                            video_published_at = datetime.strptime(video_published_at_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=ZoneInfo("UTC"))

                            # Extract live broadcast content and streaming details
                            live_broadcast_content = item['snippet'].get('liveBroadcastContent', 'none')
                            duration = item['contentDetails'].get('duration', 'PT0M0S')
                            live_streaming_details = item.get('liveStreamingDetails', {})
                            
                            # Get live streaming times
                            actual_start_time = live_streaming_details.get('actualStartTime')
                            actual_end_time = live_streaming_details.get('actualEndTime')
                            scheduled_start_time = live_streaming_details.get('scheduledStartTime')
                            
                            # Determine if this is a live stream based on various criteria
                            is_live_stream = (
                                live_broadcast_content in ['live', 'upcoming'] or 
                                duration in ['P0D', 'PT0M0S'] or 
                                actual_start_time is not None or
                                scheduled_start_time is not None
                            ) and not actual_end_time

                            if is_live_stream:
                                # Get the current UTC time for comparison
                                current_time = datetime.now(ZoneInfo("UTC"))
                                
                                # Don't skip ended streams - they need to be processed to show final state
                                # The is_live_stream check already excludes truly ended streams via "and not actual_end_time"
                                
                                # Process scheduled start time for upcoming streams
                                if scheduled_start_time:
                                    # Parse the scheduled start time
                                    scheduled_time = datetime.strptime(scheduled_start_time, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=ZoneInfo("UTC"))
                                    
                                    # Skip all upcoming streams that haven't actually started
                                    if live_broadcast_content == 'upcoming' and not actual_start_time:
                                        _LOGGER.debug(f"Skipping upcoming stream {video_id} - scheduled at {scheduled_time}")
                                        return None
                                
                                # Skip regular live streams that haven't started
                                if live_broadcast_content == 'live' and not actual_start_time:
                                    _LOGGER.debug(f"Skipping offline live stream {video_id}")
                                    return None

                                # Skip streams where actualStartTime is in the future
                                if actual_start_time:
                                    actual_start_datetime = datetime.strptime(actual_start_time, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=ZoneInfo("UTC"))
                                    if actual_start_datetime > datetime.now(ZoneInfo("UTC")):
                                        _LOGGER.debug(f"Skipping stream {video_id} with future actualStartTime: {actual_start_time}")
                                        return None
                            
                            # Initialize live status if not already set
                            if 'live_status' not in locals():
                                live_status = ""
                            if is_live_stream:
                                if (actual_start_time or (live_broadcast_content == 'upcoming' and 
                                    scheduled_start_time and 
                                    datetime.strptime(scheduled_start_time, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=ZoneInfo("UTC")) <= datetime.now(ZoneInfo("UTC")))) and not actual_end_time:
                                    # Retrieve and format the number of concurrent viewers
                                    concurrent_viewers = int(live_streaming_details.get('concurrentViewers', '0'))
                                    value = (
                                        concurrent_viewers / 1000000 if concurrent_viewers >= 1000000 
                                        else concurrent_viewers / 1000 if concurrent_viewers >= 1000 
                                        else concurrent_viewers
                                    )
                                    suffix = 'M' if concurrent_viewers >= 1000000 else 'K' if concurrent_viewers >= 1000 else ''
                                    formatted_viewers = (
                                        f"{value:.1f}".rstrip('0').rstrip('.') + suffix 
                                        if concurrent_viewers >= 1000 
                                        else str(concurrent_viewers)
                                    )
                                    
                                    # Set live status with viewer count
                                    if concurrent_viewers > 0:
                                        live_status = f"🔴 LIVE - {formatted_viewers} Watching"
                                    else:
                                        live_status = "🔴 LIVE - 🟩 Stream started"
                                    
                                    # Log the detection of a live stream with current viewer count
                                    # _LOGGER.warning(f"Live stream detected for video {video_id} - Currently streaming with {concurrent_viewers} viewers")
                                    
                                    # Filter and update the live stream title
                                    item['snippet']['title'] = filter_live_stream_title(item['snippet'].get('title', ''))
                                
                                elif actual_end_time:
                                    # Set live status to indicate the stream has ended
                                    live_status = "📴 Stream ended"
                                    # Log that the live stream has ended
                                    _LOGGER.warning(f"Live stream detected for video {video_id} - Stream ended at {actual_end_time}")

                            # Increment the current quota as this request consumes quota
                            async with self.quota_lock:
                                self.current_quota += 3
                            
                            if not is_live_stream:
                                # Get the current UTC time
                                current_utc_time = datetime.now(ZoneInfo("UTC"))
                                # Skip videos published in the future
                                if video_published_at > current_utc_time:
                                    _LOGGER.debug(f"Skipping video {video_id} published in the future at {video_published_at}")
                                    return None

                            # Define midnight in Los Angeles timezone and convert to UTC
                            midnight = datetime.now(ZoneInfo("America/Los_Angeles")).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(ZoneInfo("UTC"))
                            # Skip videos published before midnight LA time
                            if video_published_at < midnight:
                                _LOGGER.debug(f"Skipping video {video_id} published before midnight LA time at {video_published_at}")
                                return None
                            # Skip videos older than the oldest video date in stored data
                            if oldest_video_date and video_published_at < oldest_video_date:
                                _LOGGER.debug(f"Skipping video {video_id} published before the oldest video date {oldest_video_date}")
                                return None

                            runtime = ""
                            if not is_live_stream and not scheduled_start_time:
                                try:
                                    duration_timedelta = parse_duration(duration)
                                    runtime = str(max(1, int(duration_timedelta.total_seconds() / 60)))
                                except (ValueError, TypeError):
                                    _LOGGER.warning(f"Invalid duration for video {video_id}: {duration}")

                            # Import necessary functions for image processing
                            from .sensor import process_image_to_portrait, process_image_to_fanart
                            
                            # Construct the video data dictionary with relevant details
                            video_data = {
                                'id': item['id'],
                                'snippet': item['snippet'],
                                'contentDetails': item['contentDetails'],
                                'statistics': item.get('statistics', {}),
                                'runtime': runtime,
                                'genres': ', '.join(' '.join(word.capitalize() for word in tag.split()) for tag in item['snippet'].get('tags', [])[:2]),
                                'live_status': live_status
                            }

                            # Extract thumbnails and live streaming details
                            thumbnails = item['snippet'].get('thumbnails', {})
                            live_streaming_details = item.get('liveStreamingDetails', {})
                            actual_end = live_streaming_details.get('actualEndTime')
                            actual_start_time = live_streaming_details.get('actualStartTime')
                            is_active_stream = bool(is_live_stream and actual_start_time and not actual_end)

                            if is_active_stream:
                                # Set the poster URL for active live streams
                                poster_url = f"https://i.ytimg.com/vi/{item['id']}/maxresdefault_live.jpg"
                            else:
                                # Set the poster URL for regular videos
                                poster_url = thumbnails.get('maxres', {}).get('url') or thumbnails.get('high', {}).get('url')

                            if poster_url:
                                # Attempt to fetch and process the poster image with retries
                                for attempt in range(3):
                                    try:
                                        async with aiohttp.ClientSession() as thumb_session:
                                            async with thumb_session.get(poster_url, timeout=aiohttp.ClientTimeout(total=30)) as thumb_resp:
                                                if thumb_resp.status == 200:
                                                    # Process the portrait image if the request is successful
                                                    video_id_data = {'videoId': item['id'], 'is_live': is_active_stream}
                                                    video_data['poster'] = await process_image_to_portrait(self.hass, poster_url, video_id_data)
                                                    _LOGGER.debug(f"Successfully processed poster for video {video_id} on attempt {attempt + 1}")
                                                    break
                                                elif thumb_resp.status == 404 and attempt < 2:
                                                    # Log and wait before retrying if the thumbnail is not found
                                                    _LOGGER.debug(
                                                        f"Thumbnail 404 for video {video_id}, URL: {poster_url}, attempt {attempt + 1}/3, "
                                                        f"waiting 30s before retry"
                                                    )
                                                    await asyncio.sleep(30)
                                                elif attempt < 2:
                                                    # Log and wait before retrying for other HTTP errors
                                                    _LOGGER.debug(
                                                        f"Thumbnail error for video {video_id}, URL: {poster_url}, status: {thumb_resp.status}, "
                                                        f"attempt {attempt + 1}/3, waiting 5s before retry"
                                                    )
                                                    await asyncio.sleep(5)
                                                else:
                                                    # Log an error after exhausting all retry attempts
                                                    # _LOGGER.error(f"Failed to fetch thumbnail after all retries for video {video_id}")
                                                    video_data['poster'] = (
                                                        "/local/youtube_thumbnails/default_poster.jpg" 
                                                        if not is_active_stream 
                                                        else f"/local/youtube_thumbnails/{item['id']}_live_portrait.jpg"
                                                    )
                                    except Exception as e:
                                        if attempt == 2:
                                            # Log a critical error if all retry attempts fail
                                            _LOGGER.error(f"Error processing poster for video {video_id}: {e}")
                                            video_data['poster'] = (
                                                "/local/youtube_thumbnails/default_poster.jpg" 
                                                if not is_active_stream 
                                                else f"/local/youtube_thumbnails/{item['id']}_live_portrait.jpg"
                                            )
                                        else:
                                            # Log the exception and wait before retrying
                                            _LOGGER.debug(f"Error on attempt {attempt + 1}/3 for video {video_id}: {e}, retrying in 5s")
                                            await asyncio.sleep(5)
                            else:
                                # Set a default poster if no poster URL is available
                                video_data['poster'] = (
                                    "/local/youtube_thumbnails/default_poster.jpg" 
                                    if not is_active_stream 
                                    else f"/local/youtube_thumbnails/{item['id']}_live_portrait.jpg"
                                )
                                _LOGGER.debug(f"Set default poster for video {video_id}")

                            try:
                                # Process fanart for the video
                                video_id_data = {'videoId': item['id'], 'is_live': is_active_stream}
                                video_data['fanart'] = await process_image_to_fanart(self.hass, video_id_data, thumbnails)
                                _LOGGER.debug(f"Successfully processed fanart for video {video_id}")
                            except Exception as e:
                                # Log an error if fanart processing fails and set a default fanart
                                _LOGGER.error(f"Error processing fanart for video {video_id}: {e}")
                                video_data['fanart'] = (
                                    "/local/youtube_thumbnails/default_fanart.jpg" 
                                    if not is_active_stream 
                                    else f"/local/youtube_thumbnails/{item['id']}_live_fanart.jpg"
                                )
                            
                            # Log that the summary processing is about to begin
                            # _LOGGER.critical(f"About to process summary for video {video_id}")
                            
                            if get_USE_COMMENTS_AS_SUMMARY():
                                try:
                                    # Log the attempt to fetch comments for the summary
                                    # _LOGGER.critical("Attempting to fetch comments for summary")
                                    
                                    # Fetch top comments asynchronously
                                    comments = await self.fetch_top_comments(item['id'])
                                    
                                    # Log the number of comments fetched
                                    # _LOGGER.critical(f"Fetched {len(comments)} comments")
                                    
                                    # Combine comments into a summary
                                    video_data['summary'] = '\n\n'.join(comments) if comments else ''
                                    
                                    # Log a snippet of the final summary
                                    # _LOGGER.critical(f"Final summary (first 100 chars): {video_data.get('summary', '')[:100]}")
                                except Exception as e:
                                    # Log an error if fetching comments fails and set an empty summary
                                    _LOGGER.error(f"Error processing comments for video {item['id']}: {e}")
                                    video_data['summary'] = ''
                            else:
                                # Log that the description is being used as the summary
                                _LOGGER.debug("Using description as summary (USE_COMMENTS_AS_SUMMARY is False)")
                                
                                # Create a summary from the video description
                                video_data['summary'] = '\n\n'.join(item['snippet'].get('description', '').split('\n\n')[:2]).strip()
                            
                            # Return the constructed video data
                            _LOGGER.debug(f"Returning video data for video ID: {video_id}")
                            return video_data
                        else:
                            # Log a warning if no video details are found for the given video ID
                            _LOGGER.warning(f"No video details found for video ID: {video_id}")
                            return None
                    elif response.status == 403:
                        # Handle quota exceeded error
                        _LOGGER.warning(f"Received 403 Forbidden for video ID: {video_id}. Handling quota exceeded.")
                        await self._handle_quota_exceeded(response)
                    elif response.status == 401:
                        # Handle unauthorized error by attempting to refresh the OAuth token
                        _LOGGER.warning("Access token expired or unauthorized during video details fetch. Attempting to refresh token.")
                        refresh_result = await self.refresh_oauth_token()
                        if refresh_result:
                            # Update the authorization header with the new token
                            headers["Authorization"] = f"Bearer {self.access_token}"
                            # Retry fetching the video details with the refreshed token
                            _LOGGER.debug(f"Retrying get_video_by_id for video ID: {video_id} after refreshing token")
                            return await self.get_video_by_id(video_id)
                        else:
                            # Log an error if refreshing the token fails
                            _LOGGER.error("Failed to refresh access token after 401 error during video details fetch.")
                    else:
                        # Log an error for other unexpected HTTP status codes
                        _LOGGER.error(f"Failed to fetch video details for {video_id}. Status: {response.status}, Error: {response_text}")
        except Exception as e:
            # Log any exceptions that occur during the HTTP request or processing
            _LOGGER.error(f"Exception while fetching video details for {video_id}: {e}")
        
        # Return None if video data could not be retrieved or processed
        _LOGGER.debug(f"Returning None for video ID: {video_id} due to earlier issues")
        return None

    async def _fetch_subscriptions(self, session, headers):
        current_time = datetime.now(ZoneInfo("UTC"))
        if self.quota_reset_time and current_time >= self.quota_reset_time:
            self.current_quota = 0
            await self._save_persistent_data()
            _LOGGER.info(f"Quota reset to 0. Next reset time: {self.quota_reset_time}")
        
        if self.subscriptions and self.last_subscription_update and (current_time - self.last_subscription_update).days < 1:
            _LOGGER.info("Using cached subscriptions data")
            return self.subscriptions

        _LOGGER.info("Fetching fresh subscriptions data from API")
        subscriptions = []
        page_token = None
        while True:
            url = f"https://www.googleapis.com/youtube/v3/subscriptions?part=snippet&mine=true&maxResults=50&key={self.api_key}"
            if page_token:
                url += f"&pageToken={page_token}"

            try:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        async with self.quota_lock:
                            self.current_quota += 1  # subscriptions.list costs 1 unit
                        _LOGGER.debug(f"Quota incremented by 1 unit. Current quota usage: {self.current_quota}")
                        await self._save_persistent_data()
                        channel_ids = [item['snippet']['resourceId']['channelId'] for item in data.get('items', [])]
                        subscriptions.extend(channel_ids)

                        page_token = data.get('nextPageToken')
                        if not page_token:
                            break
                    elif response.status == 401:
                        _LOGGER.warning("Access token expired or unauthorized during subscriptions fetch. Attempting to refresh token.")
                        refresh_result = await self.refresh_oauth_token()
                        if refresh_result:
                            headers["Authorization"] = f"Bearer {self.access_token}"
                            continue
                        else:
                            _LOGGER.error("Failed to refresh access token after 401 error during subscriptions fetch.")
                            break
                    elif response.status == 403:
                        await self._handle_quota_exceeded(response)
                        break
                    else:
                        _LOGGER.error(f"Failed to fetch subscriptions. Status: {response.status}")
                        break
            except Exception as e:
                _LOGGER.error(f"Error fetching subscriptions: {e}")
                break

        self.subscriptions = subscriptions
        self.last_subscription_update = current_time
        await self._save_persistent_data()
        return subscriptions

    async def _fetch_activities(self, session, headers, uploads_playlists):
        # Initialize lists to store regular videos and shorts
        regular_videos = []
        shorts = []
        
        # Load stored data asynchronously
        _LOGGER.debug("Loading stored data for fetching activities")
        stored_data = await self._store.async_load()
        oldest_video_date = None
        
        if stored_data:
            # Combine regular and shorts videos to determine the oldest video date
            all_videos = stored_data.get('data', []) + stored_data.get('shorts_data', [])
            publish_dates = [video.get('snippet', {}).get('publishedAt') for video in all_videos if video.get('snippet')]
            if publish_dates:
                oldest_video_date = min(
                    datetime.fromisoformat(date.rstrip('Z')).replace(tzinfo=ZoneInfo("UTC")) 
                    for date in publish_dates if date
                )
                _LOGGER.debug(f"Oldest video date determined: {oldest_video_date}")
        
        # Initialize sets and dictionaries to track video IDs and their corresponding items
        all_video_ids = set()
        video_id_to_item = {}
        
        # Iterate over each channel's uploads playlist, limited to the first 50
        for channel_id, playlist_id in list(uploads_playlists.items())[:50]:
            # Check if approaching API quota limit
            if self.current_quota >= 9900:
                _LOGGER.warning("Approaching quota limit. Stopping fetch.")
                break
            
            # Construct the YouTube API URL for playlist items
            playlist_url = f"https://www.googleapis.com/youtube/v3/playlistItems?part=snippet,contentDetails&maxResults=50&playlistId={playlist_id}&key={self.api_key}"
            _LOGGER.debug(f"Fetching playlist items from URL: {playlist_url}")
            
            retry_count = 0
            max_retries = 3
            while retry_count < max_retries:
                try:
                    # Set a timeout for the HTTP request
                    async with async_timeout.timeout(60):
                        # Make the GET request to the playlist URL
                        _LOGGER.debug(f"Attempting to fetch playlist {playlist_id}, try {retry_count + 1}/{max_retries}")
                        async with session.get(playlist_url, headers=headers) as response:
                            if response.status == 200:
                                # Parse the JSON response
                                data = await response.json()
                                _LOGGER.debug(f"Successfully fetched playlist {playlist_id}")
                                
                                # Increment quota usage and save persistent data
                                async with self.quota_lock:
                                    self.current_quota += 1
                                await self._save_persistent_data()
                                
                                # Process each item in the playlist
                                for item in data.get('items', []):
                                    video_published_at_str = item['snippet']['publishedAt']
                                    video_published_at = datetime.strptime(video_published_at_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=ZoneInfo("UTC"))
                                    
                                    current_utc_time = datetime.now(ZoneInfo("UTC"))
                                    
                                    # Skip videos published in the future
                                    if video_published_at > current_utc_time:
                                        _LOGGER.debug(f"Skipping video {item['snippet']['resourceId']['videoId']} published in the future at {video_published_at}")
                                        continue
                                    
                                    # Define midnight in Los Angeles timezone and convert to UTC
                                    midnight = datetime.now(ZoneInfo("America/Los_Angeles")).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(ZoneInfo("UTC"))
                                    
                                    # Check if the video was published after midnight LA time and is newer than the oldest stored video
                                    if video_published_at >= midnight and (not oldest_video_date or video_published_at >= oldest_video_date):
                                        video_id = item['snippet']['resourceId']['videoId']
                                        if video_id not in self.fetched_video_ids:
                                            # Add unique video IDs for further processing
                                            all_video_ids.add(video_id)
                                            video_id_to_item[video_id] = item
                                            _LOGGER.debug(f"Added video ID {video_id} for processing")
                                break  # Exit the retry loop on successful fetch
                            elif response.status == 403:
                                # Handle quota exceeded error
                                _LOGGER.warning(f"Received 403 Forbidden for playlist {playlist_id}. Handling quota exceeded.")
                                quota_exceeded = await self._handle_quota_exceeded(response)
                                if quota_exceeded:
                                    _LOGGER.warning("Quota exceeded while fetching playlist activities.")
                                    raise QuotaExceededException("Quota exceeded while fetching playlist activities.")
                                break
                            elif response.status == 401:
                                # Handle unauthorized error by refreshing the OAuth token
                                _LOGGER.warning("Access token expired or unauthorized during playlist fetch. Attempting to refresh token.")
                                refresh_result = await self.refresh_oauth_token()
                                if refresh_result:
                                    # Update the authorization header with the new token
                                    headers["Authorization"] = f"Bearer {self.access_token}"
                                    retry_count += 1
                                    _LOGGER.debug(f"Retrying playlist fetch for {playlist_id} after refreshing token")
                                    continue
                                else:
                                    _LOGGER.error("Failed to refresh access token after 401 error during playlist fetch.")
                                    break
                            else:
                                # Log unexpected HTTP status codes and retry
                                _LOGGER.warning(f"Unexpected status {response.status} for playlist {playlist_id}. Retrying.")
                                retry_count += 1
                except asyncio.TimeoutError:
                    # Handle timeout errors with retries
                    retry_count += 1
                    _LOGGER.warning(f"Timeout while fetching playlist {playlist_id}. Retry {retry_count}/{max_retries}")
                    if retry_count < max_retries:
                        await asyncio.sleep(1)
                    continue
                except Exception as e:
                    # Log any other exceptions that occur during the fetch
                    _LOGGER.error(f"Error fetching playlist {playlist_id}: {e}")
                    break
            
            # Small delay to prevent overwhelming the API
            await asyncio.sleep(0.1)
        
        _LOGGER.debug(f"Total unique video IDs fetched: {len(all_video_ids)}")
        
        # Prepare to check videos in batches
        video_ids_to_check = list(all_video_ids)
        batch_size = 50
        for i in range(0, len(video_ids_to_check), batch_size):
            batch = video_ids_to_check[i:i + batch_size]
            videos_url = f"https://www.googleapis.com/youtube/v3/videos?part=contentDetails,snippet,liveStreamingDetails&id={','.join(batch)}&key={self.api_key}"
            _LOGGER.debug(f"Fetching video details for batch {i // batch_size + 1}: {batch}")
            
            try:
                # Set a timeout for the HTTP request
                async with async_timeout.timeout(60):
                    # Make the GET request to the videos URL
                    async with session.get(videos_url, headers=headers) as response:
                        if response.status == 200:
                            # Parse the JSON response
                            data = await response.json()
                            items = data.get('items', [])
                            if not items:
                                _LOGGER.debug("No video details found in the current batch.")
                                continue
                            
                            # Increment quota usage and save persistent data
                            async with self.quota_lock:
                                self.current_quota += 2
                            await self._save_persistent_data()
                            _LOGGER.debug(f"Fetched details for {len(items)} videos in the current batch")
                            
                            # Update statistics and comments for the batch
                            stats_and_comments = await self.batch_update_video_statistics_and_comments(batch)
                            _LOGGER.debug(f"Fetched statistics and comments for batch {i // batch_size + 1}")
                            
                            # Process each video item
                            for item in items:
                                video_id = item['id']
                                duration = item['contentDetails'].get('duration', 'PT0M0S')
                                live_streaming_details = item.get('liveStreamingDetails', {})
            
                                live_broadcast_content = item['snippet'].get('liveBroadcastContent')
                                actual_start_time = live_streaming_details.get('actualStartTime')
                                actual_end = live_streaming_details.get('actualEndTime')
                                scheduled_start = live_streaming_details.get('scheduledStartTime')
                                
                                # Determine if the video is a live stream
                                is_live_stream = (
                                    live_broadcast_content in ['live', 'upcoming'] or
                                    duration in ['P0D', 'PT0M0S'] or
                                    actual_start_time is not None or
                                    scheduled_start is not None
                                ) and not actual_end

                                # Skip scheduled streams that haven't started
                                if is_live_stream and scheduled_start and not actual_start_time:
                                    _LOGGER.debug(f"Skipping scheduled stream: {video_id}")
                                    continue

                                # Skip streams where actualStartTime is in the future
                                if actual_start_time:
                                    actual_start_datetime = datetime.strptime(actual_start_time, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=ZoneInfo("UTC"))
                                    if actual_start_datetime > datetime.now(ZoneInfo("UTC")):
                                        _LOGGER.debug(f"Skipping stream {video_id} with future actualStartTime")
                                        continue
                                    
                                if actual_start_time and not actual_end:
                                    # Acquire lock to safely update fetched_video_ids
                                    async with self.fetched_video_ids_lock:
                                        if video_id not in self.fetched_video_ids:
                                            # Add video ID to fetched set to prevent reprocessing
                                            self.fetched_video_ids.add(video_id)
                                            live_status = ""
                                            runtime = ""
                                            
                                            if actual_start_time and not actual_end:
                                                # Retrieve and format the number of concurrent viewers
                                                concurrent_viewers = int(live_streaming_details.get('concurrentViewers', '0'))
                                                value = (
                                                    concurrent_viewers / 1000000 if concurrent_viewers >= 1000000 
                                                    else concurrent_viewers / 1000 if concurrent_viewers >= 1000 
                                                    else concurrent_viewers
                                                )
                                                suffix = 'M' if concurrent_viewers >= 1000000 else 'K' if concurrent_viewers >= 1000 else ''
                                                formatted_viewers = (
                                                    f"{value:.1f}".rstrip('0').rstrip('.') + suffix 
                                                    if concurrent_viewers >= 1000 
                                                    else str(concurrent_viewers)
                                                )
                                                
                                                # Set live status with viewer count
                                                if concurrent_viewers > 0:
                                                    live_status = f"🔴 LIVE - {formatted_viewers} Watching"
                                                else:
                                                    live_status = "🔴 LIVE - 🟩 Stream started"
                                                
                                                # Log the detection of an active live stream with viewer count
                                                _LOGGER.debug(f"Live stream detected for video {video_id} - Currently streaming with {concurrent_viewers} viewers")
                                            
                                            elif actual_end:
                                                # Set live status to indicate the stream has ended
                                                live_status = "📴 Stream ended"
                                                try:
                                                    # Calculate runtime based on actual start and end times
                                                    duration_timedelta = parse_duration(duration)
                                                    runtime = str(max(1, int(duration_timedelta.total_seconds() / 60)))
                                                    _LOGGER.debug(f"Runtime calculated for ended stream {video_id}: {runtime} minutes")
                                                except (ValueError, TypeError):
                                                    # Handle invalid duration formats and calculate runtime manually
                                                    runtime = str(
                                                        max(1, int(
                                                            (datetime.strptime(actual_end, '%Y-%m-%dT%H:%M:%SZ') - 
                                                            datetime.strptime(actual_start_time, '%Y-%m-%dT%H:%M:%SZ')
                                                            ).total_seconds() / 60
                                                        ))
                                                    )
                                                    _LOGGER.debug(f"Manual runtime calculation for ended stream {video_id}: {runtime} minutes")
                                            
                                            # Retrieve the snippet, preferring the stored item if available
                                            snippet = video_id_to_item.get(video_id, {}).get('snippet', item['snippet'])
                                            if actual_start_time and not actual_end and 'title' in snippet:
                                                # Filter and update the live stream title
                                                snippet = snippet.copy()
                                                snippet['title'] = filter_live_stream_title(snippet['title'])
                                                _LOGGER.debug(f"Filtered live stream title for video {video_id}")
                                            
                                            # Construct the video data dictionary
                                            video_data = {
                                                'id': video_id,
                                                'snippet': snippet,
                                                'contentDetails': item['contentDetails'],
                                                'duration': duration,
                                                'runtime': runtime,
                                                'live_status': live_status,
                                                'liveStreamingDetails': live_streaming_details
                                            }
                                            
                                            # Update statistics and summary if available
                                            if video_id in stats_and_comments:
                                                if 'statistics' not in video_data:
                                                    video_data['statistics'] = {}
                                                video_data['statistics'].update(stats_and_comments[video_id])
                                                if 'comments' in stats_and_comments[video_id]:
                                                    video_data['summary'] = stats_and_comments[video_id]['comments']
                                            else:
                                                # Set default statistics and summary if not available
                                                video_data['statistics'] = {
                                                    'viewCount': '0',
                                                    'likeCount': '0'
                                                }
                                                video_data['summary'] = '\n\n'.join(video_data['snippet'].get('description', '').split('\n\n')[:2]).strip()
                                                
                                            # Append the processed live stream video data to regular_videos
                                            regular_videos.append(video_data)
                                            _LOGGER.debug(f"Appended live stream video data for video {video_id} to regular_videos")
                                            continue
                                
                                try:
                                    # Parse the video's duration and convert it to seconds
                                    duration_timedelta = parse_duration(duration)
                                    duration_seconds = int(duration_timedelta.total_seconds())
                                    _LOGGER.debug(f"Duration seconds for video {video_id}: {duration_seconds}")
                                except (ValueError, TypeError):
                                    # Handle invalid duration formats and set duration_seconds to 0
                                    duration_seconds = 0
                                    _LOGGER.debug(f"Invalid duration format for video {video_id}: {duration}")
                                
                                # Acquire lock to safely update fetched_video_ids
                                async with self.fetched_video_ids_lock:
                                    if video_id not in self.fetched_video_ids:
                                        # Add video ID to fetched set to prevent reprocessing
                                        self.fetched_video_ids.add(video_id)
                                        if duration_seconds <= 60:
                                            # Classify the video as a short if its duration is 60 seconds or less
                                            _LOGGER.debug(f"Classifying video {video_id} as short (duration <= 60s)")
                                            video_data = {
                                                'id': video_id,
                                                'snippet': video_id_to_item.get(video_id, {}).get('snippet', item['snippet']),
                                                'contentDetails': item['contentDetails'],
                                                'duration': duration
                                            }
                                            
                                            # Update statistics and summary if available
                                            if video_id in stats_and_comments:
                                                if 'statistics' not in video_data:
                                                    video_data['statistics'] = {}
                                                video_data['statistics'].update(stats_and_comments[video_id])
                                                if 'comments' in stats_and_comments[video_id]:
                                                    video_data['summary'] = stats_and_comments[video_id]['comments']
                                            else:
                                                # Set default summary if comments are not available
                                                video_data['summary'] = '\n\n'.join(video_data['snippet'].get('description', '').split('\n\n')[:2]).strip()
                                                
                                            # Append the processed short video data to shorts
                                            shorts.append(video_data)
                                            _LOGGER.debug(f"Appended short video data for video {video_id} to shorts")
                                        elif duration_seconds <= 180:
                                            # Handle videos with duration between 61 and 180 seconds
                                            async def check_portrait(url):
                                                if not url:
                                                    _LOGGER.debug(f"No thumbnail URL provided for portrait check on video {video_id}")
                                                    return False
                                                try:
                                                    _LOGGER.debug(f"Fetching thumbnail from URL: {url} for portrait check on video {video_id}")
                                                    session = async_get_clientsession(self.hass)
                                                    async with session.get(url) as resp:
                                                        if resp.status != 200:
                                                            _LOGGER.debug(f"Failed to fetch image from URL: {url} with status {resp.status}")
                                                            return False
                                                        image_data = await resp.read()
                                                    
                                                    with Image.open(io.BytesIO(image_data)) as img:
                                                        width, height = img.size
                                                        _LOGGER.debug(f"Image dimensions for portrait check on video {video_id}: {width}x{height}")
                                                        if width == height:
                                                            _LOGGER.debug(f"Image is square for video {video_id}; skipping portrait check.")
                                                            return False
                                                        if width < height:
                                                            _LOGGER.debug(f"Image is in portrait orientation for video {video_id}.")
                                                            return True
                                                        
                                                        img_gray = img.convert('L')
                                                        third_width = width // 3
                                                        left_side = np.array(img_gray.crop((0, 0, third_width, height)))
                                                        center = np.array(img_gray.crop((third_width, 0, third_width * 2, height)))
                                                        right_side = np.array(img_gray.crop((third_width * 2, 0, width, height)))
                                                        left_mean = np.mean(left_side)
                                                        right_mean = np.mean(right_side)
                                                        center_mean = np.mean(center)
                                                        left_dark = np.percentile(left_side, 20)
                                                        right_dark = np.percentile(right_side, 20)
                                                        edges_darker = (left_mean < center_mean * 0.9 and right_mean < center_mean * 0.9)
                                                        
                                                        is_portrait = edges_darker
                                                        if is_portrait:
                                                            center_width = third_width
                                                            center_ratio = center_width / height
                                                            ratio_tolerance = 0.1
                                                            if any(x in url for x in ['hqdefault', 'sddefault', 'mqdefault', 'default']):
                                                                ratio_tolerance = 0.2
                                                            is_portrait = abs(center_ratio - (9/16)) <= ratio_tolerance
                                                        
                                                        _LOGGER.debug(f"Side means for portrait check - Left: {left_mean:.1f}, Center: {center_mean:.1f}, Right: {right_mean:.1f}")
                                                        _LOGGER.debug(f"Dark percentiles for portrait check - Left: {left_dark:.1f}, Right: {right_dark:.1f}")
                                                        _LOGGER.debug(f"Is portrait for video {video_id}: {is_portrait}")
                                                        
                                                        return is_portrait
                                                        
                                                except Exception as e:
                                                    _LOGGER.debug(f"Error in check_portrait for video {video_id}: {str(e)}")
                                                    return False
                                            
                                            # Retrieve the thumbnail URL for the portrait check
                                            thumb_url = item['snippet'].get('thumbnails', {}).get('maxres', {}).get('url') or item['snippet'].get('thumbnails', {}).get('high', {}).get('url')
                                            _LOGGER.debug(f"Checking portrait orientation for video {video_id} with duration <= 180s")
                                            
                                            portrait_result = False
                                            if thumb_url:
                                                portrait_result = await check_portrait(thumb_url)
                                                if portrait_result is None:
                                                    # Thumbnail fetch failed, check title for #shorts
                                                    title = video_data.get('snippet', {}).get('title', '').lower()
                                                    if '#shorts' in title or '#short' in title:
                                                        portrait_result = True
                                                        _LOGGER.debug(f"Classifying video {video_id} as short based on title tag")

                                            if portrait_result:
                                                # Classify the video as a short with portrait orientation
                                                _LOGGER.debug(f"Classifying video {video_id} as short (portrait orientation)")
                                                video_data = {
                                                    'id': video_id,
                                                    'snippet': video_id_to_item.get(video_id, {}).get('snippet', item['snippet']),
                                                    'contentDetails': item['contentDetails'],
                                                    'duration': duration
                                                }
                                                
                                                # Update statistics and summary if available
                                                if video_id in stats_and_comments:
                                                    if 'statistics' not in video_data:
                                                        video_data['statistics'] = {}
                                                    video_data['statistics'].update(stats_and_comments[video_id])
                                                    if 'comments' in stats_and_comments[video_id]:
                                                        video_data['summary'] = stats_and_comments[video_id]['comments']
                                                else:
                                                    # Set default summary if comments are not available
                                                    video_data['summary'] = '\n\n'.join(video_data['snippet'].get('description', '').split('\n\n')[:2]).strip()
                                                    
                                                # Append the processed short video data to shorts
                                                shorts.append(video_data)
                                                _LOGGER.debug(f"Appended short video data for video {video_id} to shorts")
                                            else:
                                                # Classify the video as a regular video if not in portrait orientation
                                                _LOGGER.debug(f"Classifying video {video_id} as regular video (not portrait)")
                                                video_data = {
                                                    'id': video_id,
                                                    'snippet': video_id_to_item.get(video_id, {}).get('snippet', item['snippet']),
                                                    'contentDetails': item['contentDetails'],
                                                    'duration': duration,
                                                    'runtime': str(max(1, int(duration_seconds / 60)))
                                                }
                                                
                                                # Update statistics and summary if available
                                                if video_id in stats_and_comments:
                                                    if 'statistics' not in video_data:
                                                        video_data['statistics'] = {}
                                                    video_data['statistics'].update(stats_and_comments[video_id])
                                                    if 'comments' in stats_and_comments[video_id]:
                                                        video_data['summary'] = stats_and_comments[video_id]['comments']
                                                else:
                                                    # Set default statistics and summary if not available
                                                    video_data['statistics'] = {
                                                        'viewCount': '0',
                                                        'likeCount': '0'
                                                    }
                                                    video_data['summary'] = '\n\n'.join(video_data['snippet'].get('description', '').split('\n\n')[:2]).strip()
                                                    
                                                # Append the processed regular video data to regular_videos
                                                regular_videos.append(video_data)
                                                _LOGGER.debug(f"Appended regular video data for video {video_id} to regular_videos")
                                    else:
                                        # Classify the video as a regular video if duration is greater than 180 seconds
                                        _LOGGER.debug(f"Classifying video {video_id} as regular video (duration > 180s)")
                                        video_data = {
                                            'id': video_id,
                                            'snippet': video_id_to_item.get(video_id, {}).get('snippet', item['snippet']),
                                            'contentDetails': item['contentDetails'],
                                            'duration': duration,
                                            'runtime': str(max(1, int(duration_seconds / 60)))
                                        }
                                        
                                        # Update statistics and summary if available
                                        if video_id in stats_and_comments:
                                            if 'statistics' not in video_data:
                                                video_data['statistics'] = {}
                                            video_data['statistics'].update(stats_and_comments[video_id])
                                            if 'comments' in stats_and_comments[video_id]:
                                                video_data['summary'] = stats_and_comments[video_id]['comments']
                                        else:
                                            # Set default statistics and summary if not available
                                            video_data['statistics'] = {
                                                'viewCount': '0',
                                                'likeCount': '0'
                                            }
                                            video_data['summary'] = '\n\n'.join(video_data['snippet'].get('description', '').split('\n\n')[:2]).strip()
                                            
                                        # Append the processed regular video data to regular_videos
                                        regular_videos.append(video_data)
                                        _LOGGER.debug(f"Appended regular video data for video {video_id} to regular_videos")
                        elif response.status == 403:
                            # Handle quota exceeded error
                            _LOGGER.warning(f"Received 403 Forbidden for videos batch fetch. Handling quota exceeded.")
                            quota_exceeded = await self._handle_quota_exceeded(response)
                            if quota_exceeded:
                                _LOGGER.warning("Quota exceeded while fetching video details.")
                                raise QuotaExceededException("Quota exceeded while fetching video details.")
                        elif response.status == 401:
                            # Handle unauthorized error by refreshing the OAuth token
                            _LOGGER.warning("Access token expired or unauthorized during video details fetch. Attempting to refresh token.")
                            refresh_result = await self.refresh_oauth_token()
                            if refresh_result:
                                # Update the authorization header with the new token
                                headers["Authorization"] = f"Bearer {self.access_token}"
                                _LOGGER.debug(f"Retrying video details fetch for batch {i // batch_size + 1} after refreshing token")
                                continue
                            else:
                                _LOGGER.error("Failed to refresh access token after 401 error during video details fetch.")
                        else:
                            # Log unexpected HTTP status codes
                            _LOGGER.error(f"Failed to fetch video details for batch {i // batch_size + 1}. Status: {response.status}")
            except asyncio.TimeoutError:
                # Handle timeout errors during video batch processing
                _LOGGER.warning(f"Timeout while fetching video details for batch {i // batch_size + 1}. Continuing to next batch.")
                continue
            except Exception as e:
                # Log any other exceptions that occur during video batch processing
                _LOGGER.error(f"Error processing video batch {i // batch_size + 1}: {e}")
                continue
            
            # Small delay to prevent overwhelming the API
            await asyncio.sleep(0.1)
        
        # Log the total number of regular videos and shorts fetched
        _LOGGER.debug(f"Finished fetching activities. Total regular videos: {len(regular_videos)}, Total shorts: {len(shorts)}")
        
        # Return the collected regular videos and shorts
        return {"data": regular_videos, "shorts_data": shorts}

    async def _handle_quota_exceeded(self, response):
        # _LOGGER.critical("Entering _handle_quota_exceeded method")
        error = await response.json()
        _LOGGER.error(f"Full error response: {error}")
        if error.get("error", {}).get("errors"):
            reason = error["error"]["errors"][0].get("reason")
            _LOGGER.error(f"Error reason: {reason}")
            if reason == "quotaExceeded":
                self.quota_reset_time = self._calculate_quota_reset_time()
                self.current_quota = 10000
                _LOGGER.warning(f"YouTube API quota exceeded. Quota reset scheduled at {self.quota_reset_time}")
                await self._save_persistent_data(force=True)
                message = f"YouTube API quota exceeded. Further attempts will resume after the quota resets at {self.quota_reset_time}"
                persistent_notification.async_create(
                    self.hass,
                    message,
                    title="YouTube API Quota Exceeded",
                    notification_id="youtube_quota_exceeded"
                )
                return True
            else:
                _LOGGER.warning(f"Received error, but not quota exceeded. Reason: {reason}")
        _LOGGER.debug("Exiting _handle_quota_exceeded method")
        return False

    async def fetch_video_details(self, video_ids, session, headers):
        # _LOGGER.debug(f"Fetching video details for {len(video_ids)} videos")
        if not video_ids:
            _LOGGER.debug("No video IDs provided. Skipping video details fetch.")
            return []

        detailed_videos = []
        batch_size = 50
        batched_video_ids = [video_ids[i:i + batch_size] for i in range(0, len(video_ids), batch_size)]

        for batch in batched_video_ids:
            batch_videos_url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,contentDetails,statistics&id={','.join(batch)}&key={self.api_key}"

            try:
                async with session.get(batch_videos_url, headers=headers, timeout=60) as response:
                    if response.status == 200:
                        videos_result = await response.json()
                        items = videos_result.get("items", [])
                        if not items:
                            _LOGGER.warning(f"No video details found for video IDs: {batch}")
                            continue
                        async with self.quota_lock:
                            self.current_quota += 1  # videos.list costs 1 unit
                        _LOGGER.debug(f"Quota incremented by 1 unit. Current quota usage: {self.current_quota}")
                        await self._save_persistent_data()
                        for item in items:
                            video_id = item['id']
                            duration = item['contentDetails'].get('duration', 'PT0M0S')
                            try:
                                duration_timedelta = parse_duration(duration)
                                runtime = max(1, int(duration_timedelta.total_seconds() / 60))
                            except (ValueError, TypeError):
                                _LOGGER.warning(f"Invalid duration for video {video_id}: {duration}")
                                runtime = 1

                            video_data = {
                                'id': video_id,
                                'snippet': item['snippet'],
                                'contentDetails': item['contentDetails'],
                                'statistics': item['statistics'],
                                'runtime': runtime,
                                'genres': ', '.join(' '.join(word.capitalize() for word in tag.split()) for tag in item['snippet'].get('tags', [])[:2])
                                # 'genres': ', '.join(item['snippet'].get('tags', [])[:2])
                            }
                            detailed_videos.append(video_data)
                    elif response.status == 403:
                        error_content = await response.text()
                        _LOGGER.warning(f"Received 403 status. Error content: {error_content}")
                        quota_exceeded = await self._handle_quota_exceeded(response)
                        if quota_exceeded:
                            _LOGGER.warning("Quota exceeded. Stopping fetch.")
                            return detailed_videos
                    elif response.status == 401:
                        _LOGGER.warning("Access token expired or unauthorized during video details fetch. Attempting to refresh token.")
                        refresh_result = await self.refresh_oauth_token()
                        if refresh_result:
                            headers["Authorization"] = f"Bearer {self.access_token}"
                            continue
                        else:
                            _LOGGER.error("Failed to refresh access token after 401 error during video details fetch.")
                            return detailed_videos
                    else:
                        error_message = await response.text()
                        _LOGGER.error(f"Failed to fetch video details batch. Status: {response.status}, error: {error_message}")
                        continue
            except asyncio.TimeoutError:
                _LOGGER.error("Timeout while fetching video details for batch. Continuing with the next batch.")
                continue
            except Exception as e:
                _LOGGER.error(f"Unexpected error in fetch_video_details: {e}")
                continue

        _LOGGER.debug(f"Fetched details for {len(detailed_videos)} videos")
        return detailed_videos

    async def subscribe_to_pubsub(self, callback_url):
        # _LOGGER.critical("Entering subscribe_to_pubsub method")
        # _LOGGER.critical(f"Current channel_subscription_times: {self.channel_subscription_times}")
        # _LOGGER.critical(f"Number of channel_ids: {len(self.channel_ids)}, Number of cached subscriptions: {len(self.channel_subscription_times)}")
        hub_url = "https://pubsubhubbub.appspot.com/subscribe"

        if not self.channel_ids:
            _LOGGER.error("No channel IDs are defined for subscription.")
            return False

        if not callback_url:
            _LOGGER.error("Callback URL is not defined.")
            return False

        current_time = datetime.now(ZoneInfo("UTC"))
        # _LOGGER.critical(f"Current time: {current_time}, Quota reset time: {self.quota_reset_time}, Current quota: {self.current_quota}")
        
        if self.current_quota >= 10000:
            _LOGGER.warning(f"API quota exceeded. Skipping PubSubHubbub subscription until reset at {self.quota_reset_time}.")
            return False

        # _LOGGER.critical(f"Starting PubSubHubbub subscription process. Channel IDs: {self.channel_ids}")

        subscription_results = []
        channels_to_subscribe = 0
        for channel_id in self.channel_ids:
            channel_id = channel_id.strip()
            last_subscription_time = self.channel_subscription_times.get(channel_id)
            
            if last_subscription_time:
                time_since_last_subscription = (current_time - last_subscription_time).total_seconds()
                # _LOGGER.critical(f"Channel {channel_id} last subscribed {time_since_last_subscription} seconds ago")
            else:
                time_since_last_subscription = None  # Ensure else block has an indented statement

            if last_subscription_time and (current_time - last_subscription_time).total_seconds() < 86000:  # 23 hours and 53 minutes
                # _LOGGER.critical(f"Subscription for channel {channel_id} is still valid. Skipping.")
                subscription_results.append((channel_id, True))
                continue

            channels_to_subscribe += 1
            topic_url = f"https://www.youtube.com/xml/feeds/videos.xml?channel_id={channel_id}"
            data = {
                'hub.callback': callback_url,
                'hub.topic': topic_url,
                'hub.verify': 'async',
                'hub.mode': 'subscribe',
                'hub.lease_seconds': '86400'
            }

            # _LOGGER.critical(f"Attempting to subscribe to channel {channel_id} with data: {data}")

            try:
                async with aiohttp.ClientSession() as session:
                    # _LOGGER.critical(f"Sending POST request to {hub_url} for channel {channel_id}")
                    async with session.post(hub_url, data=data, timeout=60) as response:
                        response_text = await response.text()
                        # _LOGGER.critical(f"Received response for channel {channel_id}: Status {response.status}, Body: {response_text}")
                        if response.status == 202:
                            # _LOGGER.critical(f"Successfully subscribed to PubSubHubbub for channel {channel_id}")
                            self.channel_subscription_times[channel_id] = current_time
                            subscription_results.append((channel_id, True))
                        else:
                            _LOGGER.error(f"Failed to subscribe to PubSubHubbub for channel {channel_id}. Status: {response.status}, Error: {response_text}")
                            subscription_results.append((channel_id, False))
            except asyncio.CancelledError:
                _LOGGER.debug(f"Subscription cancelled for channel {channel_id} during reload")
                return subscription_results
            except Exception as e:
                _LOGGER.error(f"Unexpected error in subscribe_to_pubsub for channel {channel_id}: {str(e)}")
                subscription_results.append((channel_id, False))

        # _LOGGER.critical(f"PubSubHubbub subscription process completed. Subscribed to {channels_to_subscribe} channels out of {len(self.channel_ids)}")
        # _LOGGER.critical(f"Subscription results summary: {len([r for r in subscription_results if r[1]])} successes, {len([r for r in subscription_results if not r[1]])} failures")
        # _LOGGER.critical(f"Final channel_subscription_times: {self.channel_subscription_times}")

        await self._save_persistent_data()
        return subscription_results

    async def unsubscribe_from_pubsub(self, callback_url):
        _LOGGER.debug("Entering unsubscribe_from_pubsub method")
        hub_url = "https://pubsubhubbub.appspot.com/subscribe"

        if not self.channel_ids:
            _LOGGER.error("No channel IDs are defined for unsubscription.")
            return

        if not callback_url:
            _LOGGER.error("Callback URL is not defined.")
            return

        # Ensure channel_ids is always a list
        for channel_id in self.channel_ids:
            channel_id = channel_id.strip()
            topic_url = f"https://www.youtube.com/xml/feeds/videos.xml?channel_id={channel_id}"

            data = {
                'hub.callback': callback_url,
                'hub.topic': topic_url,
                'hub.verify': 'async',
                'hub.mode': 'unsubscribe',
            }

            _LOGGER.debug(f"Attempting to unsubscribe from channel {channel_id} with data: {data}")

            for attempt in range(3):
                try:
                    async with aiohttp.ClientSession() as session:
                        _LOGGER.debug(f"Sending POST request to {hub_url} for channel {channel_id} (Attempt {attempt + 1}/3)")
                        async with session.post(hub_url, data=data, timeout=60) as response:
                            response_text = await response.text()
                            # _LOGGER.debug(f"Received response for channel {channel_id}: Status {response.status}, Body: {response_text}")

                            if response.status == 202:
                                _LOGGER.info(f"Successfully unsubscribed from PubSubHubbub for channel {channel_id}")
                                break  # Exit retry loop on success
                            elif 500 <= response.status < 600:
                                # Transient server error, retry
                                _LOGGER.error(f"Transient error unsubscribing from PubSubHubbub for channel {channel_id}. Status: {response.status}, Error: {response_text}")
                            elif response.status == 403 and 'quotaExceeded' in response_text:
                                self.quota_reset_time = self._calculate_quota_reset_time()
                                _LOGGER.warning(f"API quota exceeded. Unsubscription attempts will not proceed until {self.quota_reset_time}")
                                return  # Exit method as quota is exceeded
                            else:
                                # Non-retriable error
                                _LOGGER.error(f"Failed to unsubscribe from PubSubHubbub for channel {channel_id}. Status: {response.status}, Error: {response_text}")
                                break  # Exit retry loop on non-retriable error
                except asyncio.TimeoutError:
                    _LOGGER.error(f"Timeout error while unsubscribing from PubSubHubbub for channel {channel_id} (Attempt {attempt + 1}/3)")
                except aiohttp.ClientError as err:
                    _LOGGER.error(f"Network error unsubscribing from PubSubHubbub for channel {channel_id}: {err} (Attempt {attempt + 1}/3)")
                except Exception as e:
                    _LOGGER.error(f"Unexpected error in unsubscribe_from_pubsub for channel {channel_id}: {e} (Attempt {attempt + 1}/3)")

                if attempt < 2:
                    _LOGGER.debug(f"Retrying unsubscription for channel {channel_id} after 5 seconds")
                    await asyncio.sleep(5)  # Wait before retrying

        _LOGGER.debug("PubSubHubbub unsubscription process completed")
        await self._save_persistent_data()

    async def refresh_oauth_token(self):
        _LOGGER.debug("Entering refresh_oauth_token method")
        
        if self.permanent_token_failure:
            raise ConfigEntryAuthFailed("OAuth token permanently revoked. Please reconfigure the integration.")
        
        async with self.token_refresh_lock:
            # Re-check if token is still valid after acquiring lock
            if self.access_token and self.token_expiration and datetime.now(ZoneInfo("UTC")) < self.token_expiration - timedelta(minutes=5):
                _LOGGER.debug("Token became valid while waiting for lock")
                return self.access_token
            
            # DEBUG: Log refresh token operation start
            _LOGGER.debug(f"[TOKEN_REFRESH] Starting refresh for entry {self.config_entry.entry_id if self.config_entry else 'unknown'}")
            
            # Always reload refresh token from config to prevent race conditions
            if self.config_entry and self.config_entry.data.get(CONF_REFRESH_TOKEN):
                old_refresh_hash = hash(self.refresh_token) if self.refresh_token else None
                self.refresh_token = self.config_entry.data[CONF_REFRESH_TOKEN]
                new_refresh_hash = hash(self.refresh_token)
                if old_refresh_hash != new_refresh_hash:
                    _LOGGER.debug(f"[TOKEN_REFRESH] Refresh token updated from storage: {old_refresh_hash} -> {new_refresh_hash}")
                
            token_url = "https://oauth2.googleapis.com/token"
            data = {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
                "grant_type": "refresh_token"
            }

            try:
                async with aiohttp.ClientSession() as session:
                    _LOGGER.debug(f"Sending refresh request with token hash: {hash(self.refresh_token)}")
                    async with session.post(token_url, data=data, timeout=10) as response:
                        _LOGGER.debug(f"Refresh response status: {response.status}")
                        if response.status == 200:
                            result = await response.json()
                            new_access_token = result.get("access_token")
                            new_refresh_token = result.get("refresh_token")
                            expires_in = result.get("expires_in", 3600)
                            
                            _LOGGER.debug(f"Got new_refresh_token: {'Yes' if new_refresh_token else 'No'}")
                            
                            if not new_access_token:
                                raise ValueError("Received 200 status but no access token in response")
                            
                            self.access_token = new_access_token
                            self.token_expiration = datetime.now(ZoneInfo("UTC")) + timedelta(seconds=expires_in)
                            
                            # CRITICAL: Update refresh token if Google provided a new one
                            if new_refresh_token:
                                _LOGGER.info("[TOKEN_REFRESH] Google provided new refresh token - updating stored token")
                                old_refresh_token = self.refresh_token
                                
                                # Update config entry FIRST before changing instance variable
                                if self.config_entry:
                                    try:
                                        _LOGGER.debug(f"[TOKEN_REFRESH] Updating config entry {self.config_entry.entry_id} with new token")
                                        new_data = dict(self.config_entry.data)
                                        new_data[CONF_REFRESH_TOKEN] = new_refresh_token
                                        self.hass.config_entries.async_update_entry(
                                            self.config_entry,
                                            data=new_data
                                        )
                                        _LOGGER.debug("[TOKEN_REFRESH] Config entry updated successfully")
                                        # Only update instance variable after successful config update
                                        self.refresh_token = new_refresh_token
                                        # Force save to persistent storage immediately to prevent race conditions
                                        await self._save_persistent_data(force=True)
                                        _LOGGER.debug("[TOKEN_REFRESH] Persistent storage updated with new token")
                                    except Exception as e:
                                        _LOGGER.error(f"[TOKEN_REFRESH] Failed to update config entry with new refresh token: {e}")
                                        # Don't update instance variable if config update failed
                                        raise
                                else:
                                    # No config entry, just update instance variable
                                    self.refresh_token = new_refresh_token
                                    # Force save to persistent storage immediately to prevent race conditions
                                    await self._save_persistent_data(force=True)
                            else:
                                # Still save to ensure consistency even without new token
                                await self._save_persistent_data(force=True)
                            _LOGGER.debug(f"[TOKEN_REFRESH] Successfully refreshed access token, expires in {expires_in}s")
                            self.token_refresh_failures = 0
                            return new_access_token
                        else:
                            error_text = await response.text()
                            _LOGGER.error(f"[TOKEN_REFRESH] Token refresh failed with status {response.status}: {error_text}")
                            _LOGGER.debug(f"[TOKEN_REFRESH] Failed with refresh_token hash: {hash(self.refresh_token)}")
                            if response.status == 400 and "invalid_grant" in error_text:
                                self.token_refresh_failures += 1
                                _LOGGER.error(f"Invalid grant error #{self.token_refresh_failures}, refresh_token hash: {hash(self.refresh_token)}")
                                if self.token_refresh_failures >= 10:
                                    self.permanent_token_failure = True
                                    persistent_notification.async_create(
                                        self.hass,
                                        f"YouTube integration OAuth token has been revoked. Please reconfigure the integration.",
                                        title="YouTube Integration Authentication Failed",
                                        notification_id="youtube_auth_failed"
                                    )
                                    raise ConfigEntryAuthFailed(f"OAuth refresh token permanently revoked after {self.token_refresh_failures} attempts")
                            raise ValueError(f"Failed to refresh access token. Status: {response.status}, Error: {error_text}")
            except ConfigEntryAuthFailed:
                raise
            except Exception as e:
                _LOGGER.error(f"Error refreshing OAuth token: {str(e)}")
                self.access_token = None
                self.token_expiration = None
                await self._save_persistent_data()
                raise

    async def ensure_valid_token(self):
        try:
            _LOGGER.debug("Entering ensure_valid_token method")
            
            if self.permanent_token_failure:
                _LOGGER.error("Token marked as permanently failed")
                return None
                
            if not self.refresh_token:
                _LOGGER.error("No refresh token available")
                return None
            
            # Use a shared lock for token validation to prevent race conditions
            async with self.token_refresh_lock:
                current_time = datetime.now(ZoneInfo("UTC"))
                if self.token_expiration and current_time < self.token_expiration - timedelta(minutes=5):
                    if self.access_token:
                        _LOGGER.debug("Existing token is still valid")
                        return self.access_token
            
            _LOGGER.debug("Token expired or not set, attempting to refresh")
            return await self.refresh_oauth_token()
        except ConfigEntryAuthFailed:
            raise
        except Exception as e:
            _LOGGER.error(f"Failed to ensure valid token: {str(e)}")
            # Removed the immediate second refresh attempt that could trigger rate limiting
            return None
        
    async def fetch_user_channels(self, refresh_token):
        current_time = datetime.now(ZoneInfo("UTC"))
        _LOGGER.warning(f"Config entry during fetch_user_channels: {self.config_entry}")
        _LOGGER.warning("Debugging call stack - methods being called during fetch_user_channels")
        
        if self.quota_reset_time and current_time >= self.quota_reset_time:
            _LOGGER.warning("Resetting quota during fetch_user_channels")
            self.current_quota = 0
            await self._save_persistent_data()
            _LOGGER.info(f"Quota reset to 0. Next reset time: {self.quota_reset_time}")
        
        max_retries = 3
        retry_delay = 5  # seconds
        channel_ids = []

        for attempt in range(max_retries):
            try:
                # Step 1: Obtain a new access token using the refresh token
                _LOGGER.warning(f"Attempt {attempt + 1}: Starting token refresh")
                async with aiohttp.ClientSession() as session:
                    token_url = "https://oauth2.googleapis.com/token"
                    data = {
                        'client_id': self.client_id,
                        'client_secret': self.client_secret,
                        'refresh_token': refresh_token,
                        'grant_type': 'refresh_token'
                    }

                    _LOGGER.warning("Attempting to get access token")
                    async with session.post(token_url, data=data, timeout=10) as token_response:
                        if token_response.status != 200:
                            error_text = await token_response.text()
                            _LOGGER.warning(f"Token response error: Status {token_response.status} - {error_text}")
                            raise Exception(f"Failed to refresh access token: {token_response.status} - {error_text}")

                        token_data = await token_response.json()
                        access_token = token_data.get('access_token')
                        _LOGGER.warning("Successfully obtained access token")

                        if not access_token:
                            _LOGGER.warning("No access token in response data")
                            raise ValueError("No access token received in response")

                    _LOGGER.warning("Starting subscription fetch")
                    headers = {
                        'Authorization': f'Bearer {access_token}',
                        'Accept': 'application/json',
                    }

                    _LOGGER.warning(f"Current quota before API call: {self.current_quota}")
                    _LOGGER.warning(f"Quota reset time: {self.quota_reset_time}")

                    async with aiohttp.ClientSession() as session:
                        next_page_token = None
                        while True:
                            url = f'https://www.googleapis.com/youtube/v3/subscriptions?part=snippet&mine=true&maxResults=50'
                            if next_page_token:
                                url += f'&pageToken={next_page_token}'

                            _LOGGER.warning(f"Making API request to: {url}")
                            async with session.get(url, headers=headers, timeout=10) as response:
                                _LOGGER.warning(f"API response status: {response.status}")
                                if response.status == 200:
                                    data = await response.json()
                                    _LOGGER.warning(f"API response received. Items count: {len(data.get('items', []))}")
                                    _LOGGER.warning("About to increment quota")
                                    try:
                                        async with self.quota_lock:
                                            old_quota = self.current_quota
                                            self.current_quota += 5
                                        _LOGGER.warning(f"Quota incremented from {old_quota} to {self.current_quota}")
                                    except Exception as quota_error:
                                        _LOGGER.error(f"Error incrementing quota: {quota_error}")
                                        raise
                                    
                                    await self._save_persistent_data()
                                    channel_ids.extend([item['snippet']['resourceId']['channelId'] for item in data.get('items', [])])
                                    next_page_token = data.get('nextPageToken')
                                    if not next_page_token:
                                        _LOGGER.warning("No more pages to fetch")
                                        break
                                elif response.status == 401:
                                    _LOGGER.warning("Access token expired or unauthorized during channel fetch")
                                    refresh_result = await self.refresh_oauth_token()
                                    if refresh_result:
                                        _LOGGER.warning("Token refreshed successfully, continuing fetch")
                                        headers['Authorization'] = f'Bearer {self.access_token}'
                                        continue
                                    else:
                                        _LOGGER.error("Failed to refresh access token after 401 error")
                                        break
                                elif response.status == 403:
                                    _LOGGER.warning("Received 403 response, checking for quota exceeded")
                                    quota_exceeded = await self._handle_quota_exceeded(response)
                                    if quota_exceeded:
                                        raise QuotaExceededException(f"API quota exceeded. Further attempts will resume after the quota resets at {self.quota_reset_time}")
                                else:
                                    error_text = await response.text()
                                    _LOGGER.warning(f"Unexpected response: Status {response.status} - {error_text}")
                                    raise Exception(f"Failed to fetch subscriptions: {response.status} - {error_text}")

                        _LOGGER.warning(f"Fetch complete. Total channels found: {len(channel_ids)}")
                        if not channel_ids:
                            _LOGGER.warning("No subscribed channels found for the user")

                        return channel_ids

            except QuotaExceededException as qee:
                _LOGGER.warning(f"Quota exceeded during user channels fetch: {qee}")
                raise
            except Exception as e:
                _LOGGER.error(f"Error in fetch_user_channels (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    _LOGGER.warning(f"Retrying in {retry_delay} seconds")
                    await asyncio.sleep(retry_delay)
                else:
                    raise

        _LOGGER.error(f"Failed to fetch user channels after {max_retries} attempts")
        return channel_ids

    async def _fetch_uploads_playlists(self, session, headers, channel_ids):
        current_time = datetime.now(ZoneInfo("UTC"))
        if self.quota_reset_time and current_time >= self.quota_reset_time:
            self.current_quota = 0
            await self._save_persistent_data()
            _LOGGER.info(f"Quota reset to 0. Next reset time: {self.quota_reset_time}")
        
        uploads_playlists = {}
        channels_to_remove = []
        channels_to_fetch = [cid for cid in channel_ids if cid not in self.channel_playlist_mapping]
        
        for i in range(0, len(channels_to_fetch), 50):
            batch = channels_to_fetch[i:i+50]
            channels_url = f"https://www.googleapis.com/youtube/v3/channels?part=contentDetails&id={','.join(batch)}&key={self.api_key}"

            try:
                async with session.get(channels_url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        async with self.quota_lock:
                            self.current_quota += 1  # channels.list costs 1 unit
                        _LOGGER.debug(f"Quota incremented by 1 unit. Current quota usage: {self.current_quota}")
                        await self._save_persistent_data()
                        for item in data.get('items', []):
                            channel_id = item['id']
                            uploads_playlist_id = item['contentDetails']['relatedPlaylists']['uploads']
                            uploads_playlists[channel_id] = uploads_playlist_id
                            self.channel_playlist_mapping[channel_id] = uploads_playlist_id
                        missing_channels = set(batch) - set(item['id'] for item in data.get('items', []))
                        channels_to_remove.extend(missing_channels)
                    elif response.status == 403:
                        await self._handle_quota_exceeded(response)
                        break
                    elif response.status == 401:
                        _LOGGER.warning("Access token expired or unauthorized during channels fetch. Attempting to refresh token.")
                        refresh_result = await self.refresh_oauth_token()
                        if refresh_result:
                            headers["Authorization"] = f"Bearer {self.access_token}"
                            continue
                        else:
                            _LOGGER.error("Failed to refresh access token after 401 error during channels fetch.")
                            break
                    else:
                        error_content = await response.text()
                        _LOGGER.error(f"Failed to fetch channels. Status: {response.status}, Error: {error_content}")
                        _LOGGER.error(f"Problematic URL: {channels_url}")
            except Exception as e:
                _LOGGER.error(f"Error fetching channels: {str(e)}")
                _LOGGER.error(f"Problematic URL: {channels_url}")
                break

        if channels_to_remove:
            _LOGGER.warning(f"Removing non-existent channels: {channels_to_remove}")
            self.channel_ids = [cid for cid in self.channel_ids if cid not in channels_to_remove]
            await self._save_persistent_data()

        return {**self.channel_playlist_mapping, **uploads_playlists}

    async def _is_subscription_channel(self, channel_id, session, headers):
        if self.quota_reset_time and datetime.now(ZoneInfo("UTC")) < self.quota_reset_time:
            _LOGGER.info(f"API quota exceeded. Skipping channel check for {channel_id} until reset at {self.quota_reset_time}.")
            return False

        # Fetch channel details with brandingSettings
        url = f"https://www.googleapis.com/youtube/v3/channels?part=brandingSettings,topicDetails&id={channel_id}&key={self.api_key}"
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    async with self.quota_lock:
                        self.current_quota += 5  # channels.list with part=brandingSettings,topicDetails costs 5 units
                    _LOGGER.debug(f"Quota incremented by 5 units. Current quota usage: {self.current_quota}")
                    await self._save_persistent_data()
                    data = await response.json()
                    items = data.get('items', [])
                    if items:
                        # Check for keywords in channel title
                        branding_settings = items[0].get('brandingSettings', {})
                        channel_title = branding_settings.get('channel', {}).get('title', '').lower()
                        if 'music' in channel_title or 'movies' in channel_title:
                            return True
                        # Check for specific topic categories
                        topic_details = items[0].get('topicDetails', {})
                        topic_categories = topic_details.get('topicCategories', [])
                        for category in topic_categories:
                            if 'music' in category.lower() or 'film' in category.lower():
                                return True
                elif response.status == 403:
                    quota_exceeded = await self._handle_quota_exceeded(response)
                    if quota_exceeded:
                        raise QuotaExceededException(f"API quota exceeded. Further attempts will resume after the quota resets at {self.quota_reset_time}")
                elif response.status == 401:
                    _LOGGER.warning("Access token expired or unauthorized during channel check. Attempting to refresh token.")
                    refresh_result = await self.refresh_oauth_token()
                    if refresh_result:
                        headers["Authorization"] = f"Bearer {self.access_token}"
                        return await self._is_subscription_channel(channel_id, session, headers)
                    else:
                        _LOGGER.error("Failed to refresh access token after 401 error during channel check.")
                else:
                    _LOGGER.warning(f"Failed to fetch details for channel {channel_id}. Status: {response.status}")
        except QuotaExceededException:
            raise
        except Exception as e:
            _LOGGER.error(f"Unexpected error checking subscription channel {channel_id}: {e}")
        
        return False

    async def batch_update_video_statistics_and_comments(self, video_ids):
        """
        Batch update video statistics and comments for a list of video IDs.

        Args:
            video_ids (list): List of YouTube video IDs to update.

        Returns:
            dict: A dictionary containing updated statistics and comments for each video.
        """
        # Log the initiation of the batch update method
        # _LOGGER.critical("batch_update_video_statistics_and_comments method called")
        
        # Check if there are video IDs to process and if the quota is not exceeded
        if not video_ids:
            _LOGGER.debug(f"Skipping updates - Empty IDs: {not video_ids}")
            return {}
            
        if self.current_quota >= 10000:
            _LOGGER.warning(f"Skipping updates - Quota: {self.current_quota}")
            message = f"API quota exceeded. Further attempts will resume after the quota resets at {self.quota_reset_time}"
            persistent_notification.async_create(
                self.hass,
                message,
                title="YouTube API Quota Exceeded",
                notification_id="youtube_quota_exceeded"
            )
            raise QuotaExceededException(message)
        
        # Check if we're in a permanent token failure state
        if self.permanent_token_failure:
            _LOGGER.debug("Skipping batch update due to permanent token failure")
            return {}
        
        # Ensure we have a valid access token before making API calls
        try:
            valid_token = await self.ensure_valid_token()
            if not valid_token:
                _LOGGER.error("Failed to ensure valid access token in batch_update_video_statistics_and_comments")
                return {}
        except ConfigEntryAuthFailed:
            _LOGGER.error("Auth failed in batch_update_video_statistics_and_comments - refresh token is invalid")
            return {}
        
        # Log the number of videos being processed
        # _LOGGER.critical(f"Processing {len(video_ids)} videos")
        
        # Initialize a dictionary to store video statistics and comments
        video_stats = {}
        batch_size = 50  # Define the batch size for API requests
        
        # Create an asynchronous HTTP session
        async with aiohttp.ClientSession() as session:
            # Ensure token is fresh before starting batch processing
            try:
                fresh_token = await self.ensure_valid_token()
                if not fresh_token:
                    _LOGGER.error("Failed to ensure valid token before batch processing")
                    return {}
            except ConfigEntryAuthFailed:
                _LOGGER.error("Auth failed before batch processing - refresh token is invalid")
                return {}
            
            headers = {"Authorization": f"Bearer {self.access_token}"}
            
            # Iterate over the video IDs in batches
            for i in range(0, len(video_ids), batch_size):
                batch = video_ids[i:i + batch_size]
                # _LOGGER.critical(f"Processing batch {i // batch_size + 1} with {len(batch)} videos")
                
                # Construct the YouTube API URL for fetching video details
                videos_url = (
                    f"https://www.googleapis.com/youtube/v3/videos?"
                    f"part=statistics,contentDetails,liveStreamingDetails,snippet&"
                    f"id={','.join(batch)}&key={self.api_key}"
                )
                
                try:
                    # Make the GET request to the YouTube API
                    async with session.get(videos_url, headers=headers) as response:
                        # _LOGGER.critical(f"Video stats API response status: {response.status}")
                        
                        if response.status == 200:
                            # Parse the JSON response
                            data = await response.json()
                            items = data.get('items', [])
                            
                            # Increment quota usage and save persistent data
                            async with self.quota_lock:
                                self.current_quota += 1
                            await self._save_persistent_data()
                            
                            # Iterate over each video item in the response
                            for item in items:
                                video_id = item['id']
                                # _LOGGER.critical(f"Processing video {video_id}")
                                
                                # Initialize the statistics dictionary for the video
                                video_stats[video_id] = {
                                    **item.get('statistics', {}),
                                    'liveStreamingDetails': item.get('liveStreamingDetails', {})
                                }
                                
                                # Ensure viewCount and likeCount are present
                                video_stats[video_id]['viewCount'] = video_stats[video_id].get('viewCount', '0')
                                video_stats[video_id]['likeCount'] = video_stats[video_id].get('likeCount', '0')
                                
                                # Extract live streaming details
                                live_streaming_details = item.get('liveStreamingDetails', {})
                                video_stats[video_id]['liveStreamingDetails'] = live_streaming_details
                                actual_start_time = live_streaming_details.get('actualStartTime')
                                actual_end_time = live_streaming_details.get('actualEndTime')
                                
                                # Log live streaming details
                                # _LOGGER.critical(f"Live streaming details for {video_id}: {live_streaming_details}")
                                
                                # Determine if the video is currently live
                                if actual_start_time and not actual_end_time:
                                    # _LOGGER.critical(f"Video {video_id} is live")
                                    # _LOGGER.critical(f"Full response item for live video: {item}")
                                    
                                    # Retrieve and format the number of concurrent viewers
                                    concurrent_viewers = int(live_streaming_details.get('concurrentViewers', '0'))
                                    value = (
                                        concurrent_viewers / 1000000 if concurrent_viewers >= 1000000
                                        else concurrent_viewers / 1000 if concurrent_viewers >= 1000
                                        else concurrent_viewers
                                    )
                                    suffix = 'M' if concurrent_viewers >= 1000000 else 'K' if concurrent_viewers >= 1000 else ''
                                    formatted_viewers = (
                                        f"{value:.1f}".rstrip('0').rstrip('.') + suffix
                                        if concurrent_viewers >= 1000
                                        else str(concurrent_viewers)
                                    )
                                    
                                    # Set live status with viewer count
                                    if concurrent_viewers > 0:
                                        live_status = f"🔴 LIVE - {formatted_viewers} Watching"
                                    else:
                                        live_status = "🔴 LIVE - 🟩 Stream started"
                                    
                                    # Update the live status and viewer count in the statistics
                                    video_stats[video_id]['live_status'] = live_status
                                    video_stats[video_id]['concurrentViewers'] = concurrent_viewers
                                    video_stats[video_id]['runtime'] = ""
                                    
                                    # Ensure the 'statistics' key exists
                                    if 'statistics' not in video_stats[video_id]:
                                        video_stats[video_id]['statistics'] = {}
                                
                                elif actual_end_time:
                                    # Handle the scenario where the live stream has ended
                                    # _LOGGER.critical(f"Video {video_id} stream ended")
                                    video_stats[video_id]['live_status'] = "📴 Stream ended"
                                    
                                    try:
                                        # Parse the video's duration and calculate runtime in minutes
                                        duration = item['contentDetails'].get('duration', 'PT0M0S')
                                        duration_timedelta = parse_duration(duration)
                                        runtime = str(max(1, int(duration_timedelta.total_seconds() / 60)))
                                    except (ValueError, TypeError):
                                        # If duration parsing fails, calculate runtime manually
                                        runtime = str(
                                            max(1, int(
                                                (datetime.strptime(actual_end_time, '%Y-%m-%dT%H:%M:%SZ') - 
                                                datetime.strptime(actual_start_time, '%Y-%m-%dT%H:%M:%SZ')
                                                ).total_seconds() / 60
                                            ))
                                        )
                                    
                                    # Update the runtime in the statistics
                                    video_stats[video_id]['runtime'] = runtime
                                
                                # Determine whether to use comments as summary
                                if get_USE_COMMENTS_AS_SUMMARY():
                                    # _LOGGER.critical(f"Fetching comments for video {video_id}")
                                    
                                    # Construct the YouTube API URL for fetching comments
                                    comments_url = (
                                        f"https://www.googleapis.com/youtube/v3/commentThreads?"
                                        f"part=snippet&videoId={video_id}&maxResults=5&order=relevance&key={self.api_key}"
                                    )
                                    
                                    # Make the GET request to fetch comments
                                    async with session.get(comments_url, headers=headers) as comments_response:
                                        # _LOGGER.critical(f"Comments API response status for {video_id}: {comments_response.status}")
                                        
                                        if comments_response.status == 200:
                                            # Parse the JSON response for comments
                                            comments_data = await comments_response.json()
                                            
                                            # Increment quota usage and save persistent data
                                            async with self.quota_lock:
                                                self.current_quota += 1
                                            await self._save_persistent_data()
                                            
                                            comments = []
                                            # Iterate over each comment item
                                            for comment_item in comments_data.get('items', []):
                                                snippet = comment_item.get('snippet', {}).get('topLevelComment', {}).get('snippet', {})
                                                author = snippet.get('authorDisplayName', '')
                                                text = snippet.get('textDisplay', '')
                                                
                                                if author and text:
                                                    # Clean and format the comment text
                                                    text = re.sub(r'[.,]?\s*<br\s*/?>\s*[.,]?', '\n', text, flags=re.IGNORECASE)
                                                    text = re.sub(r'<a\s+href=["\'](.*?)["\']\s*>(.*?)</a>', r'\1 \2', text, flags=re.IGNORECASE)
                                                    text = re.sub(r'<[^>]+>', '', text)
                                                    text = html.unescape(text)
                                                    text = text.strip()
                                                    
                                                    # Append the formatted comment
                                                    comments.append(f"{author}\n{text}")
                                            
                                            # Update the summary with fetched comments
                                            video_stats[video_id]['comments'] = '\n\n'.join(comments) if comments else ''
                                            # _LOGGER.critical(f"Added {len(comments)} comments for video {video_id}")
                                else:
                                    # Use the video description as the summary if comments are not used
                                    description = item.get('snippet', {}).get('description', '')
                                    video_stats[video_id]['comments'] = '\n\n'.join(description.split('\n\n')[:2]).strip()
                                    _LOGGER.debug(f"Using description as summary for video {video_id}")
                        
                        elif response.status == 401:
                            # Handle unauthorized error by refreshing token and retrying
                            _LOGGER.warning("Access token expired during batch statistics update. Attempting to refresh token.")
                            try:
                                refresh_result = await self.refresh_oauth_token()
                                if refresh_result:
                                    headers["Authorization"] = f"Bearer {self.access_token}"
                                    _LOGGER.debug(f"Retrying batch {i // batch_size + 1} after refreshing token")
                                    # Decrement i to retry the same batch in the next iteration
                                    i -= batch_size
                                    continue
                                else:
                                    _LOGGER.error("Failed to refresh access token during batch update")
                                    return video_stats
                            except ConfigEntryAuthFailed:
                                _LOGGER.error("Refresh token is invalid. Skipping batch update.")
                                return video_stats
                        elif response.status == 403:
                            # Handle quota exceeded error
                            _LOGGER.warning("Received 403 error, checking quota")
                            quota_exceeded = await self._handle_quota_exceeded(response)
                            if quota_exceeded:
                                raise QuotaExceededException("Quota exceeded while fetching video details.")
                        else:
                            # Log unexpected HTTP status codes
                            _LOGGER.warning(f"Unexpected API status: {response.status}")
                
                except Exception as e:
                    # Log any exceptions that occur during batch processing
                    _LOGGER.warning(f"Error processing batch: {str(e)}", exc_info=True)
            
            # Log the completion of the batch update process
            # _LOGGER.critical(f"Completed processing with {len(video_stats)} video updates")
            
            # Return the updated video statistics and comments
            return video_stats

    async def fetch_top_comments(self, video_id, max_results=5):
        # _LOGGER.critical(f"Starting fetch_top_comments for video {video_id}")
        # _LOGGER.critical(f"Current quota: {self.current_quota}, USE_COMMENTS_AS_SUMMARY: {get_USE_COMMENTS_AS_SUMMARY}")

        if self.current_quota >= 10000:
            _LOGGER.warning("Skipping comments fetch due to quota limit")
            return []
            
        try:
            # _LOGGER.critical("Ensuring valid token for comments fetch")
            await self.ensure_valid_token()
            
            headers = {"Authorization": f"Bearer {self.access_token}"}
            async with aiohttp.ClientSession() as session:
                # Check if video is live stream first
                video_url = f"https://www.googleapis.com/youtube/v3/videos?part=liveStreamingDetails&id={video_id}&key={self.api_key}"
                async with session.get(video_url, headers=headers) as video_response:
                    if video_response.status == 200:
                        video_data = await video_response.json()
                        live_details = video_data.get('items', [{}])[0].get('liveStreamingDetails', {})
                        if live_details.get('actualStartTime') and not live_details.get('actualEndTime'):
                            _LOGGER.debug(f"Skipping comments for active live stream {video_id}")
                            return []

                # Preliminary check for comment accessibility with part="id"
                access_check_url = f"https://www.googleapis.com/youtube/v3/commentThreads?part=id&videoId={video_id}&maxResults=1&key={self.api_key}"
                async with session.get(access_check_url, headers=headers) as access_response:
                    if access_response.status == 403:
                        # _LOGGER.critical(f"Insufficient permissions to fetch comments for video {video_id}")
                        return []

                url = f"https://www.googleapis.com/youtube/v3/commentThreads?part=snippet&videoId={video_id}&maxResults={max_results}&order=relevance&key={self.api_key}"
                
                # _LOGGER.critical(f"Making API request to fetch comments for video {video_id}")
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.current_quota += 1
                        await self._save_persistent_data()
                        
                        comments = []
                        for item in data.get('items', []):
                            snippet = item.get('snippet', {}).get('topLevelComment', {}).get('snippet', {})
                            author = snippet.get('authorDisplayName', '')
                            text = snippet.get('textDisplay', '')
                            
                            if author and text:
                                # Replace <br> tags with newline characters
                                text = re.sub(r'[.,]?\s*<br\s*/?>\s*[.,]?', '\n', text, flags=re.IGNORECASE)
                                # text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
                                
                                # Convert <a href="URL">Text</a> to 'URL Text'
                                text = re.sub(
                                    r'<a\s+href=["\'](.*?)["\']\s*>(.*?)</a>',
                                    r'\1 \2',
                                    text,
                                    flags=re.IGNORECASE
                                )

                                # Remove any remaining HTML tags
                                text = re.sub(r'<[^>]+>', '', text)
                                
                                # Unescape HTML entities
                                text = html.unescape(text)
                                
                                # Remove leading/trailing whitespace
                                text = text.strip()
                                
                                comments.append(f"{author}\n{text}")
                        # _LOGGER.critical(f"Successfully fetched {len(comments)} comments for video {video_id}")
                        if comments:
                            # _LOGGER.critical(f"First comment: {comments[0][:100]}...")
                            return comments
                    elif response.status == 403:
                        _LOGGER.warning(f"403 error fetching comments for video {video_id}")
                        await self._handle_quota_exceeded(response)
                        return []
                    else:
                        _LOGGER.warning(f"Error {response.status} fetching comments for video {video_id}")
                        return []
        except Exception as e:
            _LOGGER.warning(f"Exception fetching comments for video {video_id}: {e}")
            return []
