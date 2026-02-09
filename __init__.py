import logging
import os
import asyncio
from datetime import datetime, timedelta
import async_timeout

from homeassistant.helpers.event import async_track_time_interval
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store
from homeassistant.components.webhook import async_register, async_unregister, async_generate_path
from homeassistant.helpers.network import get_url
from homeassistant.exceptions import ConfigEntryNotReady, ConfigEntryAuthFailed
from homeassistant.components import persistent_notification

from .youtube_api import QuotaExceededException
from .const import DOMAIN, CONF_API_KEY, CONF_CLIENT_ID, CONF_CLIENT_SECRET, CONF_REFRESH_TOKEN, CONF_CHANNEL_IDS, WEBHOOK_ID, CALLBACK_PATH, CONF_MAX_REGULAR_VIDEOS, CONF_MAX_SHORT_VIDEOS, DEFAULT_MAX_REGULAR_VIDEOS, DEFAULT_MAX_SHORT_VIDEOS, CONF_FAVORITE_CHANNELS, CONF_FILTER_CHANNELS, DEFAULT_FILTER_CHANNELS, CONF_USE_COMMENTS, DEFAULT_USE_COMMENTS, get_USE_COMMENTS_AS_SUMMARY
from .youtube_api import YouTubeAPI, QuotaExceededException

import aiohttp
import xmltodict
from aiohttp import web


_LOGGER = logging.getLogger(__name__)
# _LOGGER.critical("YouTube Recently Added integration module loaded")

PLATFORMS = ["sensor"]

async def handle_webhook(hass: HomeAssistant, webhook_id: str, request):
    from zoneinfo import ZoneInfo
    # _LOGGER.critical("handle_webhook method invoked")
    # _LOGGER.critical(f"Webhook ID received: {webhook_id}")
    # _LOGGER.critical(f"Request method: {request.method}")
    from .const import get_USE_COMMENTS_AS_SUMMARY
    # _LOGGER.critical(f"USE_COMMENTS_AS_SUMMARY setting: {get_USE_COMMENTS_AS_SUMMARY()}")
    try:
        if not isinstance(hass.data.get(DOMAIN), dict):
            hass.data[DOMAIN] = {}
            _LOGGER.debug("Initialized hass.data[DOMAIN] as a dictionary")
        
        if webhook_id != WEBHOOK_ID:
            _LOGGER.error(f"Received webhook for unknown ID: {webhook_id}")
            return web.Response(status=400, text="Unknown webhook ID")
        
        if request.method == 'GET':
            hub_mode = request.query.get('hub.mode')
            hub_challenge = request.query.get('hub.challenge')
            hub_topic = request.query.get('hub.topic')
            
            # _LOGGER.critical(f"Processing GET request: hub.mode={hub_mode}, hub.challenge={hub_challenge}, hub.topic={hub_topic}")
            
            if hub_mode and hub_challenge:
                # _LOGGER.critical(f"Subscription verification successful. Mode: {hub_mode}, Challenge: {hub_challenge}")
                response = web.Response(text=hub_challenge)
                return response
            
            _LOGGER.error(f"Invalid GET request for webhook verification. Missing hub.mode or hub.challenge.")
            return web.Response(status=400, text="Invalid request")

        elif request.method == 'POST':
            data = await request.text()
            # _LOGGER.critical("Received POST data for webhook")
            try:
                xml_dict = xmltodict.parse(data, process_namespaces=True, namespaces={
                    'http://www.w3.org/2005/Atom': None,
                    'http://www.youtube.com/xml/schemas/2015': 'yt',
                    'http://purl.org/atompub/tombstones/1.0': 'at',
                })
                # _LOGGER.critical("Successfully parsed XML data")
            except Exception as e:
                _LOGGER.error(f"Error parsing XML data from webhook: {e}")
                return web.Response(status=400, text="Invalid XML")

            is_deleted = False
            video_id = None
            channel_id = None

            feed = xml_dict.get('feed', {})
            if 'at:deleted-entry' in feed:
                deleted_entry = feed['at:deleted-entry']
                video_id = deleted_entry.get('@ref', '').split(':')[-1]
                # _LOGGER.critical(f"Video deletion notification received for ID: {video_id}")
                is_deleted = True

                at_by = deleted_entry.get('at:by', {})
                channel_uri = at_by.get('uri', '').strip()
                if channel_uri:
                    channel_id = channel_uri.split('/')[-1]
            elif 'entry' in feed:
                entry = feed['entry']
                video_id = entry.get('yt:videoId', '').strip()
                channel_id = entry.get('yt:channelId', '').strip()
                # _LOGGER.critical(f"New video notification received. Video ID: {video_id}, Channel ID: {channel_id}")

            if not channel_id:
                _LOGGER.warning(f"Channel ID is missing for video ID: {video_id}. Cannot proceed.")
                return web.Response(status=400, text="Missing channel ID")

            coordinators = []
            matched_entry_ids = []
            for entry in hass.config_entries.async_entries(DOMAIN):
                entry_id = entry.entry_id
                youtube = hass.data[DOMAIN].get(entry_id)
                if youtube and (channel_id in youtube.channel_ids or channel_id in youtube.subscriptions):
                    coordinator = hass.data[DOMAIN].get(f"{entry_id}_coordinator")
                    if coordinator:
                        coordinators.append(coordinator)
                        matched_entry_ids.append(entry_id)
                        # _LOGGER.critical(f"Found matching coordinator for entry_id: {entry_id}")

            if coordinators:
                for coord, eid in zip(coordinators, matched_entry_ids):
                    coord.youtube.last_webhook_time = datetime.now(ZoneInfo("UTC"))
                    await coord.youtube._save_persistent_data()
                    # _LOGGER.critical(f"Processing webhook update for coordinator {eid}")
                    if not coord.last_update_success:
                        _LOGGER.warning(f"Coordinator {eid} not fully initialized. Scheduling delayed refresh.")
                        async def delayed_refresh(coord=coord, eid=eid):
                            await asyncio.sleep(5)
                            if not is_deleted and video_id not in {v['id'] for v in coord.data.get('data', [])} and video_id not in {v['id'] for v in coord.data.get('shorts_data', [])}:
                                try:
                                    await coord.handle_webhook_update(video_id=video_id, is_deleted=is_deleted)
                                    _LOGGER.debug(f"Delayed refresh completed for entry {eid} with video {video_id}")
                                except Exception as e:
                                    _LOGGER.error(f"Error during delayed refresh for entry {eid}: {e}", exc_info=True)
                        hass.async_create_task(delayed_refresh())
                    else:
                        async def process_webhook_async():
                            try:
                                await coord.handle_webhook_update(video_id=video_id, is_deleted=is_deleted)
                            except Exception as e:
                                _LOGGER.error(f"Error updating coordinator {eid}: {e}", exc_info=True)
                        hass.async_create_task(process_webhook_async())
            else:
                _LOGGER.warning(f"No matching coordinator found for channel {channel_id}")

            return web.Response(status=200, text="Webhook processed")

    except Exception as e:
        _LOGGER.error(f"Unexpected error processing webhook data: {e}", exc_info=True)
        return web.Response(status=500, text="Internal Server Error")

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    from zoneinfo import ZoneInfo
    # _LOGGER.critical("Starting async_setup_entry for YouTube Recently Added")
    # _LOGGER.critical(f"Entry data: {entry.data}")

    from .const import set_USE_COMMENTS_AS_SUMMARY
    use_comments = entry.options.get(CONF_USE_COMMENTS, DEFAULT_USE_COMMENTS)
    # _LOGGER.critical(f"Loading USE_COMMENTS_AS_SUMMARY setting from config: {use_comments}")
    set_USE_COMMENTS_AS_SUMMARY(use_comments)

    try:
        # _LOGGER.critical("Initializing YouTubeAPI")
        channel_ids = entry.data.get(CONF_CHANNEL_IDS, [])
        # _LOGGER.critical(f"Channel IDs from entry: {len(channel_ids)}")
        
        # Create copy of channel IDs to ensure we don't lose them
        youtube = YouTubeAPI(
            entry.data[CONF_API_KEY],
            entry.data[CONF_CLIENT_ID],
            entry.data[CONF_CLIENT_SECRET],
            entry.data.get(CONF_REFRESH_TOKEN),
            channel_ids.copy(),  # Pass a copy
            hass,
            entry
        )

        # Load persistent data but preserve channel IDs
        await youtube._load_persistent_data()
        if not youtube.channel_ids and channel_ids:
            _LOGGER.info("Restoring channel IDs after data load")
            youtube.channel_ids = channel_ids.copy()
            await youtube._save_persistent_data()
        _LOGGER.debug(f"Persistent data loaded. Current quota: {youtube.current_quota}, Reset time: {youtube.quota_reset_time}")

        _LOGGER.debug("Ensuring valid OAuth token")
        valid_token = await youtube.ensure_valid_token()
        if not valid_token:
            _LOGGER.error("Failed to ensure valid OAuth token during initialization")
            raise ConfigEntryAuthFailed("OAuth token is invalid or revoked. Please reconfigure the integration.")

        _LOGGER.debug("OAuth token validated successfully")

        init_result = await youtube.initialize()
        if not init_result:
            _LOGGER.error("Failed to initialize YouTube API")
            raise ConfigEntryNotReady("Failed to initialize YouTube API. Please check your API credentials and permissions.")

        hass.data.setdefault(DOMAIN, {})[entry.entry_id] = youtube
        hass.data[DOMAIN][f"{entry.entry_id}_tasks"] = []

        await youtube._save_persistent_data()
        # _LOGGER.critical(f"Persistent data saved")

        from .sensor import YouTubeDataUpdateCoordinator
        coordinator = YouTubeDataUpdateCoordinator(hass, youtube, entry)
        await coordinator.delete_orphaned_images({'data': []}, {'shorts_data': []})  # Clean orphaned files on startup

        # Store coordinator and forward to sensor platform FIRST to avoid HA's 60-second setup timeout.
        # Heavy I/O work (initial video fetch, coordinator refresh) runs in a background task afterward.
        hass.data[DOMAIN][f"{entry.entry_id}_coordinator"] = coordinator

        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

        # Deferred startup: fetch initial videos and refresh coordinator in background
        async def _deferred_initial_fetch():
            try:
                should_fetch = True
                fetch_time = datetime.now(ZoneInfo("UTC"))
                if hasattr(youtube, 'last_fetch_time') and youtube.last_fetch_time:
                    time_since_last_fetch = (fetch_time - youtube.last_fetch_time).total_seconds()
                    if time_since_last_fetch < 3600:  # 1 hour cooldown
                        _LOGGER.info(f"Skipping initial video fetch - last fetch was {time_since_last_fetch/60:.1f} minutes ago")
                        should_fetch = False

                if should_fetch:
                    await initial_video_fetch(coordinator)
                    youtube.last_fetch_time = datetime.now(ZoneInfo("UTC"))
                    await youtube._save_persistent_data()

                await coordinator.async_refresh()
            except Exception as e:
                _LOGGER.error(f"Error in deferred initial fetch: {e}", exc_info=True)

        hass.async_create_task(_deferred_initial_fetch())

        async def update_statistics_and_comments(now=None):
            # _LOGGER.critical("10-minute scheduled update triggered")
            if not coordinator or not coordinator.data:
                _LOGGER.warning("No coordinator data available for update")
                return
                
            all_videos = []
            if isinstance(coordinator.data.get('data'), list):
                all_videos.extend(coordinator.data['data'])
            if isinstance(coordinator.data.get('shorts_data'), list):
                all_videos.extend(coordinator.data['shorts_data'])
            if isinstance(coordinator.data.get('favorite_channels'), list):
                all_videos.extend(coordinator.data['favorite_channels'])
            
            video_ids = list({v['id'] for v in all_videos if isinstance(v, dict) and 'id' in v})
            # _LOGGER.critical(f"Found {len(video_ids)} videos to update in scheduled refresh")
            
            if video_ids:
                try:
                    # Check remaining quota before updating
                    current_time = datetime.now(ZoneInfo("America/Los_Angeles"))
                    hours_until_reset = (24 - current_time.hour) + (60 - current_time.minute) / 60.0
                    remaining_quota = max(0, 10000 - youtube.current_quota)

                    # Calculate if we can sustain current update rate until reset
                    updates_until_reset = max(1, hours_until_reset * 6)  # 6 updates per hour
                    if get_USE_COMMENTS_AS_SUMMARY():
                        # With comments: 1 unit per video for comments + 1 unit per 50 videos for stats
                        estimated_units_per_update = len(video_ids) + max(1, len(video_ids) // 50)
                    else:
                        # Without comments: just 1 unit per 50 videos for stats
                        estimated_units_per_update = max(1, len(video_ids) // 50)

                    projected_usage = updates_until_reset * estimated_units_per_update

                    # Only apply conservation if we'd exceed quota before reset
                    if projected_usage > remaining_quota * 0.9 and remaining_quota < 5000:  # 90% safety margin
                        # Prioritize updates: live streams first, then recent videos
                        live_videos = [v for v in all_videos if v.get('live_status', '').startswith('🔴')]
                        recent_videos = sorted([v for v in all_videos if not v.get('live_status', '').startswith('🔴')], 
                                            key=lambda x: x.get('snippet', {}).get('publishedAt', ''), reverse=True)
                        
                        # Calculate how many videos we can afford to update
                        units_per_video = 1.02 if get_USE_COMMENTS_AS_SUMMARY() else 0.02
                        safe_videos_per_update = max(1, int((remaining_quota * 0.9) / (updates_until_reset * units_per_video)))
                        
                        # Take live videos + most recent up to safe limit
                        priority_videos = (live_videos + recent_videos)[:safe_videos_per_update]
                        
                        original_count = len(video_ids)
                        video_ids = list({v['id'] for v in priority_videos if 'id' in v})
                        
                        # Detailed warning message
                        _LOGGER.warning(
                            f"YouTube API quota conservation active: Updating {len(video_ids)}/{original_count} videos "
                            f"({len(live_videos)} live streams + {len(video_ids) - len(live_videos)} recent videos). "
                            f"Quota: {youtube.current_quota}/10000 used, {remaining_quota} remaining. "
                            f"Time until reset: {hours_until_reset:.1f} hours (midnight PST). "
                            f"The integration will continue updating only live streams and recent videos until quota resets. "
                            f"To reduce quota usage, consider: 1) Disabling comment summaries in integration options, "
                            f"2) Reducing max videos/shorts in configuration, or 3) Waiting for automatic midnight reset."
                        )

                    stats_and_comments = {}
                    if video_ids:
                        try:
                            stats_and_comments = await youtube.batch_update_video_statistics_and_comments(video_ids)
                        except QuotaExceededException as qee:
                            _LOGGER.warning(f"Quota exceeded in 10-minute update: {qee}")
                            return
                    if stats_and_comments:
                        updates = 0
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
                                    updates += 1
                        # _LOGGER.critical(f"Scheduled update completed: {updates} videos updated")
                        if updates > 0:
                            coordinator.async_set_updated_data(coordinator.data)
                except Exception as e:
                    _LOGGER.error(f"Error in scheduled update: {str(e)}", exc_info=True)
        # Register the interval (ensure this is called)
        task1 = async_track_time_interval(hass, update_statistics_and_comments, timedelta(minutes=10))
        hass.data[DOMAIN][f"{entry.entry_id}_tasks"].append(task1)
        # _LOGGER.critical("Registered 10-minute update interval")

        async def handle_quota_reset(event):
            _LOGGER.info("Quota reset event received. Resuming API calls and webhook subscriptions.")
            await resume_after_quota_reset()

        async def resume_after_quota_reset():
            _LOGGER.debug("Resubscribing to PubSubHubbub and refreshing data after quota reset.")
            callback_url = f"{get_url(hass, prefer_external=True)}{async_generate_path(WEBHOOK_ID)}"
            # _LOGGER.critical(f"Webhook callback URL: {callback_url}")
            if youtube.current_quota < 10000:
                try:
                    await youtube.subscribe_to_pubsub(callback_url)
                    await youtube.schedule_subscription_renewal(callback_url)
                    _LOGGER.debug("Resumed API calls and webhook subscriptions after quota reset.")
                except asyncio.CancelledError:
                    _LOGGER.debug("Subscription setup cancelled during quota reset - this is expected during reload")
                except Exception as e:
                    _LOGGER.error(f"Error resuming subscriptions after quota reset: {e}")
            else:
                _LOGGER.warning("Quota still exceeds limit after reset. Cannot resubscribe.")

        entry.async_on_unload(hass.bus.async_listen("youtube_quota_reset", handle_quota_reset))

        try:
            async_unregister(hass, WEBHOOK_ID)
        except Exception:
            pass
            
        try:
            async_register(
                hass,
                DOMAIN,
                "YouTube Recently Added",
                WEBHOOK_ID,
                handle_webhook,
                allowed_methods=["GET", "POST"]
            )
            _LOGGER.debug("Webhook registered successfully")
        except Exception as e:
            _LOGGER.warning(f"Failed to register webhook: {e}")
        
        callback_url = f"{get_url(hass, prefer_external=True)}{async_generate_path(WEBHOOK_ID)}"

        # If user deleted and immediately re-added, cancel any in-flight
        # background unsubscribe task to prevent it from racing against
        # our new subscribe calls.
        old_unsub_task = hass.data[DOMAIN].pop("_unsubscribe_task", None)
        if old_unsub_task and not old_unsub_task.done():
            old_unsub_task.cancel()
            _LOGGER.debug("Cancelled previous background unsubscribe task to avoid race condition")

        # Defer PubSubHubbub subscription to a background task.
        # subscribe_to_pubsub makes an HTTP POST for EVERY channel and
        # can take 60-120+ seconds with many subscriptions.  Running it
        # synchronously blocks async_setup_entry, which blocks the config
        # flow from returning its CREATE_ENTRY result to the frontend,
        # causing the UI to display "Unknown error occurred" even though
        # the entry is created and everything works.
        async def _deferred_pubsub_subscription():
            try:
                if youtube.current_quota >= 10000:
                    _LOGGER.warning("API quota exceeded. Skipping PubSubHubbub subscription.")
                    return
                subscription_result = await youtube.subscribe_to_pubsub(callback_url)
                if subscription_result is not False:  # Check if subscription wasn't completely failed
                    youtube.last_subscription_renewal_time = datetime.now(ZoneInfo("UTC"))
                    await youtube._save_persistent_data()
                    await youtube.schedule_subscription_renewal(callback_url)
                    _LOGGER.debug("Scheduled subscription renewal successfully")
            except asyncio.CancelledError:
                _LOGGER.debug("Subscription setup cancelled - this is expected during reload")
            except Exception as e:
                _LOGGER.error(f"Error during deferred subscription setup: {e}")

        hass.async_create_task(_deferred_pubsub_subscription())

        async def save_quota_usage(now):
            await youtube._save_persistent_data()

        task2 = async_track_time_interval(hass, save_quota_usage, timedelta(minutes=15))
        hass.data[DOMAIN][f"{entry.entry_id}_tasks"].append(task2)

        async def update_sensors(now):
            await coordinator.async_request_refresh()

        task3 = async_track_time_interval(hass, update_sensors, timedelta(hours=6))
        hass.data[DOMAIN][f"{entry.entry_id}_tasks"].append(task3)

        entry.async_on_unload(entry.add_update_listener(update_listener))

        _LOGGER.info("YouTube Recently Added integration setup completed successfully")
        return True

    except ConfigEntryAuthFailed as auth_failed:
        _LOGGER.error(f"Authentication failed: {auth_failed}")

        # Retrieve the base URL (prefer_external=True ensures it uses the external URL if available)
        base_url = get_url(hass, prefer_external=True)

        # Manually construct the integration overview URL
        integration_url = f"{base_url}/config/integrations/integration/youtube_recently_added"

        # Create a persistent notification with the hyperlink as 'YouTube Recently Added'
        persistent_notification.async_create(
            hass,
            f"Please reconfigure [YouTube Recently Added]({integration_url}) integration. OAuth token is invalid or revoked.",
            title="YouTube Integration Authentication Failed",
            notification_id="youtube_auth_failed"
        )
        raise
    except ConfigEntryNotReady as not_ready:
        _LOGGER.error(f"Setup failed: {not_ready}")
        raise
    except asyncio.CancelledError:
        _LOGGER.debug("Setup cancelled - this is expected during reload")
        return True  # Return True to allow reload to complete
    except Exception as e:
        _LOGGER.exception(f"Unexpected error setting up YouTube Recently Added integration: {str(e)}")
        return False

async def initial_video_fetch(coordinator):
    """Fetch initial set of videos before setting up sensors."""
    _LOGGER.debug("Starting initial video fetch.")
    try:
        videos = await coordinator.youtube.get_recent_videos()
        if videos['data'] or videos['shorts_data'] or videos.get('favorite_channels', []):
            _LOGGER.debug(f"Initial video fetch retrieved {len(videos['data'])} regular videos and {len(videos['shorts_data'])} shorts.")
            
            # Get all video IDs from all three sensors
            all_video_ids = set()
            all_video_ids.update([v['id'] for v in videos['data']])
            all_video_ids.update([v['id'] for v in videos['shorts_data']])
            all_video_ids.update([v['id'] for v in videos.get('favorite_channels', [])])
            
            if all_video_ids:
                _LOGGER.debug(f"Updating statistics and comments for {len(all_video_ids)} videos")
                stats_and_comments = await coordinator.youtube.batch_update_video_statistics_and_comments(list(all_video_ids))
                
                if stats_and_comments:
                    # Function to update a single video with stats and comments
                    def update_video_with_stats(video):
                        video_id = video['id']
                        if video_id in stats_and_comments:
                            if 'statistics' not in video:
                                video['statistics'] = {}
                            video['statistics'].update({
                                'viewCount': stats_and_comments[video_id].get('viewCount', '0'),
                                'likeCount': stats_and_comments[video_id].get('likeCount', '0')
                            })
                            if 'live_status' in stats_and_comments[video_id]:
                                video['live_status'] = stats_and_comments[video_id]['live_status']
                            if get_USE_COMMENTS_AS_SUMMARY():
                                if 'comments' in stats_and_comments[video_id]:
                                    video['summary'] = stats_and_comments[video_id]['comments']
                            else:
                                video['summary'] = '\n\n'.join(video['snippet'].get('description', '').split('\n\n')[:2]).strip()

                    # Update all videos in all three sensors
                    for video in videos['data']:
                        update_video_with_stats(video)
                    for video in videos['shorts_data']:
                        update_video_with_stats(video)
                    for video in videos.get('favorite_channels', []):
                        update_video_with_stats(video)

            max_regular = coordinator.config_entry.options.get(CONF_MAX_REGULAR_VIDEOS, DEFAULT_MAX_REGULAR_VIDEOS)
            max_shorts = coordinator.config_entry.options.get(CONF_MAX_SHORT_VIDEOS, DEFAULT_MAX_SHORT_VIDEOS)
            
            coordinator.async_set_updated_data({
                "data": videos['data'][:max_regular],
                "shorts_data": videos['shorts_data'][:max_shorts],
                "favorite_channels": videos.get('favorite_channels', [])
            })
            _LOGGER.info("Initial video fetch completed successfully.")
        # else:
        #     _LOGGER.warning("Initial video fetch retrieved no videos.")
    except Exception as e:
        _LOGGER.error(f"Error during initial video fetch: {e}", exc_info=True)

async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    _LOGGER.debug("Unloading YouTube Recently Added integration with entry ID: %s", entry.entry_id)

    # Unregister the webhook immediately
    async_unregister(hass, WEBHOOK_ID)

    youtube = hass.data[DOMAIN].get(entry.entry_id)

    # Cancel the subscription renewal task immediately
    if youtube and youtube.subscription_renewal_task:
        youtube.subscription_renewal_task.cancel()
        try:
            await youtube.subscription_renewal_task
        except asyncio.CancelledError:
            _LOGGER.debug("Subscription renewal task cancelled successfully.")

    # Clean up tracked tasks immediately
    tasks = hass.data[DOMAIN].get(f"{entry.entry_id}_tasks", [])
    for task in tasks:
        if hasattr(task, 'cancel'):
            task.cancel()
        elif callable(task):
            try:
                task()
            except Exception as e:
                _LOGGER.debug(f"Error cleaning up task: {e}")

    # Unload platforms (removes all 3 sensors) — this is fast
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)

    # Clean up hass.data references
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id, None)
        hass.data[DOMAIN].pop(f"{entry.entry_id}_tasks", None)
        hass.data[DOMAIN].pop(f"{entry.entry_id}_coordinator", None)

    # Defer slow PubSubHubbub unsubscription to background.
    # This makes HTTP POSTs to every channel and can take minutes.
    if youtube:
        async def _deferred_unsubscribe():
            try:
                callback_url = f"{get_url(hass, prefer_external=True)}{async_generate_path(WEBHOOK_ID)}"
                _LOGGER.debug(f"Background: unsubscribing from PubSubHubbub with callback URL: {callback_url}")
                await youtube.unsubscribe_from_pubsub(callback_url)
                _LOGGER.debug("Background: unsubscribed from PubSubHubbub successfully")
            except Exception as e:
                _LOGGER.warning(f"Background: failed to unsubscribe from PubSubHubbub: {e}")
            try:
                await youtube._save_persistent_data()
            except Exception:
                pass

        # Store reference so async_setup_entry can cancel if user re-adds quickly
        hass.data.setdefault(DOMAIN, {})["_unsubscribe_task"] = hass.async_create_task(_deferred_unsubscribe())

    _LOGGER.debug("YouTube Recently Added integration unloaded: %s", unload_ok)
    return unload_ok

async def update_listener(hass: HomeAssistant, entry: ConfigEntry):
    """Handle options update."""
    coordinator = hass.data[DOMAIN][f"{entry.entry_id}_coordinator"]
    coordinator.config_entry = entry
    coordinator.favorite_channels = [channel.strip() for channel in entry.options.get(CONF_FAVORITE_CHANNELS, "").split(',') if channel.strip()]
    coordinator.filter_channels = [channel.strip().lower() for channel in entry.options.get(CONF_FILTER_CHANNELS, "").split(',') if channel.strip()]
    
    from .const import set_USE_COMMENTS_AS_SUMMARY, get_USE_COMMENTS_AS_SUMMARY
    use_comments = entry.options.get(CONF_USE_COMMENTS, DEFAULT_USE_COMMENTS)
    _LOGGER.debug(f"update_listener: Setting USE_COMMENTS_AS_SUMMARY to: {use_comments}")
    old_value = get_USE_COMMENTS_AS_SUMMARY()
    if old_value != use_comments:
        set_USE_COMMENTS_AS_SUMMARY(use_comments)
        
        # Update all videos in all sensors
        if coordinator.data:
            all_updates = 0
            all_videos = []
            for videos in [coordinator.data.get('data', []), coordinator.data.get('shorts_data', []), coordinator.data.get('favorite_channels', [])]:
                if videos:
                    all_videos.extend([v for v in videos if isinstance(v, dict) and 'snippet' in v])

            if all_videos:
                video_ids = [v['id'] for v in all_videos]
                stats_and_comments = await coordinator.youtube.batch_update_video_statistics_and_comments(video_ids)
                
                if stats_and_comments:
                    for videos in [coordinator.data.get('data', []), coordinator.data.get('shorts_data', []), coordinator.data.get('favorite_channels', [])]:
                        for video in videos:
                            video_id = video.get('id')
                            if video_id and video_id in stats_and_comments:
                                if 'statistics' not in video:
                                    video['statistics'] = {}
                                video['statistics'].update({
                                    'viewCount': stats_and_comments[video_id].get('viewCount', '0'),
                                    'likeCount': stats_and_comments[video_id].get('likeCount', '0')
                                })
                                if 'live_status' in stats_and_comments[video_id]:
                                    video['live_status'] = stats_and_comments[video_id]['live_status']
                                
                                # Update summary based on new setting
                                if use_comments:
                                    if 'comments' in stats_and_comments[video_id]:
                                        video['summary'] = stats_and_comments[video_id]['comments']
                                else:
                                    video['summary'] = '\n\n'.join(video['snippet'].get('description', '').split('\n\n')[:2]).strip()
                                all_updates += 1

                    if all_updates > 0:
                        coordinator.async_set_updated_data(coordinator.data)
                        
        await coordinator.async_request_refresh()
