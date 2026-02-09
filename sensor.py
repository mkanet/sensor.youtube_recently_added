import logging
import os
import io
import json
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from homeassistant.components.sensor import SensorEntity
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import CoordinatorEntity, DataUpdateCoordinator, UpdateFailed
from homeassistant.core import HomeAssistant, callback
from homeassistant.components import persistent_notification
from .const import DOMAIN, CONF_MAX_REGULAR_VIDEOS, CONF_MAX_SHORT_VIDEOS, DEFAULT_MAX_REGULAR_VIDEOS, DEFAULT_MAX_SHORT_VIDEOS, CONF_FAVORITE_CHANNELS, DEFAULT_FAVORITE_CHANNELS, CONF_FILTER_CHANNELS, DEFAULT_FILTER_CHANNELS, CONF_USE_COMMENTS, get_USE_COMMENTS_AS_SUMMARY
from dateutil import parser
import async_timeout
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from .youtube_api import YouTubeAPI, QuotaExceededException
import numpy as np
from PIL import Image, ImageDraw
import aiofiles
import isodate
from isodate import parse_duration
import re

_LOGGER = logging.getLogger(__name__)

async def check_portrait_unified(url, hass):
    """Unified portrait detection for YouTube Shorts."""
    if not url:
        return False
    try:
        session = async_get_clientsession(hass)
        async with session.get(url) as resp:
            if resp.status != 200:
                return False
            image_data = await resp.read()
        with Image.open(io.BytesIO(image_data)) as img:
            width, height = img.size
            if width == height:
                return False
            if width < height:
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
            return is_portrait
    except Exception:
        return False

def remove_black_bars(img, dark_threshold=35, padding_ratio_threshold=(0.35, 0.8)):
    """
    Remove black bars using the same detection logic as check_portrait.
    """
    width, height = img.size
    target_ratio = 9 / 16
    content_ratio = width / height

    if content_ratio > target_ratio:
        img_gray = img.convert('L')
        third_width = width // 3
        left_third = np.array(img_gray.crop((0, 0, third_width, height)))
        right_third = np.array(img_gray.crop((third_width * 2, 0, width, height)))
        dark_pixels_left = np.sum(left_third < dark_threshold) / left_third.size
        dark_pixels_right = np.sum(right_third < dark_threshold) / right_third.size

        if dark_pixels_left > 0.75 and dark_pixels_right > 0.75:
            left = third_width
            right = width - third_width
            cropped_img = img.crop((left, 0, right, height))
            return cropped_img

    return img

def replace_white_border_with_black(img, threshold=250):
    """
    Replace any white or near-white strip at the bottom of the image with black.
    """
    # Convert image to RGB if not already
    if img.mode != 'RGB':
        img = img.convert('RGB')
    
    img_np = np.array(img)
    
    # Check the bottom rows of pixels to see if they are white (or near-white)
    bottom_row = img_np[-1, :, :]  # The last row (bottom strip)
    if np.all(bottom_row > threshold):  # If all pixels in the bottom row are near white
        # Replace the bottom strip with black
        draw = ImageDraw.Draw(img)
        width, height = img.size
        # Draw a black rectangle at the bottom
        draw.rectangle([(0, height - 10), (width, height)], fill=(0, 0, 0))  # Adjust 10px as needed
    
    return img

async def process_image_to_portrait(hass: HomeAssistant, url: str, video_id) -> str:
    """
    Process the image to create a portrait thumbnail with a 2:3 aspect ratio.
    """
    save_dir = hass.config.path("www", "youtube_thumbnails")
    os.makedirs(save_dir, exist_ok=True)

    if isinstance(video_id, dict):
        if video_id.get('is_live'):
            file_name = f"{video_id.get('videoId', '')}_live_portrait.jpg"
        else:
            file_name = f"{video_id.get('videoId', '')}_portrait.jpg"
    else:
        file_name = f"{video_id}_portrait.jpg"
    file_path = os.path.join(save_dir, file_name)

    if not url:
        _LOGGER.error(f"No URL provided for video_id: {video_id}")
        return ""

    session = async_get_clientsession(hass)
    try:
        async with session.get(url) as resp:
            if resp.status == 404:
                _LOGGER.debug(f"Thumbnail not yet available for video {video_id} (404 response)")
                return url
            elif resp.status != 200:
                # _LOGGER.error(f"Failed to download image from URL: {url} with status code: {resp.status}")
                return url
            image_data = await resp.read()
    except Exception as e:
        _LOGGER.error(f"Exception occurred while fetching image from URL: {url}, error: {e}")
        return ""

    try:
        _LOGGER.debug(f"Processing thumbnail for video_id {video_id} from URL: {url}")
        with Image.open(io.BytesIO(image_data)) as img:
            _LOGGER.debug(f"Original image size for video_id {video_id}: {img.size}")

            if isinstance(video_id, dict) and video_id.get('is_live'):
                img_resized = img.resize((1280, 720), Image.LANCZOS)
                image_byte_array = io.BytesIO()
                img_resized.save(image_byte_array, format="JPEG", quality=100)
                async with aiofiles.open(file_path, "wb") as f:
                    await f.write(image_byte_array.getvalue())
                return f"/local/youtube_thumbnails/{file_name}"

            img_no_black_bars = remove_black_bars(img, dark_threshold=35, padding_ratio_threshold=(0.35, 0.8))

            # Extract the center 9:16 content using same logic as check_portrait
            width, height = img_no_black_bars.size
            target_ratio = 9 / 16
            content_ratio = width / height

            _LOGGER.debug(f"Initial crop - width: {width}, height: {height}, ratio: {content_ratio:.4f}")

			# Calculate dimensions for extracting the center content
            expected_width = height * target_ratio
            padding_total = width - expected_width
            if padding_total > 0 and 0.35 <= padding_total / width <= 0.8 and img_no_black_bars != img:
                # Calculate potential crop dimensions
                left = int(padding_total / 2)
                right = width - int(padding_total / 2)
                potential_width = right - left
                
                # Add check for 9:16 ratio of center content with dynamic tolerance
                center_ratio = potential_width / height
                ratio_tolerance = 0.1  # Default for maxresdefault
                if any(x in url for x in ['hqdefault', 'sddefault', 'mqdefault', 'default']):
                    ratio_tolerance = 0.2
                if abs(center_ratio - (9/16)) <= ratio_tolerance:  
                    cropped_img = img_no_black_bars.crop((left, 0, right, height))
                    _LOGGER.debug(f"Extracted center content: {cropped_img.size}")
                else:
                    # Fall back to regular image handling
                    if width >= height * target_ratio:
                        target_width = height * target_ratio
                        left = (width - target_width) // 2
                        right = left + target_width
                        cropped_img = img_no_black_bars.crop((left, 0, right, height))
                _LOGGER.debug(f"Extracted center content: {cropped_img.size}")
            else:
                # Regular image, crop to 9:16
                if width >= height * target_ratio:
                    target_width = height * target_ratio
                    left = (width - target_width) // 2
                    right = left + target_width
                    cropped_img = img_no_black_bars.crop((left, 0, right, height))
                    _LOGGER.debug(f"Extracted center content: {cropped_img.size}")
                else:
                    target_height = width / target_ratio
                    top = (height - target_height) // 2
                    bottom = top + target_height
                    cropped_img = img_no_black_bars.crop((0, top, width, bottom))

            # Now crop to 2:3 for Upcoming Media Card
            width, height = cropped_img.size
            target_ratio = 2 / 3
            _LOGGER.debug(f"Adjusting to 2:3 ratio - Current size: {width}x{height}")

            if width / height > target_ratio:
                new_width = int(height * target_ratio)
                left = (width - new_width) // 2
                right = left + new_width
                img_cropped = cropped_img.crop((left, 0, right, height))
            else:
                new_height = int(width / target_ratio)
                top = (height - new_height) // 2
                bottom = top + new_height
                img_cropped = cropped_img.crop((0, top, width, bottom))

            _LOGGER.debug(f"Pre-resize size: {img_cropped.size}")
            img_resized = img_cropped.resize((480, 720), Image.LANCZOS)
            _LOGGER.debug(f"Final size: 480x720")

            image_byte_array = io.BytesIO()
            img_resized.save(image_byte_array, format="JPEG", quality=100)
            async with aiofiles.open(file_path, "wb") as f:
                await f.write(image_byte_array.getvalue())

    except Exception as e:
        _LOGGER.error(f"Error processing image for video_id {video_id}: {e}", exc_info=True)
        return "/local/youtube_thumbnails/default_thumbnail.jpg"

    return f"/local/youtube_thumbnails/{file_name}"

async def process_image_to_fanart(hass: HomeAssistant, video_id, thumbnails: dict) -> str:
    save_dir = hass.config.path("www", "youtube_thumbnails")
    os.makedirs(save_dir, exist_ok=True)

    if isinstance(video_id, dict):
        if video_id.get('is_live'):
            file_name = f"{video_id.get('videoId', '')}_live_fanart.jpg"
        else:
            file_name = f"{video_id.get('videoId', '')}_fanart.jpg"
    else:
        file_name = f"{video_id}_fanart.jpg"
    file_path = os.path.join(save_dir, file_name)  # Added this line

    url = thumbnails.get('maxres', {}).get('url')
    if not url:
        url = thumbnails.get('standard', {}).get('url') or thumbnails.get('high', {}).get('url')

    if not url:
        _LOGGER.error(f"No suitable thumbnail found for video_id: {video_id}")
        return ""

    session = async_get_clientsession(hass)
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                # _LOGGER.error(f"Failed to download image from URL: {url} with status code: {resp.status}")
                return url
            image_data = await resp.read()
    except Exception as e:
        _LOGGER.error(f"Exception occurred while fetching image from URL: {url}, error: {e}")
        return ""

    try:
        with Image.open(io.BytesIO(image_data)) as img:
            _LOGGER.debug(f"Processing image for video_id {video_id} - Original size: {img.size}")
            img_no_black_bars = img.convert("RGB")
            width, height = img_no_black_bars.size
            pixels = img_no_black_bars.load()

            # Detect black bars dynamically
            top, bottom, left, right = 0, height - 1, 0, width - 1
            threshold = 30

            # Detect top black bar
            for y in range(height):
                if any(not all(v <= threshold for v in pixels[x, y]) for x in range(width)):
                    top = y
                    break

            # Detect bottom black bar
            for y in range(height - 1, -1, -1):
                if any(not all(v <= threshold for v in pixels[x, y]) for x in range(width)):
                    bottom = y
                    break

            # Detect left black bar
            for x in range(width):
                if any(not all(v <= threshold for v in pixels[x, y]) for y in range(height)):
                    left = x
                    break

            # Detect right black bar
            for x in range(width - 1, -1, -1):
                if any(not all(v <= threshold for v in pixels[x, y]) for y in range(height)):
                    right = x
                    break

            # Log black bar detection results
            if left > 0 or top > 0 or right < width - 1 or bottom < height - 1:
                _LOGGER.debug(
                    f"Black bars detected - Left: {left}px, Top: {top}px, "
                    f"Right: {width - right - 1}px, Bottom: {height - bottom - 1}px"
                )

            # Crop to remove black bars
            img_cropped = img_no_black_bars.crop((left, top, right + 1, bottom + 1))
            width, height = img_cropped.size
            _LOGGER.debug(f"Size after black bar removal: {width}x{height}")

            # Adjust to 16:9 if necessary
            target_ratio = 16 / 9
            current_ratio = width / height
            if abs(current_ratio - target_ratio) > 0.01:
                _LOGGER.debug(f"Adjusting aspect ratio from {current_ratio:.2f} to {target_ratio}")
                if current_ratio > target_ratio:
                    new_height = int(width / target_ratio)
                    top = (height - new_height) // 2
                    _LOGGER.debug(f"Cropping {height - new_height}px from height to achieve 16:9")
                    img_cropped = img_cropped.crop((0, top, width, top + new_height))
                else:
                    new_width = int(height * target_ratio)
                    left = (width - new_width) // 2
                    _LOGGER.debug(f"Cropping {width - new_width}px from width to achieve 16:9")
                    img_cropped = img_cropped.crop((left, 0, left + new_width, height))

            # Resize to 1280x720
            img_resized = img_cropped.resize((1280, 720), Image.LANCZOS)
            _LOGGER.debug("Final resize to 1280x720 completed")
            img_byte_arr = io.BytesIO()
            img_resized.save(img_byte_arr, format='JPEG', quality=100)
            img_byte_arr = img_byte_arr.getvalue()

            async with aiofiles.open(file_path, 'wb') as out_file:
                await out_file.write(img_byte_arr)

    except Exception as e:
        _LOGGER.error(f"Error processing image for video_id {video_id}: {e}", exc_info=True)
        if isinstance(video_id, dict) and video_id.get('is_live'):
            return f"/local/youtube_thumbnails/{video_id.get('videoId', '')}_live_fanart.jpg"
        return "/local/youtube_thumbnails/default_thumbnail.jpg"

    return f"/local/youtube_thumbnails/{file_name}"

async def async_setup_entry(hass: HomeAssistant, entry, async_add_entities):
    """Set up YouTube Recently Added sensor platform."""
    # Use the existing coordinator from hass.data
    coordinator = hass.data[DOMAIN][entry.entry_id + "_coordinator"]

    sensor = YouTubeRecentlyAddedSensor(coordinator)
    shorts_sensor = YouTubeRecentlyAddedShortsSensor(coordinator)
    favorites_sensor = YouTubeRecentlyAddedFavoriteChannelsSensor(coordinator)

    async_add_entities([sensor, shorts_sensor, favorites_sensor], True)
    _LOGGER.debug("YouTube Recently Added sensors set up completed.")

class YouTubeDataUpdateCoordinator(DataUpdateCoordinator):
    def __init__(self, hass: HomeAssistant, youtube, entry):
        super().__init__(
            hass,
            _LOGGER,
            name="YouTube Recently Added",
            update_interval=timedelta(hours=6),
        )
        self.youtube = youtube
        self.hass = hass
        self.data = {"data": [], "shorts_data": [], "favorite_channels": []}
        self.config_entry = entry
        self.store = Store(hass, 1, f"{DOMAIN}.{entry.entry_id}")
        self.last_webhook_time = None
        self.favorite_channels = [channel.strip() for channel in entry.options.get(CONF_FAVORITE_CHANNELS, DEFAULT_FAVORITE_CHANNELS).split(',') if channel.strip()]
        self.filter_channels = [channel.strip().lower() for channel in entry.options.get(CONF_FILTER_CHANNELS, DEFAULT_FILTER_CHANNELS).split(',') if channel.strip()]
        self._video_processing_locks = {}
        self._webhook_processing_videos = set()
        self._shutdown = False
        self._favorites_task = asyncio.create_task(self._schedule_favorites_reset())
        self._cleanup_task = asyncio.create_task(self._cleanup_old_locks())

    async def _schedule_favorites_reset(self):
        async def reset_favorites(initial_delay):
            delay = initial_delay
            while not self._shutdown:
                try:
                    await asyncio.sleep(delay)
                    if self._shutdown:
                        break
                    self.data["favorite_channels"] = []
                    await self.store.async_save(self.data)
                    self.async_set_updated_data(self.data)
                    now = datetime.now(ZoneInfo(self.hass.config.time_zone))
                    midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
                    delay = (midnight - now).total_seconds()
                except asyncio.CancelledError:
                    _LOGGER.debug("Favorites reset task cancelled")
                    break
        
        now = datetime.now(ZoneInfo(self.hass.config.time_zone))
        midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        initial_delay = (midnight - now).total_seconds()
        
        await reset_favorites(initial_delay)

    async def _cleanup_old_locks(self):
        while not self._shutdown:
            try:
                await asyncio.sleep(300)
                if self._shutdown:
                    break
                current_video_ids = set()
                for videos in [self.data.get('data', []), self.data.get('shorts_data', []), self.data.get('favorite_channels', [])]:
                    current_video_ids.update(v['id'] for v in videos if isinstance(v, dict) and 'id' in v)
                
                to_remove = [vid for vid in self._video_processing_locks if vid not in current_video_ids]
                for vid in to_remove:
                    del self._video_processing_locks[vid]
                
                if to_remove:
                    _LOGGER.debug(f"Cleaned up {len(to_remove)} old video processing locks")
                
                self._webhook_processing_videos.clear()
            except asyncio.CancelledError:
                _LOGGER.debug("Cleanup task cancelled")
                break

    async def async_shutdown(self):
        """Shutdown coordinator and cleanup tasks."""
        _LOGGER.debug("Shutting down coordinator")
        self._shutdown = True
        
        # Cancel background tasks
        if self._favorites_task and not self._favorites_task.done():
            self._favorites_task.cancel()
            try:
                await self._favorites_task
            except asyncio.CancelledError:
                pass
        
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

    async def async_setup(self):
        stored_data = await self.store.async_load()
        # _LOGGER.critical(f"Loaded stored_data: {stored_data}")
        if stored_data and isinstance(stored_data, dict):
            self.data = {
                "data": stored_data.get("data", []),
                "shorts_data": stored_data.get("shorts_data", []),
                "favorite_channels": stored_data.get("favorite_channels", [])
            }
            # _LOGGER.critical(f"Initialized self.data: {self.data}")
            self.last_webhook_time = stored_data.get("last_webhook_time")
            if self.last_webhook_time:
                self.last_webhook_time = datetime.fromisoformat(self.last_webhook_time)
        else:
            self.data = {"data": [], "shorts_data": [], "favorite_channels": []}

    async def handle_webhook_update(self, video_id, is_deleted):
        # Log the initiation of the webhook update handling with video ID and deletion status
        # _LOGGER.critical(f"handle_webhook_update called for video {video_id}, is_deleted: {is_deleted}")
        
        # Log the current setting for using comments as summary
        # _LOGGER.critical(f"USE_COMMENTS_AS_SUMMARY setting: {get_USE_COMMENTS_AS_SUMMARY()}")
        
        if video_id in self._webhook_processing_videos and not is_deleted:
            _LOGGER.debug(f"Video {video_id} already being processed by webhook, skipping")
            return
        
        self._webhook_processing_videos.add(video_id)
        try:
            if video_id not in self._video_processing_locks:
                self._video_processing_locks[video_id] = asyncio.Lock()
            
            async with self._video_processing_locks[video_id]:
                if is_deleted:
                    # Log that the video is marked for deletion
                    # _LOGGER.critical(f"Processing deletion for video {video_id}")
                    
                    # Remove the video from the main data and shorts data lists
                    self.data["data"] = [v for v in self.data.get("data", []) if v["id"] != video_id]
                    self.data["shorts_data"] = [v for v in self.data.get("shorts_data", []) if v["id"] != video_id]
                    
                    # Save the updated data asynchronously
                    _LOGGER.debug(f"Saving updated data after deletion of video {video_id}")
                    await self.store.async_save(self.data)
                else:
                    # Log that the system is fetching video details for the given video ID
                    # _LOGGER.critical(f"Fetching video details for {video_id}")
                    
                    # Fetch video data asynchronously using the YouTube API
                    video_data = await self.youtube.get_video_by_id(video_id)
                    
                    if not video_data:
                        # Log that no video data was returned and the video will be skipped
                        # _LOGGER.critical(f"Skipping video {video_id} because get_video_by_id returned None.")
                        return
                    
                    if video_data:
                        # Check if channel should be filtered
                        channel_title = video_data.get('snippet', {}).get('channelTitle', '').lower()
                        if any(filter_channel in channel_title for filter_channel in self.filter_channels if filter_channel):
                            _LOGGER.debug(f"Skipping video {video_id} from filtered channel: {channel_title}")
                            return
                        # Log successful retrieval of video data
                        # _LOGGER.critical(f"Successfully fetched video data for {video_id}")
                        
                        # Extract relevant details from the video data
                        duration = video_data['contentDetails'].get('duration', 'PT0M0S')
                        live_streaming_details = video_data.get('liveStreamingDetails', {})
                        live_broadcast_content = video_data['snippet'].get('liveBroadcastContent')
                        actual_start_time = live_streaming_details.get('actualStartTime')
                        actual_end = live_streaming_details.get('actualEndTime')
                        scheduled_start = live_streaming_details.get('scheduledStartTime')
                        
                        # Determine if the video is a live stream based on various criteria
                        is_live_stream = (
                            live_broadcast_content in ['live', 'upcoming'] or
                            duration in ['P0D', 'PT0M0S'] or
                            actual_start_time is not None or
                            scheduled_start is not None
                        ) and not actual_end
                        
                        # Log the live stream status of the video
                        # _LOGGER.critical(f"Video {video_id} live stream status: {is_live_stream}")
                        
                        if is_live_stream:
                            # First, check for scheduled streams that haven't started yet
                            if scheduled_start and not actual_start_time:
                                # Skip all scheduled streams that haven't actually started
                                _LOGGER.debug(f"Skipping stream {video_id} - Not yet started")
                                return

                            # Skip streams where actualStartTime is in the future
                            if actual_start_time:
                                actual_start_datetime = datetime.strptime(actual_start_time, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=ZoneInfo("UTC"))
                                if actual_start_datetime > datetime.now(ZoneInfo("UTC")):
                                    _LOGGER.debug(f"Skipping stream {video_id} - actualStartTime in future: {actual_start_time}")
                                    return
                            
                            if actual_start_time and not actual_end:
                                # Retrieve and format the number of concurrent viewers
                                concurrent_viewers = int(live_streaming_details.get('concurrentViewers', '0'))
                                formatted_viewers = str(concurrent_viewers) if concurrent_viewers < 1000 else (
                                    f"{concurrent_viewers//1000}K" if concurrent_viewers < 1000000 else f"{concurrent_viewers//1000000}M"
                                )
                                
                                # Set the live status with the number of viewers
                                live_status = f"🔴 LIVE - {formatted_viewers} Watching"
                                video_data['live_status'] = live_status
                                _LOGGER.debug(f"Set live_status for video {video_id}: {live_status}")
                            
                            # Reset runtime for live streams
                            video_data['runtime'] = ""
                            
                            # Initialize retry parameters for fetching viewers
                            max_retries = 5
                            retry_delay = 15
                            live_status = ""
                            
                            if actual_start_time and not actual_end:
                                # Loop to attempt fetching viewers up to max_retries
                                for attempt in range(max_retries):
                                    _LOGGER.debug(f"Attempting to fetch viewers (Attempt {attempt + 1}/{max_retries}) for video {video_id}")
                                    
                                    # Batch update video statistics and comments
                                    stats_and_comments = await self.youtube.batch_update_video_statistics_and_comments([video_id])
                                    
                                    if stats_and_comments and video_id in stats_and_comments and stats_and_comments[video_id]:
                                        # Update video statistics with fetched data
                                        if 'statistics' not in video_data:
                                            video_data['statistics'] = {}
                                        video_data['statistics'].update(stats_and_comments[video_id])
                                        
                                        # Update summary if comments are available
                                        if 'comments' in stats_and_comments[video_id]:
                                            video_data['summary'] = stats_and_comments[video_id]['comments']
                                        
                                        # Retrieve and format concurrent viewers
                                        concurrent_viewers = int(stats_and_comments[video_id].get('concurrentViewers', '0'))
                                        value = concurrent_viewers / 1000000 if concurrent_viewers >= 1000000 else concurrent_viewers / 1000 if concurrent_viewers >= 1000 else concurrent_viewers
                                        suffix = 'M' if concurrent_viewers >= 1000000 else 'K' if concurrent_viewers >= 1000 else ''
                                        formatted_viewers = (f"{value:.1f}".rstrip('0').rstrip('.') + suffix) if concurrent_viewers >= 1000 else str(concurrent_viewers)
                                        
                                        # Set live status with viewer count
                                        if concurrent_viewers > 0:
                                            live_status = f"🔴 LIVE - {formatted_viewers} Watching"
                                        else:
                                            live_status = "🔴 LIVE - 🟩 Stream started"
                                        
                                        # Log the detection of a live stream with current viewer count
                                        # _LOGGER.critical(f"Live stream detected for video {video_id} - Currently streaming with {concurrent_viewers} viewers")
                                        
                                        # If viewers are present or it's the last retry, update the video data and save
                                        if concurrent_viewers > 0 or attempt == max_retries - 1:
                                            video_data['live_status'] = live_status
                                            video_data['airdate'] = datetime.now(ZoneInfo("UTC")).isoformat()
                                            target_list = 'data'
                                            
                                            # Ensure the target list exists in data
                                            if target_list not in self.data:
                                                self.data[target_list] = []
                                            
                                            # Remove existing entry and insert the updated video data at the top
                                            self.data[target_list] = [v for v in self.data[target_list] if v['id'] != video_data['id']]
                                            self.data[target_list].insert(0, video_data)
                                            
                                            # Save the updated data and set it as the current data
                                            _LOGGER.debug(f"Saving updated data after processing live stream for video {video_id}")
                                            await self.store.async_save(self.data)
                                            self.async_set_updated_data(self.data)
                                            break
                                        else:
                                            # Log that there are zero viewers and a retry will be attempted after delay
                                            _LOGGER.debug(f"Zero viewers, waiting {retry_delay}s before retry for video {video_id}")
                                            await asyncio.sleep(retry_delay)
                                    else:
                                        # Log a warning if no statistics are returned and a retry will be attempted
                                        _LOGGER.warning(f"No statistics returned for video {video_id}. Retrying if applicable.")
                                        if attempt < max_retries - 1:
                                            await asyncio.sleep(retry_delay)
                                        else:
                                            break
                                
                                # Final fallback after all retries if live_status was not set
                                if 'live_status' not in video_data:
                                    live_status = "🔴 LIVE - 🟩 Stream started"
                                    video_data['live_status'] = live_status
                                    video_data['airdate'] = datetime.now(ZoneInfo("UTC")).isoformat()
                                    target_list = 'data'
                                    
                                    if target_list not in self.data:
                                        self.data[target_list] = []
                                    
                                    self.data[target_list] = [v for v in self.data[target_list] if v['id'] != video_data['id']]
                                    self.data[target_list].insert(0, video_data)
                                    
                                    _LOGGER.debug(f"Final fallback: Saving live_status for video {video_id} with 0 viewers")
                                    await self.store.async_save(self.data)
                                    self.async_set_updated_data(self.data)
                            
                            elif actual_end:
                                # Fetch updated statistics and comments if the live stream has ended
                                stats_and_comments = await self.youtube.batch_update_video_statistics_and_comments([video_id])
                                if stats_and_comments and video_id in stats_and_comments:
                                    if 'statistics' not in video_data:
                                        video_data['statistics'] = {}
                                    video_data['statistics'].update(stats_and_comments[video_id])
                                    
                                    if 'comments' in stats_and_comments[video_id]:
                                        video_data['summary'] = stats_and_comments[video_id]['comments']
                                    
                                    # Set live status to indicate the stream has ended
                                    video_data['live_status'] = "📴 Stream ended"
                                    
                                    # Calculate runtime for ended stream based on actual start and end times
                                    if actual_start_time:
                                        try:
                                            start_time = datetime.strptime(actual_start_time, '%Y-%m-%dT%H:%M:%SZ')
                                            end_time = datetime.strptime(actual_end, '%Y-%m-%dT%H:%M:%SZ')
                                            stream_duration_seconds = int((end_time - start_time).total_seconds())
                                            video_data['runtime'] = str(max(1, int(stream_duration_seconds / 60)))
                                        except (ValueError, TypeError) as e:
                                            _LOGGER.error(f"Error calculating runtime for ended stream {video_id}: {e}")
                                            video_data['runtime'] = ""
                                    
                                    video_data['airdate'] = datetime.now(ZoneInfo("UTC")).isoformat()
                                    target_list = 'data'
                                    
                                    if target_list not in self.data:
                                        self.data[target_list] = []
                                    
                                    self.data[target_list] = [v for v in self.data[target_list] if v['id'] != video_data['id']]
                                    self.data[target_list].insert(0, video_data)
                                    
                                    # Save the updated data and set it as the current data
                                    _LOGGER.debug(f"Live stream detected for video {video_id} - Stream ended at {actual_end}")
                                    _LOGGER.debug(f"Saving updated data after stream end for video {video_id}")
                                    await self.store.async_save(self.data)
                                    self.async_set_updated_data(self.data)
                            
                            # Reset runtime only for active streams
                            if not actual_end:
                                video_data['runtime'] = ""
                            
                            # Process thumbnails to generate fanart and poster images
                            thumbnails = video_data['snippet'].get('thumbnails', {})
                            video_data['fanart'] = await process_image_to_fanart(self.hass, video_id, thumbnails)
                            
                            if is_live_stream and not actual_end:
                                # Set the poster URL for live streams
                                poster_url = f"https://i.ytimg.com/vi/{video_id}/maxresdefault_live.jpg"
                            else:
                                # Set the poster URL for regular videos
                                poster_url = thumbnails.get('maxres', {}).get('url') or thumbnails.get('high', {}).get('url')
                            
                            if poster_url:
                                # Prepare data for processing the portrait image
                                video_id_data = {'videoId': video_id, 'is_live': bool(is_live_stream and not actual_end)}
                                video_data['poster'] = await process_image_to_portrait(self.hass, poster_url, video_id_data)
                            else:
                                # Set a default poster if no poster URL is available
                                video_data['poster'] = "/local/youtube_thumbnails/default_poster.jpg" if not is_live_stream else f"/local/youtube_thumbnails/{video_id}_live_portrait.jpg"
                            
                            # Assign the live status to the video data
                            if is_live_stream and not live_status:
                                # Ensure live streams always have a status
                                if actual_end:
                                    video_data['live_status'] = "📴 Stream ended"
                                else:
                                    video_data['live_status'] = "🔴 LIVE - 🟩 Stream started"
                            else:
                                video_data['live_status'] = live_status
                            
                            target_list = 'data'
                            max_videos = self.config_entry.options.get(CONF_MAX_REGULAR_VIDEOS, DEFAULT_MAX_REGULAR_VIDEOS)
                            
                            if target_list not in self.data:
                                self.data[target_list] = []
                            
                            # Remove existing entries of the video and insert the updated video data
                            self.data[target_list] = [v for v in self.data.get(target_list, []) if v['id'] != video_data['id']]
                            self.data['shorts_data'] = [v for v in self.data.get('shorts_data', []) if v['id'] != video_data['id']]
                            
                            # Set the airdate based on whether it's a live stream or a regular video
                            video_data['airdate'] = datetime.now(ZoneInfo("UTC")).isoformat() if is_live_stream else video_data.get('snippet', {}).get('publishedAt')
                            self.data[target_list].insert(0, video_data)
                            
                            # Sort the videos by airdate and limit the number of videos to max_videos
                            self.data[target_list] = sorted(self.data[target_list], key=lambda x: x.get('airdate', ''), reverse=True)[:max_videos]
                            
                            # Save the updated data asynchronously
                            _LOGGER.debug(f"Saving updated data after processing video {video_id} to {target_list}")
                            await self.store.async_save(self.data)
                            
                            # Check if the video belongs to any favorite channels and update accordingly
                            if video_data and any(channel.lower() in video_data.get("snippet", {}).get("channelTitle", "").lower() 
                                                for channel in self.favorite_channels if channel):
                                _LOGGER.debug(f"Video {video_id} is from a favorite channel. Updating favorite_channels list.")
                                self.data["favorite_channels"] = [v for v in self.data.get("favorite_channels", []) if v["id"] != video_data["id"]]
                                self.data["favorite_channels"].insert(0, video_data)
                            
                            # Set the updated data
                            self.async_set_updated_data(self.data)
                            
                            # Log that the live stream video has been processed and saved
                            # _LOGGER.critical(f"Live stream video {video_id} processed and saved")
                            return
                        
                        try:
                            # Parse the duration of the video and convert it to seconds
                            duration_timedelta = parse_duration(duration)
                            duration_seconds = int(duration_timedelta.total_seconds())
                            # _LOGGER.critical(f"Video {video_id} duration: {duration_seconds} seconds")
                        except (ValueError, TypeError):
                            # Handle invalid duration formats
                            duration_seconds = 0
                            _LOGGER.warning(f"Invalid duration format for video {video_id}: {duration}")
                        
                        # Fetch statistics and comments for the video
                        stats_and_comments = await self.youtube.batch_update_video_statistics_and_comments([video_id])
                        if stats_and_comments and video_id in stats_and_comments:
                            if 'statistics' not in video_data:
                                video_data['statistics'] = {}
                            video_data['statistics'].update(stats_and_comments[video_id])
                            
                            if 'live_status' in stats_and_comments[video_id]:
                                video_data['live_status'] = stats_and_comments[video_id]['live_status']
                            
                            if 'comments' in stats_and_comments[video_id]:
                                video_data['summary'] = stats_and_comments[video_id]['comments']
                        
                        is_short = False
                        if duration_seconds <= 60:
                            # Classify the video as a short if its duration is 60 seconds or less
                            # _LOGGER.critical(f"Video {video_id} classified as short (duration <= 60s)")
                            is_short = True
                        elif duration_seconds <= 180:
                            thumbnails = video_data['snippet'].get('thumbnails', {})
                            poster_url = thumbnails.get('maxres', {}).get('url') or thumbnails.get('high', {}).get('url')
                            
                            # Log that the system is checking for portrait orientation in a short video
                            # _LOGGER.critical(f"Checking portrait orientation for video {video_id} (<= 180s)")
                            
                            if poster_url and await check_portrait_unified(poster_url, self.hass):
                                # Log that the video is classified as a short with portrait orientation
                                # _LOGGER.critical(f"Video {video_id} classified as short (portrait orientation)")
                                is_short = True
                            # else:
                            #     # Log that the video is classified as a regular video
                            #     _LOGGER.critical(f"Video {video_id} classified as regular video (not portrait)")
                        
                        if is_short:
                            # Set the target list and maximum number of shorts videos
                            target_list = 'shorts_data'
                            max_videos = self.config_entry.options.get(CONF_MAX_SHORT_VIDEOS, DEFAULT_MAX_SHORT_VIDEOS)
                            _LOGGER.debug(f"Processing video {video_id} as short in {target_list} with max_videos={max_videos}")
                        else:
                            # Set the target list and maximum number of regular videos
                            target_list = 'data'
                            max_videos = self.config_entry.options.get(CONF_MAX_REGULAR_VIDEOS, DEFAULT_MAX_REGULAR_VIDEOS)
                            has_livestream_markers = (
                                is_live_stream or
                                live_broadcast_content in ['live', 'upcoming'] or
                                actual_start_time is not None or
                                scheduled_start is not None
                            )
                            if not has_livestream_markers:
                                video_data['runtime'] = str(max(1, int(duration_seconds / 60)))
                            _LOGGER.debug(f"Processing video {video_id} as regular video in {target_list} with max_videos={max_videos}")
                        
                        # Log the target list being used for processing the video
                        # _LOGGER.critical(f"Processing video {video_id} as {target_list}")
                        
                        if 'fanart' not in video_data:
                            # Log that fanart is being processed for the video
                            _LOGGER.debug(f"Processing fanart for video {video_id}")
                            video_data['fanart'] = await process_image_to_fanart(self.hass, video_id, thumbnails)
                        
                        if 'poster' not in video_data:
                            if poster_url:
                                # Prepare data for processing the portrait image
                                video_id_data = {'videoId': video_id, 'is_live': bool(is_live_stream and not actual_end)}
                                video_data['poster'] = await process_image_to_portrait(self.hass, poster_url, video_id_data)
                                _LOGGER.debug(f"Processed poster for video {video_id} from URL {poster_url}")
                            else:
                                # Set a default poster if no poster URL is available
                                video_data['poster'] = "/local/youtube_thumbnails/default_poster.jpg" if not is_live_stream else f"/local/youtube_thumbnails/{video_id}_live_portrait.jpg"
                                _LOGGER.debug(f"Set default poster for video {video_id}")
                        
                        if target_list not in self.data:
                            # Initialize the target list in data if it doesn't exist
                            self.data[target_list] = []
                            _LOGGER.debug(f"Initialized target list '{target_list}' in data")
                        
                        # Remove any existing entries of the video in both data and shorts_data
                        self.data['data'] = [v for v in self.data.get('data', []) if v['id'] != video_data['id']]
                        self.data['shorts_data'] = [v for v in self.data.get('shorts_data', []) if v['id'] != video_data['id']]
                        
                        # Set the airdate based on live stream status
                        video_data['airdate'] = datetime.now(ZoneInfo("UTC")).isoformat() if is_live_stream else video_data.get('snippet', {}).get('publishedAt')
                        self.data[target_list].insert(0, video_data)
                        
                        # Sort the videos by airdate in descending order and limit to max_videos
                        self.data[target_list] = sorted(self.data[target_list], key=lambda x: x.get('airdate', ''), reverse=True)[:max_videos]
                        _LOGGER.debug(f"Sorted and limited videos in {target_list} to {max_videos} entries")
                        
                        # Save the updated data asynchronously
                        _LOGGER.debug(f"Saving updated data after processing video {video_id} to {target_list}")
                        await self.store.async_save(self.data)
                        
                        if video_data and any(channel.lower() in video_data.get("snippet", {}).get("channelTitle", "").lower() 
                                            for channel in self.favorite_channels if channel):
                            # Log that the video is from a favorite channel and update favorite_channels list
                            _LOGGER.debug(f"Video {video_id} is from a favorite channel. Updating favorite_channels list.")
                            self.data["favorite_channels"] = [v for v in self.data.get("favorite_channels", []) if v["id"] != video_data["id"]]
                            self.data["favorite_channels"].insert(0, video_data)
                        
                        # Set the updated data
                        self.async_set_updated_data(self.data)
                        
                        # Log that the video has been processed and saved to the target list
                        # _LOGGER.critical(f"Video {video_id} processed and saved to {target_list}")
        finally:
            self._webhook_processing_videos.discard(video_id)

    async def async_request_refresh(self, video_id=None, is_deleted=False):
        """Trigger a data refresh, possibly for a specific video."""
        if video_id:
            self.force_update = True
            self.webhook_video_id = video_id
            self.is_deleted = is_deleted
            await self.async_refresh()
        else:
            await super().async_request_refresh()
    
    def is_quota_exceeded(self):
        current_time = datetime.now(ZoneInfo("UTC"))
        _LOGGER.debug(f"Checking if quota is exceeded. Current quota: {self.youtube.current_quota}, Reset time: {self.youtube.quota_reset_time}")
        is_exceeded = self.youtube.current_quota >= 10000
        _LOGGER.debug(f"Quota exceeded: {is_exceeded}")
        return is_exceeded

    async def _validate_video_data(self, video):
        if isinstance(video, str):
            _LOGGER.debug(f"Received string instead of video data object: {video}")
            return False
            
        required_fields = ['id', 'snippet', 'contentDetails']
        if not all(field in video for field in required_fields):
            video_id = video.get('id', 'Unknown')
            _LOGGER.debug(f"Video ID {video_id} is missing required fields.")
            return False

        duration_iso = video['contentDetails'].get('duration', 'PT0M0S')
        try:
            duration_timedelta = isodate.parse_duration(duration_iso)
            duration_seconds = int(duration_timedelta.total_seconds())
            _LOGGER.debug(f"Video ID {video['id']} has duration_seconds: {duration_seconds}")
        except (ValueError, TypeError) as e:
            _LOGGER.debug(f"Video ID {video.get('id', 'Unknown')} has invalid duration format: {e}")
            return False

        # Exclude videos with duration <= 0
        if duration_seconds <= 0:
            _LOGGER.debug(f"Video ID {video['id']} excluded due to non-positive duration: {duration_seconds}")
            return False

        if video.get('live_status'):
            _LOGGER.debug(f"Video ID {video['id']} is a live stream.")
            video['runtime'] = ""
            return True
        elif duration_seconds <= 60 and not video.get('live_status'):
            _LOGGER.debug(f"Video ID {video['id']} classified as Short.")
            return True  # It's a Short; no need for 'statistics'
        elif duration_seconds <= 180:
            thumbnails = video['snippet'].get('thumbnails', {})
            thumb_url = thumbnails.get('maxres', {}).get('url') or thumbnails.get('high', {}).get('url')
            if not thumb_url:
                _LOGGER.debug(f"Video ID {video['id']} classified as Regular (no thumbnail).")
                return True
            if await check_portrait_unified(thumb_url, self.hass):
                _LOGGER.debug(f"Video ID {video['id']} classified as Short (portrait thumbnail).")
                return True
            _LOGGER.debug(f"Video ID {video['id']} classified as Regular.")
            if 'statistics' not in video:
                video['statistics'] = {'viewCount': '0', 'likeCount': '0'}
            return True
        else:
            if 'statistics' not in video:
                video['statistics'] = {'viewCount': '0', 'likeCount': '0'}
            _LOGGER.debug(f"Video ID {video['id']} classified as Regular.")
            return True

    async def _async_update_data(self):
        stored_data = await self.store.async_load()
        if stored_data:
            self.data = {
                "data": stored_data.get("data", []),
                "shorts_data": stored_data.get("shorts_data", []),
                "favorite_channels": stored_data.get("favorite_channels", [])
            }
            self.favorite_channels = [channel.strip() for channel in self.config_entry.options.get(CONF_FAVORITE_CHANNELS, DEFAULT_FAVORITE_CHANNELS).split(',') if channel.strip()]
            self.filter_channels = [channel.strip().lower() for channel in self.config_entry.options.get(CONF_FILTER_CHANNELS, DEFAULT_FILTER_CHANNELS).split(',') if channel.strip()]
        else:
            self.data = {"data": [], "shorts_data": [], "favorite_channels": []}
            self.favorite_channels = [channel.strip() for channel in self.config_entry.options.get(CONF_FAVORITE_CHANNELS, DEFAULT_FAVORITE_CHANNELS).split(',') if channel.strip()]
            self.filter_channels = [channel.strip().lower() for channel in self.config_entry.options.get(CONF_FILTER_CHANNELS, DEFAULT_FILTER_CHANNELS).split(',') if channel.strip()]

        current_time = datetime.now(ZoneInfo('UTC'))
        is_initial_setup = not self.last_update_success

        if not is_initial_setup and self.last_webhook_time and (current_time - self.last_webhook_time) < timedelta(hours=6):
            _LOGGER.info("Skipping scheduled update due to recent webhook activity")
            await self.store.async_save(self.data)
            return self.data

        try:
            if self.youtube is None:
                _LOGGER.error("YouTube API instance is None in YouTubeDataUpdateCoordinator")
                await self.store.async_save(self.data)
                return {"data": [], "shorts_data": [], "info": "YouTube API instance is not initialized."}

            _LOGGER.debug("Fetching recent videos from YouTube API")
            videos = await self.youtube.get_recent_videos()

            if not videos['data'] and not videos['shorts_data']:
                message = "No new videos were fetched from YouTube API. Keeping existing data."
                _LOGGER.info(message)
                await self.store.async_save(self.data)
                return self.data

            _LOGGER.debug("Processing fetched videos")
            processed_data = await self._process_videos(videos['data'], "data")
            processed_shorts = await self._process_videos(videos['shorts_data'], "shorts_data")

            if processed_data.get('data') or processed_shorts.get('shorts_data'):
                _LOGGER.info(f"Found {len(processed_data.get('data', []))} new regular videos and {len(processed_shorts.get('shorts_data', []))} new shorts")
            else:
                _LOGGER.info("No new videos found in this update")

            existing_videos = {v['id']: v for v in self.data.get('data', [])}
            existing_shorts = {v['id']: v for v in self.data.get('shorts_data', [])}

            for video in processed_data.get('data', []):
                existing_videos[video['id']] = video
            
            for video in processed_shorts.get('shorts_data', []):
                existing_shorts[video['id']] = video

            merged_data = {
                "data": list(existing_videos.values()),
                "shorts_data": list(existing_shorts.values()),
                "favorite_channels": self.data.get("favorite_channels", [])
            }

            max_regular_videos = self.config_entry.options.get(CONF_MAX_REGULAR_VIDEOS, DEFAULT_MAX_REGULAR_VIDEOS)
            max_short_videos = self.config_entry.options.get(CONF_MAX_SHORT_VIDEOS, DEFAULT_MAX_SHORT_VIDEOS)
            
            all_regular_videos = sorted(merged_data['data'], key=lambda x: x.get('airdate', ''), reverse=True)
            all_shorts = sorted(merged_data['shorts_data'], key=lambda x: x.get('airdate', ''), reverse=True)
            merged_data['data'] = all_regular_videos[:max_regular_videos]
            merged_data['shorts_data'] = all_shorts[:max_short_videos]

            favorite_channels_data = self.data.get("favorite_channels", [])
            all_videos = all_regular_videos + all_shorts

            for video in all_videos:
                if (any(channel.lower() in video.get("snippet", {}).get("channelTitle", "").lower() 
                    for channel in self.favorite_channels if channel) and 
                    video['id'] not in {v['id'] for v in favorite_channels_data}):
                    favorite_channels_data.insert(0, video)

            merged_data['favorite_channels'] = favorite_channels_data

            self.data = merged_data
            await self.store.async_save(self.data)
            return self.data

        except QuotaExceededException as qee:
            _LOGGER.warning(f"API quota exceeded: {qee}")
            message = str(qee)
            persistent_notification.async_create(
                self.hass,
                message,
                title="YouTube API Quota Exceeded",
                notification_id="youtube_quota_exceeded"
            )
            await self.store.async_save(self.data)
            return self.data
        except Exception as err:
            _LOGGER.error(f"Error updating data from YouTube API: {err}", exc_info=True)
            await self.store.async_save(self.data)
            return self.data

    async def _process_videos(self, videos, video_type):
        if not videos:
            return {"data": [], "shorts_data": []}

        processed_videos = []
        shorts_videos = []

        for video in videos:
            if await self._validate_video_data(video):
                # Check if channel should be filtered
                channel_title = video.get('snippet', {}).get('channelTitle', '').lower()
                if any(filter_channel in channel_title for filter_channel in self.filter_channels if filter_channel):
                    _LOGGER.debug(f"Filtering out video from channel: {channel_title}")
                    continue
                duration_iso = video['contentDetails'].get('duration', 'PT0M0S')
                try:
                    duration_timedelta = isodate.parse_duration(duration_iso)
                    duration_seconds = int(duration_timedelta.total_seconds())
                    runtime = max(1, int(duration_seconds / 60))
                    video['runtime'] = str(runtime)
                except (ValueError, TypeError):
                    _LOGGER.debug(f"Video ID {video.get('id', 'Unknown')} has invalid duration format.")
                    continue

                if get_USE_COMMENTS_AS_SUMMARY() and 'summary' not in video:
                    comments = await self.youtube.fetch_top_comments(video['id'])
                    video['summary'] = '\n\n'.join(comments) if comments else None
                elif not get_USE_COMMENTS_AS_SUMMARY():
                    video['summary'] = '\n\n'.join(video['snippet'].get('description', '').split('\n\n')[:2]).strip()

                # Process images
                video_id = video['id']
                thumbnails = video['snippet'].get('thumbnails', {})
                live_streaming_details = video.get('liveStreamingDetails', {})
                actual_start_time = live_streaming_details.get('actualStartTime')
                actual_end = live_streaming_details.get('actualEndTime')
                is_live_stream = bool(actual_start_time and not actual_end)

                try:
                    video_id_data = {'videoId': video_id, 'is_live': is_live_stream}
                    video['fanart'] = await process_image_to_fanart(self.hass, video_id_data, thumbnails)
                except Exception as e:
                    _LOGGER.error(f"Error processing fanart for video {video_id}: {e}")
                    video['fanart'] = "/local/youtube_thumbnails/default_fanart.jpg" if not is_live_stream else f"/local/youtube_thumbnails/{video_id}_live_fanart.jpg"

                # Prioritize 'maxres' thumbnail, fallback to 'high'
                if is_live_stream:
                    poster_url = f"https://i.ytimg.com/vi/{video_id}/maxresdefault_live.jpg"
                else:
                    poster_url = thumbnails.get('maxres', {}).get('url') or thumbnails.get('high', {}).get('url')

                if poster_url:
                    try:
                        video_id_data = {'videoId': video_id, 'is_live': is_live_stream}
                        video['poster'] = await process_image_to_portrait(self.hass, poster_url, video_id_data)
                        _LOGGER.debug(f"Using {'live' if is_live_stream else 'maxres' if thumbnails.get('maxres', {}).get('url') else 'high'} thumbnail for video {video_id}")
                    except Exception as e:
                        _LOGGER.error(f"Error processing poster for video {video_id}: {e}")
                        video['poster'] = "/local/youtube_thumbnails/default_poster.jpg" if not is_live_stream else f"/local/youtube_thumbnails/{video_id}_live_portrait.jpg"
                else:
                    _LOGGER.warning(f"No high-resolution thumbnail available for video: {video_id}")
                    video['poster'] = "/local/youtube_thumbnails/default_poster.jpg" if not is_live_stream else f"/local/youtube_thumbnails/{video_id}_live_portrait.jpg"

                if duration_seconds <= 60:
                    _LOGGER.debug(f"Video ID {video['id']} classified as Short.")
                    shorts_videos.append(video)
                elif duration_seconds <= 180:
                    if poster_url and await check_portrait_unified(poster_url, self.hass):
                        _LOGGER.debug(f"Video ID {video['id']} classified as Short (portrait thumbnail).")
                        shorts_videos.append(video)
                    else:
                        _LOGGER.debug(f"Video ID {video['id']} classified as Regular video.")
                        processed_videos.append(video)
                else:
                    _LOGGER.debug(f"Video ID {video['id']} classified as Regular video.")
                    processed_videos.append(video)

        if video_type == "data":
            return {"data": processed_videos, "shorts_data": []}
        elif video_type == "shorts_data":
            return {"data": [], "shorts_data": shorts_videos}
        else:
            return {"data": processed_videos, "shorts_data": shorts_videos}

    async def delete_orphaned_images(self, processed_data, processed_shorts):
        save_dir = self.hass.config.path("www", "youtube_thumbnails")

        directory_exists = await self.hass.async_add_executor_job(os.path.exists, save_dir)
        if not directory_exists:
            _LOGGER.error(f"Directory does not exist: {save_dir}")
            return

        valid_files = set()

        # Include videos from processed data
        video_list = processed_data.get('data', []) + processed_shorts.get('shorts_data', [])

        # Include videos from persisted data
        persisted_data = await self.store.async_load()
        if persisted_data:
            video_list.extend(persisted_data.get('data', []))
            video_list.extend(persisted_data.get('shorts_data', []))
            video_list.extend(persisted_data.get('favorite_channels', []))

        for video in video_list:
            if isinstance(video, dict):
                video_id = video.get('id')
            elif isinstance(video, str):
                video_id = video
            else:
                _LOGGER.warning(f"Unexpected video format: {video}")
                continue

            if video_id:
                if isinstance(video, dict) and video.get('is_live'):
                    valid_files.add(f"{video_id}_live_portrait.jpg")
                    valid_files.add(f"{video_id}_live_fanart.jpg")
                else:
                    valid_files.add(f"{video_id}_portrait.jpg")
                    valid_files.add(f"{video_id}_fanart.jpg")

        # Add all previously fetched video IDs
        for video_id in self.youtube.fetched_video_ids:
            valid_files.add(f"{video_id}_portrait.jpg")
            valid_files.add(f"{video_id}_fanart.jpg")
            valid_files.add(f"{video_id}_live_portrait.jpg")
            valid_files.add(f"{video_id}_live_fanart.jpg")

        # _LOGGER.debug(f"Valid files to keep: {valid_files}")

        try:
            all_files = await aiofiles.os.listdir(save_dir)
            # _LOGGER.debug(f"All files in '{save_dir}': {all_files}")
        except Exception as e:
            _LOGGER.error(f"Error listing files in {save_dir}: {e}")
            return

        to_delete = [file_name for file_name in all_files if file_name not in valid_files and not file_name.startswith('default_')]

        _LOGGER.debug(f"Orphaned files to delete: {to_delete}")

        if not to_delete:
            _LOGGER.info("No orphaned files to delete.")
            return

        for file_name in to_delete:
            file_path = os.path.join(save_dir, file_name)
            _LOGGER.debug(f"Attempting to delete orphaned file: {file_path}")
            try:
                await aiofiles.os.remove(file_path)
                _LOGGER.info(f"Deleted orphaned file: {file_path}")
            except Exception as e:
                _LOGGER.error(f"Failed to delete orphaned file {file_path}: {e}")

class YouTubeRecentlyAddedSensor(CoordinatorEntity, SensorEntity):
    def __init__(self, coordinator):
        super().__init__(coordinator)
        self._attr_unique_id = f"youtube_recently_added"
        self._attr_name = f"YouTube Recently Added"
        self._attr_icon = "mdi:youtube"
        # _LOGGER.debug(f"Initializing YouTubeRecentlyAddedSensor with coordinator data: {coordinator.data}")

    async def async_added_to_hass(self):
        """Handle when entity is added to hass."""
        await super().async_added_to_hass()
        if self.coordinator.data:
            if isinstance(self.coordinator.data, dict) and "data" in self.coordinator.data:
                video_data = self.coordinator.data["data"]
                updated_video_data = []
                for video in video_data:
                    if isinstance(video, str):
                        video_details = await self.coordinator.youtube.get_video_by_id(video)
                        if video_details:
                            updated_video_data.append(video_details)
                    else:
                        updated_video_data.append(video)
                self.coordinator.data["data"] = updated_video_data
            _LOGGER.debug("Sensor added to hass with existing coordinator data.")
            self.async_write_ha_state()

    @property
    def state(self):
        if isinstance(self.coordinator.data, dict) and "data" in self.coordinator.data:
            video_data = self.coordinator.data["data"]
            _LOGGER.debug(f"Processing {len(video_data)} videos for state")
            
            processed_videos = []
            for video in video_data:
                if isinstance(video, dict) and "snippet" in video and "publishedAt" in video["snippet"]:
                    try:
                        processed_video = self._process_video_data(video)
                        if processed_video:
                            processed_videos.append(processed_video)
                    except Exception as e:
                        _LOGGER.error(f"Error processing video data: {e}", exc_info=True)

            sorted_videos = sorted(processed_videos, key=lambda x: x.get('airdate', ''), reverse=True)
            max_videos = self.coordinator.config_entry.options.get(CONF_MAX_REGULAR_VIDEOS, DEFAULT_MAX_REGULAR_VIDEOS)
            sorted_videos = sorted_videos[:max_videos]

            base_attributes = {
                "data": [
                    {
                        "title_default": "$title",
                        "line1_default": "$channel",
                        "line2_default": "$release",
                        "line3_default": "$runtime - $live_status - $genres",
                        "line4_default": "$views views - $likes likes",
                        "icon": "mdi:youtube"
                    }
                ],
                "friendly_name": "YouTube Recently Added",
                "current_quota": getattr(self.coordinator.youtube, 'current_quota', None)
            }

            self._prepared_items = []
            current_data = base_attributes["data"].copy()

            for item in sorted_videos:
                test_item = item.copy()
                test_attributes = base_attributes.copy()
                test_attributes["data"] = current_data + [test_item]
                if len(json.dumps(test_attributes).encode('utf-8')) > 16000:
                    break

                current_data.append(test_item)
                self._prepared_items.append(test_item)

            _LOGGER.debug(f"YouTubeRecentlyAddedSensor state (limited): {len(self._prepared_items)}")
            return len(self._prepared_items)
        _LOGGER.debug("YouTubeRecentlyAddedSensor state: 0 (no data available)")
        return 0

    @property
    def extra_state_attributes(self):
        attributes = {
            "data": [
                {
                    "title_default": "$title",
                    "line1_default": "$channel",
                    "line2_default": "$release",
                    "line3_default": "$runtime - $live_status - $genres",
                    "line4_default": "$views views - $likes likes",
                    "icon": "mdi:youtube"
                }
            ]
        }

        if hasattr(self, '_prepared_items'):
            attributes["data"].extend(self._prepared_items)
        else:
            if isinstance(self.coordinator.data, dict) and "data" in self.coordinator.data:
                video_data = self.coordinator.data["data"]
                video_data_list = []
                for video in video_data:
                    if isinstance(video, dict) and "snippet" in video and "publishedAt" in video["snippet"]:
                        try:
                            processed_video = self._process_video_data(video)
                            if processed_video:
                                if 'live_status' not in processed_video:
                                    processed_video['live_status'] = video.get('live_status', '')
                                video_data_list.append(processed_video)
                        except Exception as e:
                            _LOGGER.error(f"Error processing video data: {e}", exc_info=True)
                            continue

                sorted_video_data = sorted(video_data_list, key=lambda x: x.get('airdate', ''), reverse=True)
                
                current_data = attributes["data"]
                                
                for item in sorted_video_data[:self.state]:
                    test_item = item.copy()
                    test_attributes = attributes.copy()
                    test_attributes["data"] = current_data + [test_item]
                    if len(json.dumps(test_attributes).encode('utf-8')) > 16000:
                        _LOGGER.warning(f"YouTube Recently Added sensor items reduced to {len(current_data) - 1} due to 16KB attribute size limit.")
                        break

                    current_data.append(test_item)

        attributes["friendly_name"] = "YouTube Recently Added"
        if self.coordinator.last_update_success:
            attributes["current_quota"] = getattr(self.coordinator.youtube, 'current_quota', None)

        return attributes

    def _process_video_data(self, video):
        video_id = video.get('id')
        published_at = video["snippet"].get("publishedAt")
        if not published_at:
            _LOGGER.warning(f"Video {video_id} is missing 'publishedAt' field.")
            return None

        try:
            live_streaming_details = video.get('liveStreamingDetails', {})
            scheduled_start = live_streaming_details.get('scheduledStartTime')
            actual_start = live_streaming_details.get('actualStartTime')

            if scheduled_start and not actual_start:
                _LOGGER.debug(f"Skipping scheduled stream {video_id} that hasn't started yet")
                return None

            published_date = parser.isoparse(published_at).replace(tzinfo=ZoneInfo("UTC"))
            local_tz = ZoneInfo(self.hass.config.time_zone)
            local_published_date = published_date.astimezone(local_tz)
            airdate = local_published_date.isoformat()
            aired = local_published_date.strftime("%Y-%m-%d")
            release = local_published_date.strftime("%A, %B %d, %Y %I:%M %p")
            
            # Check if this is a live stream video and its status
            live_status = video.get('live_status', '')
            is_live = bool(live_status)
            stream_ended = live_status == "📴 Stream ended"
            
            # Pass live status and stream ended status to _convert_duration
            runtime = self._convert_duration(video['contentDetails'].get('duration', 'PT0M0S'), is_live, stream_ended)

            statistics = video.get('statistics', {})
            view_count = statistics.get('viewCount')
            like_count = statistics.get('likeCount')

            def format_count(count):
                if count is None:
                    return "0"
                count = int(count)
                if count >= 1000000:
                    value = count / 1000000
                    return f"{value:.1f}".rstrip('0').rstrip('.') + "M"
                elif count >= 1000:
                    value = count / 1000
                    return f"{value:.1f}".rstrip('0').rstrip('.') + "K"
                return str(count)

            views = format_count(view_count)
            likes = format_count(like_count)

            processed_data = {
                "id": video_id,
                "airdate": airdate,
                "aired": aired,
                "release": release,
                "title": video["snippet"].get("title", ""),
                "channel": video["snippet"].get("channelTitle", ""),
                "runtime": runtime,
                "genres": video.get("genres", ""),
                "views": views,
                "likes": likes,
                "summary": video.get("summary", video["snippet"].get("description", "")),
                "poster": video.get("poster", ""),
                "fanart": video.get("fanart", ""),
                "deep_link": f"https://www.youtube.com/watch?v={video_id}",
                "live_status": video.get("live_status", "")
            }
            
            # Add live_status if it exists
            if live_status:
                processed_data["live_status"] = live_status
                
            return processed_data
        except Exception as e:
            _LOGGER.error(f"Error processing video data for ID {video_id}: {e}", exc_info=True)
            return None

    def _convert_duration(self, duration, is_live=False, stream_ended=False):
        if is_live and not stream_ended:
            return ""
        try:
            duration_timedelta = isodate.parse_duration(duration)
            minutes = int(duration_timedelta.total_seconds() // 60)
            return str(max(1, minutes))
        except (isodate.ISO8601Error, ValueError, TypeError) as e:
            _LOGGER.error(f"Error converting duration: {e}")
            return ""

class YouTubeRecentlyAddedShortsSensor(YouTubeRecentlyAddedSensor):
    """Representation of a YouTube Recently Added Shorts Sensor."""

    def __init__(self, coordinator):
        super().__init__(coordinator)
        self._attr_unique_id = "youtube_recently_added_shorts"
        self._attr_name = "YouTube Recently Added Shorts"
        self._attr_icon = "mdi:youtube"
        _LOGGER.debug("Initializing YouTube Recently Added Shorts sensor")

    @property
    def state(self):
        _LOGGER.debug("Calculating state for YouTubeRecentlyAddedShortsSensor")
        if isinstance(self.coordinator.data, dict) and "shorts_data" in self.coordinator.data:
            shorts_data = self.coordinator.data["shorts_data"]
            _LOGGER.debug(f"Processing {len(shorts_data)} shorts for state")
            
            processed_shorts = []
            for video in shorts_data:
                if isinstance(video, dict) and "snippet" in video and "publishedAt" in video["snippet"]:
                    try:
                        processed_video = self._process_video_data(video)
                        if processed_video:
                            processed_shorts.append(processed_video)
                    except Exception as e:
                        _LOGGER.error(f"Error processing shorts data: {e}", exc_info=True)

            sorted_shorts = sorted(processed_shorts, key=lambda x: x.get('airdate', ''), reverse=True)
            max_videos = self.coordinator.config_entry.options.get(CONF_MAX_SHORT_VIDEOS, DEFAULT_MAX_SHORT_VIDEOS)
            sorted_shorts = sorted_shorts[:max_videos]

            base_attributes = {
                "data": [
                    {
                        "title_default": "$title",
                        "line1_default": "$channel",
                        "line2_default": "$release",
                        "line3_default": "$runtime - $live_status - $genres",
                        "line4_default": "$views views - $likes likes",
                        "icon": "mdi:youtube"
                    }
                ],
                "friendly_name": "YouTube Recently Added Shorts",
                "current_quota": getattr(self.coordinator.youtube, 'current_quota', None)
            }

            self._prepared_items = []
            current_data = base_attributes["data"].copy()

            for item in sorted_shorts:
                test_item = item.copy()
                test_attributes = base_attributes.copy()
                test_attributes["data"] = current_data + [test_item]
                if len(json.dumps(test_attributes).encode('utf-8')) > 16000:
                    break

                current_data.append(test_item)
                self._prepared_items.append(test_item)

            _LOGGER.debug(f"YouTubeRecentlyAddedShortsSensor state (limited): {len(self._prepared_items)}")
            return len(self._prepared_items)

        _LOGGER.warning("No shorts data available. Returning 0.")
        return 0

    @property
    def extra_state_attributes(self):
        attributes = {
            "data": [
                {
                    "title_default": "$title",
                    "line1_default": "$channel",
                    "line2_default": "$release",
                    "line3_default": "$runtime - $live_status - $genres",
                    "line4_default": "$views views - $likes likes",
                    "icon": "mdi:youtube"
                }
            ]
        }

        if hasattr(self, '_prepared_items'):
            attributes["data"].extend(self._prepared_items)
        else:
            if isinstance(self.coordinator.data, dict) and "shorts_data" in self.coordinator.data:
                shorts_data = self.coordinator.data["shorts_data"]
                shorts_data_list = []
                for video in shorts_data:
                    if isinstance(video, dict) and "snippet" in video and "publishedAt" in video["snippet"]:
                        try:
                            processed_video = self._process_video_data(video)
                            if processed_video:
                                shorts_data_list.append(processed_video)
                        except Exception as e:
                            _LOGGER.error(f"Error processing shorts data: {e}", exc_info=True)
                            continue

                sorted_shorts_data = sorted(shorts_data_list, key=lambda x: x.get('airdate', ''), reverse=True)
                
                current_data = attributes["data"]
                                
                for item in sorted_shorts_data[:self.state]:
                    test_item = item.copy()
                    test_attributes = attributes.copy()
                    test_attributes["data"] = current_data + [test_item]
                    if len(json.dumps(test_attributes).encode('utf-8')) > 16000:
                        _LOGGER.warning(f"YouTube Recently Added Shorts sensor items reduced to {len(current_data) - 1} due to 16KB attribute size limit.")
                        break

                    current_data.append(test_item)

        attributes["friendly_name"] = "YouTube Recently Added Shorts"
        if self.coordinator.last_update_success:
            attributes["current_quota"] = getattr(self.coordinator.youtube, 'current_quota', None)

        return attributes

class YouTubeRecentlyAddedFavoriteChannelsSensor(YouTubeRecentlyAddedSensor):
    def __init__(self, coordinator):
        super().__init__(coordinator)
        self._attr_unique_id = "youtube_recently_added_favorite_channels"
        self._attr_name = "YouTube Recently Added Favorite Channels"
        self._attr_icon = "mdi:youtube"

    @property
    def state(self):
        if isinstance(self.coordinator.data, dict) and "favorite_channels" in self.coordinator.data:
            favorite_videos = self.coordinator.data["favorite_channels"]
            
            base_attributes = {
                "data": [
                    {
                        "title_default": "$title",
                        "line1_default": "$channel",
                        "line2_default": "$release",
                        "line3_default": "$runtime - $live_status - $genres",
                        "line4_default": "$views views - $likes likes",
                        "icon": "mdi:youtube"
                    }
                ]
            }

            self._prepared_items = []
            current_data = base_attributes["data"].copy()

            sorted_videos = sorted(favorite_videos, key=lambda x: x.get('snippet', {}).get('publishedAt', ''), reverse=True)

            for item in sorted_videos:
                test_item = {
                    "id": item.get("id"),
                    "airdate": item.get("snippet", {}).get("publishedAt"),
                    "aired": item.get("snippet", {}).get("publishedAt", "").split("T")[0],
                    "release": datetime.strptime(item.get("snippet", {}).get("publishedAt"), "%Y-%m-%dT%H:%M:%SZ").strftime("%A, %B %d, %Y %I:%M %p"),
                    "title": item.get("snippet", {}).get("title"),
                    "channel": item.get("snippet", {}).get("channelTitle"),
                    "runtime": item.get("runtime", "1"),
                    "genres": item.get("genres", ""),
                    "views": self._process_video_data(item)['views'],
                    "likes": self._process_video_data(item)['likes'],
                    "summary": item.get("summary", item.get("snippet", {}).get("description", "")),
                    "poster": item.get("poster") or (
                        f"/local/youtube_thumbnails/{item.get('id')}_live_portrait.jpg" 
                        if item.get('liveStreamingDetails', {}).get('actualStartTime') and not item.get('liveStreamingDetails', {}).get('actualEndTime')
                        else f"/local/youtube_thumbnails/{item.get('id')}_portrait.jpg"
                    ),
                    "fanart": item.get("fanart") or (
                        f"/local/youtube_thumbnails/{item.get('id')}_live_fanart.jpg"
                        if item.get('liveStreamingDetails', {}).get('actualStartTime') and not item.get('liveStreamingDetails', {}).get('actualEndTime')
                        else f"/local/youtube_thumbnails/{item.get('id')}_fanart.jpg"
                    ),
                    "deep_link": f"https://www.youtube.com/watch?v={item.get('id')}"
                }

                test_attributes = base_attributes.copy()
                test_attributes["data"] = current_data + [test_item]
                if len(json.dumps(test_attributes).encode('utf-8')) > 16000:
                    break

                current_data.append(test_item)
                self._prepared_items.append(test_item)

            return len(self._prepared_items)
        return 0

    @property
    def extra_state_attributes(self):
        attributes = {
            "data": [
                {
                    "title_default": "$title",
                    "line1_default": "$channel",
                    "line2_default": "$release",
                    "line3_default": "$runtime - $live_status - $genres",
                    "line4_default": "$views views - $likes likes",
                    "icon": "mdi:youtube"
                }
            ]
        }

        if hasattr(self, '_prepared_items'):
            attributes["data"].extend(self._prepared_items)

        attributes["friendly_name"] = "YouTube Recently Added Favorite Channels"
        if self.coordinator.last_update_success:
            attributes["current_quota"] = getattr(self.coordinator.youtube, 'current_quota', None)

        return attributes
