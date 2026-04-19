import asyncio
import base64
import html
from io import BytesIO
import json
import logging
import os
import random
import re
import shutil
import threading
import time
import warnings
from urllib.parse import parse_qs, quote, urlparse
import socket
from urllib.request import Request, build_opener, ProxyHandler, HTTPSHandler
import ssl

import discord
from discord.ext import commands
from dotenv import load_dotenv
warnings.filterwarnings(
    "ignore",
    message=r"urllib3 .* doesn't match a supported version!",
    category=Warning,
)
import requests
import yt_dlp as youtube_dl

try:
    import deezer
except Exception:
    deezer = None

try:
    from PIL import Image, ImageDraw, ImageFont
except Exception:
    Image = None
    ImageDraw = None
    ImageFont = None


load_dotenv()

TOKEN = os.getenv("TOKEN")
PREFIX = "r!"
IS_WINDOWS = os.name == "nt"
USE_ENV_PROXY = os.getenv("BOT_USE_ENV_PROXY", "").strip().lower() in {"1", "true", "yes", "on"}
AUTO_DISCONNECT_SECONDS = 180
VOICE_READY_WAIT_SECONDS = 4.0
VOICE_READY_POLL_SECONDS = 0.2
QUEUE_PREVIEW_LIMIT = 10
QUEUE_COMPACT_THRESHOLD = 12

if not TOKEN:
    raise ValueError("❌ Aucun TOKEN trouvé dans .env !")


def sanitize_linux_preload():
    if IS_WINDOWS:
        return

    preload_value = os.environ.get("LD_PRELOAD", "")
    if not preload_value:
        return

    entries = [entry.strip() for entry in preload_value.split(":") if entry.strip()]
    cleaned_entries = []
    for entry in entries:
        lowered = entry.lower()
        if "gameoverlayrenderer.so" in lowered:
            continue
        cleaned_entries.append(entry)

    if cleaned_entries:
        os.environ["LD_PRELOAD"] = ":".join(cleaned_entries)
    else:
        os.environ.pop("LD_PRELOAD", None)


sanitize_linux_preload()

class QuietYTDLLogger:
    def debug(self, msg):
        pass

    def warning(self, msg):
        pass

    def error(self, msg):
        pass


dz = deezer.Client() if deezer else None


intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True
intents.guilds = True

bot = commands.Bot(command_prefix=PREFIX, intents=intents)
bot.remove_command("help")
logging.getLogger("discord.player").setLevel(logging.WARNING)


ytdl_format_options = {
    "format": "bestaudio[protocol^=http][abr<=256]/bestaudio[protocol^=http]/bestaudio/best",
    "noplaylist": True,
    "quiet": True,
    "default_search": "auto",
    "source_address": "0.0.0.0",
    "geo_bypass": True,
    "ignoreerrors": True,
    "extractor_args": {
        "youtube": ["player_client=default"],
    },
    "logger": QuietYTDLLogger(),
}
if not USE_ENV_PROXY:
    ytdl_format_options["proxy"] = ""

ytdl = youtube_dl.YoutubeDL(ytdl_format_options)
ytdl_search_format_options = dict(ytdl_format_options)
ytdl_search_format_options["extract_flat"] = True
ytdl_search = youtube_dl.YoutubeDL(ytdl_search_format_options)
ytdl_playlist_format_options = dict(ytdl_format_options)
ytdl_playlist_format_options["noplaylist"] = False
ytdl_playlist_format_options["extract_flat"] = "in_playlist"
ytdl_playlist = youtube_dl.YoutubeDL(ytdl_playlist_format_options)
thumbnail_color_cache = {}
_http_session_local = threading.local()
spotify_tracks_cache = {}
deezer_tracks_cache = {}
youtube_resolution_cache = {}
youtube_details_cache = {}
youtube_owner_cache = {}
youtube_page_metadata_cache = {}
youtube_playlist_cache = {}
webpage_media_metadata_cache = {}
queue_item_resolution_cache = {}
recent_command_invocations = {}
recent_feedback_messages = {}
recent_embed_messages = {}
recent_feedback_refs = {}
recent_embed_refs = {}
embed_color_refresh_tasks = {}
message_embed_suppression_tasks = {}

TRACK_CACHE_TTL_SECONDS = 900
YOUTUBE_CACHE_TTL_SECONDS = 1800
COMMAND_INVOCATION_TTL_SECONDS = 30
FEEDBACK_DEDUPE_WINDOW_SECONDS = 8.0
EMBED_DEDUPE_WINDOW_SECONDS = 8.0
_instance_name = os.getenv("HOSTNAME") or os.getenv("COMPUTERNAME") or socket.gethostname() or ""
INSTANCE_FEEDBACK_JITTER_SECONDS = (
    ((sum(ord(char) for char in _instance_name) + (os.getpid() % 17)) % 6) * 0.05
)
instance_lock_handle = None
USE_DYNAMIC_EMBED_COLORS = True
MAX_ENRICHED_CANDIDATES = 1
YOUTUBE_PRIMARY_SEARCH_SIZE = 2
YOUTUBE_FALLBACK_SEARCH_SIZE = 1
YOUTUBE_EARLY_ACCEPT_SCORE = 145
YOUTUBE_EARLY_ACCEPT_MARGIN = 16
PREFERRED_AUDIO_ABR = 224
MATCHING_ALGO_VERSION = 7
STRICT_FAST_ACCEPT_SCORE = 160
STRICT_SLOW_SEARCH_SIZE = 2
FFMPEG_STREAM_BEFORE_OPTIONS = (
    "-nostdin "
    "-thread_queue_size 8192 "
    "-reconnect 1 "
    "-reconnect_streamed 1 "
    "-reconnect_on_network_error 1 "
    "-reconnect_on_http_error 4xx,5xx "
    "-reconnect_delay_max 3 "
    "-rw_timeout 20000000"
)
FFMPEG_STREAM_OPTIONS = "-vn -sn -dn -loglevel quiet"
FFMPEG_SOUNDCLOUD_STREAM_OPTIONS = (
    "-vn -sn -dn "
    "-af aresample=48000:first_pts=0 "
    "-loglevel quiet"
)
_ffmpeg_executable = None


def find_ffmpeg():
    global _ffmpeg_executable

    if _ffmpeg_executable:
        return _ffmpeg_executable

    local_candidates = []
    if IS_WINDOWS:
        local_candidates.append(os.path.join(os.getcwd(), "bin", "ffmpeg.exe"))
        local_candidates.append(r"C:\ffmpeg\bin\ffmpeg.exe")
    else:
        local_candidates.append(os.path.join(os.getcwd(), "bin", "ffmpeg"))
        local_candidates.append(os.path.join(os.getcwd(), "bin", "ffmpeg.exe"))
        local_candidates.extend([
            "/usr/bin/ffmpeg",
            "/usr/local/bin/ffmpeg",
        ])

    for local_path in local_candidates:
        if os.path.isfile(local_path):
            _ffmpeg_executable = local_path
            return _ffmpeg_executable

    discovered = shutil.which("ffmpeg.exe" if IS_WINDOWS else "ffmpeg")
    if discovered:
        _ffmpeg_executable = discovered
        return _ffmpeg_executable

    _ffmpeg_executable = "ffmpeg"
    return _ffmpeg_executable


def resolve_local_asset(*names):
    candidates = []
    for name in names:
        if not name:
            continue
        candidates.append(os.path.join(os.getcwd(), name))
        candidates.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), name))
        if not IS_WINDOWS:
            candidates.append(os.path.join("/home/deck", name))

    seen = set()
    for candidate in candidates:
        normalized = os.path.abspath(candidate)
        if normalized in seen:
            continue
        seen.add(normalized)
        if os.path.isfile(normalized):
            return normalized
    return None


def format_duration(seconds):
    if seconds is None:
        return "?"
    try:
        total = int(round(float(seconds)))
        return f"{total // 60}:{total % 60:02d}"
    except Exception:
        return "?"


def format_timecode(seconds):
    try:
        total = max(0, int(round(float(seconds))))
    except Exception:
        return "0:00"

    hours = total // 3600
    minutes = (total % 3600) // 60
    remaining_seconds = total % 60
    if hours:
        return f"{hours}:{minutes:02d}:{remaining_seconds:02d}"
    return f"{minutes}:{remaining_seconds:02d}"


def parse_timecode(value):
    text = str(value or "").strip().lower()
    if not text:
        raise ValueError("Indique un timecode, par exemple `1:30`.")

    if re.fullmatch(r"\d+", text):
        return int(text)

    if ":" in text:
        parts = text.split(":")
        if len(parts) not in {2, 3} or any(not part.isdigit() for part in parts):
            raise ValueError("Format invalide. Utilise `ss`, `mm:ss` ou `hh:mm:ss`.")

        values = [int(part) for part in parts]
        if len(values) == 2:
            minutes, seconds = values
            if seconds >= 60:
                raise ValueError("Les secondes doivent être inférieures à 60.")
            return (minutes * 60) + seconds

        hours, minutes, seconds = values
        if minutes >= 60 or seconds >= 60:
            raise ValueError("Utilise un format valide comme `1:23:45`.")
        return (hours * 3600) + (minutes * 60) + seconds

    unit_match = re.fullmatch(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?", text)
    if unit_match and any(group is not None for group in unit_match.groups()):
        hours = int(unit_match.group(1) or 0)
        minutes = int(unit_match.group(2) or 0)
        seconds = int(unit_match.group(3) or 0)
        return (hours * 3600) + (minutes * 60) + seconds

    raise ValueError("Format invalide. Utilise `ss`, `mm:ss`, `hh:mm:ss` ou `1m30s`.")


def spotify_headers():
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    }


def image_request_headers(url=None):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    }
    host = (urlparse(url).netloc or "").lower() if url else ""
    if "ytimg.com" in host:
        headers["Referer"] = "https://www.youtube.com/"
    elif "googleusercontent.com" in host or "ggpht.com" in host:
        headers["Referer"] = "https://www.youtube.com/"
    elif "scdn.co" in host:
        headers["Referer"] = "https://open.spotify.com/"
    elif "dzcdn.net" in host or "deezer" in host:
        headers["Referer"] = "https://www.deezer.com/"
    return headers


def http_session():
    session = getattr(_http_session_local, "session", None)
    if session is None:
        session = requests.Session()
        session.trust_env = bool(USE_ENV_PROXY)
        _http_session_local.session = session
    return session


def acquire_instance_lock():
    lock_path = os.path.join(os.getcwd(), ".renaud_bot.instance.lock")
    lock_file = open(lock_path, "a+b")
    try:
        if os.name == "nt":
            import msvcrt
            lock_file.seek(0)
            if lock_file.tell() == 0:
                lock_file.write(b"0")
                lock_file.flush()
            lock_file.seek(0)
            msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except Exception:
        try:
            lock_file.close()
        except Exception:
            pass
        raise RuntimeError("Une autre instance du bot est déjà en cours sur cette machine.")

    try:
        lock_file.seek(0)
        lock_file.truncate()
        lock_file.write(str(os.getpid()).encode("utf-8"))
        lock_file.flush()
    except Exception:
        pass
    return lock_file


def get_cached(cache, key):
    cached = cache.get(key)
    if not cached:
        return None

    expires_at, value = cached
    if expires_at <= time.monotonic():
        cache.pop(key, None)
        return None
    return value


def set_cached(cache, key, value, ttl_seconds):
    cache[key] = (time.monotonic() + ttl_seconds, value)


def clone_tracks(tracks):
    return [dict(track) for track in tracks or []]


def sanitize_embed_text(value):
    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    return discord.utils.escape_markdown(text, as_needed=False)


def truncate_display_text(value, max_length):
    text = str(value or "").strip()
    if len(text) <= max_length:
        return text
    return f"{text[:max_length - 1].rstrip()}…"


def clean_playlist_display_title(title, *, source=None):
    text = clean_spotify_text(title) or "Titre inconnu"
    if source == "youtube":
        noise_patterns = [
            r"\s*\[(official|lyrics?|audio|video|visualizer|hd|4k)[^\]]*\]",
            r"\s*\((official|lyrics?|audio|video|visualizer|hd|4k)[^)]*\)",
            r"\s*-\s*(official|lyrics?|audio|video|visualizer)\s*$",
            r"\s*\|\s*official[^|]*$",
        ]
        for pattern in noise_patterns:
            text = re.sub(pattern, "", text, flags=re.I)
        text = simplify_track_title(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or "Titre inconnu"


def clean_playlist_display_artist(artist, *, source=None):
    text = clean_spotify_text(artist)
    if not text:
        return None

    lowered = normalize_search_text(text)
    if lowered in {"artiste inconnu", "chaine inconnue", "chaine inconnu"}:
        return None

    if source == "youtube" and lowered in {"unknown channel", "unknown artist"}:
        return None

    return text


def compact_youtube_playlist_title(title, artist=None):
    text = clean_playlist_display_title(title, source="youtube")

    if " - " in text and artist:
        parts = [part.strip() for part in text.split(" - ") if part.strip()]
        while len(parts) > 1:
            last_part = parts[-1]
            if is_strong_token_match(artist, last_part) or has_any_token_match(artist, last_part):
                parts.pop()
                text = " - ".join(parts).strip()
            else:
                break

    if " - " in text and len(text) > 34:
        parts = [part.strip() for part in text.split(" - ") if part.strip()]
        if len(parts) > 1 and len(parts[-1]) <= 22:
            text = " - ".join(parts[:-1]).strip()

    if len(text) > 34:
        shortened = re.sub(r"\s*\([^)]{1,24}\)\s*$", "", text).strip()
        if shortened:
            text = shortened

    return truncate_display_text(text, 38)


def attach_collection_to_tracks(tracks, collection):
    if not tracks or not isinstance(collection, dict):
        return tracks

    base_collection = {key: value for key, value in collection.items() if value is not None}
    for index, track in enumerate(tracks, start=1):
        track["_collection"] = dict(base_collection)
        track["_collection_index"] = index
    return tracks


def build_compact_tracklist_chunks(tracks, *, max_chunk_length=950, max_total_length=None):
    chunks = []
    current_lines = []
    current_length = 0
    total_length = 0
    displayed_count = 0

    for index, track in enumerate(tracks, start=1):
        is_youtube_track = track.get("source") == "youtube"
        display_artist = clean_playlist_display_artist(track.get("artist"), source=track.get("source"))
        display_title = (
            compact_youtube_playlist_title(track.get("title"), artist=display_artist)
            if is_youtube_track
            else clean_playlist_display_title(track.get("title"), source=track.get("source"))
        )
        title = sanitize_embed_text(
            truncate_display_text(
                display_title,
                38 if is_youtube_track else 72,
            )
        )
        if is_youtube_track and display_artist and has_any_token_match(display_artist, display_title):
            display_artist = None
        artist = sanitize_embed_text(
            truncate_display_text(display_artist, 20 if is_youtube_track else 42)
        ) if display_artist else ""

        if not artist:
            line = f"`{index:02d}.` **{title}**"
        elif is_youtube_track and len(f"{display_title} {display_artist}") > 44:
            line = f"`{index:02d}.` **{title}**"
        else:
            line = f"`{index:02d}.` **{title}** • {artist}"
        line_length = len(line) + (1 if current_lines else 0)

        if current_lines and current_length + line_length > max_chunk_length:
            chunk = "\n".join(current_lines)
            if max_total_length is not None and total_length + len(chunk) > max_total_length:
                break
            chunks.append(chunk)
            total_length += len(chunk)
            current_lines = []
            current_length = 0
            line_length = len(line)

        if max_total_length is not None and total_length + current_length + line_length > max_total_length:
            break

        current_lines.append(line)
        current_length += line_length
        displayed_count = index

    if current_lines and (max_total_length is None or total_length + len("\n".join(current_lines)) <= max_total_length):
        chunks.append("\n".join(current_lines))

    remaining_count = max(0, len(tracks) - displayed_count)
    return chunks, remaining_count


async def wait_for_voice_client(guild, *, timeout=VOICE_READY_WAIT_SECONDS, poll_interval=VOICE_READY_POLL_SECONDS):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        voice_client = guild.voice_client
        if voice_client and voice_client.is_connected():
            return voice_client
        await asyncio.sleep(poll_interval)
    voice_client = guild.voice_client
    if voice_client and voice_client.is_connected():
        return voice_client
    return None


def should_handle_command(ctx, command_name, *, ttl=COMMAND_INVOCATION_TTL_SECONDS):
    message = getattr(ctx, "message", None)
    message_id = getattr(message, "id", None)
    guild = getattr(ctx, "guild", None)
    guild_id = getattr(guild, "id", 0)
    if not message_id:
        return True

    now = time.monotonic()
    stale_keys = [key for key, expires_at in recent_command_invocations.items() if expires_at <= now]
    for key in stale_keys:
        recent_command_invocations.pop(key, None)

    dedupe_key = (guild_id, str(command_name).lower(), int(message_id))
    expires_at = recent_command_invocations.get(dedupe_key)
    if isinstance(expires_at, (int, float)) and expires_at > now:
        return False

    recent_command_invocations[dedupe_key] = now + max(1.0, float(ttl))
    return True


def command_feedback_key(ctx, base):
    message_id = getattr(getattr(ctx, "message", None), "id", None)
    if message_id:
        return f"{base}:{int(message_id)}"
    return str(base)


def guild_feedback_key(ctx, base):
    guild_id = getattr(getattr(ctx, "guild", None), "id", 0)
    return f"{base}:{int(guild_id)}"


async def send_unique_feedback(ctx, content, *, key=None, window=FEEDBACK_DEDUPE_WINDOW_SECONDS):
    if not content:
        return None

    now = time.monotonic()
    expired_keys = [cache_key for cache_key, expires_at in recent_feedback_messages.items() if expires_at <= now]
    for cache_key in expired_keys:
        recent_feedback_messages.pop(cache_key, None)
        recent_feedback_refs.pop(cache_key, None)

    guild_id = getattr(getattr(ctx, "guild", None), "id", 0)
    channel_id = getattr(getattr(ctx, "channel", None), "id", 0)
    dedupe_key = (guild_id, channel_id, str(key or content))
    expires_at = recent_feedback_messages.get(dedupe_key)
    if isinstance(expires_at, (int, float)) and expires_at > now:
        return recent_feedback_refs.get(dedupe_key)

    jitter = min(max(INSTANCE_FEEDBACK_JITTER_SECONDS, 0.0), max(float(window) * 0.35, 0.0))
    seed_source = f"{_instance_name}:{os.getpid()}:{dedupe_key}"
    jitter_seed = sum(ord(ch) for ch in seed_source)
    jitter += (jitter_seed % 11) * 0.09
    jitter = min(jitter, max(float(window) * 0.8, 0.0))
    if jitter:
        await asyncio.sleep(jitter)
        refreshed_expiry = recent_feedback_messages.get(dedupe_key)
        if isinstance(refreshed_expiry, (int, float)) and refreshed_expiry > time.monotonic():
            return None

    try:
        if bot.user is not None and hasattr(ctx.channel, "history"):
            now_wall = time.time()
            async for message in ctx.channel.history(limit=12):
                if message.author.id != bot.user.id:
                    continue
                age_seconds = now_wall - message.created_at.timestamp()
                if age_seconds > float(window):
                    break
                if str(message.content).strip() == str(content).strip():
                    recent_feedback_messages[dedupe_key] = time.monotonic() + max(1.0, float(window))
                    recent_feedback_refs[dedupe_key] = message
                    return message
    except Exception:
        pass

    sent = await ctx.send(content)
    recent_feedback_messages[dedupe_key] = time.monotonic() + max(1.0, float(window))
    recent_feedback_refs[dedupe_key] = sent
    return sent


async def find_recent_matching_embed_message(ctx, signature, *, key=None, window=EMBED_DEDUPE_WINDOW_SECONDS, limit=10):
    if not signature or bot.user is None or not hasattr(ctx.channel, "history"):
        return None

    try:
        now_wall = time.time()
        async for message in ctx.channel.history(limit=limit):
            if message.author.id != bot.user.id or not message.embeds:
                continue
            age_seconds = now_wall - message.created_at.timestamp()
            if age_seconds > float(window):
                break
            existing_signature = embed_signature(message.embeds[0], key=key)
            if existing_signature == signature:
                return message
    except Exception:
        return None

    return None


def embed_signature(embed, *, key=None):
    if embed is None:
        return None
    footer_text = embed.footer.text if embed.footer else ""
    image_url = embed.image.url if embed.image else ""
    color_value = embed.color.value if embed.color else 0
    parts = [
        str(key or ""),
        str(embed.title or ""),
        str(embed.description or ""),
        str(footer_text or ""),
        str(image_url or ""),
        str(int(color_value)),
    ]
    return "||".join(parts)


async def send_unique_embed(ctx, embed, *, key=None, window=EMBED_DEDUPE_WINDOW_SECONDS):
    signature = embed_signature(embed, key=key)
    if not signature:
        return await ctx.send(embed=embed)

    now = time.monotonic()
    expired_keys = [cache_key for cache_key, expires_at in recent_embed_messages.items() if expires_at <= now]
    for cache_key in expired_keys:
        recent_embed_messages.pop(cache_key, None)
        recent_embed_refs.pop(cache_key, None)

    guild_id = getattr(getattr(ctx, "guild", None), "id", 0)
    channel_id = getattr(getattr(ctx, "channel", None), "id", 0)
    dedupe_key = (guild_id, channel_id, signature)
    expires_at = recent_embed_messages.get(dedupe_key)
    if isinstance(expires_at, (int, float)) and expires_at > now:
        existing_message = recent_embed_refs.get(dedupe_key)
        if existing_message is None:
            existing_message = await find_recent_matching_embed_message(ctx, signature, key=key, window=window)
        if existing_message is not None:
            recent_embed_refs[dedupe_key] = existing_message
            return existing_message
        return None

    existing_message = await find_recent_matching_embed_message(ctx, signature, key=key, window=window)
    if existing_message is not None:
        recent_embed_messages[dedupe_key] = time.monotonic() + max(1.0, float(window))
        recent_embed_refs[dedupe_key] = existing_message
        return existing_message

    sent = await ctx.send(embed=embed)
    recent_embed_messages[dedupe_key] = time.monotonic() + max(1.0, float(window))
    recent_embed_refs[dedupe_key] = sent
    return sent


def schedule_message_embed_suppression(message, *, attempts=12, delay=0.7):
    if message is None or not getattr(message, "id", None):
        return

    message_id = int(message.id)
    existing_task = message_embed_suppression_tasks.get(message_id)
    if existing_task and not existing_task.done():
        return

    async def _worker():
        try:
            total_attempts = max(1, int(attempts))
            retry_delay = max(0.25, float(delay))
            target_message = message
            suppressed_streak = 0
            for attempt_index in range(total_attempts):
                try:
                    try:
                        target_message = await message.channel.fetch_message(message_id)
                    except Exception:
                        target_message = target_message or message

                    updated_message = await target_message.edit(suppress=True)
                    target_message = updated_message or target_message
                    flags = getattr(target_message, "flags", None)
                    if getattr(flags, "suppress_embeds", False):
                        suppressed_streak += 1
                    else:
                        suppressed_streak = 0
                except Exception:
                    suppressed_streak = 0

                # Some providers (notably SoundCloud) can attach additional embeds a
                # few seconds later, so we keep reapplying suppression instead of
                # exiting immediately on the first suppressed state.
                if suppressed_streak >= 3 and attempt_index >= 4:
                    return

                if attempt_index + 1 < total_attempts:
                    sleep_for = min(4.0, retry_delay + (attempt_index * 0.65))
                    await asyncio.sleep(sleep_for)
        finally:
            message_embed_suppression_tasks.pop(message_id, None)

    message_embed_suppression_tasks[message_id] = asyncio.create_task(_worker())


async def fetch_text(url, *, headers=None, timeout=20):
    loop = asyncio.get_event_loop()

    def _do():
        session = http_session()
        response = session.get(url, headers=headers or spotify_headers(), timeout=timeout)
        response.raise_for_status()
        return response.text

    return await loop.run_in_executor(None, _do)


async def fetch_json(url, *, headers=None, timeout=20):
    loop = asyncio.get_event_loop()

    def _do():
        session = http_session()
        response = session.get(url, headers=headers or spotify_headers(), timeout=timeout)
        response.raise_for_status()
        return response.json()

    return await loop.run_in_executor(None, _do)


def decode_json_string(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return json.loads(f'"{text}"')
    except Exception:
        return html.unescape(text.replace("\\u0026", "&"))


def extract_youtube_owner_name_from_html(page_html):
    if not page_html:
        return None

    patterns = [
        r'"ownerChannelName":"([^"]+)"',
        r'"ownerText":\{"runs":\[\{"text":"([^"]+)"',
        r'"shortBylineText":\{"runs":\[\{"text":"([^"]+)"',
        r'"longBylineText":\{"runs":\[\{"text":"([^"]+)"',
        r'"author":"([^"]+)"',
        r'<meta[^>]+itemprop="author"[^>]+content="([^"]+)"',
    ]

    for pattern in patterns:
        match = re.search(pattern, page_html)
        if not match:
            continue
        owner_name = clean_spotify_text(decode_json_string(match.group(1)))
        if owner_name:
            return owner_name
    return None


def extract_youtube_title_from_html(page_html):
    if not page_html:
        return None

    patterns = [
        r'<meta[^>]+property="og:title"[^>]+content="([^"]+)"',
        r'<meta[^>]+name="title"[^>]+content="([^"]+)"',
        r'"title":"([^"]+)"',
        r"<title>([^<]+)</title>",
    ]

    for pattern in patterns:
        match = re.search(pattern, page_html)
        if not match:
            continue
        title = clean_spotify_text(decode_json_string(match.group(1)))
        if title:
            title = re.sub(r"\s*-\s*YouTube\s*$", "", title, flags=re.I)
            title = re.sub(r"\s*-\s*YouTube\s+Music\s*$", "", title, flags=re.I)
            if title:
                return title
    return None


async def resolve_youtube_page_metadata(url):
    if not url or "youtu" not in str(url).lower():
        return {}

    cached_metadata = get_cached(youtube_page_metadata_cache, url)
    if cached_metadata is not None:
        return cached_metadata

    try:
        page_html = await fetch_text(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
                "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
            },
            timeout=15,
        )
    except Exception:
        metadata = {}
        set_cached(youtube_page_metadata_cache, url, metadata, 120)
        return metadata

    metadata = {
        "owner": extract_youtube_owner_name_from_html(page_html),
        "title": extract_youtube_title_from_html(page_html),
    }
    set_cached(youtube_page_metadata_cache, url, metadata, YOUTUBE_CACHE_TTL_SECONDS)
    return metadata


async def resolve_youtube_owner_name(data):
    if not isinstance(data, dict):
        return None

    webpage_url = data.get("webpage_url") or data.get("original_url") or data.get("url")
    if not webpage_url or "youtu" not in str(webpage_url).lower():
        return None

    cached_owner = get_cached(youtube_owner_cache, webpage_url)
    if cached_owner is not None:
        return cached_owner

    metadata = await resolve_youtube_page_metadata(webpage_url)
    owner_name = metadata.get("owner")
    set_cached(youtube_owner_cache, webpage_url, owner_name, YOUTUBE_CACHE_TTL_SECONDS)
    return owner_name


def extract_page_owner_name_from_html(page_html):
    if not page_html:
        return None

    patterns = [
        r'<meta[^>]+name="author"[^>]+content="([^"]+)"',
        r'<meta[^>]+property="music:musician"[^>]+content="([^"]+)"',
        r'<meta[^>]+name="twitter:creator"[^>]+content="([^"]+)"',
        r'"author":"([^"]+)"',
    ]

    for pattern in patterns:
        match = re.search(pattern, page_html, flags=re.I)
        if not match:
            continue
        owner_name = clean_spotify_text(decode_json_string(match.group(1)))
        if owner_name:
            owner_name = owner_name.lstrip("@").strip()
            if owner_name:
                return owner_name
    return None


def extract_page_title_from_html(page_html):
    if not page_html:
        return None

    patterns = [
        r'<meta[^>]+property="og:title"[^>]+content="([^"]+)"',
        r'<meta[^>]+name="twitter:title"[^>]+content="([^"]+)"',
        r'<meta[^>]+name="title"[^>]+content="([^"]+)"',
        r"<title>([^<]+)</title>",
    ]

    for pattern in patterns:
        match = re.search(pattern, page_html, flags=re.I)
        if not match:
            continue
        title = clean_spotify_text(decode_json_string(match.group(1)))
        if title:
            return title
    return None


def extract_page_thumbnail_from_html(page_html):
    if not page_html:
        return None

    patterns = [
        r'<meta[^>]+property="og:image"[^>]+content="([^"]+)"',
        r'<meta[^>]+name="twitter:image"[^>]+content="([^"]+)"',
    ]

    for pattern in patterns:
        match = re.search(pattern, page_html, flags=re.I)
        if not match:
            continue
        thumbnail_url = str(decode_json_string(match.group(1)) or "").strip()
        if thumbnail_url.startswith("http://") or thumbnail_url.startswith("https://"):
            return thumbnail_url
    return None


async def resolve_webpage_media_metadata(url):
    if not url or not looks_like_url(str(url)):
        return {}

    cache_key = str(url).strip()
    cached_metadata = get_cached(webpage_media_metadata_cache, cache_key)
    if cached_metadata is not None:
        return dict(cached_metadata)

    metadata = {}
    lowered_url = cache_key.lower()

    if is_soundcloud_url(lowered_url):
        try:
            oembed_url = f"https://soundcloud.com/oembed?format=json&url={quote(cache_key, safe='')}"
            oembed = await fetch_json(
                oembed_url,
                headers={"User-Agent": "Mozilla/5.0", "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8"},
                timeout=12,
            )
            if isinstance(oembed, dict):
                clean_title, clean_owner = clean_provider_metadata(
                    oembed.get("title"),
                    oembed.get("author_name"),
                    source="soundcloud",
                )
                metadata = {
                    "owner": clean_owner,
                    "title": clean_title,
                    "thumbnail": oembed.get("thumbnail_url"),
                }
        except Exception:
            metadata = {}

    if not metadata.get("title") or not metadata.get("owner") or not metadata.get("thumbnail"):
        try:
            page_html = await fetch_text(
                cache_key,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
                    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
                },
                timeout=15,
            )
        except Exception:
            page_html = None

        if page_html:
            metadata = {
                "owner": metadata.get("owner") or extract_page_owner_name_from_html(page_html),
                "title": metadata.get("title") or extract_page_title_from_html(page_html),
                "thumbnail": metadata.get("thumbnail") or extract_page_thumbnail_from_html(page_html),
            }

    if is_soundcloud_url(lowered_url):
        clean_title, clean_owner = clean_provider_metadata(
            metadata.get("title"),
            metadata.get("owner"),
            source="soundcloud",
        )
        metadata["title"] = clean_title
        metadata["owner"] = clean_owner

    set_cached(
        webpage_media_metadata_cache,
        cache_key,
        dict(metadata),
        YOUTUBE_CACHE_TTL_SECONDS if any(metadata.values()) else 180,
    )
    return metadata


def rgb_to_int(red, green, blue):
    return (red << 16) + (green << 8) + blue


def int_to_rgb(color_value):
    try:
        color_value = int(color_value)
    except Exception:
        color_value = 0x2B2D31
    red = (color_value >> 16) & 0xFF
    green = (color_value >> 8) & 0xFF
    blue = color_value & 0xFF
    return red, green, blue


def extract_image_color(image_bytes):
    if Image is None:
        return None
    try:
        with Image.open(BytesIO(image_bytes)) as image:
            image = image.convert("RGB")
            image.thumbnail((24, 24))
            raw_pixels = image.get_flattened_data() if hasattr(image, "get_flattened_data") else image.getdata()
    except Exception:
        return None

    pixels = []
    for pixel in raw_pixels:
        if isinstance(pixel, int):
            red = green = blue = int(pixel)
        elif isinstance(pixel, (tuple, list)):
            if len(pixel) >= 3:
                red, green, blue = int(pixel[0]), int(pixel[1]), int(pixel[2])
            elif len(pixel) == 1:
                red = green = blue = int(pixel[0])
            else:
                continue
        else:
            continue
        pixels.append((red, green, blue))

    if not pixels:
        return None

    # Favor brighter, more saturated pixels to avoid muddy embed colors.
    filtered = []
    for red, green, blue in pixels:
        brightness = red + green + blue
        saturation = max(red, green, blue) - min(red, green, blue)
        if brightness > 60 and saturation > 20:
            filtered.append((red, green, blue))

    palette = filtered or pixels
    red = int(sum(pixel[0] for pixel in palette) / len(palette))
    green = int(sum(pixel[1] for pixel in palette) / len(palette))
    blue = int(sum(pixel[2] for pixel in palette) / len(palette))
    return rgb_to_int(red, green, blue)


def thumbnail_url_variants(url):
    if not isinstance(url, str):
        return []

    base = url.strip()
    if not base:
        return []

    variants = []
    seen = set()

    def push(candidate):
        if not isinstance(candidate, str):
            return
        normalized = candidate.strip()
        if not normalized:
            return
        if normalized in seen:
            return
        seen.add(normalized)
        variants.append(normalized)

    push(base)
    push(base.split("?", 1)[0])

    lowered = base.lower()
    if "ytimg.com" in lowered:
        no_query = base.split("?", 1)[0]
        push(no_query.replace(".webp", ".jpg"))
        if "/vi/" in no_query:
            for name in ("maxresdefault.jpg", "hqdefault.jpg", "mqdefault.jpg", "sddefault.jpg", "default.jpg"):
                push(re.sub(r"/[^/]+\.((jpg)|(webp))$", f"/{name}", no_query, flags=re.I))

    return variants


def extract_thumbnail_urls(value):
    urls = []
    seen = set()

    def add(url):
        for candidate in thumbnail_url_variants(url):
            if candidate not in seen:
                seen.add(candidate)
                urls.append(candidate)

    def walk(item):
        if item is None:
            return
        if isinstance(item, str):
            if item.startswith("http://") or item.startswith("https://"):
                add(item)
            return
        if isinstance(item, dict):
            for key in (
                "url",
                "thumbnail",
                "thumbnail_url",
                "image_url",
                "image",
                "image_large",
                "artwork_url",
                "artwork",
                "avatar_url",
                "cover",
                "cover_url",
                "cover_small",
                "cover_medium",
                "cover_big",
                "cover_xl",
                "picture",
                "picture_small",
                "picture_medium",
                "picture_big",
                "picture_xl",
                "large",
                "xl",
            ):
                value_at_key = item.get(key)
                if isinstance(value_at_key, str):
                    add(value_at_key)
            for key in ("thumbnails", "images", "sources", "coverArt", "album", "visualIdentity"):
                nested = item.get(key)
                if nested is not None:
                    walk(nested)
            return
        if isinstance(item, (list, tuple, set)):
            for nested in item:
                walk(nested)

    walk(value)
    return urls


def youtube_thumbnail_candidates_from_data(data):
    if not isinstance(data, dict):
        return []

    video_id = str(data.get("id") or "").strip()
    if not video_id:
        webpage_url = str(data.get("webpage_url") or data.get("original_url") or "").strip()
        parsed = urlparse(webpage_url)
        if parsed.netloc:
            query_id = parse_qs(parsed.query).get("v", [None])[0]
            if query_id:
                video_id = str(query_id).strip()
            elif parsed.path and "youtu.be" in parsed.netloc:
                video_id = parsed.path.strip("/").split("/")[0]

    if not video_id:
        return []

    base = f"https://i.ytimg.com/vi/{video_id}"
    return [
        f"{base}/maxresdefault.jpg",
        f"{base}/hqdefault.jpg",
        f"{base}/mqdefault.jpg",
        f"{base}/sddefault.jpg",
        f"{base}/default.jpg",
    ]


def youtube_thumbnail_candidates_from_url(url):
    if not isinstance(url, str):
        return []
    text = url.strip()
    if not text:
        return []

    parsed = urlparse(text)
    video_id = None
    if parsed.netloc:
        query_id = parse_qs(parsed.query).get("v", [None])[0]
        if query_id:
            video_id = str(query_id).strip()
        elif "youtu.be" in parsed.netloc and parsed.path:
            video_id = parsed.path.strip("/").split("/")[0]

    if not video_id:
        return []

    base = f"https://i.ytimg.com/vi/{video_id}"
    return [
        f"{base}/maxresdefault.jpg",
        f"{base}/hqdefault.jpg",
        f"{base}/mqdefault.jpg",
        f"{base}/sddefault.jpg",
        f"{base}/default.jpg",
    ]


def find_cached_thumbnail_color(value):
    for candidate in extract_thumbnail_urls(value):
        cached_color = thumbnail_color_cache.get(candidate)
        if isinstance(cached_color, int) and 0 <= cached_color <= 0xFFFFFF:
            return cached_color
    return None


async def get_thumbnail_color(url, fallback):
    urls = extract_thumbnail_urls(url)
    if not urls:
        return fallback
    for candidate in urls:
        if candidate in thumbnail_color_cache:
            return thumbnail_color_cache[candidate]

    loop = asyncio.get_event_loop()

    def _do(candidate):
        session = http_session()
        attempts = [
            {"headers": image_request_headers(candidate), "timeout": 12, "allow_redirects": True},
            {"headers": spotify_headers(), "timeout": 12, "allow_redirects": True},
            {"headers": None, "timeout": 12, "allow_redirects": True},
            {"headers": image_request_headers(candidate), "timeout": 12, "allow_redirects": True, "verify": False},
        ]

        last_error = None
        for kwargs in attempts:
            try:
                response = session.get(candidate, **kwargs)
                response.raise_for_status()
                color = extract_image_color(response.content)
                if color is not None:
                    return color
            except Exception as exc:
                last_error = exc
                continue

        try:
            request_headers = image_request_headers(candidate)
            request = Request(candidate, headers=request_headers)
            insecure_context = ssl._create_unverified_context()
            opener = build_opener(ProxyHandler({}), HTTPSHandler(context=insecure_context))
            with opener.open(request, timeout=12) as response:
                payload = response.read()
            color = extract_image_color(payload)
            if color is not None:
                return color
        except Exception as exc:
            last_error = exc

        if last_error:
            raise last_error
        return None

    for candidate in urls:
        try:
            color = await loop.run_in_executor(None, lambda candidate_url=candidate: _do(candidate_url))
        except Exception:
            color = None

        if color is None:
            continue

        for cache_candidate in urls:
            thumbnail_color_cache[cache_candidate] = color
        return color

    # Do not permanently cache fallback values when image fetch fails.
    # This allows transient CDN/network failures to recover on next attempt.
    return fallback


def load_card_font(size, *, bold=False):
    if ImageFont is None:
        return None

    windows_fonts = [
        ("segoeuib.ttf" if bold else "segoeui.ttf"),
        ("arialbd.ttf" if bold else "arial.ttf"),
    ]
    fonts_dir = os.path.join(os.environ.get("WINDIR", "C:\\Windows"), "Fonts")

    for font_name in windows_fonts:
        font_path = os.path.join(fonts_dir, font_name)
        if os.path.isfile(font_path):
            try:
                return ImageFont.truetype(font_path, size=size)
            except Exception:
                pass

    try:
        return ImageFont.load_default()
    except Exception:
        return None


def wrap_card_text(draw, text, font, max_width):
    if not text:
        return []

    wrapped_lines = []
    paragraphs = str(text).splitlines() or [str(text)]
    for paragraph in paragraphs:
        words = paragraph.split()
        if not words:
            wrapped_lines.append("")
            continue

        current_line = words[0]
        for word in words[1:]:
            candidate = f"{current_line} {word}"
            try:
                line_width = draw.textbbox((0, 0), candidate, font=font)[2]
            except Exception:
                line_width = len(candidate) * 8

            if line_width <= max_width:
                current_line = candidate
            else:
                wrapped_lines.append(current_line)
                current_line = word
        wrapped_lines.append(current_line)

    return wrapped_lines


async def build_now_playing_card_file(
    thumbnail_url,
    *,
    accent_color,
    artist_label,
    artist_value,
    duration_value,
    source_value,
    filename="now_playing_card.png",
):
    if not thumbnail_url or Image is None or ImageDraw is None or ImageFont is None:
        return None

    loop = asyncio.get_event_loop()

    def _do():
        session = http_session()
        response = session.get(thumbnail_url, headers=spotify_headers(), timeout=15)
        response.raise_for_status()

        with Image.open(BytesIO(response.content)) as thumbnail:
            thumbnail = thumbnail.convert("RGB")
            card_width = 720
            ratio = card_width / max(thumbnail.width, 1)
            image_height = max(220, int(thumbnail.height * ratio))
            thumbnail = thumbnail.resize((card_width, image_height))

            accent_rgb = int_to_rgb(accent_color)
            background_rgb = (18, 18, 20)
            label_rgb = (170, 175, 186)
            value_rgb = (244, 246, 248)

            label_font = load_card_font(26, bold=True)
            value_font = load_card_font(30, bold=False)

            padding_x = 38
            block_gap = 24
            line_gap = 10
            section_gap = 18
            text_width = card_width - (padding_x * 2)

            draw_probe = ImageDraw.Draw(Image.new("RGB", (card_width, 10)))
            sections = [
                (artist_label, artist_value or "Inconnu"),
                ("Durée", duration_value or "?"),
                ("Source", source_value or "Inconnu"),
            ]

            rendered_sections = []
            info_height = 28
            for label, value in sections:
                value_lines = wrap_card_text(draw_probe, value, value_font, text_width)
                rendered_sections.append((label, value_lines))
                label_box = draw_probe.textbbox((0, 0), label, font=label_font)
                label_height = label_box[3] - label_box[1]
                info_height += label_height + line_gap
                for value_line in value_lines:
                    value_box = draw_probe.textbbox((0, 0), value_line or " ", font=value_font)
                    info_height += (value_box[3] - value_box[1]) + 6
                info_height += section_gap

            card_height = image_height + block_gap + info_height + 14
            card = Image.new("RGB", (card_width, card_height), background_rgb)
            card.paste(thumbnail, (0, 0))

            draw = ImageDraw.Draw(card)
            draw.rectangle((0, image_height, card_width, image_height + 8), fill=accent_rgb)

            cursor_y = image_height + block_gap
            for label, value_lines in rendered_sections:
                draw.text((padding_x, cursor_y), label, font=label_font, fill=label_rgb)
                label_box = draw.textbbox((padding_x, cursor_y), label, font=label_font)
                cursor_y = label_box[3] + line_gap
                for value_line in value_lines:
                    draw.text((padding_x, cursor_y), value_line, font=value_font, fill=value_rgb)
                    value_box = draw.textbbox((padding_x, cursor_y), value_line or " ", font=value_font)
                    cursor_y = value_box[3] + 6
                cursor_y += section_gap

            output = BytesIO()
            card.save(output, format="PNG")
            output.seek(0)
            return output.getvalue()

    try:
        image_bytes = await loop.run_in_executor(None, _do)
    except Exception:
        return None

    return discord.File(BytesIO(image_bytes), filename=filename)


async def build_now_playing_card_visual(
    thumbnail_url,
    *,
    title_text,
    accent_color,
    artist_label,
    artist_value,
    duration_value,
    source_value,
    filename="now_playing_card.png",
):
    if not thumbnail_url or Image is None or ImageDraw is None or ImageFont is None:
        return None

    loop = asyncio.get_event_loop()

    def _do():
        session = http_session()
        response = session.get(thumbnail_url, headers=spotify_headers(), timeout=15)
        response.raise_for_status()

        with Image.open(BytesIO(response.content)) as thumbnail:
            thumbnail = thumbnail.convert("RGB")
            card_width = 720
            ratio = card_width / max(thumbnail.width, 1)
            image_height = max(260, int(thumbnail.height * ratio))
            thumbnail = thumbnail.resize((card_width, image_height))

            accent_rgb = int_to_rgb(accent_color)
            background_rgb = (18, 18, 20)
            title_rgb = (244, 246, 248)
            info_rgb = (215, 220, 228)

            title_font = load_card_font(48, bold=True)
            info_font = load_card_font(30, bold=False)

            padding_x = 38
            top_gap = 28
            block_gap = 26
            text_width = card_width - (padding_x * 2)

            draw_probe = ImageDraw.Draw(Image.new("RGB", (card_width, 10)))
            title_lines = wrap_card_text(draw_probe, title_text or "Titre inconnu", title_font, text_width)
            if len(title_lines) > 2:
                title_lines = title_lines[:2]
                last_line = title_lines[-1]
                while last_line and draw_probe.textbbox((0, 0), f"{last_line}…", font=title_font)[2] > text_width:
                    last_line = last_line[:-1].rstrip()
                title_lines[-1] = f"{last_line}…" if last_line else "…"

            info_line = (
                f"{artist_label}: {artist_value or 'Inconnu'}"
                f" • Durée: {duration_value or '?'}"
                f" • Source: {source_value or 'Inconnu'}"
            )
            info_lines = wrap_card_text(draw_probe, info_line, info_font, text_width)

            title_height = 0
            for line in title_lines:
                title_box = draw_probe.textbbox((0, 0), line or " ", font=title_font)
                title_height += (title_box[3] - title_box[1]) + 8

            info_height = 0
            for line in info_lines:
                info_box = draw_probe.textbbox((0, 0), line or " ", font=info_font)
                info_height += (info_box[3] - info_box[1]) + 8

            card_height = top_gap + title_height + block_gap + image_height + block_gap + info_height + 24
            card = Image.new("RGB", (card_width, card_height), background_rgb)
            draw = ImageDraw.Draw(card)

            cursor_y = top_gap
            for line in title_lines:
                draw.text((padding_x, cursor_y), line, font=title_font, fill=title_rgb)
                title_box = draw.textbbox((padding_x, cursor_y), line or " ", font=title_font)
                cursor_y = title_box[3] + 8

            cursor_y += block_gap
            card.paste(thumbnail, (0, cursor_y))
            image_bottom = cursor_y + image_height
            draw.rectangle((0, image_bottom, card_width, image_bottom + 8), fill=accent_rgb)

            cursor_y = image_bottom + block_gap
            for line in info_lines:
                draw.text((padding_x, cursor_y), line, font=info_font, fill=info_rgb)
                info_box = draw.textbbox((padding_x, cursor_y), line or " ", font=info_font)
                cursor_y = info_box[3] + 8

            output = BytesIO()
            card.save(output, format="PNG")
            output.seek(0)
            return output.getvalue()

    try:
        image_bytes = await loop.run_in_executor(None, _do)
    except Exception:
        return None

    return discord.File(BytesIO(image_bytes), filename=filename)


def is_spotify_url(url: str) -> bool:
    lowered = url.lower()
    return "spotify.com" in lowered or lowered.startswith("spotify:")


def is_soundcloud_url(url: str) -> bool:
    lowered = url.lower()
    return "soundcloud.com" in lowered or "snd.sc" in lowered


def is_youtube_url(url: str) -> bool:
    lowered = url.lower()
    return "youtube.com" in lowered or "youtu.be" in lowered


def is_deezer_url(url: str) -> bool:
    lowered = url.lower()
    return "deezer.com" in lowered or "link.deezer.com" in lowered or "deezer.page.link" in lowered


def is_bandcamp_url(url: str) -> bool:
    return "bandcamp.com" in url.lower()


def looks_like_url(value: str) -> bool:
    return bool(re.match(r"^https?://", value.strip()))


def extract_youtube_video_id(url: str):
    if not looks_like_url(url):
        return None

    parsed = urlparse(url)
    host = (parsed.netloc or "").lower()
    path_segments = [segment for segment in parsed.path.strip("/").split("/") if segment]

    if "youtu.be" in host and path_segments:
        return path_segments[0]

    if "youtube.com" not in host:
        return None

    if parsed.path == "/watch":
        query = parse_qs(parsed.query)
        video_ids = query.get("v")
        return video_ids[0] if video_ids else None

    if len(path_segments) >= 2 and path_segments[0] in {"shorts", "embed", "live"}:
        return path_segments[1]

    return None


def youtube_extraction_urls(url: str):
    candidates = []

    def add(candidate):
        if candidate and candidate not in candidates:
            candidates.append(candidate)

    add(url)

    if not is_youtube_url(url):
        return candidates

    video_id = extract_youtube_video_id(url)
    if video_id:
        add(f"https://www.youtube.com/watch?v={video_id}")
        add(f"https://www.youtube.com/shorts/{video_id}")
        add(f"https://youtu.be/{video_id}")

    return candidates


def extract_youtube_playlist_id(url: str):
    if not looks_like_url(url) or not is_youtube_url(url):
        return None

    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    playlist_ids = query.get("list")
    return playlist_ids[0] if playlist_ids else None


def is_youtube_playlist_url(url: str) -> bool:
    return bool(extract_youtube_playlist_id(url))


def infer_source_from_input(value: str):
    lowered = value.lower()
    if is_soundcloud_url(lowered):
        return "soundcloud"
    if is_bandcamp_url(lowered):
        return "bandcamp"
    return "youtube"


def normalize_spotify_target(url_or_id: str):
    raw = url_or_id.strip()

    if raw.startswith("spotify:"):
        parts = raw.split(":")
        if len(parts) >= 3:
            return parts[1], parts[2]
        return None, None

    if re.fullmatch(r"[A-Za-z0-9]{22}", raw):
        return "playlist", raw

    parsed = urlparse(raw)
    segments = [segment for segment in parsed.path.strip("/").split("/") if segment]
    if not segments:
        return None, None

    if segments[0].startswith("intl-") and len(segments) >= 3:
        segments = segments[1:]

    if len(segments) >= 2 and segments[0] in {"track", "album", "playlist"}:
        return segments[0], segments[1]

    if len(segments) >= 3 and segments[0] == "embed" and segments[1] in {"track", "album", "playlist"}:
        return segments[1], segments[2]

    return None, None


def spotify_public_url(kind: str, item_id: str) -> str:
    return f"https://open.spotify.com/{kind}/{item_id}"


def spotify_oembed_url(kind: str, item_id: str) -> str:
    return f"https://open.spotify.com/oembed?url={quote(spotify_public_url(kind, item_id), safe='')}"


def spotify_embed_url(kind: str, item_id: str) -> str:
    return f"https://open.spotify.com/embed/{kind}/{item_id}?utm_source=oembed"


def clean_spotify_text(value):
    if not value:
        return None
    text = str(value).strip()
    text = re.sub(r"\s*\|\s*Spotify\s*$", "", text, flags=re.I)
    text = text.replace("â€“", "-").replace("Â·", "·")
    return text.strip() or None


def strip_artist_prefix_from_title(title, artist):
    clean_title = clean_spotify_text(title)
    clean_artist = clean_spotify_text(artist)
    if not clean_title or not clean_artist:
        return clean_title

    stripped = re.sub(
        rf"^\s*{re.escape(clean_artist)}\s*[-–—:|]\s*",
        "",
        clean_title,
        flags=re.I,
    ).strip()

    if stripped and stripped != clean_title:
        return stripped
    return clean_title


def clean_provider_metadata(title, artist=None, source=None):
    clean_title = clean_spotify_text(html.unescape(str(title))) if title else None
    clean_artist = clean_spotify_text(html.unescape(str(artist))) if artist else None
    provider = str(source or "").strip().lower()

    if provider == "soundcloud":
        if clean_title:
            clean_title = re.sub(
                r"\s+by\s+.+?\s+on\s+soundcloud\s*$",
                "",
                clean_title,
                flags=re.I,
            ).strip()
            clean_title = re.sub(
                r"\s*\|\s*listen online.*$",
                "",
                clean_title,
                flags=re.I,
            ).strip()
            clean_title = strip_artist_prefix_from_title(clean_title, clean_artist)
            clean_title = clean_title.strip(" -–—:|") or None

        if clean_artist and clean_title:
            normalized_title = re.sub(r"[\W_]+", "", clean_title, flags=re.I).lower()
            normalized_artist = re.sub(r"[\W_]+", "", clean_artist, flags=re.I).lower()
            if normalized_title == normalized_artist:
                clean_title = None

    return clean_title, clean_artist


def parse_artist_and_title(text):
    cleaned = clean_spotify_text(text)
    if not cleaned:
        return None, None

    patterns = [
        r"^(?P<title>.+?)\s*-\s*song and lyrics by\s+(?P<artist>.+)$",
        r"^(?P<title>.+?)\s*-\s*song by\s+(?P<artist>.+)$",
        r"^(?P<title>.+?)\s*[·|]\s*song by\s+(?P<artist>.+)$",
        r"^(?P<title>.+?)\s*[·|]\s*(?P<artist>.+)$",
        r"^(?P<artist>.+?)\s*-\s*(?P<title>.+)$",
    ]
    for pattern in patterns:
        match = re.match(pattern, cleaned, flags=re.I)
        if match:
            return clean_spotify_text(match.group("artist")), clean_spotify_text(match.group("title"))

    match = re.search(r"(?:song by|by)\s+(?P<artist>.+)$", cleaned, flags=re.I)
    if match:
        return clean_spotify_text(match.group("artist")), None

    return None, cleaned


def decode_spotify_state(html: str):
    patterns = [
        r'<script id="initial-state" type="text/plain">([\s\S]+?)</script>',
        r'<script id="initial-state" type="application/json">([\s\S]+?)</script>',
        r"Spotify\.Entity\s*=\s*([\s\S]+?);",
    ]
    for pattern in patterns:
        match = re.search(pattern, html)
        if not match:
            continue
        raw = match.group(1).strip()
        candidates = [raw, raw.replace("&quot;", '"').replace("&amp;", "&")]
        for candidate in candidates:
            try:
                return json.loads(candidate)
            except Exception:
                pass
            try:
                decoded = base64.b64decode(candidate).decode("utf-8")
                return json.loads(decoded)
            except Exception:
                pass
    return None


def decode_spotify_next_data(html: str):
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">([\s\S]+?)</script>', html)
    if not match:
        return None
    try:
        return json.loads(match.group(1))
    except Exception:
        return None


def walk_json(value):
    if isinstance(value, dict):
        yield value
        for nested in value.values():
            yield from walk_json(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from walk_json(nested)


def extract_spotify_meta(html):
    if not html:
        return {}

    meta = {}
    patterns = {
        "og_title": r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']',
        "og_description": r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)["\']',
        "twitter_title": r'<meta[^>]+name=["\']twitter:title["\'][^>]+content=["\']([^"\']+)["\']',
        "music_musician": r'<meta[^>]+name=["\']music:musician["\'][^>]+content=["\']([^"\']+)["\']',
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, html, flags=re.I)
        if match:
            meta[key] = match.group(1).strip()

    json_blocks = re.findall(r'<script type=["\']application/ld\+json["\']>([\s\S]+?)</script>', html, flags=re.I)
    for block in json_blocks:
        try:
            payload = json.loads(block.strip())
        except Exception:
            continue
        nodes = payload if isinstance(payload, list) else [payload]
        for node in nodes:
            if not isinstance(node, dict):
                continue
            if "ld_title" not in meta and node.get("name"):
                meta["ld_title"] = str(node["name"]).strip()
            artist_value = node.get("byArtist") or node.get("artist")
            if isinstance(artist_value, dict) and artist_value.get("name"):
                meta.setdefault("ld_artist", artist_value["name"].strip())
            elif isinstance(artist_value, list) and artist_value and isinstance(artist_value[0], dict) and artist_value[0].get("name"):
                meta.setdefault("ld_artist", artist_value[0]["name"].strip())
            elif isinstance(artist_value, str):
                meta.setdefault("ld_artist", artist_value.strip())
    return meta


def find_spotify_entity(state, kind: str, item_id: str):
    target_uri = f"spotify:{kind}:{item_id}"
    target_key = f"spotify:{kind}:{item_id}"
    for node in walk_json(state):
        if not isinstance(node, dict):
            continue
        if node.get("uri") == target_uri or node.get("entityUri") == target_uri or node.get("spotifyUri") == target_uri:
            return node
        if node.get("id") == item_id and node.get("type") == kind:
            return node
        if target_key in node and isinstance(node[target_key], dict):
            return node[target_key]
    return None


def spotify_track_from_meta(title, artist, thumbnail=None, duration=None, color=None):
    clean_title = clean_spotify_text(title) or "Titre inconnu"
    clean_artist = clean_spotify_text(artist) or "Artiste inconnu"
    return {
        "query": f'"{clean_artist}" "{clean_title}" audio',
        "title": clean_title,
        "artist": clean_artist,
        "thumbnail": thumbnail,
        "duration": duration,
        "color": color,
        "source": "spotify",
    }


def spotify_collection_kind_label(kind):
    return {
        "playlist": "Playlist",
        "album": "Album",
    }.get(kind, "Collection")


def spotify_track_thumbnail_from_item(item, fallback=None):
    if not isinstance(item, dict):
        return fallback

    candidates = [
        spotify_thumbnail_from_entity(item),
        spotify_thumbnail_from_entity(item.get("album") if isinstance(item.get("album"), dict) else {}),
        spotify_thumbnail_from_entity(item.get("albumOfTrack") if isinstance(item.get("albumOfTrack"), dict) else {}),
    ]

    for candidate in candidates:
        if candidate:
            return candidate

    for container in (item, item.get("album"), item.get("albumOfTrack")):
        if not isinstance(container, dict):
            continue

        direct_url = container.get("thumbnail") or container.get("image")
        if isinstance(direct_url, str) and direct_url:
            return direct_url

        images = container.get("images")
        if isinstance(images, list):
            for image in images:
                if isinstance(image, dict) and image.get("url"):
                    return image["url"]

    return fallback


def spotify_collection_from_entity(entity, kind, *, fallback_title=None, fallback_thumbnail=None, track_count=None):
    if not isinstance(entity, dict):
        return None

    title = clean_spotify_text(entity.get("name") or entity.get("title")) or clean_spotify_text(fallback_title)
    thumbnail = spotify_thumbnail_from_entity(entity, fallback=fallback_thumbnail)
    subtitle = clean_spotify_text(entity.get("subtitle"))
    color = spotify_color_from_entity(entity)

    return {
        "kind": kind,
        "label": spotify_collection_kind_label(kind),
        "title": title or spotify_collection_kind_label(kind),
        "subtitle": subtitle,
        "thumbnail": thumbnail,
        "color": color,
        "source": "spotify",
        "count": track_count,
    }


def spotify_track_from_embed_item(item, *, fallback_thumbnail=None):
    title = clean_spotify_text(item.get("title") or item.get("name")) or "Titre inconnu"

    artist = None
    artists = item.get("artists")
    if isinstance(artists, list) and artists and isinstance(artists[0], dict):
        artist = clean_spotify_text(artists[0].get("name"))
    if not artist:
        artist = clean_spotify_text(item.get("subtitle"))

    duration = item.get("duration")
    if isinstance(duration, (int, float)):
        duration_value = int(duration // 1000) if duration > 1000 else int(duration)
        if item.get("audioPreview") and item.get("artists") and duration_value <= 60:
            duration_value = None
    else:
        duration_value = None

    return spotify_track_from_meta(
        title,
        artist,
        thumbnail=spotify_track_thumbnail_from_item(item, fallback=fallback_thumbnail),
        duration=duration_value,
    )


def spotify_color_from_entity(entity):
    visual = entity.get("visualIdentity")
    if not isinstance(visual, dict):
        return None
    base = visual.get("backgroundBase")
    if not isinstance(base, dict):
        return None
    red = base.get("red")
    green = base.get("green")
    blue = base.get("blue")
    if all(isinstance(channel, int) for channel in (red, green, blue)):
        return rgb_to_int(red, green, blue)
    return None


def spotify_thumbnail_from_entity(entity, fallback=None):
    cover_art = entity.get("coverArt")
    if isinstance(cover_art, dict):
        sources = cover_art.get("sources")
        if isinstance(sources, list):
            for source in reversed(sources):
                if isinstance(source, dict) and source.get("url"):
                    return source["url"]

    visual = entity.get("visualIdentity")
    if isinstance(visual, dict):
        images = visual.get("image")
        if isinstance(images, list):
            for image in reversed(images):
                if isinstance(image, dict) and image.get("url"):
                    return image["url"]

    return fallback


def normalize_spotify_track(track, *, fallback_thumbnail=None):
    title = clean_spotify_text(track.get("name")) or "Titre inconnu"
    artists = track.get("artists") or []
    artist_name = None
    if artists and isinstance(artists[0], dict):
        artist_name = clean_spotify_text(artists[0].get("name"))

    album = track.get("album") if isinstance(track.get("album"), dict) else {}
    album_images = album.get("images") if isinstance(album.get("images"), list) else []
    track_images = track.get("images") if isinstance(track.get("images"), list) else []
    thumbnail = (
        track.get("thumbnail")
        or track.get("image")
        or (track_images[0].get("url") if track_images else None)
        or (album_images[0].get("url") if album_images else None)
        or fallback_thumbnail
    )

    duration_ms = track.get("duration_ms")
    duration_seconds = track.get("duration")
    if isinstance(duration_ms, (int, float)):
        duration = int(duration_ms // 1000)
    elif isinstance(duration_seconds, (int, float)):
        duration = int(duration_seconds)
    else:
        duration = None

    return spotify_track_from_meta(title, artist_name, thumbnail=thumbnail, duration=duration)


def extract_spotify_items(entity, *, fallback_thumbnail=None):
    if not isinstance(entity, dict):
        return []

    containers = [
        entity.get("tracks"),
        entity.get("items"),
        entity.get("trackList"),
    ]

    for container in containers:
        if isinstance(container, dict):
            items = container.get("items") or container.get("tracks") or container.get("entities")
        elif isinstance(container, list):
            items = container
        else:
            items = None
        if not items:
            continue

        results = []
        for item in items:
            if not item:
                continue
            track = item.get("track") if isinstance(item, dict) and isinstance(item.get("track"), dict) else item
            if not isinstance(track, dict) or not track.get("name"):
                continue
            results.append(normalize_spotify_track(track, fallback_thumbnail=fallback_thumbnail))
        if results:
            return results

    return []


async def spotify_to_tracks(url_or_id: str):
    kind, item_id = normalize_spotify_target(url_or_id)
    if not kind:
        raise RuntimeError("Lien Spotify non reconnu.")

    cache_key = (kind, item_id)
    cached_tracks = get_cached(spotify_tracks_cache, cache_key)
    if cached_tracks is not None:
        return clone_tracks(cached_tracks)

    public_url = spotify_public_url(kind, item_id)
    embed_url = spotify_embed_url(kind, item_id)
    html = None
    oembed_result, embed_result = await asyncio.gather(
        fetch_json(spotify_oembed_url(kind, item_id)),
        fetch_text(embed_url),
        return_exceptions=True,
    )
    oembed = None if isinstance(oembed_result, Exception) else oembed_result
    embed_html = None if isinstance(embed_result, Exception) else embed_result

    if embed_html:
        next_data = decode_spotify_next_data(embed_html)
        if next_data:
            entity = (
                next_data.get("props", {})
                .get("pageProps", {})
                .get("state", {})
                .get("data", {})
                .get("entity", {})
            )
            if isinstance(entity, dict) and entity:
                thumbnail = spotify_thumbnail_from_entity(entity, fallback=(oembed.get("thumbnail_url") if oembed else None))
                color = spotify_color_from_entity(entity)

                if kind == "track":
                    track = spotify_track_from_embed_item(entity, fallback_thumbnail=thumbnail)
                    track["color"] = color
                    tracks = [track]
                    set_cached(spotify_tracks_cache, cache_key, clone_tracks(tracks), TRACK_CACHE_TTL_SECONDS)
                    return tracks

                track_list = entity.get("trackList")
                if isinstance(track_list, list):
                    tracks = []
                    for item in track_list:
                        if not isinstance(item, dict) or not (item.get("title") or item.get("name")):
                            continue
                        track = spotify_track_from_embed_item(item, fallback_thumbnail=thumbnail)
                        track["color"] = color
                        tracks.append(track)
                    if tracks:
                        attach_collection_to_tracks(
                            tracks,
                            spotify_collection_from_entity(
                                entity,
                                kind,
                                fallback_title=oembed.get("title") if oembed else None,
                                fallback_thumbnail=thumbnail,
                                track_count=len(tracks),
                            ),
                        )
                        set_cached(spotify_tracks_cache, cache_key, clone_tracks(tracks), TRACK_CACHE_TTL_SECONDS)
                        return tracks

    try:
        html = await fetch_text(public_url)
    except Exception:
        html = None

    meta = extract_spotify_meta(html)
    thumbnail = oembed.get("thumbnail_url") if oembed else None

    if kind == "track":
        raw_title = oembed.get("title") if oembed else None
        raw_artist = oembed.get("author_name") if oembed else None

        if not raw_title:
            raw_title = meta.get("og_title") or meta.get("twitter_title") or meta.get("ld_title")
        if not raw_artist:
            raw_artist = meta.get("music_musician") or meta.get("ld_artist")

        artist_from_desc, title_from_desc = parse_artist_and_title(meta.get("og_description"))
        artist_from_title, title_from_title = parse_artist_and_title(raw_title)

        title = clean_spotify_text(raw_title) or title_from_desc or title_from_title
        artist = clean_spotify_text(raw_artist) or artist_from_desc or artist_from_title

        if not title:
            raise RuntimeError("Impossible de lire les infos Spotify du morceau.")

        tracks = [spotify_track_from_meta(title, artist, thumbnail=thumbnail, duration=None)]
        set_cached(spotify_tracks_cache, cache_key, clone_tracks(tracks), TRACK_CACHE_TTL_SECONDS)
        return tracks

    if not html:
        raise RuntimeError("Impossible de lire la page Spotify.")

    state = decode_spotify_state(html)
    if not state:
        raise RuntimeError("Impossible d'extraire la playlist ou l'album Spotify.")

    entity = find_spotify_entity(state, kind, item_id) or state
    tracks = extract_spotify_items(entity, fallback_thumbnail=thumbnail)
    if tracks:
        attach_collection_to_tracks(
            tracks,
            spotify_collection_from_entity(
                entity,
                kind,
                fallback_title=oembed.get("title") if oembed else None,
                fallback_thumbnail=thumbnail,
                track_count=len(tracks),
            ),
        )
        set_cached(spotify_tracks_cache, cache_key, clone_tracks(tracks), TRACK_CACHE_TTL_SECONDS)
        return tracks

    raise RuntimeError("Aucun morceau Spotify n'a pu être extrait.")


def resolve_deezer_share_link(url: str) -> str:
    def normalize_deezer_url(candidate: str):
        if not candidate:
            return None
        clean = html.unescape(str(candidate).strip())
        clean = clean.replace("deezer://", "https://")
        clean = clean.split("?")[0]
        clean = re.sub(r"(deezer\.com)/(?:[a-z]{2})/", r"\1/", clean, flags=re.I)
        if not re.search(r"deezer\.com/(track|album|playlist)/\d+", clean, flags=re.I):
            return None
        if not clean.startswith("http://") and not clean.startswith("https://"):
            clean = f"https://{clean.lstrip('/')}"
        return clean

    def extract_deezer_url_from_text(text: str):
        if not text:
            return None
        patterns = [
            r"https?://(?:www\.)?deezer\.com/(?:[a-z]{2}/)?(?:track|album|playlist)/\d+",
            r"(?:www\.)?deezer\.com/(?:[a-z]{2}/)?(?:track|album|playlist)/\d+",
            r"deezer://(?:www\.)?deezer\.com/(?:[a-z]{2}/)?(?:track|album|playlist)/\d+",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.I)
            if match:
                normalized = normalize_deezer_url(match.group(0))
                if normalized:
                    return normalized
        return None

    try:
        session = http_session()
        headers = spotify_headers()

        response = session.head(url, timeout=12, allow_redirects=True, headers=headers)
        from_head = normalize_deezer_url(getattr(response, "url", None))
        if from_head:
            return from_head

        response = session.get(url, timeout=15, allow_redirects=True, headers=headers)
        from_get_url = normalize_deezer_url(getattr(response, "url", None))
        if from_get_url:
            return from_get_url

        from_html = extract_deezer_url_from_text(getattr(response, "text", ""))
        if from_html:
            return from_html

        return url
    except Exception:
        return url


def deezer_extract_type_id(final_url: str):
    match = re.search(r"deezer\.com/(?:[a-z]{2}/)?(track|album|playlist)/(\d+)", final_url, flags=re.I)
    if not match:
        return None, None
    return match.group(1).lower(), match.group(2)


async def deezer_to_tracks(url: str):
    cache_key = url.strip()
    cached_tracks = get_cached(deezer_tracks_cache, cache_key)
    if cached_tracks is not None:
        return clone_tracks(cached_tracks)

    async def deezer_api_get(path):
        endpoint = f"https://api.deezer.com/{path.lstrip('/')}"
        return await fetch_json(endpoint, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)

    def normalize_api_track(track_obj, *, fallback_thumb=None):
        if not isinstance(track_obj, dict):
            return None
        title = clean_spotify_text(track_obj.get("title")) or "Titre inconnu"
        artist_name = clean_spotify_text((track_obj.get("artist") or {}).get("name")) or "Artiste inconnu"
        album_obj = track_obj.get("album") if isinstance(track_obj.get("album"), dict) else {}
        thumb = (
            album_obj.get("cover_medium")
            or album_obj.get("cover_big")
            or album_obj.get("cover")
            or fallback_thumb
        )
        duration = track_obj.get("duration")
        if isinstance(duration, (int, float)):
            duration = int(duration)
        else:
            duration = None
        return {
            "query": f'"{artist_name}" "{title}" audio',
            "title": title,
            "artist": artist_name,
            "thumbnail": thumb,
            "duration": duration,
            "source": "deezer",
        }

    results = []
    try:
        normalized = resolve_deezer_share_link(url)
        item_type, item_id = deezer_extract_type_id(normalized)
        if not item_type or not item_id:
            # Fallback: let yt-dlp parse deezer pages/redirects when short-link parsing fails.
            try:
                loop = asyncio.get_event_loop()
                data = await loop.run_in_executor(None, lambda: ytdl.extract_info(url, download=False))
                if isinstance(data, dict) and data.get("entries"):
                    data = next((entry for entry in data.get("entries", []) if entry), None)
                if isinstance(data, dict):
                    title = clean_spotify_text(data.get("track") or data.get("title")) or "Titre inconnu"
                    artist = clean_spotify_text(data.get("artist") or data.get("uploader") or data.get("channel")) or "Artiste inconnu"
                    duration = data.get("duration")
                    if isinstance(duration, (int, float)):
                        duration = int(duration)
                    else:
                        duration = None
                    results.append({
                        "query": f'"{artist}" "{title}" audio',
                        "title": title,
                        "artist": artist,
                        "thumbnail": data.get("thumbnail"),
                        "duration": duration,
                        "source": "deezer",
                    })
            except Exception:
                pass
            return results

        if dz is not None:
            if item_type == "track":
                track = dz.get_track(item_id)
                if track:
                    results.append({
                        "query": f'"{track.artist.name}" "{track.title}" audio',
                        "title": track.title,
                        "artist": track.artist.name,
                        "thumbnail": track.album.cover_medium,
                        "duration": track.duration,
                        "source": "deezer",
                    })
            elif item_type == "album":
                album = dz.get_album(item_id)
                if album:
                    for track in album.tracks:
                        results.append({
                            "query": f'"{track.artist.name}" "{track.title}" audio',
                            "title": track.title,
                            "artist": track.artist.name,
                            "thumbnail": album.cover_medium,
                            "duration": track.duration,
                            "source": "deezer",
                        })
                    attach_collection_to_tracks(
                        results,
                        {
                            "kind": "album",
                            "label": "Album",
                            "title": getattr(album, "title", None) or "Album Deezer",
                            "subtitle": getattr(getattr(album, "artist", None), "name", None),
                            "thumbnail": getattr(album, "cover_medium", None) or getattr(album, "cover_big", None),
                            "source": "deezer",
                            "count": len(results),
                        },
                    )
            elif item_type == "playlist":
                playlist = dz.get_playlist(item_id)
                if playlist:
                    for track in playlist.tracks:
                        results.append({
                            "query": f'"{track.artist.name}" "{track.title}" audio',
                            "title": track.title,
                            "artist": track.artist.name,
                            "thumbnail": track.album.cover_medium,
                            "duration": track.duration,
                            "source": "deezer",
                        })
                    attach_collection_to_tracks(
                        results,
                        {
                            "kind": "playlist",
                            "label": "Playlist",
                            "title": getattr(playlist, "title", None) or "Playlist Deezer",
                            "subtitle": getattr(getattr(playlist, "creator", None), "name", None),
                            "thumbnail": getattr(playlist, "picture_medium", None) or getattr(playlist, "picture_big", None),
                            "source": "deezer",
                            "count": len(results),
                        },
                    )

        if not results:
            if item_type == "track":
                track_data = await deezer_api_get(f"track/{item_id}")
                normalized_track = normalize_api_track(track_data)
                if normalized_track:
                    results.append(normalized_track)
            elif item_type == "album":
                album_data = await deezer_api_get(f"album/{item_id}")
                album_thumb = (
                    album_data.get("cover_medium")
                    or album_data.get("cover_big")
                    or album_data.get("cover")
                ) if isinstance(album_data, dict) else None
                for track_obj in ((album_data.get("tracks") or {}).get("data") or []) if isinstance(album_data, dict) else []:
                    normalized_track = normalize_api_track(track_obj, fallback_thumb=album_thumb)
                    if normalized_track:
                        results.append(normalized_track)
                if results:
                    attach_collection_to_tracks(
                        results,
                        {
                            "kind": "album",
                            "label": "Album",
                            "title": clean_spotify_text(album_data.get("title")) if isinstance(album_data, dict) else "Album Deezer",
                            "subtitle": clean_spotify_text(((album_data.get("artist") or {}).get("name"))) if isinstance(album_data, dict) else None,
                            "thumbnail": album_thumb,
                            "source": "deezer",
                            "count": int(album_data.get("nb_tracks") or len(results)) if isinstance(album_data, dict) else len(results),
                        },
                    )
            elif item_type == "playlist":
                playlist_data = await deezer_api_get(f"playlist/{item_id}")
                playlist_thumb = (
                    playlist_data.get("picture_medium")
                    or playlist_data.get("picture_big")
                    or playlist_data.get("picture")
                ) if isinstance(playlist_data, dict) else None
                for track_obj in ((playlist_data.get("tracks") or {}).get("data") or []) if isinstance(playlist_data, dict) else []:
                    normalized_track = normalize_api_track(track_obj, fallback_thumb=playlist_thumb)
                    if normalized_track:
                        results.append(normalized_track)
                if results:
                    creator_obj = (playlist_data.get("creator") or {}) if isinstance(playlist_data, dict) else {}
                    attach_collection_to_tracks(
                        results,
                        {
                            "kind": "playlist",
                            "label": "Playlist",
                            "title": clean_spotify_text(playlist_data.get("title")) if isinstance(playlist_data, dict) else "Playlist Deezer",
                            "subtitle": clean_spotify_text(creator_obj.get("name")),
                            "thumbnail": playlist_thumb,
                            "source": "deezer",
                            "count": int(playlist_data.get("nb_tracks") or len(results)) if isinstance(playlist_data, dict) else len(results),
                        },
                    )
    except Exception as exc:
        raise RuntimeError(f"Erreur Deezer : {exc}")
    set_cached(deezer_tracks_cache, cache_key, clone_tracks(results), TRACK_CACHE_TTL_SECONDS)
    return results


async def bandcamp_to_tracks(url: str):
    loop = asyncio.get_event_loop()
    try:
        data = await loop.run_in_executor(None, lambda: ytdl.extract_info(url, download=False))
    except Exception as exc:
        raise RuntimeError(f"Erreur Bandcamp : {exc}")

    def normalize_entry(entry):
        duration = entry.get("duration")
        if duration is not None:
            try:
                duration = int(round(duration))
            except Exception:
                pass
        return {
            "url": entry.get("webpage_url") or entry.get("url"),
            "title": entry.get("title"),
            "artist": entry.get("uploader"),
            "thumbnail": entry.get("thumbnail"),
            "duration": duration,
            "source": "bandcamp",
        }

    if isinstance(data, dict) and data.get("entries"):
        return [normalize_entry(entry) for entry in data["entries"] if entry]
    return [normalize_entry(data)]


def youtube_playlist_thumbnail(data):
    if not isinstance(data, dict):
        return None

    if data.get("thumbnail"):
        return data["thumbnail"]

    thumbnails = data.get("thumbnails")
    if isinstance(thumbnails, list):
        for thumbnail in reversed(thumbnails):
            if isinstance(thumbnail, dict) and thumbnail.get("url"):
                return thumbnail["url"]
    return None


def normalize_youtube_playlist_entry(entry, *, fallback_thumbnail=None):
    if not isinstance(entry, dict):
        return None

    video_id = entry.get("id")
    webpage_url = entry.get("webpage_url")
    if not webpage_url and video_id:
        webpage_url = f"https://www.youtube.com/watch?v={video_id}"

    title = clean_spotify_text(entry.get("title")) or "Titre inconnu"
    artist = clean_playlist_display_artist(entry.get("channel") or entry.get("uploader"), source="youtube")
    thumbnail = youtube_playlist_thumbnail(entry) or fallback_thumbnail
    duration = entry.get("duration")
    if isinstance(duration, (int, float)):
        duration = int(duration)
    else:
        duration = None

    return {
        "url": webpage_url,
        "query": webpage_url or title,
        "title": title,
        "artist": artist,
        "thumbnail": thumbnail,
        "duration": duration,
        "source": "youtube",
    }


async def youtube_playlist_to_tracks(url: str):
    playlist_id = extract_youtube_playlist_id(url)
    if not playlist_id:
        raise RuntimeError("Lien YouTube playlist non reconnu.")

    cached_tracks = get_cached(youtube_playlist_cache, playlist_id)
    if cached_tracks is not None:
        return clone_tracks(cached_tracks)

    loop = asyncio.get_event_loop()
    try:
        data = await loop.run_in_executor(None, lambda: ytdl_playlist.extract_info(url, download=False))
    except Exception as exc:
        raise RuntimeError(f"Erreur YouTube playlist : {exc}")

    if not isinstance(data, dict):
        raise RuntimeError("Impossible de lire la playlist YouTube.")

    fallback_thumbnail = youtube_playlist_thumbnail(data)
    tracks = []
    for entry in data.get("entries") or []:
        normalized = normalize_youtube_playlist_entry(entry, fallback_thumbnail=fallback_thumbnail)
        if normalized:
            tracks.append(normalized)

    if not tracks:
        raise RuntimeError("Aucune musique YouTube n'a pu être extraite de la playlist.")

    attach_collection_to_tracks(
        tracks,
        {
            "kind": "playlist",
            "label": "Playlist",
            "title": clean_spotify_text(data.get("title")) or "Playlist YouTube",
            "subtitle": clean_spotify_text(data.get("channel") or data.get("uploader")),
            "thumbnail": fallback_thumbnail,
            "source": "youtube",
            "count": len(tracks),
            # Keep dynamic resolution in sync_collection_message to avoid freezing
            # a default color when first thumbnail fetch fails.
            "color": None,
        },
    )

    set_cached(youtube_playlist_cache, playlist_id, clone_tracks(tracks), TRACK_CACHE_TTL_SECONDS)
    return tracks


def normalize_search_text(value):
    if not value:
        return ""
    text = str(value).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalized_token_set(value):
    return {token for token in normalize_search_text(value).split() if len(token) > 1}


def simplify_track_title(value):
    text = clean_spotify_text(value) or ""
    text = re.sub(r"\s*\((feat|ft|with)[^)]+\)", "", text, flags=re.I)
    text = re.sub(r"\s*\[(feat|ft|with)[^\]]+\]", "", text, flags=re.I)
    text = re.sub(r"\s*-\s*(feat|ft|with)\s+.+$", "", text, flags=re.I)
    return text.strip() or value


def token_overlap_score(expected, haystack):
    expected_tokens = [token for token in normalize_search_text(expected).split() if len(token) > 2]
    if not expected_tokens:
        return 0, 0
    haystack_tokens = normalized_token_set(haystack)
    matched = sum(1 for token in expected_tokens if token in haystack_tokens)
    return matched, len(expected_tokens)


def token_match_count(expected, actual):
    expected_tokens = normalized_token_set(expected)
    actual_tokens = normalized_token_set(actual)
    if not expected_tokens or not actual_tokens:
        return 0, len(expected_tokens)
    return len(expected_tokens & actual_tokens), len(expected_tokens)


def is_strong_token_match(expected, actual):
    matched, total = token_match_count(expected, actual)
    return total > 0 and matched == total


def has_any_token_match(expected, actual):
    matched, _ = token_match_count(expected, actual)
    return matched > 0


def score_youtube_candidate(entry, *, title=None, artist=None, duration=None, query_text=None, source=None):
    raw_entry_artist = " ".join([
        entry.get("artist") or "",
        entry.get("uploader") or "",
        entry.get("channel") or "",
    ])
    entry_title_text = normalize_search_text(" ".join([
        entry.get("title") or "",
        entry.get("track") or "",
    ]))
    entry_artist_text = normalize_search_text(raw_entry_artist)
    haystack = normalize_search_text(
        " ".join([
            entry.get("title") or "",
            entry.get("uploader") or "",
            entry.get("channel") or "",
            entry.get("description") or "",
            entry.get("track") or "",
            entry.get("artist") or "",
            entry.get("album") or "",
        ])
    )

    title_text = normalize_search_text(title)
    artist_text = normalize_search_text(artist)
    query_tokens = [token for token in normalize_search_text(query_text).split() if len(token) > 2]
    haystack_tokens = normalized_token_set(haystack)
    explicit_artist = clean_spotify_text(entry.get("artist"))
    explicit_channel = clean_spotify_text(entry.get("channel") or entry.get("uploader"))
    score = 0
    title_matches, title_total = token_overlap_score(title, haystack)
    artist_matches, artist_total = token_overlap_score(artist, haystack)
    title_matches_in_title, title_total_in_title = token_overlap_score(title, entry_title_text)
    artist_matches_in_artist, artist_total_in_artist = token_overlap_score(artist, entry_artist_text)

    if title_text:
        if title_text in haystack:
            score += 50
        if title_text == entry_title_text:
            score += 45
        elif entry_title_text.startswith(title_text):
            score += 25
        score += title_matches * 5
        score += title_matches_in_title * 8

    if artist_text:
        if artist_text in haystack:
            score += 40
        if artist_text in entry_artist_text:
            score += 20
        score += artist_matches * 4
        score += artist_matches_in_artist * 6
        if f"{artist_text} topic" in haystack or f"{artist_text} official" in haystack:
            score += 10
        if haystack.startswith(f"{artist_text} ") or haystack.startswith(f"{artist_text} topic"):
            score += 8
        if explicit_artist:
            if is_strong_token_match(artist, explicit_artist):
                score += 110
            elif has_any_token_match(artist, explicit_artist):
                score += 24
            else:
                score -= 130
        if explicit_channel:
            if is_strong_token_match(artist, explicit_channel):
                score += 60
            elif not has_any_token_match(artist, explicit_channel):
                score -= 35

    if query_tokens:
        matched = 0
        for token in query_tokens:
            if token in haystack_tokens:
                matched += 1
        score += matched * 3
        if matched == len(query_tokens) and matched > 0:
            score += 15

    bad_terms = [
        "lyrics",
        "lyric video",
        "sped up",
        "slowed",
        "nightcore",
        "8d",
        "live",
        "cover",
        "karaoke",
        "instrumental",
        "fan made",
        "edit audio",
        "reverb",
        "remix",
        "full album",
        "playlist",
        "1 hour",
        "10 hours",
    ]
    for term in bad_terms:
        if term in haystack:
            score -= 12

    good_terms = [
        "official audio",
        "audio",
        "topic",
        "provided to youtube by",
    ]
    for term in good_terms:
        if term in haystack:
            score += 6

    if "provided to youtube by" in haystack:
        score += 35
    if entry.get("uploader", "").endswith(" - Topic") or entry.get("channel", "").endswith(" - Topic"):
        score += 20
    if title_text and artist_text:
        if normalize_search_text(entry.get("track")) == title_text:
            score += 25
        if normalize_search_text(entry.get("artist")) == artist_text:
            score += 25

    if source in {"spotify", "deezer"}:
        if artist_total and artist_matches == 0:
            score -= 120
        elif artist_total and artist_matches < max(1, artist_total // 2):
            score -= 50

        if artist_total_in_artist and artist_matches_in_artist == 0:
            score -= 90
        elif artist_total_in_artist and artist_matches_in_artist < max(1, artist_total_in_artist // 2):
            score -= 35

        if title_total and title_matches == 0:
            score -= 90
        elif title_total and title_matches < max(1, title_total // 2):
            score -= 35

        if title_total_in_title and title_matches_in_title == 0:
            score -= 130
        elif title_total_in_title and title_matches_in_title < max(1, title_total_in_title // 2):
            score -= 55

        if "official video" in haystack and "audio" not in haystack and "topic" not in haystack:
            score -= 10

        if "remix" in entry_title_text or "sped up" in entry_title_text or "slowed" in entry_title_text:
            score -= 80
        if "live" in entry_title_text or "karaoke" in entry_title_text or "cover" in entry_title_text:
            score -= 90
        if explicit_artist and not is_strong_token_match(artist, explicit_artist):
            score -= 90
        if explicit_channel and not has_any_token_match(artist, explicit_channel):
            score -= 45

    entry_duration = entry.get("duration")
    if isinstance(entry_duration, (int, float)) and isinstance(duration, (int, float)):
        delta = abs(int(entry_duration) - int(duration))
        if delta <= 3:
            score += 20
        elif delta <= 10:
            score += 10
        elif delta >= 45:
            score -= 45
        elif delta >= 20:
            score -= 20

        if source in {"spotify", "deezer"}:
            if delta >= 30:
                score -= 70
            elif delta >= 15:
                score -= 35
            elif delta >= 8:
                score -= 12

    return score


def pick_best_entry(data, *, title=None, artist=None, duration=None, query_text=None, source=None):
    entries = data.get("entries") if isinstance(data, dict) else None
    if not entries:
        return data

    best_entry = None
    best_score = None
    for entry in entries:
        if not entry:
            continue
        score = score_youtube_candidate(
            entry,
            title=title,
            artist=artist,
            duration=duration,
            query_text=query_text,
            source=source,
        )
        if best_entry is None or score > best_score:
            best_entry = entry
            best_score = score
    return best_entry or next((entry for entry in entries if entry), None)


def build_track_cache_key(track):
    duration = track.get("duration")
    if isinstance(duration, (int, float)):
        duration = int(round(duration))
    else:
        duration = None

    return (
        MATCHING_ALGO_VERSION,
        track.get("source") or "",
        normalize_search_text(track.get("artist")),
        normalize_search_text(simplify_track_title(track.get("title"))),
        duration,
        normalize_search_text(track.get("query")),
    )


def build_ytsearch_query(query_text, limit):
    return f"ytsearch{limit}:{query_text}"


def score_audio_format(fmt, *, prefer_stable_http=False):
    if not fmt or fmt.get("acodec") in {None, "none"} or not fmt.get("url"):
        return -10_000

    score = 0
    protocol = str(fmt.get("protocol") or "").lower()
    ext = str(fmt.get("ext") or "").lower()
    acodec = str(fmt.get("acodec") or "").lower()
    abr = fmt.get("abr")
    asr = fmt.get("asr")

    if protocol.startswith("http") and "dash" not in protocol and "m3u8" not in protocol:
        score += 60
    elif "m3u8" in protocol:
        score -= 90
    elif "dash" in protocol or "fragment" in protocol:
        score -= 55

    if ext == "m4a":
        score += 24
    elif ext == "webm":
        score += 18
    elif ext in {"mp3", "opus"}:
        score += 12

    if fmt.get("vcodec") == "none":
        score += 30
    else:
        score -= 20

    if isinstance(abr, (int, float)):
        score += max(0, 24 - abs(int(abr) - PREFERRED_AUDIO_ABR) // 4)
        if abr < 48:
            score -= 35
        elif abr < 96:
            score -= 10

    if isinstance(asr, (int, float)):
        score += min(int(asr) // 4000, 12)

    if fmt.get("manifest_url"):
        score -= 20

    if prefer_stable_http:
        format_id = str(fmt.get("format_id") or "").lower()
        if protocol.startswith("http") and "dash" not in protocol and "m3u8" not in protocol:
            score += 45
        if "m3u8" in protocol or "dash" in protocol or "fragment" in protocol or "hls" in format_id:
            score -= 70
        if ext == "mp3":
            score += 28
        elif ext == "m4a":
            score += 22
        elif ext in {"webm", "opus", "ogg"}:
            score -= 18

        if acodec in {"mp3", "aac"}:
            score += 12
        elif acodec == "opus":
            score -= 10

    return score


def select_audio_url(data):
    formats = data.get("formats") or []
    extractor_name = str(data.get("extractor_key") or data.get("extractor") or "").lower()
    webpage_url = str(data.get("webpage_url") or data.get("original_url") or "").lower()
    prefer_stable_http = "soundcloud" in extractor_name or "soundcloud.com" in webpage_url or "snd.sc" in webpage_url
    candidates = [fmt for fmt in formats if fmt.get("acodec") != "none" and fmt.get("url")]
    if candidates:
        best_format = max(candidates, key=lambda fmt: score_audio_format(fmt, prefer_stable_http=prefer_stable_http))
        return best_format.get("url")
    return data.get("url")


def select_audio_format(data):
    formats = data.get("formats") or []
    extractor_name = str(data.get("extractor_key") or data.get("extractor") or "").lower()
    webpage_url = str(data.get("webpage_url") or data.get("original_url") or "").lower()
    prefer_stable_http = "soundcloud" in extractor_name or "soundcloud.com" in webpage_url or "snd.sc" in webpage_url
    candidates = [fmt for fmt in formats if fmt.get("acodec") not in {None, "none"} and fmt.get("url")]
    if candidates:
        return max(candidates, key=lambda fmt: score_audio_format(fmt, prefer_stable_http=prefer_stable_http))
    return None


def rank_entries(entries, *, title=None, artist=None, duration=None, query_text=None, source=None):
    return sorted(
        [
            (
                score_youtube_candidate(
                    entry,
                    title=title,
                    artist=artist,
                    duration=duration,
                    query_text=query_text,
                    source=source,
                ),
                entry,
            )
            for entry in entries
            if entry
        ],
        key=lambda item: item[0],
        reverse=True,
    )


def is_confident_top_result(scored_entries):
    if not scored_entries:
        return False
    top_score = scored_entries[0][0]
    second_score = scored_entries[1][0] if len(scored_entries) > 1 else None
    return top_score >= YOUTUBE_EARLY_ACCEPT_SCORE and (
        second_score is None or (top_score - second_score) >= YOUTUBE_EARLY_ACCEPT_MARGIN
    )


def is_strict_confident_top_result(scored_entries):
    if not scored_entries:
        return False
    top_score = scored_entries[0][0]
    second_score = scored_entries[1][0] if len(scored_entries) > 1 else None
    return top_score >= STRICT_FAST_ACCEPT_SCORE and (
        second_score is None or (top_score - second_score) >= YOUTUBE_EARLY_ACCEPT_MARGIN
    )


async def search_ytdl_entries(loop, extract, queries, *, seen=None):
    if not queries:
        return []

    seen = seen if seen is not None else set()
    tasks = [
        loop.run_in_executor(None, lambda q=query: extract(q))
        for query in queries
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    merged_entries = []
    for result in results:
        if isinstance(result, Exception) or not result:
            continue
        for entry in result.get("entries", []):
            if not entry:
                continue
            key = entry.get("id") or entry.get("webpage_url") or entry.get("url") or entry.get("title")
            if not key or key in seen:
                continue
            seen.add(key)
            merged_entries.append(entry)
    return merged_entries


async def enrich_ytdl_entries(loop, extract, entries, *, cache, ttl_seconds):
    if not entries:
        return []

    enriched_entries = []
    pending = []

    for entry in entries:
        candidate_url = entry.get("webpage_url") or entry.get("url")
        if not candidate_url:
            enriched_entries.append(entry)
            continue

        cached_details = get_cached(cache, candidate_url)
        if cached_details is not None:
            enriched_entries.append(cached_details)
            continue

        pending.append((entry, candidate_url, loop.run_in_executor(None, lambda u=candidate_url: extract(u))))

    if not pending:
        return enriched_entries

    results = await asyncio.gather(*(task for _, _, task in pending), return_exceptions=True)
    for (entry, candidate_url, _), result in zip(pending, results):
        if isinstance(result, Exception) or not result or result.get("entries"):
            enriched_entries.append(entry)
            continue
        set_cached(cache, candidate_url, result, ttl_seconds)
        enriched_entries.append(result)

    return enriched_entries


async def resolve_text_query_candidates(loop, query_text, *, source="youtube", limit=5, enrich_limit=2):
    query_value = str(query_text or "").strip()
    if not query_value:
        return []

    def extract(candidate):
        candidate_text = str(candidate or "").strip().lower()
        extractor = ytdl_search if candidate_text.startswith("ytsearch") else ytdl
        return extractor.extract_info(candidate, download=False)

    entries = await search_ytdl_entries(
        loop,
        extract,
        [build_ytsearch_query(query_value, max(1, int(limit)))],
        seen=set(),
    )

    if not entries:
        try:
            direct_result = await loop.run_in_executor(None, lambda: ytdl.extract_info(query_value, download=False))
        except Exception:
            direct_result = None

        if isinstance(direct_result, dict):
            if isinstance(direct_result.get("entries"), list):
                entries = [entry for entry in direct_result.get("entries", []) if entry]
            else:
                entries = [direct_result]

    if not entries:
        return []

    ranked_entries = [entry for _, entry in rank_entries(entries, query_text=query_value, source=source)]
    if not ranked_entries or enrich_limit <= 0:
        return ranked_entries

    enriched_entries = await enrich_ytdl_entries(
        loop,
        extract,
        ranked_entries[:max(1, int(enrich_limit))],
        cache=youtube_details_cache,
        ttl_seconds=YOUTUBE_CACHE_TTL_SECONDS,
    )

    merged_entries = []
    seen_keys = set()
    for entry in list(enriched_entries) + ranked_entries[max(1, int(enrich_limit)):]:
        if not entry:
            continue
        key = entry.get("id") or entry.get("webpage_url") or entry.get("url") or entry.get("title")
        if key and key in seen_keys:
            continue
        if key:
            seen_keys.add(key)
        merged_entries.append(entry)

    reranked_entries = [entry for _, entry in rank_entries(merged_entries, query_text=query_value, source=source)]
    return reranked_entries or merged_entries


class YTDLSource(discord.AudioSource):
    def __init__(self, source, *, data, volume=1.0):
        self.source = source
        self.data = data
        self.title = data.get("title")
        self.volume = volume

    def read(self):
        return self.source.read()

    def is_opus(self):
        return self.source.is_opus()

    def cleanup(self):
        try:
            self.source.cleanup()
        except Exception:
            pass

    @classmethod
    def from_data(cls, data, *, volume=1.0, start_at=0):
        playback_data = dict(data or {})
        selected_format = playback_data.get("_selected_audio_format")
        if not isinstance(selected_format, dict):
            selected_format = select_audio_format(playback_data)
            if isinstance(selected_format, dict):
                playback_data["_selected_audio_format"] = dict(selected_format)

        audio_url = playback_data.get("_selected_audio_url")
        if not audio_url and isinstance(selected_format, dict):
            audio_url = selected_format.get("url")
        if not audio_url:
            audio_url = select_audio_url(playback_data)
        if not audio_url:
            raise RuntimeError("Impossible d'obtenir l'URL audio pour FFmpeg")

        playback_data["_selected_audio_url"] = audio_url
        codec_name = str((selected_format or {}).get("acodec") or "").lower()
        ext_name = str((selected_format or {}).get("ext") or "").lower()
        protocol_name = str((selected_format or {}).get("protocol") or "").lower()
        extractor_name = str(playback_data.get("extractor_key") or playback_data.get("extractor") or "").lower()
        webpage_url = str(playback_data.get("webpage_url") or playback_data.get("original_url") or "").lower()
        is_soundcloud_stream = "soundcloud" in extractor_name or "soundcloud.com" in webpage_url or "snd.sc" in webpage_url
        is_fragmented_stream = (
            "m3u8" in protocol_name
            or "dash" in protocol_name
            or "fragment" in protocol_name
            or bool((selected_format or {}).get("manifest_url"))
        )
        before_options = FFMPEG_STREAM_BEFORE_OPTIONS
        stream_options = FFMPEG_SOUNDCLOUD_STREAM_OPTIONS if is_soundcloud_stream else FFMPEG_STREAM_OPTIONS
        if is_soundcloud_stream:
            before_options = f"{before_options} -fflags +genpts -probesize 512k -analyzeduration 3000000"
        if start_at and start_at > 0:
            before_options = f"{before_options} -ss {int(start_at)}"

        use_opus_copy = (
            codec_name == "opus"
            and ext_name in {"webm", "ogg", "opus"}
            and not is_soundcloud_stream
            and not is_fragmented_stream
        )

        if use_opus_copy:
            source = discord.FFmpegOpusAudio(
                audio_url,
                executable=find_ffmpeg(),
                codec="copy",
                before_options=before_options,
                options=stream_options,
            )
        else:
            source = discord.FFmpegPCMAudio(
                audio_url,
                executable=find_ffmpeg(),
                before_options=before_options,
                options=stream_options,
            )
        return cls(source, data=playback_data, volume=volume)

    @classmethod
    async def from_url(
        cls,
        url,
        *,
        loop=None,
        volume=1.0,
        expected_title=None,
        expected_artist=None,
        expected_duration=None,
        start_at=0,
    ):
        loop = loop or asyncio.get_event_loop()

        def extract(candidate):
            candidate_text = str(candidate or "").strip().lower()
            extractor = ytdl_search if candidate_text.startswith("ytsearch") else ytdl
            return extractor.extract_info(candidate, download=False)

        data = None
        resolved_url = url

        for candidate_url in youtube_extraction_urls(url):
            cached_data = get_cached(youtube_details_cache, candidate_url)
            if cached_data is not None:
                data = cached_data
                resolved_url = candidate_url
                break

            try:
                extracted = await loop.run_in_executor(None, lambda u=candidate_url: extract(u))
            except Exception:
                extracted = None

            if not extracted:
                continue

            data = extracted
            resolved_url = candidate_url
            break

        if not data:
            raise RuntimeError("yt-dlp n'a rien retourné")

        if "entries" in data:
            data = pick_best_entry(data, title=expected_title, artist=expected_artist, duration=expected_duration)

        if data and not data.get("entries") and looks_like_url(resolved_url):
            set_cached(youtube_details_cache, resolved_url, data, YOUTUBE_CACHE_TTL_SECONDS)
            if resolved_url != url:
                set_cached(youtube_details_cache, url, data, YOUTUBE_CACHE_TTL_SECONDS)

        return cls.from_data(data, volume=volume, start_at=start_at)

    @classmethod
    async def from_track(cls, track, *, loop=None, volume=1.0, start_at=0):
        loop = loop or asyncio.get_event_loop()
        expected_title = track.get("title")
        expected_artist = track.get("artist")
        expected_duration = track.get("duration")
        raw_url = track.get("url")
        query = track.get("query") or expected_title or raw_url
        source_name = track.get("source")
        simplified_title = simplify_track_title(expected_title)
        playback_data = track.get("_playback_data")
        strict_source = (
            source_name in {"spotify", "deezer"}
            and bool(expected_title)
            and bool(expected_artist)
            and expected_artist != "Artiste inconnu"
        )
        primary_search_size = 2 if strict_source else YOUTUBE_PRIMARY_SEARCH_SIZE
        fallback_search_size = 1 if strict_source else YOUTUBE_FALLBACK_SEARCH_SIZE
        enrich_limit = 1 if strict_source else MAX_ENRICHED_CANDIDATES

        def extract(candidate):
            candidate_text = str(candidate or "").strip().lower()
            extractor = ytdl_search if candidate_text.startswith("ytsearch") else ytdl
            return extractor.extract_info(candidate, download=False)

        async def try_candidate_entries(entries, *, minimum_score=None, max_candidates=4, raise_last_error=False):
            last_runtime_error = None
            if not entries:
                return None

            for index, entry in enumerate(entries):
                if index >= max_candidates:
                    break
                if not entry:
                    continue

                entry_score = score_youtube_candidate(
                    entry,
                    title=expected_title,
                    artist=expected_artist,
                    duration=expected_duration,
                    query_text=query,
                    source=source_name,
                )
                if minimum_score is not None and entry_score < minimum_score:
                    continue

                candidate_url = entry.get("webpage_url") or entry.get("url")
                if not candidate_url:
                    continue

                try:
                    if entry.get("formats"):
                        resolved_source = cls.from_data(entry, volume=volume, start_at=start_at)
                    else:
                        resolved_source = await cls.from_url(
                            candidate_url,
                            loop=loop,
                            volume=volume,
                            expected_title=expected_title,
                            expected_artist=expected_artist,
                            expected_duration=expected_duration,
                            start_at=start_at,
                        )
                except RuntimeError as exc:
                    last_runtime_error = exc
                    continue

                set_cached(youtube_resolution_cache, cache_key, candidate_url, YOUTUBE_CACHE_TTL_SECONDS)
                return resolved_source

            if raise_last_error and last_runtime_error is not None:
                raise last_runtime_error
            return None

        async def try_text_query_fallback(search_text, *, limit=6, enrich_limit=3, max_candidates=6):
            nonlocal expected_title, expected_artist, expected_duration, query

            search_value = str(search_text or "").strip()
            if not search_value or looks_like_url(search_value):
                return None

            fallback_entries = await resolve_text_query_candidates(
                loop,
                search_value,
                source=source_name or "youtube",
                limit=limit,
                enrich_limit=enrich_limit,
            )
            if not fallback_entries:
                return None

            fallback_best = next((entry for entry in fallback_entries if isinstance(entry, dict)), None)
            if isinstance(fallback_best, dict):
                track_title = fallback_best.get("title")
                track_artist = fallback_best.get("artist") or fallback_best.get("channel") or fallback_best.get("uploader")
                track_duration = fallback_best.get("duration")
                if track_title and (not track.get("title") or track.get("title") in {query, search_value, "Titre inconnu"}):
                    track["title"] = track_title
                    expected_title = track_title
                if track_artist and (not track.get("artist") or track.get("artist") == "Artiste inconnu"):
                    track["artist"] = track_artist
                    expected_artist = track_artist
                if track_duration and not track.get("duration"):
                    track["duration"] = track_duration
                    expected_duration = track_duration
                if not track.get("thumbnail"):
                    thumbnail_candidates = youtube_thumbnail_candidates_from_data(fallback_best)
                    if thumbnail_candidates:
                        track["thumbnail"] = thumbnail_candidates[0]

                if expected_title and expected_artist and expected_artist != "Artiste inconnu":
                    query = f'"{expected_artist}" "{expected_title}"'
                    track["query"] = query
                elif expected_title:
                    query = expected_title
                    track["query"] = expected_title

            resolved_source = await try_candidate_entries(fallback_entries, max_candidates=max_candidates, raise_last_error=False)
            if resolved_source is not None:
                return resolved_source
            return None

        if isinstance(playback_data, dict) and playback_data:
            return cls.from_data(playback_data, volume=volume, start_at=start_at)

        direct_url_error = None

        if raw_url and re.match(r"^https?://", raw_url.strip()):
            try:
                return await cls.from_url(
                    raw_url,
                    loop=loop,
                    volume=volume,
                    expected_title=expected_title,
                    expected_artist=expected_artist,
                    expected_duration=expected_duration,
                    start_at=start_at,
                )
            except RuntimeError as exc:
                direct_url_error = exc
                page_metadata = (
                    await resolve_youtube_page_metadata(raw_url)
                    if is_youtube_url(raw_url)
                    else await resolve_webpage_media_metadata(raw_url)
                )
                page_title = clean_spotify_text(page_metadata.get("title"))
                page_owner = clean_spotify_text(page_metadata.get("owner"))
                page_thumbnail = page_metadata.get("thumbnail")

                if page_title and not expected_title:
                    expected_title = page_title
                    track["title"] = page_title
                if page_owner and (not expected_artist or expected_artist == "Artiste inconnu"):
                    expected_artist = page_owner
                    track["artist"] = page_owner
                if page_thumbnail and not track.get("thumbnail"):
                    thumbnail_urls = extract_thumbnail_urls(page_thumbnail)
                    if thumbnail_urls:
                        track["thumbnail"] = thumbnail_urls[0]

                if page_title:
                    query = f'"{page_owner}" "{page_title}"' if page_owner else page_title
                    track["query"] = query

        if not query:
            if direct_url_error:
                raise direct_url_error
            raise RuntimeError("Aucune requête audio disponible")

        cache_key = build_track_cache_key(track)
        cached_url = get_cached(youtube_resolution_cache, cache_key)
        if cached_url:
            try:
                return await cls.from_url(
                    cached_url,
                    loop=loop,
                    volume=volume,
                    expected_title=expected_title,
                    expected_artist=expected_artist,
                    expected_duration=expected_duration,
                    start_at=start_at,
                )
            except RuntimeError:
                youtube_resolution_cache.pop(cache_key, None)

        if expected_title and expected_artist and expected_artist != "Artiste inconnu":
            primary_queries = [
                build_ytsearch_query(f'"{expected_artist}" "{expected_title}" official audio', primary_search_size),
                build_ytsearch_query(f'"{expected_artist}" "{expected_title}" topic', primary_search_size),
            ]
            fallback_queries = [
                build_ytsearch_query(f'"{expected_artist}" "{expected_title}" audio', fallback_search_size),
                build_ytsearch_query(f'"{expected_artist}" "{expected_title}" "provided to youtube by"', fallback_search_size),
            ]
            if simplified_title and simplified_title != expected_title:
                fallback_queries.extend([
                    build_ytsearch_query(f'"{expected_artist}" "{simplified_title}" official audio', fallback_search_size),
                    build_ytsearch_query(f'"{expected_artist}" "{simplified_title}" audio', fallback_search_size),
                ])
        elif expected_title:
            primary_queries = [
                build_ytsearch_query(f'"{expected_title}" official audio', primary_search_size),
                build_ytsearch_query(f'"{expected_title}" audio', primary_search_size),
            ]
            fallback_queries = [build_ytsearch_query(f'"{expected_title}"', fallback_search_size)]
            if simplified_title and simplified_title != expected_title:
                fallback_queries.append(build_ytsearch_query(f'"{simplified_title}" audio', fallback_search_size))
        else:
            primary_queries = [build_ytsearch_query(query, primary_search_size)]
            fallback_queries = []

        fast_seen = set()
        fast_entries = await search_ytdl_entries(loop, extract, primary_queries, seen=fast_seen)
        fast_scored_entries = rank_entries(
            fast_entries,
            title=expected_title,
            artist=expected_artist,
            duration=expected_duration,
            query_text=query,
            source=source_name,
        )

        fast_is_confident = is_strict_confident_top_result(fast_scored_entries) if strict_source else is_confident_top_result(fast_scored_entries)
        if fallback_queries and not fast_is_confident:
            fast_entries.extend(await search_ytdl_entries(loop, extract, fallback_queries, seen=fast_seen))
            fast_scored_entries = rank_entries(
                fast_entries,
                title=expected_title,
                artist=expected_artist,
                duration=expected_duration,
                query_text=query,
                source=source_name,
            )

        if fast_scored_entries:
            fast_ranked_entries = [entry for _, entry in fast_scored_entries]
            fast_top_score = fast_scored_entries[0][0]
            fast_second_score = fast_scored_entries[1][0] if len(fast_scored_entries) > 1 else None
            should_enrich_fast = (
                enrich_limit > 0
                and (
                    (strict_source and not fast_is_confident)
                    or (
                        len(fast_scored_entries) > 1
                        and (
                            fast_top_score < (STRICT_FAST_ACCEPT_SCORE if strict_source else 120)
                            or (fast_second_score is not None and (fast_top_score - fast_second_score) < 18)
                        )
                    )
                )
            )

            if should_enrich_fast:
                enriched_entries = await enrich_ytdl_entries(
                    loop,
                    extract,
                    fast_ranked_entries[:enrich_limit],
                    cache=youtube_details_cache,
                    ttl_seconds=YOUTUBE_CACHE_TTL_SECONDS,
                )
                picked = pick_best_entry(
                    {"entries": enriched_entries or fast_ranked_entries},
                    title=expected_title,
                    artist=expected_artist,
                    duration=expected_duration,
                    query_text=query,
                    source=source_name,
                )
                candidate_entries = enriched_entries or fast_ranked_entries
            else:
                picked = fast_ranked_entries[0]
                candidate_entries = fast_ranked_entries

            if picked:
                accept_score = STRICT_FAST_ACCEPT_SCORE if strict_source else YOUTUBE_EARLY_ACCEPT_SCORE
                resolved_source = await try_candidate_entries(
                    candidate_entries,
                    minimum_score=accept_score,
                    max_candidates=4,
                    raise_last_error=False,
                )
                if resolved_source is not None:
                    return resolved_source

        if expected_title and expected_artist and expected_artist != "Artiste inconnu":
            search_queries = [
                build_ytsearch_query(f'"{expected_artist}" "{expected_title}" official audio', STRICT_SLOW_SEARCH_SIZE),
                build_ytsearch_query(f'"{expected_artist}" "{expected_title}" topic', STRICT_SLOW_SEARCH_SIZE),
                build_ytsearch_query(f'"{expected_artist}" "{expected_title}" audio', STRICT_SLOW_SEARCH_SIZE),
            ]
            if simplified_title and simplified_title != expected_title:
                search_queries.extend([
                    build_ytsearch_query(f'"{expected_artist}" "{simplified_title}" official audio', STRICT_SLOW_SEARCH_SIZE),
                    build_ytsearch_query(f'"{expected_artist}" "{simplified_title}" audio', STRICT_SLOW_SEARCH_SIZE),
                ])
        elif expected_title:
            search_queries = [
                build_ytsearch_query(f'"{expected_title}" official audio', STRICT_SLOW_SEARCH_SIZE),
                build_ytsearch_query(f'"{expected_title}" audio', STRICT_SLOW_SEARCH_SIZE),
                build_ytsearch_query(f'"{expected_title}"', STRICT_SLOW_SEARCH_SIZE),
            ]
            if simplified_title and simplified_title != expected_title:
                search_queries.append(build_ytsearch_query(f'"{simplified_title}" audio', STRICT_SLOW_SEARCH_SIZE))
        else:
            search_queries = [build_ytsearch_query(query, STRICT_SLOW_SEARCH_SIZE)]

        seen = set()
        merged_entries = await search_ytdl_entries(loop, extract, search_queries, seen=seen)

        if not merged_entries:
            fallback_searches = []
            if strict_source and expected_title and expected_artist and expected_artist != "Artiste inconnu":
                fallback_searches.extend([
                    f"{expected_artist} {expected_title}",
                    f"{expected_title} {expected_artist}",
                ])
            if expected_title:
                fallback_searches.append(expected_title)
            if query and not looks_like_url(str(query)):
                fallback_searches.append(query)

            tried_searches = set()
            for search_text in fallback_searches:
                normalized_search = normalize_search_text(search_text)
                if not normalized_search or normalized_search in tried_searches:
                    continue
                tried_searches.add(normalized_search)
                resolved_source = await try_text_query_fallback(
                    search_text,
                    limit=max(6, STRICT_SLOW_SEARCH_SIZE + 3),
                    enrich_limit=3,
                    max_candidates=8,
                )
                if resolved_source is not None:
                    return resolved_source

            raise RuntimeError("Aucun resultat audio fiable n'a ete trouvé")

        scored_entries = sorted(
            [
                (
                    score_youtube_candidate(
                        entry,
                        title=expected_title,
                        artist=expected_artist,
                        duration=expected_duration,
                        query_text=query,
                        source=source_name,
                    ),
                    entry,
                )
                for entry in merged_entries
            ],
            key=lambda item: item[0],
            reverse=True,
        )
        ranked_entries = [entry for _, entry in scored_entries]
        top_score = scored_entries[0][0]
        second_score = scored_entries[1][0] if len(scored_entries) > 1 else None
        slow_is_confident = is_strict_confident_top_result(scored_entries) if strict_source else is_confident_top_result(scored_entries)
        should_enrich = (
            enrich_limit > 0
            and (
                (strict_source and not slow_is_confident)
                or (
                    len(scored_entries) > 1
                    and (top_score < 120 or (second_score is not None and (top_score - second_score) < 18))
                )
            )
        )

        if should_enrich:
            enriched_entries = await enrich_ytdl_entries(
                loop,
                extract,
                ranked_entries[:enrich_limit],
                cache=youtube_details_cache,
                ttl_seconds=YOUTUBE_CACHE_TTL_SECONDS,
            )
            picked = pick_best_entry(
                {"entries": enriched_entries or ranked_entries},
                title=expected_title,
                artist=expected_artist,
                duration=expected_duration,
                query_text=query,
                source=source_name,
            )
            candidate_entries = enriched_entries or ranked_entries
        else:
            picked = ranked_entries[0]
            candidate_entries = ranked_entries

        if not ranked_entries:
            raise RuntimeError("Impossible de selectionner un audio")

        if picked:
            resolved_source = await try_candidate_entries(candidate_entries, max_candidates=6, raise_last_error=True)
            if resolved_source is not None:
                return resolved_source

        fallback_searches = []
        if query and not looks_like_url(str(query)):
            fallback_searches.append(query)
        if strict_source and expected_title and expected_artist and expected_artist != "Artiste inconnu":
            fallback_searches.extend([
                f"{expected_artist} {expected_title}",
                f"{expected_title} {expected_artist}",
            ])
        if expected_title:
            fallback_searches.append(expected_title)

        tried_searches = set()
        for search_text in fallback_searches:
            normalized_search = normalize_search_text(search_text)
            if not normalized_search or normalized_search in tried_searches:
                continue
            tried_searches.add(normalized_search)
            resolved_source = await try_text_query_fallback(
                search_text,
                limit=max(6, STRICT_SLOW_SEARCH_SIZE + 3),
                enrich_limit=3,
                max_candidates=8,
            )
            if resolved_source is not None:
                return resolved_source

        raise RuntimeError("Impossible de selectionner un audio")


class MusicPlayer:
    def __init__(self, ctx):
        self.ctx = ctx
        self.queue = []
        self.playing = False
        self.current = None
        self.history = []
        self.loop = False
        self.loopqueue = False
        self.original_queue = []
        self.queue_message = None
        self.queue_message_signature = None
        self.skip_status_message = None
        self.idle_disconnect_task = None
        self.suppress_next_after = False
        self.force_advance_once = False
        self.play_start_task = None
        self.playback_transition = False
        self.auto_queue_message_enabled = True
        self.collection_message = None
        self.collection_extra_messages = []
        self.collection_signature_key = None
        self.skip_history_once = False
        self.navigation_token = 0
        self.stop_in_progress = False
        self.replay_current_on_error = False
        self.voice_recovery_attempts = 0
        self.voice_recovery_task = None
        self.playback_watchdog_task = None
        self.playback_started_at = None
        self.playback_paused_at = None
        self.playback_paused_total = 0.0
        self.queue_sync_lock = asyncio.Lock()
        self.control_lock = asyncio.Lock()
        self.after_playback_advance_pending = False

    def get_voice_client(self):
        voice_client = self.ctx.guild.voice_client or self.ctx.voice_client
        if voice_client and voice_client.is_connected():
            return voice_client
        return None

    def has_live_voice_activity(self, voice_client=None):
        vc = voice_client or self.get_voice_client()
        return bool(vc and (vc.is_playing() or vc.is_paused()))

    def has_pending_playback(self, voice_client=None):
        return bool(
            self.current
            or self.queue
            or self.playing
            or self.playback_transition
            or (self.play_start_task and not self.play_start_task.done())
            or self.has_live_voice_activity(voice_client)
        )

    def has_following_track(self):
        return bool(self.queue) or bool(self.loopqueue and self.original_queue)

    async def wait_for_stable_state(self, *, timeout=1.5, poll_interval=0.05):
        deadline = time.monotonic() + max(0.1, float(timeout))
        while True:
            if (
                not self.playback_transition
                and not self.stop_in_progress
                and not (self.play_start_task and not self.play_start_task.done())
            ):
                return True
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(poll_interval)

    async def wait_for_voice_idle(self, voice_client=None, *, timeout=1.4, poll_interval=0.05):
        vc = voice_client or self.get_voice_client()
        if vc is None:
            return True

        deadline = time.monotonic() + max(0.1, float(timeout))
        while time.monotonic() < deadline:
            if not (vc.is_playing() or vc.is_paused()):
                return True
            await asyncio.sleep(poll_interval)
        return not (vc.is_playing() or vc.is_paused())

    def current_retry_count(self):
        if not isinstance(self.current, dict):
            return 0
        try:
            return max(0, int(self.current.get("_playback_retry_count") or 0))
        except Exception:
            return 0

    def can_retry_current_track(self):
        return bool(isinstance(self.current, dict) and self.current_retry_count() < 1)

    def mark_current_retry(self):
        if not isinstance(self.current, dict):
            return
        self.current["_playback_retry_count"] = self.current_retry_count() + 1

    def reset_track_retry(self, track):
        if isinstance(track, dict):
            track["_playback_retry_count"] = 0

    def update_snapshot(self):
        snapshot = []
        if self.current:
            snapshot.append(self.current)
        snapshot.extend(self.queue.copy())
        self.original_queue = snapshot

    def build_playback_timeline(self):
        timeline = list(self.history)
        cursor_index = len(self.history)
        if self.current:
            timeline.append(self.current)
        timeline.extend(self.queue)
        return timeline, cursor_index

    def jump_to_timeline_index(self, timeline, target_index):
        self.navigation_token += 1
        self.after_playback_advance_pending = False
        if not timeline:
            self.history = []
            self.queue = []
            self.current = None
            self.playing = False
            self.skip_history_once = True
            self.force_advance_once = False
            return None

        clamped_index = max(0, min(int(target_index), len(timeline)))
        self.history = list(timeline[:clamped_index])
        self.queue = list(timeline[clamped_index:])
        self.current = None
        self.playing = False
        self.skip_history_once = True
        self.force_advance_once = False
        return self.queue[0] if self.queue else None

    def start_playback_background(self):
        if self.playing:
            return
        if self.play_start_task and not self.play_start_task.done():
            return

        self.play_start_task = asyncio.create_task(self.play_next())

        def clear_task(task):
            if self.play_start_task is task:
                self.play_start_task = None
            try:
                task.result()
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
            if (
                self.after_playback_advance_pending
                and not self.stop_in_progress
                and not self.playback_transition
                and not self.playing
            ):
                self.start_playback_background()

        self.play_start_task.add_done_callback(clear_task)

    async def _advance_after_playback(self):
        try:
            await asyncio.sleep(0.35)
        except asyncio.CancelledError:
            return

        if self.stop_in_progress:
            return

        for _ in range(20):
            if self.stop_in_progress:
                return
            if not self.playback_transition:
                break
            await asyncio.sleep(0.15)

        if self.stop_in_progress:
            return

        if self.playback_transition:
            self.schedule_voice_recovery(delay=0.35)
            return

        if self.has_following_track():
            self.force_advance_once = True
        self.start_playback_background()

    def cancel_playback_watchdog(self):
        if self.playback_watchdog_task and not self.playback_watchdog_task.done():
            self.playback_watchdog_task.cancel()
        self.playback_watchdog_task = None
        self.playback_started_at = None
        self.playback_paused_at = None
        self.playback_paused_total = 0.0

    def mark_playback_started(self, start_offset=0, *, paused=False):
        now = time.monotonic()
        try:
            offset_value = max(0, int(start_offset or 0))
        except Exception:
            offset_value = 0
        self.playback_started_at = now - offset_value
        self.playback_paused_total = 0.0
        self.playback_paused_at = now if paused else None

    def mark_playback_paused(self):
        if self.playback_started_at is None or self.playback_paused_at is not None:
            return
        self.playback_paused_at = time.monotonic()

    def mark_playback_resumed(self):
        if self.playback_started_at is None or self.playback_paused_at is None:
            return
        self.playback_paused_total += max(0.0, time.monotonic() - self.playback_paused_at)
        self.playback_paused_at = None

    def playback_elapsed_seconds(self):
        if self.playback_started_at is None:
            return None
        elapsed = time.monotonic() - self.playback_started_at - self.playback_paused_total
        if self.playback_paused_at is not None:
            elapsed -= max(0.0, time.monotonic() - self.playback_paused_at)
        return max(0.0, elapsed)

    def track_duration_seconds(self, track):
        if not isinstance(track, dict):
            return None

        duration_value = track.get("duration")
        if isinstance(duration_value, (int, float)):
            return float(duration_value)

        playback_data = track.get("_playback_data") if isinstance(track.get("_playback_data"), dict) else {}
        duration_value = playback_data.get("duration")
        if isinstance(duration_value, (int, float)):
            return float(duration_value)
        return None

    def is_track_near_end(self, track, *, elapsed_seconds=None, tail_seconds=6.0, fraction=0.82):
        duration_value = self.track_duration_seconds(track)
        if not isinstance(duration_value, (int, float)) or duration_value <= 0:
            return False

        if elapsed_seconds is None:
            elapsed_seconds = self.playback_elapsed_seconds()
        if elapsed_seconds is None:
            return False

        threshold = max(float(duration_value) - float(tail_seconds), float(duration_value) * float(fraction))
        return float(elapsed_seconds) >= threshold

    def start_playback_watchdog(self, track):
        self.cancel_playback_watchdog()
        if not isinstance(track, dict):
            return

        navigation_token = self.navigation_token

        async def _watch():
            silent_checks = 0
            try:
                while True:
                    await asyncio.sleep(2.5)

                    if self.current is not track or self.navigation_token != navigation_token:
                        return

                    if self.playback_transition or self.stop_in_progress:
                        silent_checks = 0
                        continue

                    voice_client = self.get_voice_client()
                    duration_value = track.get("duration")
                    elapsed_value = self.playback_elapsed_seconds()
                    if (
                        isinstance(duration_value, (int, float))
                        and duration_value > 0
                        and elapsed_value is not None
                        and elapsed_value >= (float(duration_value) + 8.0)
                    ):
                        if self.has_following_track():
                            self.force_advance_once = True
                            self.replay_current_on_error = False
                        else:
                            if self.can_retry_current_track():
                                self.mark_current_retry()
                            self.force_advance_once = False
                            self.replay_current_on_error = True
                        if voice_client and (voice_client.is_playing() or voice_client.is_paused()) and self.has_following_track():
                            voice_client.stop()
                        else:
                            self.start_playback_background()
                        return

                    if voice_client and (voice_client.is_playing() or voice_client.is_paused()):
                        silent_checks = 0
                        continue

                    silent_checks += 1
                    if silent_checks >= 2 and self.current is track:
                        if self.has_following_track():
                            self.force_advance_once = True
                            self.replay_current_on_error = False
                        else:
                            if self.can_retry_current_track():
                                self.mark_current_retry()
                            self.force_advance_once = False
                            self.replay_current_on_error = True
                        self.start_playback_background()
                        return
            except asyncio.CancelledError:
                return
            finally:
                if self.playback_watchdog_task is asyncio.current_task():
                    self.playback_watchdog_task = None

        self.playback_watchdog_task = asyncio.create_task(_watch())

    def source_color(self, source):
        return {
            "spotify": 0x1DB954,
            "soundcloud": 0xFF7700,
            "deezer": 0x8E44AD,
            "bandcamp": 0x00C7F2,
            "youtube": 0xFF0000,
        }.get(source, 0x5865F2)

    def source_label(self, source):
        return {
            "spotify": "Spotify",
            "soundcloud": "SoundCloud",
            "deezer": "Deezer",
            "bandcamp": "Bandcamp",
            "youtube": "YouTube",
        }.get(source, "Inconnu")

    def collection_signature(self, collection):
        if not isinstance(collection, dict):
            return None
        return (
            collection.get("source"),
            collection.get("kind"),
            collection.get("title"),
        )

    def active_collection_tracks(self):
        seed_track = None
        if self.current and isinstance(self.current.get("_collection"), dict):
            seed_track = self.current
        elif self.queue:
            seed_track = self.queue[0]

        if not seed_track:
            return None

        signature = self.collection_signature(seed_track.get("_collection"))
        if signature is None:
            return None

        tracks = []
        if self.current and self.collection_signature(self.current.get("_collection")) == signature:
            tracks.append(self.current)

        for track in self.queue:
            if self.collection_signature(track.get("_collection")) != signature:
                return None
            tracks.append(track)

        return tracks or None

    def playback_source_key(self, data, fallback=None):
        extractor = str((data or {}).get("extractor_key") or "").lower()
        webpage_url = str((data or {}).get("webpage_url") or "").lower()

        if "soundcloud" in extractor or "soundcloud.com" in webpage_url:
            return "soundcloud"
        if "bandcamp" in extractor or "bandcamp.com" in webpage_url:
            return "bandcamp"
        if "youtube" in extractor or "youtu" in webpage_url:
            return "youtube"
        return fallback or "youtube"

    def youtube_channel_name(self, data):
        if not isinstance(data, dict):
            return None

        channel = clean_spotify_text(data.get("channel"))
        uploader = clean_spotify_text(data.get("uploader"))

        if channel:
            return channel
        if uploader:
            return uploader
        return None

    def playback_source_name(self, data, *, expected_artist=None, actual_source=None, requested_source=None):
        if not isinstance(data, dict):
            return None

        if actual_source == "youtube":
            youtube_channel = self.youtube_channel_name(data)
            if youtube_channel:
                return youtube_channel

        preferred_artist = clean_spotify_text(data.get("artist"))
        if preferred_artist and expected_artist and requested_source in {"spotify", "deezer"}:
            if is_strong_token_match(expected_artist, preferred_artist):
                return preferred_artist

        candidates = []
        for priority, key in enumerate(("channel", "uploader", "artist", "creator"), start=1):
            value = clean_spotify_text(data.get(key))
            if value:
                candidates.append((priority, value))

        if not candidates:
            return None

        expected_text = normalize_search_text(expected_artist)

        def candidate_score(item):
            priority, value = item
            score = 40 - (priority * 5)
            value_text = normalize_search_text(value)

            if value.endswith(" - Topic"):
                score += 8

            if expected_text and value_text:
                if value_text == expected_text:
                    score += 120
                elif expected_text in value_text or value_text in expected_text:
                    score += 60

                matches, _ = token_overlap_score(expected_artist, value_text)
                score += matches * 14

                if priority == 1 and (expected_text in value_text or value_text in expected_text):
                    score += 45
                if priority == 1 and value.endswith(" - Topic") and expected_text in value_text:
                    score += 40

            return score

        best_value = max(candidates, key=candidate_score)[1]
        if requested_source in {"spotify", "deezer"} and expected_artist and not has_any_token_match(expected_artist, best_value):
            return clean_spotify_text(expected_artist) or best_value
        return best_value

    def track_thumbnail_candidates(self, track):
        if not isinstance(track, dict):
            return []

        collection = track.get("_collection") if isinstance(track.get("_collection"), dict) else {}
        playback_data = track.get("_playback_data") if isinstance(track.get("_playback_data"), dict) else {}
        track_url = track.get("url") or track.get("query")

        return [
            track.get("thumbnail"),
            playback_data.get("thumbnail"),
            playback_data.get("thumbnails"),
            youtube_thumbnail_candidates_from_data(playback_data),
            youtube_thumbnail_candidates_from_url(track_url),
            collection.get("thumbnail"),
        ]

    def collection_thumbnail_candidates(self, collection, tracks=None):
        if not isinstance(collection, dict):
            return list(tracks or [])

        candidates = [
            collection.get("thumbnail"),
            collection,
        ]
        if tracks:
            candidates.extend(list(tracks))
        return candidates

    def immediate_embed_color_for_track(self, track):
        if not isinstance(track, dict):
            return self.source_color(None)
        collection = track.get("_collection") if isinstance(track.get("_collection"), dict) else None
        cached_color = find_cached_thumbnail_color(self.track_thumbnail_candidates(track))
        if isinstance(cached_color, int) and 0 <= cached_color <= 0xFFFFFF:
            return cached_color

        for candidate in (
            track.get("color"),
            collection.get("color") if isinstance(collection, dict) else None,
        ):
            if isinstance(candidate, int) and 0 <= candidate <= 0xFFFFFF:
                return candidate

        return self.source_color(track.get("source") or (collection.get("source") if collection else None))

    def immediate_embed_color_for_collection(self, collection, tracks):
        source_key = collection.get("source") if isinstance(collection, dict) else None
        cached_color = find_cached_thumbnail_color(self.collection_thumbnail_candidates(collection, tracks))
        if isinstance(cached_color, int) and 0 <= cached_color <= 0xFFFFFF:
            return cached_color

        if isinstance(collection, dict):
            stored_color = collection.get("color")
            if isinstance(stored_color, int) and 0 <= stored_color <= 0xFFFFFF:
                return stored_color

        if tracks:
            first_track = tracks[0]
            source_key = first_track.get("source") or source_key
            track_color = first_track.get("color")
            if isinstance(track_color, int) and 0 <= track_color <= 0xFFFFFF:
                return track_color
        return self.source_color(source_key)

    async def resolve_track_embed_color(self, track):
        fallback_color = self.immediate_embed_color_for_track(track)
        if not USE_DYNAMIC_EMBED_COLORS or not isinstance(track, dict):
            return fallback_color

        dynamic_color = await get_thumbnail_color(self.track_thumbnail_candidates(track), fallback_color)
        if isinstance(dynamic_color, int) and 0 <= dynamic_color <= 0xFFFFFF:
            track["color"] = dynamic_color
            return dynamic_color
        return fallback_color

    async def resolve_collection_embed_color(self, collection, tracks=None):
        fallback_color = self.immediate_embed_color_for_collection(collection, tracks or [])
        if not USE_DYNAMIC_EMBED_COLORS or not isinstance(collection, dict):
            return fallback_color

        dynamic_color = await get_thumbnail_color(self.collection_thumbnail_candidates(collection, tracks or []), fallback_color)
        if isinstance(dynamic_color, int) and 0 <= dynamic_color <= 0xFFFFFF:
            collection["color"] = dynamic_color
            return dynamic_color
        return fallback_color

    def schedule_message_color_refresh(self, message, *, track=None, collection=None, refresh_all_collection_messages=False):
        return

    def build_queue_embed(self, *, title="📜 File d'attente", embed_color=None):
        next_track = self.queue[0] if self.queue else self.current
        if not isinstance(embed_color, int):
            embed_color = self.source_color(next_track.get("source") if next_track else None)
        embed = discord.Embed(
            title=title,
            color=embed_color,
        )

        if not self.queue:
            if self.current:
                title_value = sanitize_embed_text(self.current.get("title") or "Titre inconnu")
                artist_value = sanitize_embed_text(self.current.get("artist") or "Artiste inconnu")
                duration_value = format_duration(self.current.get("duration"))
                source_value = self.source_label(self.current.get("source"))
                embed.description = (
                    f"`En cours` **{title_value}**\n"
                    f"{artist_value} • {duration_value} • {source_value}"
                )
                embed.set_footer(text="0 en attente • 1 en cours • 🎧 Discordbot (music)")
                return embed

            embed.description = "📭 La file est vide."
            embed.set_footer(text="🎧 Discordbot (music)")
            return embed

        compact_mode = len(self.queue) >= QUEUE_COMPACT_THRESHOLD
        preview_limit = 18 if compact_mode else QUEUE_PREVIEW_LIMIT
        preview_tracks = self.queue[:preview_limit]
        lines = []
        known_duration = 0
        known_duration_count = 0

        for track in self.queue:
            duration_value = track.get("duration")
            if isinstance(duration_value, (int, float)):
                known_duration += int(duration_value)
                known_duration_count += 1

        for index, track in enumerate(preview_tracks, start=1):
            title_value = sanitize_embed_text(track.get("title") or "Titre inconnu")
            artist_value = sanitize_embed_text(track.get("artist") or "Artiste inconnu")
            duration_value = format_duration(track.get("duration"))
            source_value = self.source_label(track.get("source"))
            if compact_mode:
                lines.append(f"`{index:02d}.` **{title_value}** • {artist_value} • {duration_value} • {source_value}")
            else:
                lines.append(
                    f"`{index:02d}.` **{title_value}**\n"
                    f"{artist_value} • {duration_value} • {source_value}"
                )

        embed.description = "\n".join(lines) if compact_mode else "\n\n".join(lines)

        remaining_count = max(0, len(self.queue) - len(preview_tracks))
        if remaining_count:
            embed.add_field(
                name="Suite",
                value=f"`+{remaining_count}` autres musiques dans la file.",
                inline=False,
            )

        footer_parts = [f"{len(self.queue)} en attente"]
        if self.current:
            footer_parts.insert(0, "1 en cours")
        if known_duration_count:
            footer_parts.append(f"{format_duration(known_duration)} restantes")
        footer_parts.append("🎧 Discordbot (music)")
        embed.set_footer(text=" • ".join(footer_parts))
        return embed

    def build_collection_embeds(self, tracks):
        if not tracks:
            return []

        collection = tracks[0].get("_collection")
        if not isinstance(collection, dict):
            return []

        collection_title = collection.get("title") or collection.get("label") or "Collection"
        collection_source = collection.get("source") or tracks[0].get("source")
        embed_color = collection.get("color")
        if not isinstance(embed_color, int):
            embed_color = self.source_color(collection_source)

        total_duration = 0
        known_duration_count = 0
        for track in tracks:
            duration_value = track.get("duration")
            if isinstance(duration_value, (int, float)):
                total_duration += int(duration_value)
                known_duration_count += 1

        collection_kind = str(collection.get("kind") or "").lower()
        kind_label = {
            "playlist": "Playlist",
            "album": "Album",
        }.get(collection_kind, "Collection")

        try:
            collection_count = int(collection.get("count"))
        except Exception:
            collection_count = len(tracks)
        displayed_count = len(tracks)

        info_parts = [f"`{kind_label}`"]
        if collection.get("subtitle"):
            info_parts.append(f"**{sanitize_embed_text(collection['subtitle'])}**")
        info_parts.append(f"`{collection_count} morceaux`")
        if displayed_count != collection_count:
            info_parts.append(f"`{displayed_count} visibles`")
        if known_duration_count:
            info_parts.append(f"`{format_duration(total_duration)}`")
        info_parts.append(f"`{self.source_label(collection_source)}`")
        info_line = " • ".join(info_parts)

        reserved_length = len(info_line) + 64
        track_chunks, remaining_count = build_compact_tracklist_chunks(
            tracks,
            max_chunk_length=3900,
            max_total_length=max(1200, 3900 - reserved_length),
        )

        tracklist_text = "\n".join(track_chunks).strip()
        if remaining_count:
            remainder_line = f"`+{remaining_count}` autres musiques non affichées pour garder le message lisible."
            tracklist_text = f"{tracklist_text}\n{remainder_line}".strip() if tracklist_text else remainder_line

        description_parts = [info_line]
        if tracklist_text:
            description_parts.append(tracklist_text)
        else:
            description_parts.append("**Aucun morceau affichable pour cette playlist.**")

        embed = discord.Embed(
            title=collection_title,
            color=embed_color,
            description="\n\n".join(description_parts),
        )
        if collection.get("thumbnail"):
            embed.set_image(url=collection["thumbnail"])

        embed.set_footer(text="🎧 Discordbot (music)")
        return [embed]

    async def clear_collection_messages(self):
        messages = []
        if self.collection_message:
            messages.append(self.collection_message)
        messages.extend(self.collection_extra_messages)

        for message in messages:
            try:
                await message.delete()
            except Exception:
                pass

        self.collection_message = None
        self.collection_extra_messages = []
        self.collection_signature_key = None

    def forget_collection_messages(self):
        self.collection_message = None
        self.collection_extra_messages = []
        self.collection_signature_key = None

    async def sync_collection_message(self, tracks=None, *, move_to_bottom=False):
        tracks = tracks or self.active_collection_tracks()
        if not tracks:
            self.collection_message = None
            self.collection_extra_messages = []
            self.collection_signature_key = None
            return False

        collection = tracks[0].get("_collection")
        if isinstance(collection, dict):
            resolved_color = await self.resolve_collection_embed_color(collection, tracks)
            collection["color"] = resolved_color
            for track in tracks:
                track_collection = track.get("_collection")
                if isinstance(track_collection, dict):
                    track_collection["color"] = resolved_color

        embeds = self.build_collection_embeds(tracks)
        if not embeds:
            self.collection_message = None
            self.collection_extra_messages = []
            self.collection_signature_key = None
            return False

        current_signature = self.collection_signature(tracks[0].get("_collection"))

        existing_messages = []
        if self.collection_message:
            existing_messages.append(self.collection_message)
        existing_messages.extend(self.collection_extra_messages)

        if existing_messages and existing_messages[0].channel != self.ctx.channel:
            self.collection_message = None
            self.collection_extra_messages = []
            self.collection_signature_key = None
            existing_messages = []

        if (
            existing_messages
            and not move_to_bottom
            and self.collection_signature_key == current_signature
        ):
            return True

        sent_messages = []
        for embed_index, embed in enumerate(embeds, start=1):
            try:
                sent = await send_unique_embed(
                    self.ctx,
                    embed,
                    key=f"collection:{current_signature}:{embed_index}",
                )
                if sent is not None:
                    sent_messages.append(sent)
            except Exception:
                continue

        self.collection_message = sent_messages[0] if sent_messages else None
        self.collection_extra_messages = sent_messages[1:] if len(sent_messages) > 1 else []
        self.collection_signature_key = current_signature if sent_messages else None
        return bool(sent_messages)

    async def clear_queue_message(self):
        if not self.queue_message:
            self.queue_message_signature = None
            return
        try:
            await self.queue_message.delete()
        except Exception:
            pass
        self.queue_message = None
        self.queue_message_signature = None

    async def purge_queue_messages_in_channel(self, limit=30):
        if bot.user is None:
            return
        try:
            async for message in self.ctx.channel.history(limit=limit):
                if message.author.id != bot.user.id or not message.embeds:
                    continue
                first_embed = message.embeds[0]
                if str(first_embed.title or "").strip() == "📜 File d'attente":
                    try:
                        await message.delete()
                    except Exception:
                        pass
        except Exception:
            return

    async def find_existing_queue_message(self):
        if bot.user is None:
            return None
        try:
            async for message in self.ctx.channel.history(limit=20):
                if message.author.id != bot.user.id or not message.embeds:
                    continue
                first_embed = message.embeds[0]
                if str(first_embed.title or "").strip() == "📜 File d'attente":
                    return message
        except Exception:
            return None
        return None

    async def sync_queue_message(self, *, move_to_bottom=False, force=False, allow_auto=False):
        # By default queue embed is manual-only (r!queue). allow_auto is used for
        # specific UX cases, like adding a track while another one is already playing.
        if not force and not allow_auto:
            return

        async with self.queue_sync_lock:
            if not self.queue and not self.current:
                await self.clear_queue_message()
                return

            if self.queue_message and self.queue_message.channel != self.ctx.channel:
                await self.clear_queue_message()

            if self.queue_message is None:
                existing_message = await self.find_existing_queue_message()
                if existing_message and existing_message.channel == self.ctx.channel:
                    self.queue_message = existing_message
                    existing_embed = existing_message.embeds[0] if existing_message.embeds else None
                    self.queue_message_signature = embed_signature(existing_embed, key="queue_message")

            next_track = self.queue[0] if self.queue else self.current
            color_seed_track = self.current if isinstance(self.current, dict) else next_track
            if isinstance(color_seed_track, dict):
                embed_color = (
                    await self.resolve_track_embed_color(color_seed_track)
                    if force
                    else self.immediate_embed_color_for_track(color_seed_track)
                )
            else:
                embed_color = None
            embed = self.build_queue_embed(embed_color=embed_color)
            new_signature = embed_signature(embed, key="queue_message")

            if self.queue_message and not move_to_bottom:
                existing_signature = self.queue_message_signature
                if existing_signature is None:
                    try:
                        existing_embed = self.queue_message.embeds[0] if self.queue_message.embeds else None
                        existing_signature = embed_signature(existing_embed, key="queue_message")
                    except Exception:
                        existing_signature = None
                if existing_signature == new_signature:
                    self.queue_message_signature = new_signature
                    return

            if self.queue_message:
                await self.clear_queue_message()
            else:
                self.queue_message_signature = None

            try:
                self.queue_message = await send_unique_embed(self.ctx, embed, key="queue_message")
                self.queue_message_signature = new_signature if self.queue_message else None
            except Exception:
                self.queue_message = None
                self.queue_message_signature = None

    async def normalize_queue_item(self, item):
        if isinstance(item, dict):
            return item

        query_text = str(item or "").strip()
        if query_text:
            cached_track = get_cached(queue_item_resolution_cache, query_text)
            if isinstance(cached_track, dict):
                return dict(cached_track)
        inferred_source = infer_source_from_input(item) if looks_like_url(item) else "youtube"

        if looks_like_url(item) and not self.playing and not self.current and not self.queue:
            if inferred_source in {"youtube", "soundcloud"}:
                page_metadata = (
                    await resolve_youtube_page_metadata(item)
                    if inferred_source == "youtube"
                    else await resolve_webpage_media_metadata(item)
                )
                page_title = clean_spotify_text(page_metadata.get("title"))
                page_owner = clean_spotify_text(page_metadata.get("owner"))
                thumbnail_candidates = (
                    youtube_thumbnail_candidates_from_url(item)
                    if inferred_source == "youtube"
                    else extract_thumbnail_urls(page_metadata.get("thumbnail"))
                )
                track_data = {
                    "url": item,
                    "query": item,
                    "title": page_title or None,
                    "thumbnail": thumbnail_candidates[0] if thumbnail_candidates else None,
                    "artist": page_owner or None,
                    "duration": None,
                    "source": inferred_source,
                }
                set_cached(queue_item_resolution_cache, query_text, dict(track_data), TRACK_CACHE_TTL_SECONDS)
                return track_data
            track_data = {
                "url": item,
                "query": item,
                "title": None,
                "thumbnail": None,
                "artist": None,
                "duration": None,
                "source": inferred_source,
            }
            set_cached(queue_item_resolution_cache, query_text, dict(track_data), TRACK_CACHE_TTL_SECONDS)
            return track_data

        if looks_like_url(item):
            loop = asyncio.get_event_loop()
            try:
                data = await loop.run_in_executor(None, lambda: ytdl.extract_info(item, download=False))
                if "entries" in data and data["entries"]:
                    data = next((entry for entry in data["entries"] if entry), None) or data["entries"][0]
                title = data.get("title", item) if data else item
                thumb = data.get("thumbnail") if data else None
                uploader = (data.get("channel") or data.get("uploader")) if data else None
                duration = data.get("duration") if data else None
                extractor = ((data.get("extractor_key") or "").lower()) if data else ""
            except Exception:
                title, thumb, uploader, duration, extractor = item, None, None, None, ""
                if inferred_source in {"youtube", "soundcloud"}:
                    page_metadata = (
                        await resolve_youtube_page_metadata(item)
                        if inferred_source == "youtube"
                        else await resolve_webpage_media_metadata(item)
                    )
                    page_title = clean_spotify_text(page_metadata.get("title"))
                    page_owner = clean_spotify_text(page_metadata.get("owner"))
                    thumbnail_candidates = (
                        youtube_thumbnail_candidates_from_url(item)
                        if inferred_source == "youtube"
                        else extract_thumbnail_urls(page_metadata.get("thumbnail"))
                    )
                    if page_title:
                        title = page_title
                    if page_owner:
                        uploader = page_owner
                    if thumbnail_candidates and not thumb:
                        thumb = thumbnail_candidates[0]
        else:
            title, thumb, uploader, duration, extractor = None, None, None, None, ""
            if query_text:
                loop = asyncio.get_event_loop()
                candidates = await resolve_text_query_candidates(
                    loop,
                    query_text,
                    source="youtube",
                    limit=max(4, YOUTUBE_PRIMARY_SEARCH_SIZE + 2),
                    enrich_limit=2,
                )
                first_entry = next((entry for entry in candidates if isinstance(entry, dict)), None)
                if isinstance(first_entry, dict):
                    title = first_entry.get("title") or query_text
                    thumb = first_entry.get("thumbnail")
                    uploader = first_entry.get("artist") or first_entry.get("channel") or first_entry.get("uploader")
                    duration = first_entry.get("duration")
                    extractor = str(first_entry.get("extractor_key") or first_entry.get("extractor") or "").lower()
                    if not thumb:
                        thumbnail_candidates = youtube_thumbnail_candidates_from_data(first_entry)
                        if thumbnail_candidates:
                            thumb = thumbnail_candidates[0]

                if not title:
                    title = query_text

        if "soundcloud" in extractor:
            source = "soundcloud"
        elif "bandcamp" in extractor or is_bandcamp_url(item):
            source = "bandcamp"
        elif inferred_source == "soundcloud":
            source = "soundcloud"
        elif inferred_source == "bandcamp":
            source = "bandcamp"
        else:
            source = "youtube"

        title, uploader = clean_provider_metadata(title, uploader, source=source)
        if not title and query_text:
            title = query_text

        track_data = {
            "url": item if looks_like_url(item) else None,
            "query": item,
            "title": title,
            "thumbnail": thumb,
            "artist": uploader,
            "duration": duration,
            "source": source,
        }
        if query_text:
            set_cached(queue_item_resolution_cache, query_text, dict(track_data), TRACK_CACHE_TTL_SECONDS)
        return track_data

    async def resolve_skip_status(self, content):
        if not self.skip_status_message:
            return
        try:
            await self.skip_status_message.edit(content=content)
        except Exception:
            pass
        self.skip_status_message = None

    def handle_playback_after(self, error):
        elapsed_seconds = self.playback_elapsed_seconds()
        current_track = self.current if isinstance(self.current, dict) else None
        current_duration = None
        current_source = None
        if current_track:
            current_duration = current_track.get("duration")
            playback_data = current_track.get("_playback_data") if isinstance(current_track.get("_playback_data"), dict) else {}
            if not isinstance(current_duration, (int, float)):
                current_duration = playback_data.get("duration")
            current_source = self.playback_source_key(playback_data, fallback=current_track.get("source"))

        self.cancel_playback_watchdog()
        self.playing = False

        if self.suppress_next_after:
            self.suppress_next_after = False
            self.after_playback_advance_pending = False
            return

        near_track_end = self.is_track_near_end(current_track, elapsed_seconds=elapsed_seconds, tail_seconds=12.0, fraction=0.75)
        has_following_track = self.has_following_track()
        if has_following_track:
            self.force_advance_once = True
            self.replay_current_on_error = False
        should_retry_current = (
            error is not None
            and current_track
            and not self.force_advance_once
            and current_source != "soundcloud"
            and not near_track_end
            and not has_following_track
            and self.can_retry_current_track()
        )

        if should_retry_current:
            # Voice/network interruptions can stop FFmpeg early; retry same track
            # once instead of consuming the next item in queue.
            self.mark_current_retry()
            self.replay_current_on_error = True
        else:
            self.replay_current_on_error = False

        self.after_playback_advance_pending = True
        asyncio.run_coroutine_threadsafe(self._advance_after_playback(), bot.loop)

    async def seek_current(self, seconds):
        voice_client = self.get_voice_client()
        if voice_client is None:
            raise RuntimeError("Le bot n'est pas connecté à un salon vocal.")

        track = self.current
        if not track:
            raise RuntimeError("Aucune musique n'est en cours.")

        if seconds < 0:
            raise RuntimeError("Le timecode doit être positif.")

        duration = track.get("duration")
        if isinstance(duration, (int, float)) and seconds >= int(duration):
            raise RuntimeError("Ce timecode dépasse la durée du morceau.")

        was_paused = voice_client.is_paused()
        try:
            player = await YTDLSource.from_track(track, volume=1.0, start_at=seconds)
        except Exception:
            track.pop("_playback_data", None)
            player = await YTDLSource.from_track(track, volume=1.0, start_at=seconds)

        if not track.get("title"):
            track["title"] = player.data.get("title") or "Titre inconnu"
        if not track.get("artist") or track.get("artist") == "Artiste inconnu":
            track["artist"] = player.data.get("artist") or player.data.get("uploader") or player.data.get("channel") or "Artiste inconnu"
        if not track.get("duration"):
            track["duration"] = player.data.get("duration")
        if not track.get("thumbnail"):
            track["thumbnail"] = player.data.get("thumbnail")
        track["_playback_data"] = dict(player.data)
        track["_seek_position"] = int(seconds)
        self.playing = True
        self.cancel_idle_disconnect()

        if voice_client.is_playing() or voice_client.is_paused():
            self.cancel_playback_watchdog()
            self.suppress_next_after = True
            voice_client.stop()
            await asyncio.sleep(0.05)

        try:
            voice_client.play(player, after=self.handle_playback_after)
        except Exception as exc:
            self.suppress_next_after = False
            raise RuntimeError(f"Impossible de lancer la lecture : {exc}")

        self.mark_playback_started(start_offset=seconds, paused=was_paused)
        if was_paused:
            voice_client.pause()

        return int(seconds)

    def cancel_idle_disconnect(self):
        if self.idle_disconnect_task and not self.idle_disconnect_task.done():
            self.idle_disconnect_task.cancel()
        self.idle_disconnect_task = None

    def schedule_idle_disconnect(self):
        self.cancel_idle_disconnect()
        self.idle_disconnect_task = asyncio.create_task(self._idle_disconnect_worker())

    def schedule_voice_recovery(self, delay=1.2):
        if self.voice_recovery_task and not self.voice_recovery_task.done():
            return

        async def _recover():
            try:
                await asyncio.sleep(delay)
                for _ in range(20):
                    if self.stop_in_progress:
                        return
                    if not self.playback_transition:
                        break
                    await asyncio.sleep(0.15)

                if self.stop_in_progress:
                    return

                if not self.playback_transition and not self.playing:
                    if self.has_following_track():
                        self.force_advance_once = True
                    self.start_playback_background()
            except asyncio.CancelledError:
                return
            finally:
                self.voice_recovery_task = None

        self.voice_recovery_task = asyncio.create_task(_recover())

    async def _idle_disconnect_worker(self):
        try:
            await asyncio.sleep(AUTO_DISCONNECT_SECONDS)
        except asyncio.CancelledError:
            return

        voice_client = self.get_voice_client()
        if voice_client is None:
            return
        if voice_client.is_playing():
            return

        try:
            self.cancel_playback_watchdog()
            await voice_client.disconnect()
        except Exception:
            return

        self.forget_collection_messages()
        players.pop(self.ctx.guild.id, None)
        await bot.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.listening,
                name=f"{PREFIX}help",
            )
        )
        await self.ctx.send("⏹️ Déconnexion automatique après 3 minutes d'inactivité.")

    async def play_next(self):
        if self.playback_transition:
            return

        self.playback_transition = True
        restart_needed = False
        after_playback_advance = self.after_playback_advance_pending
        self.after_playback_advance_pending = False
        voice_client = self.get_voice_client()
        try:
            if voice_client is None:
                for _ in range(8):
                    await asyncio.sleep(0.35)
                    voice_client = self.get_voice_client()
                    if voice_client is not None:
                        break
            if voice_client is None:
                self.playing = False
                if self.current and not self.force_advance_once and not self.has_following_track() and self.can_retry_current_track():
                    self.mark_current_retry()
                    self.replay_current_on_error = True
                    self.voice_recovery_attempts += 1
                    if self.voice_recovery_attempts <= 10:
                        self.schedule_voice_recovery(delay=1.2)
                        return
                self.replay_current_on_error = False
                self.voice_recovery_attempts = 0
                await self.resolve_skip_status("**⚠️ Le bot n'est plus connecté au vocal.**")
                return

            if (
                self.has_live_voice_activity(voice_client)
                and self.current
                and self.has_following_track()
                and not after_playback_advance
                and not self.force_advance_once
                and not self.replay_current_on_error
                and not self.is_track_near_end(self.current, tail_seconds=4.0, fraction=0.90)
            ):
                self.playing = True
                self.start_playback_watchdog(self.current)
                return

            force_advance = self.force_advance_once
            self.force_advance_once = False

            if self.replay_current_on_error and self.current and not force_advance:
                track = self.current
                self.replay_current_on_error = False
            elif self.loop and self.current and not force_advance:
                track = self.current
            else:
                self.replay_current_on_error = False
                if not self.queue:
                    if self.loopqueue and self.original_queue:
                        self.queue = self.original_queue.copy()
                    else:
                        self.playing = False
                        self.schedule_idle_disconnect()
                        await self.sync_queue_message()
                        await self.resolve_skip_status("**⚠️ Plus aucune musique dans la file.**")
                        return
                skip_history = self.skip_history_once
                self.skip_history_once = False
                if self.current and not skip_history:
                    self.history.append(self.current)
                track = self.queue.pop(0)
                self.current = track

            transition_token = self.navigation_token
            self.playing = True
            self.cancel_idle_disconnect()

            try:
                player = await YTDLSource.from_track(track, volume=1.0)
            except Exception as exc:
                await send_unique_feedback(
                    self.ctx,
                    f"**❌ Impossible de lire : {exc}**",
                    key=f"play_next_load_error:{track.get('title') or track.get('url') or 'unknown'}",
                )
                self.playing = False
                await self.resolve_skip_status("**❌ Impossible de charger la musique suivante.**")
                self.playback_transition = False
                self.start_playback_background()
                return

            if transition_token != self.navigation_token or self.current is not track:
                try:
                    player.cleanup()
                except Exception:
                    pass
                restart_needed = True
            if restart_needed:
                self.playing = False
            else:
                if not track.get("title"):
                    track["title"] = player.data.get("title") or "Titre inconnu"
                if not track.get("artist") or track.get("artist") == "Artiste inconnu":
                    track["artist"] = player.data.get("artist") or player.data.get("uploader") or player.data.get("channel") or "Artiste inconnu"
                if not track.get("duration"):
                    track["duration"] = player.data.get("duration")
                collection = track.get("_collection") if isinstance(track.get("_collection"), dict) else None
                collection_thumbnail = collection.get("thumbnail") if collection else None
                if not track.get("thumbnail"):
                    track["thumbnail"] = player.data.get("thumbnail")
                elif (
                    collection
                    and collection.get("kind") == "playlist"
                    and collection_thumbnail
                    and track.get("thumbnail") == collection_thumbnail
                    and player.data.get("thumbnail")
                ):
                    track["thumbnail"] = player.data.get("thumbnail")
                track["_playback_data"] = dict(player.data)
                track["_seek_position"] = 0

                actual_source_key = self.playback_source_key(player.data, fallback=track.get("source"))
                actual_source_name = self.playback_source_name(
                    player.data,
                    expected_artist=track.get("artist"),
                    actual_source=actual_source_key,
                    requested_source=track.get("source"),
                )

                is_direct_youtube = actual_source_key == "youtube" and track.get("source") == "youtube"
                display_artist = track.get("artist", "Artiste inconnu")
                if is_direct_youtube and actual_source_name:
                    display_artist = actual_source_name

                requested_source_key = track.get("source") or actual_source_key
                source_value = self.source_label(requested_source_key)

                voice_client = self.get_voice_client()
                if voice_client is None:
                    await asyncio.sleep(0.5)
                    voice_client = self.get_voice_client()
                if voice_client is None:
                    self.playing = False
                    await send_unique_feedback(self.ctx, "**❌ Le salon vocal n'est plus disponible.**", key="voice_unavailable")
                    return

                if after_playback_advance or force_advance:
                    is_idle = await self.wait_for_voice_idle(voice_client, timeout=1.25, poll_interval=0.05)
                    if not is_idle and (voice_client.is_playing() or voice_client.is_paused()):
                        self.suppress_next_after = True
                        try:
                            voice_client.stop()
                        except Exception:
                            self.suppress_next_after = False
                        await asyncio.sleep(0.12)

                playback_started = False
                try:
                    voice_client.play(player, after=self.handle_playback_after)
                    playback_started = True
                except Exception as exc:
                    if "Already playing audio" in str(exc):
                        if after_playback_advance or force_advance:
                            await asyncio.sleep(0.35)
                            voice_client = self.get_voice_client() or voice_client
                            if voice_client and (voice_client.is_playing() or voice_client.is_paused()):
                                self.suppress_next_after = True
                                try:
                                    voice_client.stop()
                                except Exception:
                                    self.suppress_next_after = False
                                await asyncio.sleep(0.15)
                            try:
                                voice_client = self.get_voice_client() or voice_client
                                if voice_client is None:
                                    raise RuntimeError("Le salon vocal n'est plus disponible.")
                                voice_client.play(player, after=self.handle_playback_after)
                                playback_started = True
                            except Exception as retry_exc:
                                if "Already playing audio" not in str(retry_exc):
                                    self.playing = False
                                    await send_unique_feedback(
                                        self.ctx,
                                        f"**❌ Impossible de lancer la lecture : {retry_exc}**",
                                        key=f"voice_play_error:{type(retry_exc).__name__}",
                                    )
                                    self.playback_transition = False
                                    self.start_playback_background()
                                    return
                        if not playback_started:
                            self.playing = False
                            self.after_playback_advance_pending = True
                            self.schedule_voice_recovery(delay=0.35)
                            return
                    self.playing = False
                    await send_unique_feedback(self.ctx, f"**❌ Impossible de lancer la lecture : {exc}**", key=f"voice_play_error:{type(exc).__name__}")
                    self.playback_transition = False
                    self.start_playback_background()
                    return
                self.reset_track_retry(track)
                self.mark_playback_started(start_offset=track.get("_seek_position") or 0, paused=False)
                self.start_playback_watchdog(track)
                await self.resolve_skip_status("**⏭️ Musique passée !**")
                self.voice_recovery_attempts = 0
                if self.voice_recovery_task and not self.voice_recovery_task.done():
                    self.voice_recovery_task.cancel()

                if actual_source_key == "youtube":
                    resolved_owner_name = await resolve_youtube_owner_name(player.data)
                    if resolved_owner_name:
                        actual_source_name = resolved_owner_name
                        if is_direct_youtube:
                            display_artist = resolved_owner_name

                display_title, cleaned_display_artist = clean_provider_metadata(
                    track.get("title", player.title) or player.title,
                    display_artist,
                    source=requested_source_key,
                )
                if display_title:
                    track["title"] = display_title
                if cleaned_display_artist:
                    display_artist = cleaned_display_artist
                    if not track.get("artist") or track.get("artist") == "Artiste inconnu" or requested_source_key == "soundcloud":
                        track["artist"] = cleaned_display_artist

                embed_color = await self.resolve_track_embed_color(track)
                if isinstance(collection, dict):
                    collection["color"] = embed_color

                embed = discord.Embed(
                    title=track.get("title") or player.title or "Titre inconnu",
                    color=embed_color,
                )
                artist_field_name = "Chaîne" if is_direct_youtube else "Artiste"
                details_line = (
                    f"{artist_field_name}: {display_artist} • "
                    f"Durée: {format_duration(track.get('duration'))} • "
                    f"Source: {source_value}"
                )
                if track.get("thumbnail"):
                    embed.set_image(url=track["thumbnail"])
                embed.set_footer(text=details_line)
                await send_unique_embed(self.ctx, embed, key="now_playing")

                await bot.change_presence(
                    activity=discord.Activity(
                        type=discord.ActivityType.listening,
                        name=f"{display_artist} - {track.get('title', 'Titre inconnu')}",
                    )
                )
                await self.sync_queue_message()
        finally:
            self.playback_transition = False
            if (
                self.after_playback_advance_pending
                and not self.stop_in_progress
                and not self.playing
                and not self.has_live_voice_activity()
            ):
                self.start_playback_background()

        if restart_needed:
            await self.play_next()

    async def add_many_to_queue(self, items):
        voice_client = self.get_voice_client()
        had_active_playback = self.has_pending_playback(voice_client)

        added_tracks = []
        for item in items:
            track = await self.normalize_queue_item(item)
            self.queue.append(track)
            added_tracks.append(track)

        if not added_tracks:
            return added_tracks

        self.cancel_idle_disconnect()

        if self.loopqueue:
            self.update_snapshot()

        if not any(isinstance(track.get("_collection"), dict) for track in added_tracks):
            self.auto_queue_message_enabled = True

        if had_active_playback:
            await self.sync_queue_message(move_to_bottom=True, allow_auto=True)

        has_live_voice_playback = self.has_live_voice_activity(voice_client)
        should_start_playback = (
            not has_live_voice_playback
            and not self.playing
            and not self.playback_transition
            and not self.current
        )
        if should_start_playback:
            self.start_playback_background()

        return added_tracks

    async def add_to_queue(self, item):
        tracks = await self.add_many_to_queue([item])
        return tracks[0] if tracks else None


players = {}


def get_player(ctx):
    if ctx.guild.id not in players:
        players[ctx.guild.id] = MusicPlayer(ctx)
    else:
        players[ctx.guild.id].ctx = ctx
    return players[ctx.guild.id]


@bot.command(help="🔊 Joue une musique ou l'ajoute à la file d'attente")
async def play(ctx, *, url: str = ""):
    if not should_handle_command(ctx, "play"):
        return

    url = re.sub(rf"^(?:{re.escape(PREFIX)}play\s+)+", "", str(url or "").strip(), flags=re.I).strip()
    if not url:
        await send_unique_feedback(ctx, "⚠️ Indique un lien ou une recherche après `r!play`.", key=command_feedback_key(ctx, "play_empty"))
        return

    if ctx.author.voice is None:
        return await ctx.send("**⚠️ Tu dois être dans un salon vocal !**")

    player = get_player(ctx)
    channel = ctx.author.voice.channel
    guild_vc = ctx.guild.voice_client
    has_active_playback = player.has_pending_playback(guild_vc)

    if guild_vc is None:
        if has_active_playback:
            # During transient reconnect states, keep queueing instead of forcing
            # a fresh voice connect that can interrupt current playback.
            pass
        else:
            connect_error = None
            for _ in range(2):
                try:
                    await channel.connect(timeout=20, reconnect=True)
                    connect_error = None
                    break
                except Exception as exc:
                    connect_error = exc
                    stale_vc = ctx.guild.voice_client
                    if stale_vc:
                        try:
                            await stale_vc.disconnect()
                        except Exception:
                            pass
                    await asyncio.sleep(0.8)

            if connect_error and not (ctx.guild.voice_client and ctx.guild.voice_client.is_connected()):
                await send_unique_feedback(ctx, "**❌ Connexion vocale impossible (timeout). Réessaie dans quelques secondes.**", key=command_feedback_key(ctx, "play_connect_timeout"))
                return
    else:
        if guild_vc.channel and guild_vc.channel != channel and guild_vc.is_connected():
            try:
                await guild_vc.move_to(channel)
            except Exception:
                await send_unique_feedback(ctx, "**❌ Impossible de rejoindre ce salon vocal. Réessaie dans quelques secondes.**", key=command_feedback_key(ctx, "play_move_fail"))
                return
        elif not guild_vc.is_connected():
            if not has_active_playback:
                # Give Discord's internal reconnect loop a short chance before forcing a reconnect.
                existing_ready = await wait_for_voice_client(ctx.guild, timeout=2.0, poll_interval=0.2)
                if existing_ready is None:
                    try:
                        await guild_vc.disconnect()
                    except Exception:
                        pass
                    try:
                        await channel.connect(timeout=20, reconnect=True)
                    except Exception:
                        await send_unique_feedback(ctx, "**❌ Connexion vocale impossible (timeout). Réessaie dans quelques secondes.**", key=command_feedback_key(ctx, "play_reconnect_timeout"))
                        return

    voice_client = await wait_for_voice_client(ctx.guild, timeout=2.5 if has_active_playback else VOICE_READY_WAIT_SECONDS)
    if voice_client is None and not has_active_playback:
        await send_unique_feedback(ctx, "**❌ La connexion vocale n'est pas encore prête. Réessaie dans un instant.**", key=command_feedback_key(ctx, "play_voice_not_ready"))
        return

    if voice_client is not None:
        player.cancel_idle_disconnect()

    if looks_like_url(url):
        schedule_message_embed_suppression(ctx.message, attempts=12, delay=0.7)

    async with ctx.typing():
        if is_spotify_url(url):
            try:
                tracks = await spotify_to_tracks(url)
                if not tracks:
                    await ctx.send("**⚠️ Aucun morceau Spotify trouvé.**")
                    return
                await player.add_many_to_queue(tracks)
            except Exception as exc:
                await ctx.send(f"**❌ Erreur Spotify : {exc}**")
            return

        if is_deezer_url(url):
            try:
                tracks = await deezer_to_tracks(url)
                if tracks:
                    await player.add_many_to_queue(tracks)
                else:
                    await ctx.send("**❌ Lien Deezer non reconnu après redirection. Envoie un lien deezer.com/track|album|playlist/ID.**")
            except Exception as exc:
                await ctx.send(f"**❌ Erreur Deezer : {exc}**")
            return

        if is_bandcamp_url(url):
            try:
                tracks = await bandcamp_to_tracks(url)
                await player.add_many_to_queue(tracks)
            except Exception as exc:
                await ctx.send(f"**❌ Erreur Bandcamp : {exc}**")
            return

        if is_youtube_playlist_url(url):
            try:
                tracks = await youtube_playlist_to_tracks(url)
                await player.add_many_to_queue(tracks)
            except Exception as exc:
                await ctx.send(f"**❌ Erreur YouTube playlist : {exc}**")
            return

        player.collection_message = None
        player.collection_extra_messages = []
        player.collection_signature_key = None
        await player.add_to_queue(url)

        return


@bot.command(help="⏮️ Retourne à la musique précédente, ou plusieurs avec un nombre")
async def previous(ctx, count: str = "1"):
    player = get_player(ctx)
    async with player.control_lock:
        if not await player.wait_for_stable_state(timeout=1.25):
            return await send_unique_feedback(ctx, "⚠️ Transition en cours.", key=guild_feedback_key(ctx, "player_busy"), window=1.5)

        timeline, cursor_index = player.build_playback_timeline()
        if not timeline or cursor_index <= 0:
            return await ctx.send("⚠️ Aucune musique précédente.")

        try:
            previous_count = int(str(count).strip())
        except Exception:
            return await ctx.send("⚠️ Utilise un nombre, par exemple `r!previous 2`.")

        if previous_count <= 0:
            return await ctx.send("⚠️ Le nombre de musiques à remonter doit être supérieur à 0.")

        target_index = max(0, cursor_index - previous_count)
        target_track = player.jump_to_timeline_index(timeline, target_index) or timeline[target_index]

        voice_client = player.get_voice_client()
        should_start_directly = not (voice_client and (voice_client.is_playing() or voice_client.is_paused()))
        if voice_client and (voice_client.is_playing() or voice_client.is_paused()):
            voice_client.stop()

    if should_start_directly:
        await player.play_next()

    await ctx.send(f"⏮️ Retour à : **{target_track.get('title', 'Inconnu')}**")


@bot.command(help="⏭️ Passe la musique en cours, ou plusieurs avec un nombre")
async def skip(ctx, count: str = "1"):
    if not should_handle_command(ctx, "skip"):
        return

    player = get_player(ctx)
    async with player.control_lock:
        if not await player.wait_for_stable_state(timeout=1.25):
            return await send_unique_feedback(ctx, "⚠️ Transition en cours.", key=guild_feedback_key(ctx, "player_busy"), window=1.5)

        vc = player.get_voice_client()

        try:
            skip_count = int(str(count).strip())
        except Exception:
            await send_unique_feedback(ctx, "⚠️ Utilise un nombre, par exemple `r!skip 3`.", key=command_feedback_key(ctx, "skip_bad_count"))
            return

        if skip_count <= 0:
            await send_unique_feedback(ctx, "⚠️ Le nombre de musiques à passer doit être supérieur à 0.", key=command_feedback_key(ctx, "skip_bad_positive"))
            return

        has_voice_activity = player.has_live_voice_activity(vc)
        has_track_state = player.has_pending_playback(vc)
        if not has_voice_activity and not has_track_state:
            await send_unique_feedback(ctx, "⚠️ Aucune musique à skip.", key=command_feedback_key(ctx, "skip_none"))
            return

        timeline, cursor_index = player.build_playback_timeline()

        if timeline and cursor_index < len(timeline):
            target_index = min(len(timeline), cursor_index + skip_count)
            player.jump_to_timeline_index(timeline, target_index)

        should_start_directly = not (vc and (vc.is_playing() or vc.is_paused()))
        if vc and (vc.is_playing() or vc.is_paused()):
            vc.stop()

    if should_start_directly:
        await player.play_next()


@bot.command(help="⏸️ Met la musique en pause")
async def pause(ctx):
    player = get_player(ctx)
    vc = ctx.voice_client
    if vc and vc.is_playing():
        vc.pause()
        player.mark_playback_paused()
        await ctx.send("⏸️ Musique mise en pause.")
    else:
        await ctx.send("⚠️ Pas de musique à mettre en pause.")


@bot.command(help="⏹️ Stoppe la lecture et vide la file d'attente")
async def stop(ctx):
    if not should_handle_command(ctx, "stop"):
        return

    player = get_player(ctx)
    async with player.control_lock:
        if player.stop_in_progress:
            return
        player.stop_in_progress = True
        try:
            vc = player.get_voice_client() or ctx.voice_client or ctx.guild.voice_client
            has_voice_connection = bool(vc and vc.is_connected())
            has_voice_activity = player.has_live_voice_activity(vc)
            has_player_activity = player.has_pending_playback(vc)

            if not has_voice_connection and not has_player_activity:
                await send_unique_feedback(ctx, "⏹️ Lecture déjà stoppée.", key=command_feedback_key(ctx, "stop_already_stopped"))
                return

            if player.play_start_task and not player.play_start_task.done():
                player.play_start_task.cancel()
            if player.voice_recovery_task and not player.voice_recovery_task.done():
                player.voice_recovery_task.cancel()
            player.navigation_token += 1

            player.queue.clear()
            player.original_queue = []
            player.loop = False
            player.loopqueue = False
            player.current = None
            player.playing = False
            player.auto_queue_message_enabled = True
            player.replay_current_on_error = False
            player.force_advance_once = False
            player.voice_recovery_attempts = 0
            player.after_playback_advance_pending = False

            await player.clear_queue_message()
            await player.clear_collection_messages()

            if has_voice_activity:
                player.cancel_playback_watchdog()
                player.suppress_next_after = True
                vc.stop()
            else:
                player.cancel_playback_watchdog()
                player.suppress_next_after = False

            player.schedule_idle_disconnect()
            await send_unique_feedback(ctx, "⏹️ Lecture stoppée et file vidée !", key=command_feedback_key(ctx, "stop_done"))
        finally:
            player.stop_in_progress = False


@bot.command(help="▶️ Reprend la musique en pause")
async def resume(ctx):
    player = get_player(ctx)
    vc = ctx.voice_client
    if vc and vc.is_paused():
        vc.resume()
        player.mark_playback_resumed()
        await ctx.send("▶️ Musique reprise.")
    else:
        await ctx.send("⚠️ Aucune musique en pause.")


@bot.command(help="⏩ Va à un timecode dans la musique en cours")
async def seek(ctx, *, timecode: str):
    player = get_player(ctx)

    try:
        seconds = parse_timecode(timecode)
    except ValueError as exc:
        return await ctx.send(f"⚠️ {exc}")

    async with player.control_lock:
        if not await player.wait_for_stable_state(timeout=1.25):
            return await send_unique_feedback(ctx, "⚠️ Transition en cours.", key=guild_feedback_key(ctx, "player_busy"), window=1.5)

        async with ctx.typing():
            try:
                new_position = await player.seek_current(seconds)
            except RuntimeError as exc:
                return await ctx.send(f"⚠️ {exc}")
            except Exception as exc:
                return await ctx.send(f"❌ Impossible d'aller à ce timecode : {exc}")

    await ctx.send(f"⏩ Position : **{format_timecode(new_position)}**")


@bot.command(help="📜 Affiche la file d'attente")
async def queue(ctx):
    player = get_player(ctx)
    if not player.queue and not player.current:
        return await ctx.send("📭 La file est vide.")
    collection_tracks = player.active_collection_tracks()
    if collection_tracks:
        await player.sync_collection_message(collection_tracks)
        return
    player.auto_queue_message_enabled = True
    await player.sync_queue_message(move_to_bottom=True, force=True)


@bot.command(help="🗑️ Vide la file d'attente")
async def clear(ctx):
    player = get_player(ctx)
    player.queue.clear()
    player.auto_queue_message_enabled = True
    if player.loopqueue:
        player.original_queue = []
    if ctx.voice_client and not ctx.voice_client.is_playing():
        player.schedule_idle_disconnect()
    await player.clear_queue_message()
    await player.clear_collection_messages()
    await ctx.send("🗑️ File vidée !")


@bot.command(help="⏹️ Stoppe la musique et déconnecte le bot")
async def leave(ctx):
    if not should_handle_command(ctx, "leave"):
        return

    player = players.get(ctx.guild.id)
    if player:
        async with player.control_lock:
            voice_client = ctx.guild.voice_client or ctx.voice_client
            player.cancel_idle_disconnect()
            player.cancel_playback_watchdog()
            if player.play_start_task and not player.play_start_task.done():
                player.play_start_task.cancel()
            if player.voice_recovery_task and not player.voice_recovery_task.done():
                player.voice_recovery_task.cancel()
            player.queue.clear()
            player.original_queue = []
            player.current = None
            player.playing = False
            player.replay_current_on_error = False
            player.force_advance_once = False
            player.voice_recovery_attempts = 0
            player.after_playback_advance_pending = False
            player.auto_queue_message_enabled = True
            await player.clear_queue_message()
            player.forget_collection_messages()
            players.pop(ctx.guild.id, None)

            if voice_client and voice_client.is_connected():
                await voice_client.disconnect()
                await send_unique_feedback(ctx, "⏹️ Déconnecté.", key=command_feedback_key(ctx, "leave_done"))
            else:
                await send_unique_feedback(ctx, "⏹️ Déconnecté.", key=command_feedback_key(ctx, "leave_done"))
        return

    voice_client = ctx.guild.voice_client or ctx.voice_client
    players.pop(ctx.guild.id, None)
    if voice_client and voice_client.is_connected():
        await voice_client.disconnect()
    await send_unique_feedback(ctx, "⏹️ Déconnecté.", key=command_feedback_key(ctx, "leave_done"))


@bot.command(help="🔂 Active/Désactive la boucle de la musique actuelle")
async def loop(ctx):
    player = get_player(ctx)
    player.loop = not player.loop
    await ctx.send(f"🔂 Loop: {'activé' if player.loop else 'désactivé'}")


@bot.command(help="🔁 Active/Désactive la boucle de la file d'attente")
async def loop_queue(ctx):
    player = get_player(ctx)
    player.loopqueue = not player.loopqueue
    if player.loopqueue:
        player.update_snapshot()
    else:
        player.original_queue = []
    await ctx.send(f"🔁 LoopQueue: {'activé' if player.loopqueue else 'désactivé'}")


@bot.command(help="🔀 Mélange la file d'attente")
async def shuffle(ctx):
    player = get_player(ctx)
    if not player.queue:
        return await ctx.send("📭 File vide.")
    random.shuffle(player.queue)
    if player.loopqueue:
        player.update_snapshot()
    await ctx.send("🔀 File mélangée !")


@bot.command(hidden=True)
async def penis(ctx):
    if not should_handle_command(ctx, "penis"):
        return
    asset_path = resolve_local_asset("penis.png")
    if not asset_path:
        return await send_unique_feedback(ctx, "⚠️ Image introuvable.", key=command_feedback_key(ctx, "penis_missing"))
    await ctx.send(file=discord.File(asset_path))


@bot.command(hidden=True)
async def XXXL(ctx):
    if not should_handle_command(ctx, "XXXL"):
        return
    asset_path = resolve_local_asset("XXXL.jpg")
    if not asset_path:
        return await send_unique_feedback(ctx, "⚠️ Image introuvable.", key=command_feedback_key(ctx, "xxxl_missing"))
    await ctx.send(file=discord.File(asset_path))


@bot.command(name="XXXXXL", hidden=True)
async def XXXXXL(ctx):
    if not should_handle_command(ctx, "XXXXXL"):
        return
    asset_path = resolve_local_asset("XXXXXL.jpg")
    if not asset_path:
        return await send_unique_feedback(ctx, "⚠️ Image introuvable.", key=command_feedback_key(ctx, "xxxxxl_missing"))
    await ctx.send(file=discord.File(asset_path))


@bot.command(help="❓ Affiche toutes les commandes disponibles")
async def help(ctx):
    embed = discord.Embed(
        title="🎵 Commandes du Bot Musique 🎵",
        description="Voici toutes les commandes disponibles :",
        color=0x5AC800,
    )

    ordered_commands = [
        ("`play`", "🔊 Joue une musique ou l'ajoute à la file d'attente"),
        ("`clear`", "🗑️ Vide la file d'attente"),
        ("`skip x`", "⏭️ Passe la musique en cours, ou x nombre"),
        ("`stop`", "⏹️ Stoppe la lecture et vide la file d'attente"),
        ("`previous`", "⏮️ Retourne à la musique précédente"),
        ("`pause`", "⏸️ Met la musique en pause"),
        ("`resume`", "▶️ Reprend la musique en pause"),
        ("`seek`", "⏩ Va à un timecode dans la musique en cours"),
        ("`queue`", "📜 Affiche la file d'attente"),
        ("`shuffle`", "🔀 Mélange la file d'attente"),
        ("`loop`", "🔂 Active/Désactive la boucle de la musique actuelle"),
        ("`loop_queue`", "🔁 Active/Désactive la boucle de la file d'attente"),
        ("`leave`", "⏹️ Stoppe la musique et déconnecte le bot"),
        ("`help`", "❓ Affiche toutes les commandes disponibles"),
    ]

    for name, description in ordered_commands:
        embed.add_field(name=f"{PREFIX}{name}", value=description, inline=False)

    embed.set_footer(text="🎧 Discordbot (music)")
    await ctx.send(embed=embed)


@bot.event
async def on_ready():
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.listening,
            name=f"{PREFIX}help",
        )
    )
    host_label = _instance_name or "unknown-host"
    print(f"\n✅ Connecté en tant que {bot.user} ({host_label}:{os.getpid()})\n")


instance_lock_handle = acquire_instance_lock()
bot.run(TOKEN)


