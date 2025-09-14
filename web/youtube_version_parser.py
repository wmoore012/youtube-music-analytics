# SPDX-License-Identifier: GPL-3.0-or-later
"""
YouTube Version Parser

This module provides functions to extract artist names and version information
from YouTube video titles and channel names. It helps identify the artists and
versions in YouTube videos, which can be used to link them to the appropriate
songs and artists in the database.

*New in 2025-07-10:* If the **RapidFuzz** package is installed, the parser automatically uses its C-accelerated fuzzy-matching routines for faster and more accurate similarity checks.

*New in 2025-07-14:* Improved title parsing algorithm to better handle:
1. Titles containing "with" as part of the title (e.g., "Sleep With The Light On")
2. Multiple artists in titles (e.g., "Rapper Big Pooh & Nottz - Preach")
3. Various formats of featuring indicators (e.g., "ft.", "feat.", "featuring")
4. Better artist name identification using channel information

*New in 2025-07-15:* Further improvements to the title parsing algorithm:
1. Added support for possessive forms (e.g., "Ryan Destiny's song The Same")
2. Added support for titles with "with the label" phrases (e.g., "Ezri's song apostles with the label mass appeal")
3. Better handling of artist names in complex title formats

The parser now uses a more selective approach to identify featuring indicators and
avoids splitting titles at common words like "with" when they're part of the title.
It also uses a more comprehensive regex pattern for featuring clauses and better
handles cases where what we think is the artist part might actually be part of the title.

The parser now also recognizes possessive forms like "Artist's song Title" and correctly
extracts the artist name and song title. It also handles titles with "with the label" phrases
and correctly identifies the artist name in these cases.
"""
import csv
import io
import re
import unicodedata
from typing import Dict, List, Optional, Set, Tuple

try:
    import unidecode

    UNIDECODE_AVAILABLE = True
except ImportError:
    UNIDECODE_AVAILABLE = False

# --- optional high‑performance fuzzy matching -----------------------------
try:
    from rapidfuzz import fuzz, process

    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False

# Common version types that might appear in YouTube titles
VERSION_TYPES = [
    "Official Video",
    "Official Music Video",
    "Official Audio",
    "Audio",
    "Lyric Video",
    "Lyrics",
    "Visualizer",
    "Official Visualizer",
    "Live",
    "Live Performance",
    "Acoustic",
    "Acoustic Version",
    "Remix",
    "Extended",
    "Extended Mix",
    "Radio Edit",
    "Clean",
    "Explicit",
    "Instrumental",
    "Karaoke",
    "Cover",
    "Demo",
    "Remastered",
    "Remaster",
    "HQ",
    "HD",
    "4K",
    "8K",
    "360°",
    "Slowed",
    "Reverb",
    "Slowed + Reverb",
    "Sped Up",
    "Nightcore",
    "Mashup",
    "Medley",
    "Snippet",
    "Preview",
    "Teaser",
    "Trailer",
    "Behind The Scenes",
    "Making Of",
    "Interview",
    "Q&A",
    "Commentary",
    "Reaction",
    "Review",
    "Analysis",
    "Breakdown",
    "Explained",
    "Tutorial",
    "How To",
    "Guide",
    "Walkthrough",
    "Playthrough",
    "Gameplay",
    "Speedrun",
    "Montage",
    "Compilation",
    "Best Of",
    "Highlights",
    "Top 10",
    "Top 5",
    "Countdown",
    "Ranking",
    "Tier List",
    "Tier Ranking",
    "Tier Maker",
    "Tier Builder",
    "Tier Creator",
    "Tier Designer",
    "Tier Crafter",
    "Tier Forge",
    # Additional performance types
    "On The Radar Performance",
    "COLORS Performance",
    "VEVO DSCVR",
    "NPR Tiny Desk Concert",
    "BBC Radio 1 Live Lounge",
    "A COLORS SHOW",
    "Genius Verified",
    "Vevo Ctrl",
    "Vevo LIFT",
    "Vevo X",
    "VEVO Live Performance",
    "Live Session",
    "Studio Performance",
    "Acoustic Session",
    "Unplugged",
    "In Studio",
    "In Session",
    "Live at",
    "Recorded Live",
    "Live from",
    "Performance Video",
]

# Patterns to detect and remove meaningless descriptors
MEANINGLESS_DESCRIPTORS = [
    r"\(ASOHH Standout Track\)",
    r"\(ASOHH\s+[^)]*\)",
    r"\(\d{1,2}\.\d{1,2}\.\d{2,4}\)",  # Date patterns like (7.7.24)
    r"\(\d{4}\)",  # Year patterns like (2024)
    r"\(HD\)",
    r"\(HQ\)",
    r"\(4K\)",
    r"\(8K\)",
    r"\(Explicit\)",
    r"\(Clean\)",
    r"\([0-9]+[Kk]\)",  # Resolution like (4K)
    r"\(.*?[Rr]epost.*?\)",
    r"\(.*?[Pp]remiere.*?\)",
    r"\(.*?[Ee]xclusive.*?\)",
    r"\(.*?[Ss]tandout.*?\)",  # Standout track patterns
    r"\[.*?[Ss]tandout.*?\]",
    r"\(.*?[Hh]igh.*?[Qq]uality.*?\)",
]

# Known ripper/unofficial channel patterns - UPDATED with validation results + broadcaster detection
RIPPER_CHANNEL_PATTERNS = [
    r".*[Ll]yrics?.*",
    r"Cardinal Music",  # Confirmed ripper (user's Side B example)
    r"Old For This",  # Confirmed ripper (user's Side A Freestyle example)
    r"Bleakk TV",  # Validated as ripper
    r"MaxxMusic",  # Validated as ripper
    r"FUSION MUSIC",  # Validated as ripper
    r"ALPHA MUSIC",  # Validated as ripper
    r"Joann Media",  # Validated as ripper
    r"Baby Demon Lyrics.*",  # Validated as ripper
    r"DepthofSoundTV",  # Validated as ripper
    # Broadcaster patterns (radio stations, media companies)
    r"SiriusXM",
    r"iHeartRadio",
    r"BBC Radio.*",
    r"NPR.*",
    r"Hot 97",
    r"Power 105",
    r".*Radio.*Station.*",
    r".*FM$",
    r".*AM$",
    r".*Broadcasting.*",
    # Generic patterns
    r".*TV$",
    r".*Beats$",
    r".*Sounds$",
    r".*Audio$",
    r".*Media$",
    r".*Entertainment$",
    r".*[Rr]ecords?$",
    r".*[Cc]hannel$",
    r".*[Vv]ideo[sz]?$",
    r".*[Pp]roductions?$",
    r".*[Ss]tudio[sz]?$",
    r".*[Ll]abel[sz]?$",
    r".*[Dd]istribution?$",
    r".*[Pp]ublishing$",
    r".*[Hh]ub$",
    r".*[Nn]etwork$",
    r".*[Cc]ontent$",
]

# Legitimate artist channels that should NOT be flagged as rippers
LEGITIMATE_ARTIST_CHANNELS = [
    r"Emanny Music",  # Validated as Emanny's official channel
    r".*VEVO$",  # All VEVO channels are legitimate
    r".*Official$",  # Official channels are legitimate
]

# Common featuring artist indicators
FEATURING_INDICATORS = ["feat", "feat.", "featuring", "ft", "ft.", "w/", "x", "&"]

# Words that might be confused with featuring indicators but are actually part of titles
TITLE_WORDS = ["with", "the", "and", "by", "in", "on", "at", "of", "for"]

# Common separators in titles
SEPARATORS = ["-", "–", "—", "|", ":", "//", "///"]


def _norm(s: str) -> str:
    """
    Normalize text by folding diacritics, straightening quotes, and collapsing whitespace.

    This is a more robust normalization function that handles various text issues
    that can cause parsing problems.

    Args:
        s (str): The text to normalize

    Returns:
        str: The normalized text
    """
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    if UNIDECODE_AVAILABLE:
        s = unidecode.unidecode(s)  # fold é → e
    s = re.sub(r"[''´`]", "'", s)  # straighten quotes
    s = re.sub(r"\s+", " ", s).strip()
    return s


def clean_text(text: str) -> str:
    """
    Clean text by removing extra whitespace, converting to lowercase, etc.

    Args:
        text (str): The text to clean

    Returns:
        str: The cleaned text
    """
    if not text:
        return ""

    # Normalize the text first
    text = _norm(text)

    # Remove common YouTube-specific suffixes
    text = re.sub(r"\s*\(\s*Official\s+Video\s*\)\s*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*\[\s*Official\s+Video\s*\]\s*$", "", text, flags=re.IGNORECASE)

    return text


def extract_version_from_title(title: str, channel_name: str = None) -> Tuple[str, Optional[str]]:
    """
    Extract version information from a YouTube title.

    Args:
        title (str): The YouTube video title
        channel_name (str, optional): The YouTube channel name

    Returns:
        Tuple[str, Optional[str]]: The cleaned title and the version type (if found)
    """
    if not title:
        return "", None

    # Clean the title
    title = clean_text(title)

    # First, try to extract version from parentheses or brackets
    for pattern in [r"\(\s*(.*?)\s*\)", r"\[\s*(.*?)\s*\]"]:
        matches = re.findall(pattern, title)
        for match in matches:
            # Check if the match contains a version indicator
            version_type = extract_version_type(match, channel_name)
            if version_type and version_type != "Audio":  # Skip default "Audio" type
                # Remove the version type from the title
                title = re.sub(
                    pattern.replace("(.*?)", re.escape(match)),
                    "",
                    title,
                    flags=re.IGNORECASE,
                )
                return clean_text(title), version_type

    # Check for version types after a separator
    for separator in SEPARATORS:
        if separator in title:
            parts = title.split(separator, 1)
            if len(parts) == 2:
                version_type = extract_version_type(parts[1], channel_name)
                if version_type and version_type != "Audio":  # Skip default "Audio" type
                    return clean_text(parts[0]), version_type

    # If no specific version type is found in the title structure,
    # use the extract_version_type function on the whole title
    version_type = extract_version_type(title, channel_name)

    # Return the title and version type (which might be "Audio" as default)
    return title, version_type


def extract_artists_from_title(title: str, channel_name: str = None) -> Tuple[List[str], str]:
    """
    Extract artist names from a YouTube title and channel name.

    Args:
        title (str): The YouTube video title
        channel_name (str, optional): The YouTube channel name

    Returns:
        Tuple[List[str], str]: A list of artist names and the cleaned title
    """
    if not title:
        return [], ""

    # Clean the title
    title = clean_text(title)

    # Special case for "On The Radar Performance" and similar formats
    radar_match = re.search(r"([^|]+)\s*\|\s*On The Radar Performance", title, re.IGNORECASE)
    if radar_match:
        artist_and_title = radar_match.group(1).strip()
        # Check if the title is in quotes
        quote_match = re.search(r'([^"\']+)\s*["\']([^"\']+)["\']', artist_and_title)
        if quote_match:
            artist = quote_match.group(1).strip()
            song_title = quote_match.group(2).strip()
            return [artist], song_title
        else:
            # Try to split by common patterns
            for separator in SEPARATORS:
                if separator in artist_and_title:
                    parts = artist_and_title.split(separator, 1)
                    if len(parts) == 2:
                        artist = parts[0].strip()
                        song_title = parts[1].strip()
                        return [artist], song_title
            # If no separator found, assume the whole thing is the artist
            return [artist_and_title], "On The Radar Performance"

    # Special case for other performance formats
    performance_match = re.search(r"([^|]+)\s*\|\s*(.+?Performance)", title, re.IGNORECASE)
    if performance_match:
        artist_and_title = performance_match.group(1).strip()
        performance_type = performance_match.group(2).strip()
        # Check if the title is in quotes
        quote_match = re.search(r'([^"\']+)\s*["\']([^"\']+)["\']', artist_and_title)
        if quote_match:
            artist = quote_match.group(1).strip()
            song_title = quote_match.group(2).strip()
            return [artist], song_title
        else:
            # Try to split by common patterns
            for separator in SEPARATORS:
                if separator in artist_and_title:
                    parts = artist_and_title.split(separator, 1)
                    if len(parts) == 2:
                        artist = parts[0].strip()
                        song_title = parts[1].strip()
                        return [artist], song_title
            # If no separator found, assume the whole thing is the artist
            return [artist_and_title], performance_type

    # Extract version information
    title, _ = extract_version_from_title(title, channel_name)

    # Try to extract artists from the title
    artists = []

    # Check for featuring artists using a more robust pattern
    FEAT_RE = re.compile(
        r"(?P<prefix>.*?)(?:\(|\[)?\b(?:feat\.?|ft\.?|featuring)\b\s+(?P<rest>[^)\]]+)",
        re.I,
    )

    def split_feat(title_text: str) -> tuple[str, list[str]]:
        """Split a title into main part and featured artists."""
        m = FEAT_RE.search(title_text)
        if not m:
            return title_text, []
        main, rest = m.group("prefix"), m.group("rest")
        featured = re.split(r",\s*|\s+&\s+|\s+and\s+", rest)
        return main.strip(" -"), [a.strip() for a in featured if a.strip()]

    # Apply the feat splitting
    main_part, featured_artists = split_feat(title)
    if featured_artists:
        artists.extend(featured_artists)
        title = main_part

    # Check for artists before a separator
    for separator in SEPARATORS:
        if separator in title:
            parts = title.split(separator, 1)
            if len(parts) == 2:
                # The first part might be the artist
                potential_artist = parts[0].strip()
                # Check if it's not just a single word (likely not an artist)
                if " " in potential_artist or (channel_name and potential_artist.lower() in channel_name.lower()):
                    artists.append(potential_artist)
                    title = parts[1].strip()
                    break

    # If no artists found from the title, try the channel name
    if not artists and channel_name:
        # Remove common channel suffixes
        channel = re.sub(
            r"VEVO$|Official$|Music$|Records$|Recordings$",
            "",
            channel_name,
            flags=re.IGNORECASE,
        ).strip()
        if channel:
            artists.append(channel)

    # Remove duplicates while preserving order
    seen = set()
    artists = [artist for artist in artists if not (artist.lower() in seen or seen.add(artist.lower()))]

    return artists, title


def split_if_csv(text: str) -> tuple[str | None, str | None, str]:
    """
    Split a text string if it's in CSV format, handling embedded commas properly.

    Args:
        text (str): The text to check and potentially split

    Returns:
        tuple[str | None, str | None, str]: A tuple of (video_id, isrc, title) or (None, None, original_text)
    """
    try:
        vid, isrc, title = next(csv.reader([text]))
        if re.fullmatch(r"[A-Za-z0-9_-]{11}", vid) and len(isrc) == 12:
            return vid, isrc, title
    except Exception:
        pass
    return None, None, text


_RX_BRACKETS = re.compile(r"\s*[\[\(].*?(official|audio|video|hq|mv|lyric).*?[\]\)]", re.I)
# More comprehensive regex for featuring clauses
# Handles variations like "ft.", "feat.", "featuring", "ft", etc.
# Also handles cases where the featuring clause is in parentheses or brackets
_RX_FEAT_CLAUSE = re.compile(r"(?:\(|\[|\s+)(?:ft\.?|feat\.?|featuring|ft|feat)\s+([^)\]]+)(?:\)|\])?", re.I)
# Only match "with" when it's in parentheses or brackets, or preceded by a dash
# This helps avoid splitting titles like "Sleep With The Light On"
_RX_WITH_CLAUSE = re.compile(r"(?:[\(\[]|\s-\s)\s*with\s+([^)\]]+)", re.I)
# Pattern to detect possessive forms like "Ryan Destiny's song"
_RX_POSSESSIVE = re.compile(
    r"^([A-Za-z0-9\s&.']+)'s\s+(?:song|track|tune|single|record|release|video)\s+(.+)$",
    re.I,
)
# Pattern to detect "with the label" phrases
_RX_LABEL = re.compile(r"(.+?)\s+with\s+the\s+label\s+(.+)$", re.I)

_DELIMITERS = re.compile(r"\s*(?:,|&| and | x |/)\s*", re.I)


def _split_primary_block(block: str) -> List[str]:
    """Enhanced multi-artist detection with better handling of complex artist combinations."""
    if not block or not block.strip():
        return []

    # Remove common prefixes that can interfere with artist detection
    cleaned_block = block.strip()

    # Handle special cases where artists are separated by more complex patterns
    # Example: "Lute featuring Blakk Soul & Ari Lennox" should split into ["Lute", "Blakk Soul", "Ari Lennox"]

    # First check for featuring patterns and handle them specially
    featuring_match = re.search(r"(.+?)\s+(?:feat\.?|featuring|ft\.?)\s+(.+)", cleaned_block, re.IGNORECASE)
    if featuring_match:
        primary_artist = featuring_match.group(1).strip()
        featured_artists = featuring_match.group(2).strip()

        # Split the primary artist if it contains delimiters
        primary_parts = [p.strip() for p in _DELIMITERS.split(primary_artist) if p.strip()]

        # Split the featured artists
        featured_parts = [p.strip() for p in _DELIMITERS.split(featured_artists) if p.strip()]

        return primary_parts + featured_parts

    # Standard delimiter splitting
    parts = [p.strip() for p in _DELIMITERS.split(cleaned_block) if p.strip()]

    # Filter out obvious non-artist parts
    filtered_parts = []
    for part in parts:
        # Skip parts that are clearly not artist names
        if (
            len(part) > 50  # Too long to be an artist name
            or part.lower() in ["official", "music", "video", "hd", "hq", "audio", "lyrics", "vevo"]
            or re.match(r"^\d+$", part)
        ):  # Just numbers
            continue
        filtered_parts.append(part)

    return filtered_parts if filtered_parts else parts


def _is_ripper_channel(channel_name: str) -> bool:
    """
    Check if a channel name matches patterns of ripper/unofficial channels.
    Updated with validation results to exclude legitimate artist channels.
    """
    if not channel_name:
        return False

    # First check if it's a legitimate artist channel
    for pattern in LEGITIMATE_ARTIST_CHANNELS:
        if re.match(pattern, channel_name, re.IGNORECASE):
            return False

    # Then check ripper patterns
    for pattern in RIPPER_CHANNEL_PATTERNS:
        if re.match(pattern, channel_name, re.IGNORECASE):
            return True
    return False


def _remove_meaningless_descriptors(title: str) -> str:
    """
    Remove meaningless descriptors from the title that don't represent actual version types.
    """
    cleaned_title = title

    for pattern in MEANINGLESS_DESCRIPTORS:
        cleaned_title = re.sub(pattern, "", cleaned_title, flags=re.IGNORECASE)

    # Clean up extra spaces and empty parentheses/brackets
    cleaned_title = re.sub(r"\s+", " ", cleaned_title).strip()
    cleaned_title = re.sub(r"\(\s*\)", "", cleaned_title)  # Empty parentheses
    cleaned_title = re.sub(r"\[\s*\]", "", cleaned_title)  # Empty brackets

    return cleaned_title


def _extract_artist_from_title_start(title: str, channel_name: str = None) -> tuple[List[str], str]:
    """
    Try to extract artist names from the beginning of a title, even if no separator is present.
    This handles cases like "JID, Lute Ma Boy Lyrics" where the artists are at the start.
    """
    # First try the more specific pattern for "Artist1, Artist2 Title"
    comma_pattern = (
        r'^([A-Za-z0-9\s&.\']{1,15}),\s+([A-Za-z0-9\s&.\']{1,15})\s+([A-Za-z0-9\s\'"]{3,})(?:\s+[Ll]yrics?)?$'
    )
    comma_match = re.match(comma_pattern, title)

    if comma_match:
        artist1 = comma_match.group(1).strip()
        artist2 = comma_match.group(2).strip()
        song_title = comma_match.group(3).strip()

        # Make sure these look like artist names (not too short, reasonable length)
        if (
            len(artist1) > 1
            and len(artist2) > 1
            and len(artist1.split()) <= 3
            and len(artist2.split()) <= 3
            and len(song_title.split()) >= 1
        ):
            return [artist1, artist2], song_title

    # Then try the more general pattern for multiple artists separated by various delimiters
    artist_pattern = r'^([A-Za-z0-9\s&.,\']+?)(\s+)([A-Z][A-Za-z0-9\s\'"]+?)(?:\s+[Ll]yrics?)?$'
    match = re.match(artist_pattern, title)

    if match:
        potential_artists_str = match.group(1).strip()
        potential_title = match.group(3).strip()

        # Check if this looks like a list of artists
        if "," in potential_artists_str or " and " in potential_artists_str.lower():
            artists = _split_primary_block(potential_artists_str)
            # Filter out single letters or very short "names" that might be noise
            artists = [artist for artist in artists if len(artist) > 1]

            # Additional validation: the title part should be a reasonable song title
            if artists and len(potential_title.split()) >= 1:
                return artists, potential_title

    return [], title


def _detect_multi_song_performance(title: str) -> Dict:
    """
    Detect if a title represents multiple songs performed together (medley/setlist).
    Returns information about detected songs for special handling.
    """
    result = {
        "is_multi_song": False,
        "songs": [],
        "artist": None,
        "performance_type": None,
    }

    # Pattern for quoted song lists: Artist "Song1, Song2 & Song3"
    quoted_pattern = r'(\w+(?:\s+\w+)*)\s*["""]([^"""]+)["""]'
    quoted_match = re.search(quoted_pattern, title)

    if quoted_match:
        artist = quoted_match.group(1).strip()
        songs_text = quoted_match.group(2).strip()

        # Split songs by common separators
        song_separators = re.compile(r"\s*(?:,|&|\sand\s)\s*", re.IGNORECASE)
        songs = [song.strip() for song in song_separators.split(songs_text) if song.strip()]

        if len(songs) > 1:  # Multiple songs detected
            result["is_multi_song"] = True
            result["songs"] = songs
            result["artist"] = artist

            # Detect performance type
            if re.search(r"\blive\b", title, re.IGNORECASE):
                result["performance_type"] = "Live Performance"
            elif re.search(r"\bmedley\b", title, re.IGNORECASE):
                result["performance_type"] = "Medley"
            else:
                result["performance_type"] = "Multi-Song Performance"

    return result


def parse_youtube_title(video_title: str, channel_title: str) -> Dict[str, List[str] | str]:
    """
    Return dict{'title': str, 'primary': [..], 'featured':[..]}.
    """
    raw_title = video_title

    # 0️⃣ remove extraneous bracketed tags early (keeps feat./with clauses)
    cleaned = _RX_BRACKETS.sub("", raw_title).strip()

    # 0.1️⃣ Remove meaningless descriptors
    cleaned = _remove_meaningless_descriptors(cleaned)

    # 0.2️⃣ Check for multi-song performances (e.g., Lute "Eye to Eye, 100 & GED")
    multi_song_info = _detect_multi_song_performance(cleaned)
    if multi_song_info["is_multi_song"]:
        # Handle as primary song + featured songs with special version
        primary_song = multi_song_info["songs"][0]  # First song as primary
        featured_songs = multi_song_info["songs"][1:]  # Rest as "featured"

        # Create a composite title indicating it's a medley
        if len(featured_songs) > 0:
            title_part = f"{primary_song} (with {', '.join(featured_songs)})"
        else:
            title_part = primary_song

        return {
            "title": title_part,
            "primary": [multi_song_info["artist"]] if multi_song_info["artist"] else [],
            "featured": [],
            "version_type": multi_song_info["performance_type"],
        }

    # 0.3️⃣ Check for live performance broadcasts (e.g., "Lute — GED | LIVE Performance | SiriusXM")
    if channel_title and _is_ripper_channel(channel_title):
        # Pattern: "Artist — Song | LIVE Performance | Broadcaster"
        live_pattern = r"^([A-Za-z\s&.\']+)\s*[—-]\s*([^|]+)(?:\s*\|\s*LIVE\s*Performance)?(?:\s*\|\s*(.+))?$"
        live_match = re.match(live_pattern, cleaned)

        if live_match:
            artist = live_match.group(1).strip()
            song_part = live_match.group(2).strip()
            broadcaster = live_match.group(3).strip() if live_match.group(3) else channel_title

            # Clean up the song part
            song_part = re.sub(r"\s*\|\s*LIVE\s*Performance.*$", "", song_part, flags=re.IGNORECASE)

            return {
                "title": song_part.strip(),
                "primary": [artist],
                "featured": [],
                "version_type": "Live Performance",
                "broadcaster": broadcaster,
            }

    # 0.5️⃣ Check for possessive forms like "Ryan Destiny's song The Same"
    possessive_match = _RX_POSSESSIVE.match(cleaned)
    if possessive_match:
        artist_name, song_title = possessive_match.groups()
        primary_artists = _split_primary_block(artist_name)
        title_part = song_title

        # Check if the title part contains "with the label"
        label_match = _RX_LABEL.match(title_part)
        if label_match:
            # If it does, extract just the title part
            title_part, _ = label_match.groups()

        # Extract featuring artists if present
        featured = []

        def _extract(rx, tgt):
            m = rx.search(tgt)
            if not m:
                return tgt
            names = _split_primary_block(m.group(1))
            featured.extend(names)
            return tgt[: m.start()].strip()

        title_part = _extract(_RX_FEAT_CLAUSE, title_part)
        title_part = _extract(_RX_WITH_CLAUSE, title_part)

        # Return early with the parsed information
        return {
            "title": title_part.strip(" \"'"),
            "primary": list(dict.fromkeys(primary_artists)),
            "featured": list(dict.fromkeys(featured)),
        }

    # 0.6️⃣ Check for "with the label" phrases
    label_match = _RX_LABEL.match(cleaned)
    if label_match:
        song_info, label_name = label_match.groups()

        # Check if song_info contains artist information
        if "'" in song_info and "song" in song_info.lower():
            # This is likely a possessive form like "Ezri's song apostles"
            artist_song_match = re.match(
                r"([A-Za-z0-9\s&.']+)'s\s+(?:song|track|tune|single)\s+(.+)$",
                song_info,
                re.I,
            )
            if artist_song_match:
                artist_name, song_title = artist_song_match.groups()
                primary_artists = _split_primary_block(artist_name)

                # Return early with the parsed information - just use the song title
                return {
                    "title": song_title.strip(" \"'"),
                    "primary": list(dict.fromkeys(primary_artists)),
                    "featured": [],
                }

        # If no artist information found but we have a "with the label" phrase,
        # treat the song_info as the title and ignore the label part
        return {"title": song_info.strip(" \"'"), "primary": [], "featured": []}

    # 0.7️⃣ Try to extract artists from title start (handles "JID, Lute Ma Boy Lyrics")
    extracted_artists, remaining_title = _extract_artist_from_title_start(cleaned, channel_title)
    if extracted_artists:
        # Found artists at the start, use them and continue with remaining title
        cleaned = remaining_title
        primary_artists = extracted_artists
        title_part = cleaned

        # Extract featuring artists if present
        featured = []

        def _extract(rx, tgt):
            m = rx.search(tgt)
            if not m:
                return tgt
            names = _split_primary_block(m.group(1))
            featured.extend(names)
            return tgt[: m.start()].strip()

        title_part = _extract(_RX_FEAT_CLAUSE, title_part)
        title_part = _extract(_RX_WITH_CLAUSE, title_part)

        # Return early with extracted artists
        return {
            "title": title_part.strip(" \"'"),
            "primary": list(dict.fromkeys(primary_artists)),
            "featured": list(dict.fromkeys(featured)),
        }

    # 0.8️⃣ Handle quoted titles like 'LUTE "GED (Gettin Every Dolla)" (7.7.24)'
    quoted_pattern = r'^([A-Za-z0-9\s&.\']+?)\s*["\']([^"\']+)["\']'
    quoted_match = re.match(quoted_pattern, cleaned)
    if quoted_match:
        potential_artist = quoted_match.group(1).strip()
        quoted_title = quoted_match.group(2).strip()

        # If the potential artist looks like an artist name, use it
        if len(potential_artist.split()) <= 3 and not any(
            word.lower() in potential_artist.lower() for word in ["official", "music", "channel", "video"]
        ):
            primary_artists = [potential_artist]
            title_part = quoted_title

            # Extract featuring artists if present
            featured = []

            def _extract(rx, tgt):
                m = rx.search(tgt)
                if not m:
                    return tgt
                names = _split_primary_block(m.group(1))
                featured.extend(names)
                return tgt[: m.start()].strip()

            title_part = _extract(_RX_FEAT_CLAUSE, title_part)
            title_part = _extract(_RX_WITH_CLAUSE, title_part)

            return {
                "title": title_part.strip(" \"'"),
                "primary": list(dict.fromkeys(primary_artists)),
                "featured": list(dict.fromkeys(featured)),
            }

    # 1️⃣ detect Topic channel ⇒ channel artist is authoritative
    topic_artist = None
    if channel_title and channel_title.lower().endswith(" - topic"):
        topic_artist = channel_title[:-7].strip()

    # 2️⃣ split on first ' - '  (most common "Artist – Title" pattern)
    if " - " in cleaned:
        artist_part, title_part = map(str.strip, cleaned.split(" - ", 1))

        # 2.1️⃣ Check if the artist part is a known ripper channel
        if _is_ripper_channel(artist_part):
            # Don't use ripper channels as artists, treat the whole thing as a title
            title_part = cleaned
            primary_artists = []
        else:
            # Check if what we think is the artist part might actually be part of the title
            # This helps with cases like "RIVER - We'll Be Together (feat. Lute)"
            if any(word.lower() in artist_part.lower() for word in TITLE_WORDS):
                # If the artist part contains common title words, check if it's a single word
                # Single words are more likely to be artist names than title fragments
                if len(artist_part.split()) <= 2 and not any(
                    indicator in artist_part.lower() for indicator in FEATURING_INDICATORS
                ):
                    # Likely an artist name despite containing title words
                    primary_artists = _split_primary_block(artist_part)
                else:
                    # Check if the channel name contains part of what we think is the artist
                    # If so, it's more likely to be an artist name
                    if channel_title and any(part.lower() in channel_title.lower() for part in artist_part.split()):
                        primary_artists = _split_primary_block(artist_part)
                    else:
                        # Likely a title fragment, not an artist name
                        title_part = cleaned
                        primary_artists = []
            else:
                # Normal case - artist part doesn't contain title words
                primary_artists = _split_primary_block(artist_part)
    else:
        # fallback: treat whole string as title; artist comes from channel or later feat./with
        title_part, primary_artists = cleaned, []

    # 3️⃣ pull out feat. / with clauses from *title_part*
    featured = []

    def _extract(rx, tgt):
        m = rx.search(tgt)
        if not m:
            return tgt
        names = _split_primary_block(m.group(1))
        featured.extend(names)
        return tgt[: m.start()].strip()

    title_part = _extract(_RX_FEAT_CLAUSE, title_part)
    title_part = _extract(_RX_WITH_CLAUSE, title_part)

    # 4️⃣ if Topic channel provided artist, make it *the* primary artist
    if topic_artist:
        primary_artists = [topic_artist]

    # 4.5️⃣ If no primary artists found but channel name looks like an artist, use it
    # BUT only if it's not a known ripper channel
    if not primary_artists and channel_title and not _is_ripper_channel(channel_title):
        # Remove common channel suffixes
        channel = re.sub(
            r"VEVO$|Official$|Music$|Records$|Recordings$",
            "",
            channel_title,
            flags=re.IGNORECASE,
        ).strip()
        # Check if the channel name is likely an artist name (not too long, no common words)
        if (
            channel
            and len(channel.split()) <= 3
            and not any(
                word.lower() in channel.lower()
                for word in [
                    "official",
                    "music",
                    "records",
                    "recordings",
                    "channel",
                    "vevo",
                ]
            )
        ):
            primary_artists = [channel]

    # 5️⃣ final title cleanup – single spaces, trim quotes
    title_part = re.sub(r"\s{2,}", " ", title_part).strip(" \"'")

    return {
        "title": title_part,
        "primary": list(dict.fromkeys(primary_artists)),  # de-dupe order-preserved
        "featured": list(dict.fromkeys(featured)),
    }


def is_lyric_video(title: str, description: str = None) -> bool:
    """
    Determine if a video is a lyric video based on its title and description.

    Args:
        title (str): The video title
        description (str, optional): The video description

    Returns:
        bool: True if it's a lyric video, False otherwise
    """
    if not title:
        return False

    # Check title for lyric indicators
    lyric_indicators = ["lyric", "lyrics", "with lyrics", "official lyrics"]
    for indicator in lyric_indicators:
        if indicator.lower() in title.lower():
            return True

    # Check description if provided
    if description:
        for indicator in lyric_indicators:
            if indicator.lower() in description.lower():
                return True

    return False


def is_official_video(title: str, channel_name: str = None) -> bool:
    """
    Determine if a video is an official music video based on its title and channel.

    Args:
        title (str): The video title
        channel_name (str, optional): The channel name

    Returns:
        bool: True if it's an official video, False otherwise
    """
    if not title:
        return False

    # Check title for official video indicators
    official_indicators = [
        "official video",
        "official music video",
        "official mv",
        "official m/v",
    ]
    for indicator in official_indicators:
        if indicator.lower() in title.lower():
            return True

    # Check if the channel name contains the artist name and has "VEVO" or "Official"
    if channel_name:
        if "vevo" in channel_name.lower() or "official" in channel_name.lower():
            return True

    return False


def extract_artists_and_title_from_youtube(video_data: Dict[str, any]) -> Dict[str, any]:
    """
    Extract artists and title information from YouTube video data.

    Args:
        video_data (Dict[str, any]): The YouTube video data including title, channel, etc.

    Returns:
        Dict[str, any]: Enhanced video data with extracted artists and title
    """
    # Get the video title and channel name
    title = video_data.get("title", "")
    channel_name = video_data.get("channel_name", "")
    description = video_data.get("description", "")

    # Parse the YouTube title
    parsed = parse_youtube_title(title, channel_name)

    # Determine version type
    version_type = extract_version_type(title, channel_name)

    # Update the video data with the parsed information
    video_data.update(
        {
            "parsed_title": parsed["title"],
            "parsed_artists": ", ".join(parsed["primary"] + parsed["featured"]),
            "primary_artists": parsed["primary"],
            "featured_artists": parsed["featured"],
            "version_type": version_type,
        }
    )

    return video_data


def match_youtube_to_song(video_data: Dict[str, any], songs: List[Dict[str, any]]) -> Optional[Dict[str, any]]:
    """
    Match a YouTube video to a song in the database.

    Args:
        video_data (Dict[str, any]): The YouTube video data
        songs (List[Dict[str, any]]): List of songs from the database

    Returns:
        Optional[Dict[str, any]]: The matched song or None if no match is found
    """
    # Extract ISRC from the video data if available
    isrc = video_data.get("isrc")
    if isrc:
        # Try to find a direct match by ISRC
        for song in songs:
            if song.get("ISRC") == isrc:
                return song

    # If no ISRC match, try to match by title and artists
    parsed_title = video_data.get("parsed_title", "").lower()
    parsed_artists = [artist.lower() for artist in video_data.get("parsed_artists", [])]

    if not parsed_title:
        return None

    # Calculate match scores for each song
    matches = []
    for song in songs:
        song_title = song.get("song_title", "").lower()

        # Skip if the titles don't match at all
        if not song_title or parsed_title not in song_title and song_title not in parsed_title:
            continue

        # Calculate title similarity (0-100)
        title_similarity = calculate_similarity(parsed_title, song_title)

        # Calculate artist similarity if we have artist information
        artist_similarity = 0
        if parsed_artists and "artists" in song:
            song_artists = [artist.lower() for artist in song.get("artists", [])]
            artist_similarity = calculate_artist_similarity(parsed_artists, song_artists)

        # Calculate overall match score (weighted average)
        match_score = (title_similarity * 0.7) + (artist_similarity * 0.3)

        matches.append((song, match_score))

    # Sort matches by score (descending)
    matches.sort(key=lambda x: x[1], reverse=True)

    # Return the best match if it's above a threshold
    if matches and matches[0][1] >= 50:  # Threshold of 50%
        return matches[0][0]

    return None


def calculate_similarity(str1: str, str2: str) -> float:
    """
    Calculate the similarity between two strings (0‑100).

    Prefers RapidFuzz’s token‑set ratio (fast C‑extension) when available;
    falls back to the previous pure‑Python Levenshtein implementation if the
    package is not installed.

    The similarity score algorithm works as follows:
    1. If RapidFuzz is available, it uses token_set_ratio which:
       - Tokenizes both strings (splits into words)
       - Creates three sets: intersection, str1-only, str2-only
       - Sorts and joins each set
       - Computes the Levenshtein ratio between the resulting strings
       - This is tolerant to word order and duplicates
       - Returns a score from 0-100

    2. If RapidFuzz is not available, it falls back to:
       - Computing the Levenshtein distance between the strings
       - Normalizing by the length of the longer string
       - Converting to a similarity score (0-100)

    This approach handles cases where titles might have slight variations
    or different word orders but still refer to the same song.

    Args:
        str1 (str): First string
        str2 (str): Second string

    Returns:
        float: Similarity score (0‑100)
    """
    # Normalise inputs early
    str1 = (str1 or "").lower().strip()
    str2 = (str2 or "").lower().strip()

    # Fast‑path identical strings
    if str1 == str2:
        return 100.0
    if not str1 or not str2:
        return 0.0

    if RAPIDFUZZ_AVAILABLE:
        # token_set_ratio is tolerant to word order / duplicates
        return float(fuzz.token_set_ratio(str1, str2))
    # ──────────────────────────────────────────────────────────────
    # Fallback → previous algorithm (unchanged logic below)
    len1, len2 = len(str1), len(str2)
    matrix = [[0 for _ in range(len2 + 1)] for _ in range(len1 + 1)]
    for i in range(len1 + 1):
        matrix[i][0] = i
    for j in range(len2 + 1):
        matrix[0][j] = j
    for i in range(1, len1 + 1):
        for j in range(1, len2 + 1):
            cost = 0 if str1[i - 1] == str2[j - 1] else 1
            matrix[i][j] = min(
                matrix[i - 1][j] + 1,
                matrix[i][j - 1] + 1,
                matrix[i - 1][j - 1] + cost,
            )
    distance = matrix[len1][len2]
    return (1 - (distance / max(len1, len2))) * 100.0


def calculate_artist_similarity(artists1: List[str], artists2: List[str]) -> float:
    """
    Calculate the similarity between two artist lists (0‑100).

    Uses RapidFuzz for robust matching when available.

    The artist similarity algorithm works as follows:
    1. If RapidFuzz is available:
       - For each artist in the first list, find the best matching artist in the second list
       - Calculate the token_set_ratio between each pair of artists
       - Take the maximum similarity score across all pairs
       - This handles cases where artist names might be slightly different or in different orders

    2. If RapidFuzz is not available:
       - For each artist in the first list, check if there's a matching artist in the second list
       - A match is found if the similarity score between the artists is >= 80
       - Calculate the percentage of artists that have matches
       - This approach is less accurate but still provides a reasonable fallback

    This approach handles cases where artist names might be spelled differently or have
    different formats (e.g., "J. Cole" vs "J Cole" or "Jay-Z" vs "JAY Z").
    """
    if not artists1 or not artists2:
        return 0.0

    if RAPIDFUZZ_AVAILABLE:
        # Compute max pair‑wise token_set_ratio across lists
        best = 0.0
        for a1 in artists1:
            score = max(fuzz.token_set_ratio(a1.lower(), a2.lower()) for a2 in artists2)
            if score > best:
                best = score
        return float(best)

    # Fallback – keep previous element‑wise logic
    max_matches = max(len(artists1), len(artists2))
    matches = 0
    for artist1 in artists1:
        for artist2 in artists2:
            if calculate_similarity(artist1, artist2) >= 80:
                matches += 1
                break
    return (matches / max_matches) * 100.0


def extract_version_type(title: str, channel_name: str = None) -> Optional[str]:
    """
    Extract the version type from a YouTube title and channel name.
    Enhanced with unicode slowed/reverb detection.

    Args:
        title (str): The YouTube video title
        channel_name (str, optional): The YouTube channel name

    Returns:
        Optional[str]: The version type or None if not found
    """
    # First check for unicode slowed/reverb patterns (Chopped & Screwed)
    unicode_patterns = [
        r"[𝕊-𝟿]+.*[𝕊-𝟿]+",  # Mathematical script characters
        r"[ℂ-ℝ]+.*[ℂ-ℝ]+",  # Double-struck characters
        r"[𝒜-𝓏]+.*[𝒜-𝓏]+",  # Script characters
    ]

    for pattern in unicode_patterns:
        if re.search(pattern, title):
            return "Chopped and Screwed"

    # Define version type patterns with their corresponding labels
    VERSION_MAP = {
        r"\bofficial (music )?video\b": "Official Music Video",
        r"\bofficial audio\b": "Official Audio",
        r"\blyric(s)? video\b": "Lyric Video",
        r"\b(acoustic|unplugged)\b": "Acoustic",
        r"\blive( at| from)?\b": "Live Performance",
        r"\b(chopped.*screwed|slowed.*reverb)\b": "Chopped and Screwed",
        r"\b(remix|mashup)\b": "Remix",
        r"\bon the radar performance\b": "On The Radar Performance",
        r"\bcolors (show|performance)\b": "COLORS Performance",
        r"\bvevo dscvr\b": "VEVO DSCVR",
        r"\bperformance video\b": "Performance Video",
        r"\blive session\b": "Live Session",
    }

    # Normalize the text
    t = _norm(title.lower())

    # Check each pattern
    for pattern, label in VERSION_MAP.items():
        if re.search(pattern, t, re.IGNORECASE):
            return label

    # Check channel name for Topic channels (usually official audio)
    if channel_name and "- topic" in channel_name.lower():
        return "Official Audio"

    # Default to Audio if no specific version type is found
    return "Audio"


if __name__ == "__main__":
    # Test the functions with some example YouTube titles
    test_titles = [
        "Lute - Eye to Eye ft. Cozz [Official Video]",
        "RIVER - We'll Be Together (feat. Lute)",
        "Cantrell, Stro, 070 Phi, Liana Bank$ - When Morning Comes [HQ Audio]",
        "Ryan Destiny - Do You (Quarantine Video)",
        "Lute - Amen featuring Little Brother (Official Audio)",
        "Lute - Flossin' featuring WESTSIDE BOOGIE (Official Audio)",
        "Rapper Big Pooh - LS400",
        "Rapper Big Pooh & Nottz - Preach",
        "Emanny - Don't Give Up (Audio)",
        "Rapper Big Pooh - Dreaming In Color ft. J.Smash of The Nukez",
        "Rapper Big Pooh - Changing Again",
        "Black Magic (feat. Akilz Amari)",
        "Lute - Outta Sight (Official Audio)",
        "Spice Girls - Stop (Official Music Video)",
        "Cantrell, Ezri, 070 Phi - 6 Rings (Official Video)",
        "Lute - Changes ft. BJ The Chicago Kid [Official Video]",
        "Type Of Day",
        "Rapper Big Pooh - Gold Chain",
        "Rapper Big Pooh - Thoughts & Prayers (Interlude)",
        "Rapper Big Pooh - In Surround Sound ft. Tre'mar",
        # Additional test cases for special formats
        "Miss Kaniyah 'Sassy' | On The Radar Performance",
        'BravoTheBagChaser - "Juggin" Official Video Shot By @StopSignPros',
        "Edward Sharpe & The Magnetic Zeros - Home (Official Video)",
        'SE4URxm5Wjc,QZLL92532644,"Miss Kaniyah "Sassy" | On The Radar Performance"',
    ]

    test_channels = [
        "LuteVEVO",
        "R&B Nation",
        "Mass Appeal Records",
        "RYAN DESTINY",
        "LuteVEVO",
        "LuteVEVO",
        "rapperbigpoohVEVO",
        "Tha Realness",
        "Emanny Music",
        "rapperbigpoohVEVO",
        "rapperbigpoohVEVO",
        "Steve Roxx - Topic",
        "LuteVEVO",
        "SpiceGirlsVEVO",
        "Mass Appeal",
        "LuteVEVO",
        "B.J. The Chicago Kid - Topic",
        "rapperbigpoohVEVO",
        "rapperbigpoohVEVO",
        "rapperbigpoohVEVO",
        # Channels for additional test cases
        "On The Radar",
        "Bravo The Bagchaser",
        "Rough Trade Records",
        "On The Radar",
    ]

    for i, title in enumerate(test_titles):
        channel = test_channels[i] if i < len(test_channels) else None
        parsed = parse_youtube_title(title, channel)
        print(f"Title: {title}")
        print(f"Channel: {channel}")
        print(f"Primary Artists: {parsed['primary']}")
        print(f"Featured Artists: {parsed['featured']}")
        print(f"Cleaned Title: {parsed['title']}")
        # Version type is not returned by parse_youtube_title, need to get it separately
        version_type = extract_version_type(title, channel)
        print(f"Version Type: {version_type}")
        print("---")
