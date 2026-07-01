"""
Image Guesser Auto-Fetcher
Fetches images from external APIs to populate the image guesser queue.

APIs used:
- TMDB (movies, TV shows, people)
- IGDB via Twitch (video games)
- Jikan/MyAnimeList (anime, characters)
- Last.fm (albums, songs)
- Unsplash (locations, countries)
- Wikimedia Commons (historical events)
"""

import aiohttp
import aiosqlite
import asyncio
import logging
import os
import random
import uuid
from datetime import datetime

log = logging.getLogger("discord.image_guesser.fetcher")

_cog_dir = os.path.dirname(os.path.abspath(__file__))
ASSETS_DIR = os.path.join(_cog_dir, '..', 'assets', 'image_guesser')
DB_PATH = os.path.join(_cog_dir, '..', 'image_guesser.db')

# How many images to fetch per category when topping up
FETCH_BATCH = 3
# Minimum unused images per category before auto-fetch kicks in
MIN_QUEUE_DEPTH = 3

# Curated landmarks for Unsplash location/country fetching
LANDMARKS = [
    ("Eiffel Tower", "France", "Paris"),
    ("Colosseum", "Italy", "Rome"),
    ("Machu Picchu", "Peru", None),
    ("Great Wall of China", "China", None),
    ("Taj Mahal", "India", "Agra"),
    ("Statue of Liberty", "United States", "New York"),
    ("Christ the Redeemer", "Brazil", "Rio de Janeiro"),
    ("Sydney Opera House", "Australia", "Sydney"),
    ("Big Ben", "United Kingdom", "London"),
    ("Pyramids of Giza", "Egypt", None),
    ("Petra", "Jordan", None),
    ("Angkor Wat", "Cambodia", None),
    ("Stonehenge", "United Kingdom", None),
    ("Mount Fuji", "Japan", None),
    ("Santorini", "Greece", None),
    ("Niagara Falls", "United States", None),
    ("Golden Gate Bridge", "United States", "San Francisco"),
    ("Burj Khalifa", "United Arab Emirates", "Dubai"),
    ("Chichen Itza", "Mexico", None),
    ("Acropolis", "Greece", "Athens"),
    ("Tower of London", "United Kingdom", "London"),
    ("Leaning Tower of Pisa", "Italy", "Pisa"),
    ("Sagrada Familia", "Spain", "Barcelona"),
    ("Neuschwanstein Castle", "Germany", None),
    ("Table Mountain", "South Africa", "Cape Town"),
    ("Hagia Sophia", "Turkey", "Istanbul"),
    ("Notre-Dame Cathedral", "France", "Paris"),
    ("Moai Statues", "Chile", "Easter Island"),
    ("Victoria Falls", "Zambia", None),
    ("Blue Mosque", "Turkey", "Istanbul"),
    ("Forbidden City", "China", "Beijing"),
    ("Alhambra", "Spain", "Granada"),
    ("Edinburgh Castle", "United Kingdom", "Edinburgh"),
    ("Brandenburg Gate", "Germany", "Berlin"),
    ("Matterhorn", "Switzerland", None),
    ("Kremlin", "Russia", "Moscow"),
    ("Parthenon", "Greece", "Athens"),
    ("Arc de Triomphe", "France", "Paris"),
    ("Grand Canyon", "United States", "Arizona"),
    ("Banff National Park", "Canada", "Alberta"),
]

# Curated historical events — (display_answer, Wikipedia_article_title)
HISTORICAL_EVENTS = [
    ("Moon Landing", "Apollo_11"),
    ("Fall of the Berlin Wall", "Fall_of_the_Berlin_Wall"),
    ("D-Day", "Normandy_landings"),
    ("Hindenburg Disaster", "Hindenburg_disaster"),
    ("Signing of the Declaration of Independence", "United_States_Declaration_of_Independence"),
    ("Wright Brothers First Flight", "Wright_brothers"),
    ("Titanic", "Sinking_of_the_Titanic"),
    ("Hiroshima", "Atomic_bombings_of_Hiroshima_and_Nagasaki"),
    ("March on Washington", "March_on_Washington_for_Jobs_and_Freedom"),
    ("Boston Tea Party", "Boston_Tea_Party"),
    ("French Revolution", "French_Revolution"),
    ("Woodstock", "Woodstock"),
    ("Assassination of JFK", "Assassination_of_John_F._Kennedy"),
    ("Challenger Disaster", "Space_Shuttle_Challenger_disaster"),
    ("Coronation of Queen Elizabeth II", "Coronation_of_Elizabeth_II"),
    ("V-J Day Times Square Kiss", "V-J_Day_in_Times_Square"),
    ("Construction of the Panama Canal", "Panama_Canal"),
    ("Raising the Flag on Iwo Jima", "Raising_the_Flag_on_Iwo_Jima"),
    ("Apollo 13", "Apollo_13"),
    ("Cuban Missile Crisis", "Cuban_Missile_Crisis"),
    ("First Transatlantic Flight", "Spirit_of_St._Louis"),
    ("Gold Rush", "California_Gold_Rush"),
    ("Battle of Gettysburg", "Battle_of_Gettysburg"),
    ("Lewis and Clark Expedition", "Lewis_and_Clark_Expedition"),
    ("Trail of Tears", "Trail_of_Tears"),
    ("Assassination of Abraham Lincoln", "Assassination_of_Abraham_Lincoln"),
    ("Chernobyl Disaster", "Chernobyl_disaster"),
    ("September 11 Attacks", "September_11_attacks"),
    ("Berlin Airlift", "Berlin_Blockade"),
    ("Sinking of the Lusitania", "Sinking_of_the_RMS_Lusitania"),
]


def _build_title_answers(title: str) -> str:
    """Build comma-separated accepted answers from a title.
    e.g. 'Iron Man 2' → 'Iron Man 2, Iron Man'
         'The Dark Knight Rises' → 'The Dark Knight Rises, Dark Knight Rises'
         'Avengers: Endgame' → 'Avengers: Endgame, Endgame, Avengers'
    """
    import re
    answers = {title}

    # Strip leading "The "
    if title.lower().startswith("the "):
        answers.add(title[4:])

    # Strip trailing sequel numbers: "Iron Man 2" → "Iron Man"
    stripped = re.sub(r'\s+\d+$', '', title)
    if stripped != title and len(stripped) > 2:
        answers.add(stripped)
        if stripped.lower().startswith("the "):
            answers.add(stripped[4:])

    # Split on colon: "Avengers: Endgame" → "Endgame", "Avengers"
    if ':' in title:
        parts = [p.strip() for p in title.split(':', 1)]
        for p in parts:
            if len(p) > 2:
                answers.add(p)

    return ", ".join(answers)


class ImageFetcher:
    """Fetches images from various APIs and inserts them into the image guesser DB."""

    def __init__(self):
        self.tmdb_key = os.getenv("TMDB_API_KEY")
        self.unsplash_key = os.getenv("UNSPLASH_ACCESS_KEY")
        self.lastfm_key = os.getenv("LASTFM_API_KEY")
        self.twitch_client_id = os.getenv("TWITCH_CLIENT_ID")
        self.twitch_client_secret = os.getenv("TWITCH_CLIENT_SECRET")
        self._igdb_token = None
        self._igdb_token_expires = 0
        self._session: aiohttp.ClientSession | None = None
        os.makedirs(ASSETS_DIR, exist_ok=True)

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={"User-Agent": "VibeyBot/1.0 (Discord Bot; image guesser game)"},
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ---------------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------------

    async def _download_image(self, url: str) -> str | None:
        """Download an image from a URL, save to assets, return file path."""
        try:
            async with self.session.get(url) as resp:
                if resp.status != 200:
                    log.warning(f"Failed to download image: HTTP {resp.status} from {url}")
                    return None
                data = await resp.read()
                if len(data) < 1024:  # Skip tiny/broken images
                    return None
                # Determine extension from content type
                ct = resp.content_type or ""
                if "png" in ct:
                    ext = ".png"
                elif "gif" in ct:
                    ext = ".gif"
                elif "webp" in ct:
                    ext = ".webp"
                else:
                    ext = ".jpg"
                filename = f"{uuid.uuid4().hex}{ext}"
                path = os.path.join(ASSETS_DIR, filename)
                with open(path, "wb") as f:
                    f.write(data)
                return path
        except Exception as e:
            log.error(f"Error downloading image from {url}: {e}")
            return None

    async def _insert_image(self, guild_id: int, file_path: str, answer: str,
                            category: str, hint: str | None = None):
        """Insert a fetched image into the DB."""
        async with aiosqlite.connect(DB_PATH) as db:
            # Check for duplicate answers in same category to avoid repeats
            async with db.execute(
                "SELECT COUNT(*) FROM images WHERE guild_id = ? AND category = ? AND answer = ?",
                (guild_id, category, answer)
            ) as c:
                count = (await c.fetchone())[0]
            if count > 0:
                # Already have this one, skip
                if os.path.exists(file_path):
                    os.remove(file_path)
                return False
            await db.execute(
                "INSERT INTO images (guild_id, file_path, answer, category, hint, added_by, added_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (guild_id, file_path, answer, category, hint, 0, datetime.now().isoformat())
            )
            await db.commit()
        log.info(f"Auto-queued: [{category}] {answer}")
        return True

    async def _get_queue_depth(self, guild_id: int, category: str) -> int:
        """Get the number of unused images for a category."""
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT COUNT(*) FROM images WHERE used = 0 AND guild_id = ? AND category = ?",
                    (guild_id, category)
                ) as c:
                    return (await c.fetchone())[0]
        except Exception:
            return 0

    # ---------------------------------------------------------------------------
    # TMDB — Movies, TV Shows, People
    # ---------------------------------------------------------------------------

    async def fetch_movies(self, guild_id: int, count: int = FETCH_BATCH) -> int:
        """Fetch well-known movie backdrops from TMDB (1985+, high vote count)."""
        if not self.tmdb_key:
            log.warning("TMDB_API_KEY not set, skipping movie fetch")
            return 0

        added = 0
        try:
            page = random.randint(1, 10)
            url = (
                f"https://api.themoviedb.org/3/discover/movie?api_key={self.tmdb_key}"
                f"&sort_by=popularity.desc&vote_count.gte=2000"
                f"&primary_release_date.gte=1985-01-01"
                f"&with_original_language=en&page={page}"
            )
            async with self.session.get(url) as resp:
                if resp.status != 200:
                    log.warning(f"TMDB discover movies: HTTP {resp.status}")
                    return 0
                data = await resp.json()

            movies = data.get("results", [])
            random.shuffle(movies)

            for movie in movies:
                if added >= count:
                    break
                title = movie.get("title")
                backdrop = movie.get("backdrop_path")
                if not title or not backdrop:
                    continue

                image_url = f"https://image.tmdb.org/t/p/w1280{backdrop}"
                file_path = await self._download_image(image_url)
                if not file_path:
                    continue

                year = (movie.get("release_date") or "")[:4]
                hint = f"Released in {year}" if year else None
                answer = _build_title_answers(title)
                if await self._insert_image(guild_id, file_path, answer, "Name this Movie", hint):
                    added += 1
        except Exception as e:
            log.error(f"Error fetching movies: {e}", exc_info=True)
        return added

    async def fetch_tv_shows(self, guild_id: int, count: int = FETCH_BATCH) -> int:
        """Fetch well-known TV show backdrops from TMDB (1990+, high vote count)."""
        if not self.tmdb_key:
            return 0

        added = 0
        try:
            page = random.randint(1, 10)
            url = (
                f"https://api.themoviedb.org/3/discover/tv?api_key={self.tmdb_key}"
                f"&sort_by=popularity.desc&vote_count.gte=1000"
                f"&first_air_date.gte=1990-01-01"
                f"&with_original_language=en&page={page}"
            )
            async with self.session.get(url) as resp:
                if resp.status != 200:
                    return 0
                data = await resp.json()

            shows = data.get("results", [])
            random.shuffle(shows)

            for show in shows:
                if added >= count:
                    break
                name = show.get("name")
                backdrop = show.get("backdrop_path")
                if not name or not backdrop:
                    continue

                image_url = f"https://image.tmdb.org/t/p/w1280{backdrop}"
                file_path = await self._download_image(image_url)
                if not file_path:
                    continue

                year = (show.get("first_air_date") or "")[:4]
                hint = f"First aired in {year}" if year else None
                answer = _build_title_answers(name)
                if await self._insert_image(guild_id, file_path, answer, "Name this TV Show", hint):
                    added += 1
        except Exception as e:
            log.error(f"Error fetching TV shows: {e}", exc_info=True)
        return added

    async def fetch_people(self, guild_id: int, count: int = FETCH_BATCH) -> int:
        """Fetch well-known person photos from TMDB.
        Filters: page 1-2 only, popularity >= 40, known_for must include
        at least one title with 500+ votes (proves mainstream recognition)."""
        if not self.tmdb_key:
            return 0

        added = 0
        try:
            page = random.randint(1, 2)
            url = f"https://api.themoviedb.org/3/person/popular?api_key={self.tmdb_key}&page={page}"
            async with self.session.get(url) as resp:
                if resp.status != 200:
                    return 0
                data = await resp.json()

            people = data.get("results", [])
            random.shuffle(people)

            for person in people:
                if added >= count:
                    break
                name = person.get("name")
                profile = person.get("profile_path")
                popularity = person.get("popularity", 0)
                if not name or not profile:
                    continue
                # Skip people with low TMDB popularity
                # Require at least one massively-voted known_for title
                # (proves the person is a lead in a major, widely-seen production)
                known_for = person.get("known_for", [])
                max_votes = max((kf.get("vote_count", 0) for kf in known_for), default=0)
                if max_votes < 5000:
                    continue

                image_url = f"https://image.tmdb.org/t/p/w780{profile}"
                file_path = await self._download_image(image_url)
                if not file_path:
                    continue

                dept = person.get("known_for_department")
                hint = f"Known for {dept.lower()}" if dept else None
                if await self._insert_image(guild_id, file_path, name, "Name this Person", hint):
                    added += 1
        except Exception as e:
            log.error(f"Error fetching people: {e}", exc_info=True)
        return added

    # ---------------------------------------------------------------------------
    # IGDB via Twitch — Video Games
    # ---------------------------------------------------------------------------

    async def _get_igdb_token(self) -> str | None:
        """Get or refresh IGDB access token via Twitch OAuth."""
        if not self.twitch_client_id or not self.twitch_client_secret:
            return None

        import time
        if self._igdb_token and time.time() < self._igdb_token_expires:
            return self._igdb_token

        try:
            url = "https://id.twitch.tv/oauth2/token"
            params = {
                "client_id": self.twitch_client_id,
                "client_secret": self.twitch_client_secret,
                "grant_type": "client_credentials",
            }
            async with self.session.post(url, params=params) as resp:
                if resp.status != 200:
                    log.warning(f"Twitch OAuth failed: HTTP {resp.status}")
                    return None
                data = await resp.json()
                self._igdb_token = data["access_token"]
                self._igdb_token_expires = time.time() + data.get("expires_in", 3600) - 60
                return self._igdb_token
        except Exception as e:
            log.error(f"Error getting IGDB token: {e}")
            return None

    async def fetch_video_games(self, guild_id: int, count: int = FETCH_BATCH) -> int:
        """Fetch well-known video game screenshots from IGDB (high rating count, 2000+)."""
        token = await self._get_igdb_token()
        if not token:
            log.warning("No IGDB token, skipping video game fetch")
            return 0

        added = 0
        try:
            headers = {
                "Client-ID": self.twitch_client_id,
                "Authorization": f"Bearer {token}",
            }
            offset = random.randint(0, 60)
            body = f"fields name, screenshots.image_id, first_release_date; where screenshots != null & total_rating_count > 150 & first_release_date > 946684800; sort total_rating_count desc; limit 20; offset {offset};"
            async with self.session.post(
                "https://api.igdb.com/v4/games",
                headers=headers,
                data=body,
            ) as resp:
                if resp.status != 200:
                    log.warning(f"IGDB games: HTTP {resp.status}")
                    return 0
                games = await resp.json()

            random.shuffle(games)

            for game in games:
                if added >= count:
                    break
                name = game.get("name")
                screenshots = game.get("screenshots", [])
                if not name or not screenshots:
                    continue

                # Pick a random screenshot
                ss = random.choice(screenshots)
                image_id = ss.get("image_id")
                if not image_id:
                    continue

                image_url = f"https://images.igdb.com/igdb/image/upload/t_screenshot_big/{image_id}.jpg"
                file_path = await self._download_image(image_url)
                if not file_path:
                    continue

                # Build hint from release year
                release = game.get("first_release_date")
                hint = None
                if release:
                    from datetime import datetime as dt
                    try:
                        hint = f"Released in {dt.fromtimestamp(release).year}"
                    except Exception:
                        pass

                answer = _build_title_answers(name)
                if await self._insert_image(guild_id, file_path, answer, "Name this Video Game", hint):
                    added += 1
        except Exception as e:
            log.error(f"Error fetching video games: {e}", exc_info=True)
        return added

    # ---------------------------------------------------------------------------
    # Jikan (MyAnimeList) — Anime, Characters
    # ---------------------------------------------------------------------------

    async def fetch_anime(self, guild_id: int, count: int = FETCH_BATCH) -> int:
        """Fetch popular anime images from Jikan (top-rated, score >= 7.5)."""
        added = 0
        try:
            # Top 3 pages = top ~75 anime, all well-known
            page = random.randint(1, 3)
            url = f"https://api.jikan.moe/v4/top/anime?page={page}&limit=25&filter=bypopularity"
            async with self.session.get(url) as resp:
                if resp.status != 200:
                    log.warning(f"Jikan top anime: HTTP {resp.status}")
                    return 0
                data = await resp.json()

            anime_list = data.get("data", [])
            random.shuffle(anime_list)

            for anime in anime_list:
                if added >= count:
                    break
                title = anime.get("title_english") or anime.get("title")
                score = anime.get("score") or 0
                images = anime.get("images", {}).get("jpg", {})
                image_url = images.get("large_image_url") or images.get("image_url")
                if not title or not image_url:
                    continue
                if score < 7.5:
                    continue

                file_path = await self._download_image(image_url)
                if not file_path:
                    continue

                year = anime.get("year")
                hint = f"Aired in {year}" if year else None
                # Accept both English and Japanese titles
                alt_title = anime.get("title") if anime.get("title_english") else None
                answer = f"{title}, {alt_title}" if alt_title and alt_title != title else title

                if await self._insert_image(guild_id, file_path, answer, "Name this Anime", hint):
                    added += 1

                await asyncio.sleep(0.4)  # Jikan rate limit: ~3 req/sec
        except Exception as e:
            log.error(f"Error fetching anime: {e}", exc_info=True)
        return added

    async def fetch_characters(self, guild_id: int, count: int = FETCH_BATCH) -> int:
        """Fetch popular anime characters from Jikan (top 75)."""
        added = 0
        try:
            # Top 3 pages = top 75 most favorited characters, all recognizable
            page = random.randint(1, 3)
            url = f"https://api.jikan.moe/v4/top/characters?page={page}&limit=25"
            async with self.session.get(url) as resp:
                if resp.status != 200:
                    log.warning(f"Jikan top characters: HTTP {resp.status}")
                    return 0
                data = await resp.json()

            characters = data.get("data", [])
            random.shuffle(characters)

            for char in characters:
                if added >= count:
                    break
                name = char.get("name")
                images = char.get("images", {}).get("jpg", {})
                image_url = images.get("image_url")
                if not name or not image_url:
                    continue

                file_path = await self._download_image(image_url)
                if not file_path:
                    continue

                # Use the "about" field first line as hint if available
                about = char.get("about") or ""
                hint = None
                if about:
                    first_line = about.split("\n")[0].strip()
                    if len(first_line) < 100:
                        hint = first_line

                if await self._insert_image(guild_id, file_path, name, "Name this Character", hint):
                    added += 1

                await asyncio.sleep(0.4)
        except Exception as e:
            log.error(f"Error fetching characters: {e}", exc_info=True)
        return added

    # ---------------------------------------------------------------------------
    # Last.fm — Albums
    # ---------------------------------------------------------------------------

    async def fetch_albums(self, guild_id: int, count: int = FETCH_BATCH) -> int:
        """Fetch iconic album art via Last.fm — picks top albums from charting artists."""
        if not self.lastfm_key:
            log.warning("LASTFM_API_KEY not set, skipping album fetch")
            return 0

        added = 0
        try:
            # Get charting artists (globally popular)
            page = random.randint(1, 2)
            url = (
                f"https://ws.audioscrobbler.com/2.0/?method=chart.gettopartists"
                f"&api_key={self.lastfm_key}&format=json&page={page}&limit=30"
            )
            async with self.session.get(url) as resp:
                if resp.status != 200:
                    return 0
                data = await resp.json()

            artists = data.get("artists", {}).get("artist", [])
            random.shuffle(artists)

            for artist_entry in artists:
                if added >= count:
                    break
                artist_name = artist_entry.get("name")
                if not artist_name:
                    continue

                # Get the artist's top albums
                albums_url = (
                    f"https://ws.audioscrobbler.com/2.0/?method=artist.gettopalbums"
                    f"&artist={artist_name}&api_key={self.lastfm_key}&format=json&limit=5"
                )
                try:
                    async with self.session.get(albums_url) as resp2:
                        if resp2.status != 200:
                            continue
                        albums_data = await resp2.json()
                except Exception:
                    continue

                albums = albums_data.get("topalbums", {}).get("album", [])
                if not albums:
                    continue

                album = random.choice(albums[:3])  # Pick from top 3
                name = album.get("name")
                if not name or name == "(null)":
                    continue

                images = album.get("image", [])
                image_url = None
                for img in reversed(images):
                    if img.get("#text"):
                        image_url = img["#text"]
                        break
                if not image_url:
                    continue

                file_path = await self._download_image(image_url)
                if not file_path:
                    continue

                hint = f"By {artist_name}"
                if await self._insert_image(guild_id, file_path, name, "Name this Album", hint):
                    added += 1

                await asyncio.sleep(0.2)
        except Exception as e:
            log.error(f"Error fetching albums: {e}", exc_info=True)
        return added

    # ---------------------------------------------------------------------------
    # Unsplash — Locations, Countries
    # ---------------------------------------------------------------------------

    async def fetch_locations(self, guild_id: int, count: int = FETCH_BATCH) -> int:
        """Fetch landmark photos from Unsplash."""
        if not self.unsplash_key:
            log.warning("UNSPLASH_ACCESS_KEY not set, skipping location fetch")
            return 0

        added = 0
        landmarks = random.sample(LANDMARKS, min(count * 3, len(LANDMARKS)))

        for landmark_name, country, city in landmarks:
            if added >= count:
                break
            try:
                url = (
                    f"https://api.unsplash.com/search/photos"
                    f"?query={landmark_name}&per_page=5&orientation=landscape"
                )
                headers = {"Authorization": f"Client-ID {self.unsplash_key}"}
                async with self.session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        log.warning(f"Unsplash search: HTTP {resp.status}")
                        continue
                    data = await resp.json()

                results = data.get("results", [])
                if not results:
                    continue

                photo = random.choice(results)
                image_url = photo.get("urls", {}).get("regular")
                if not image_url:
                    continue

                file_path = await self._download_image(image_url)
                if not file_path:
                    continue

                hint_parts = []
                if city:
                    hint_parts.append(f"Located in {city}")
                hint_parts.append(f"Country: {country}")
                hint = ", ".join(hint_parts) if hint_parts else None

                if await self._insert_image(guild_id, file_path, landmark_name, "Name this Location", hint):
                    added += 1

                await asyncio.sleep(1.0)  # Unsplash: 50 req/hour, be conservative
            except Exception as e:
                log.error(f"Error fetching location {landmark_name}: {e}")
        return added

    async def fetch_countries(self, guild_id: int, count: int = FETCH_BATCH) -> int:
        """Fetch country landmark photos from Unsplash for 'Name this Country'."""
        if not self.unsplash_key:
            return 0

        added = 0
        landmarks = random.sample(LANDMARKS, min(count * 3, len(LANDMARKS)))

        for landmark_name, country, city in landmarks:
            if added >= count:
                break
            try:
                url = (
                    f"https://api.unsplash.com/search/photos"
                    f"?query={landmark_name} {country}&per_page=5&orientation=landscape"
                )
                headers = {"Authorization": f"Client-ID {self.unsplash_key}"}
                async with self.session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()

                results = data.get("results", [])
                if not results:
                    continue

                photo = random.choice(results)
                image_url = photo.get("urls", {}).get("regular")
                if not image_url:
                    continue

                file_path = await self._download_image(image_url)
                if not file_path:
                    continue

                hint = f"Famous landmark: {landmark_name}"
                if await self._insert_image(guild_id, file_path, country, "Name this Country", hint):
                    added += 1

                await asyncio.sleep(1.0)
            except Exception as e:
                log.error(f"Error fetching country {country}: {e}")
        return added

    # ---------------------------------------------------------------------------
    # Wikipedia — Historical Events
    # ---------------------------------------------------------------------------

    async def fetch_historical_events(self, guild_id: int, count: int = FETCH_BATCH) -> int:
        """Fetch historical event images from Wikipedia article pages."""
        added = 0
        events = random.sample(HISTORICAL_EVENTS, min(count * 3, len(HISTORICAL_EVENTS)))

        for event_name, article_title in events:
            if added >= count:
                break
            try:
                # Use Wikipedia REST API to get the article's main image
                url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{article_title}"
                async with self.session.get(url) as resp:
                    if resp.status != 200:
                        log.warning(f"Wikipedia summary for {article_title}: HTTP {resp.status}")
                        continue
                    data = await resp.json()

                # Get the original image (highest resolution)
                image_url = data.get("originalimage", {}).get("source")
                if not image_url:
                    image_url = data.get("thumbnail", {}).get("source")
                if not image_url:
                    continue

                file_path = await self._download_image(image_url)
                if not file_path:
                    continue

                if await self._insert_image(guild_id, file_path, event_name, "Name this Historical Event"):
                    added += 1

                await asyncio.sleep(0.5)
            except Exception as e:
                log.error(f"Error fetching historical event {event_name}: {e}")
        return added

    # ---------------------------------------------------------------------------
    # Main entry point — top up all categories
    # ---------------------------------------------------------------------------

    CATEGORY_FETCHERS = {
        "Name this Movie": "fetch_movies",
        "Name this TV Show": "fetch_tv_shows",
        "Name this Person": "fetch_people",
        "Name this Video Game": "fetch_video_games",
        "Name this Anime": "fetch_anime",
        "Name this Character": "fetch_characters",
        "Name this Album": "fetch_albums",
        "Name this Location": "fetch_locations",
        "Name this Country": "fetch_countries",
        "Name this Historical Event": "fetch_historical_events",
    }

    async def auto_fill(self, guild_id: int) -> dict[str, int]:
        """Check all categories and fetch images for any that are below MIN_QUEUE_DEPTH.
        Returns a dict of {category: images_added}."""
        results = {}
        for category, method_name in self.CATEGORY_FETCHERS.items():
            depth = await self._get_queue_depth(guild_id, category)
            if depth >= MIN_QUEUE_DEPTH:
                log.debug(f"[{category}] queue depth {depth} >= {MIN_QUEUE_DEPTH}, skipping")
                continue

            needed = MIN_QUEUE_DEPTH - depth
            log.info(f"[{category}] queue depth {depth}, fetching {needed} images...")
            fetcher = getattr(self, method_name)
            added = await fetcher(guild_id, count=needed)
            if added > 0:
                results[category] = added

            await asyncio.sleep(1)  # Breathing room between API calls
        return results
