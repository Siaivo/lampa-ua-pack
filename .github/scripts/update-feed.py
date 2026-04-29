import os
import json
import hashlib
import asyncio
import aiohttp
from pathlib import Path
from typing import Optional, Dict, List, Any
from aiohttp import ClientTimeout, TCPConnector
from asyncio import Semaphore

# ------------------ Configuration ------------------
CUB_URL = os.environ.get("FEED_API_URL")
FEED_DIR = Path("feed")

# List of locales to process (add/remove as needed)
LOCALIZATIONS = os.environ.get("LOCALIZATIONS", "uk").split(",")
# Example: "uk,en,ru" -> ["uk", "en", "ru"]

TMDB_API_KEY = os.environ.get("TMDB_API_KEY")
if not TMDB_API_KEY:
    raise ValueError("TMDB_API_KEY environment variable not set")

TMDB_BASE_URL = "https://api.themoviedb.org/3"
REQUEST_DELAY = 0.05
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 1.0
TIMEOUT_SECONDS = 15
MAX_CONCURRENT_REQUESTS = 10
# --------------------------------------------------


def get_locale_paths(locale: str) -> tuple[Path, Path]:
    """Generate file paths for a given locale"""
    data_file = FEED_DIR / f"data.{locale}.json"
    hash_file = FEED_DIR / f"data.{locale}.hash"
    return data_file, hash_file


class TMDBClient:
    """Async client for TMDB with retry and rate limiting"""
    
    def __init__(self, session: aiohttp.ClientSession, semaphore: Semaphore, language: str):
        self.session = session
        self.semaphore = semaphore
        self.language = language
        self.stats = {"requests": 0, "errors": 0, "success": 0}
    
    async def fetch_with_retry(self, url: str, params: Dict) -> Optional[Dict]:
        for attempt in range(RETRY_ATTEMPTS):
            try:
                async with self.semaphore:
                    self.stats["requests"] += 1
                    async with self.session.get(
                        url, 
                        params=params, 
                        timeout=ClientTimeout(total=TIMEOUT_SECONDS)
                    ) as resp:
                        if resp.status == 200:
                            self.stats["success"] += 1
                            return await resp.json()
                        elif resp.status == 429:
                            wait = RETRY_BACKOFF * (2 ** attempt)
                            print(f"Rate limit, waiting {wait:.1f}s...")
                            await asyncio.sleep(wait)
                            continue
                        else:
                            print(f"TMDB {resp.status}: {url}")
                            return None
            except asyncio.TimeoutError:
                print(f"Timeout (attempt {attempt + 1}/{RETRY_ATTEMPTS})")
            except Exception as e:
                print(f"Request error: {e}")
            await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))
        
        self.stats["errors"] += 1
        return None
    
    async def get_data(self, tmdb_id: int, media_type: str) -> Optional[Dict]:
        url = f"{TMDB_BASE_URL}/{media_type}/{tmdb_id}"
        params = {"api_key": TMDB_API_KEY, "language": self.language}
        return await self.fetch_with_retry(url, params)
    
    async def get_episode(self, tmdb_id: int, season: int, episode: int) -> Optional[Dict]:
        url = f"{TMDB_BASE_URL}/tv/{tmdb_id}/season/{season}/episode/{episode}"
        params = {"api_key": TMDB_API_KEY, "language": self.language}
        return await self.fetch_with_retry(url, params)


def localize_data(data_obj: Dict, tmdb_info: Dict, media_type: str) -> None:
    """Localize data object in-place using TMDB response"""
    if media_type == "movie":
        if tmdb_info.get("title"):
            data_obj["title"] = tmdb_info["title"]
            if "name" in data_obj:
                data_obj["name"] = tmdb_info["title"]
        if tmdb_info.get("original_title"):
            data_obj["original_title"] = tmdb_info["original_title"]
    else:
        if tmdb_info.get("name"):
            data_obj["name"] = tmdb_info["name"]
        if tmdb_info.get("original_name"):
            data_obj["original_name"] = tmdb_info["original_name"]
    
    if tmdb_info.get("overview"):
        data_obj["overview"] = tmdb_info["overview"]
    
    if "names" in data_obj and isinstance(data_obj["names"], list):
        new_name = tmdb_info.get("name") or tmdb_info.get("title")
        if new_name and new_name not in data_obj["names"]:
            data_obj["names"].insert(0, new_name)
    
    if "genres" in tmdb_info:
        data_obj["genres"] = [g["name"] for g in tmdb_info["genres"] if "name" in g]
    
    if "production_countries" in tmdb_info:
        data_obj["countries"] = [c["name"] for c in tmdb_info["production_countries"] if "name" in c]
    
    for field in ["poster_path", "backdrop_path"]:
        if tmdb_info.get(field):
            data_obj[field] = tmdb_info[field]


async def localize_item_async(item: Dict, client: TMDBClient) -> Dict:
    """Async localization of a single item"""
    tmdb_id = item.get("card_id")
    if not tmdb_id:
        return item
    
    card_type = item.get("card_type")
    media_type = "tv" if card_type == "tv" else "movie"
    
    if "data" in item:
        tmdb_info = await client.get_data(tmdb_id, media_type)
        if tmdb_info:
            localize_data(item["data"], tmdb_info, media_type)
        
        if item.get("type") == "episode" and "episode" in item["data"]:
            ep = item["data"]["episode"]
            season, ep_num = ep.get("season_number"), ep.get("episode_number")
            if season and ep_num:
                ep_info = await client.get_episode(tmdb_id, season, ep_num)
                if ep_info:
                    for field in ["name", "overview", "still_path", "air_date"]:
                        if ep_info.get(field):
                            ep[field] = ep_info[field]
                    if ep_info.get("runtime"):
                        ep["runtime"] = ep_info["runtime"]
                    if ep_info.get("vote_average") is not None:
                        ep["vote_average"] = ep_info["vote_average"]
    
    return item


async def process_batch(items: list, client: TMDBClient, start_idx: int, total: int) -> list:
    """Process a batch of items in parallel"""
    tasks = [localize_item_async(item, client) for item in items]
    results = await asyncio.gather(*tasks)
    
    end_idx = min(start_idx + len(items), total)
    print(f"Progress: {end_idx}/{total} | Success: {client.stats['success']}, Errors: {client.stats['errors']}")
    return list(results)


def should_update(original_text: str, hash_file: Path) -> tuple[bool, str]:
    """Check if data has changed based on hash"""
    new_hash = hashlib.sha256(original_text.encode('utf-8')).hexdigest()
    old_hash = hash_file.read_text().strip() if hash_file.exists() else None
    return old_hash != new_hash, new_hash


async def process_locale(locale: str, original_data: Dict, original_text: str) -> bool:
    """Process a single locale and save results"""
    data_file, hash_file = get_locale_paths(locale)
    
    # Check if update is needed
    needs_update, new_hash = should_update(original_text, hash_file)
    if not needs_update and data_file.exists():
        print(f"[{locale}] Data unchanged - file already up to date")
        hash_file.write_text(new_hash)
        return True
    
    print(f"[{locale}] Changes detected - starting localization...")
    
    # Setup TMDB client for this locale
    semaphore = Semaphore(MAX_CONCURRENT_REQUESTS)
    
    async with aiohttp.ClientSession(
        connector=TCPConnector(limit=MAX_CONCURRENT_REQUESTS * 2, ttl_dns_cache=300),
        timeout=ClientTimeout(total=TIMEOUT_SECONDS)
    ) as tmdb_session:
        
        client = TMDBClient(tmdb_session, semaphore, locale)
        results = []
        items = original_data.get("result", [])
        total = len(items)
        batch_size = MAX_CONCURRENT_REQUESTS * 5
        
        for i in range(0, total, batch_size):
            batch = items[i:i + batch_size]
            batch_results = await process_batch(batch, client, i, total)
            results.extend(batch_results)
            
            if i + batch_size < total:
                await asyncio.sleep(REQUEST_DELAY * 2)
    
    # Save results
    uk_data = {**original_data, "result": results}
    with open(data_file, "w", encoding="utf-8") as f:
        json.dump(uk_data, f, ensure_ascii=False, indent=2)
    
    hash_file.write_text(new_hash)
    
    print(f"[{locale}] Done! Requests: {client.stats['requests']}, "
          f"Success: {client.stats['success']}, Errors: {client.stats['errors']}")
    return True


async def main():
    FEED_DIR.mkdir(exist_ok=True)
    
    # Fetch data from source
    print("Fetching from source...")
    async with aiohttp.ClientSession(
        connector=TCPConnector(limit=1, ttl_dns_cache=300),
        timeout=ClientTimeout(total=30)
    ) as cub_session:
        try:
            async with cub_session.get(CUB_URL) as resp:
                resp.raise_for_status()
                original_text = await resp.text()
                original_data = await resp.json()
        except Exception as e:
            print(f"Failed to fetch source: {e}")
            return
    
    print(f"Processing locales: {LOCALIZATIONS}")
    
    # Process each locale sequentially (to avoid overwhelming TMDB)
    for locale in LOCALIZATIONS:
        locale = locale.strip()
        if not locale:
            continue
        await process_locale(locale, original_data, original_text)
        # Small delay between locales
        if locale != LOCALIZATIONS[-1]:
            await asyncio.sleep(1)
    
    print("All locales processed successfully")


if __name__ == "__main__":
    asyncio.run(main())