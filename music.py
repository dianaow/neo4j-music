import asyncio
import json
import logging
import os
import random
import sqlite3
import time
import functools
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import aiohttp
import aiometer
import backoff
import musicbrainzngs
import psutil
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
import re

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logging.getLogger('musicbrainzngs').setLevel(logging.WARNING) 
logger = logging.getLogger(__name__)

@dataclass
class RateLimiter:
    """Rate limiter for API requests"""
    def __init__(self, requests_per_second: float):
        self.delay = 1.0 / requests_per_second
        self.last_request_time = 0.0
        self.lock = asyncio.Lock()

    async def acquire(self):
        """Wait until we can make another request"""
        async with self.lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.delay:
                await asyncio.sleep(self.delay - time_since_last)
            self.last_request_time = time.time()


class MusicBatchProcessor:
    # Include configurations - optimized to request only what's needed
    ARTIST_INCLUDES = {
        "basic": [
            "annotation", "aliases", "tags", "user-tags", "url-rels",
        ],
        "relationships": [
            "area-rels", "artist-rels", "label-rels", "place-rels", 
            "event-rels", "recording-rels", "release-rels", 
            "release-group-rels", "series-rels", "work-rels", 
            "instrument-rels"
        ]
    }

    def __init__(self, app_name: str, version: str, contact: str, batch_size: int = 100, 
                db_path: str = "musicbrainz.db", max_cache_size: int = 10000,
                max_concurrent_requests: int = 5):
        """Initialize the processor with MusicBrainz credentials and configuration"""
        musicbrainzngs.set_useragent(app_name, version, contact)
        
        if os.environ.get('MUSICBRAINZ_USER') and os.environ.get('MUSICBRAINZ_PASSWORD'):
            musicbrainzngs.auth(os.environ.get('MUSICBRAINZ_USER'), os.environ.get('MUSICBRAINZ_PASSWORD'))
        
        # API rate limiting
        self.rate_limiter = RateLimiter(requests_per_second=5)
        self.max_concurrent_requests = max_concurrent_requests
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        
        # Batch and cache configuration
        self.batch_size = self._optimize_batch_size() if batch_size is None else batch_size
        self.max_cache_size = max_cache_size
        
        # Database configuration
        self.db_path = db_path
        self.progress_file = "extraction_progress.json"
        
        # Initialize LRU caches with size limits
        self.artist_cache = {}  # Will implement LRU behavior
        self.release_cache = {}
        self.recording_cache = {}
        self.work_cache = {}  # Add work cache
        self.cache_access_times = {}  # Track access times for LRU eviction
        
        # Initialize batch storage with locks
        self.recording_batch = []
        self.contributor_batch = []
        self.artist_batch = []
        self.release_batch = []
        self.work_batch = []
        self.batch_lock = asyncio.Lock()
        
        # Progress tracking
        self.batch_progress = {
            "last_artist_offset": 0,
            "processed_artists": set(),
            "processed_releases": set(),
            "total_time_seconds": 0,
            "current_artist_id": None,
            "current_artist_name": None
        }
        
        # Initialize database connection
        self._db_initialized = False

    async def initialize(self):
        """Initialize the database and load progress"""
        if not self._db_initialized:
            await self._init_db()
            self.load_progress()
            self._db_initialized = True

    def close(self):
        """Clean up resources and close connections"""
        try:
            # Save any remaining progress
            self.save_progress()
            
            # Clear caches
            self.artist_cache.clear()
            self.release_cache.clear()
            self.recording_cache.clear()
            self.work_cache.clear()
            self.cache_access_times.clear()
            
            # Clear batches
            self.recording_batch.clear()
            self.contributor_batch.clear()
            self.artist_batch.clear()
            self.release_batch.clear()
            self.work_batch.clear()
            
            # Clear progress tracking
            self.batch_progress.clear()
            
            logger.info("Successfully closed MusicBatchProcessor")
        except Exception as e:
            logger.error(f"Error during close: {e}")
            raise

    def _optimize_batch_size(self) -> int:
        """Optimize batch size based on available system memory"""
        memory = psutil.virtual_memory()
        available_mb = memory.available / (1024 * 1024)  # Convert to MB
        
        # Use 20% of available memory for batch processing
        optimal_batch_size = int((available_mb * 0.2) / 0.5)  # Assuming 0.5MB per item
        
        # Ensure batch size is within reasonable limits
        return max(50, min(optimal_batch_size, 200))

    @asynccontextmanager
    async def _db_connection(self):
        """Context manager for database connections with retry logic"""
        max_retries = 3
        retry_delay = 1  # seconds
        
        for attempt in range(max_retries):
            try:
                conn = sqlite3.connect(self.db_path, timeout=30)
                conn.execute('PRAGMA journal_mode=WAL')  # Enable WAL mode for better concurrent access
                conn.execute('PRAGMA synchronous=NORMAL')
                conn.execute('PRAGMA cache_size=-2000000')  # Use 2GB cache
                conn.execute('PRAGMA temp_store=MEMORY')
                conn.execute('PRAGMA mmap_size=30000000000')  # 30GB memory map
                conn.execute('PRAGMA busy_timeout=60000')  # 60 second timeout
                yield conn
                conn.commit()  # Commit changes if no exceptions were raised
                return
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e):
                    if attempt < max_retries - 1:
                        logger.warning(f"Database is locked, retrying in {retry_delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        logger.error("Failed to get database connection after multiple attempts")
                        raise
                else:
                    raise
            except Exception as e:
                logger.error(f"Error getting database connection: {e}")
                raise
            finally:
                try:
                    if 'conn' in locals() and conn:
                        conn.close()
                except Exception:
                    pass

    async def _init_db(self):
        """Initialize SQLite database for persistent storage"""
        try:
            async with self._db_connection() as conn:
                cursor = conn.cursor()
                
                # Create tables if they don't exist
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS artists (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    type TEXT,
                    disambiguation TEXT,
                    gender TEXT,
                    country TEXT,
                    area TEXT,
                    begin_area TEXT,
                    dob TEXT,
                    urls TEXT,
                    aliases TEXT,
                    processed_at TEXT
                )
                ''')
                
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS recordings (
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    length INTEGER,
                    tags TEXT,
                    credits TEXT,
                    release_id TEXT,
                    artist_id TEXT,
                    work_ids TEXT,
                    urls TEXT,
                    processed_at TEXT
                )
                ''')
                
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS releases (
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    labels TEXT,
                    track_count INTEGER,
                    format TEXT,
                    country TEXT,
                    date TEXT,
                    artist_id TEXT,
                    primary_type TEXT,
                    secondary_types TEXT,
                    processed_at TEXT
                )
                ''')
                
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS contributors (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    type TEXT,
                    disambiguation TEXT,
                    gender TEXT,
                    country TEXT,
                    area TEXT,
                    begin_area TEXT,
                    dob TEXT,
                    urls TEXT,
                    aliases TEXT,
                    processed_at TEXT
                )
                ''')
                
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS processed_artists (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    processed_at TEXT,
                    total_time_seconds REAL
                )
                ''')
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise

    async def _save_to_db(self, table: str, id: str, data: Union[Dict, List[Dict]]):
        """Save data to the appropriate table in the database"""
        if not data:
            return
            
        # Convert single item to list for consistent processing
        if isinstance(data, dict):
            data = [data]
            
        try:
            async with self._db_connection() as conn:
                cursor = conn.cursor()
                
                if table == 'recordings':
                    # Convert complex fields to JSON strings
                    for item in data:
                        item['tags'] = json.dumps(item.get('tags', []), ensure_ascii=False)
                        item['credits'] = json.dumps(item.get('credits', {}), ensure_ascii=False)
                        item['release_id'] = json.dumps(item.get('release_id', []), ensure_ascii=False)
                        item['work_ids'] = json.dumps(item.get('work_ids', []), ensure_ascii=False)
                        item['urls'] = json.dumps(item.get('urls', []), ensure_ascii=False)

                    cursor.executemany('''
                    INSERT OR REPLACE INTO recordings 
                    (id, title, length, tags, credits, release_id, artist_id, work_ids, urls, processed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', [(item['id'], item.get('title', ''), item.get('length', 0),
                          item.get('tags', '[]'), item.get('credits', '{}'),
                          item.get('release_id', []), item.get('artist_id', ''),
                          item.get('work_ids', '[]'), item.get('urls', '[]'), datetime.now(UTC).isoformat())
                         for item in data])
                    
                elif table == 'releases':
                    # Convert complex fields to JSON strings
                    for item in data:
                        item['labels'] = json.dumps(item.get('labels', {}), ensure_ascii=False)

                    cursor.executemany('''
                    INSERT OR REPLACE INTO releases 
                    (id, title, labels, track_count, format, country, date, artist_id, secondary_types, processed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', [(item['id'], item.get('title', ''), item.get('labels', '{}'),
                          item.get('track_count', 0), item.get('format', ''),
                          item.get('country', ''), item.get('date', ''), 
                          item.get('artist_id', ''), json.dumps(item.get('secondary-types', []), ensure_ascii=False),
                          datetime.now(UTC).isoformat())
                         for item in data])
                    
                elif table == 'works':
                    # Convert complex fields to JSON strings
                    for item in data:
                        item['tags'] = json.dumps(item.get('tags', []), ensure_ascii=False)
                        item['credits'] = json.dumps(item.get('credits', {}), ensure_ascii=False)

                    cursor.executemany('''
                    INSERT OR REPLACE INTO works 
                    (id, title, type, language, tags, credits, processed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', [(item['id'], item.get('title', ''), item.get('type', ''),
                          item.get('language', ''), item.get('tags', '[]'),
                          item.get('credits', '{}'), datetime.now(UTC).isoformat())
                         for item in data])
                    
                elif table == 'artists':
                    # Convert complex fields to JSON strings
                    for item in data:
                        item['urls'] = json.dumps(item.get('urls', []), ensure_ascii=False)
                        item['aliases'] = json.dumps(item.get('aliases', []), ensure_ascii=False)
                    
                    cursor.executemany('''
                    INSERT OR REPLACE INTO artists 
                    (id, name, type, disambiguation, gender, country, area, begin_area, dob, urls, aliases, processed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', [(item['id'], item.get('name', ''), item.get('type', ''),
                          item.get('disambiguation', ''), item.get('gender', ''),
                          item.get('country', ''), item.get('area', ''),
                          item.get('begin-area', ''), item.get('DOB', ''),
                          item.get('urls', '[]'), item.get('aliases', '[]'),
                          datetime.now(UTC).isoformat())
                         for item in data])
                    
                elif table == 'contributors':
                    # Convert complex fields to JSON strings
                    for item in data:
                        item['urls'] = json.dumps(item.get('urls', []), ensure_ascii=False)
                        item['aliases'] = json.dumps(item.get('aliases', []), ensure_ascii=False)

                    cursor.executemany('''
                    INSERT OR REPLACE INTO contributors 
                    (id, name, type, disambiguation, gender, country, area, begin_area, dob, urls, aliases, processed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', [(item['id'], item.get('name', ''), item.get('type', ''),
                          item.get('disambiguation', ''), item.get('gender', ''),
                          item.get('country', ''), item.get('area', ''),
                          item.get('begin-area', ''), item.get('DOB', ''),
                          item.get('urls', '[]'), item.get('aliases', '[]'),
                          datetime.now(UTC).isoformat())
                         for item in data])
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error saving {table} data to database: {e}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def _concurrent_api_request(self, type: str, id: str | None, includes: list = None, params: dict = None) -> Optional[Dict]:
        """Make concurrent API request with rate limiting and improved error handling"""

        async with self.semaphore:
            try:
                await self.rate_limiter.acquire()  # This will handle the rate limiting
                result = await self._make_request(id, type, includes, params)
                
                if not result:
                    logger.warning(f"No data returned for {type} {id}")
                    return None
                    
                return result
                
            except asyncio.TimeoutError as e:
                logger.warning(f"Timeout while requesting {type} {id} - Attempting retry")
                raise  # Re-raise for retry decorator to handle
                
            except musicbrainzngs.WebServiceError as e:
                logger.error(f"API request failed for {type} {id}: {e}")
                return None
                
            except Exception as e:
                logger.error(f"Unexpected error for {type} {id}: {e}")
                return None

    async def _make_request(self, id: str, type: str, includes: list = None, params: dict = None) -> Optional[Dict]:
        """Make a request to MusicBrainz API with exponential backoff retry"""
        # Add jitter to avoid thundering herd problem
        jitter = random.uniform(0, 0.2)
        await asyncio.sleep(jitter)  # Add jitter to rate limiting
        
        try:
            # Use asyncio.to_thread with timeout to run the synchronous musicbrainzngs calls
            if type == 'artist':
                result = await asyncio.wait_for(
                    asyncio.to_thread(musicbrainzngs.get_artist_by_id, id, includes),
                    timeout=25
                )
            elif type == 'release':
                result = await asyncio.wait_for(
                    asyncio.to_thread(musicbrainzngs.browse_releases, includes=includes, **params),
                    timeout=25
                )
            elif type == 'recording' and params:
                # Handle browse API for recordings
                result = await asyncio.wait_for(
                    asyncio.to_thread(musicbrainzngs.browse_recordings, includes=includes, **params),
                    timeout=25
                )
            elif type == 'work':
                # Handle browse API for works
                result = await asyncio.wait_for(
                    asyncio.to_thread(musicbrainzngs.get_work_by_id, id, includes),
                    timeout=25
                )
            else:
                logger.warning(f"Unknown request type: {type}")
                return None
                
            return result
        except asyncio.TimeoutError:
            logger.error(f"Thread operation timed out for {type} {id}")
            raise  # Re-raise for retry decorator to handle
        except musicbrainzngs.WebServiceError as e:
            logger.error(f"API request failed for {type} {id}: {e}")
            # Check for rate limiting response (503)
            if "503" in str(e):
                logger.warning(f"Rate limiting detected. Adding additional delay.")
                await asyncio.sleep(2)  # Additional delay for rate limiting
            raise  # Re-raise for backoff to handle
        except Exception as e:
            logger.error(f"Unexpected error for {type} {id}: {e}")
            return None

    async def _process_batch(self, items: List[Any], process_func: Callable, max_concurrent: int = None) -> List[Any]:
        """Process a batch of items concurrently with proper error handling"""
        if max_concurrent is None:
            max_concurrent = self.max_concurrent_requests
            
        async def process_with_limit(item):
            try:
                async with self.semaphore:
                    return await process_func(item)
            except Exception as e:
                logger.error(f"Error processing item: {e}")
                return None
                
        try:
            results = await aiometer.run_all(
                [functools.partial(process_with_limit, item) for item in items],
                max_at_once=max_concurrent
            )
            # Filter out None results (failed items)
            return [r for r in results if r is not None]
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
            return []

    async def _update_cache(self, cache_dict: Dict, key: str, value: Any):
        """Update a cache with LRU eviction policy"""
        # Update access time for LRU
        self.cache_access_times[key] = time.time()
        
        # Add to cache
        cache_dict[key] = value
        
        # Check if cache needs eviction
        if len(cache_dict) > self.max_cache_size:
            # Sort by access time and keep only the most recently used items
            oldest_keys = sorted(self.cache_access_times.items(), key=lambda x: x[1])[:len(cache_dict) - self.max_cache_size]
            for old_key, _ in oldest_keys:
                if old_key in cache_dict:
                    del cache_dict[old_key]
                    del self.cache_access_times[old_key]
   
    def save_progress(self):
        """Save the current progress to a file"""
        progress_data = {
            "last_artist_offset": self.batch_progress.get("last_artist_offset", 0),
            "processed_artists": list(self.batch_progress.get("processed_artists", set())),  # Convert set to list
            "last_update": datetime.now(UTC).isoformat(),
            "total_time_seconds": self.batch_progress.get("total_time_seconds", 0),
            "processed_releases": list(self.batch_progress.get("processed_releases", set())),  # Convert set to list
            "current_artist_id": self.batch_progress.get("current_artist_id", None),
            "current_artist_name": self.batch_progress.get("current_artist_name", None)
        }
        
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(progress_data, f)
            logger.info(f"Progress saved: {len(self.batch_progress.get('processed_artists', set()))} artists processed, last offset: {self.batch_progress.get('last_artist_offset', 0)}")
        except Exception as e:
            logger.error(f"Error saving progress: {e}")
            raise

    def load_progress(self):
        """Load progress from file if it exists"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    progress_data = json.load(f)
                
                self.batch_progress["last_artist_offset"] = progress_data.get("last_artist_offset", 0)
                self.batch_progress["processed_artists"] = set(progress_data.get("processed_artists", []))  # Convert list back to set
                self.batch_progress["processed_releases"] = set(progress_data.get("processed_releases", []))  # Convert list back to set
                self.batch_progress["current_artist_id"] = progress_data.get("current_artist_id")
                self.batch_progress["current_artist_name"] = progress_data.get("current_artist_name")
                
                logger.info(f"Progress loaded: {len(self.batch_progress.get('processed_artists', set()))} artists processed, {len(self.batch_progress.get('processed_releases', set()))} releases processed")
            except Exception as e:
                logger.error(f"Error loading progress: {e}")
                self.batch_progress = {
                    "last_artist_offset": 0,
                    "processed_artists": set(),
                    "processed_releases": set(),
                    "total_time_seconds": 0,
                    "current_artist_id": None,
                    "current_artist_name": None
                }
        else:
            logger.info("No progress file found, starting fresh")
            self.batch_progress = {
                "last_artist_offset": 0,
                "processed_artists": set(),
                "processed_releases": set(),
                "total_time_seconds": 0,
                "current_artist_id": None,
                "current_artist_name": None
            }

    def is_artist_processed(self, artist_id: str) -> bool:
        """Check if an artist has already been processed"""
        # Check memory cache first
        if artist_id in self.batch_progress.get("processed_artists", set()):
            return True
            
        # Check database
        if not os.path.exists(self.db_path):
            logger.warning(f"Database {self.db_path} does not exist, cannot check if artist {artist_id} is processed")
            return False
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT id FROM processed_artists WHERE id = ?", (artist_id,))
            result = cursor.fetchone()
            if result:
                # Add to memory cache
                if "processed_artists" not in self.batch_progress:
                    self.batch_progress["processed_artists"] = set()
                self.batch_progress["processed_artists"].add(artist_id)
                return True
            return False
        finally:
            conn.close()

    def mark_artist_processed(self, artist_id: str, artist_name: str, total_time: float = None):
        """Mark an artist as processed and save timing information"""
        # Add to memory cache
        if "processed_artists" not in self.batch_progress:
            self.batch_progress["processed_artists"] = set()
        self.batch_progress["processed_artists"].add(artist_id)
        
        # Add to database
        if not os.path.exists(self.db_path):
            logger.warning(f"Database {self.db_path} does not exist, cannot mark artist {artist_id} as processed")
            return
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            processed_at = datetime.now(UTC).isoformat()
            cursor.execute(
                "INSERT OR REPLACE INTO processed_artists (id, name, processed_at, total_time_seconds) VALUES (?, ?, ?, ?)",
                (artist_id, artist_name, processed_at, total_time)
            )
            conn.commit()
        finally:
            conn.close()
             
    def transform_artist_data(self, original_data: Dict) -> Optional[Dict]:
        """Transform artist data from original format to simplified format"""
        try:
            transformed = {
                'id': '',
                'type': '',
                'name': '',
                'disambiguation': '',
                'gender': '',
                'country': '',
                'area': '',
                'begin-area': '',
                'DOB': '',
                'urls': [],
                'aliases': [],
            }
            
            # Basic fields
            transformed['id'] = original_data.get('id', '')
            transformed['type'] = original_data.get('type', '')
            transformed['name'] = original_data.get('name', '')
            transformed['disambiguation'] = original_data.get('disambiguation', '')
            transformed['gender'] = original_data.get('gender', '')
            transformed['country'] = original_data.get('country', '')

            # Handle nested area objects
            area_data = original_data.get('area', {})
            transformed['area'] = area_data.get('name', '') if isinstance(area_data, dict) else str(area_data)
            
            begin_area_data = original_data.get('begin-area', {})
            transformed['begin-area'] = begin_area_data.get('name', '') if isinstance(begin_area_data, dict) else str(begin_area_data)
            
            # Handle life-span to DOB conversion
            life_span = original_data.get('life-span', {})
            transformed['DOB'] = life_span.get('begin', '') if isinstance(life_span, dict) else ''
            
            # Handle url-relation-list
            transformed['urls'] = original_data.get('url-relation-list', [])

            # Handle aliases
            alias_list = original_data.get('alias-list', [])
            transformed['aliases'] = json.dumps([alias.get('sort-name', '') for alias in alias_list] if isinstance(alias_list, list) else [])
                
            return transformed
            
        except Exception as e:
            logger.error(f"Error transforming artist data: {e}")
            return None

    async def get_artist_details(self, artist_id: str) -> Optional[Dict]:
        """Get detailed artist information with improved caching and error handling"""
        if not artist_id:
            logger.warning("No artist ID provided")
            return None

        # Check memory cache first
        if artist_id in self.artist_cache:
            # Update access time for LRU
            self.cache_access_times[artist_id] = time.time()
            return self.artist_cache[artist_id]
        
        try:
            result = await self._concurrent_api_request(
                'artist',
                artist_id,
                includes=self.ARTIST_INCLUDES["basic"] + self.ARTIST_INCLUDES["relationships"]
            )
            
            if not result or 'artist' not in result:
                return None
                
            artist_data = self.transform_artist_data(result['artist'])
            
            # Save to memory cache
            await self._update_cache(self.artist_cache, artist_id, artist_data)
            
            return artist_data
            
        except Exception as e:
            logger.error(f"Error fetching artist details for {artist_id}: {e}")
            return None

    async def transform_recording_data(self, recording: Dict, release_id: str, artist_id: str) -> Optional[Dict]:
        """Transform recording data from original format to simplified format"""
        try:
            work_ids = []
            credits = {
                'lyricist': [],
                'producer': [],
                'arranger': [],
                'performer': [],
                'composer': [],
            }

            # Process relationships
            if 'artist-relation-list' in recording:
                for rel in recording['artist-relation-list']:
                    role = rel['type']
                    if role == 'composer':
                        credits['composer'].append(rel['artist']['id'])
                    elif role == 'lyricist':
                        credits['lyricist'].append(rel['artist']['id'])
                    elif role == 'producer':
                        credits['producer'].append(rel['artist']['id'])
                    elif role == 'arranger':
                        credits['arranger'].append(rel['artist']['id'])
                    elif ((role == 'vocal') and ("guest" in rel.get("attribute-list", []) or "lead" in rel.get("attribute-list", [])) and (rel['artist']['id'] != artist_id)):
                        credits['performer'].append(rel['artist']['id'])
            
            # Process work relationships
            if 'work-relation-list' in recording:
                for work_rel in recording['work-relation-list']:
                    work_id = work_rel.get('work', {}).get('id')
                    if work_id:
                        work_ids.append(work_id)

            # Extract tags
            try:
                recording['tags'] = [t['name'] for t in recording.get('tag-list', [])]
            except (KeyError, TypeError):
                recording['tags'] = []
                logger.debug(f"Could not parse tags for record {recording['id']}")

            return {
                'id': recording['id'],
                'title': recording.get('title', ''),
                'length': recording.get('length', 0),
                'tags': recording.get('tags', []),
                'credits': credits,
                'release_id': release_id,
                'artist_id': artist_id,
                'work_ids': work_ids,
                'urls': recording.get('url-relation-list', [])
            }

        except Exception as e:
            logger.error(f"Error transforming recording data: {e}")
            return None
            
    async def get_release_recordings(self, release_id: str, artist_id: str) -> List[Dict]:
        """Get all recordings associated with a release with improved error handling"""
        if not release_id:
            logger.warning("No release ID provided")
            return []

        # Check memory cache first
        if release_id in self.release_cache:
            self.cache_access_times[release_id] = time.time()
            return self.release_cache[release_id]
            
        try:
            # Use browse API to get recordings directly
            result = await self._concurrent_api_request(
                'recording',
                None,  # No specific recording ID needed for browse
                includes= ["tags", "url-rels", "work-rels", "artist-rels"],
                params={'release': release_id}
            )
            
            if not result or 'recording-list' not in result:
                return []
                
            recordings = result['recording-list']
            processed_recordings = await self._process_batch(
                recordings,
                lambda r: self.transform_recording_data(r, release_id, artist_id)
            )
            
            # Update cache with merged recordings
            if processed_recordings:
                await self._update_cache(self.release_cache, release_id, processed_recordings)
            
            return processed_recordings
            
        except Exception as e:
            logger.error(f"Error fetching recordings for release {release_id}: {e}")
            return []

    async def transform_release_data(self, release: Dict, artist_id) -> Optional[Dict]:
        """Transform release data from original format to simplified format"""
        try:
            label_lookup = []
            if release and 'label-info-list' in release:
                for label_info in release['label-info-list']:
                    if 'label' in label_info:
                        label = label_info['label']
                        label_id = label.get('id')
                        
                        if label_id:
                            # Check if label_id already exists in the list (optional if needed)
                            if not any(l['id'] == label_id for l in label_lookup):
                                label_lookup.append({
                                    'id': label_id,
                                    'name': label.get('name', ''),
                                    'disambiguation': label.get('disambiguation', ''),
                                })

            release_info = {
                'id': release.get('id', ''),
                'title': release.get('title', ''),     
                'labels': label_lookup,
                'track_count': release['medium-list'][0]['track-count'] if 'track-count' in release['medium-list'][0] else 0,
                'format': release['medium-list'][0]['format'] if 'format' in release['medium-list'][0] else '',
                'country': release.get('country', ''),
                'status': release.get('status', ''),
                'date': release.get('date', ''),
                'artist_id': artist_id
            }
            
            # Include release group information if available
            if 'release-group' in release:
                release_group = release['release-group']
                release_info['primary-type'] = release_group.get('primary-type', '')
                release_info['secondary-types'] = [t for t in release_group.get('secondary-type-list', [])]

            return release_info

        except Exception as e:
            logger.error(f"Error transforming release data: {e}")
            return None

    async def get_work_details(self, work_id: str) -> Optional[Dict]:
        """Get detailed work information with caching and error handling"""
        if not work_id:
            logger.warning("No work ID provided")
            return None

        # Check memory cache first
        if work_id in self.work_cache:
            # Update access time for LRU
            self.cache_access_times[work_id] = time.time()
            return self.work_cache[work_id]
        
        try:
            result = await self._concurrent_api_request(
                'work',
                work_id,
                includes=['artist-rels']
            )

            if not result or 'work' not in result:
                return None
                
            work_data = await self.transform_work_data(result['work'])
            
            # Save to memory cache
            await self._update_cache(self.work_cache, work_id, work_data)
            
            return work_data
            
        except Exception as e:
            logger.error(f"Error fetching work details for {work_id}: {e}")
            return None

    async def transform_work_data(self, work: Dict) -> Optional[Dict]:
        """Transform work data from original format to simplified format"""
        try:
            credits = {
                'lyricist': [],
                'composer': [],
                'arranger': []
            }

            # Process relationships
            if 'artist-relation-list' in work:
                for rel in work['artist-relation-list']:
                    role = rel['type']
                    if role in ['composer']:
                        credits['composer'].append(rel['artist']['id'])
                    elif role == 'lyricist':
                        credits['lyricist'].append(rel['artist']['id'])
                    elif role == 'arranger':
                        credits['arranger'].append(rel['artist']['id'])

            return {
                'id': work['id'],
                'title': work.get('title', ''),
                'type': work.get('type', ''),
                'language': work.get('language', ''),
                'credits': credits
            }

        except Exception as e:
            logger.error(f"Error transforming work data: {e}")
            return None

    async def extract_works_from_recording(self, recording: Dict) -> Dict:
        """Extract work information from a recording's relationships"""
        if not recording or 'credits' not in recording:
            return {'composer': [], 'lyricist': []}

        work_ids = recording.get('work_ids', [])
        
        combined_credits = {
            'composer': [],
            'lyricist': []
        }
        
        for work_id in work_ids:
            try:
                # Fetch work details one by one (with caching)
                work_data = await self.get_work_details(work_id)
                if work_data and 'credits' in work_data:
                    work_credits = work_data['credits']
                    
                    # Add composers
                    if 'composer' in work_credits:
                        for composer_id in work_credits['composer']:
                            if composer_id not in combined_credits['composer']:
                                combined_credits['composer'].append(composer_id)
                    
                    # Add lyricists
                    if 'lyricist' in work_credits:
                        for lyricist_id in work_credits['lyricist']:
                            if lyricist_id not in combined_credits['lyricist']:
                                combined_credits['lyricist'].append(lyricist_id)

            except Exception as e:
                logger.error(f"Error processing work {work_id}: {e}")
        
        return combined_credits
 
    async def get_artist_releases(self, artist_id: str) -> List[Dict]:
        """Get all releases associated with an artist"""
        offset = 0
        batch_size = 100
        all_releases = []
        
        while True:
            try:
                # Use _concurrent_api_request for proper rate limiting and error handling
                releases = await self._concurrent_api_request(
                    'release',
                    None,  # No specific release ID needed for browse
                    includes=['media', 'labels', 'release-groups'],
                    params={
                        'artist': artist_id,
                        'limit': batch_size,
                        'offset': offset
                    }
                )
            
                if not releases or 'release-list' not in releases:
                    break
                    
                release_list = releases['release-list']
                if not release_list:
                    break
                    
                processed_releases = await self._process_batch(
                    release_list,
                    lambda r: self.transform_release_data(r, artist_id)
                )
                
                if not processed_releases:
                    logger.warning(f"No releases processed for artist {artist_id} at offset {offset}")
                    break
                    
                all_releases.extend(processed_releases)
                
                if len(release_list) < batch_size:
                    break
                    
                offset += batch_size
                
            except Exception as e:
                logger.error(f"Error fetching releases for artist {artist_id}: {e}")
                break
                
        return all_releases

    async def _filter_artist_releases(self, releases: List[Dict]) -> List[Dict]:
        """Filter and deduplicate releases, excluding singles and non-music formats"""
        unique_releases = {}
        
        # Define formats to exclude
        excluded_formats = ['Blu-ray', 'DVD', 'VHS', 'LaserDisc']
        
        for release in releases:
            title = release.get('title', '')
            country = release.get('country', '')
            status = release.get('status', '')
            
            # Skip if not from Japan or not official
            if country != 'JP' or status != 'Official':
                continue
                
            # Check format
            format_str = release.get('format', '')
            if any(excluded_format in format_str for excluded_format in excluded_formats):
                continue
            
            # Check secondary types and filter out
            secondary_types = release.get('secondary-types', [])
            if any(t in ['Live', 'Remix', 'Soundtrack'] for t in secondary_types):
                continue
            
            primary_type = release.get('primary-type', '')

            # Only add non-single releases to unique_releases
            if primary_type != 'Single':
                # Use title as key for deduplication
                if title not in unique_releases:
                    unique_releases[title] = release
                else:
                    # If we have a duplicate, prefer the one with more information
                    existing_release = unique_releases[title]
                    if len(release.get('labels', [])) > len(existing_release.get('labels', [])):
                        unique_releases[title] = release

        # Convert back to list and sort by title
        filtered_releases = list(unique_releases.values())
        filtered_releases.sort(key=lambda x: x.get('title', ''))
        
        return filtered_releases
    
    async def search_japanese_artists(self, limit: int = 100, offset: int = 0, sort: str = "name") -> List[Dict]:
        """
        Search for artists from Japan using MusicBrainz API
        
        Args:
            limit: Maximum number of artists to return
            offset: Offset for pagination
            sort: Sort order (default: "name" for alphabetical)
            
        Returns:
            List of artist data dictionaries
        """
        try:
            # Search for artists from Japan
            result = musicbrainzngs.search_artists(
                country="JP",
                limit=limit,
                offset=offset,
                #sort=sort  # Sort by name (alphabetical)
            )
            
            if not result or 'artist-list' not in result:
                logger.warning("No Japanese artists found")
                return []
                
            artists = result['artist-list']
            sorted_artists = sorted(result["artist-list"], key=lambda a: a.get("name"))
            logger.info(f"Found {len(artists)} Japanese artists")
            
            # Process each artist
            artist_data = []
            for artist in sorted_artists:
                artist_id = artist['id']
                
                # Skip if already processed
                if self.is_artist_processed(artist_id):
                    logger.info(f"Artist {artist_id} already processed, skipping")
                    continue
                
                # Skip if already in cache
                if artist_id in self.artist_cache:
                    artist_data.append(self.artist_cache[artist_id])
                    continue
                
                # Get detailed artist information
                artist_details = await self.get_artist_details(artist_id)
                if artist_details:
                    artist_data.append(artist_details)
                    # Mark as processed
                    self.mark_artist_processed(artist_id, artist_details.get('name', ''))
                
                # Adaptive delay
                await self.rate_limiter.acquire()
            
            # Update progress
            self.batch_progress["last_artist_offset"] = offset + limit
            
            return artist_data
            
        except Exception as e:
            logger.error(f"Error searching for Japanese artists: {e}")
            return []
  
    def deduplicate_recordings(self, recordings: List[Dict]) -> List[Dict]:
        merged_recordings = {}

        for recording in recordings:
            if recording is None:
                continue
            
            title = recording.get('title', '')

            # Use lowercase title as key for merging
            title_key = re.sub(r'[^A-Za-z0-9 ]+', '', title.lower())

            # Remove any recordings that are a music video or PV in the release
            if any(sub in title_key for sub in ['music video', 'pv']):
                continue

            # Convert length to integer, defaulting to 0 if invalid
            try:
                length = int(recording.get('length', 0))
            except (ValueError, TypeError):
                length = 0
            
            if title_key not in merged_recordings:
                # Initialize with first recording's data
                merged_recordings[title_key] = {
                    'id': recording['id'],
                    'title': title,
                    'length': length,
                    'tags': set(recording.get('tags', [])),
                    'credits': {
                        'lyricist': set(recording['credits']['lyricist']),
                        'producer': set(recording['credits']['producer']),
                        'arranger': set(recording['credits']['arranger']),
                        'performer': set(recording['credits']['performer']),
                        'composer': set(recording['credits']['composer'])
                    },
                    'release_id': set([recording['release_id']]),
                    'artist_id': recording['artist_id'],
                    'work_ids': set(recording['work_ids']),
                    'urls': recording['urls']
                }
            else:
                # Merge with existing recording
                existing = merged_recordings[title_key]
                
                # Update length to keep the higher value
                existing['length'] = max(existing['length'], length)
                
                # Merge tags
                existing['tags'].update(recording.get('tags', []))
                
                # Merge credits
                for role in ['lyricist', 'producer', 'arranger', 'performer', 'composer']:
                    existing['credits'][role].update(recording['credits'][role])
                
                # Add release_id to set
                existing['release_id'].add(recording['release_id'])
                
                # Merge work_ids
                existing['work_ids'].update(recording['work_ids'])
                
                # Merge urls
                existing['urls'] = []

        # Convert sets back to lists and prepare final output
        final_recordings = []
        for recording in merged_recordings.values():
            # Convert all sets to lists
            recording['tags'] = list(recording['tags'])
            recording['release_id'] = list(recording['release_id'])
            recording['work_ids'] = list(recording['work_ids'])
            recording['urls'] = list(recording['urls'])
            for role in ['lyricist', 'producer', 'arranger', 'performer', 'composer']:
                recording['credits'][role] = list(recording['credits'][role])
            final_recordings.append(recording)

        return final_recordings
    
    async def _extract_catalog_for_artists(self, artists: List[Dict]) -> None:
        """
        Core method to extract catalog for a list of artists.
        Used by both extract_japanese_music_catalog and extract_single_artist_catalog.
        """
        if not artists:
            return
            
        start_time = time.time()
        artist_ids = [artist['id'] for artist in artists if 'id' in artist]

        # Step 1: Save artists to database
        await self._save_to_db('artists', None, artists)
        logger.info(f"Total artists to process: {len(artists)}")
        
        # Step 2: For each artist, get all their releases
        all_releases = []
        for i, artist in enumerate(artists):
            artist_id = artist['id']
            artist_name = artist['name']
            
            # Update current artist being processed
            self.batch_progress["current_artist_id"] = artist_id
            self.batch_progress["current_artist_name"] = artist_name
            self.save_progress()
            
            logger.info(f"Processing releases for artist {i+1}/{len(artists)}: {artist_name} ({artist_id})")
            
            # Get all releases for this artist
            artist_releases = await self.get_artist_releases(artist_id)
            
            # Filter releases using the helper method
            filtered_releases = await self._filter_artist_releases(artist_releases)
            logger.info(f"Releases found: {[item['title'] for item in filtered_releases]}")
            
            # Add to all releases
            all_releases.extend(filtered_releases)
            
            # Adaptive delay
            await self.rate_limiter.acquire()
        
        logger.info(f"Total filtered releases found: {len(all_releases)}")

        # Step 3: Extract recordings from each release
        all_recordings = []
        for i, release in enumerate(all_releases):
            try:
                release_id = release['id']
                release_title = release['title']
                artist_id = release['artist_id']
                
                # Skip if this release was already processed
                if release_id in self.batch_progress.get("processed_releases", set()):
                    logger.info(f"Skipping already processed release {i+1}/{len(all_releases)}: {release_title} (ID: {release_id})")
                    continue
                
                logger.info(f"Starting to process release {i+1}/{len(all_releases)}: {release_title} (ID: {release_id})")
                
                logger.info(f"Saving release to database...")
                await self._save_to_db('releases', release_id, [release])
                logger.info(f"Release saved successfully")
                
                logger.info(f"Fetching recordings for release...")
                recordings = await self.get_release_recordings(release_id, artist_id)
                logger.info(f"Found {len(recordings)} recordings for release")
                
                # Process works for each recording
                for recording in recordings:
                    try:
                        works = await self.extract_works_from_recording(recording)
                        if works:
                            recording['credits']['composer'] = works['composer']
                            recording['credits']['lyricist'] = works['lyricist']
                    except Exception as e:
                        logger.error(f"Error processing works for recording {recording.get('id', 'unknown')}: {e}")
                        continue
                
                # Add to all recordings
                all_recordings.extend(recordings)
              
                # Mark this release as processed
                if "processed_releases" not in self.batch_progress:
                    self.batch_progress["processed_releases"] = set()
                self.batch_progress["processed_releases"].add(release_id)
                
                # Save progress after each release
                self.save_progress()
                logger.info(f"Progress saved after processing release {release_title}")
                
                # Adaptive delay
                await self.rate_limiter.acquire()
                
                logger.info(f"Successfully completed processing release {i+1}/{len(all_releases)}")
            except Exception as e:
                logger.error(f"Error processing release {release.get('id', 'unknown')}: {e}")
                continue
        
        logger.info(f"Total {len(all_recordings)} recordings found...")
        deduplicated_recordings = self.deduplicate_recordings(all_recordings)
        logger.info(f"Saving {len(deduplicated_recordings)} recordings to database...")
        await self._save_to_db('recordings', None, deduplicated_recordings)
        logger.info("Recordings saved successfully")

        # Step 4: Extract contributors from all recording
        all_contributors = []
        logger.info("Processing contributors...")
        
        # Track processed artist IDs to avoid duplicate API calls
        processed_artist_ids = set()

        # Collect all unique contributor IDs from all recordings
        for recording in deduplicated_recordings:
            if 'credits' in recording:
                credits = json.loads(recording['credits'])
                # Get all contributor IDs from all roles
                for contributor_ids in credits.values():
                    processed_artist_ids.update(contributor_ids)
        
        # Get details for each unique contributor
        for contributor_id in processed_artist_ids:
            if contributor_id not in artist_ids:
                contributor_details = await self.get_artist_details(contributor_id)
                if contributor_details:
                    all_contributors.append(contributor_details)
        
        if all_contributors:
            logger.info(f"Saving {len(all_contributors)} contributors to database...")
            await self._save_to_db('contributors', None, all_contributors)
            logger.info("Contributors saved successfully")
            
        # Mark artists as processed and save timing information
        end_time = time.time()
        total_time = end_time - start_time
        
        for artist in artists:
            self.mark_artist_processed(artist['id'], artist['name'], total_time)
        
        # Clear current artist tracking
        self.batch_progress["current_artist_id"] = None
        self.batch_progress["current_artist_name"] = None
        self.save_progress()
        
        logger.info(f"Completed processing catalog in {total_time:.2f} seconds")
        
    async def extract_japanese_music_catalog(self, batch_size: int = 100, max_artists: int = None, start_offset: int = None) -> None:
        """
        Extract Japanese music catalog following the new flow:
        1. Extract all artists from Japan
        2. Extract all releases from each artist (filtered by country=Japan, status=Official, format, release type, removing duplicates)
        3. Extract recordings from each release
        4. Extract credits (producers, mixers, lead vocals, arrangers, lyricists, composers)
        5. Extract labels associated with recordings
        
        Args:
            batch_size: Number of items to process in each batch
            max_artists: Maximum number of artists to process (None for all)
            start_offset: Starting offset for artist search (None to use saved progress)
        """
        # Use saved progress if start_offset is not provided
        if start_offset is None:
            start_offset = self.batch_progress.get("last_artist_offset", 0)
            logger.info(f"Using saved progress, starting from offset {start_offset}")
        
        # Step 1: Search for Japanese artists
        offset = start_offset
        all_artists = []
        
        while True:
            artists_batch = await self.search_japanese_artists(limit=batch_size, offset=offset)
            if not artists_batch:
                break
                
            all_artists.extend(artists_batch)
            logger.info(f"Found {len(all_artists)} Japanese artists so far")
            
            # Check if we've reached the maximum number of artists
            if max_artists is not None and len(all_artists) >= max_artists:
                all_artists = all_artists[:max_artists]
                logger.info(f"Limiting to {max_artists} artists")
                break
                
            offset += batch_size
            await self.rate_limiter.acquire()
        
        # Process the catalog for all found artists
        await self._extract_catalog_for_artists(all_artists)
        
    async def extract_single_artist_catalog(self, artist_id: str) -> None:
        """
        Extract the complete music catalog for a single Japanese artist
        
        Args:
            artist_id: MusicBrainz ID of the artist to process
        """
        try:
            logger.info(f"Starting to extract catalog for artist ID: {artist_id}")
            
            # Step 1: Get artist details
            artist_details = await self.get_artist_details(artist_id)
            if not artist_details:
                logger.error(f"Could not find artist with ID: {artist_id}")
                return
                
            artist_name = artist_details.get('name', 'Unknown Artist')
            logger.info(f"Processing catalog for artist: {artist_name} ({artist_id})")
            
            # Process the catalog for this single artist
            await self._extract_catalog_for_artists([artist_details])
            
        except Exception as e:
            logger.error(f"Error in extract_single_artist_catalog: {e}")
            raise

async def main():
    # Use environment variables for credentials or blank defaults
    processor = MusicBatchProcessor(
        app_name="JPOPMusicExtractor",
        version="1.0",
        contact=os.environ.get("CONTACT_EMAIL", "user@example.com"),
        batch_size=None,  # Use optimized batch size
        db_path=os.environ.get("DB_PATH", "musicbrainz1.db"),
        max_concurrent_requests=os.environ.get("MAX_CONCURRENT", 5)
    )
    
    try:
        # Get command line arguments
        import argparse
        parser = argparse.ArgumentParser(description='Process Japanese artists catalog')
        parser.add_argument('--batch-size', type=int, default=100, help='Number of artists to process in each batch')
        parser.add_argument('--max-artists', type=int, default=None, help='Maximum number of artists to process')
        parser.add_argument('--start-offset', type=int, default=None, help='Starting offset for artist search')
        parser.add_argument('--artist-id', type=str, default=None, help='Process a single artist by ID')
        parser.add_argument('--db-path', type=str, default=None, help='Path to the SQLite database')
        args = parser.parse_args()
        
        # Override DB path if provided
        if args.db_path:
            processor.db_path = args.db_path
            
        # Load progress before starting
        await processor.initialize()
        
        if args.artist_id:
            # Process a single artist
            logger.info(f"Processing single artist with ID: {args.artist_id}")
            await processor.extract_single_artist_catalog(args.artist_id)
            logger.info("Completed processing single artist catalog")
        else:
            # Process multiple artists
            logger.info("Starting to process Japanese artists and their catalog")
            await processor.extract_japanese_music_catalog(
                batch_size=args.batch_size, 
                max_artists=args.max_artists,
                start_offset=args.start_offset
            )
            logger.info("Completed processing Japanese artists catalog")
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user. Saving progress...")
        processor.save_progress()
        logger.info("Progress saved. Exiting.")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        import traceback
        logger.error(traceback.format_exc())
        processor.save_progress()
    finally:
        processor.close()  # Ensure resources are properly closed

if __name__ == "__main__":
    asyncio.run(main())