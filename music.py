import asyncio
import musicbrainzngs
import logging
from typing import List, Dict, Set, Optional
from dataclasses import dataclass
from datetime import datetime
import json

logging.basicConfig(level=logging.INFO)
logging.getLogger('musicbrainzngs').setLevel(logging.WARNING) 
logger = logging.getLogger(__name__)

@dataclass
class BatchConfig:
    """Configuration for batch processing"""
    start_track: int  # Starting track number (0-based)
    end_track: int    # Ending track number (exclusive)
    batch_size: int   # Size of each batch
    genre: str        # Genre to search for

class MusicBatchProcessor:
    # Include configurations
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

    RELEASE_INCLUDES = {
        "basic": [
            "artists", "labels", "recordings", "release-groups", 
            "media", "artist-credits", "discids", "annotation", 
            "aliases", "tags", "user-tags"
        ],
        "relationships": [
            "area-rels", "artist-rels", "label-rels", "place-rels", 
            "event-rels", "recording-rels", "release-rels", 
            "release-group-rels", "series-rels", "url-rels", "work-rels"
        ],
        "additional": ["isrcs", "work-level-rels"]
    }

    RECORDING_INCLUDES = {
        "basic": [
            "artists", "releases", "annotation", "aliases", "tags"
        ],
        "relationships": [
            "area-rels", "artist-rels", "label-rels", "place-rels", 
            "event-rels", "recording-rels", "release-rels", 
            "release-group-rels", "series-rels", "url-rels", "work-rels"
        ]
    }

    def __init__(self, app_name: str, version: str, contact: str, batch_size: int = 100):
        """Initialize the processor with MusicBrainz credentials and configuration"""
        musicbrainzngs.set_useragent(app_name, version, contact)
        musicbrainzngs.auth("dianamusic", "forensics25")
        self.batch_size = batch_size
        self.delay = 1  # Respect rate limiting
        self.processed_tracks: Set[str] = set()
        self.batch_progress: Dict[str, int] = {}
        self.artist_cache: Dict[str, Dict] = {}

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
                'tags': [],
                'aliases': []
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
            url_relations = original_data.get('url-relation-list', [])
            if isinstance(url_relations, list):
                transformed['urls'] = [
                    {'type': url.get('type', ''), 'target': url.get('target', '')}
                    for url in url_relations
                ]
                
            # Handle tags
            tag_list = original_data.get('tag-list', [])
            transformed['tags'] = [tag.get('name', '') for tag in tag_list] if isinstance(tag_list, list) else []
                
            # Handle aliases
            alias_list = original_data.get('alias-list', [])
            transformed['aliases'] = [alias.get('sort-name', '') for alias in alias_list] if isinstance(alias_list, list) else []
                
            return transformed
            
        except Exception as e:
            logger.error(f"Error transforming artist data: {e}")
            return None

    async def get_artist_details(self, artist_id: str) -> Optional[Dict]:
        """Get detailed artist information with caching"""
        if artist_id in self.artist_cache:
            return self.artist_cache[artist_id]
            
        await asyncio.sleep(self.delay)
        try:
            result = musicbrainzngs.get_artist_by_id(
                artist_id,
                includes=self.ARTIST_INCLUDES["basic"]
            )
            
            if not result or 'artist' not in result:
                return None
                
            artist_data = self.transform_artist_data(result['artist'])
            self.artist_cache[artist_id] = artist_data
            return artist_data
            
        except Exception as e:
            logger.error(f"Error fetching artist details for {artist_id}: {e}")
            return None

    async def get_track_details(self, recording_id: str) -> Optional[Dict]:
        """Get detailed track information including credits"""
        try:
            result = await self._make_request(
                recording_id, 
                'record',
                includes=self.RECORDING_INCLUDES["basic"] + self.RECORDING_INCLUDES["relationships"]
            )
            
            if not result or 'recording' not in result:
                return None
                
            recording = result['recording']
            artist_ids = set()
            credits = {
                'songwriters': [],
                'lyricists': [],
                'producers': [],
                'arrangers': [],
                'performers': []
            }
            
            # Process main artists and credits
            print('artist-credit', recording['artist-credit'])
            if 'artist-credit' in recording:
                for credit in recording['artist-credit']:
                    if isinstance(credit, dict) and 'artist' in credit:
                        artist_ids.add(credit['artist']['id'])
                        credits['performers'].append(credit['artist']['id'])

            # Process relationships
            if 'artist-relation-list' in recording:
                for rel in recording['artist-relation-list']:
                    artist_ids.add(rel['artist']['id'])
                    role = rel['type']
                    if role in ['songwriter', 'composer']:
                        credits['songwriters'].append(rel['artist']['id'])
                    elif role == 'lyricist':
                        credits['lyricists'].append(rel['artist']['id'])
                    elif role == 'producer':
                        credits['producers'].append(rel['artist']['id'])
                    elif role == 'arranger':
                        credits['arrangers'].append(rel['artist']['id'])
                    elif ((role == 'performer') and ('artist-credit' not in recording)):
                        credits['perfomers'].append(rel['artist']['id'])

            # Fetch all artists
            artist_tasks = [self.get_artist_details(artist_id) for artist_id in artist_ids]
            artists_data = await asyncio.gather(*artist_tasks)
            artists_lookup = {
                artist['id']: artist for artist in artists_data if artist
            }
            
            # Replace IDs with full artist data
            for role, artist_ids in credits.items():
                credits[role] = [artists_lookup[aid] for aid in artist_ids if aid in artists_lookup]

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
                'artist-credit': recording.get('artist-credit', [])
            }
            
        except Exception as e:
            logger.error(f"Error fetching track details for {recording_id}: {e}")
            return None

    async def get_album_details(self, release_id: str) -> Optional[Dict]:
        """Get detailed album information"""
        try:
            result = await self._make_request(
                release_id, 
                'release',
                includes=self.RELEASE_INCLUDES["basic"] + ["label-rels", "url-rels"]
            )

            if not result or 'release' not in result:
                return None
                
            release = result['release']
            label_info_list = release.get('label-info-list', [])
            medium_list = release.get('medium-list', [])

            url_relations = release.get('url-relation-list', [])
            if isinstance(url_relations, list):
                release['url-relation-list'] = [
                    {'type': url.get('type', ''), 'target': url.get('target', '')}
                    for url in url_relations
                ]

            return {
                'id': release['id'],
                'title': release.get('title', ''),
                'date': release.get('date', ''),
                'country': release.get('country', ''),
                'label': label_info_list[0].get('label', {}).get('name', '') if label_info_list else '',
                'format': medium_list[0].get('format', '') if medium_list else '',
                'track_count': medium_list[0].get('track-count', 0) if medium_list else 0,
                'urls': release.get('url-relation-list', []),
            }
            
        except Exception as e:
            logger.error(f"Error fetching album details for {release_id}: {e}")
            return None

    async def _make_request(self, id: str, type: str, includes: list = None) -> Optional[Dict]:
        """Make a request to MusicBrainz API"""
        await asyncio.sleep(self.delay)
        try:
            if type == 'record':
                return musicbrainzngs.get_recording_by_id(id, includes)
            elif type == 'release':
                return musicbrainzngs.get_release_by_id(id, includes)
        except musicbrainzngs.WebServiceError as e:
            logger.error(f"API request failed for {type} {id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error for {type} {id}: {e}")
            return None

    async def process_genre_batch(
        self, 
        genre: str, 
        offset: int,
        start_track: int,
        end_track: int
    ) -> List[Dict]:
        """Process a batch of tracks for a given genre with specified boundaries"""
        try:
            effective_offset = offset + start_track
            
            if effective_offset >= end_track:
                logger.info(f"Reached end track boundary ({end_track})")
                return []
            
            effective_batch_size = min(self.batch_size, end_track - effective_offset)
            
            logger.info(f"Fetching tracks {effective_offset} to {effective_offset + effective_batch_size} for genre {genre}")
            
            # Attempt the search with retries
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    result = musicbrainzngs.search_recordings(
                        query=f'tag:{genre} AND date:[1990 TO *]',
                        limit=effective_batch_size,
                        offset=effective_offset
                    )
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(2 ** attempt)
            
            if not result or 'recording-list' not in result:
                return []
            
            tasks = []
            track_index_map = {}  # Map track_id to result index
            albums_map = {}  # Map track_id to list of album results
            for recording in result['recording-list']:
                track_id = recording['id']
                
                if track_id in self.processed_tracks:
                    continue
                
                self.processed_tracks.add(track_id)

                # Get track details
                track_task = self.get_track_details(track_id)
                track_index_map[track_id] = len(tasks)  # Store its index
                tasks.append(track_task)

                # Get album details (if any)
                album_tasks = []
                if 'release-list' in recording:
                    for album in recording['release-list']:
                        album_tasks.append(self.get_album_details(album['id']))
                
                if not album_tasks:
                    album_tasks.append(asyncio.create_task(asyncio.sleep(0)))  # Ensure correct result structure
                
                albums_map[track_id] = [len(tasks) + i for i in range(len(album_tasks))]  # Store album indexes
                tasks.extend(album_tasks)

            if not tasks:
                return []

            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            tracks_data = []
            for track_id, track_index in track_index_map.items():
                track = results[track_index]
                
                if isinstance(track, Exception):
                    continue

                # Gather albums
                album_indices = albums_map.get(track_id, [])
                albums = [results[i] for i in album_indices if not isinstance(results[i], Exception)]

                track['albums'] = albums
                track['_meta'] = {
                    'processed_at': datetime.utcnow().isoformat(),
                    'batch_number': effective_offset // self.batch_size,
                    'genre': genre
                }

                tracks_data.append(track)
            
            return tracks_data
            
        except Exception as e:
            logger.error(f"Error processing batch for genre {genre}: {e}")
            return []

    async def get_tracks_by_genre(self, config: BatchConfig) -> List[Dict]:
        """Extract tracks metadata for a specific genre using async batching with boundaries"""
        all_tracks = []
        offset = 0
        
        if config.start_track < 0 or config.end_track <= config.start_track:
            raise ValueError("Invalid track boundaries")
        
        tracks_to_process = config.end_track - config.start_track
        
        while len(all_tracks) < tracks_to_process:
            batch = await self.process_genre_batch(
                config.genre,
                offset,
                config.start_track,
                config.end_track
            )
            
            if not batch:
                logger.info(f"No more tracks found for genre {config.genre}")
                break
            
            all_tracks.extend(batch)
            offset += config.batch_size
            
            logger.info(
                f"Processed {len(all_tracks)}/{tracks_to_process} tracks "
                f"for genre {config.genre} (batch {offset // config.batch_size})"
            )
            
            await asyncio.sleep(1)
        
        return all_tracks

    def save_to_json(self, tracks: List[Dict], filename: Optional[str] = None) -> str:
        """Save processed tracks to a JSON file"""
        if filename is None:
            timestamp = datetime.now().str


async def main():
    processor = MusicBatchProcessor(
        app_name="JPOPMusicExtractor",
        version="1.0",
        contact="immacoolcat03@gmail.com",
        batch_size=25
    )
    
    config = BatchConfig(
        start_track=0,  # Start from the 200th track
        end_track=50,    # Process until 300th track
        batch_size=25,    # Process 25 tracks at a time
        genre="j-pop"      # Genre to search for
    )
    
    tracks = await processor.get_tracks_by_genre(config)

        # Save to JSON file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"musicbrainz_data_{timestamp}.json"
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(tracks, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Retrieved {len(tracks)} tracks")
    logger.info(f"Data saved to {filename}")

if __name__ == "__main__":
    asyncio.run(main())