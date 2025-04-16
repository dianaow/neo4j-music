import sqlite3
import json
import os
import logging
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
from neo4j import GraphDatabase
import time
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class MusicDataIngester:
    def __init__(self, sqlite_db_path: str):
        """
        Initialize the ingester with SQLite database path
        
        Args:
            sqlite_db_path: Path to the SQLite database
        """
        self.sqlite_db_path = sqlite_db_path
        self.neo4j_uri = os.getenv("NEO4J_URI")
        self.neo4j_user = os.getenv("NEO4J_USER")
        self.neo4j_password = os.getenv("NEO4J_PASSWORD")
        
        # Neo4j driver will be initialized in connect_to_neo4j
        self.driver = None
        
        # Cache for nodes to avoid duplicates
        self.node_cache = {
            "Artist": {},
            "Contributor": {},
            "Release": {},
            "Recording": {},
            "Label": {}
        }
        
        # Batch processing settings
        self.batch_size = 1000
        self.current_batch = 0
        
    def connect_to_neo4j(self):
        """Connect to Neo4j database"""
        try:
            self.driver = GraphDatabase.driver(
                self.neo4j_uri, 
                auth=(self.neo4j_user, self.neo4j_password)
            )
            logger.info("Connected to Neo4j database")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            raise
    
    def close_neo4j_connection(self):
        """Close Neo4j connection"""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")
    
    def parse_json_field(self, json_str: str) -> Any:
        """
        Parse a JSON string into a Python object
        
        Args:
            json_str: JSON string to parse
            
        Returns:
            Parsed Python object or None if parsing fails
        """
        if not json_str:
            return None
        
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}")
            return None
    
    def get_sqlite_connection(self):
        """Get a connection to the SQLite database"""
        try:
            conn = sqlite3.connect(self.sqlite_db_path)
            conn.row_factory = sqlite3.Row  # This enables column access by name
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to SQLite database: {e}")
            raise
    
    def extract_artists(self) -> List[Dict]:
        """
        Extract artists from SQLite database
        
        Returns:
            List of artist dictionaries
        """
        conn = self.get_sqlite_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT * FROM artists")
            rows = cursor.fetchall()
            
            artists = []
            for row in rows:
                artist = dict(row)
                
                # Parse JSON fields
                artist['urls'] = self.parse_json_field(artist.get('urls'))
                artist['aliases'] = self.parse_json_field(artist.get('aliases'))
                
                artists.append(artist)
            
            logger.info(f"Extracted {len(artists)} artists from SQLite")
            return artists
        finally:
            conn.close()
    
    def extract_recordings(self) -> List[Dict]:
        """
        Extract recordings from SQLite database
        
        Returns:
            List of recording dictionaries
        """
        conn = self.get_sqlite_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT * FROM recordings")
            rows = cursor.fetchall()
            
            recordings = []
            for row in rows:
                recording = dict(row)
                
                # Parse JSON fields
                recording['tags'] = self.parse_json_field(recording.get('tags'))
                recording['credits'] = self.parse_json_field(recording.get('credits'))
                recording['work_ids'] = self.parse_json_field(recording.get('work_ids'))
                recording['release_id'] = self.parse_json_field(recording.get('release_id'))

                recordings.append(recording)
            
            logger.info(f"Extracted {len(recordings)} recordings from SQLite")
            return recordings
        finally:
            conn.close()

    def extract_releases(self) -> List[Dict]:
        """
        Extract releases from SQLite database
        
        Returns:
            List of release dictionaries
        """
        conn = self.get_sqlite_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT * FROM releases")
            rows = cursor.fetchall()
            
            releases = []
            for row in rows:
                release = dict(row)
                
                # Parse JSON fields
                release['labels'] = self.parse_json_field(release.get('labels'))
                release['secondary_types'] = self.parse_json_field(release.get('secondary_types'))

                releases.append(release)
            
            logger.info(f"Extracted {len(releases)} releases from SQLite")
            return releases
        finally:
            conn.close()
    
    def extract_contributors(self) -> List[Dict]:
        """
        Extract contributors from SQLite database
        
        Returns:
            List of contributor dictionaries
        """
        conn = self.get_sqlite_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT * FROM contributors")
            rows = cursor.fetchall()
            
            contributors = []
            for row in rows:
                contributor = dict(row)
                
                # Parse JSON fields
                contributor['urls'] = self.parse_json_field(contributor.get('urls'))
                contributor['aliases'] = self.parse_json_field(contributor.get('aliases'))
    
                contributors.append(contributor)
            
            logger.info(f"Extracted {len(contributors)} contributors from SQLite")
            return contributors
        finally:
            conn.close()
    
    def create_artist_node(self, tx, artist: Dict):
        """
        Create an Artist node in Neo4j
        
        Args:
            tx: Neo4j transaction
            artist: Artist data dictionary
        """
        # Check if artist already exists in cache
        if artist['id'] in self.node_cache["Artist"]:
            return
        
        # Create Artist node
        query = """
        MERGE (a:Artist {id: $id})
        SET a.name = $name,
            a.type = $type,
            a.disambiguation = $disambiguation,
            a.gender = $gender,
            a.country = $country,
            a.area = $area,
            a.begin_area = $begin_area,
            a.dob = $dob
        """
        
        # Add to cache
        self.node_cache["Artist"][artist['id']] = True
        
        # Execute query
        tx.run(query, 
               id=artist['id'],
               name=artist.get('name', ''),
               type=artist.get('type', ''),
               disambiguation=artist.get('disambiguation', ''),
               gender=artist.get('gender', ''),
               country=artist.get('country', ''),
               area=artist.get('area', ''),
               begin_area=artist.get('begin-area', ''),
               dob=artist.get('DOB', ''))
    
    def create_recording_node(self, tx, recording: Dict):
        """
        Create a Recording node in Neo4j
        
        Args:
            tx: Neo4j transaction
            recording: Recording data dictionary
        """
        # Check if recording already exists in cache
        if recording['id'] in self.node_cache["Recording"]:
            return
        
        # Create Recording node
        query = """
        MERGE (r:Recording {id: $id})
        SET r.title = $title,
            r.length = $length
        """
        
        # Add to cache
        self.node_cache["Recording"][recording['id']] = True
        
        # Execute query
        tx.run(query, 
               id=recording['id'],
               title=recording.get('title', ''),
               length=recording.get('length', 0))
    
    def create_release_node(self, tx, release: Dict):
        """
        Create a Release node in Neo4j
        
        Args:
            tx: Neo4j transaction
            release: Release data dictionary
        """
        # Check if release already exists in cache
        if release['id'] in self.node_cache["Release"]:
            return
        
        # Create Release node
        query = """
        MERGE (r:Release {id: $id})
        SET r.title = $title
        SET r.artist_id = $artist_id
        SET r.track_count = $track_count
        SET r.format = $format
        SET r.country = $country
        SET r.date = $date
        """
        
        # Add to cache
        self.node_cache["Release"][release['id']] = True
        
        # Execute query
        tx.run(query, 
               id=release['id'],
               title=release.get('title', ''),
               artist_id=release.get('artist_id', ''),
               track_count=release.get('track_count', ''),
               format=release.get('format', ''),
               country=release.get('country', ''),
               date=release.get('date', ''))
    
    def create_label_node(self, tx, label: Dict):
        """
        Create a Label node in Neo4j
        
        Args:
            tx: Neo4j transaction
            label: Label data dictionary
        """
        # Check if label already exists in cache
        if label['id'] in self.node_cache["Label"]:
            return
        
        # Create Label node
        query = """
        MERGE (l:Label {id: $id})
        SET l.name = $name,
            l.disambiguation = $disambiguation,
            l.label_code = $label_code,
            l.type = $type,
            l.country = $country
        """
        
        # Add to cache
        self.node_cache["Label"][label['id']] = True
        
        # Execute query
        tx.run(query, 
               id=label['id'],
               name=label.get('name', ''),
               disambiguation=label.get('disambiguation', ''),
               label_code=label.get('label_code', ''),
               type=label.get('type', ''),
               country=label.get('country', ''))
    
    def create_contributor_node(self, tx, contributor: Dict):
        """
        Create a Contributor node in Neo4j
        
        Args:
            tx: Neo4j transaction
            contributor_id: Contributor ID
            contributor_name: Contributor name (optional)
        """
        # Check if contributor already exists in cache
        if contributor['id'] in self.node_cache["Contributor"]:
            return
        
        # Create Contributor node
        query = """
        MERGE (c:Contributor {id: $id})
        SET c.name = $name,
            c.type = $type,
            c.disambiguation = $disambiguation,
            c.gender = $gender,
            c.country = $country,
            c.area = $area,
            c.begin_area = $begin_area,
            c.dob = $dob
        """
        
        # Add to cache
        self.node_cache["Contributor"][contributor['id']] = True
        
        # Execute query
        tx.run(query, id=contributor['id'], 
               name=contributor.get('name', ''), 
               type=contributor.get('type', ''), 
               disambiguation=contributor.get('disambiguation', ''), 
               gender=contributor.get('gender', ''), 
               country=contributor.get('country', ''), 
               area=contributor.get('area', ''), 
               begin_area=contributor.get('begin-area', ''), 
               dob=contributor.get('DOB', ''))
      
    def create_performed_relationship(self, tx, artist_id: str, recording_id: str):
        """
        Create a PERFORMED relationship between Artist and Recording
        
        Args:
            tx: Neo4j transaction
            artist_id: Artist ID
            recording_id: Recording ID
        """
        query = """
        MATCH (a:Contributor {id: $artist_id})
        MATCH (r:Recording {id: $recording_id})
        MERGE (a)-[:PERFORMED]->(r)
        """
        
        tx.run(query, artist_id=artist_id, recording_id=recording_id)
    
    def create_composed_relationship(self, tx, contributor_id: str, recording_id: str):
        """
        Create a COMPOSED relationship between Contributor and Recording
        
        Args:
            tx: Neo4j transaction
            contributor_id: Contributor ID
            recording_id: Recording ID
        """
        query = """
        MATCH (c:Contributor {id: $contributor_id})
        MATCH (r:Recording {id: $recording_id})
        MERGE (c)-[:COMPOSED]->(r)
        """
        
        tx.run(query, contributor_id=contributor_id, recording_id=recording_id)
    
    def create_wrote_lyrics_relationship(self, tx, contributor_id: str, recording_id: str):
        """
        Create a WROTE_LYRICS relationship between Contributor and Recording
        
        Args:
            tx: Neo4j transaction
            contributor_id: Contributor ID
            recording_id: Recording ID
        """
        query = """
        MATCH (c:Contributor {id: $contributor_id})
        MATCH (r:Recording {id: $recording_id})
        MERGE (c)-[:WROTE_LYRICS]->(r)
        """
        
        tx.run(query, contributor_id=contributor_id, recording_id=recording_id)
    
    def create_produced_relationship(self, tx, contributor_id: str, recording_id: str):
        """
        Create a PRODUCED relationship between Contributor and Recording
        
        Args:
            tx: Neo4j transaction
            contributor_id: Contributor ID
            recording_id: Recording ID
        """
        query = """
        MATCH (c:Contributor {id: $contributor_id})
        MATCH (r:Recording {id: $recording_id})
        MERGE (c)-[:PRODUCED]->(r)
        """
        
        tx.run(query, contributor_id=contributor_id, recording_id=recording_id)
    
    def create_arranged_relationship(self, tx, contributor_id: str, recording_id: str):
        """
        Create an ARRANGED relationship between Contributor and Recording
        
        Args:
            tx: Neo4j transaction
            contributor_id: Contributor ID
            recording_id: Recording ID
        """
        query = """
        MATCH (c:Contributor {id: $contributor_id})
        MATCH (r:Recording {id: $recording_id})
        MERGE (c)-[:ARRANGED]->(r)
        """
        
        tx.run(query, contributor_id=contributor_id, recording_id=recording_id)
    
    def create_appears_on_relationship(self, tx, recording_id: str, release_id: str):
        """
        Create an APPEARS_ON relationship between Recording and Release
        
        Args:
            tx: Neo4j transaction
            recording_id: Recording ID
            release_id: Release ID
        """
        query = """
        MATCH (r:Recording {id: $recording_id})
        MATCH (rel:Release {id: $release_id})
        MERGE (r)-[:APPEARS_ON]->(rel)
        """
        
        tx.run(query, recording_id=recording_id, release_id=release_id)
    
    def create_released_by_relationship(self, tx, release_id: str, label_id: str):
        """
        Create a RELEASED_BY relationship between Release and Label
        
        Args:
            tx: Neo4j transaction
            release_id: Release ID
            label_id: Label ID
        """
        query = """
        MATCH (rel:Release {id: $release_id})
        MATCH (l:Label {id: $label_id})
        MERGE (rel)-[:RELEASED_BY]->(l)
        """
        
        tx.run(query, release_id=release_id, label_id=label_id)
    
    def create_artist_release_relationship(self, tx, artist_id: str, release_id: str):
        """
        Create a RELEASED relationship between Artist and Release
        
        Args:
            tx: Neo4j transaction
            artist_id: Artist ID
            release_id: Release ID
        """
        query = """
        MATCH (a:Artist {id: $artist_id})
        MATCH (r:Release {id: $release_id})
        MERGE (a)-[:RELEASED]->(r)
        """
        
        tx.run(query, artist_id=artist_id, release_id=release_id)
    
    def process_recordings(self, recordings: List[Dict]):
        """
        Process recordings and create nodes and relationships
        
        Args:
            recordings: List of recording dictionaries
        """
        with self.driver.session() as session:
            for recording in recordings:
                recording_id = recording.get('id')
                release_id = recording.get('release_id')

                # Create Recording node
                session.execute_write(self.create_recording_node, recording)
                
                # Process credits
                credits = recording.get('credits', {})
                if not credits:
                    continue

                # Process performers (Artists)
                performers = credits.get('performer', [])
                for performer_id in performers:
                    # Create PERFORMED relationship
                    session.execute_write(self.create_performed_relationship, performer_id, recording_id)

                # Process composers (Contributors)
                composers = credits.get('composer', [])
                for composer_id in composers:
                    # Create COMPOSED relationship
                    session.execute_write(self.create_composed_relationship, composer_id, recording_id)

                # Process lyricists (Contributors)
                lyricists = credits.get('lyricist', [])
                for lyricist_id in lyricists:
                    # Create WROTE_LYRICS relationship
                    session.execute_write(self.create_wrote_lyrics_relationship, lyricist_id, recording_id)

                # Process producers (Contributors)
                producers = credits.get('producer', [])
                for producer_id in producers:
                    # Create PRODUCED relationship
                    session.execute_write(self.create_produced_relationship, producer_id, recording_id)

                # Process arrangers (Contributors)
                arrangers = credits.get('arranger', [])
                for arranger_id in arrangers:
                    # Create ARRANGED relationship
                    session.execute_write(self.create_arranged_relationship, arranger_id, recording_id)
                
                # Increment batch counter
                self.current_batch += 1
                if self.current_batch % self.batch_size == 0:
                    logger.info(f"Processed {self.current_batch} recordings")
    
    def process_releases(self, releases: List[Dict]):
        """
        Process releases and create nodes and relationships
        
        Args:
            releases: List of release dictionaries
        """
        with self.driver.session() as session:
            for release in releases:
                release_id = release['id']
                artist_id = release.get('artist_id')
                
                # Create Release node
                session.execute_write(self.create_release_node, release)
                
                # # Create artist-release relationship if artist_id exists
                # if artist_id:
                #     session.execute_write(self.create_artist_release_relationship, artist_id, release_id)
                
                # Process labels
                labels = release.get('labels', [])
                if not labels:
                    continue
                
                # Create Label nodes and RELEASED_BY relationships
                for label in labels:
                    label_id = label.get('id')
                    if label_id:
                        # Create Label node
                        session.execute_write(self.create_label_node, label)
                        # Create RELEASED_BY relationship
                        session.execute_write(self.create_released_by_relationship, release_id, label_id)
                
                # Increment batch counter
                self.current_batch += 1
                if self.current_batch % self.batch_size == 0:
                    logger.info(f"Processed {self.current_batch} releases")
    
    def process_artists(self, artists: List[Dict]):
        """
        Process artists and create nodes
        
        Args:
            artists: List of artist dictionaries
        """
        with self.driver.session() as session:
            for artist in artists:
                # Create Artist node
                session.execute_write(self.create_artist_node, artist)
                
                # Increment batch counter
                self.current_batch += 1
                if self.current_batch % self.batch_size == 0:
                    logger.info(f"Processed {self.current_batch} artists")
    
    def process_labels(self, labels: List[Dict]):
        """
        Process labels and create nodes
        
        Args:
            labels: List of label dictionaries
        """
        with self.driver.session() as session:
            for label in labels:
                # Create Label node
                session.execute_write(self.create_label_node, label)
                
                # Increment batch counter
                self.current_batch += 1
                if self.current_batch % self.batch_size == 0:
                    logger.info(f"Processed {self.current_batch} labels")
    
    def process_contributors(self, contributors: List[Dict]):
        """
        Process contributors and create nodes and relationships
        
        Args:
            contributors: List of contributor dictionaries
        """
        with self.driver.session() as session:
            for contributor in contributors:

                # Create Contributor node
                session.execute_write(self.create_contributor_node, contributor)
                
                # Increment batch counter
                self.current_batch += 1
                if self.current_batch % self.batch_size == 0:
                    logger.info(f"Processed {self.current_batch} contributors")
    
    def ingest_data(self):
        """Main method to ingest data from SQLite to Neo4j"""
        try:
            # Connect to Neo4j
            self.connect_to_neo4j()
            
            # Extract data from SQLite
            artists = self.extract_artists()
            recordings = self.extract_recordings()
            releases = self.extract_releases()
            contributors = self.extract_contributors()

            # Process data in Neo4j
            logger.info("Processing artists...")
            #self.process_artists(artists)
                
            logger.info("Processing contributors...")
            self.process_contributors(contributors)

            # Process releases first to ensure all Release nodes exist
            logger.info("Processing releases...")
            self.process_releases(releases)

            # Then process recordings and their relationships
            logger.info("Processing recordings...")
            self.process_recordings(recordings)
            
            # Create APPEARS_ON relationships for all release_ids
            logger.info("Creating APPEARS_ON relationships...")
            with self.driver.session() as session:
                for recording in recordings:
                    recording_id = recording['id']
                    release_ids = recording.get('release_id', [])
                    print(recording_id, release_ids)
                    for release_id in release_ids:
                        if release_id:
                            session.execute_write(self.create_appears_on_relationship, recording_id, release_id)

            logger.info("Data ingestion completed successfully")
            
        except Exception as e:
            logger.error(f"Error during data ingestion: {e}")
            raise
        finally:
            # Close Neo4j connection
            self.close_neo4j_connection()

def main():
    """Main function to run the data ingestion"""
    # Get SQLite database path from command line or use default
    import argparse
    parser = argparse.ArgumentParser(description='Ingest music data from SQLite to Neo4j')
    parser.add_argument('--db', type=str, default='musicbrainz.db', help='Path to SQLite database')
    args = parser.parse_args()
    
    # Create ingester and run ingestion
    ingester = MusicDataIngester(args.db)
    ingester.ingest_data()

if __name__ == "__main__":
    main() 