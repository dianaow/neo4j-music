from neo4j import GraphDatabase
import json
import logging
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Neo4jImporter:
    def __init__(self, uri, username, password):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        
    def close(self):
        self.driver.close()
        
    @staticmethod
    def _import_data(tx, json_data):
        # The query parameter must be named 'json' as it's referenced as $json in the Cypher query
        result = tx.run("""
        // First, create TRACK nodes
        UNWIND $json AS track
        MERGE (t:TRACK {
            id: track.id,
            title: track.title,
            length: track.length
        })
        WITH track, t

        // Create ALBUM nodes and relationships
        UNWIND track.albums AS album
        MERGE (a:ALBUM { title: album.title })  // Ensure uniqueness by title
        ON CREATE SET
            a.id = album.id,       // Store the first seen album ID
            a.date = album.date,
            a.country = album.country,
            a.label = album.label,
            a.format = album.format,
            a.track_count = album.track_count
        MERGE (t)-[:BELONGS_TO]->(a)

        // Handle performers
        WITH track, t, a
        UNWIND (CASE track.credits.performers WHEN [] THEN [null] ELSE track.credits.performers END) AS performer
        WITH track, t, a, performer
        WHERE performer IS NOT NULL
        MERGE (p:PERSON {id: performer.id})
        ON CREATE SET
            p.name = performer.name,
            p.type = performer.type,
            p.disambiguation = performer.disambiguation,
            p.gender = performer.gender,
            p.country = performer.country,
            p.area = performer.area,
            p.begin_area = performer.`begin-area`,
            p.dob = performer.DOB,
            p.category = 'performer'
        WITH track, t, a, p
        FOREACH(_ IN CASE WHEN a.title IS NULL THEN [1] ELSE [] END |
            MERGE (p)-[:PERFORMED]->(t)
        )
        FOREACH(_ IN CASE WHEN a.title IS NOT NULL THEN [1] ELSE [] END |
            MERGE (p)-[:PERFORMED]->(a)
        )

        // Handle arrangers
        WITH track, t, a
        UNWIND (CASE track.credits.arrangers WHEN [] THEN [null] ELSE track.credits.arrangers END) AS arranger
        WITH track, t, a, arranger
        WHERE arranger IS NOT NULL
        MERGE (p:PERSON {id: arranger.id})
        ON CREATE SET
            p.name = arranger.name,
            p.type = arranger.type,
            p.disambiguation = arranger.disambiguation,
            p.gender = arranger.gender,
            p.country = arranger.country,
            p.area = arranger.area,
            p.begin_area = arranger.`begin-area`,
            p.dob = arranger.DOB,
            p.category = 'arranger'
        MERGE (p)-[:ARRANGED]->(t)
                        
        // Handle songwriters
        WITH track, t, a
        UNWIND (CASE track.credits.songwriters WHEN [] THEN [null] ELSE track.credits.songwriters END) AS songwriter
        WITH track, t, a, songwriter
        WHERE songwriter IS NOT NULL
        MERGE (p:PERSON {id: songwriter.id})
        ON CREATE SET
            p.name = songwriter.name,
            p.type = songwriter.type,
            p.category = 'songwriter'
        MERGE (p)-[:WROTE]->(t)

        // Handle lyricists
        WITH track, t, a
        UNWIND (CASE track.credits.lyricists WHEN [] THEN [null] ELSE track.credits.lyricists END) AS lyricist
        WITH track, t, a, lyricist
        WHERE lyricist IS NOT NULL
        MERGE (p:PERSON {id: lyricist.id})
        ON CREATE SET
            p.name = lyricist.name,
            p.type = lyricist.type,
            p.category = 'lyricist'
        MERGE (p)-[:WROTE_LYRICS]->(t)

        // Handle producers
        WITH track, t, a
        UNWIND (CASE track.credits.producers WHEN [] THEN [null] ELSE track.credits.producers END) AS producer
        WITH track, t, a, producer
        WHERE producer IS NOT NULL
        MERGE (p:PERSON {id: producer.id})
        ON CREATE SET
            p.name = producer.name,
            p.type = producer.type,
            p.category = 'producer'
        MERGE (p)-[:PRODUCED]->(t)
        RETURN count(t) as tracks
        """, json=json_data)
        
        # Consume the result
        summary = result.consume()
        return summary
    
    @staticmethod
    def _verify_import(tx):
        """Verify that data was imported by counting nodes and relationships"""
        result = tx.run("""
        MATCH (n)
        RETURN labels(n) as label, count(*) as count
        UNION ALL
        MATCH ()-[r]->()
        RETURN type(r) as label, count(*) as count
        """)
        return list(result)

    def import_json_file(self, file_path):
        try:
            # Read JSON file
            with open(file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            logger.info(f"Successfully loaded JSON data from {file_path}")
            
            # Execute import in a transaction
            with self.driver.session() as session:
                result = session.execute_write(self._import_data, json_data)
                logger.info(f"Query completed with {result.counters}")
                
                # Verify the import
                counts = session.execute_read(self._verify_import)
                for count in counts:
                    logger.info(f"{count['label']}: {count['count']}")
                
            return True
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON file: {e}")
            raise
        except Exception as e:
            logger.error(f"Error during import: {e}")
            raise

def main():
    # Configuration
    NEO4J_URI = os.getenv("NEO4J_URI")
    NEO4J_USER = os.getenv("NEO4J_USER")               
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")    
    JSON_FILE_PATH = "musicbrainz_data_20250209_232616.json"  
    
    try:
        # Create importer instance
        importer = Neo4jImporter(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        
        # Import data
        importer.import_json_file(JSON_FILE_PATH)
        
        logger.info("Import completed successfully")
        
    except Exception as e:
        logger.error(f"Import failed: {e}")
        raise
        
    finally:
        # Always close the driver
        importer.close()

if __name__ == "__main__":
    main()

