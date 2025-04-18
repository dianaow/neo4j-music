# Music Catalog Extractor

A Python-based tool for extracting and processing music catalog data from MusicBrainz. This project focuses on gathering comprehensive information about artists, their releases, recordings, and associated metadata.

## Features

- Extracts artist information from MusicBrainz
- Processes artist releases and recordings
- Handles credits (producers, vocalists, arrangers, lyricists, composers)
- Deduplicates recordings and releases
- Stores data in SQLite database
- Supports batch processing with progress tracking
- Implements rate limiting and caching for API requests

## Prerequisites

- Python 3.8+
- MusicBrainz API access (optional, for authenticated requests)
- SQLite3

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd neo4j-music
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file with your MusicBrainz credentials (optional):
```
MUSICBRAINZ_USER=your_username
MUSICBRAINZ_PASSWORD=your_password
CONTACT_EMAIL=your_email
DB_PATH=path_to_database
```

## Usage

### Extracting Music Catalog 

To extract the complete music catalog:

```bash
python music.py --batch-size 100 --max-artists 1000
```

### Processing a Single Artist

To process a single artist by their MusicBrainz ID:

```bash
python music.py --artist-id <artist_id>
```

### Command Line Arguments

- `--batch-size`: Number of artists to process in each batch (default: 100)
- `--max-artists`: Maximum number of artists to process (default: None)
- `--start-offset`: Starting offset for artist search (default: None)
- `--artist-id`: Process a single artist by ID
- `--db-path`: Path to the SQLite database

## Project Structure

- `music.py`: Main script for extracting and processing music data
- `ingestdb.py`: Database operations and data ingestion
- `requirements.txt`: Project dependencies
- `.env`: Environment variables and configuration
- `musicbrainz.db`: SQLite database file
- `extraction_progress.json`: Progress tracking file

## Data Model

### SQLite Database Schema

The project uses SQLite for initial data storage with the following tables:

#### Artists Table
- `id` (TEXT, PRIMARY KEY): MusicBrainz artist ID
- `name` (TEXT): Artist name
- `type` (TEXT): Artist type (person, group, etc.)
- `disambiguation` (TEXT): Disambiguation text
- `gender` (TEXT): Artist gender
- `country` (TEXT): Country of origin
- `area` (TEXT): Geographic area
- `begin_area` (TEXT): Beginning area
- `dob` (TEXT): Date of birth
- `urls` (TEXT): JSON array of URLs
- `aliases` (TEXT): JSON array of aliases
- `processed_at` (TEXT): Timestamp of processing

#### Recordings Table
- `id` (TEXT, PRIMARY KEY): MusicBrainz recording ID
- `title` (TEXT): Recording title
- `length` (INTEGER): Duration in milliseconds
- `tags` (TEXT): JSON array of tags
- `credits` (TEXT): JSON object of credits
- `release_id` (TEXT): Associated release ID
- `artist_id` (TEXT): Associated artist ID
- `work_ids` (TEXT): JSON array of work IDs
- `urls` (TEXT): JSON array of URLs
- `processed_at` (TEXT): Timestamp of processing

#### Releases Table
- `id` (TEXT, PRIMARY KEY): MusicBrainz release ID
- `title` (TEXT): Release title
- `labels` (TEXT): JSON object of labels
- `track_count` (INTEGER): Number of tracks
- `format` (TEXT): Release format
- `country` (TEXT): Country of release
- `date` (TEXT): Release date
- `artist_id` (TEXT): Associated artist ID
- `primary_type` (TEXT): Primary release type
- `secondary_types` (TEXT): JSON array of secondary types
- `processed_at` (TEXT): Timestamp of processing

#### Contributors Table
- `id` (TEXT, PRIMARY KEY): MusicBrainz contributor ID
- `name` (TEXT): Contributor name
- `type` (TEXT): Contributor type
- `disambiguation` (TEXT): Disambiguation text
- `gender` (TEXT): Gender
- `country` (TEXT): Country
- `area` (TEXT): Geographic area
- `begin_area` (TEXT): Beginning area
- `dob` (TEXT): Date of birth
- `urls` (TEXT): JSON array of URLs
- `aliases` (TEXT): JSON array of aliases
- `processed_at` (TEXT): Timestamp of processing

### Neo4j Graph Database Schema

The data is then imported into Neo4j with the following node types and relationships:

#### Node Types
- **Artist**: Represents musical artists with properties like name, type, gender, etc.
- **Recording**: Represents individual recordings with properties like title, length, etc.
- **Release**: Represents music releases with properties like title, format, etc.
- **Contributor**: Represents people who contributed to recordings (producers, composers, etc.)
- **Label**: Represents record labels

#### Relationships
- `(Artist)-[:PERFORMED]->(Recording)`: Artist performed on recording
- `(Contributor)-[:COMPOSED]->(Recording)`: Contributor composed the recording
- `(Contributor)-[:WROTE_LYRICS]->(Recording)`: Contributor wrote lyrics for recording
- `(Contributor)-[:PRODUCED]->(Recording)`: Contributor produced the recording
- `(Contributor)-[:ARRANGED]->(Recording)`: Contributor arranged the recording
- `(Recording)-[:APPEARS_ON]->(Release)`: Recording appears on release
- `(Release)-[:RELEASED_BY]->(Label)`: Release was released by label

## Neo4j Ingestion

The project includes a Neo4j ingestion process that:

1. Connects to a Neo4j database using credentials from environment variables
2. Creates necessary constraints for unique node properties
3. Establishes relationships between nodes

### Environment Variables for Neo4j
```
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=your_username
NEO4J_PASSWORD=your_password
```

### Running the Ingestion
```bash
python ingestdb.py --db-path path_to_sqlite.db
```

## Features

- Rate limiting and caching for API requests
- Progress tracking and resumption
- Error handling and retry mechanisms
- Data deduplication
- Concurrent processing
- Memory optimization

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

- MusicBrainz for providing the music metadata
- All contributors and maintainers 