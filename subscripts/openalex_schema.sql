CREATE SCHEMA IF NOT EXISTS openalex;

-- Tabell för lagring av rå JSON-data
CREATE TABLE IF NOT EXISTS openalex.raw_json (
    id SERIAL PRIMARY KEY,
    filename VARCHAR(255),
    json_content TEXT,
    processed BOOLEAN DEFAULT FALSE,
    import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Huvudtabell för artiklar/verk
CREATE TABLE IF NOT EXISTS openalex.works (
    id VARCHAR(255) PRIMARY KEY,  -- OpenAlex ID (t.ex. "https://openalex.org/W2900756811")
    doi VARCHAR(255),             -- DOI
    title TEXT,                   -- Artikelns titel
    display_name TEXT,            -- Visningsnamn (kan skilja sig från titel)
    publication_year INTEGER,     -- Publiceringsår
    publication_date DATE,        -- Publiceringsdatum
    language VARCHAR(50),         -- Språk
    type VARCHAR(100),            -- Typ (t.ex. "article")
    type_crossref VARCHAR(100),   -- Crossref-typ
    abstract TEXT,                -- Abstrakt
    cited_by_count INTEGER,       -- Antal citeringar
    is_retracted BOOLEAN,
    is_paratext BOOLEAN,
    created_date DATE,
    updated_date TIMESTAMP
);

-- Tabell för IDs
CREATE TABLE IF NOT EXISTS openalex.work_ids (
    work_id VARCHAR(255) REFERENCES openalex.works(id),
    id_type VARCHAR(100),         -- t.ex. "openalex", "doi", "pmid"
    id_value VARCHAR(255),
    PRIMARY KEY (work_id, id_type)
);

-- Tabell för författare
CREATE TABLE IF NOT EXISTS openalex.authors (
    id VARCHAR(255) PRIMARY KEY,
    display_name VARCHAR(255),
    orcid VARCHAR(255)
);

-- Kopplingstabell mellan verk och författare
CREATE TABLE IF NOT EXISTS openalex.work_authors (
    id SERIAL PRIMARY KEY,
    work_id VARCHAR(255) REFERENCES openalex.works(id),
    author_id VARCHAR(255) REFERENCES openalex.authors(id),
    author_position VARCHAR(50),  -- "first", "middle", "last"
    is_corresponding BOOLEAN DEFAULT FALSE,
    raw_author_name TEXT,
    raw_affiliation_strings TEXT[]
);

-- Tabell för institutioner
CREATE TABLE IF NOT EXISTS openalex.institutions (
    id VARCHAR(255) PRIMARY KEY,
    display_name TEXT,
    ror VARCHAR(255),
    country_code VARCHAR(10),
    type VARCHAR(100)
);

-- Kopplingstabell mellan författare och institutioner i en publikation
CREATE TABLE IF NOT EXISTS openalex.author_institutions (
    work_id VARCHAR(255) REFERENCES openalex.works(id),
    author_id VARCHAR(255) REFERENCES openalex.authors(id),
    institution_id VARCHAR(255) REFERENCES openalex.institutions(id),
    PRIMARY KEY (work_id, author_id, institution_id)
);

-- Tabell för geografiska platser (länder)
CREATE TABLE IF NOT EXISTS openalex.countries (
    country_code VARCHAR(10) PRIMARY KEY,
    country_name VARCHAR(255)
);

-- Tabell för koncept
CREATE TABLE IF NOT EXISTS openalex.concepts (
    id VARCHAR(255) PRIMARY KEY,
    wikidata VARCHAR(255),
    display_name TEXT,
    level INTEGER,
    description TEXT
);

-- Kopplingstabell mellan verk och koncept
CREATE TABLE IF NOT EXISTS openalex.work_concepts (
    work_id VARCHAR(255) REFERENCES openalex.works(id),
    concept_id VARCHAR(255) REFERENCES openalex.concepts(id),
    score FLOAT,
    PRIMARY KEY (work_id, concept_id)
);

-- Tabell för tidskrifter/källor
CREATE TABLE IF NOT EXISTS openalex.sources (
    id VARCHAR(255) PRIMARY KEY,
    display_name TEXT,
    issn_l VARCHAR(50),
    issn TEXT[],
    is_oa BOOLEAN,
    is_in_doaj BOOLEAN,
    type VARCHAR(100)
);

-- Information om primär placering
CREATE TABLE IF NOT EXISTS openalex.locations (
    id SERIAL PRIMARY KEY,
    work_id VARCHAR(255) REFERENCES openalex.works(id),
    source_id VARCHAR(255) REFERENCES openalex.sources(id),
    is_oa BOOLEAN,
    landing_page_url TEXT,
    pdf_url TEXT,
    license VARCHAR(100),
    version VARCHAR(100),
    is_accepted BOOLEAN,
    is_published BOOLEAN
);

-- Tabell för ämnesområden/topics
CREATE TABLE IF NOT EXISTS openalex.topics (
    id VARCHAR(255) PRIMARY KEY,
    display_name TEXT,
    subfield_id VARCHAR(255),
    subfield_name TEXT,
    field_id VARCHAR(255),
    field_name TEXT,
    domain_id VARCHAR(255),
    domain_name TEXT
);

-- Kopplingstabell mellan verk och ämnesområden
CREATE TABLE IF NOT EXISTS openalex.work_topics (
    work_id VARCHAR(255) REFERENCES openalex.works(id),
    topic_id VARCHAR(255) REFERENCES openalex.topics(id),
    score FLOAT,
    PRIMARY KEY (work_id, topic_id)
);

-- Referenser
CREATE TABLE IF NOT EXISTS openalex.referenced_works (
    work_id VARCHAR(255) REFERENCES openalex.works(id),
    referenced_work_id VARCHAR(255),
    PRIMARY KEY (work_id, referenced_work_id)
);

-- Bidrag (grants)
CREATE TABLE IF NOT EXISTS openalex.grants (
    id SERIAL PRIMARY KEY,
    work_id VARCHAR(255) REFERENCES openalex.works(id),
    funder VARCHAR(255),
    funder_display_name TEXT,
    award_id VARCHAR(255)
);

-- Keywords/nyckelord
CREATE TABLE IF NOT EXISTS openalex.keywords (
    id VARCHAR(255) PRIMARY KEY,
    display_name TEXT
);

CREATE TABLE IF NOT EXISTS openalex.work_keywords (
    work_id VARCHAR(255) REFERENCES openalex.works(id),
    keyword_id VARCHAR(255) REFERENCES openalex.keywords(id),
    score FLOAT,
    PRIMARY KEY (work_id, keyword_id)
);

-- Hållbarhetsmål (SDGs)
CREATE TABLE IF NOT EXISTS openalex.sdgs (
    id VARCHAR(255) PRIMARY KEY,
    display_name TEXT
);

CREATE TABLE IF NOT EXISTS openalex.work_sdgs (
    work_id VARCHAR(255) REFERENCES openalex.works(id),
    sdg_id VARCHAR(255) REFERENCES openalex.sdgs(id),
    score FLOAT,
    PRIMARY KEY (work_id, sdg_id)
);
