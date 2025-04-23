#!/usr/bin/env python3
"""
OpenAlex Extraction Tool
------------------------
Importerar och bearbetar OpenAlex JSON-data till relationsdatabas
"""

import os
import sys
import json
import psycopg2
import argparse
from datetime import datetime
import re

# Databaskonfiguration - hämta från miljövariabler
DB_NAME = os.environ.get("BIBMET_DB", "bibmet")
DB_USER = os.environ.get("BIBMET_USER", "bibmetuser")
DB_HOST = os.environ.get("BIBMET_HOST", "localhost")
DB_PASSWORD = os.environ.get("PGPASSWORD", "")

def log_message(logfile, message):
    """Logga ett meddelande med tidsstämpel"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(logfile, 'a') as f:
        f.write(f"{timestamp} {message}\n")
    print(f"{timestamp} {message}")

def get_nested_value(data, *args, default=None):
    """
    Säker åtkomst till nästlade värden i dictionary.
    Exempel: get_nested_value(data, 'ids', 'doi')
    """
    current = data
    for arg in args:
        if isinstance(current, dict) and arg in current:
            current = current[arg]
        else:
            return default
    return current

def ensure_table_structure(conn, logfile):
    """Säkerställ att databastabellerna har rätt struktur"""
    log_message(logfile, "Säkerställer korrekt databasstruktur")
    
    # Här implementeras skapandet av alla tabeller som definierats ovan
    # Samma princip som i wos_python_extractor.py
    
    # Exempel:
    cur = conn.cursor()
    try:
        # ... SQL-kod för att skapa tabeller om de inte finns
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        log_message(logfile, f"Fel vid uppdatering av tabellstruktur: {str(e)}")
        return False

def extract_raw_json_records(conn, batch_size=100):
    """Hämta obearbetade poster från openalex.raw_json"""
    cur = conn.cursor()
    cur.execute("SELECT id, json_content FROM openalex.raw_json WHERE NOT processed ORDER BY id LIMIT %s", (batch_size,))
    records = cur.fetchall()
    return records

def mark_record_processed(conn, record_id):
    """Markera en post som bearbetad"""
    cur = conn.cursor()
    cur.execute("UPDATE openalex.raw_json SET processed = TRUE WHERE id = %s", (record_id,))
    conn.commit()

def extract_work_data(data):
    """Extrahera grundläggande artikelinformation från JSON-data"""
    # Exempel på extraktion från OpenAlex JSON-struktur
    work = {}
    
    # Basdata
    work['id'] = data.get('id')
    work['doi'] = data.get('doi')
    work['title'] = data.get('title')
    work['display_name'] = data.get('display_name')
    work['publication_year'] = data.get('publication_year')
    work['publication_date'] = data.get('publication_date')
    work['language'] = data.get('language')
    work['type'] = data.get('type')
    work['type_crossref'] = data.get('type_crossref')
    work['cited_by_count'] = data.get('cited_by_count')
    work['is_retracted'] = data.get('is_retracted')
    work['is_paratext'] = data.get('is_paratext')
    work['created_date'] = data.get('created_date')
    work['updated_date'] = data.get('updated_date')
    
    # Abstrakt (behöver byggas från inverterat index)
    if 'abstract_inverted_index' in data and data['abstract_inverted_index']:
        abstract = rebuild_abstract_from_inverted_index(data['abstract_inverted_index'])
        work['abstract'] = abstract
    else:
        work['abstract'] = None
    
    return work

def rebuild_abstract_from_inverted_index(inverted_index):
    """Återskapa abstrakt från inverterat index"""
    if not inverted_index:
        return None
    
    # Skapa en lista av (ord, position) tupler
    word_positions = []
    for word, positions in inverted_index.items():
        for pos in positions:
            word_positions.append((word, pos))
    
    # Sortera efter position
    word_positions.sort(key=lambda x: x[1])
    
    # Bygg abstrakt
    abstract = ' '.join(word for word, _ in word_positions)
    return abstract

def extract_ids(data):
    """Extrahera olika ID-typer"""
    ids = []
    if 'ids' in data and isinstance(data['ids'], dict):
        for id_type, id_value in data['ids'].items():
            ids.append({
                'id_type': id_type,
                'id_value': id_value
            })
    return ids

def extract_authors(data):
    """Extrahera författarinformation"""
    authors = []
    if 'authorships' in data and isinstance(data['authorships'], list):
        for authorship in data['authorships']:
            author = authorship.get('author', {})
            authors.append({
                'author_id': author.get('id'),
                'display_name': author.get('display_name'),
                'orcid': author.get('orcid'),
                'author_position': authorship.get('author_position'),
                'is_corresponding': authorship.get('is_corresponding', False),
                'raw_author_name': authorship.get('raw_author_name'),
                'raw_affiliation_strings': authorship.get('raw_affiliation_strings', []),
                'institutions': authorship.get('institutions', []),
                'countries': authorship.get('countries', [])
            })
    return authors

def extract_institutions(authors):
    """Extrahera unika institutioner från författarinformation"""
    institutions = {}
    for author in authors:
        for inst in author.get('institutions', []):
            if 'id' in inst and inst['id'] not in institutions:
                institutions[inst['id']] = {
                    'id': inst.get('id'),
                    'display_name': inst.get('display_name'),
                    'ror': inst.get('ror'),
                    'country_code': inst.get('country_code'),
                    'type': inst.get('type')
                }
    return list(institutions.values())

def extract_concepts(data):
    """Extrahera koncept/ämnesområden"""
    concepts = []
    if 'concepts' in data and isinstance(data['concepts'], list):
        for concept in data['concepts']:
            concepts.append({
                'id': concept.get('id'),
                'wikidata': concept.get('wikidata'),
                'display_name': concept.get('display_name'),
                'level': concept.get('level'),
                'score': concept.get('score')
            })
    return concepts

def extract_topics(data):
    """Extrahera topics/ämnesområden"""
    topics = []
    if 'topics' in data and isinstance(data['topics'], list):
        for topic in data['topics']:
            subfield = topic.get('subfield', {})
            field = topic.get('field', {})
            domain = topic.get('domain', {})
            
            topics.append({
                'id': topic.get('id'),
                'display_name': topic.get('display_name'),
                'score': topic.get('score'),
                'subfield_id': subfield.get('id'),
                'subfield_name': subfield.get('display_name'),
                'field_id': field.get('id'),
                'field_name': field.get('display_name'),
                'domain_id': domain.get('id'),
                'domain_name': domain.get('display_name')
            })
    return topics

def extract_locations(data):
    """Extrahera information om publikationsplatser"""
    locations = []
    
    # Primär publiceringsplats
    if 'primary_location' in data and data['primary_location']:
        primary = data['primary_location']
        source = primary.get('source', {})
        locations.append({
            'is_primary': True,
            'is_oa': primary.get('is_oa'),
            'landing_page_url': primary.get('landing_page_url'),
            'pdf_url': primary.get('pdf_url'),
            'source_id': source.get('id') if source else None,
            'license': primary.get('license'),
            'version': primary.get('version'),
            'is_accepted': primary.get('is_accepted'),
            'is_published': primary.get('is_published')
        })
    
    # Övriga platser
    if 'locations' in data and isinstance(data['locations'], list):
        for location in data['locations']:
            source = location.get('source', {})
            locations.append({
                'is_primary': False,
                'is_oa': location.get('is_oa'),
                'landing_page_url': location.get('landing_page_url'),
                'pdf_url': location.get('pdf_url'),
                'source_id': source.get('id') if source else None,
                'license': location.get('license'),
                'version': location.get('version'),
                'is_accepted': location.get('is_accepted'),
                'is_published': location.get('is_published')
            })
    
    return locations

def extract_sources(locations):
    """Extrahera unika källor från publikationsplatser"""
    sources = {}
    for location in locations:
        if location.get('source_id') and location['source_id'] not in sources:
            # Här extraheras källinformation från OpenAlex JSON
            # I ett verkligt scenario skulle du hämta mer data från platsinformationen
            sources[location['source_id']] = {
                'id': location['source_id']
                # Andra fält skulle läggas till här
            }
    return list(sources.values())

def extract_references(data):
    """Extrahera referenser"""
    references = []
    if 'referenced_works' in data and isinstance(data['referenced_works'], list):
        for ref_work_id in data['referenced_works']:
            references.append(ref_work_id)
    return references

def extract_grants(data):
    """Extrahera bidragsinformation"""
    grants = []
    if 'grants' in data and isinstance(data['grants'], list):
        for grant in data['grants']:
            grants.append({
                'funder': grant.get('funder'),
                'funder_display_name': grant.get('funder_display_name'),
                'award_id': grant.get('award_id')
            })
    return grants

def extract_keywords(data):
    """Extrahera nyckelord"""
    keywords = []
    if 'keywords' in data and isinstance(data['keywords'], list):
        for keyword in data['keywords']:
            keywords.append({
                'id': keyword.get('id'),
                'display_name': keyword.get('display_name'),
                'score': keyword.get('score')
            })
    return keywords

def extract_sdgs(data):
    """Extrahera hållbarhetsmål (SDGs)"""
    sdgs = []
    if 'sustainable_development_goals' in data and isinstance(data['sustainable_development_goals'], list):
        for sdg in data['sustainable_development_goals']:
            sdgs.append({
                'id': sdg.get('id'),
                'display_name': sdg.get('display_name'),
                'score': sdg.get('score')
            })
    return sdgs

def insert_work(conn, work, logfile):
    """Infoga artikeldata i databasen"""
    cur = conn.cursor()
    
    try:
        # Förbered parametrar
        columns = []
        values = []
        placeholders = []
        
        for key, value in work.items():
            if value is not None:
                columns.append(key)
                values.append(value)
                placeholders.append('%s')
        
        # Bygg SQL-fråga
        sql = f"""
            INSERT INTO openalex.works ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON CONFLICT (id) DO UPDATE SET
            {', '.join([f"{col} = EXCLUDED.{col}" for col in columns])}
        """
        
        cur.execute(sql, values)
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        log_message(logfile, f"Fel vid inmatning av verk {work.get('id')}: {str(e)}")
        return False

# Liknande funktioner för att infoga andra entiteter som författare, institutioner, etc.
# ...

def process_json_records(conn, logfile, batch_size=100):
    """Bearbeta JSON-poster från databasen"""
    log_message(logfile, f"Påbörjar bearbetning med batch_size={batch_size}")
    total_processed = 0
    total_success = 0
    
    while True:
        records = extract_raw_json_records(conn, batch_size)
        if not records:
            break
        
        log_message(logfile, f"Bearbetar batch med {len(records)} poster")
        batch_success = 0
        
        for record_id, json_content in records:
            try:
                # Kontrollera att json_content är en sträng
                if json_content is None:
                    log_message(logfile, f"Null JSON-innehåll i post {record_id}")
                    mark_record_processed(conn, record_id)
                    continue
                
                # Parse JSON-innehåll
                try:
                    data = json.loads(json_content)
                except json.JSONDecodeError as json_err:
                    log_message(logfile, f"JSON-avkodningsfel i post {record_id}: {str(json_err)}")
                    mark_record_processed(conn, record_id)
                    continue
                
                # Hantera både enskilda poster och listor
                if isinstance(data, list):
                    items = data
                else:
                    items = [data]
                
                for item in items:
                    # Extrahera data från OpenAlex JSON
                    work = extract_work_data(item)
                    ids = extract_ids(item)
                    authors = extract_authors(item)
                    institutions = extract_institutions(authors)
                    concepts = extract_concepts(item)
                    topics = extract_topics(item)
                    locations = extract_locations(item)
                    sources = extract_sources(locations)
                    references = extract_references(item)
                    grants = extract_grants(item)
                    keywords = extract_keywords(item)
                    sdgs = extract_sdgs(item)
                    
                    # Infoga i databasen
                    if insert_work(conn, work, logfile):
                        # Infoga relaterad data (författare, institutioner, etc.)
                        # ...
                        
                        batch_success += 1
                        total_success += 1
                
                # Markera post som bearbetad
                mark_record_processed(conn, record_id)
                total_processed += 1
                
                if total_processed % 100 == 0:
                    log_message(logfile, f"Bearbetat {total_processed} poster, framgångsfrekvens: {(total_success/total_processed)*100:.1f}%")
                
            except Exception as e:
                log_message(logfile, f"Fel vid bearbetning av post {record_id}: {str(e)}")
                # Markera posten som bearbetad ändå för att undvika oändlig loop
                try:
                    mark_record_processed(conn, record_id)
                except:
                    log_message(logfile, f"Misslyckades att markera post {record_id} som bearbetad")
                total_processed += 1
        
        log_message(logfile, f"Batch slutförd: {batch_success}/{len(records)} poster bearbetades framgångsrikt")
    
    return total_processed, total_success

def main():
    """Huvudfunktion"""
    parser = argparse.ArgumentParser(description='Bearbeta OpenAlex JSON-data och importera till databas')
    parser.add_argument('--logfile', default=None, help='Sökväg till loggfil')
    parser.add_argument('--clean', action='store_true', help='Rensa databastabeller före bearbetning')
    parser.add_argument('--batch-size', type=int, default=100, help='Batchstorlek för bearbetning av poster')
    parser.add_argument('--match', default=None, help='Sökväg till matchningsskript')
    parser.add_argument('--no-match', action='store_true', help='Hoppa över matchningsskript')
    parser.add_argument('--only-match', action='store_true', help='Kör endast matchningsskript, hoppa över extraktion')
    
    args = parser.parse_args()
    
    # Standardvärden
    ROOT_DIR = os.environ.get('ROOT_DIR', os.path.expanduser('~/bibmet-utils'))
    
    if args.logfile is None:
        log_dir = os.path.join(ROOT_DIR, 'prepare_openalex/current/log')
        os.makedirs(log_dir, exist_ok=True)
        logfile = os.path.join(log_dir, 'openalex-extractor.log')
    else:
        logfile = args.logfile
    
    if args.match is None:
        match_script = os.path.join(ROOT_DIR, 'scripts/subscripts/openalex_matching_script.sql')
    else:
        match_script = args.match
    
    # Starta loggning
    log_message(logfile, "=================================================")
    log_message(logfile, "Startar OpenAlex JSON Extractor")
    
    try:
        # Anslut till databasen
        log_message(logfile, "Ansluter till databas...")
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            host=DB_HOST,
            password=DB_PASSWORD
        )
        
        # Säkerställ databastabellernas struktur
        ensure_table_structure(conn, logfile)
        
        # Rensa databastabellerna om begärt
        if args.clean:
            # Implementera rensning här
            pass
        
        # Bearbeta JSON-poster om inte i match-only-läge
        if not args.only_match:
            log_message(logfile, "Bearbetar JSON-poster...")
            total_processed, total_success = process_json_records(conn, logfile, args.batch_size)
            log_message(logfile, f"Slutförd bearbetning: {total_success}/{total_processed} poster bearbetades framgångsrikt")
        else:
            log_message(logfile, "Hoppar över databearbetning (--only-match-läge)")
        
        # Kör matchningsskript om inte inaktiverat
        if not args.no_match and os.path.exists(match_script):
            # Implementera matchning här
            pass
        
        # Visa statistik
        # ...
        
        # Stäng anslutning
        conn.close()
        
        log_message(logfile, "OpenAlex JSON Extractor slutfördes framgångsrikt")
        
    except Exception as e:
        log_message(logfile, f"Kritiskt fel: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
