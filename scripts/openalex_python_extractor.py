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
                'id': location['source_id'],
                'display_name': None,  # Behöver källinformation från ursprungliga datan
                'issn_l': None,
                'type': None
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

def insert_work_ids(conn, work_id, ids, logfile):
    """Infoga ID-relaterad data"""
    cur = conn.cursor()
    
    try:
        for id_item in ids:
            cur.execute("""
                INSERT INTO openalex.work_ids (work_id, id_type, id_value)
                VALUES (%s, %s, %s)
                ON CONFLICT (work_id, id_type) DO UPDATE SET
                id_value = EXCLUDED.id_value
            """, (work_id, id_item['id_type'], id_item['id_value']))
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        log_message(logfile, f"Fel vid inmatning av IDs för verk {work_id}: {str(e)}")
        return False

def insert_author(conn, author, logfile):
    """Infoga författardata och returnera ID"""
    cur = conn.cursor()
    
    try:
        # Kontrollera om författaren redan finns
        if author['author_id'] is None:
            return None
            
        cur.execute("""
            SELECT id FROM openalex.authors 
            WHERE id = %s
        """, (author['author_id'],))
        
        result = cur.fetchone()
        if result:
            # Författaren finns redan, uppdatera
            cur.execute("""
                UPDATE openalex.authors 
                SET display_name = %s, orcid = %s
                WHERE id = %s
            """, (author['display_name'], author['orcid'], author['author_id']))
        else:
            # Infoga ny författare
            cur.execute("""
                INSERT INTO openalex.authors (id, display_name, orcid)
                VALUES (%s, %s, %s)
            """, (author['author_id'], author['display_name'], author['orcid']))
        
        conn.commit()
        return author['author_id']
    except Exception as e:
        conn.rollback()
        log_message(logfile, f"Fel vid inmatning av författare {author.get('display_name', 'unknown')}: {str(e)}")
        return None

def link_author_to_work(conn, work_id, author_id, author_position, is_corresponding, raw_author_name, logfile):
    """Koppla författare till verk"""
    cur = conn.cursor()
    
    try:
        # Kontrollera om kopplingen redan finns
        cur.execute("""
            SELECT id FROM openalex.work_authors 
            WHERE work_id = %s AND author_id = %s
        """, (work_id, author_id))
        
        result = cur.fetchone()
        if result:
            # Kopplingen finns redan, uppdatera
            cur.execute("""
                UPDATE openalex.work_authors 
                SET author_position = %s, is_corresponding = %s, raw_author_name = %s
                WHERE work_id = %s AND author_id = %s
            """, (author_position, is_corresponding, raw_author_name, work_id, author_id))
        else:
            # Skapa ny koppling
            cur.execute("""
                INSERT INTO openalex.work_authors 
                (work_id, author_id, author_position, is_corresponding, raw_author_name) 
                VALUES (%s, %s, %s, %s, %s)
            """, (work_id, author_id, author_position, is_corresponding, raw_author_name))
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        log_message(logfile, f"Fel vid koppling av författare {author_id} till verk {work_id}: {str(e)}")
        return False

def insert_institution(conn, institution, logfile):
    """Infoga institutionsdata och returnera ID"""
    cur = conn.cursor()
    
    try:
        # Kontrollera om institutionen redan finns
        if institution['id'] is None:
            return None
            
        cur.execute("""
            SELECT id FROM openalex.institutions 
            WHERE id = %s
        """, (institution['id'],))
        
        result = cur.fetchone()
        if result:
            # Institutionen finns redan, uppdatera
            cur.execute("""
                UPDATE openalex.institutions 
                SET display_name = %s, ror = %s, country_code = %s, type = %s
                WHERE id = %s
            """, (institution['display_name'], institution['ror'], 
                  institution['country_code'], institution['type'], institution['id']))
        else:
            # Infoga ny institution
            cur.execute("""
                INSERT INTO openalex.institutions 
                (id, display_name, ror, country_code, type)
                VALUES (%s, %s, %s, %s, %s)
            """, (institution['id'], institution['display_name'], 
                  institution['ror'], institution['country_code'], institution['type']))
        
        conn.commit()
        return institution['id']
    except Exception as e:
        conn.rollback()
        log_message(logfile, f"Fel vid inmatning av institution {institution.get('display_name', 'unknown')}: {str(e)}")
        return None

def link_author_to_institution(conn, work_id, author_id, institution_id, logfile):
    """Koppla författare till institution i en publikation"""
    cur = conn.cursor()
    
    try:
        # Kontrollera om kopplingen redan finns
        cur.execute("""
            SELECT work_id FROM openalex.author_institutions 
            WHERE work_id = %s AND author_id = %s AND institution_id = %s
        """, (work_id, author_id, institution_id))
        
        if cur.rowcount == 0:
            # Skapa ny koppling
            cur.execute("""
                INSERT INTO openalex.author_institutions 
                (work_id, author_id, institution_id) 
                VALUES (%s, %s, %s)
            """, (work_id, author_id, institution_id))
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        log_message(logfile, f"Fel vid koppling av författare {author_id} till institution {institution_id}: {str(e)}")
        return False

def insert_concept(conn, concept, logfile):
    """Infoga konceptdata och returnera ID"""
    cur = conn.cursor()
    
    try:
        # Kontrollera om konceptet redan finns
        if concept['id'] is None:
            return None
            
        cur.execute("""
            SELECT id FROM openalex.concepts 
            WHERE id = %s
        """, (concept['id'],))
        
        result = cur.fetchone()
        if result:
            # Konceptet finns redan, uppdatera
            cur.execute("""
                UPDATE openalex.concepts 
                SET display_name = %s, wikidata = %s, level = %s
                WHERE id = %s
            """, (concept['display_name'], concept['wikidata'], concept['level'], concept['id']))
        else:
            # Infoga nytt koncept
            cur.execute("""
                INSERT INTO openalex.concepts 
                (id, display_name, wikidata, level)
                VALUES (%s, %s, %s, %s)
            """, (concept['id'], concept['display_name'], concept['wikidata'], concept['level']))
        
        conn.commit()
        return concept['id']
    except Exception as e:
        conn.rollback()
        log_message(logfile, f"Fel vid inmatning av koncept {concept.get('display_name', 'unknown')}: {str(e)}")
        return None

def link_concept_to_work(conn, work_id, concept_id, score, logfile):
    """Koppla koncept till verk"""
    cur = conn.cursor()
    
    try:
        # Kontrollera om kopplingen redan finns
        cur.execute("""
            SELECT work_id FROM openalex.work_concepts 
            WHERE work_id = %s AND concept_id = %s
        """, (work_id, concept_id))
        
        if cur.rowcount > 0:
            # Kopplingen finns redan, uppdatera
            cur.execute("""
                UPDATE openalex.work_concepts 
                SET score = %s
                WHERE work_id = %s AND concept_id = %s
            """, (score, work_id, concept_id))
        else:
            # Skapa ny koppling
            cur.execute("""
                INSERT INTO openalex.work_concepts 
                (work_id, concept_id, score) 
                VALUES (%s, %s, %s)
            """, (work_id, concept_id, score))
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        log_message(logfile, f"Fel vid koppling av koncept {concept_id} till verk {work_id}: {str(e)}")
        return False

def insert_topic(conn, topic, logfile):
    """Infoga topic och relaterad information"""
    cur = conn.cursor()
    
    try:
        # Kontrollera om topic redan finns
        if topic['id'] is None:
            return None
            
        cur.execute("""
            SELECT id FROM openalex.topics 
            WHERE id = %s
        """, (topic['id'],))
        
        result = cur.fetchone()
        if result:
            # Topic finns redan, uppdatera
            cur.execute("""
                UPDATE openalex.topics 
                SET display_name = %s, subfield_id = %s, subfield_name = %s,
                    field_id = %s, field_name = %s, domain_id = %s, domain_name = %s
                WHERE id = %s
            """, (topic['display_name'], topic['subfield_id'], topic['subfield_name'], 
                  topic['field_id'], topic['field_name'], topic['domain_id'], 
                  topic['domain_name'], topic['id']))
        else:
            # Infoga ny topic
            cur.execute("""
                INSERT INTO openalex.topics 
                (id, display_name, subfield_id, subfield_name, field_id, field_name, domain_id, domain_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (topic['id'], topic['display_name'], topic['subfield_id'], topic['subfield_name'], 
                  topic['field_id'], topic['field_name'], topic['domain_id'], topic['domain_name']))
        
        conn.commit()
        return topic['id']
    except Exception as e:
        conn.rollback()
        log_message(logfile, f"Fel vid inmatning av topic {topic.get('display_name', 'unknown')}: {str(e)}")
        return None

def link_topic_to_work(conn, work_id, topic_id, score, logfile):
    """Koppla topic till verk"""
    cur = conn.cursor()
    
    try:
        # Kontrollera om kopplingen redan finns
        cur.execute("""
            SELECT work_id FROM openalex.work_topics 
            WHERE work_id = %s AND topic_id = %s
        """, (work_id, topic_id))
        
        if cur.rowcount > 0:
            # Kopplingen finns redan, uppdatera
            cur.execute("""
                UPDATE openalex.work_topics 
                SET score = %s
                WHERE work_id = %s AND topic_id = %s
            """, (score, work_id, topic_id))
        else:
            # Skapa ny koppling
            cur.execute("""
                INSERT INTO openalex.work_topics 
                (work_id, topic_id, score) 
                VALUES (%s, %s, %s)
            """, (work_id, topic_id, score))
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        log_message(logfile, f"Fel vid koppling av topic {topic_id} till verk {work_id}: {str(e)}")
        return False

def insert_location(conn, work_id, location, logfile):
    """Infoga lokationsdata"""
    cur = conn.cursor()
    
    try:
        # Infoga lokation
        cur.execute("""
            INSERT INTO openalex.locations 
            (work_id, source_id, is_oa, landing_page_url, pdf_url, license, version, is_accepted, is_published)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (work_id, location['source_id'], location['is_oa'], 
              location['landing_page_url'], location['pdf_url'], 
              location['license'], location['version'], 
              location['is_accepted'], location['is_published']))
        
        location_id = cur.fetchone()[0]
        conn.commit()
        return location_id
    except Exception as e:
        conn.rollback()
        log_message(logfile, f"Fel vid inmatning av lokation för verk {work_id}: {str(e)}")
        return None

def insert_source(conn, source, logfile):
    """Infoga källinformation"""
    cur = conn.cursor()
    
    try:
        # Kontrollera om källan redan finns
        if source['id'] is None:
            return None
            
        cur.execute("""
            SELECT id FROM openalex.sources 
            WHERE id = %s
        """, (source['id'],))
        
        result = cur.fetchone()
        if result:
            # Källan finns redan, uppdatera om det finns värden att uppdatera
            if source['display_name'] or source['issn_l'] or source['type']:
                cur.execute("""
                    UPDATE openalex.sources 
                    SET display_name = COALESCE(%s, display_name),
                        issn_l = COALESCE(%s, issn_l),
                        type = COALESCE(%s, type)
                    WHERE id = %s
                """, (source['display_name'], source['issn_l'], source['type'], source['id']))
        else:
            # Infoga ny källa
            cur.execute("""
                INSERT INTO openalex.sources 
                (id, display_name, issn_l, type)
                VALUES (%s, %s, %s, %s)
            """, (source['id'], source['display_name'], source['issn_l'], source['type']))
        
        conn.commit()
        return source['id']
    except Exception as e:
        conn.rollback()
        log_message(logfile, f"Fel vid inmatning av källa {source.get('id', 'unknown')}: {str(e)}")
        return None

def insert_reference(conn, work_id, referenced_work_id, logfile):
    """Infoga referens"""
    cur = conn.cursor()
    
    try:
        # Infoga referens om den inte redan finns
        cur.execute("""
            INSERT INTO openalex.referenced_works 
            (work_id, referenced_work_id)
            VALUES (%s, %s)
            ON CONFLICT (work_id, referenced_work_id) DO NOTHING
        """, (work_id, referenced_work_id))
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        log_message(logfile, f"Fel vid inmatning av referens från {work_id} till {referenced_work_id}: {str(e)}")
        return False

def insert_grant(conn, work_id, grant, logfile):
    """Infoga bidrag"""
    cur = conn.cursor()
    
    try:
        # Infoga bidrag
        cur.execute("""
            INSERT INTO openalex.grants 
            (work_id, funder, funder_display_name, award_id)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """, (work_id, grant['funder'], grant['funder_display_name'], grant['award_id']))
        
        grant_id = cur.fetchone()[0]
        conn.commit()
        return grant_id
    except Exception as e:
        conn.rollback()
        log_message(logfile, f"Fel vid inmatning av bidrag för verk {work_id}: {str(e)}")
        return None

def insert_keyword(conn, keyword, logfile):
    """Infoga nyckelord"""
    cur = conn.cursor()
    
    try:
        # Kontrollera om nyckelordet redan finns
        if keyword['id'] is None:
            return None
            
        cur.execute("""
            SELECT id FROM openalex.keywords 
            WHERE id = %s
        """, (keyword['id'],))
        
        result = cur.fetchone()
        if result:
            # Nyckelordet finns redan, uppdatera
            cur.execute("""
                UPDATE openalex.keywords 
                SET display_name = %s
                WHERE id = %s
            """, (keyword['display_name'], keyword['id']))
        else:
            # Infoga nytt nyckelord
            cur.execute("""
                INSERT INTO openalex.keywords 
                (id, display_name)
                VALUES (%s, %s)
            """, (keyword['id'], keyword['display_name']))
        
        conn.commit()
        return keyword['id']
    except Exception as e:
        conn.rollback()
        log_message(logfile, f"Fel vid inmatning av nyckelord {keyword.get('display_name', 'unknown')}: {str(e)}")
        return None

def link_keyword_to_work(conn, work_id, keyword_id, score, logfile):
    """Koppla nyckelord till verk"""
    cur = conn.cursor()
    
    try:
        # Kontrollera om kopplingen redan finns
        cur.execute("""
            SELECT work_id FROM openalex.work_keywords 
            WHERE work_id = %s AND keyword_id = %s
        """, (work_id, keyword_id))
        
        if cur.rowcount > 0:
            # Kopplingen finns redan, uppdatera
            cur.execute("""
                UPDATE openalex.work_keywords 
                SET score = %s
                WHERE work_id = %s AND keyword_id = %s
            """, (score, work_id, keyword_id))
        else:
            # Skapa ny koppling
            cur.execute("""
                INSERT INTO openalex.work_keywords 
                (work_id, keyword_id, score) 
                VALUES (%s, %s, %s)
            """, (work_id, keyword_id, score))
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        log_message(logfile, f"Fel vid koppling av nyckelord {keyword_id} till verk {work_id}: {str(e)}")
        return False

def insert_sdg(conn, sdg, logfile):
    """Infoga hållbarhetsmål"""
    cur = conn.cursor()
    
    try:
        # Kontrollera om SDG redan finns
        if sdg['id'] is None:
            return None
            
        cur.execute("""
            SELECT id FROM openalex.sdgs 
            WHERE id = %s
        """, (sdg['id'],))
        
        result = cur.fetchone()
        if result:
            # SDG finns redan, uppdatera
            cur.execute("""
                UPDATE openalex.sdgs 
                SET display_name = %s
                WHERE id = %s
            """, (sdg['display_name'], sdg['id']))
        else:
            # Infoga ny SDG
            cur.execute("""
                INSERT INTO openalex.sdgs 
                (id, display_name)
                VALUES (%s, %s)
            """, (sdg['id'], sdg['display_name']))
        
        conn.commit()
        return sdg['id']
    except Exception as e:
        conn.rollback()
        log_message(logfile, f"Fel vid inmatning av SDG {sdg.get('display_name', 'unknown')}: {str(e)}")
        return None

def link_sdg_to_work(conn, work_id, sdg_id, score, logfile):
    """Koppla SDG till verk"""
    cur = conn.cursor()
    
    try:
        # Kontrollera om kopplingen redan finns
        cur.execute("""
            SELECT work_id FROM openalex.work_sdgs 
            WHERE work_id = %s AND sdg_id = %s
        """, (work_id, sdg_id))
        
        if cur.rowcount > 0:
            # Kopplingen finns redan, uppdatera
            cur.execute("""
                UPDATE openalex.work_sdgs 
                SET score = %s
                WHERE work_id = %s AND sdg_id = %s
            """, (score, work_id, sdg_id))
        else:
            # Skapa ny koppling
            cur.execute("""
                INSERT INTO openalex.work_sdgs 
                (work_id, sdg_id, score) 
                VALUES (%s, %s, %s)
            """, (work_id, sdg_id, score))
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        log_message(logfile, f"Fel vid koppling av SDG {sdg_id} till verk {work_id}: {str(e)}")
        return False

def run_matching_script(conn, script_path, logfile):
    """Kör matchningsskript"""
    log_message(logfile, f"Kör matchningsskript {script_path}")
    
    try:
        # Skapa nödvändiga matchningstabeller om de inte finns
        cur = conn.cursor()
        
        # Kontrollera om kopplingstabell finns
        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS "cross".gup2openalex (
                    work_id VARCHAR(255) NOT NULL,
                    matched_round VARCHAR(255) NOT NULL,
                    last_modified DATE DEFAULT CURRENT_DATE,
                    pubid INTEGER NOT NULL,
                    CONSTRAINT "PK_GUPOALEX" PRIMARY KEY (work_id, matched_round, pubid)
                )
            """)
            conn.commit()
        except Exception as e:
            conn.rollback()
            log_message(logfile, f"Fel vid skapande av kopplingstabell: {str(e)}")
        
        # Kör matchningsskriptet
        with open(script_path, 'r') as f:
            sql = f.read()
        
        cur.execute(sql)
        conn.commit()
        
        # Kontrollera matchningsresultat
        cur.execute("""
            SELECT matched_round, COUNT(*) 
            FROM "cross".gup2openalex 
            GROUP BY matched_round 
            ORDER BY COUNT(*) DESC
        """)
        
        log_message(logfile, "Matchningsresultat:")
        for row in cur.fetchall():
            log_message(logfile, f"  {row[0]}: {row[1]} matchningar")
        
        return True
    except Exception as e:
        conn.rollback()
        log_message(logfile, f"Fel vid körning av matchningsskript: {str(e)}")
        return False

def clean_db_tables(conn, logfile):
    """Rensa databastabeller för ny import"""
    cur = conn.cursor()
    try:
        # Återställ processed-flaggan
        cur.execute("UPDATE openalex.raw_json SET processed = FALSE")
        
        # Töm tabellerna
        cur.execute("TRUNCATE openalex.works CASCADE")
        cur.execute("TRUNCATE openalex.authors CASCADE")
        cur.execute("TRUNCATE openalex.institutions CASCADE")
        cur.execute("TRUNCATE openalex.concepts CASCADE")
        cur.execute("TRUNCATE openalex.topics CASCADE")
        cur.execute("TRUNCATE openalex.keywords CASCADE")
        cur.execute("TRUNCATE openalex.sdgs CASCADE")
        
        # Töm även cross-schemat om det finns
        try:
            cur.execute('TRUNCATE "cross".gup2openalex CASCADE')
        except:
            log_message(logfile, "Notera: cross.gup2openalex-tabell finns inte eller kan inte rensas")
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        log_message(logfile, f"Fel vid rensning av tabeller: {str(e)}")
        return False

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
                    
                    # Infoga i databasen
                    if insert_work(conn, work, logfile):
                        # Infoga relaterad data
                        
                        # IDs
                        ids = extract_ids(item)
                        insert_work_ids(conn, work['id'], ids, logfile)
                        
                        # Författare och institutioner
                        authors = extract_authors(item)
                        for author in authors:
                            author_id = insert_author(conn, author, logfile)
                            if author_id:
                                link_author_to_work(conn, work['id'], author_id, 
                                                  author['author_position'], 
                                                  author['is_corresponding'],
                                                  author['raw_author_name'], logfile)
                                
                                # Koppla författare till institutioner
                                for inst in author.get('institutions', []):
                                    if 'id' in inst:
                                        institution_id = insert_institution(conn, {
                                            'id': inst.get('id'),
                                            'display_name': inst.get('display_name'),
                                            'ror': inst.get('ror'),
                                            'country_code': inst.get('country_code'),
                                            'type': inst.get('type')
                                        }, logfile)
                                        
                                        if institution_id:
                                            link_author_to_institution(conn, work['id'], author_id, institution_id, logfile)
                        
                        # Koncept
                        concepts = extract_concepts(item)
                        for concept in concepts:
                            concept_id = insert_concept(conn, concept, logfile)
                            if concept_id:
                                link_concept_to_work(conn, work['id'], concept_id, concept.get('score'), logfile)
                        
                        # Topics
                        topics = extract_topics(item)
                        for topic in topics:
                            topic_id = insert_topic(conn, topic, logfile)
                            if topic_id:
                                link_topic_to_work(conn, work['id'], topic_id, topic.get('score'), logfile)
                        
                        # Lokationer och källor
                        locations = extract_locations(item)
                        for location in locations:
                            if location.get('source_id'):
                                insert_source(conn, {'id': location['source_id'], 'display_name': None, 'issn_l': None, 'type': None}, logfile)
                            insert_location(conn, work['id'], location, logfile)
                        
                        # Referenser
                        references = extract_references(item)
                        for reference in references:
                            insert_reference(conn, work['id'], reference, logfile)
                        
                        # Bidrag
                        grants = extract_grants(item)
                        for grant in grants:
                            insert_grant(conn, work['id'], grant, logfile)
                        
                        # Nyckelord
                        keywords = extract_keywords(item)
                        for keyword in keywords:
                            if keyword.get('id'):
                                keyword_id = insert_keyword(conn, keyword, logfile)
                                if keyword_id:
                                    link_keyword_to_work(conn, work['id'], keyword_id, keyword.get('score'), logfile)
                        
                        # SDGs
                        sdgs = extract_sdgs(item)
                        for sdg in sdgs:
                            if sdg.get('id'):
                                sdg_id = insert_sdg(conn, sdg, logfile)
                                if sdg_id:
                                    link_sdg_to_work(conn, work['id'], sdg_id, sdg.get('score'), logfile)
                        
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
        log_dir = os.path.join(ROOT_DIR, 'extract_openalex_json/current/log')
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
            log_message(logfile, "Rensar databastabeller...")
            if clean_db_tables(conn, logfile):
                log_message(logfile, "Databastabeller rensade framgångsrikt")
            else:
                log_message(logfile, "Misslyckades att rensa databastabeller")
        
        # Bearbeta JSON-poster om inte i match-only-läge
        if not args.only_match:
            log_message(logfile, "Bearbetar JSON-poster...")
            total_processed, total_success = process_json_records(conn, logfile, args.batch_size)
            log_message(logfile, f"Slutförd bearbetning: {total_success}/{total_processed} poster bearbetades framgångsrikt")
        else:
            log_message(logfile, "Hoppar över databearbetning (--only-match-läge)")
        
        # Kör matchningsskript om inte inaktiverat
        if not args.no_match and os.path.exists(match_script):
            log_message(logfile, "Kör matchningsskript...")
            if run_matching_script(conn, match_script, logfile):
                log_message(logfile, "Matchning slutförd framgångsrikt")
            else:
                log_message(logfile, "Matchningsskript misslyckades")
        
        # Visa statistik
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM openalex.works")
        work_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM openalex.authors")
        author_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM openalex.work_authors")
        work_author_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM openalex.institutions")
        institution_count = cur.fetchone()[0]
        
        # Försök få matchningsantal
        try:
            cur.execute('SELECT COUNT(*) FROM "cross".gup2openalex')
            match_count = cur.fetchone()[0]
        except:
            match_count = 0
        
        log_message(logfile, f"Statistik:")
        log_message(logfile, f"  Verk: {work_count}")
        log_message(logfile, f"  Författare: {author_count}")
        log_message(logfile, f"  Verk-Författare-kopplingar: {work_author_count}")
        log_message(logfile, f"  Institutioner: {institution_count}")
        log_message(logfile, f"  GUP-OpenAlex-matchningar: {match_count}")
        
        # Stäng anslutning
        conn.close()
        
        log_message(logfile, "OpenAlex JSON Extractor slutfördes framgångsrikt")
        
    except Exception as e:
        log_message(logfile, f"Kritiskt fel: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
