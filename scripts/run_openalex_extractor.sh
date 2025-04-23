#!/bin/bash

# OpenAlex Extractor Wrapper Script
# Detta skript kör den Python-baserade OpenAlex JSON-extraktorn

export ACTION='extract_openalex_json'
if [[ -z "${RUNNING_ENV}" ]];then source subscripts/settings.sh; subscripts/create_directories.sh;fi
source "${RUNNING_ENV}"

LOGFILE="${LOG_DIR}/openalex-extractor-wrapper.log"

# Exportera alla nödvändiga databasvariabler explicit
export PGPASSWORD="${PGPASSWORD}"
export BIBMET_DB="${BIBMET_DB}"
export BIBMET_USER="${BIBMET_USER}"
export BIBMET_HOST="${BIBMET_HOST}"

# Kontrollera om det finns JSON-filer i inkommande-mappen
ls "${INCOMING_DIR}"/*.json > /dev/null 2>&1
if [[ $? > 0 ]] 
then
    echo "Inga JSON-filer hittades i inkommande-mappen"
    exit
fi
echo "JSON-filer hittades i inkommande-mappen"

# Starta loggning
echo "==================================================" > $LOGFILE
echo $(date '+%x %X') "Startar OpenAlex Extractor Wrapper" >> $LOGFILE
echo $(date '+%x %X') "Databasinställningar: ${BIBMET_DB} ${BIBMET_USER}@${BIBMET_HOST}" >> $LOGFILE

# Uppdatera databasen från GUP om nödvändigt
UPDATE_DB_START_TIME=$(date '+%x %X')
./update_bibmet_from_gup.sh
echo ${UPDATE_DB_START_TIME} "Börjar köra update_bibmet_from_gup.sh" >> $LOGFILE
echo $(date '+%x %X') "Avslutar körning av update_bibmet_from_gup.sh" >> $LOGFILE

# Kopiera JSON-filer från inkommande till indata
cp "${INCOMING_DIR}"/*.json "${INDATA_DIR}/"
echo $(date '+%x %X') "Kopierade JSON-filer från inkommande till indata" >> $LOGFILE

# Installera psycopg2 om det inte finns
python3 -c "import psycopg2" 2>/dev/null
if [[ $? > 0 ]]; then
    echo $(date '+%x %X') "Installerar psycopg2..." >> $LOGFILE
    pip3 install --user psycopg2-binary
    echo $(date '+%x %X') "psycopg2 installerat" >> $LOGFILE
fi

# Importera JSON-data till databasen
echo $(date '+%x %X') "Importerar JSON-filer till raw_json-tabell" >> $LOGFILE
for json_file in "${INDATA_DIR}"/*.json; do
    echo $(date '+%x %X') "Bearbetar fil: $(basename $json_file)" >> $LOGFILE
    
    # Använd Python för att importera fil till raw_json-tabell
    python3 -c "
import psycopg2
import sys
import os
import json

try:
    # Använd samma miljövariabler som i extraktorn
    db_name = os.environ.get('BIBMET_DB', 'bibmet')
    db_user = os.environ.get('BIBMET_USER', 'bibmetuser')
    db_host = os.environ.get('BIBMET_HOST', 'localhost')
    db_password = os.environ.get('PGPASSWORD', '')
    
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        host=db_host,
        password=db_password
    )
    
    cur = conn.cursor()
    
    with open('$json_file', 'r') as f:
        data = json.load(f)
    
    # Hantera olika JSON-strukturer
    records = []
    if isinstance(data, dict):
        records = [data]  # Enskild post
    elif isinstance(data, list):
        records = data  # Lista av poster
    
    for record in records:
        record_json = json.dumps(record)
        cur.execute(
            \"\"\"
            INSERT INTO openalex.raw_json (filename, json_content, processed)
            VALUES (%s, %s, FALSE)
            \"\"\"
            , ('$(basename $json_file)', record_json)
        )
    
    conn.commit()
    print(f'Importerade {len(records)} poster från $json_file')
    conn.close()
    
except Exception as e:
    print(f'Fel vid import av $json_file: {str(e)}')
    sys.exit(1)
"
    
    if [[ $? > 0 ]]; then
        echo $(date '+%x %X') "FEL vid import av $(basename $json_file)" >> $LOGFILE
    else
        echo $(date '+%x %X') "Lyckad import av $(basename $json_file)" >> $LOGFILE
    fi
done

# Kör Python JSON-extraktorn
echo $(date '+%x %X') "Kör OpenAlex Python JSON-extraktor" >> $LOGFILE
python3 "${ROOT_DIR}/scripts/openalex_python_extractor.py" \
  --clean \
  --logfile="${LOG_DIR}/openalex-python-extractor.log" \
  --match="${ROOT_DIR}/scripts/subscripts/openalex_matching_script.sql" \
  --batch-size=50

PYTHON_RESULT=$?
if [ $PYTHON_RESULT -ne 0 ]; then
    echo $(date '+%x %X') "FEL: OpenAlex Python-extraktor misslyckades" >> $LOGFILE
    exit 1
fi

echo $(date '+%x %X') "OpenAlex Python-extraktor slutförd" >> $LOGFILE

# Flytta bearbetade filer till processed-katalog
mkdir -p "${INCOMING_DIR}/processed"
mv "${INCOMING_DIR}"/*.json "${INCOMING_DIR}/processed/"
echo $(date '+%x %X') "Flyttade JSON-filer till processed-katalog" >> $LOGFILE

echo "==================================================" >> $LOGFILE
echo $(date '+%x %X') "OpenAlex-extraktorprocess slutförd framgångsrikt" >> $LOGFILE
