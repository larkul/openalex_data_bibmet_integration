# Installations- och användarinstruktioner för OpenAlex Python Extractor

## Översikt
OpenAlex JSON-importsysteme för att effektivt importera och bearbeta metadata från OpenAlex API till ett relationsdatabasformat. Systemet hanterar JSON-strukturen från OpenAlex och omvandlar den till strukturerad data i postgresql
## Ingående komponenter

Systemet består av följande huvudkomponenter:

### 1. run_openalex_extractor.sh
Detta är ett bash-skript som fungerar som huvudingång för systemet. Skriptet:
- Konfigurerar miljövariabler från settings.sh
- Skapar nödvändiga kataloger
- Kontrollerar om det finns JSON-filer i inkommande-katalogen
- Uppdaterar databasen från GUP om nödvändigt
- Kopierar JSON-filer till arbetskatalogen
- Installerar Python-beroenden (psycopg2)
- Importerar JSON-filer till databasens raw_json-tabell
- Kör Python-baserade JSON-extraktorn
- Flyttar bearbetade filer till en "processed"-katalog

### 2. openalex_python_extractor.py
Detta är huvudskriptet som gör det faktiska arbetet med att extrahera och transformera data från OpenAlex JSON-format till relationsdatabasformat. Skriptet:
- Ansluter till PostgreSQL-databasen med konfiguration från miljövariabler
- Säkerställer att databastabellerna har rätt struktur
- Hämtar obearbetade JSON-poster från databasen
- Extraherar metadata som:
  - Artikelinformation (titel, DOI, publikationsår, etc.)
  - Författarinformation
  - Institutioner och deras kopplingar till författare
  - Koncept, ämnesområden och nyckelord
  - Publikationsplatser och Open Access-status
  - Referenser och citeringar
  - Bidrag och finansiering
- Infogar extraherad data i lämpliga databastabeller
- Kör matchningsskriptet för att länka OpenAlex-poster till GUP-publikationer
- Loggar processens framsteg och statistik

### 3. settings.sh
Konfigurerar miljövariabler för hela systemet, inklusive:
- Databaskonfiguration (anslutningsdetaljer för GUP och Bibmet)
- Katalogsökvägar
- Lösenord och autentiseringsinformation
- Exporterar nödvändiga variabler för andra skript

### 4. openalex_data_analysis.sql
Ett SQL-skript för att analysera och validera importerad OpenAlex-data:
- Visar övergripande statistik om importerade artiklar
- Analyserar matchningsresultat
- Visar matchningsfördelning per år
- Listar de vanligaste tidskrifterna och ämneskategorierna
- Visar de mest citerade artiklarna
- Identifierar författare med flest publikationer
- Kontrollerar datakvalitet och potentiella matchningsproblem

### 5. openalex_matching_script.sql
Ett SQL-skript som implementerar flera matchningsrundor för att länka OpenAlex-poster till GUP-publikationer genom olika kriterier som:
- Exakt match på DOI
- Match på DOI från publikationslänkar
- Match på titel och publiceringsår
- Indirekta matchningar via WoS
- Och flera andra kombinationer med varierande exakthet

### 6. openalex_schema.sql
Definierar den fullständiga databasstrukturen för OpenAlex-data, inklusive tabeller för:
- Verk/artiklar (huvudinformation)
- Författare
- Institutioner
- Källor/tidskrifter
- Koncept och ämnesområden
- Nyckelord
- Hållbarhetsmål (SDG)
- Bidrag och finansiering
- Samt alla nödvändiga relationer och index

## Installationsanvisningar

### Lägg till skript via FileZilla eller annan SFTP-klient

1. Kopiera till `/bibmet-utils/scripts/`:
   - run_openalex_extractor.sh
   - openalex_python_extractor.py

2. Kopiera till `/bibmet-utils/scripts/subscripts`:
   - settings.sh
   - openalex_data_analysis.sql
   - openalex_matching_script.sql
   - openalex_schema.sql

3. Gör extraktionsskriptet körbart:
   ```bash
   chmod +x ~/bibmet-utils/scripts/run_openalex_extractor.sh
   chmod +x ~/bibmet-utils/scripts/openalex_python_extractor.py
   ```

4. Installera Python-beroenden:
   ```bash
   pip3 install --user psycopg2-binary
   ```

5. Skapa nödvändiga databasschemata:
   ```bash
   psql -U bibmetuser -d bibmet -c "CREATE SCHEMA IF NOT EXISTS openalex;"
   psql -U bibmetuser -d bibmet -c "CREATE SCHEMA IF NOT EXISTS cross;"
   psql -U bibmetuser -d bibmet -c "CREATE SCHEMA IF NOT EXISTS tmp;"
   ```

6. Initiera databasstrukturen:
   ```bash
   psql -U bibmetuser -d bibmet -f ~/bibmet-utils/scripts/subscripts/openalex_schema.sql
   ```

## Användarinstruktioner

### Bearbeta OpenAlex JSON-filer

1. Ladda upp dina OpenAlex JSON-filer till inkommande-katalogen:
   ```bash
   # Placera dina JSON-filer i:
   ~/bibmet-utils/incoming/
   ```

2. Kör den Python-baserade extraktorn:
   ```bash
   cd ~/bibmet-utils/scripts
   ./run_openalex_extractor.sh
   ```

3. Övervaka förloppet:
   ```bash
   tail -f ~/bibmet-utils/extract_openalex_json/current/log/openalex-extractor-wrapper.log
   tail -f ~/bibmet-utils/extract_openalex_json/current/log/openalex-python-extractor.log
   ```

4. Kör endast matchningsskript (om data redan finns i databasen):
   ```bash
   python3 ~/bibmet-utils/scripts/openalex_python_extractor.py --only-match
   ```

### Manuell körning (valfritt)

Du kan också köra Python-extraktorn direkt med specifika alternativ:
```bash
cd ~/bibmet-utils/scripts
python3 openalex_python_extractor.py --clean --batch-size=50
```

Tillgängliga alternativ:
- `--clean`: Rensa databastabellerna innan bearbetning
- `--batch-size=N`: Bearbeta N poster åt gången
- `--no-match`: Hoppa över matchningssteget
- `--logfile=SÖKVÄG`: Ange en anpassad loggfilsplats
- `--only-match`: Kör endast matchningsskriptet, hoppa över extraktion

## Felsökning

### Vanliga problem och lösningar

1. **Databasanslutningsproblem:**
   - Verifiera databasuppgifter i dina miljövariabler i settings.sh
   - Kontrollera att PostgreSQL körs: `pg_isready -h localhost`

2. **JSON-tolkningsfel:**
   - Verifiera strukturen på dina JSON-filer
   - Prova att köra med mindre batchstorlek: `--batch-size=10`
   - Kontrollera loggfilen för detaljerade felmeddelanden

3. **Matchningsfel:**
   - Om du får matchningsfel, prova att köra utan matchningssteget: `--no-match`
   - Senare kan du köra bara matchningsskriptet direkt
   - Kontrollera att databastabellerna i cross-schemat finns

### Verifiering

För att verifiera att allt importerades korrekt:
```sql
-- Kontrollera huvudtabeller
SELECT COUNT(*) FROM openalex.works;
SELECT COUNT(*) FROM openalex.authors;
SELECT COUNT(*) FROM openalex.work_authors;
SELECT COUNT(*) FROM openalex.institutions;

-- Stickprov på data
SELECT id, title, publication_year, doi FROM openalex.works LIMIT 10;

-- Kontrollera matchningar
SELECT matched_round, COUNT(*) 
FROM cross.gup2openalex 
GROUP BY matched_round 
ORDER BY COUNT(*) DESC;
```

### Kör endast matchningsskript (separat)
```bash
export BIBMET_DB="bibmet"
export BIBMET_USER="bibmetuser"
export BIBMET_HOST="localhost"
export PGPASSWORD="ditt_lösenord_här"

# Kör skriptet
python3 ~/bibmet-utils/scripts/openalex_python_extractor.py --only-match
```

## Underhåll

### Uppdatera skripten

Om du behöver uppdatera skripten i framtiden:
1. Stoppa alla pågående OpenAlex-importprocesser
2. Ersätt skriptfilerna med nya versioner
3. Se till att de är körbara: `chmod +x ~/bibmet-utils/scripts/*.py ~/bibmet-utils/scripts/*.sh`
4. Kör de uppdaterade skripten

### Städa upp

För att börja om från början:
```sql
-- Återställ databastabellerna
TRUNCATE openalex.raw_json CASCADE;
TRUNCATE openalex.works CASCADE;
TRUNCATE openalex.authors CASCADE;
TRUNCATE openalex.institutions CASCADE;
TRUNCATE openalex.concepts CASCADE;
TRUNCATE "cross".gup2openalex CASCADE;
```

Sedan starta om importprocessen.

## Datastruktur och -flöde

### OpenAlex-datamodell

OpenAlex-data har en specifik struktur med flera viktiga entiteter:

1. **Works** - Akademiska publikationer (artiklar, böcker, etc.)
2. **Authors** - Författare av arbetena
3. **Institutions** - Organisationer kopplade till författare
4. **Concepts** - Ämnesområden och forskningsidéer
5. **Sources** - Publikationskanaler (tidskrifter, förlag, etc.)
6. **Publishers** - Förlag och publiceringsorganisationer

Dataflödet i systemet:

1. JSON-filer placeras i inkommande-katalogen
2. run_openalex_extractor.sh kopierar filerna till arbetskatalogen
3. Filerna läses in i raw_json-tabellen i databasen
4. openalex_python_extractor.py extraherar data från JSON och omvandlar den till relationsdataformat
5. Data sparas i openalex-schemats olika tabeller
6. Matchningsskriptet körs för att länka OpenAlex-poster till GUP-publikationer
7. Matchningsresultat sparas i cross.gup2openalex
8. Bearbetade JSON-filer flyttas till en "processed"-katalog

### Speciella inslag i OpenAlex-data

1. **Inverterade index för abstrakt**: 
   OpenAlex levererar abstrakt som inverterade index som måste rekonstrueras.

2. **ROR-identifierare**:
   OpenAlex använder Research Organization Registry (ROR) identifierare för institutioner.

3. **Ämnesklassificering**:
   OpenAlex använder en flernivåhierarki: Domäner > Fält > Delfält > Ämnen.

4. **Open Access-information**:
   Detaljerad information om OA-status, licenser och publikationsversioner.

5. **Hållbarhetsmål (SDG)**:
   Kopplingar till FN:s hållbarhetsmål för många publikationer.

## Användningsområden

Importerad OpenAlex-data kan användas för flera syften:

1. **Bibliometriska analyser** av publikationer, författare och institutioner
2. **Kompletterande metadata** till befintliga publikationsdatabaser
3. **Identifiering av potentiella samarbetspartners** baserat på ämnesområden
4. **Uppföljning av hållbarhetsarbete** genom SDG-mappning
5. **Förbättrad sökning** i forskningspublikationer med hjälp av koncept och nyckelord
6. **Övervakning av Open Access-status** för institutionens publikationer
