
-- Matchning mot GUP-publikationer

-- Skapa temporär tabell för normaliserade publikationsdata
DROP TABLE IF EXISTS tmp.openalex_tmp;
CREATE TABLE tmp.openalex_tmp
(
  work_id VARCHAR(255),
  title_normal TEXT,
  doi_normal TEXT,
  publication_year INTEGER
);

-- Rensa bort den nya tabellen om den redan finns
DROP TABLE IF EXISTS "cross".gup2openalex;

-- Skapa kopplingstabell för OpenAlex-data
CREATE TABLE "cross".gup2openalex
(
  work_id VARCHAR(255) NOT NULL,
  matched_round VARCHAR(255) NOT NULL,
  last_modified DATE DEFAULT CURRENT_DATE,
  pubid INTEGER NOT NULL,
  CONSTRAINT "PK_GUPOALEX" PRIMARY KEY (work_id, matched_round, pubid)
);

-- Fyll den temporära tabellen med normaliserade data från OpenAlex
INSERT INTO tmp.openalex_tmp
(
  work_id,
  title_normal,
  doi_normal,
  publication_year
)
SELECT 
  id,
  btrim(translate(upper(title::text), 'ÅÄÖÜËÏÉÈÁÀÓÒÑ":'::text, 'AAOUEIEEAAOON  '::text)) AS title_normal,
  UPPER(TRIM(doi)) AS doi_normal,
  publication_year
FROM openalex.works;

-- Skapa index för bättre prestanda
CREATE INDEX IF NOT EXISTS idx_openalex_tmp_title ON tmp.openalex_tmp(title_normal);
CREATE INDEX IF NOT EXISTS idx_openalex_tmp_doi ON tmp.openalex_tmp(doi_normal);
CREATE INDEX IF NOT EXISTS idx_openalex_tmp_year ON tmp.openalex_tmp(publication_year);

-- MATCHNINGSRUNDOR

-- 1. Exakt match på DOI
INSERT INTO "cross".gup2openalex (pubid, work_id, matched_round)
SELECT DISTINCT p.id, 
                oa.work_id, 
                'exact-doi-improved'
FROM publications p
JOIN publication_versions pv ON pv.id = p.current_version_id
JOIN publication_identifiers pi ON pi.publication_version_id = pv.id
JOIN tmp.openalex_tmp oa 
ON REPLACE(UPPER(oa.doi_normal), 'HTTPS://DOI.ORG/', '') = UPPER(pi.identifier_value)
WHERE pi.identifier_code = 'doi'
AND p.deleted_at IS NULL
AND p.published_at IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM "cross".gup2openalex WHERE pubid = p.id);

-- 2. Match på DOI från publikationslänkar
INSERT INTO "cross".gup2openalex (pubid, work_id, matched_round)
SELECT DISTINCT p.id, 
                oa.work_id,
                'doi-from-link'
FROM publications p
JOIN publication_versions pv ON pv.id = p.current_version_id
JOIN publication_links pl ON pl.publication_version_id = pv.id
JOIN tmp.openalex_tmp oa ON UPPER(oa.doi_normal) = UPPER(SUBSTRING(pl.url, 19))
WHERE p.deleted_at IS NULL
AND pl.url LIKE 'http://dx.doi.org%'
AND p.published_at IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM "cross".gup2openalex WHERE pubid = p.id);

-- 3. Titel + publiceringsår
INSERT INTO "cross".gup2openalex (pubid, work_id, matched_round)
SELECT DISTINCT p.id,
                oa.work_id,
                'title-year'
FROM publications p
JOIN publication_versions pv ON pv.id = p.current_version_id
JOIN tmp.openalex_tmp oa ON 
    btrim(translate(upper(pv.title::text), 'ÅÄÖÜËÏÉÈÁÀÓÒÑ":'::text, 'AAOUEIEEAAOON  '::text)) = oa.title_normal
    AND pv.pubyear = oa.publication_year
WHERE p.deleted_at IS NULL
AND p.published_at IS NOT NULL
AND NOT EXISTS (SELECT 1 FROM "cross".gup2openalex WHERE pubid = p.id);


-- Skapa index för bättre prestanda
CREATE INDEX IF NOT EXISTS idx_gup2openalex_pubid ON "cross".gup2openalex(pubid);
CREATE INDEX IF NOT EXISTS idx_gup2openalex_work_id ON "cross".gup2openalex(work_id);

-- Visa statistik över matchningsresultat
SELECT matched_round, COUNT(*) AS antal_matchningar
FROM "cross".gup2openalex
GROUP BY matched_round
ORDER BY COUNT(*) DESC;
