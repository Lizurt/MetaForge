DO '
DECLARE
    r RECORD;
BEGIN
FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = ''metaforge'') LOOP
    EXECUTE ''DROP TABLE IF EXISTS metaforge.'' || r.tablename || '' CASCADE'';
    END LOOP;
END ';
