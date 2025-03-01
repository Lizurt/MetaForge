DO '
    DECLARE
        col RECORD;
        ortab RECORD;
        create_table_query TEXT := '''';
    BEGIN
        FOR ortab IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = ''metaforge''
          AND table_type = ''BASE TABLE''
        LOOP
            create_table_query := ''CREATE TABLE IF NOT EXISTS metaforge.'' || ortab.table_name || ''_broken ('';

            FOR col IN
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = ''metaforge''
                  AND table_name = ortab.table_name
                LOOP
                    create_table_query := create_table_query || col.column_name || '' TEXT, '';
                END LOOP;

            create_table_query := left(create_table_query, length(create_table_query) - 2) || '')'';

            EXECUTE create_table_query;

            create_table_query := '''';
        END LOOP;
    END ';
