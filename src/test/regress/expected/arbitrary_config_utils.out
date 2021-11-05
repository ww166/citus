CREATE OR REPLACE FUNCTION pg_catalog.grant_schema_to_regularuser(schemaname text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format('GRANT ALL ON SCHEMA %s TO regularuser', schemaname);
    PERFORM run_command_on_workers(format('GRANT ALL ON SCHEMA %s TO regularuser;', schemaname));
END;
$$;
CREATE OR REPLACE FUNCTION pg_catalog.grant_table_to_regularuser(tablename text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format('GRANT ALL ON TABLE %s TO regularuser', tablename);
    PERFORM run_command_on_workers(format('GRANT ALL ON TABLE %s TO regularuser;', tablename));
    PERFORM run_command_on_placements(tablename::regclass, 'GRANT ALL ON TABLE %s TO regularuser');
END;
$$;
