-- Local educational credentials; Grafana cannot write to the scores database.
CREATE USER grafana_reader WITH PASSWORD 'grafana_read';
GRANT CONNECT ON DATABASE scores TO grafana_reader;
GRANT USAGE ON SCHEMA public TO grafana_reader;
GRANT SELECT ON transaction_scores TO grafana_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE scores IN SCHEMA public GRANT SELECT ON TABLES TO grafana_reader;
