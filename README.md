# trawler
Trawler is a Go-based microservice that reads row changes from a trigger-populated capture table in Postgres, optionally enriches each change with SQL lookups against the same database, and writes the result to shared Redis streams.
