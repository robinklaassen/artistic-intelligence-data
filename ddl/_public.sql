-- put extensions in the public schema to use them from all other schemas in the database
create extension postgis with schema public;
create extension timescaledb with schema public;