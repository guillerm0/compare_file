create table if not exists current
(
    range      String,
    city       String,
    country    String,
    lat        String,
    lng        String,
    postal     String,
    region     String,
    source     String,
    timezone   String,
    geoname_id String,
    hash_key   String,
    first_ip   UInt128,
    last_ip    UInt128
)
    engine = MergeTree ORDER BY hash_key
        SETTINGS index_granularity = 8192;

