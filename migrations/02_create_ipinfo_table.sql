-- Create IP info table to store data from ipinfo.io
CREATE TABLE IF NOT EXISTS crawlers_data.ipinfo (
    ip String,
    hostname String,
    city String,
    region String,
    country String,
    loc String,
    org String,
    postal String,
    timezone String,
    asn String,
    company String,
    carrier String,
    is_bogon Boolean DEFAULT false,
    is_mobile Boolean DEFAULT false,
    abuse_email String,
    abuse_phone String,
    error String,
    attempts UInt8 DEFAULT 1,
    success Boolean DEFAULT true,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ip, updated_at)
SETTINGS index_granularity = 8192;

-- Create index on IP to speed up lookups
CREATE INDEX IF NOT EXISTS ipinfo_ip_idx ON crawlers_data.ipinfo (ip) TYPE minmax GRANULARITY 1;