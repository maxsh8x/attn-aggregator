CREATE TABLE aggregator_visits
(
    userId UInt32,
    appId Enum8('theanswer' = 1, 'thesalt' = 2),
    ip String,
    ua String,
    referer String,
    pagePath String,
    UTMSource UInt8,
    UTMMedium UInt8,
    UTMCampaign String,
    UTMContent String,
    UTMTerm String,
    browserName UInt16,
    browserMajorVersion UInt16,
    deviceType UInt16,
    deviceVendor UInt16,
    operationSystem UInt16,
    eventTime DateTime,
    longitude Float32,
    latitude Float32,
    eventDate Date
) ENGINE = MergeTree(eventDate, appId, 8192);

CREATE TABLE aggregator_events
(
    userId UInt32,
    appId Enum8('theanswer' = 1, 'thesalt' = 2),
    eventId UInt8,
    questionId UInt32,
    answerId UInt32,
    eventDate Date DEFAULT toDate(eventTime),
    eventTime DateTime
) ENGINE = MergeTree(eventDate, (appId, eventId), 8192);

CREATE TABLE aggregator_recommendations
(
    userId UInt32,
    appId Enum8('theanswer' = 1, 'thesalt' = 2),
    fromUrl String,
    toUrl String,
    eventDate Date DEFAULT toDate(eventTime),
    eventTime DateTime
) ENGINE = MergeTree(eventDate, appId, 8192);

