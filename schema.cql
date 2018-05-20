CREATE KEYSPACE staticweb WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 2
};

CREATE TABLE file_contents (
  site ascii,
  path ascii,
  file_location ascii,
  offset bigint,
  length bigint,
  modified timestamp,
  primary key (site, path));

CREATE INDEX file_contents_location_idx ON file_contents (file_location);
