Cassandra Static Content Service
================================

cstatic is a static content web server which stores its metadata in Cassandra
(but the regular data is stored in some other file store which must be
supported by https://github.com/childoftheuniverse/filesystem/).

cstatic stores many small files in large files. The placement of these files
is tracked in a Cassandra database (thus the "c"). The reason behind this is
that tracking few large files is much easier and cheaper on a distributed
file system (e.g. Amazon S3) than tracking many small files.
