 - Add support for memcached to improve average latency
 - Add bulk upload API
 - Roll over files after a certain size
 - Add cronjob binary to rewrite stored content into fewer, larger files and to
   thereby delete content that is no longer referenced
 - Improve HTTP error messages
 - Improve RPC errors from upload service
 - Improve prometheus instrumentation
 - Move common code like the config loader into its own sub-library
