Command tcpsink implements TCP server storing incoming stream of json
messages to snappy-compressed files.

	Usage of tcpsink:
	  -addr string
		address to listen (default "localhost:9000")
	  -age duration
		remove logs older than this value
	  -dir string
		directory to write logs to (default "/tmp/logs")
	  -size int
		maximum file size in bytes (split on message boundary) (default 16777216)


tcpsink was initially written as a standalone tcp endpoint for logstash:

        tcp {
                host => "127.0.0.1"
                port => 9000
        }
