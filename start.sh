cat /tmp/memcached.pid | xargs kill
memcached -u root -l 172.23.12.128 -p 34569  -c 10000 -d -P /tmp/memcached.pid
