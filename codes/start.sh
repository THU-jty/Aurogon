cat /tmp/memcached.pid | xargs kill
memcached -u root -l 172.23.12.125 -p 34567  -c 10000 -d -P /tmp/memcached.pid
