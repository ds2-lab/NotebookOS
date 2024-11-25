FROM redis:latest

ADD redis.conf /usr/local/etc/redis/redis.conf
RUN chmod 644 /usr/local/etc/redis/redis.conf

CMD ["redis-server", "/usr/local/etc/redis/redis.conf"]