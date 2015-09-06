FROM mysql:5.6

ENV DB_INIT_FILE=loviswpn.sql
COPY $DB_INIT_FILE /docker-entrypoint-initdb.d/

ENV MYSQL_ROOT_PASSWORD=mysql
ENV MYSQL_DATABASE=loviswpn
