FROM python:3.7-slim-buster as baseStage
MAINTAINER rey <sebastien.rey-coyrehourcq@univ-rouen.fr>

RUN apt-get update && apt-get install -y wget gnupg2 ca-certificates curl bash sudo

RUN echo "deb http://repo.mongodb.org/apt/debian buster/mongodb-org/4.2 main" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.2.list
RUN wget -qO - https://www.mongodb.org/static/pgp/server-4.2.asc | sudo apt-key add -

RUN apt-get update && apt-get install -y gosu gcc libxslt1-dev python-dev mongodb-org libssl-dev nmap

ARG GID
ARG UID

ENV TZ=UTC

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN groupadd -g $GID scrapy && useradd -m -s /bin/sh -g scrapy -u $UID scrapy

# INIT CRON
ENV SUPERCRONIC_URL=https://github.com/aptible/supercronic/releases/download/v0.1.5/supercronic-linux-amd64 \
    SUPERCRONIC=supercronic-linux-amd64 \
    SUPERCRONIC_SHA1SUM=9aeb41e00cc7b71d30d33c57a2333f2c2581a201

RUN curl -fsSLO "$SUPERCRONIC_URL" \
 && echo "${SUPERCRONIC_SHA1SUM}  ${SUPERCRONIC}" | sha1sum -c - \
 && chmod +x "$SUPERCRONIC" \
 && mv "$SUPERCRONIC" "/usr/local/bin/${SUPERCRONIC}" \
 && ln -s "/usr/local/bin/${SUPERCRONIC}" /usr/local/bin/supercronic


WORKDIR /home/scrapy

RUN mkdir -p /home/scrapy/backup

COPY crontab /home/scrapy

# https://denibertovic.com/posts/handling-permissions-with-docker-volumes/
# SET TIMEZONE https://serverfault.com/questions/683605/docker-container-time-timezone-will-not-reflect-changes

COPY . /home/scrapy/flight
WORKDIR /home/scrapy/flight

RUN pip install --upgrade pip
RUN pip install --upgrade setuptools
RUN pip install --no-cache-dir -r requirements.txt

VOLUME /home/scrapy/backup

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["supercronic", "-debug", "/home/scrapy/crontab"]
