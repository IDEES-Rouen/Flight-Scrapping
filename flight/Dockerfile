############################################################
# STAGE 1
############################################################

# https://denibertovic.com/posts/handling-permissions-with-docker-volumes/
FROM python:3.6-alpine3.7 as baseStage
MAINTAINER rey <sebastien.rey-coyrehourcq@univ-rouen.fr>

RUN echo http://nl.alpinelinux.org/alpine/edge/testing >> /etc/apk/repositories
RUN apk upgrade --update-cache --available
RUN apk add --update && apk add -f gnupg ca-certificates curl dpkg bash su-exec shadow gcc musl-dev libxml2-dev libxslt-dev python-dev libffi-dev mongodb mongodb-tools mongotools-db openssl-dev nmap

ARG GID=1000
ARG UID=1000

ENV TZ=UTC

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN addgroup -g $GID scrapy && adduser -h /home/scrapy -s /bin/sh -D -G scrapy -u $UID scrapy

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

# Switch user after install


# SET TIMEZONE https://serverfault.com/questions/683605/docker-container-time-timezone-will-not-reflect-changes

COPY . /home/scrapy/flight
WORKDIR /home/scrapy/flight

RUN pip install --upgrade pip
RUN pip install --upgrade setuptools
RUN pip install --no-cache-dir -r requirements.txt
# --install-option="--prefix=/install" a voir pour plus tard pour gagner de la place

VOLUME /home/scrapy/backup

#ENTRYPOINT ["/sbin/tini"]
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["supercronic", "crontab"]

#CMD ["crond", "-f", "-l", "0", "-L", "/var/log/cron.log"]

#ENTRYPOINT ["tail", "-f", "/dev/null"]

## Selenium
#RUN apt-get install -qy firefox xvfb
#RUN pip install selenium pyvirtualdisplay
