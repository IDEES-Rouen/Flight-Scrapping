############################################################
# STAGE 1
############################################################

# https://denibertovic.com/posts/handling-permissions-with-docker-volumes/
FROM gliderlabs/alpine:edge as baseStage

RUN echo http://nl.alpinelinux.org/alpine/edge/testing >> /etc/apk/repositories
RUN apk add --update && apk add -f gnupg ca-certificates curl dpkg su-exec shadow

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN addgroup scrapy \
    && adduser -S -G scrapy scrapy

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]

############################################################
# STAGE 2
############################################################

FROM baseStage
MAINTAINER rey <sebastien.rey-coyrehourcq@univ-rouen.fr>

ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

### Python Family
RUN apk add -f bash nmap patch build-base git openssl-dev bzip2-dev readline readline-devel sqlite3 wget gcc git linux-headers ncurses-dev ca-certificates
RUN apk add -f curl make bzip2 sqlite openssl openssl-dev libreadline-dev libbz2-dev libssl-dev libxml2-dev libxslt-dev libffi-dev libsqlite3-dev iputils-ping nmap mongodb-tools mongodb-clients

RUN mkdir /backup && chown scrapy:scrapy /backup

# Switch user after install
WORKDIR /home/scrapy
USER scrapy

# SET TIMEZONE https://serverfault.com/questions/683605/docker-container-time-timezone-will-not-reflect-changes

#### PYTHON

RUN git clone git://github.com/yyuu/pyenv.git .pyenv

ENV HOME /home/scrapy
ENV PYENV_ROOT $HOME/.pyenv
ENV PATH $PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH

### SQLITE 3 bug with python compile, needed but create mkdir bug in opt ...
### ENV LDFLAGS="-L/usr/local/opt/sqlite/lib -L/usr/local/opt/zlib/lib"
### ENV CPPFLAGS="-I/usr/local/opt/sqlite/include -I/usr/local/opt/zlib/include"

RUN pyenv install 3.5.2
RUN pyenv global 3.5.2
RUN pyenv rehash

COPY . /home/scrapy/flight
WORKDIR /home/scrapy/flight
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

VOLUME /backup

CMD [ "python", "main.py" ]
#ENTRYPOINT ["tail", "-f", "/dev/null"]

## Selenium
#RUN apt-get install -qy firefox xvfb
#RUN pip install selenium pyvirtualdisplay
