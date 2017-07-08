############################################################
# Dockerfile for a Scrapy development environment
# Based on Ubuntu Image
############################################################

FROM ubuntu:17.04
MAINTAINER rey <sebastien.rey-coyrehourcq@univ-rouen.fr>

## Python Family
RUN apt-get update
RUN apt-get install -qy gcc git curl make bzip2 sqlite openssl libreadline-dev libbz2-dev libssl-dev libsqlite3-dev

## MongoDB
RUN apt-get install dirmngr
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 0C49F3730359A14518585931BC711F9BA15703C6
RUN apt-get install -y --no-install-recommends software-properties-common
RUN echo "deb http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.4 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-3.4.list

## MONGODB
# Create the MongoDB data directory
RUN apt-get update
RUN apt-get install -y mongodb-org

RUN mkdir -p /data/db

# Expose port 27017 from the container to the host
EXPOSE 27017

# Set usr/bin/mongod as the dockerized entry-point application
ENTRYPOINT ["/usr/bin/mongod"]

## PYTHON

RUN useradd -m python_user

WORKDIR /home/python_user
USER python_user

RUN git clone git://github.com/yyuu/pyenv.git .pyenv

RUN git clone https://github.com/pyenv/pyenv-virtualenv.git .pyenv/plugins/pyenv-virtualenv

ENV HOME  /home/python_user
ENV PYENV_ROOT $HOME/.pyenv
ENV PATH $PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH

RUN echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.bashrc
RUN . ~/.bashrc

RUN pyenv install 3.5.2
RUN pyenv global 3.5.2
RUN pyenv rehash
RUN pyenv virtualenv venv352

COPY . /scrapy
WORKDIR /scrapy

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN scrapy crawl airports

## Selenium
#RUN apt-get install -qy firefox xvfb
#RUN pip install selenium pyvirtualdisplay
