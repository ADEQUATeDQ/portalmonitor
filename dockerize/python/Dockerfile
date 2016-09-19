FROM alpine:latest

# File Author / Maintainer
MAINTAINER Juergen Umbrich

# Adds testing package to repositories
# Install needed packages. Notes:
#   * build-base: used so we include the basic development packages (gcc)
#   * python-dev: are used for gevent e.g.
#   * bash: so we can access /bin/bash

# http://bugs.python.org/issue19846
# > At the moment, setting "LANG=C" on a Linux system *fundamentally breaks Python 3*, and that's not OK.
ENV LANG C.UTF-8

# install ca-certificates so that HTTPS works consistently
# the other runtime dependencies for Python are installed later
RUN apk add --no-cache ca-certificates

ENV GPG_KEY C01E1CAD5EA2C4F0B8E3571504C367C218ADD4FF
ENV PYTHON_VERSION 2.7.12

# if this is called "PIP_VERSION", pip explodes with "ValueError: invalid truth value '<VERSION>'"
ENV PYTHON_PIP_VERSION 8.1.2

RUN set -ex \
	&& apk add --no-cache --virtual .fetch-deps \
		gnupg \
		openssl \
		tar \
		xz \
	\
	&& wget -O python.tar.xz "https://www.python.org/ftp/python/${PYTHON_VERSION%%[a-z]*}/Python-$PYTHON_VERSION.tar.xz" \
	&& wget -O python.tar.xz.asc "https://www.python.org/ftp/python/${PYTHON_VERSION%%[a-z]*}/Python-$PYTHON_VERSION.tar.xz.asc" \
	&& export GNUPGHOME="$(mktemp -d)" \
	&& gpg --keyserver ha.pool.sks-keyservers.net --recv-keys "$GPG_KEY" \
	&& gpg --batch --verify python.tar.xz.asc python.tar.xz \
	&& rm -r "$GNUPGHOME" python.tar.xz.asc \
	&& mkdir -p /usr/src/python \
	&& tar -xJC /usr/src/python --strip-components=1 -f python.tar.xz \
	&& rm python.tar.xz \
	\
	&& apk add --no-cache --virtual .build-deps  \
		bzip2-dev \
		gcc \
		gdbm-dev \
		libc-dev \
		linux-headers \
		make \
		ncurses-dev \
		openssl \
		openssl-dev \
		pax-utils \
		readline-dev \
		sqlite-dev \
		tcl-dev \
		tk \
		tk-dev \
		zlib-dev \
# add build deps before removing fetch deps in case there's overlap
	&& apk del .fetch-deps \
	\
	&& cd /usr/src/python \
	&& ./configure \
		--enable-shared \
		--enable-unicode=ucs4 \
	&& make -j$(getconf _NPROCESSORS_ONLN) \
	&& make install \
	\
		&& wget -O /tmp/get-pip.py 'https://bootstrap.pypa.io/get-pip.py' \
		&& python2 /tmp/get-pip.py "pip==$PYTHON_PIP_VERSION" \
		&& rm /tmp/get-pip.py \
		&& pip install --no-cache-dir --upgrade --force-reinstall "pip==$PYTHON_PIP_VERSION" \
		&& [ "$(pip list |tac|tac| awk -F '[ ()]+' '$1 == "pip" { print $2; exit }')" = "$PYTHON_PIP_VERSION" ] \
		\
		&& find /usr/local -depth \
			\( \
				\( -type d -a -name test -o -name tests \) \
			-o \
			\( -type f -a -name '*.pyc' -o -name '*.pyo' \) \
		\) -exec rm -rf '{}' + \
	&& runDeps="$( \
		scanelf --needed --nobanner --recursive /usr/local \
			| awk '{ gsub(/,/, "\nso:", $2); print "so:" $2 }' \
			| sort -u \
			| xargs -r apk info --installed \
			| sort -u \
	)" \
	&& apk add --virtual .python-rundeps $runDeps \
	&& apk del .build-deps \
	&& rm -rf /usr/src/python ~/.cache



ENV PKGS='curl  postgresql-dev libjpeg-turbo-dev zlib-dev libffi-dev openssl-dev musl build-base bash git openssh openssl  gfortran freetype-dev libjpeg-turbo-dev libpng-dev rsyslog gcc libxslt-dev  libxml2-dev libxml2 libxslt libffi-dev ca-certificates'
RUN echo "@testing http://dl-4.alpinelinux.org/alpine/edge/testing" >> /etc/apk/repositories \
  && apk add --update --repository http://dl-cdn.alpinelinux.org/alpine/edge/testing/ $PKGS \
  && pip install --upgrade pip \
  && rm /var/cache/apk/*
  #&& apk del --purge *-dev build-base \
  #&& rm -rf /tmp/* /var/cache/apk/* /root/.cache/*

RUN ln -s /usr/include/locale.h /usr/include/xlocale.h

COPY libs/* /tmp/
# Build BLAS and LAPACK
RUN source /tmp/blas.sh
RUN source /tmp/lapack.sh

# Install numpy and pandas
#RUN pip install --upgrade pip
RUN BLAS=~/src/BLAS/libfblas.a LAPACK=~/src/lapack-3.5.0/liblapack.a pip install numpy scipy

#RUN BLAS=~/src/BLAS/libfblas.a LAPACK=~/src/lapack-3.5.0/liblapack.a pip install -v pandas==0.14.1

# Clean up
RUN mv ~/src/BLAS/libfblas.a /tmp/
RUN mv ~/src/lapack-3.5.0/liblapack.a /tmp/
RUN rm -rf ~/src
RUN rm -rf ~/.cache/pip
RUN mkdir -p ~/src/BLAS ~/src/lapack-3.5.0
RUN mv /tmp/libfblas.a ~/src/BLAS/libfblas.a
RUN mv /tmp/liblapack.a ~/src/lapack-3.5.0/liblapack.a
RUN rm -rf /tmp/*

# Remove all the extra build stuff
RUN apk del build-base "*-dev"

# make some useful symlinks that are expected to exist
RUN cd /usr/bin \
  && ln -sf easy_install-2.7 easy_install \
  && ln -sf python2.7 python \
  && ln -sf python2.7-config python-config \
  && ln -sf pip2.7 pip
