docker logo
Dashboard
Explore
Organizations

Create
 Create Repository
 Create Automated Build
 Create Organization
jueumb
My Profile
Documentation
Help
Settings
Log out
PUBLIC | AUTOMATED BUILD
bamx23/scipy-alpine
Last pushed: 2 months ago
Repo Info
Tags
Dockerfile
Build Details
Dockerfile
FROM alpine:3.4

ENV LANG C.UTF-8

RUN echo @testing http://dl-cdn.alpinelinux.org/alpine/edge/testing >> /etc/apk/repositories \
    && apk add --update --no-cache \
        curl \
        g++ \
        pkgconfig \
        freetype-dev \
        py-numpy-dev@testing \
        ca-certificates \
    && update-ca-certificates \
    && curl -fSL 'https://bootstrap.pypa.io/get-pip.py' | python \
    && pip install --no-cache-dir setuptools numpy matplotlib scipy \
    && find /usr/local -depth \
        \( \
            \( -type d -a -name test -o -name tests \) \
            -o \
            \( -type f -a -name '*.pyc' -o -name '*.pyo' \) \
        \) -exec rm -rf '{}' + \
    && apk add --virtual .python-deps \
        python \
        py-numpy \
        freetype \
        libpng \
    && apk del \
        curl \
        g++ \
        gcc \
        pkgconfig \
        libc-dev \
        ncurses-dev \
        openssl-dev \
        zlib-dev \
        libpng-dev \
        freetype-dev \
        python-dev \
        cython-dev \
        py-numpy-dev \
    && rm -rf /usr/src/python ~/.cache
