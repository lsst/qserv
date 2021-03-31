FROM centos:8 AS qserv-build

RUN dnf install -y 'dnf-command(config-manager)' \
    && dnf config-manager --set-enabled powertools \
    && dnf install -y epel-release \
    && dnf config-manager --add-repo=https://download.docker.com/linux/centos/docker-ce.repo \
    && dnf update -y \
    && dnf install -y \
        apr-devel \
        apr-util-devel \
        bash-completion \
        boost-devel \
        docker-ce-cli \
        flex \
        gcc-c++ \
        gdb \
        git \
        glib2-devel \
        java-devel \
        jemalloc \
        libcurl-devel \
        libevent-devel \
        libtool \
        libuuid-devel \
        lua-devel \
        make \
        mariadb-connector-c-devel \
        openssl-devel \
        patch \
        protobuf-compiler \
        protobuf-devel \
        python3-devel \
        tree \
        vim \
    && dnf clean all \
    && rm -rf /var/cache/yum

RUN curl -s "https://cmake.org/files/v3.17/cmake-3.17.2-Linux-x86_64.tar.gz" \
    | tar --strip-components=1 -xz -C /usr/local

RUN cd /tmp \
    && git clone https://github.com/apache/logging-log4cxx \
    && cd logging-log4cxx \
    && git checkout a7d562806f0512683d9f32b3c3f95300d4c62fbe \
    && ./autogen.sh \
    && ./configure --disable-doxygen \
    && make -j8 \
    && make install \
    && cd /tmp \
    && rm -rf logging-log4cxx

RUN cd /tmp \
    && git clone https://github.com/nlohmann/json \
    && mkdir json/build \
    && cd json/build \
    && cmake -DJSON_BuildTests=off .. \
    && make -j8 \
    && make install \
    && cd /tmp \
    && rm -rf json

RUN cd /usr/share/java \
    && curl -O https://www.antlr.org/download/antlr-4.8-complete.jar \
    && cd /tmp \
    && git clone https://github.com/antlr/antlr4 \
    && cd antlr4 \
    && git reset --hard 1284814c2112c7ffe275a4831511b4a0f21dd44c \
    && git config user.email "qserv@qserv-build" \
    && curl https://patch-diff.githubusercontent.com/raw/antlr/antlr4/pull/2773.patch | git am \
    && mkdir runtime/Cpp/build \
    && cd runtime/Cpp/build \
    && cmake -DANTLR4_INSTALL=on .. \
    && make -j8 \
    && make install \
    && cp ../cmake/FindANTLR.cmake /usr/local/share/cmake-3.17/Modules/ \
    && cd /tmp \
    && rm -rf antlr4

RUN cd /tmp \
    && git clone https://github.com/lsst/mysqlproxy \
    && cd mysqlproxy \
    && git checkout tickets/DM-25794 \
    && ./configure \
    && make \
    && make install \
    && cd /tmp \
    && rm -rf mysqlproxy

RUN cd /tmp \
    && git clone https://github.com/lsst/xrootd \
    && cd xrootd \
    && git checkout lsst-dev-2 \
    && mkdir build \
    && cd build \
    && cmake -DENABLE_PYTHON=off .. \
    && make -j8 \
    && make install \
    && cd /tmp \
    && rm -rf xrootd

RUN echo /usr/local/lib > /etc/ld.so.conf.d/local.conf \
    && ldconfig

RUN pip3 install \
    click \
    deprecated \
    pybind11[global] \
    pyyaml \
    sqlalchemy

RUN useradd --create-home --uid 1000 --shell /bin/bash qserv

ENV QSERV_ROOT "/home/qserv/code/qserv"
ENV PATH "${PATH}:${QSERV_ROOT}/admin/local/docker/cli/"

VOLUME /home/qserv
WORKDIR /home/qserv

CMD ["tail", "-f", "/dev/null"]

#-----------------------------------------------------------------------------

FROM centos:8 AS qserv-run

RUN dnf install -y 'dnf-command(config-manager)' \
    && dnf config-manager --set-enabled powertools \
    && dnf install -y epel-release \
    && dnf update -y \
    && dnf install -y \
        apr \
        apr-util \
        bash-completion \
        boost-filesystem \
        boost-program-options \
        boost-regex \
        boost-system \
        boost-thread \
        gdb \
        gdb-gdbserver \
        glib2 \
        jemalloc \
        libevent \
        libuuid \
        lua \
        mariadb-connector-c \
        openssl \
        protobuf \
        python3 \
        tree \
        vim \
    && dnf clean all \
    && rm -rf /var/cache/yum

COPY --from=qserv-build /usr/local/lib/liblog4cxx.so /usr/local/lib/
COPY --from=qserv-build /usr/local/lib/libantlr4-runtime.so /usr/local/lib/
COPY --from=qserv-build /usr/local/bin/mysql-proxy /usr/local/bin/
COPY --from=qserv-build /usr/local/lib/libmysql-*.so /usr/local/lib/
COPY --from=qserv-build /usr/local/lib/mysql-proxy/lua/*.so /usr/local/lib/mysql-proxy/lua/
COPY --from=qserv-build /usr/local/lib/mysql-proxy/plugins/*.so /usr/local/lib/mysql-proxy/plugins/

COPY --from=qserv-build /usr/local/include/xrootd/ /usr/local/include/
COPY --from=qserv-build /usr/local/lib64/libXrd*.so /usr/local/lib64/
COPY --from=qserv-build /usr/local/bin/xrootd /usr/local/bin/
COPY --from=qserv-build /usr/local/bin/cmsd /usr/local/bin/
COPY --from=qserv-build /usr/local/bin/xrdfs /usr/local/bin/

RUN echo /usr/local/lib > /etc/ld.so.conf.d/local.conf \
    && ldconfig

RUN pip3 install \
    backoff \
    click \
    deprecated \
    flask \
    jinja2 \
    mysql-connector-python \
    pyyaml \
    requests \
    sqlalchemy \
    werkzeug


