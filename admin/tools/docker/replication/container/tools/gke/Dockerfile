# Using the development toolchain
FROM centos/devtoolset-6-toolchain-centos7

USER 0

RUN useradd qserv -u 1000 -U -d /qserv -c "Qserv Account" && \
    mkdir -p /qserv/bin && \
    mkdir -p /qserv/lib && \
    chown -R 1000:1000 /qserv

USER 1000

# Copy applications and libraries into the container at
# the destination folder
ADD bin/* /qserv/bin/
ADD lib/lib*.so* /qserv/lib/

ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/qserv/lib
ENV PATH=${PATH}:/qserv/bin

# Set the default CMD to print the usage of the image
ENTRYPOINT ["container-entrypoint"]
CMD ["ls","-al","/qserv/bin"]
