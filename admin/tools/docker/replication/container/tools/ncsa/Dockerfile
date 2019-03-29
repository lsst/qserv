# Using the development toolchain
FROM centos/devtoolset-6-toolchain-centos7

USER 0

RUN mkdir -p /qserv/bin && \
    mkdir -p /qserv/lib && \
    mkdir -p /qserv/work && \
    chown -R 1001:1001 /qserv

USER 1001

# Copy applications and libraries into the container at
# the destination folder
ADD bin/* /qserv/bin/
ADD lib/lib*.so* /qserv/lib/

ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/qserv/lib
ENV PATH=${PATH}:/qserv/bin

RUN cd /qserv/work

# Set the default CMD to print the usage of the image
ENTRYPOINT ["container-entrypoint"]
CMD ["ls","-al","/qserv/bin"]
