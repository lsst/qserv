#-------------------------------------------------------------------------------------------------------------
# The 'qserv-build-user' target is used to customize the 'qserv-build-base' image to a specific user in order
# to ease developer workflow (see 'qserv-build-base' target in ../base/Dockerfile for more details on the
# starting point for this target). In particular, an account with the same username, primary group, uid, and
# gid as the user is added to the image; this makes it possible to bind-mount e.g. a git working tree into a
# build container on behalf of the user without any further mucking about with file ownership/permissions. The
# default runtime user and default working directory of the build base image are also customized for user
# convenience.
#
# Argument QSERV_BUILD_BASE specifies the name+tag of the base image on which to build.  Arguments USER, UID,
# GROUP, and GID are the username, uid, primary group name, and primary gid to be used for the new custom
# account in the image. Values are provided at build time via the --build-arg option to `docker build`.
#-------------------------------------------------------------------------------------------------------------

ARG QSERV_BUILD_BASE=qserv/lite-build:latest
FROM ${QSERV_BUILD_BASE} AS qserv-build-user

ARG USER
ARG UID
ARG GROUP
ARG GID

RUN OGROUP=$(getent group $GID | cut -d: -f1) && if [ "$OGROUP" != "" ]; then groupdel $OGROUP; fi
RUN groupadd --gid $GID $GROUP
RUN OUSER=$(getent passwd $UID | cut -d: -f1) && if [ "$OUSER" != "" ]; then userdel $OUSER; fi
RUN useradd --uid $UID --gid $GROUP $USER

USER $USER
WORKDIR /home/$USER
