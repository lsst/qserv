#-------------------------------------------------------------------------------------------------------------
# The 'lite-build-user' target is used to customize the 'lite-build' image to a specific user in order to ease
# developer workflow (see 'lite-build' target in ../base/Dockerfile for more details on the starting point for
# this target). In particular, an account with the same username, primary group, uid, and gid as the user is
# added to the image; this makes it possible to bind-mount e.g. a git working tree into a build container on
# behalf of the user without any further mucking about with file ownership/permissions. The default runtime
# user and default working directory of the build image are also customized for user convenience.
#
# Argument QSERV_BUILD_BASE specifies the name+tag of the base image on which to build.  Arguments USER, UID,
# GROUP, and GID are the username, uid, primary group name, and primary gid to be used for the new custom
# account in the image. Values can be provided via the -e option to `docker run`.
#-------------------------------------------------------------------------------------------------------------

ARG QSERV_BUILD_BASE=qserv/lite-build:latest
FROM ${QSERV_BUILD_BASE} as lite-build-user

ARG USER
ARG UID
ARG GROUP
ARG GID

RUN getent group $GID || groupadd --gid $GID $GROUP
RUN getent passwd $UID || useradd --uid $UID --gid $GROUP $USER

USER $USER
WORKDIR /home/$USER
