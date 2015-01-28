FROM centos:latest
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>
RUN yum update --assumeyes
ADD install-deps.sh /tmp/install-deps.sh
RUN yum install --assumeyes git
# newinstall.sh
RUN yum install --assumeyes bash tar make
# sconsUtils
RUN yum install --assumeyes gettext flex bison 
# eups
RUN yum install --assumeyes patch bzip2 bzip2-devel
# xrootd
RUN yum install --assumeyes gcc gcc-c++ zlib-devel cmake
# zope_interface
RUN yum install --assumeyes python-devel
# mysql
RUN yum install --assumeyes ncurses-devel glibc-devel
# qserv
RUN yum install --assumeyes openssl-devel java redhat-lsb
# lua
RUN yum install --assumeyes readline-devel
# mysql-proxy
RUN yum install --assumeyes glib2-devel
# kazoo
RUN yum install --assumeyes python-setuptools
# qserv: deprecated and removed on 2015_01
RUN yum install --assumeyes numpy swig
# development tools
RUN yum install --assumeyes vim byobu
RUN groupadd -r qserv
RUN useradd -r -m -g qserv qserv
USER qserv
ENV HOME /home/qserv
RUN mkdir /home/qserv/stack
WORKDIR /home/qserv/stack
RUN curl -O "https://sw.lsstcorp.org/eupspkg/newinstall.sh"
# Running newinstall in batch mode should remove above line: DM-1495
RUN GIT=yes; ANACONDA=no; printf "$GIT\n$ANACONDA\n" > /tmp/answers.txt
# LSST stack require bash
RUN bash newinstall.sh < /tmp/answers.txt
RUN rm /tmp/answers.txt
# Run long builds atomically to ease Docker debugging 
RUN bash -c ". loadLSST.bash && eups distrib install boost -t qserv"
RUN bash -c ". loadLSST.bash && eups distrib install mysql -t qserv"
RUN bash -c ". loadLSST.bash && eups distrib install mysqlclient -t qserv"
RUN bash -c ". loadLSST.bash && eups distrib install db -t qserv"
RUN bash -c ". loadLSST.bash && eups distrib install qserv_distrib -t qserv --onlydepend"
RUN bash -c ". loadLSST.bash && eups distrib install qserv_distrib -t qserv"
# Mono-node configuration and tests
ADD mono-node-test.sh $HOME/mono-node-test.sh
CMD ["/home/qserv/mono-node-test.sh"]
