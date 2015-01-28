FROM debian:stable
MAINTAINER Fabrice Jammes <fabrice.jammes@in2p3.fr>
RUN apt-get clean 
RUN apt-get update
ADD install-deps.sh /tmp/install-deps.sh
RUN /tmp/install-deps.sh
# development tools
RUN apt-get install --assumeyes vim byobu
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
ADD scripts/mono-node-test.sh $HOME/mono-node-test.sh
CMD ["/home/qserv/mono-node-test.sh"]
