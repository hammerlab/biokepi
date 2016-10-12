# We pull from `ketrew-server` because a bunch of stuff has been done right
# in https://github.com/ocaml/opam-dockerfiles
# and we trust the people who did it (@avsm).
# And we get `aspcud` on the way.
FROM hammerlab/ketrew-server
# Installing easy Biokepi dependencies:
RUN sudo apt-get update
RUN sudo apt-get install --yes cmake r-base tcsh libx11-dev libfreetype6-dev pkg-config wget gawk graphviz

# The hard-one Oracle's Java 7

RUN sudo add-apt-repository --yes ppa:webupd8team/java
RUN sudo apt-get update
# On top of that we have to fight with interactive licensing questions
# http://askubuntu.com/questions/190582/installing-java-automatically-with-silent-option
RUN sudo bash -c "echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections"
RUN sudo bash -c "echo debconf shared/accepted-oracle-license-v1-1 seen true |  debconf-set-selections"
RUN sudo bash -c "DEBIAN_FRONTEND=noninteractive apt-get install --yes --allow-unauthenticated oracle-java7-installer"

# Now a fresh non-sudo user:

RUN sudo  bash -c "\
  adduser --uid 20042 --disabled-password --gecos '' biokepi && \
  passwd -l biokepi && \
  chown -R biokepi:biokepi /home/biokepi"
USER biokepi
ENV HOME /home/biokepi
WORKDIR /home/biokepi
RUN opam init -a --yes --comp 4.02.3
