# We pull form `ketrew-server` beause the user management has been done right
# in https://github.com/ocaml/opam-dockerfiles
# and we trust the people who did it (@avsm).
FROM hammerlab/ketrew-server
# Installing easy Biokepi dependencies:
RUN sudo apt-get update
RUN sudo apt-get install --yes cmake r-base tcsh libx11-dev libfreetype6-dev pkg-config wget

# The hard-one Oracle's Java 7

RUN sudo add-apt-repository --yes ppa:webupd8team/java
RUN sudo apt-get update
# On top of that we have to fight with interactive licensing questions
# http://askubuntu.com/questions/190582/installing-java-automatically-with-silent-option
RUN sudo bash -c "echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections"
RUN sudo bash -c "echo debconf shared/accepted-oracle-license-v1-1 seen true |  debconf-set-selections"
RUN sudo bash -c "DEBIAN_FRONTEND=noninteractive apt-get install --yes --allow-unauthenticated oracle-java7-installer"

