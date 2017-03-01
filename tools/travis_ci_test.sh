#!/usr/bin/env bash



travis_install_on_linux () {
    # Install OCaml and OPAM PPAs
    export ppa=avsm/ocaml42+opam12

    echo "yes" | sudo add-apt-repository ppa:$ppa
    sudo apt-get update -qq

    export opam_init_options="--comp=$OCAML_VERSION"
    sudo apt-get install -qq  opam time git
}

travis_install_on_osx () {
    curl -OL "http://xquartz.macosforge.org/downloads/SL/XQuartz-2.7.6.dmg"
    sudo hdiutil attach XQuartz-2.7.6.dmg
    sudo installer -verbose -pkg /Volumes/XQuartz-2.7.6/XQuartz.pkg -target /

    brew update
    brew install opam
    export opam_init_options="--comp=$OCAML_VERSION"
}


case $TRAVIS_OS_NAME in
  osx) travis_install_on_osx ;;
  linux) travis_install_on_linux ;;
  *) echo "Unknown $TRAVIS_OS_NAME"; exit 1
esac

# configure and view settings
export OPAMYES=1
echo "ocaml -version"
ocaml -version
echo "opam --version"
opam --version
echo "git --version"
git --version

# install OCaml packages
opam init $opam_init_options
eval `opam config env`

opam update

# Cf. https://github.com/mirleft/ocaml-nocrypto/issues/104
opam pin add oasis 0.4.6

opam pin add ketrew https://github.com/hammerlab/ketrew.git

echo 'ocamlfind list | grep lwt'
ocamlfind list | grep lwt
echo 'ocamlfind list | grep cohttp'
ocamlfind list | grep cohttp

echo "Setting Warn-Error for the Travis test"
export OCAMLPARAM="warn-error=A,_"

opam pin add biokepi --yes .
opam install --yes biokepi

# Also build all the tests:
omake build-all
# and run a few:
_build/biokepi-edsl-input-json/biokepi-edsl-input-json.opt
DEST_PATH=/tmp KHOST=/tmp _build/biokepi-test-all-downloads/biokepi-test-all-downloads.opt view rg
DEST_PATH=/tmp KHOST=/tmp _build/biokepi-test-all-downloads/biokepi-test-all-downloads.opt view tools
_build/biokepi-tests/biokepi-tests.opt

# We try the example
cat > my_cluster.ml <<EOCAML
module My_cluster = struct
  let max_processors = 42
  let work_dir = "/work/dir/"
  let datasets_home = "/datasets/"
  let machine =
    Biokepi.Setup.Build_machine.create "ssh://example.com/tmp/KT/"
end
EOCAML
ls -la my_cluster.ml
cat my_cluster.ml
ocaml src/examples/edsl_extension_register_result.ml
