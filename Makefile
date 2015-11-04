
.PHONY: all clean build configure distclean doc apidoc

all: build

configure: distclean
	oasis setup -setup-update dynamic && \
	    ocaml setup.ml -configure && \
	    echo 'Configured'

build:
	ocaml setup.ml -build && \
	    rm -f main.byte main.native  && \
	    mv _build/src/test/main.native test_biokepi && \
	    mv _build/src/app/main.native biokepi-demo

doc:
	ocaml setup.ml -doc

oredoc: doc build
	INPUT=src  \
	INDEX=README.md \
	TITLE_PREFIX="Biokepi: " \
	OUTPUT_DIR=_oredoc \
	API=biokepi.docdir \
	CATCH_MODULE_PATHS='^(Biokepi[A-Z_a-z]+):', \
	TITLE_SUBSTITUTIONS="main.ml:Literate Tests" \
	oredoc

clean:
	rm -fr _build test_biokepi biokepi-demo

distclean: clean
	ocaml setup.ml -distclean || echo OK ; \
	    rm -f setup.ml _tags myocamlbuild.ml src/*/META src/*/*.mldylib src/*/*.mllib
