
JOBS=1
OCAMLBUILD=ocamlbuild -j $(JOBS) -use-ocamlfind -plugin-tag "package(solvuu-build,nonstd)"
include _build/project.mk
_build/project.mk:
	$(OCAMLBUILD) $(notdir $@)

.PHONY: merlin
merlin:
	rm -f .merlin _build/.merlin && $(MAKE) .merlin && cat .merlin

.PHONY: doc
doc:
	./tools/build-doc.sh ketrew,ppx_deriving.std

