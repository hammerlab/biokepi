#!/bin/sh

set -e
LIB_PACKAGES=$1

OCAMLDOC_OPTIONS="-package  $LIB_PACKAGES -thread "
for dir in run_environment environment_setup bfx_tools lib ; do
  OCAMLDOC_OPTIONS="$OCAMLDOC_OPTIONS -I _build/src/$dir src/$dir/*"
done
echo "OCAMLDOC_OPTIONS: $OCAMLDOC_OPTIONS"
OCAMLDOC_DOT_OPTIONS="-dot $OCAMLDOC_OPTIONS -dot-reduce"

mkdir -p _build/apidoc/

ocamlfind ocamldoc -html -d _build/apidoc/ $OCAMLDOC_OPTIONS \
  -charset UTF-8 -t "Biokepi API" -keep-code -colorize-code

ocamlfind ocamldoc -dot $OCAMLDOC_DOT_OPTIONS \
  -o _build/apidoc/biokepi-all.dot \
  -dot-include-all

dot -x -Grotate=180 -v -Tsvg  -O _build/apidoc/biokepi-all.dot

ocamlfind ocamldoc -dot $OCAMLDOC_DOT_OPTIONS \
  -o _build/apidoc/biokepi.dot

dot -x -Grotate=180 -v -Tsvg  -O _build/apidoc/biokepi.dot

ocamlfind ocamldoc -dot $OCAMLDOC_DOT_OPTIONS -dot-types \
  -o _build/apidoc/biokepi-types.dot

dot -x -Grotate=180 -v -Tsvg  -O _build/apidoc/biokepi-types.dot

generated_dot_md=_build/Module_Hierarchy.md
cat <<EOBLOB > $generated_dot_md
# Module Graphs

Those graphs are the result of a transitive reduction of the selected
dependency graph.

## Biokepi Modules

![biokepi](api/biokepi.dot.svg)

## Biokepi Types

![biokepi types](api/biokepi-types.dot.svg)


## Including External

This also shows modules from other libraries (like \`Ketrew\`,
or \`ppx_deriving\`).

![all](api/biokepi-all.dot.svg)


EOBLOB

INPUT=src,$generated_dot_md  \
  INDEX=README.md \
  TITLE_PREFIX="Biokepi: " \
  OUTPUT_DIR=_build/doc \
  API=_build/apidoc/ \
  TITLE_SUBSTITUTIONS="main.ml:Literate Tests" \
  oredoc
