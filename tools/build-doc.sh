#!/usr/bin/env bash

set -e
LIB_PACKAGES=$1

OCAMLDOC_OPTIONS="-package  $LIB_PACKAGES -thread "

mkdir -p _build/doc_src/

for dir in run_environment environment_setup bfx_tools pipeline_edsl lib ; do
  OCAMLDOC_OPTIONS="$OCAMLDOC_OPTIONS -I _build/src/$dir "
done

OCAML_FILES=""
for dir in run_environment environment_setup bfx_tools pipeline_edsl ; do

  lib_mls=$(ocamldep -one-line -modules _build/src/$dir/*.ml  | cut -d : -f 1)
  out_lib=_build/doc_src/biokepi_${dir}.ml
  echo "(* code generated with [$0 $*] *)" > $out_lib
  for i in $lib_mls ; do
    base=`basename $i`
    basebase=${base%.ml}
    echo "base: $base, basebase: $basebase "
    echo "module ${basebase^}"
    echo "module ${basebase^} " >> $out_lib
    mli=${i}i
    if [ -f $mli ]; then
      echo ": sig" >> $out_lib
      cat $mli >> $out_lib
      echo "end " >> $out_lib
    else
      #ocamlfind ocamlc -i $OCAMLDOC_OPTIONS $i >> $out_lib
      echo "bouh"
    fi
    echo "= struct" >> $out_lib
      cat $i >> $out_lib
    echo "end" >> $out_lib
  done
  OCAML_FILES="$OCAML_FILES $out_lib"
done

OCAMLDOC_OPTIONS="$OCAMLDOC_OPTIONS $OCAML_FILES src/test/*.ml src/lib/biokepi.ml"


echo "OCAMLDOC_OPTIONS: $OCAMLDOC_OPTIONS"
OCAMLDOC_DOT_OPTIONS="-dot $OCAMLDOC_OPTIONS -dot-reduce"

mkdir -p _build/apidoc/
SRC_CSS=./tools/ocamldoc-style.css
CSS=style.css
cp $SRC_CSS _build/apidoc/$CSS

ocamlfind ocamldoc -html -css-style $CSS -d _build/apidoc/ $OCAMLDOC_OPTIONS \
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

<div>
<a href="api/biokepi.dot.svg">
<img src="api/biokepi.dot.svg" width="100%">
</a>
</div>

## Biokepi Types

<div>
<a href="api/biokepi-types.dot.svg">
<img src="api/biokepi-types.dot.svg" width="100%">
</a>
</div>


## Including External

This also shows modules from other libraries (like \`Ketrew\`,
or \`ppx_deriving\`).

<div>
<a href="api/biokepi-all.dot.svg">
<img src="api/biokepi-all.dot.svg" width="100%">
</a>
</div>


EOBLOB

INPUT=src,$generated_dot_md,src/doc,src/examples  \
  INDEX=README.md \
  TITLE_PREFIX="Biokepi: " \
  OUTPUT_DIR=_build/doc \
  API=_build/apidoc/ \
  TITLE_SUBSTITUTIONS="main.ml:Literate Tests,edsl_extension_register_result.ml:EDSL Extension “Register Results”" \
  CATCH_MODULE_PATHS='^Biokepi.*:,' \
  oredoc
