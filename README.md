Biokepi: Bioinformatics Ketrew Pipelines
========================================

This project provides a family of libraries to construct
“[Ketrew](https://github.com/hammerlab/ketrew/) Workflows” for
bioinformatics pipelines.

This should be considered *“alpha / preview”* software.

See the documentation at 
[`www.hammerlab.org/docs/biokepi/master`](http://www.hammerlab.org/docs/biokepi/master/index.html).

Build
-----

The main dependency is
[Ketrew](http://hammerlab.org/docs/ketrew/master/index.html) (which requires
OCaml ≥ 4.02.2).

To install the pre-release through `opam`:

    opam pin add biokepi "https://github.com/hammerlab/biokepi.git#biokepi.0.0.0"
    [opam install biokepi]

To use the `master` branch you need also to track the `master` branch of Ketrew:

    opam pin add ketrew https://github.com/hammerlab/ketrew.git
    opam pin add biokepi https://github.com/hammerlab/biokepi.git


To build locally:

    make
    make clean ; WITH_TESTS=true make  # To build also all the tests
    make doc  # To build the documentation, then cf. _build/doc/index.html

Usage
-----

The `Biokepi` module is the main entry point for most use cases.

There are “demo” command-line applications in the `src/app/` directory and
tests in `src/test/`.

The main demo application (`src/app/main.ml`) is documented here: [Use the
demo](src/doc/Use_the_demo.md).
