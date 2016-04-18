For Developpers
===============


Libraries
---------

Biokepi is made of 5 libraries: 4 building blocs and wrapping module
(`Biokepi`).

`Biokepi` contains a single module `./src/lib/biokepi.ml` that wraps modules
form the other 4 libraries under a cleaner API.


`Biokepi_run_environment` (`src/run_environment`): is the common API used all
across the library (incl.  extensions to Ketrew's EDSL, and the `Machine.t`
interface to computing infrastructure).


`Biokepi_environment_setup`: provides a set of (*optional*/*customisable*)
defaults to setup a `Biokepi.Machine.t`, it includes Ketrew workflow-nodes to
install tools (`Machine.Tool.t`) and to download/prepare reference data
(reference genomes, databases, etc.).

`Biokepi_bfx_tools`: contains the implementations of the Ketrew workflows to
run all supported bioinformatics tools.

`Biokepi_pipeline_edsl`: in-progress high-level API, to build very concise,
typed, and readable bioinformatics workflows.

Tests & Apps
------------

There are “demo” command-line applications in the `src/app` directory and tests
in `src/test/`.

