Biokepi: Bioinformatics Ketrew Pipelines
========================================

This project provides a library to construct “Ketrew Targets” for
bioinformatics pipelines.

The library also contains an experimental module called `Biokepi_pipeline` which
uses a GADT to concisely express well-typed analysis pipelines. A *demo* command
line application `biokepi` demonstrates how to run such a pipeline.
The library can download most of the tools by itself (through more Ketrew
targets).


This should be considered *“alpha / preview”* software.


Build
-----

The main dependency is [Ketrew](http://seb.mondet.org/software/ketrew/) (which
requires OCaml ≥ 4.02.2).

To install the pre-release through `opam`:

    opam pin add biokepi "https://github.com/hammerlab/biokepi.git#biokepi.0.0.0"
    [opam install biokepi]

To use the `master` branch you need also to track the `master` branch of Ketrew:

    opam pin add ketrew https://github.com/hammerlab/ketrew.git
    opam pin add biokepi https://github.com/hammerlab/biokepi.git


To build locally:

    omake
    omake build-all  # To build also all the tests
    omake doc  # To build the documentation, cf. _build/doc/index.html

Getting Started
---------------

Once all set with Ketrew's
[mini-tutorial](http://seb.mondet.org/software/ketrew/#GettingStarted), one can
submit a few Biokepi “example” pipelines to that same Ketrew daemon.

### Using The Demo Application

The application is meant to show how to program with the library, especially
how to build pipelines with [Biokepi.Pipeline](src/lib/pipeline.ml).

For now the demo looks like a somatic variant calling pipeline (hence
the use of the words “tumor,” “normal,” etc.). They have not been debugged
enough to be used in production.

It is configured with environment variables. It will run the pipelines on a
given machine, accessed through SSH. Unfortunately within the demo one cannot
specify a scheduler interface.

Variables to set:

- `BIOKEPI_DATASET_NAME`: a name for the dataset to be analyzed.
  This is used as a namespace for file-naming and separating work environments
  from other analyses run by Biokepi on target machine.
- `BIOKEPI_NORMAL_R1`, `BIOKEPI_NORMAL_R2`, `BIOKEPI_TUMOR_R1`, and
  `BIOKEPI_TUMOR_R2`: the input files. For now each variable may contain a
  comma-separated list of
  [*.fastq.gz](http://en.wikipedia.org/wiki/FASTQ_format) (absolute) files on
  the running machine.
- `BIOKEPI_SSH_BOX_URI`: an URI describing the machine to run on. For example
  `ssh://SshName//home/user/biokepi-test/metaplay` where:
    - `SshName` would be an entry in the `.ssh/config` of the server running
    Ketrew.
    - `/home/user/biokepi-test/metaplay` is the top-level directory where every
    generated file will go.
- `BIOKEPI_MUTECT_JAR_SCP` or `BIOKEPI_MUTECT_JAR_WGET`: if you use
  [Mutect](http://www.broadinstitute.org/cancer/cga/mutect) (the default
  pipeline does) you need to provide a way to download the JAR file (`biokepi`
  would violate its non-free license by doing it itself). So use something like:
  `BIOKEPI_MUTECT_JAR_SCP=MyServer:/path/to/mutect.jar`.<br/>
  Same goes for `GATK` (with `BIOKEPI_GATK_JAR_{SCP,WGET}`).
- `BIOKEPI_CYCLEDASH_URL`: if you are using
  [Cycledash](https://github.com/hammerlab/cycledash) (i.e. the option `-P`)
  then you need to provide the base URL (Biokepi will append `/runs` and
  `/upload` to that URL).
  
For example:

```shell
export BIOKEPI_DATASET_NAME="CP4242"
export BIOKEPI_NORMAL_R1=/path/to/R1_L001.fastq.gz,/R1_L002.fastq.gz
export BIOKEPI_NORMAL_R2=/path/to/R2_L001.fastq.gz,/R2_L002.fastq.gz
export BIOKEPI_TUMOR_R1=/path/to/R1_L001.fastq.gz,/R1_L002.fastq.gz
export BIOKEPI_TUMOR_R2=/path/to/R2_L001.fastq.gz,/R2_L002.fastq.gz
export BIOKEPI_SSH_BOX_URI=ssh://SshName//home/user/biokepi-test/metaplay
export BIOKEPI_MUTECT_JAR_SCP=MyServer:path/to/mutect.jar
export BIOKEPI_GATK_JAR_WGET=http://example.com/top-secret/gatk.jar
export BIOKEPI_CYCLEDASH_URL=http://cycledash.example.com
```

Then you can run a few predefined somatic pipelines:

    ./biokepi-demo list-named-pipelines

will list the names you can use.

    ./biokepi-demo dump-pipeline -N somatic-crazy

will display the JSON representation of the pipeline named `somatic-crazy`.

    ./biokepi-demo run -N somatic-simple-mutect

should submit a the pipeline to your Ketrew server.

