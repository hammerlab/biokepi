Using The Demo Application
==========================

Once all set with Ketrew's
[mini-tutorial](http://hammerlab.org/docs/ketrew/master/index.html#GettingStarted),
one can submit a few Biokepi “example” pipelines to that same Ketrew daemon.


The application is meant to show how to program with the library, especially
how to build pipelines with [Biokepi.Pipeline](src/pipeline_edsl/pipeline.ml).

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

