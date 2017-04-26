
module type Lambda_calculus = sig
  type 'a repr  (* representation type *)

  (* lambda abstract *)
  val lambda : ('a repr -> 'b repr) -> ('a -> 'b) repr
  (* application *)
  val apply : ('a -> 'b) repr -> 'a repr -> 'b repr

  type 'a observation
  val observe : (unit -> 'a repr) -> 'a observation
end

module type Lambda_with_list_operations = sig
  include Lambda_calculus
  val list: ('a repr) list -> 'a list repr
  val list_map: ('a list repr) -> f:('a -> 'b) repr -> ('b list repr)
end

module type Bioinformatics_base = sig

  include Lambda_with_list_operations

  open Biokepi_bfx_tools

  val pair: 'a repr -> 'b repr -> ('a * 'b) repr
  val pair_first: ('a * 'b) repr -> 'a repr
  val pair_second: ('a * 'b) repr -> 'b repr

  (** This is used to opacify the type of the expression. *)
  val to_unit: 'a repr -> unit repr

  val input_url: string -> [ `Raw_file ] repr
  (**
     Decrlare an URL as a input to a a pipeline.

     - If the URL has the scheme ["file://"] or no scheme it will be
       treated as a “local file” (i.e. local to the {!Biokepi.Machine.t}).
     - If the URL has the schemes ["http://"] or ["https://"] the file
       will be downloaded into the work-directory.
     - If the URL has the scheme ["gs://"] the file will be fetched thanks
       to ["gsutil"] (Google Cloud's storage buckets management tool).

     For both ["http(s)://"] and ["gs://"] schemes one can override
     the local filename, by adding a ["filename"] argument to the
     query part of the
     URL. E.g. ["https://data.example.com/my-sample.bam?filename=sample-from-example.bam"].
  *)

  val fastq :
    sample_name : string ->
    ?fragment_id : string ->
    r1: [ `Raw_file ] repr ->
    ?r2: [ `Raw_file ] repr ->
    unit -> [ `Fastq ] repr

  val fastq_gz:
    sample_name : string ->
    ?fragment_id : string ->
    r1: [ `Raw_file ] repr ->
    ?r2: [ `Raw_file ] repr ->
    unit -> [ `Gz of [ `Fastq ] ] repr

  val bam :
    sample_name : string ->
    ?sorting: [ `Coordinate | `Read_name ] ->
    reference_build: string ->
    [ `Raw_file ] repr ->
    [ `Bam ] repr

  val bed :
    [ `Raw_file ] repr ->
    [ `Bed ] repr

  (** Input a file containing HLA allelles for Topiary  *)
  val mhc_alleles:
    [ `File of [ `Raw_file ] repr | `Names of string list] ->
    [ `MHC_alleles ] repr

  val index_bam:
    [ `Bam ] repr ->
    [ `Bai ] repr

  val kallisto:
    reference_build: string ->
    ?bootstrap_samples: int ->
    [ `Fastq ] repr ->
    [ `Kallisto_result ] repr

  val cufflinks:
    ?reference_build: string ->
    [ `Bam ] repr ->
    [ `Cufflinks_result ] repr

  val gunzip: [ `Gz of 'a] repr -> 'a repr

  val gunzip_concat: ([ `Gz of 'a] list) repr -> 'a repr

  val concat: ('a list) repr -> 'a repr

  val merge_bams:
    ?delete_input_on_success: bool ->
    ?attach_rg_tag: bool ->
    ?uncompressed_bam_output: bool ->
    ?compress_level_one: bool ->
    ?combine_rg_headers: bool ->
    ?combine_pg_headers: bool ->
    ([ `Bam ] list) repr -> [ `Bam ] repr

  val bam_to_fastq:
    ?fragment_id : string ->
    [ `SE | `PE ] ->
    [ `Bam ] repr ->
    [ `Fastq ] repr

  val bwa_aln:
    ?configuration: Biokepi_bfx_tools.Bwa.Configuration.Aln.t ->
    reference_build: Biokepi_run_environment.Reference_genome.name ->
    [ `Fastq ] repr ->
    [ `Bam ] repr

  val bwa_mem:
    ?configuration: Biokepi_bfx_tools.Bwa.Configuration.Mem.t ->
    reference_build: Biokepi_run_environment.Reference_genome.name ->
    [ `Fastq ] repr ->
    [ `Bam ] repr

  (** Optimized version of bwa-mem. *)
  val bwa_mem_opt:
    ?configuration: Biokepi_bfx_tools.Bwa.Configuration.Mem.t ->
    reference_build: Biokepi_run_environment.Reference_genome.name ->
    [
      | `Fastq of [ `Fastq ] repr
      | `Fastq_gz of [ `Gz of [ `Fastq ] ] repr
      | `Bam of [ `Bam ] repr * [ `PE | `SE ]
    ] ->
    [ `Bam ] repr

  val star:
    ?configuration: Star.Configuration.Align.t ->
    reference_build: Biokepi_run_environment.Reference_genome.name ->
    [ `Fastq ] repr ->
    [ `Bam ] repr

  val hisat:
    ?configuration: Hisat.Configuration.t ->
    reference_build: Biokepi_run_environment.Reference_genome.name ->
    [ `Fastq ] repr ->
    [ `Bam ] repr

  val mosaik:
    reference_build: Biokepi_run_environment.Reference_genome.name ->
    [ `Fastq ] repr ->
    [ `Bam ] repr

  val stringtie:
    ?configuration: Stringtie.Configuration.t ->
    [ `Bam ] repr ->
    [ `Gtf ] repr

  val gatk_indel_realigner:
    ?configuration : Gatk.Configuration.indel_realigner ->
    [ `Bam ] repr ->
    [ `Bam ] repr

  val gatk_indel_realigner_joint:
    ?configuration : Gatk.Configuration.indel_realigner ->
    ([ `Bam ] * [ `Bam ]) repr ->
    ([ `Bam ] * [ `Bam ]) repr

  val picard_mark_duplicates:
    ?configuration : Picard.Mark_duplicates_settings.t ->
    [ `Bam ] repr ->
    [ `Bam ] repr

  val picard_reorder_sam:
    ?mem_param : string ->
    ?reference_build : string ->
    [ `Bam ] repr ->
    [ `Bam ] repr

  val picard_clean_bam:
    [ `Bam ] repr ->
    [ `Bam ] repr

  val gatk_bqsr:
    ?configuration : Gatk.Configuration.bqsr ->
    [ `Bam ] repr ->
    [ `Bam ] repr

  val seq2hla:
    [ `Fastq ] repr ->
    [ `Seq2hla_result ] repr

  val optitype:
    [`DNA | `RNA] ->
    [ `Fastq ] repr ->
    [ `Optitype_result ] repr

  val hlarp:
    [ `Optitype of [`Optitype_result] repr
    | `Seq2hla of [`Seq2hla_result] repr] ->
    [ `MHC_alleles ] repr

  val filter_to_region:
    [ `Vcf ] repr ->
    [ `Bed ] repr ->
    [ `Vcf ] repr

  (** FreeBayes' bamleftalign utility, which left-normalizes indels. That is,
      indels which could be aligned multiple ways (e.g. AAA_G_GAA is the same as
      AAAG_G_AA) are moved to the left, as in the first example. This is so that
      they can be treated uniformly in post-processing. *)
  val bam_left_align:
    reference_build: string ->
    [ `Bam ] repr ->
    [ `Bam ] repr

  (** Sambamba's view filter tool, used to filter down a BAM to one with reads
      matching some predicate (filter language at https://github.com/lomereiter/sambamba/wiki/%5Bsambamba-view%5D-Filter-expression-syntax).
  *)
  val sambamba_filter:
    filter: Sambamba.Filter.t ->
    [ `Bam ] repr ->
    [ `Bam ] repr

  val gatk_haplotype_caller:
    [ `Bam ] repr ->
    [ `Vcf ] repr

  val mutect:
    ?configuration: Biokepi_bfx_tools.Mutect.Configuration.t ->
    normal: [ `Bam ] repr ->
    tumor: [ `Bam ] repr ->
    unit ->
    [ `Vcf ] repr

  val mutect2:
    ?configuration: Gatk.Configuration.Mutect2.t ->
    normal: [ `Bam ] repr ->
    tumor: [ `Bam ] repr ->
    unit ->
    [ `Vcf ] repr

  val somaticsniper:
    ?configuration: Somaticsniper.Configuration.t ->
    normal: [ `Bam ] repr ->
    tumor: [ `Bam ] repr ->
    unit ->
    [ `Vcf ] repr

  val delly2:
    ?configuration: Delly2.Configuration.t ->
    normal: [ `Bam ] repr ->
    tumor: [ `Bam ] repr ->
    unit ->
    [ `Vcf ] repr
  (** Run delly2 on a tumor/normal sample. *)

  val varscan_somatic:
    ?adjust_mapq : int ->
    normal: [ `Bam ] repr ->
    tumor: [ `Bam ] repr ->
    unit ->
    [ `Vcf ] repr

  val strelka:
    ?configuration: Strelka.Configuration.t ->
    normal: [ `Bam ] repr ->
    tumor: [ `Bam ] repr ->
    unit ->
    [ `Vcf ] repr

  val virmid:
    ?configuration: Virmid.Configuration.t ->
    normal: [ `Bam ] repr ->
    tumor: [ `Bam ] repr ->
    unit ->
    [ `Vcf ] repr

  val muse:
    ?configuration: Muse.Configuration.t ->
    normal: [ `Bam ] repr ->
    tumor: [ `Bam ] repr ->
    unit ->
    [ `Vcf ] repr

  val fastqc: [ `Fastq ] repr -> [ `Fastqc ] repr
  (** Call the FASTQC tool (the result is an output directory custom to the
      tool). *)

  val flagstat: [ `Bam ] repr -> [ `Flagstat ] repr

  val vcf_annotate_polyphen:
    [ `Vcf ] repr ->
    [ `Vcf ] repr

  val isovar:
    ?configuration: Isovar.Configuration.t ->
    [ `Vcf ] repr ->
    [ `Bam ] repr ->
    [ `Isovar ] repr

  val topiary:
    ?configuration: Topiary.Configuration.t ->
    [ `Vcf ] repr list ->
    Topiary.predictor_type ->
    [ `MHC_alleles ] repr ->
    [ `Topiary ] repr

  val vaxrank:
    ?configuration: Vaxrank.Configuration.t ->
    [ `Vcf ] repr list ->
    [ `Bam ] repr ->
    Topiary.predictor_type ->
    [ `MHC_alleles ] repr ->
    [ `Vaxrank ] repr

end

