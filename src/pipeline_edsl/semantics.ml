
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

  val fastq :
    sample_name : string ->
    ?fragment_id : string ->
    r1: string ->
    ?r2: string ->
    unit -> [ `Fastq ] repr

  val fastq_gz:
    sample_name : string ->
    ?fragment_id : string ->
    r1: string -> ?r2: string ->
    unit -> [ `Gz of [ `Fastq ] ] repr

  val bam :
    path : string ->
    ?sorting: [ `Coordinate | `Read_name ] ->
    reference_build: string ->
    unit -> [ `Bam ] repr

  (** Input a file containing HLA allelles for Topiary  *)
  val mhc_alleles: [ `File of string | `Names of string list] -> [ `MHC_alleles ] repr

  val gunzip: [ `Gz of 'a] repr -> 'a repr

  val gunzip_concat: ([ `Gz of 'a] list) repr -> 'a repr

  val concat: ('a list) repr -> 'a repr

  val merge_bams: ([ `Bam ] list) repr -> [ `Bam ] repr

  val bam_to_fastq:
    sample_name : string ->
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

  val vcf_annotate_polyphen:
    Biokepi_run_environment.Reference_genome.name ->
    [ `Vcf ] repr ->
    [ `Vcf ] repr

  val isovar:
    ?configuration: Isovar.Configuration.t ->
    Biokepi_run_environment.Reference_genome.name ->
    [ `Vcf ] repr ->
    [ `Bam ] repr ->
    [ `Isovar ] repr

  val topiary:
    ?configuration: Topiary.Configuration.t ->
    Biokepi_run_environment.Reference_genome.name ->
    [ `Vcf ] repr ->
    Topiary.predictor_type ->
    [ `MHC_alleles ] repr ->
    [ `Topiary ] repr

end

