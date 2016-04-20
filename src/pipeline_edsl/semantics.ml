
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

  val gunzip: [ `Gz of 'a] repr -> 'a repr

  val gunzip_concat: ([ `Gz of 'a] list) repr -> 'a repr

  val concat: ('a list) repr -> 'a repr


  val merge_bams: ([ `Bam ] list) repr -> [ `Bam ] repr

  val mutect:
    configuration: Biokepi_bfx_tools.Mutect.Configuration.t ->
    normal: [ `Bam ] repr ->
    tumor: [ `Bam ] repr ->
    [ `Vcf ] repr

end

