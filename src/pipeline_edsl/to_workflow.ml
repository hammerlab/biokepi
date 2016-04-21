
open Nonstd
let (//) = Filename.concat

(** The link between {!Biokepi.KEDSL} values and EDSL expression types. *)
module File_type_specification = struct
  open Biokepi_run_environment.Common.KEDSL

  type 'a t = ..
  type 'a t +=
    | Fastq: fastq_reads workflow_node -> [ `Fastq ] t
    | Bam: bam_file workflow_node -> [ `Bam ] t
    | Vcf: single_file workflow_node -> [ `Vcf ] t
    | Gtf: single_file workflow_node -> [ `Gtf ] t
    | Seq2hla_result: list_of_files workflow_node -> [ `Seq2hla_result ] t
    | Optitype_result: unknown_product workflow_node -> [ `Optitype_result ] t
    | Gz: 'a t -> [ `Gz of 'a ] t
    | List: 'a t list -> 'a list t
    | Pair: 'a t * 'b t -> ('a * 'b) t
    | Lambda: ('a t -> 'b t) -> ('a -> 'b) t

  let fail_get name =
    ksprintf failwith "Error while extracting File_type_specification.t \
                       (%s case), this usually means that the type has been \
                       wrongly extended, and provides more than one \
                       [ `%s ] t case" name name

  let get_fastq : [ `Fastq ] t -> fastq_reads workflow_node = function
  | Fastq b -> b
  | _ -> fail_get "Fastq"

  let get_bam : [ `Bam ] t -> bam_file workflow_node = function
  | Bam b -> b
  | _ -> fail_get "Bam"

  let get_vcf : [ `Vcf ] t -> single_file workflow_node = function
  | Vcf v -> v
  | _ -> fail_get "Vcf"

  let get_gtf : [ `Gtf ] t -> single_file workflow_node = function
  | Gtf v -> v
  | _ -> fail_get "Gtf"

  let get_seq2hla_result : [ `Seq2hla_result ] t -> list_of_files workflow_node =
    function
    | Seq2hla_result v -> v
    | _ -> fail_get "Seq2hla_result"

  let get_optitype_result : [ `Optitype_result ] t -> unknown_product workflow_node =
    function
    | Optitype_result v -> v
    | _ -> fail_get "Optitype_result"

  let get_gz : [ `Gz of 'a ] t -> 'a t = function
  | Gz v -> v
  | _ -> fail_get "Gz"

  let get_list :  'a list  t -> 'a t list = function
  | List v -> v
  | _ -> fail_get "List"


  let pair a b = Pair (a, b)
  let pair_first =
    function
    | Pair (a, _) -> a
    | other -> fail_get "Pair"
  let pair_second =
    function
    | Pair (_, b) -> b
    | other -> fail_get "Pair"

end

open Biokepi_run_environment

module type Compiler_configuration = sig
  val processors : int
  val work_dir: string
  val machine : Machine.t
  val map_reduce_gatk_indel_realigner : bool
end
module Defaults = struct
  let map_reduce_gatk_indel_realigner = true
end


module Make (Config : Compiler_configuration) 
    : Semantics.Bioinformatics_base
    with type 'a repr = 'a File_type_specification.t and
    type 'a observation = 'a File_type_specification.t
= struct
  include File_type_specification
  module Tools = Biokepi_bfx_tools
  module KEDSL = Common.KEDSL

  let failf fmt =
    ksprintf failwith fmt

  type 'a repr = 'a t 
  type 'a observation = 'a repr

  let observe : (unit -> 'a repr) -> 'a observation = fun f -> f ()

  let lambda : ('a repr -> 'b repr) -> ('a -> 'b) repr = fun f ->
    Lambda f

  let apply : ('a -> 'b) repr -> 'a repr -> 'b repr = fun f_repr x ->
    match f_repr with
    | Lambda f -> f x
    | _ -> assert false

  let list : 'a repr list -> 'a list repr = fun l -> List l
  let list_map : 'a list repr -> f:('a -> 'b) repr -> 'b list repr = fun l ~f ->
    match l with
    | List l ->
      List (List.map ~f:(fun v -> apply f v) l)
    | _ -> assert false


  let host = Machine.as_host Config.machine
  let run_with = Config.machine

  let fastq
      ~sample_name ?fragment_id ~r1 ?r2 () =
    Fastq (
      KEDSL.workflow_node (KEDSL.fastq_reads ~host ~name:sample_name r1 r2)
        ~name:(sprintf "Input-fastq: %s (%s)" sample_name 
                 (Option.value fragment_id ~default:(Filename.basename r1)))
    )

  let fastq_gz
      ~sample_name ?fragment_id ~r1 ?r2 () =
    Gz (
      Fastq (
        KEDSL.workflow_node (KEDSL.fastq_reads ~host ~name:sample_name r1 r2)
          ~name:(sprintf "Input-fastq-gz: %s (%s)" sample_name 
                   (Option.value fragment_id ~default:(Filename.basename r1)))
      )
    )

  let bam ~path ?sorting ~reference_build () =
    Bam (
      KEDSL.workflow_node
        (KEDSL.bam_file ~host ?sorting ~reference_build path)
        ~name:(sprintf "Input-bam: %s" (Filename.basename path))
    )

  let make_aligner
      name ~make_workflow ~config_name
      ~configuration ~reference_build fastq =
    let freads = get_fastq fastq in
    let result_prefix =
      Config.work_dir // 
      sprintf "%s-%s_%s-%s"
        freads#product#escaped_sample_name
        (Option.value freads#product#fragment_id ~default:"")
        name
        (config_name configuration)
    in
    Bam (
      make_workflow
        ~reference_build ~processors:Config.processors
        ~configuration
        ~result_prefix ~run_with freads
    )

  let bwa_aln
      ?(configuration = Tools.Bwa.Configuration.Aln.default) =
    make_aligner "bwaaln" ~configuration
      ~config_name:Tools.Bwa.Configuration.Aln.name
      ~make_workflow:(
        fun
          ~reference_build ~processors
          ~configuration ~result_prefix ~run_with freads ->
          Tools.Bwa.align_to_sam
            ~reference_build ~processors
            ~configuration ~fastq:freads
            ~result_prefix ~run_with ()
          |> Tools.Samtools.sam_to_bam ~reference_build ~run_with
      )

  let bwa_mem
      ?(configuration = Tools.Bwa.Configuration.Mem.default) =
    make_aligner "bwamem" ~configuration
      ~config_name:Tools.Bwa.Configuration.Mem.name
      ~make_workflow:(
        fun
          ~reference_build ~processors
          ~configuration ~result_prefix ~run_with freads ->
          Tools.Bwa.mem_align_to_sam
            ~reference_build ~processors
            ~configuration ~fastq:freads
            ~result_prefix ~run_with ()
          |> Tools.Samtools.sam_to_bam ~reference_build ~run_with
      )

  let star =
    make_aligner "star"
      ~config_name:Tools.Star.Configuration.Align.name
      ~make_workflow:(
        fun
          ~reference_build ~processors
          ~configuration ~result_prefix ~run_with fastq ->
          Tools.Star.align ~configuration ~reference_build ~processors
            ~fastq ~result_prefix ~run_with ()
      )

  let hisat =
    make_aligner "hisat"
      ~config_name:Tools.Hisat.Configuration.name
      ~make_workflow:(
        fun
          ~reference_build ~processors
          ~configuration ~result_prefix ~run_with fastq ->
          Tools.Hisat.align ~configuration ~reference_build ~processors
            ~fastq ~result_prefix ~run_with ()
          |> Tools.Samtools.sam_to_bam ~reference_build ~run_with
      )

  let mosaik =
    make_aligner "mosaik"
      ~configuration:()
      ~config_name:(fun _ -> "default")
      ~make_workflow:(
        fun
          ~reference_build ~processors
          ~configuration ~result_prefix ~run_with fastq ->
          Tools.Mosaik.align ~reference_build ~processors
            ~fastq ~result_prefix ~run_with ())

  let gunzip: type a. [ `Gz of a ] t -> a t = fun gz ->
    let inside = get_gz gz in
    begin match inside with
    | Fastq f ->
      let make_result_path read =
        let base = Filename.basename read in
        Config.work_dir //
        (match base with
        | fastqgz when Filename.check_suffix base ".fastq.gz" ->
          Filename.chop_suffix base ".gz"
        | fqz when Filename.check_suffix base ".fqz" ->
          Filename.chop_suffix base ".fqz" ^ ".fastq"
        | other ->
          ksprintf failwith "To_workflow.gunzip: cannot recognize Gz-Fastq \
                             extension: %S" other)
      in
      let gunzip read =
        let result_path = make_result_path read#product#path in
        Workflow_utilities.Gunzip.concat
          ~run_with [read] ~result_path in
      let fastq_r1 = gunzip f#product#r1 in
      let fastq_r2 = Option.map f#product#r2 ~f:gunzip in
      Fastq (
        KEDSL.fastq_node_of_single_file_nodes ~host
          ~name:f#product#sample_name
          ?fragment_id:f#product#fragment_id
          fastq_r1 fastq_r2
      )
    | other ->
      ksprintf failwith "To_workflow.gunzip: non-FASTQ input not implemented"
    end

  let gunzip_concat gzl =
    ksprintf failwith "To_workflow.gunzip_concat: not implemented"

  let concat : type a. a list t -> a t =
    fun l ->
      let l = get_list l in
      begin match l with
      | Fastq first_fastq :: _ as lfq ->
        let fqs = List.map lfq ~f:get_fastq in
        let r1s = List.map fqs ~f:(fun f -> f#product#r1) in
        let r2s = List.filter_map fqs ~f:(fun f -> f#product#r2) in
        (* TODO add some verifications that they have the same number of files?
           i.e. that we are not mixing SE and PE fastqs
        *)
        let concat_files ~read l =
          let result_path =
            Config.work_dir //
            sprintf "%s-Read%d-Concat.fastq"
              first_fastq#product#escaped_sample_name read in
          Workflow_utilities.Cat.concat ~run_with l ~result_path in
        let read_1 = concat_files r1s ~read:1 in
        let read_2 =
          match r2s with [] -> None | more -> Some (concat_files more ~read:2)
        in
        Fastq (
          KEDSL.fastq_node_of_single_file_nodes ~host
            ~name:first_fastq#product#sample_name
            ~fragment_id:"edsl-concat"
            read_1 read_2
        )
      | other ->
        ksprintf failwith "To_workflow.concat: not implemented"
      end

  let merge_bams: [ `Bam ] list t -> [ `Bam ] t =
    function
    | List [ one_bam ] -> one_bam
    | List bam_files ->
      let bams = List.map bam_files ~f:get_bam in
      let output_path =
        let one = List.hd_exn bams in
        Filename.chop_extension one#product#path
        ^ sprintf "-merged-%s.bam"
          (List.map bams ~f:(fun bam -> bam#product#path)
           |> String.concat ""
           |> Digest.string |> Digest.to_hex)
      in
      Bam (Tools.Samtools.merge_bams ~run_with bams output_path)
    | other ->
      fail_get "To_workflow.merge_bams: not a list of bams?"

  let stringtie ~configuration bamt =
    let bam = get_bam bamt in
    let result_prefix =
      Filename.chop_extension bam#product#path
      ^ sprintf "_stringtie-%s"
        (configuration.Tools.Stringtie.Configuration.name)
    in
    Gtf (
      Tools.Stringtie.run
        ~processors:Config.processors ~configuration
        ~bam ~result_prefix ~run_with ()
    )

  let indel_realigner_function:
    type a. configuration: _ -> a KEDSL.bam_or_bams -> a =
    fun ~configuration on_what ->
      match Config.map_reduce_gatk_indel_realigner with
      | true -> 
        Tools.Gatk.indel_realigner_map_reduce 
          ~processors:Config.processors ~run_with ~compress:false
          ~configuration on_what
      | false ->
        Tools.Gatk.indel_realigner
          ~processors:Config.processors ~run_with ~compress:false
          ~configuration on_what

  let gatk_indel_realigner ~configuration bam =
    let input_bam = get_bam bam in
    Bam (indel_realigner_function ~configuration (KEDSL.Single_bam input_bam))

  let gatk_indel_realigner_joint ~configuration bam_pair =
    let bam1 = bam_pair |> pair_first |> get_bam in
    let bam2 = bam_pair |> pair_second |> get_bam in
    let bam_list_node =
      indel_realigner_function (KEDSL.Bam_workflow_list [bam1; bam2])
        ~configuration
    in
    begin match KEDSL.explode_bam_list_node bam_list_node with
    | [realigned_normal; realigned_tumor] ->
      pair (Bam realigned_normal) (Bam realigned_tumor)
    | other ->
      failf "Gatk.indel_realigner did not return the correct list \
             of length 2 (tumor, normal): it gave %d bams"
        (List.length other)
    end

  let picard_mark_duplicates ~configuration bam =
    let input_bam = get_bam bam in
    let output_bam = 
      (* We assume that the settings do not impact the actual result. *)
      Filename.chop_extension input_bam#product#path ^ "_markdup.bam" in
    Bam (
      Tools.Picard.mark_duplicates ~settings:configuration
        ~run_with ~input_bam output_bam
    )

  let gatk_bqsr ~configuration bam =
    let input_bam = get_bam bam in
    let output_bam = 
      let (bqsr, preads) = configuration in
      Filename.chop_extension input_bam#product#path
      ^ sprintf "_bqsr-B%sP%s.bam"
        bqsr.Tools.Gatk.Configuration.Bqsr.name
        preads.Tools.Gatk.Configuration.Print_reads.name
    in
    Bam (
      Tools.Gatk.base_quality_score_recalibrator ~configuration
        ~run_with ~processors:Config.processors ~input_bam ~output_bam
    )

  let seq2hla fq =
    let fastq = get_fastq fq in
    let r1 = fastq#product#r1 in
    let r2 =
      match fastq#product#r2 with
      | Some r -> r
      | None ->
        failf "Seq2HLA doesn't support Single_end_sample(s)."
    in
    let work_dir =
      Config.work_dir //
      sprintf "%s-%s_seq2hla-workdir"
        fastq#product#escaped_sample_name
        fastq#product#fragment_id_forced
    in
    Seq2hla_result (
      Tools.Seq2HLA.hla_type
        ~work_dir ~run_with ~run_name:fastq#product#escaped_sample_name ~r1 ~r2
    )

  let optitype how fq =
    let fastq = get_fastq fq in
    let r1 = fastq#product#r1 in
    let r2 = fastq#product#r2 in
    let work_dir =
      Config.work_dir //
      sprintf "%s-%s_optitype-%s-workdir"
        fastq#product#escaped_sample_name
        (match how with `RNA -> "RNA" | `DNA -> "DNA")
        fastq#product#fragment_id_forced
    in
    Optitype_result (
      Tools.Optitype.hla_type
        ~work_dir ~run_with ~run_name:fastq#product#escaped_sample_name ~r1 ?r2
        how
    )

  let gatk_haplotype_caller bam =
    let input_bam = get_bam bam in
    let result_prefix =
      Filename.chop_extension input_bam#product#path ^ sprintf "_gatkhaplo" in
    Vcf (
      Tools.Gatk.haplotype_caller ~run_with
        ~input_bam ~result_prefix `Map_reduce
    )

  let bam_to_fastq ~sample_name ?fragment_id how bam =
    let input_bam = get_bam bam in
    let sample_type = match how with `SE -> `Single_end | `PE -> `Paired_end in
    let output_prefix =
      Config.work_dir
      // sprintf "%s-b2fq-%s"
        (Filename.chop_extension input_bam#product#path)
        (match how with `PE -> "PE" | `SE -> "SE")
    in
    Fastq (
      Tools.Picard.bam_to_fastq
        ~run_with ~processors:Config.processors ~sample_type
        ~sample_name ~output_prefix input_bam
    )

  let somatic_vc name confname runfun ~configuration ~normal ~tumor =
    let normal_bam = get_bam normal in
    let tumor_bam = get_bam tumor in
    let result_prefix =
      Filename.chop_extension tumor_bam#product#path
      ^ sprintf "_%s-%s" name (confname configuration)
    in
    Vcf (
      runfun
        ~configuration ~run_with
        ~normal:normal_bam ~tumor:tumor_bam ~result_prefix
    )

  let mutect =
    somatic_vc "mutect" Tools.Mutect.Configuration.name
      (fun
        ~configuration ~run_with
        ~normal ~tumor ~result_prefix ->
        Tools.Mutect.run ~configuration ~run_with
          ~normal ~tumor ~result_prefix `Map_reduce)

  let mutect2 =
    somatic_vc "mutect2" Tools.Gatk.Configuration.Mutect2.name
      (fun
        ~configuration ~run_with
        ~normal ~tumor ~result_prefix ->
        Tools.Gatk.mutect2 ~configuration ~run_with
          ~input_normal_bam:normal
          ~input_tumor_bam:tumor
          ~result_prefix `Map_reduce)

  let somaticsniper =
    somatic_vc "somaticsniper" Tools.Somaticsniper.Configuration.name
      (fun
        ~configuration ~run_with
        ~normal ~tumor ~result_prefix ->
        Tools.Somaticsniper.run
          ~configuration ~run_with ~normal ~tumor ~result_prefix ())

  let strelka =
    somatic_vc "strelka" Tools.Strelka.Configuration.name
      (fun
        ~configuration ~run_with
        ~normal ~tumor ~result_prefix ->
        Tools.Strelka.run
          ~configuration ~normal ~tumor
          ~run_with ~result_prefix ~processors:Config.processors ())

  let varscan_somatic ?adjust_mapq =
    somatic_vc "varscan_somatic" (fun () ->
        sprintf "Amq%s"
          (Option.value_map adjust_mapq ~default:"N" ~f:Int.to_string))
      (fun
        ~configuration ~run_with
        ~normal ~tumor ~result_prefix ->
        Tools.Varscan.somatic_map_reduce ?adjust_mapq
          ~run_with ~normal ~tumor ~result_prefix ())
      ~configuration:()

  let muse =
    somatic_vc "muse" Tools.Muse.Configuration.name
      (fun
        ~configuration ~run_with
        ~normal ~tumor ~result_prefix ->
        Tools.Muse.run
          ~configuration
          ~run_with ~normal ~tumor ~result_prefix `Map_reduce)

  let virmid =
    somatic_vc "virmid" Tools.Virmid.Configuration.name
      (fun
        ~configuration ~run_with
        ~normal ~tumor ~result_prefix ->
        Tools.Virmid.run
          ~configuration ~normal ~tumor
          ~run_with ~result_prefix ~processors:Config.processors
          ())


end
