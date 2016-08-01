
open Nonstd
module String = Sosa.Native_string
let (//) = Filename.concat

(** The link between {!Biokepi.KEDSL} values and EDSL expression types. *)
module File_type_specification = struct
  open Biokepi_run_environment.Common.KEDSL

  type 'a t = ..
  type 'a t +=
    | To_unit: 'a t -> unit t
    | Fastq: fastq_reads workflow_node -> [ `Fastq ] t
    | Bam: bam_file workflow_node -> [ `Bam ] t
    | Vcf: single_file workflow_node -> [ `Vcf ] t
    | Gtf: single_file workflow_node -> [ `Gtf ] t
    | Seq2hla_result:
        Seq2HLA.product workflow_node ->
      [ `Seq2hla_result ] t
    | Optitype_result: unknown_product workflow_node -> [ `Optitype_result ] t
    | Fastqc_result: list_of_files workflow_node -> [ `Fastqc ] t
    | Flagstat_result: single_file workflow_node -> [ `Flagstat ] t
    | Isovar_result: single_file workflow_node -> [ `Isovar ] t
    | Topiary_result: single_file workflow_node -> [ `Topiary ] t
    | Vaxrank_result: 
        Biokepi_bfx_tools.Vaxrank.product workflow_node -> [ `Vaxrank ] t
    | MHC_alleles: single_file workflow_node -> [ `MHC_alleles ] t
    | Gz: 'a t -> [ `Gz of 'a ] t
    | List: 'a t list -> 'a list t
    | Pair: 'a t * 'b t -> ('a * 'b) t
    | Lambda: ('a t -> 'b t) -> ('a -> 'b) t

  let rec to_string : type a. a  t -> string =
    function
    | To_unit a -> sprintf "(to_unit %s)" (to_string a)
    | Fastq _ -> "Fastq"
    | Bam _ -> "Bam"
    | Vcf _ -> "Vcf"
    | Gtf _ -> "Gtf"
    | Seq2hla_result _ -> "Seq2hla_result"
    | Fastqc_result _ -> "Fastqc_result"
    | Flagstat_result _ -> "Flagstat_result"
    | Isovar_result _ -> "Isovar_result"
    | Topiary_result _ -> "Topiary_result"
    | Vaxrank_result _ -> "Vaxrank_result"
    | Optitype_result _ -> "Optitype_result"
    | MHC_alleles _ -> "MHC_alleles"
    | Gz a -> sprintf "(gz %s)" (to_string a)
    | List l ->
      sprintf "[%s]" (List.map l ~f:to_string |> String.concat ~sep:"; ")
    | Pair (a, b) -> sprintf "(%s, %s)" (to_string a) (to_string b)
    | Lambda _ -> "--LAMBDA--"
    | _ -> "##UNKNOWN##"

  let fail_get other name =
    ksprintf failwith "Error while extracting File_type_specification.t \
                       (%s case, in %s), this usually means that the type \
                       has been wrongly extended" (to_string other) name

  let get_fastq : [ `Fastq ] t -> fastq_reads workflow_node = function
  | Fastq b -> b
  | o -> fail_get o "Fastq"

  let get_bam : [ `Bam ] t -> bam_file workflow_node = function
  | Bam b -> b
  | o -> fail_get o "Bam"

  let get_vcf : [ `Vcf ] t -> single_file workflow_node = function
  | Vcf v -> v
  | o -> fail_get o "Vcf"

  let get_gtf : [ `Gtf ] t -> single_file workflow_node = function
  | Gtf v -> v
  | o -> fail_get o "Gtf"

  let get_seq2hla_result : [ `Seq2hla_result ] t ->
    Seq2HLA.product workflow_node =
    function
    | Seq2hla_result v -> v
    | o -> fail_get o "Seq2hla_result"

  let get_fastqc_result : [ `Fastqc ] t -> list_of_files workflow_node =
    function
    | Fastqc_result v -> v
    | o -> fail_get o "Fastqc_result"

  let get_flagstat_result : [ `Flagstat ] t -> single_file workflow_node =
    function
    | Flagstat_result v -> v
    | o -> fail_get o "Flagstat_result"

  let get_isovar_result : [ `Isovar ] t -> single_file workflow_node =
    function
    | Isovar_result v -> v
    | o -> fail_get o "Isovar_result"

  let get_topiary_result : [ `Topiary ] t -> single_file workflow_node =
    function
    | Topiary_result v -> v
    | o -> fail_get o "Topiary_result"

  let get_vaxrank_result : [ `Vaxrank ] t -> Vaxrank.product workflow_node =
    function
    | Vaxrank_result v -> v
    | o -> fail_get o "Vaxrank_result"

  let get_mhc_alleles : [ `MHC_alleles ] t -> single_file workflow_node =
    function
    | MHC_alleles v -> v
    | o -> fail_get o "Topiary_result"

  let get_optitype_result : [ `Optitype_result ] t -> unknown_product workflow_node =
    function
    | Optitype_result v -> v
    | o -> fail_get o "Optitype_result"

  let get_gz : [ `Gz of 'a ] t -> 'a t = function
  | Gz v -> v
  | o -> fail_get o "Gz"

  let get_list :  'a list  t -> 'a t list = function
  | List v -> v
  | o -> fail_get o "List"


  let pair a b = Pair (a, b)
  let pair_first =
    function
    | Pair (a, _) -> a
    | other -> fail_get other "Pair"
  let pair_second =
    function
    | Pair (_, b) -> b
    | other -> fail_get other "Pair"

  let rec as_dependency_edges : type a. a t -> workflow_edge list =
    let one_depends_on wf = [depends_on wf] in
    function
    | To_unit v -> as_dependency_edges v
    | Fastq wf -> one_depends_on wf
    | Bam wf ->   one_depends_on wf
    | Vcf wf ->   one_depends_on wf
    | Gtf wf ->   one_depends_on wf
    | Seq2hla_result wf -> one_depends_on wf
    | Fastqc_result wf -> one_depends_on wf
    | Flagstat_result wf -> one_depends_on wf
    | Isovar_result wf -> one_depends_on wf
    | Optitype_result wf -> one_depends_on wf
    | Topiary_result wf -> one_depends_on wf
    | Vaxrank_result wf -> one_depends_on wf
    | MHC_alleles wf -> one_depends_on wf
    | List l -> List.concat_map l ~f:as_dependency_edges
    | Pair (a, b) -> as_dependency_edges a @ as_dependency_edges b
    | other -> fail_get other "as_dependency_edges"

  let get_unit_workflow :
    name: string ->
    unit t ->
    unknown_product workflow_node =
    fun ~name f ->
      match f with
      | To_unit v ->
        workflow_node without_product
          ~name ~edges:(as_dependency_edges v)
      | other -> fail_get other "get_unit_workflow"

end

open Biokepi_run_environment

module type Compiler_configuration = sig
  val work_dir: string
  val machine : Machine.t
  val map_reduce_gatk_indel_realigner : bool

  (** What to do with input files: copy or link them to the work directory, or
      do nothing. Doing nothing means letting some tools like ["samtools sort"]
      write in the input-file's directory. *)
  val input_files: [ `Copy | `Link | `Do_nothing ]
end


module Defaults = struct
  let map_reduce_gatk_indel_realigner = true
  let input_files = `Link
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

  let to_unit x = To_unit x

  let host = Machine.as_host Config.machine
  let run_with = Config.machine

  let deal_with_input_file (ifile : _ KEDSL.workflow_node) ~make_product =
    let open KEDSL in
    let new_path = Config.work_dir // Filename.basename ifile#product#path in
    let make =
      Machine.quick_run_program Config.machine Program.(
          shf "mkdir -p %s" Config.work_dir
          && (
            match Config.input_files with
            | `Link ->
              shf "cd %s" Config.work_dir
              && shf "ln -s %s" ifile#product#path
            | `Copy ->
              shf "cp %s %s" ifile#product#path new_path
            | `Do_nothing ->
              shf "echo 'No input action on %s'" ifile#product#path
          )
        )
    in
    let host = ifile#product#host in
    let product =
      match Config.input_files with
      | `Do_nothing -> ifile#product
      | `Link | `Copy -> make_product new_path
    in
    let name =
      sprintf "Input file %s (%s)" (Filename.basename new_path)
        (match Config.input_files with
        | `Link -> "link"
        | `Copy -> "copy"
        | `Do_nothing -> "as-is")
    in
    let ensures =
      `Is_verified Condition.(
          chain_and [
            volume_exists
              Volume.(create ~host ~root:Config.work_dir (dir "." []));
            product#is_done
            |> Option.value_exn ~msg:"File without is_done?";
          ]
        )
    in
    workflow_node product ~ensures ~name ~make
      ~edges:[depends_on ifile]

  let fastq
      ~sample_name ?fragment_id ~r1 ?r2 () =
    Fastq (
      let open KEDSL in
      let read n path =
        workflow_node (single_file ~host path)
          ~name:(sprintf "Input: Read%d of %s (%s)" n
                   sample_name (Filename.basename path)) in
      let read1 = read 1 r1 in
      let read2 = Option.map r2 (read 2) in
      let linked_r1 =
        deal_with_input_file read1 ~make_product:(single_file ~host) in
      let linked_r2 =
        Option.map read2 (fun read ->
            deal_with_input_file read ~make_product:(single_file ~host)) in
      fastq_node_of_single_file_nodes
        ?fragment_id ~host ~name:sample_name linked_r1 linked_r2
    )

  let fastq_gz
      ~sample_name ?fragment_id ~r1 ?r2 () =
    Gz (
      fastq
        ~sample_name ?fragment_id ~r1 ?r2 ()
    )

  let bam ~path ~sample_name ?sorting ~reference_build () =
    Bam (
      let open KEDSL in
      let host = Machine.as_host Config.machine in
      let make_product path =
        bam_file ~host ~name:sample_name ?sorting ~reference_build path in
      let input =
        workflow_node (make_product path)
          ~name:(sprintf "Input-bam: %s" (Filename.basename path)) in
      let dealt_with =
        deal_with_input_file input ~make_product in
      dealt_with
    )

  let mhc_alleles how =
    match how with
    | `File path ->
      MHC_alleles (
        let open KEDSL in
        let host = Machine.as_host Config.machine in
        let make_product path = single_file ~host path in
        let input =
          workflow_node (make_product path)
            ~name:(sprintf "Input-MHC-alleles: %s" (Filename.basename path)) in
        let dealt_with =
          deal_with_input_file input ~make_product in
        dealt_with
      )
    | `Names strlist ->
      let path =
        let saninitize =
          String.map
            ~f:(function
              | ('a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '-') as c -> c
              | other -> '_')
        in
        Config.work_dir // sprintf "MHC_allelles_%s.txt"
          (List.map strlist ~f:saninitize |> String.concat ~sep:"_")
      in
      let open KEDSL in
      let host = Machine.as_host Config.machine in
      let product = single_file ~host path in
      let node =
        workflow_node product
          ~name:(sprintf "Inline-MHC-alleles: %s"
                   (String.concat ~sep:", " strlist))
          ~make:(
            Machine.quick_run_program Config.machine Program.(
                let line s =
                  shf "echo %s >> %s" (Filename.quote s) (Filename.quote path) in
                shf "mkdir -p %s" (Filename.dirname path)
                && shf "rm -f %s" (Filename.quote path)
                && chain (List.map ~f:line strlist)
              )
          )
      in
      MHC_alleles node


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
        ~reference_build ~configuration ~result_prefix ~run_with freads
    )

  let bwa_aln
      ?(configuration = Tools.Bwa.Configuration.Aln.default) =
    make_aligner "bwaaln" ~configuration
      ~config_name:Tools.Bwa.Configuration.Aln.name
      ~make_workflow:(
        fun
          ~reference_build
          ~configuration ~result_prefix ~run_with freads ->
          Tools.Bwa.align_to_sam
            ~reference_build ~configuration ~fastq:freads
            ~result_prefix ~run_with ()
          |> Tools.Samtools.sam_to_bam ~reference_build ~run_with
      )

  let bwa_mem
      ?(configuration = Tools.Bwa.Configuration.Mem.default) =
    make_aligner "bwamem" ~configuration
      ~config_name:Tools.Bwa.Configuration.Mem.name
      ~make_workflow:(
        fun
          ~reference_build
          ~configuration ~result_prefix ~run_with freads ->
          Tools.Bwa.mem_align_to_sam
            ~reference_build ~configuration ~input_reads:(`Fastq freads)
            ~result_prefix ~run_with ()
          |> Tools.Samtools.sam_to_bam ~reference_build ~run_with
      )

  let star
      ?(configuration = Tools.Star.Configuration.Align.default) =
    make_aligner "star"
      ~configuration
      ~config_name:Tools.Star.Configuration.Align.name
      ~make_workflow:(
        fun ~reference_build ~configuration ~result_prefix ~run_with fastq ->
          Tools.Star.align ~configuration ~reference_build
            ~fastq ~result_prefix ~run_with ()
      )

  let hisat
      ?(configuration = Tools.Hisat.Configuration.default_v1) =
    make_aligner "hisat"
      ~configuration
      ~config_name:Tools.Hisat.Configuration.name
      ~make_workflow:(
        fun ~reference_build ~configuration ~result_prefix ~run_with fastq ->
          Tools.Hisat.align ~configuration ~reference_build
            ~fastq ~result_prefix ~run_with ()
          |> Tools.Samtools.sam_to_bam ~reference_build ~run_with
      )

  let mosaik =
    make_aligner "mosaik"
      ~configuration:()
      ~config_name:(fun _ -> "default")
      ~make_workflow:(
        fun ~reference_build ~configuration ~result_prefix ~run_with fastq ->
          Tools.Mosaik.align ~reference_build
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
      let fastq_r1 = gunzip (KEDSL.read_1_file_node f) in
      let fastq_r2 = Option.map (KEDSL.read_2_file_node f) ~f:gunzip in
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
        let r1s = List.map fqs ~f:(KEDSL.read_1_file_node) in
        let r2s = List.filter_map fqs ~f:KEDSL.read_2_file_node in
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
           |> String.concat ~sep:""
           |> Digest.string |> Digest.to_hex)
      in
      Bam (Tools.Samtools.merge_bams ~run_with bams output_path)
    | other ->
      fail_get other "To_workflow.merge_bams: not a list of bams?"

  let stringtie ?(configuration = Tools.Stringtie.Configuration.default) bamt =
    let bam = get_bam bamt in
    let result_prefix =
      Filename.chop_extension bam#product#path
      ^ sprintf "_stringtie-%s"
        (configuration.Tools.Stringtie.Configuration.name)
    in
    Gtf (Tools.Stringtie.run ~configuration ~bam ~result_prefix ~run_with ())

  let indel_realigner_function:
    type a. ?configuration: _ -> a KEDSL.bam_or_bams -> a =
    fun
      ?(configuration = Tools.Gatk.Configuration.default_indel_realigner)
      on_what ->
      match Config.map_reduce_gatk_indel_realigner with
      | true ->
        Tools.Gatk.indel_realigner_map_reduce ~run_with ~compress:false
          ~configuration on_what
      | false ->
        Tools.Gatk.indel_realigner ~run_with ~compress:false
          ~configuration on_what

  let gatk_indel_realigner ?configuration bam =
    let input_bam = get_bam bam in
    Bam (indel_realigner_function ?configuration (KEDSL.Single_bam input_bam))

  let gatk_indel_realigner_joint ?configuration bam_pair =
    let bam1 = bam_pair |> pair_first |> get_bam in
    let bam2 = bam_pair |> pair_second |> get_bam in
    let bam_list_node =
      indel_realigner_function (KEDSL.Bam_workflow_list [bam1; bam2])
        ?configuration
    in
    begin match KEDSL.explode_bam_list_node bam_list_node with
    | [realigned_normal; realigned_tumor] ->
      pair (Bam realigned_normal) (Bam realigned_tumor)
    | other ->
      failf "Gatk.indel_realigner did not return the correct list \
             of length 2 (tumor, normal): it gave %d bams"
        (List.length other)
    end

  let picard_mark_duplicates
      ?(configuration = Tools.Picard.Mark_duplicates_settings.default) bam =
    let input_bam = get_bam bam in
    let output_bam =
      (* We assume that the settings do not impact the actual result. *)
      Filename.chop_extension input_bam#product#path ^ "_markdup.bam" in
    Bam (
      Tools.Picard.mark_duplicates ~settings:configuration
        ~run_with ~input_bam output_bam
    )

  let gatk_bqsr ?(configuration = Tools.Gatk.Configuration.default_bqsr) bam =
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
        ~run_with ~input_bam ~output_bam
    )

  let seq2hla fq =
    let fastq = get_fastq fq in
    let r1 = KEDSL.read_1_file_node fastq in
    let r2 =
      match KEDSL.read_2_file_node fastq with
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

  let fastqc fq =
    let fastq = get_fastq fq in
    let output_folder =
      Config.work_dir //
      sprintf "%s-%s_fastqc-result"
        fastq#product#escaped_sample_name
        fastq#product#fragment_id_forced
    in
    Fastqc_result (Tools.Fastqc.run ~run_with ~fastq ~output_folder)

  let flagstat bam =
    let bam = get_bam bam in
    Flagstat_result (Tools.Samtools.flagstat ~run_with bam)

  let vcf_annotate_polyphen reference_build vcf =
    let v = get_vcf vcf in
    let output_vcf = (Filename.chop_extension v#product#path) ^ "_polyphen.vcf" in
    Vcf (
      Tools.Vcfannotatepolyphen.run ~run_with ~reference_build ~vcf:v ~output_vcf
    )

  let isovar ?(configuration=Tools.Isovar.Configuration.default) reference_build vcf bam =
    let v = get_vcf vcf in
    let b = get_bam bam in
    let out_filename =
      sprintf "%s_%s_isovar_%s_result.csv"
        (Filename.chop_extension (Filename.basename v#product#path))
        (Filename.chop_extension (Filename.basename b#product#path))
        (Tools.Isovar.Configuration.name configuration)
    in
    let output_file = Config.work_dir // out_filename in 
    Isovar_result (
      Tools.Isovar.run ~configuration ~run_with ~reference_build ~vcf:v ~bam:b ~output_file
    )

  let topiary ?(configuration=Tools.Topiary.Configuration.default)
      reference_build vcf predictor alleles =
    let v = get_vcf vcf in
    let mhc = get_mhc_alleles alleles in
    let out_filename =
      sprintf "%s_%s_%s_topiary_%s_result.csv"
        (Filename.chop_extension (Filename.basename v#product#path))
        (Tools.Topiary.predictor_to_string predictor)
        (Filename.chop_extension (Filename.basename mhc#product#path))
        (Tools.Topiary.Configuration.name configuration)
    in
    let output_file = Config.work_dir // out_filename in
    Topiary_result (
      Tools.Topiary.run 
        ~configuration ~run_with 
        ~reference_build ~variants_vcf:v ~predictor
        ~alleles_file:mhc 
        ~output:(`CSV output_file)
    )

  let vaxrank
      ?(configuration=Tools.Vaxrank.Configuration.default)
      reference_build vcf bam predictor alleles =
    let v = get_vcf vcf in
    let b = get_bam bam in
    let mhc = get_mhc_alleles alleles in
    let outdir =
      Config.work_dir
      // sprintf "%s_%s_%s_vaxrank_%s_result"
        (Filename.chop_extension (Filename.basename v#product#path))
        (Tools.Topiary.predictor_to_string predictor)
        (Filename.chop_extension (Filename.basename mhc#product#path))
        (Tools.Vaxrank.Configuration.name configuration)
    in
    Vaxrank_result (
      Tools.Vaxrank.run 
        ~configuration ~run_with 
        ~reference_build ~vcf:v ~bam:b ~predictor
        ~alleles_file:mhc 
        ~output_folder:outdir
    )

  let optitype how fq =
    let fastq = get_fastq fq in
    let work_dir =
      Config.work_dir //
      sprintf "%s-%s_optitype-%s-workdir"
        fastq#product#escaped_sample_name
        (match how with `RNA -> "RNA" | `DNA -> "DNA")
        fastq#product#fragment_id_forced
    in
    Optitype_result (
      Tools.Optitype.hla_type
        ~work_dir ~run_with ~run_name:fastq#product#escaped_sample_name ~fastq
        how
      :> KEDSL.unknown_product KEDSL.workflow_node
    )

  let gatk_haplotype_caller bam =
    let input_bam = get_bam bam in
    let result_prefix =
      Filename.chop_extension input_bam#product#path ^ sprintf "_gatkhaplo" in
    Vcf (
      Tools.Gatk.haplotype_caller ~run_with
        ~input_bam ~result_prefix `Map_reduce
    )

  let bam_to_fastq ?fragment_id how bam =
    let input_bam = get_bam bam in
    let sample_type = match how with `SE -> `Single_end | `PE -> `Paired_end in
    let output_prefix =
      Config.work_dir
      // sprintf "%s-b2fq-%s"
        Filename.(chop_extension (basename input_bam#product#path))
        (match how with `PE -> "PE" | `SE -> "SE")
    in
    Fastq (
      Tools.Picard.bam_to_fastq ~run_with ~sample_type
        ~output_prefix input_bam
    )

  let somatic_vc
      name confname default_conf runfun
      ?configuration ~normal ~tumor () =
    let normal_bam = get_bam normal in
    let tumor_bam = get_bam tumor in
    let configuration = Option.value configuration ~default:default_conf in
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
    somatic_vc "mutect"
      Tools.Mutect.Configuration.name
      Tools.Mutect.Configuration.default
      (fun
        ~configuration ~run_with
        ~normal ~tumor ~result_prefix ->
        Tools.Mutect.run ~configuration ~run_with
          ~normal ~tumor ~result_prefix `Map_reduce)

  let mutect2 =
    somatic_vc "mutect2"
      Tools.Gatk.Configuration.Mutect2.name
      Tools.Gatk.Configuration.Mutect2.default
      (fun
        ~configuration ~run_with
        ~normal ~tumor ~result_prefix ->
        Tools.Gatk.mutect2 ~configuration ~run_with
          ~input_normal_bam:normal
          ~input_tumor_bam:tumor
          ~result_prefix `Map_reduce)

  let somaticsniper =
    somatic_vc "somaticsniper"
      Tools.Somaticsniper.Configuration.name
      Tools.Somaticsniper.Configuration.default
      (fun
        ~configuration ~run_with
        ~normal ~tumor ~result_prefix ->
        Tools.Somaticsniper.run
          ~configuration ~run_with ~normal ~tumor ~result_prefix ())

  let strelka =
    somatic_vc "strelka"
      Tools.Strelka.Configuration.name
      Tools.Strelka.Configuration.default
      (fun
        ~configuration ~run_with
        ~normal ~tumor ~result_prefix ->
        Tools.Strelka.run
          ~configuration ~normal ~tumor ~run_with ~result_prefix ())

  let varscan_somatic ?adjust_mapq =
    somatic_vc "varscan_somatic" (fun () ->
        sprintf "Amq%s"
          (Option.value_map adjust_mapq ~default:"N" ~f:Int.to_string))
      ()
      (fun
        ~configuration ~run_with
        ~normal ~tumor ~result_prefix ->
        Tools.Varscan.somatic_map_reduce ?adjust_mapq
          ~run_with ~normal ~tumor ~result_prefix ())
      ~configuration:()

  let muse =
    somatic_vc "muse"
      Tools.Muse.Configuration.name
      (Tools.Muse.Configuration.default `WGS)
      (fun
        ~configuration ~run_with
        ~normal ~tumor ~result_prefix ->
        Tools.Muse.run
          ~configuration
          ~run_with ~normal ~tumor ~result_prefix `Map_reduce)

  let virmid =
    somatic_vc "virmid"
      Tools.Virmid.Configuration.name
      Tools.Virmid.Configuration.default
      (fun
        ~configuration ~run_with
        ~normal ~tumor ~result_prefix ->
        Tools.Virmid.run
          ~configuration ~normal ~tumor ~run_with ~result_prefix ())


end
