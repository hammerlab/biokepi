open Nonstd
let (//) = Filename.concat

module File_type_specification = struct
  open Biokepi_run_environment.Common.KEDSL

  type 'a t = ..
  type 'a t +=
    | Fastq: fastq_reads workflow_node -> [ `Fastq ] t
    | Bam: bam_file workflow_node -> [ `Bam ] t
    | Vcf: single_file workflow_node -> [ `Vcf ] t
    | Gz: 'a t -> [ `Gz of 'a ] t
    | List: 'a t list -> 'a list t
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

  let get_gz : [ `Gz of 'a ] t -> 'a t = function
  | Gz v -> v
  | _ -> fail_get "Gz"
end

open Biokepi_run_environment

module type Compiler_configuration = sig
  val processors : int
  val work_dir: string
  val machine : Machine.t
end

module Make (Config : Compiler_configuration) 
    : Semantics.Bioinformatics_base
    with type 'a repr = 'a File_type_specification.t and
    type 'a observation = 'a File_type_specification.t
= struct
  open File_type_specification
  module Tools = Biokepi_bfx_tools
  module KEDSL = Common.KEDSL

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

  let bwa_aln
      ?(configuration = Tools.Bwa.Configuration.Aln.default)
      ~reference_build fq =
    let freads = get_fastq fq in
    let result_prefix =
      Config.work_dir // 
      sprintf "%s-%s_bwa-%s"
        freads#product#escaped_sample_name
        (Option.value freads#product#fragment_id ~default:"")
        (Tools.Bwa.Configuration.Aln.name configuration)
    in
    Bam (
      Tools.Bwa.align_to_sam ~reference_build ~processors:Config.processors
        ~configuration
        ~fastq:freads
        ~result_prefix ~run_with ()
      |> Tools.Samtools.sam_to_bam ~reference_build ~run_with
    )

  let gunzip: type a. [ `Gz of a ] t -> a t = fun gz ->
    let open KEDSL in
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
      let product =
        let r2 = Option.map fastq_r2 ~f:(fun r -> r#product#path) in
        fastq_reads ~host
          ~name:f#product#sample_name
          ?fragment_id:f#product#fragment_id
          fastq_r1#product#path r2
      in
      let edges =
        match fastq_r2 with
        | Some r2 -> [depends_on fastq_r1; depends_on r2]
        | None -> [depends_on fastq_r1]
      in
      Fastq (
        workflow_node product
          ~name:(sprintf "Gunzipped-fastq: %s (%s)" 
                   f#product#sample_name 
                   (Option.value f#product#fragment_id
                      ~default:(Filename.basename fastq_r1#product#path)))
          ~edges
      )
    | other ->
      ksprintf failwith "To_workflow.gunzip: non-FASTQ input not implemented"
    end

  let gunzip_concat gzl =
    ksprintf failwith "To_workflow.gunzip_concat: not implemented"

  let concat l =
    ksprintf failwith "To_workflow.concat: not implemented"

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

  let mutect ~configuration ~normal ~tumor =
    let normal_bam = get_bam normal in
    let tumor_bam = get_bam tumor in
    let result_prefix =
      Filename.chop_extension tumor_bam#product#path
      ^ sprintf "_mutect-%s" (configuration.Tools.Mutect.Configuration.name)
    in
    Vcf (
      Tools.Mutect.run ~configuration ~run_with
        ~normal:normal_bam ~tumor:tumor_bam
        ~result_prefix `Map_reduce)
end
