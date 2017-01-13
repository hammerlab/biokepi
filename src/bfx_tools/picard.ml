open Biokepi_run_environment
open Common

module Remove = Workflow_utilities.Remove



let create_dict ~(run_with:Machine.t) fasta =
  let open KEDSL in
  let picard_create_dict =
    Machine.get_tool run_with Machine.Tool.Default.picard in
  let src = fasta#product#path in
  let dest = sprintf "%s.%s" (Filename.chop_suffix src ".fasta") "dict" in
  let program =
    Program.(Machine.Tool.(init picard_create_dict) &&
             shf "java -jar $PICARD_JAR CreateSequenceDictionary R= %s O= %s"
               (Filename.quote src) (Filename.quote dest)) in
  let name = sprintf "picard-create-dict-%s" Filename.(basename src) in
  let make =
    Machine.run_stream_processor run_with program ~name
      ~self_ids:["picard"; "create-dict"] in
  let host = Machine.(as_host run_with) in
  workflow_node (single_file dest ~host) ~name ~make
    ~edges:[
      depends_on fasta; depends_on Machine.Tool.(ensure picard_create_dict);
      on_failure_activate (Remove.file ~run_with dest);
    ]

(* Sort the given VCF.

- [?sequence_dict] A sequence dictionary by which the VCF will be sorted.
*)
let sort_vcf ~(run_with:Machine.t) ?(sequence_dict) input_vcf =
  let open KEDSL in
  let picard = Machine.get_tool run_with Machine.Tool.Default.picard in
  let sequence_name =
    match sequence_dict with
    | None -> "default"
    | Some d -> Filename.basename d#product#path
  in
  let src = input_vcf#product#path in
  let input_vcf_base = Filename.basename src in
  let dest =
    sprintf "%s.sorted-by-%s.vcf"
      (Filename.chop_suffix src ".vcf") sequence_name in
  let name = sprintf "picard-sort-vcf-%s-by-%s" input_vcf_base sequence_name in
  let sequence_dict_opt =
    match sequence_dict with
    | None -> ""
    | Some d -> sprintf "SEQUENCE_DICTIONARY= %s" (Filename.quote d#product#path)
  in
  let sequence_dict_edge =
    match sequence_dict with
    | None -> []
    | Some d -> [depends_on d]
  in
  let program =
    Program.(Machine.Tool.(init picard) &&
             (shf "java -jar $PICARD_JAR SortVcf %s I= %s O= %s"
                sequence_dict_opt
                (Filename.quote src) (Filename.quote dest)))
  in
  let host = Machine.(as_host run_with) in
  let make =
    Machine.run_stream_processor
      run_with program ~name ~self_ids:["picard"; "sort-vcf"] in
  workflow_node (single_file dest ~host) ~name ~make
    ~edges:([
      depends_on input_vcf; depends_on Machine.Tool.(ensure picard);
      on_failure_activate (Remove.file ~run_with dest)
    ] @ sequence_dict_edge)

module Mark_duplicates_settings = struct
  type t = {
    name: string;
    tmpdir: string option;
    max_sequences_for_disk_read_ends_map: int;
    max_file_handles_for_read_ends_map: int;
    sorting_collection_size_ratio: float;
    mem_param: string option;
  }
  let factory = {
    name = "factory";
    tmpdir = None;
    max_sequences_for_disk_read_ends_map = 50000;
    max_file_handles_for_read_ends_map = 8000;
    sorting_collection_size_ratio = 0.25;
    mem_param = None;
  }

  let default = {
    factory with
    name = "default";
    max_file_handles_for_read_ends_map = 20_000;
  }

  let to_java_shell_call ~default_tmp_dir t =
    let tmp_dir =
      Option.value t.tmpdir ~default:default_tmp_dir in
    sprintf "TMPDIR=%s \
             MAX_SEQUENCES_FOR_DISK_READ_ENDS_MAP=%d \
             MAX_FILE_HANDLES_FOR_READ_ENDS_MAP=%d \
             SORTING_COLLECTION_SIZE_RATIO=%f \
             java %s -Djava.io.tmpdir=%s "
      tmp_dir
      t.max_sequences_for_disk_read_ends_map
      t.max_file_handles_for_read_ends_map
      t.sorting_collection_size_ratio
      (match t.mem_param with
      | None  -> ""
      | Some some -> sprintf "-Xmx%s" some)
      tmp_dir

end

let mark_duplicates
    ?(settings = Mark_duplicates_settings.default)
    ~(run_with: Machine.t) ~input_bam output_bam_path =
  let open KEDSL in
  let picard_jar = Machine.get_tool run_with Machine.Tool.Default.picard in
  let metrics_path =
    sprintf "%s.%s" (Filename.chop_suffix output_bam_path ".bam") ".metrics" in
  let sorted_bam =
    Samtools.sort_bam_if_necessary ~run_with input_bam ~by:`Coordinate in
  let program =
    let java_call =
      let default_tmp_dir =
        Filename.chop_extension output_bam_path ^ ".tmpdir" in
      Mark_duplicates_settings.to_java_shell_call ~default_tmp_dir settings in
    Program.(Machine.Tool.(init picard_jar) &&
             shf "%s -jar $PICARD_JAR MarkDuplicates \
                  VALIDATION_STRINGENCY=LENIENT \
                  INPUT=%s OUTPUT=%s METRICS_FILE=%s"
               java_call
               (Filename.quote sorted_bam#product#path)
               (Filename.quote output_bam_path)
               metrics_path) in
  let name =
    sprintf "picard-markdups-%s" Filename.(basename input_bam#product#path) in
  let make =
    Machine.run_big_program ~name run_with program
      ~self_ids:["picard"; "mark-duplicates"] in
  let product = transform_bam  input_bam#product output_bam_path in
  workflow_node product
    ~name ~make
    ~edges:[
      depends_on sorted_bam;
      depends_on Machine.Tool.(ensure picard_jar);
      on_failure_activate (Remove.file ~run_with output_bam_path);
      on_failure_activate (Remove.file ~run_with metrics_path);
    ]

let bam_to_fastq ~run_with ~sample_type ~output_prefix input_bam =
  let open KEDSL in
  let sorted_bam =
    Samtools.sort_bam_if_necessary
      ~run_with ~by:`Read_name input_bam in
  let sample_name = input_bam#product#sample_name in
  let fastq_output_options, r1, r2opt =
    match sample_type with
    | `Paired_end ->
      let r1 = sprintf "%s_R1.fastq" output_prefix in
      let r2 = sprintf "%s_R2.fastq" output_prefix in
      ([sprintf "FASTQ=%s" Filename.(quote r1);
        sprintf "SECOND_END_FASTQ=%s" Filename.(quote r2)],
       r1, Some r2)
    | `Single_end ->
      let r1 = sprintf "%s.fastq" output_prefix in
      ([sprintf "FASTQ=%s" r1], r1, None)
  in
  let picard_jar = Machine.get_tool run_with Machine.Tool.Default.picard in
  let program =
    Program.(
      Machine.Tool.(init picard_jar) &&
      shf "mkdir -p %s" (r1 |> Filename.dirname |> Filename.quote)
      && shf "java -jar $PICARD_JAR SamToFastq VALIDATION_STRINGENCY=LENIENT INPUT=%s %s"
        (Filename.quote sorted_bam#product#path)
        (String.concat ~sep:" " fastq_output_options)
    ) in
  let name =
    sprintf "picard-bam2fastq-%s" Filename.(basename input_bam#product#path) in
  let make =
    Machine.run_big_program ~name run_with program
      ~self_ids:["picard"; "bam-to-fastq"] in
  let edges =
    (fun list ->
       match r2opt with
       | Some r2 -> on_failure_activate (Remove.file ~run_with r2) :: list
       | None -> list)
      [
        depends_on sorted_bam;
        depends_on Machine.Tool.(ensure picard_jar);
        on_failure_activate (Remove.file ~run_with r1);
      ]
  in
  workflow_node
    (fastq_reads ~name:sample_name ~host:(Machine.as_host run_with) r1 r2opt)
    ~name ~make ~edges
