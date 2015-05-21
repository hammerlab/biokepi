
open Common

open Run_environment

open Workflow_utilities


let create_dict ~(run_with:Machine.t) fasta =
  let open KEDSL in
  let picard_create_dict = Machine.get_tool run_with Tool.Default.picard in
  let src = fasta#product#path in
  let dest = sprintf "%s.%s" (Filename.chop_suffix src ".fasta") "dict" in
  let program =
    Program.(Tool.(init picard_create_dict) &&
             shf "java -jar $PICARD_JAR CreateSequenceDictionary R= %s O= %s"
               (Filename.quote src) (Filename.quote dest)) in
  let name = sprintf "picard-create-dict-%s" Filename.(basename src) in
  let make = Machine.run_program run_with program ~name in
  let host = Machine.(as_host run_with) in
  workflow_node (single_file dest ~host) ~name ~make
    ~edges:[
      depends_on fasta; depends_on Tool.(ensure picard_create_dict);
      on_failure_activate (Remove.file ~run_with dest);
    ]

module Mark_duplicates_settings = struct
  type t = {
    tmpdir: string;
    max_sequences_for_disk_read_ends_map: int;
    max_file_handles_for_read_ends_map: int;
    sorting_collection_size_ratio: float;
    mem_param: string option;
  }
  let factory = {
    tmpdir = "/tmp";
    max_sequences_for_disk_read_ends_map = 50000;
    max_file_handles_for_read_ends_map = 8000;
    sorting_collection_size_ratio = 0.25;
    mem_param = None;
  }

  let default = {
    factory with
    max_file_handles_for_read_ends_map = 20_000;
  }

  let to_java_shell_call t =
    sprintf "TMPDIR=%s \
             MAX_SEQUENCES_FOR_DISK_READ_ENDS_MAP=%d \
             MAX_FILE_HANDLES_FOR_READ_ENDS_MAP=%d \
             SORTING_COLLECTION_SIZE_RATIO=%f \
             java %s "
      t.tmpdir
      t.max_sequences_for_disk_read_ends_map
      t.max_file_handles_for_read_ends_map
      t.sorting_collection_size_ratio
      (match t.mem_param with
      | None  -> ""
      | Some some -> sprintf "-Xmx%s" some)

end
let mark_duplicates
    ?(settings = Mark_duplicates_settings.default)
    ~(run_with: Machine.t) ~input_bam output_bam =
  let open KEDSL in
  let picard_jar = Machine.get_tool run_with Tool.Default.picard in
  let metrics_path =
    sprintf "%s.%s" (Filename.chop_suffix output_bam ".bam") ".metrics" in
  let sorted_bam =
    Samtools.sort_bam_if_necessary ~run_with input_bam ~by:`Coordinate in
  let program =
    let java_call = Mark_duplicates_settings.to_java_shell_call settings in
    Program.(Tool.(init picard_jar) &&
             shf "%s -jar $PICARD_JAR MarkDuplicates \
                  VALIDATION_STRINGENCY=LENIENT \
                  INPUT=%s OUTPUT=%s METRICS_FILE=%s"
               java_call
               (Filename.quote sorted_bam#product#path)
               (Filename.quote output_bam)
               metrics_path) in
  let name =
    sprintf "picard-markdups-%s" Filename.(basename input_bam#product#path) in
  let make = Machine.run_program ~name run_with program in
  let host = Machine.(as_host run_with) in
  workflow_node (bam_file ~sorting:`Coordinate output_bam ~host)
    ~name ~make
    ~edges:[
      depends_on sorted_bam;
      depends_on Tool.(ensure picard_jar);
      on_failure_activate (Remove.file ~run_with output_bam);
      on_failure_activate (Remove.file ~run_with metrics_path);
    ]

