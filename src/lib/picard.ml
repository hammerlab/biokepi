
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

let mark_duplicates ~(run_with:Machine.t) ~input_bam output_bam =
  let open KEDSL in
  let picard_jar = Machine.get_tool run_with Tool.Default.picard in
  let metrics_path =
    sprintf "%s.%s" (Filename.chop_suffix output_bam ".bam") ".metrics" in
  let sorted_bam =
    Samtools.sort_bam_if_necessary ~run_with input_bam ~by:`Coordinate in
  let program =
    Program.(Tool.(init picard_jar) &&
             shf "java -jar $PICARD_JAR MarkDuplicates \
                  VALIDATION_STRINGENCY=LENIENT \
                  INPUT=%s OUTPUT=%s METRICS_FILE=%s"
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

