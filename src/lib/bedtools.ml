open Common
open Run_environment
open Workflow_utilities


let bamtofastq ~(run_with:Machine.t) ~sample_type ~processors ~output_prefix input_bam =
  let open KEDSL in
  let sorted_bam =
    Samtools.sort_bam_if_necessary
      ~run_with ~processors ~by:`Read_name input_bam in
  let fastq_output_options, r1, r2opt =
    match sample_type with
    | `Paired_end ->
      let r1 = sprintf "%s_R1.fastq" output_prefix in
      let r2 = sprintf "%s_R2.fastq" output_prefix in
      (["-fq"; r1; "-fq2"; r2], r1, Some r2)
    | `Single_end ->
      let r1 = sprintf "%s.fastq" output_prefix in
      (["-fq"; r1], r1, None)
  in
  let bedtools = Machine.get_tool run_with Tool.Default.bedtools in
  let src_bam = input_bam#product#path in
  let program =
    Program.(Tool.(init bedtools)
             && exec ["mkdir"; "-p"; Filename.dirname r1]
             && exec ("bedtools" ::
                      "bamtofastq" ::  "-i" :: src_bam ::
                      fastq_output_options)) in
  let name =
    sprintf "bedtools-bamtofastq-%s"
      Filename.(basename src_bam |> chop_extension) in
  let make = Machine.run_program ~name run_with program in
  let edges = [
    depends_on Tool.(ensure bedtools);
    depends_on input_bam;
    on_failure_activate (Remove.file ~run_with r1);
    on_success_activate (Remove.file ~run_with sorted_bam#product#path);
  ] |> fun list ->
    begin match r2opt with
    | None -> list
    | Some r2 -> 
      on_failure_activate (Remove.file ~run_with r2) :: list
    end
  in
  workflow_node
    (fastq_reads ~host:(Machine.as_host run_with) r1 r2opt)
    ~edges ~name ~make
