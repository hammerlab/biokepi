open Common
open Run_environment
open Workflow_utilities

let run
    ~reference_build
    ~(run_with:Machine.t)
    ~processors
    ~bam
    ~result_prefix =
  let open KEDSL in
  let name = sprintf "stringtie-%s" (Filename.basename result_prefix) in
  let result_file suffix = result_prefix ^ suffix in
  let output_dir = result_file "-stringtie_output" in
  let output_file_path = output_dir // "output.gtf" in
  let reference_fasta =
    Machine.get_reference_genome run_with reference_build
    |> Reference_genome.fasta in
  let reference_annotations =
    Machine.get_reference_genome run_with reference_build
    |> Reference_genome.gtf_exn in
  let stringtie_tool = Machine.get_tool run_with Tool.Default.stringtie in
  let make =
    Machine.run_program run_with ~name ~processors
      Program.(
        Tool.init stringtie_tool
        && shf "mkdir -p %s" output_dir
        && shf "stringtie %s \
                -p %d \
                -G %s \
                -o %s \
               "
          bam#product#path
          processors
          reference_annotations#product#path
          output_file_path
      )
  in
  workflow_node ~name ~make
    (single_file output_file_path ~host:(Machine.as_host run_with))
    ~edges:[
      depends_on bam;
      depends_on reference_fasta;
      depends_on reference_annotations;
      depends_on (Tool.ensure stringtie_tool);
      depends_on (Samtools.index_to_bai ~run_with bam);
    ]
