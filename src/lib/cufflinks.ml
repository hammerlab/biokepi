open Common
open Run_environment
open Workflow_utilities

let run ~reference_build
    ~(run_with:Machine.t)
    ~processors 
    ~bam 
    ~result_prefix =
  let open KEDSL in
  let name = sprintf "cufflinks-%s" (Filename.basename result_prefix) in
  let result_file suffix = result_prefix ^ suffix in
  let output_dir = result_file "-cufflinks_output" in
  let genes_gtf_output = output_dir // "genes.fpkm_tracking" in
  let reference_fasta =
    Machine.get_reference_genome run_with reference_build
    |> Reference_genome.fasta in
  let reference_annotations =
    Machine.get_reference_genome run_with reference_build
    |> Reference_genome.gtf_exn in
  let cufflinks_tool = Machine.get_tool run_with Tool.Default.cufflinks in
  let sorted_bam =
    Samtools.sort_bam_if_necessary ~run_with ~processors ~by:`Coordinate bam in
  let make =
    Machine.run_big_program run_with ~name ~processors
      Program.(
        Tool.init cufflinks_tool
        && shf "mkdir -p %s" output_dir
        && shf "cufflinks \
                -p %d \
                -G %s \
                -o %s \
                %s
                "
          processors
          reference_annotations#product#path
          output_dir
          sorted_bam#product#path
      )
  in
  workflow_node ~name ~make
    (single_file genes_gtf_output ~host:(Machine.as_host run_with))
    ~edges:[
      depends_on bam;
      depends_on reference_fasta;
      depends_on reference_annotations;
      depends_on (Tool.ensure cufflinks_tool);
      depends_on (Samtools.index_to_bai ~run_with sorted_bam);
    ]
