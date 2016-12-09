open Biokepi_run_environment
open Common

module Remove = Workflow_utilities.Remove


let bam_left_align ~(run_with : Machine.t) ~reference_build ~bam output_file_path =
  let open KEDSL in
  let reference_fasta =
    Machine.get_reference_genome run_with reference_build
    |> Reference_genome.fasta in
  let name =
    sprintf "FreeBayes.bamleftalign %s"
      (Filename.basename bam#product#path) in
  let clean_up = Remove.file ~run_with output_file_path in
  let product =
    KEDSL.bam_file
      ?sorting:bam#product#sorting
      ~reference_build
      ~host:(Machine.as_host run_with) output_file_path in
  let freebayes = Machine.get_tool run_with Machine.Tool.Default.freebayes in
  workflow_node product
    ~name
    ~make:(Machine.run_big_program run_with ~processors:1 ~name
             ~self_ids:["freebayes"; "bamleftalign"]
             Program.(
               Machine.Tool.(init freebayes)
               && shf "cat %s | bamleftalign -c -d -f %s > %s"
                 bam#product#path
                 reference_fasta#product#path
                 output_file_path
             ))
    ~edges:([
        depends_on Machine.Tool.(ensure freebayes);
        depends_on bam;
        depends_on reference_fasta;
        on_failure_activate clean_up;])
