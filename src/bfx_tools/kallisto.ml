(** Workflow-nodes to run {{:https://pachterlab.github.io/kallisto/about.html}kallisto}. *)

open Biokepi_run_environment
open Common

(** Create a kallisto specific index of the transcriptome (cDNA) *)
let index
    ~reference_build
    ~(run_with : Machine.t) =
  let open KEDSL in
  let reference_transcriptome =
    Machine.get_reference_genome run_with reference_build
    |> Reference_genome.cdna_exn in
  let kallisto_tool = Machine.get_tool run_with Machine.Tool.Default.kallisto in
  let name =
    sprintf "kallisto-index-%s" (Filename.basename reference_transcriptome#product#path) in
  let reference_dir = (Filename.dirname reference_transcriptome#product#path) in
  let result = sprintf "%s.kallisto.idx" reference_dir in
  workflow_node ~name
    (single_file ~host:(Machine.(as_host run_with)) result)
    ~edges:[
      on_failure_activate (Workflow_utilities.Remove.file ~run_with result);
      depends_on reference_transcriptome;
      depends_on Machine.Tool.(ensure kallisto_tool);
    ]
    ~make:(Machine.run_big_program run_with ~name
             ~self_ids:["kallisto"; "index"]
             Program.(
               Machine.Tool.(init kallisto_tool)
               && shf "kallisto index -i %s %s"
                 result
                 reference_transcriptome#product#path
             ))

(** Quantify transcript abundance from RNA fastqs, results in abundance.tsv file *)
let run
    ~reference_build
    ?(bootstrap_samples=100)
    ~(run_with:Machine.t)
    ~processors 
    ~fastq 
    ~result_prefix 
    =
  let open KEDSL in
  let name = sprintf "kallisto-%s-bootstrap_%d" (Filename.basename result_prefix) bootstrap_samples in
  let result_file suffix = result_prefix ^ suffix in
  let output_dir = result_file "-kallisto" in
  let abundance_file = output_dir // "abundance.tsv" in
  let kallisto_index = index ~reference_build ~run_with in
  let kallisto_tool = Machine.get_tool run_with Machine.Tool.Default.kallisto in
  let r1_path, r2_path_opt = fastq#product#paths in
  let kallisto_quant_base_cmd = 
      sprintf 
        "kallisto quant \
                -i %s \
                -o %s \
                -b %d \
                -t %d \
                %s"
          kallisto_index#product#path
          output_dir
          bootstrap_samples
          processors
          r1_path
  in
  let kallisto_quant = 
    match r2_path_opt with
    | Some r2_path -> sprintf "%s %s" kallisto_quant_base_cmd r2_path
    | None -> kallisto_quant_base_cmd
  in
  let make =
    Machine.run_big_program run_with ~name ~processors
      ~self_ids:["kallisto"; "quant"]
      Program.(
        Machine.Tool.init kallisto_tool
        && sh kallisto_quant
      )
  in
  workflow_node ~name ~make
    (single_file abundance_file ~host:(Machine.as_host run_with))
    ~edges:[
      on_failure_activate
        (Workflow_utilities.Remove.directory ~run_with output_dir);
      depends_on kallisto_index;
      depends_on (Machine.Tool.ensure kallisto_tool);
    ]
