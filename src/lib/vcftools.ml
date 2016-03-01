open Common
open Run_environment
open Workflow_utilities



let vcf_process_n_to_1_no_machine
    ~host
    ~vcftools
    ~(run_program : Machine.run_function)
    ?(more_edges = [])
    ~vcfs
    ~final_vcf
    command_prefix
  =
  let open KEDSL in
  let name = sprintf "%s-%s" command_prefix (Filename.basename final_vcf) in
  let make =
    run_program ~name
      Program.(
        Tool.(init vcftools)
        && shf "%s %s > %s"
          command_prefix
          (String.concat ~sep:" "
             (List.map vcfs ~f:(fun t -> Filename.quote t#product#path)))
          final_vcf
      ) in
  workflow_node ~name
    (single_file final_vcf ~host)
    ~make
    ~edges:(
      on_failure_activate (Remove.path_on_host ~host final_vcf)
      :: depends_on Tool.(ensure vcftools)
      :: List.map ~f:depends_on vcfs
      @ more_edges)

(* We use this version where we don't yet have a Machine.t, as in
   download_reference_genome.ml
*)
let vcf_concat_no_machine
    ~host
    ~vcftools
    ~(run_program : Machine.run_function)
    ?more_edges
    vcfs
    ~final_vcf =
  vcf_process_n_to_1_no_machine
    ~host ~vcftools ~run_program ?more_edges ~vcfs ~final_vcf
    "vcf-concat"

let vcf_sort_no_machine
    ~host
    ~vcftools
    ~(run_program : Machine.run_function)
    ?more_edges
    ~src ~dest () =
  vcf_process_n_to_1_no_machine
    ~host ~vcftools ~run_program ?more_edges ~vcfs:[src] ~final_vcf:dest
    "vcf-sort -c"

let vcf_concat ~(run_with:Machine.t) ?more_edges vcfs ~final_vcf =
  let vcftools = Machine.get_tool run_with Tool.Default.vcftools in
  let host = Machine.(as_host run_with) in
  let run_program = Machine.run_program run_with in
  vcf_concat_no_machine ~host ~vcftools ~run_program ?more_edges vcfs ~final_vcf
