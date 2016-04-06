open Biokepi_run_environment
open Common



(** 
   Call a command on a list of [~vcfs] to produce a given [~final_vcf] (hence
   the {i n-to-1} naming).
*)
let vcf_process_n_to_1_no_machine
    ~host
    ~vcftools
    ~(run_program : Machine.Make_fun.t)
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
        Machine.Tool.(init vcftools)
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
      on_failure_activate
        (Workflow_utilities.Remove.path_on_host ~host final_vcf)
      :: depends_on Machine.Tool.(ensure vcftools)
      :: List.map ~f:depends_on vcfs
      @ more_edges)

(**
   Concatenate VCF files. 

   We use this version where we don't yet have a Machine.t, as in
   ["download_reference_genome.ml"].
*)
let vcf_concat_no_machine
    ~host
    ~vcftools
    ~(run_program : Machine.Make_fun.t)
    ?more_edges
    vcfs
    ~final_vcf =
  vcf_process_n_to_1_no_machine
    ~host ~vcftools ~run_program ?more_edges ~vcfs ~final_vcf
    "vcf-concat"

(**
   Sort a VCF file by choromosome position (it uses ["vcf-sort"] which itself
   relies on the ["sort"] unix tool having the ["--version-sort"] option). 

   We use this version where we don't yet have a Machine.t, as in
   ["download_reference_genome.ml"].
*)
let vcf_sort_no_machine
    ~host
    ~vcftools
    ~(run_program : Machine.Make_fun.t)
    ?more_edges
    ~src ~dest () =
  let run_program =
    Machine.Make_fun.with_requirements run_program [`Memory `Big] in 
  vcf_process_n_to_1_no_machine
    ~host ~vcftools ~run_program ?more_edges ~vcfs:[src] ~final_vcf:dest
    "vcf-sort -c"

let vcf_concat ~(run_with:Machine.t) ?more_edges vcfs ~final_vcf =
  let vcftools = Machine.get_tool run_with Machine.Tool.Default.vcftools in
  let host = Machine.(as_host run_with) in
  let run_program = Machine.run_program run_with in
  vcf_concat_no_machine ~host ~vcftools ~run_program ?more_edges vcfs ~final_vcf
