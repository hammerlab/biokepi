open Common
open Run_environment
open Workflow_utilities


(* We use this version where we don't yet have a Machine.t, as in
   download_reference_genome.ml *)
let vcf_concat_no_machine
    ~host
    ~vcftools
    ~(run_program : Machine.run_function)
    ?(more_edges = [])
    vcfs
    ~final_vcf =
  let open KEDSL in
  let name = sprintf "merge-vcfs-%s" (Filename.basename final_vcf) in
  let vcf_concat =
    let make =
      run_program ~name
        Program.(
          Tool.(init vcftools)
          && shf "vcf-concat %s > %s"
            (String.concat ~sep:" "
               (List.map vcfs ~f:(fun t -> t#product#path)))
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
  in
  vcf_concat


let vcf_concat ~(run_with:Machine.t) ?more_edges vcfs ~final_vcf =
  let vcftools = Machine.get_tool run_with Tool.Default.vcftools in
  let host = Machine.(as_host run_with) in
  let run_program = Machine.run_program run_with in
  vcf_concat_no_machine ~host ~vcftools ~run_program ?more_edges vcfs ~final_vcf
