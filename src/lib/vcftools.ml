open Common
open Run_environment
open Workflow_utilities

let vcf_concat ~(run_with:Machine.t) vcfs ~final_vcf =
  let open KEDSL in
  let name = sprintf "merge-vcfs-%s" (Filename.basename final_vcf) in
  let vcftools = Machine.get_tool run_with Tool.Default.vcftools in
  let vcf_concat =
    let make =
      Machine.run_program run_with ~name
        Program.(
          Tool.(init vcftools)
          && shf "vcf-concat %s > %s"
            (String.concat ~sep:" "
               (List.map vcfs ~f:(fun t -> t#product#path)))
            final_vcf
        ) in
    workflow_node ~name
      (single_file final_vcf ~host:Machine.(as_host run_with))
      ~make
      ~edges:(
        on_failure_activate (Remove.file ~run_with final_vcf)
        :: depends_on Tool.(ensure vcftools)
        :: List.map ~f:depends_on vcfs)
  in
  vcf_concat

