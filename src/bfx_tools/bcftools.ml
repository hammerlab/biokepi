open Biokepi_run_environment
open Common

module Remove = Workflow_utilities.Remove


let bcf_to_vcf ~reference_build ~run_with ~bcf output
  =
  let open KEDSL in
  let bcftools = Machine.get_tool run_with Machine.Tool.Default.bcftools in
  let make =
    Machine.run_big_program run_with
      Program.(Machine.Tool.(init bcftools)
               && shf "bcftools view %s > %s" bcf#product#path output)
  in
  let name = sprintf "bcftools bcf->vcf: %s" (Filename.basename output) in
  workflow_node ~name ~make
    (vcf_file ~host:(Machine.as_host run_with) ~reference_build output)
    ~edges:[depends_on bcf;
            depends_on (Machine.Tool.ensure bcftools);
            on_failure_activate (Remove.file ~run_with output)]
