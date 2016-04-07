open Biokepi_run_environment
open Common


(* 

   Most of the “implementation” part of this module is in the
   {!Workflow_utilities} module (because functions are used to prepare VCFs
   associated with reference genomres).

*)

(** Concatenate VCFs using ["vcftools concat"]. *)
let vcf_concat ~(run_with:Machine.t) ?more_edges vcfs ~final_vcf =
  let vcftools = Machine.get_tool run_with Machine.Tool.Default.vcftools in
  let host = Machine.(as_host run_with) in
  let run_program = Machine.run_program run_with in
  Workflow_utilities.Vcftools.vcf_concat_no_machine
    ~host ~vcftools ~run_program ?more_edges vcfs ~final_vcf
