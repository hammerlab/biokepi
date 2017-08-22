open Biokepi_run_environment
open Common

let run ~(run_with: Machine.t) ~reference_build ~vcf ~output_vcf =
  let open KEDSL in
  let vap_tool = 
    Machine.get_tool run_with Machine.Tool.Default.vcfannotatepolyphen
  in
  let whessdb = Machine.(get_reference_genome run_with reference_build)
    |> Reference_genome.whess_exn
  in
  let whessdb_path = whessdb#product#path in
  let vcf_path = vcf#product#path in
  let name = sprintf "vcf-annotate-polyphen_%s" (Filename.basename vcf_path) in
  workflow_node
    (transform_vcf vcf#product ~path:output_vcf)
    ~name
    ~edges:[
      depends_on Machine.Tool.(ensure vap_tool);
      depends_on whessdb;
      depends_on vcf;
    ]
    ~make:(
      Machine.run_program run_with ~name
        Program.(
          Machine.Tool.(init vap_tool)
          && shf "vcf-annotate-polyphen %s %s %s" whessdb_path vcf_path output_vcf
        )
    )
