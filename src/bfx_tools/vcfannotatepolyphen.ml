open Biokepi_run_environment
open Common

let run ~(run_with: Machine.t) ~reference_build ~vcf ~output_vcf =
  let open KEDSL in
  let vap_tool =
    Machine.get_tool run_with Machine.Tool.Definition.(create "vcf-annotate-polyphen")
  in
  let whessdb = Machine.(get_reference_genome run_with reference_build)
    |> Reference_genome.whess_exn
  in
  let whessdb_loc = whessdb#product#path in
  let name = sprintf "vcf-annotate-polyphen_%s" (Filename.basename vcf) in
  workflow_node
    (single_file output_vcf ~host:Machine.(as_host run_with))
    ~name
    ~edges:[
      depends_on Machine.Tool.(ensure vap_tool);
    ]
    ~make:(
      Machine.run_program run_with ~name
        Program.(
          Machine.Tool.(init vap_tool)
          && shf "vcf-annotate-polyphen %s %s %s" whessdb_loc vcf output_vcf
        )
    )