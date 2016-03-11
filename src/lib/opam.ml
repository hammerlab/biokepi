open Common
open Run_environment

let version ~(run_with:Machine.t) =
  let open KEDSL in
  let opam_tool = Machine.get_tool run_with Tool.Default.opam in
  let name = sprintf "opam version" in
  let make =
    Machine.run_program run_with
      Program.(Tool.init opam_tool && sh "opam --version")
  in
  workflow_node ~name ~make without_product
