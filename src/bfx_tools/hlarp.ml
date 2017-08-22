open Biokepi_run_environment
open Common

type hla_result = [
  | `Seq2hla of Seq2HLA.product KEDSL.workflow_node
  | `Optitype of Optitype.product KEDSL.workflow_node
  ]

let run ~(run_with:Machine.t)
    ~(hla_result:hla_result)
    ~output_path
  =
  let open KEDSL in
  let open Ketrew_pure.Target.Volume in
  let hlarp = Machine.get_tool run_with Machine.Tool.Default.hlarp in
  let subcommand, hla_result_directory, hla_result_dep =
    match hla_result with
    | `Optitype v -> "optitype", v#product#path, depends_on v
    | `Seq2hla v -> "seq2HLA", v#product#work_dir_path, depends_on v
  in
  let name = sprintf "Hlarp on %s @ %s" subcommand hla_result_directory in
  let make = Machine.quick_run_program run_with
      ~name
      ~requirements:[`Self_identification ["hlarp"]]
      Program.(
        Machine.Tool.init hlarp
        && shf "hlarp %s %s > %s" subcommand hla_result_directory output_path)
  in
  let edges = [
      hla_result_dep;
      depends_on (Machine.Tool.ensure hlarp);
      on_failure_activate
        (Workflow_utilities.Remove.file ~run_with output_path);
    ] in
  workflow_node ~name ~make
    (single_file output_path ~host:(Machine.as_host run_with))
    ~edges
