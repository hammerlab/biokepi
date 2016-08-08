open Biokepi_run_environment
open Common


let run ~(run_with:Machine.t)
    ~(hla_typer:[`Seq2hla | `Optitype])
    ~hla_result_directory
    ~output_path ~extract_alleles ()
  =
  let open KEDSL in
  let open Ketrew_pure.Target.Volume in
  let hlarp = Machine.get_tool run_with Machine.Tool.Default.hlarp in
  let subcommand =
    match hla_typer with
    | `Seq2hla -> "seq2HLA"
    | `Optitype -> "optitype"
  in
  let name = sprintf "Hlarp on %s @ %s" subcommand hla_result_directory in
  let make = Machine.quick_run_program run_with
      ~name
      ~requirements:[`Self_identification ["hlarp"]]
      Program.(
        Machine.Tool.init hlarp
        && shf "hlarp %s %s > %s" subcommand hla_result_directory output_path
        && sh (if extract_alleles
               then sprintf
                   "awk -F , '{ gsub(/^[ \t]+|[ \t]+$/,\
                    \"\", $2); print $2} %s' > %s"
                   output_path output_path
               else "")
      )
  in
  workflow_node ~name ~make
    (single_file output_path ~host:(Machine.as_host run_with))
    ~edges:[
      depends_on (Machine.Tool.ensure hlarp);
      on_failure_activate
        (Workflow_utilities.Remove.file ~run_with output_path);
    ]
