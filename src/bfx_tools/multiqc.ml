open Biokepi_run_environment
open Common

let run ~(run_with:Machine.t)
    ~report_title
    ~input_folder_list
    ~output_folder
  =
  let open KEDSL in
  let multiqc =
    Machine.get_tool run_with Machine.Tool.Definition.(create "multiqc")
  in
  let input_folders = String.concat ~sep:" " input_folder_list in
  let output_path = output_folder // "multiqc_report.html" in
  let host = Machine.(as_host run_with) in
  let product = single_file output_path ~host in
  let name = sprintf "Combining QC reports for %s" report_title in
  workflow_node product
    ~name
    ~edges:[ depends_on Machine.Tool.(ensure multiqc); ]
    ~make:(
      Machine.run_program run_with ~name
        Program.(
          Machine.Tool.(init multiqc)
          && (
            shf "multiqc -o %s -f -i %s %s"
              output_folder report_title input_folders
           )
        )
    )