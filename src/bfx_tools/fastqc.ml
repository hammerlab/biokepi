open Biokepi_run_environment
open Common

let run ~(run_with:Machine.t) ~fastq ~output_folder =
  let open KEDSL in
  let open Ketrew_pure.Target.Volume in
  let fastqc = Machine.get_tool run_with Machine.Tool.Default.fastqc in
  let paths =
      let r1_path, r2_path_opt = fastq#product#paths in
      match r2_path_opt with
      | Some r2_path -> [r1_path; r2_path]
      | None -> [r1_path]
  in
  let get_file_name path =
    let parts = String.split path ~on:(`Character '/') |> List.rev in
    match parts with
    | [] -> failwith "Couldn't guess the fastq filename from the path."
    | hd :: tl -> hd
  in
  let output_files =
    paths
    |> List.map ~f:get_file_name
    |> List.map ~f:(fun p ->
        Re.replace_string (Re_posix.compile_pat ".fastq$") "_fastqc.html" p)
    |> List.map ~f:(fun p -> output_folder // p)
  in
  let paths_str = String.concat paths ~sep:" " in
  let cmd = sprintf "$FASTQC_BIN --extract -o %s %s" output_folder paths_str in
  let name = sprintf "FastQC_%s" output_folder in
  let make =
    Machine.run_big_program run_with ~name
      ~self_ids:["fastqc"]
      Program.(Machine.Tool.(init fastqc) && shf "mkdir -p %s" output_folder && sh cmd)
  in
  workflow_node ~name ~make
    (list_of_files output_files ~host:(Machine.as_host run_with))
    ~edges: [
      on_failure_activate (Workflow_utilities.Remove.directory ~run_with output_folder);
      depends_on (Machine.Tool.ensure fastqc);
      depends_on fastq;
    ]
