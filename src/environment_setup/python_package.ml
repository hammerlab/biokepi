open Biokepi_run_environment
open Common

type install_tool_type = Pip | Conda

type install_source_type =
  | Package_PyPI
  | Package_Source
  | Package_Conda

let bin_in_conda_environment ~install_tools_path command =
  Conda.(environment_path ~install_path:install_tools_path) // "bin" // command

let create_python_tool ~host ~(run_program : Machine.Make_fun.t) ~install_tools_path 
    ?package_loc ?check_bin ?version
    name install_tool install_source =
  let open KEDSL in
  let install_id = match package_loc with None -> name | Some pl -> pl in
  let single_file_check id =
    single_file ~host (bin_in_conda_environment ~install_tools_path id)
  in
  let exec_check = 
    match check_bin with 
    | None -> single_file_check name
    | Some s -> single_file_check s
  in
  let install_command =
    match (install_tool, install_source) with
    | (Pip, Package_PyPI) | (Pip, Package_Source) -> ["pip"; "install"; install_id]
    | (Conda, Package_Conda) -> ["conda"; "install"; install_id]
    | (Conda, Package_PyPI) -> ["conda"; "skeleton"; "pypi"; install_id]
    | _ -> failwith "Installation type not supported."
  in
  let ensure =
    workflow_node exec_check
      ~name:("Installing Python tool: " ^ name)
      ~edges:[depends_on 
        Conda.(configured ~run_program ~host ~install_path:install_tools_path ());]
      ~make:(run_program
        ~requirements:[
          `Internet_access; `Self_identification ["python"; "installation"]
        ]
        Program.(exec install_command))
  in
  let init = Conda.init_biokepi_env install_tools_path in
  Machine.Tool.create Machine.Tool.Definition.(create name) ~ensure ~init

let default ~host ~run_program ~install_tools_path () =
   Machine.Tool.Kit.of_list [
     create_python_tool ~host ~run_program ~install_tools_path "vcf-annotate-tool" Pip Package_PyPI;
   ]

