open Biokepi_run_environment
open Common

type install_tool_type = Pip | Conda

type install_source_type =
  | Package_PyPI of string
  | Package_Source of string * string
  | Package_Conda of string

let bin_in_conda_environment ~conda_env command =
  Conda.(environment_path ~conda_env) // "bin" // command

let create_python_tool ~host ~(run_program : Machine.Make_fun.t) ~install_path 
    ?check_bin ?version ?(python_version=`Python3)
    (installation:install_tool_type * install_source_type) =
  let open KEDSL in
  let versionize ?version ~sep name = match version with
    | None -> name
    | Some v -> name ^ sep ^ v
  in
  let install_command, name =
    match installation with
    | (Pip, Package_PyPI pname) -> ["pip"; "install"; versionize ?version ~sep:"==" pname], pname
    | (Pip, Package_Source (pname, source)) -> ["pip"; "install"; source], pname
    | (Conda, Package_Conda pname) -> ["conda"; "install"; "-y"; versionize ?version ~sep:"=" pname], pname
    | (Conda, Package_PyPI pname) -> ["conda"; "skeleton"; "pypi"; pname], pname
    | _ -> failwith "Installation type not supported."
  in
  let main_subdir = name ^ "_conda_dir" in
  let conda_env = 
    Conda.setup_environment ~python_version ~main_subdir install_path name
  in
  let single_file_check id =
    single_file ~host (bin_in_conda_environment ~conda_env id)
  in
  let exec_check = 
    match check_bin with 
    | None -> single_file_check name
    | Some s -> single_file_check s
  in
  let ensure =
    workflow_node exec_check
      ~name:("Installing Python tool: " ^ name)
      ~edges:[ depends_on Conda.(configured ~run_program ~host ~conda_env) ]
      ~make:(run_program
        ~requirements:[
          `Internet_access; `Self_identification ["python"; "installation"]
        ]
        Program.(
          Conda.init_env ~conda_env ()
          && exec install_command)
        )
  in
  let init = Conda.init_env ~conda_env () in
  Machine.Tool.create Machine.Tool.Definition.(create name) ~ensure ~init

let default ~host ~run_program ~install_path () =
   Machine.Tool.Kit.of_list [
    create_python_tool ~host ~run_program ~install_path 
      ~version:"0.9.4" (Pip, Package_PyPI "pyensembl");
    create_python_tool ~host ~run_program ~install_path 
      ~version:"0.1.2" (Pip, Package_PyPI "vcf-annotate-polyphen");
    create_python_tool ~host ~run_program ~install_path 
      ~version:"0.1.3" ~check_bin:"isovar-protein-sequences.py"
      (Pip, Package_PyPI "isovar");
    create_python_tool ~host ~run_program ~install_path 
      ~version:"0.0.21" (Pip, Package_PyPI "topiary");
    create_python_tool ~host ~run_program ~install_path 
      ~version:"0.0.1" (Pip, Package_PyPI "vaxrank");
   ]