open Biokepi_run_environment
open Common

type install_tool_type = Pip | Conda

type tool_def_type = Machine.Tool.Definition.t

type install_source_type =
  | Package_PyPI of tool_def_type
  | Package_Source of tool_def_type * string
  | Package_Conda of tool_def_type

type python_package_spec =
  install_tool_type * install_source_type * (string option)


let create_python_tool ~host ~(run_program : Machine.Make_fun.t) ~install_path
    ?check_bin ?(python_version=`Python_3) package_spec =
  let open KEDSL in
  let pkg_id ?version ~sep name =
    match version with
    | None -> name
    | Some v -> name ^ sep ^ v
  in
  let describe_source ?version name =
    sprintf " # source code for: %s" (pkg_id ~sep:"=" ?version name)
  in
  let src_cmd ~src ?version name =
    ["pip"; "install"; src; describe_source ?version name]
  in
  let pypi_cmd ?version name =
    ["pip"; "install"; pkg_id ~sep:"==" ?version name]
  in
  let conda_cmd ?version name =
    ["conda"; "install"; pkg_id ~sep:"=" ?version name]
  in
  let tool, install_command, custom_bin, base_packages =
    match package_spec with
    | (Pip, Package_Source (tool, src), cbin, pkgs)
      -> (tool, src_cmd ~src, cbin, pkgs)
    | (Pip, Package_PyPI tool, cbin, pkgs) -> (tool, pypi_cmd, cbin, pkgs)
    | (Conda, Package_Conda tool, cbin, pkgs) -> (tool, conda_cmd, cbin, pkgs)
    | _ -> failwith "Installation type not supported."
  in
  let name, version =
    Machine.Tool.Definition.(get_name tool, get_version tool)
  in
  let main_subdir = name ^ "_conda_dir" in
  let conda_env =
    Conda.setup_environment
      ~python_version ~base_packages
      ~main_subdir install_path
      (name ^ Option.value_map ~default:"NOVERSION" version ~f:(sprintf ".%s"))
  in
  let single_file_check id =
    single_file ~host Conda.(bin_in_conda_environment ~conda_env id)
  in
  let exec_check =
    match custom_bin with
    | None -> single_file_check name
    | Some cb -> single_file_check cb
  in
  let ensure =
    workflow_node exec_check
      ~name:("Installing Python tool: " ^ name)
      ~edges:[ depends_on Conda.(configured ~run_program ~host ~conda_env) ]
      ~make:(
        run_program
          ~requirements:[
            `Internet_access;
            `Self_identification ["python"; "installation"];
          ]
          Program.(
            Conda.init_env ~conda_env ()
            && exec (install_command ?version name)
          )
      )
  in
  let init = Conda.init_env ~conda_env () in
  Machine.Tool.create ~init ~ensure tool


(* Versions are part of the machine's default toolkit (see machine.ml) *)
let default_python_packages =
  let open Machine.Tool.Default in
  [
    (Pip, Package_PyPI pyensembl, None, []);
    (Pip, Package_PyPI isovar, Some "isovar-protein-sequences.py", []);
    (Pip, Package_PyPI vaxrank, None, [ ("wktmltopdf", `Latest); ]);
    (Pip, Package_PyPI vcfannotatepolyphen, None, []);
    (Pip, Package_PyPI topiary, None, []);
  ]


let default ~host ~run_program ~install_path () =
  let pkg_to_tool pkg_spec =
    create_python_tool ~host ~run_program ~install_path pkg_spec
  in
  Machine.Tool.Kit.of_list (List.map ~f:pkg_to_tool default_python_packages)
