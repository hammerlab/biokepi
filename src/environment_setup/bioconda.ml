open Biokepi_run_environment
open Common

let create_bioconda_tool
    ~host ~(run_program : Machine.Make_fun.t) ~install_path
    ?check_bin tool_def =
  let open KEDSL in
  let name, version =
    Machine.Tool.Definition.(get_name tool_def, get_version tool_def)
  in
  let conda_tool_def =
    match version with
    | None -> name
    | Some v -> sprintf "%s=%s" name v
  in
  let main_subdir = name ^ "_conda_dir" in
  let python_version = Conda.(`Python_tool_dependency conda_tool_def) in
  let conda_env =
    Conda.setup_environment ~python_version ~main_subdir install_path
      (name ^ Option.value_map ~default:"" version ~f:(sprintf ".%s"))
  in
  let single_file_check id =
    single_file ~host Conda.(bin_in_conda_environment ~conda_env id)
  in
  let exec_check =
    match check_bin with
    | None -> single_file_check name
    | Some s -> single_file_check s
  in
  let ensure =
    workflow_node exec_check
      ~name:("Installing Bioconda tool: " ^ name)
      ~edges:[ depends_on Conda.(configured ~run_program ~host ~conda_env) ]
      (* No need to install the tool, it should already be in the conda env *)
  in
  let init = Conda.init_env ~conda_env () in
  Machine.Tool.create ~ensure ~init tool_def

let default ~host ~run_program ~install_path () =
  Machine.Tool.(Kit.of_list [
    create_bioconda_tool ~host ~run_program ~install_path 
      ~check_bin:"OptiTypePipeline.py" Default.optitype;
    create_bioconda_tool ~host ~run_program ~install_path
      Default.picard;
    create_bioconda_tool ~host ~run_program ~install_path
      Default.seqtk;
    create_bioconda_tool ~host ~run_program ~install_path
      ~check_bin:"seq2HLA" Default.seq2hla;
    create_bioconda_tool ~host ~run_program ~install_path
      ~check_bin:"snpEff" Default.snpeff;
  ])
