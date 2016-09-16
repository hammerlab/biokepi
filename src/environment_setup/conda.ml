(*
  Conda is a Python environment and package manager:
  http://conda.pydata.org/docs/

  We use it to ensure a consistent Python environment for tools that depend
  on Python.
*)

open Biokepi_run_environment
open Common

let rm_path = Workflow_utilities.Remove.path_on_host

type conda_version_type = [
  | `Latest
  | `Version of string
]

type conda_environment_type = {
  name: string;
  python_version: [ `Python2 | `Python3 ];
  channels: string list;
  base_packages: (string * conda_version_type) list;
  banned_packages: string list;
  install_path: string;
  main_subdir: string;
  envs_subdir: string;
}

let setup_environment
  ?(custom_channels = [])
  ?(base_packages = [])
  ?(banned_packages = [])
  ?(main_subdir = "conda_dir")
  ?(envs_subdir = "envs")
  ?(python_version = `Python2)
  install_path
  name =
  let channels = [ "bioconda"; "r" ] @ custom_channels in
  {name; python_version; channels; base_packages; banned_packages; install_path; main_subdir; envs_subdir}

let main_dir ~conda_env = conda_env.install_path // conda_env.main_subdir
let envs_dir ~conda_env = conda_env.install_path // conda_env.envs_subdir
let commands ~conda_env com = main_dir ~conda_env // "bin" // com
let bin ~conda_env = commands ~conda_env "conda"
let activate ~conda_env = commands ~conda_env "activate"
let deactivate ~conda_env = commands ~conda_env "deactivate"
let environment_path ~conda_env = envs_dir ~conda_env // conda_env.name

(* give a conda command. *)
let com ~conda_env fmt =
  Printf.sprintf ("%s " ^^ fmt) (bin ~conda_env)

(* A workflow to ensure that conda is installed. *)
let installed ~(run_program : Machine.Make_fun.t) ~host ~conda_env =
  let open KEDSL in
  let url =
    "https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh" in
  let conda_exec  = single_file ~host (bin ~conda_env) in
  let install_dir = main_dir ~conda_env in
  workflow_node conda_exec
    ~name:(sprintf "Install conda: %s" conda_env.name)
    ~make:(
      run_program
        ~requirements:[
          `Internet_access; `Self_identification ["conda"; "installation"]
        ]
        Program.(
          exec ["mkdir"; "-p"; conda_env.install_path]
          && exec ["rm";"-fr"; install_dir]
          && exec ["cd"; conda_env.install_path]
          && Workflow_utilities.Download.wget_program url
          && shf "bash Miniconda3-latest-Linux-x86_64.sh -b -p %s" install_dir
        )
    )


let configured ~conda_env ~(run_program : Machine.Make_fun.t) ~host =
  let open KEDSL in
  let create_env =
    com ~conda_env "create -y -q --prefix %s python=%d"
      (envs_dir ~conda_env // conda_env.name)
      (match conda_env.python_version with `Python2 -> 2 | `Python3 -> 3)
  in
  let install_package (package, version) =
    Program.(
      shf "conda install -y %s%s"
        package
        (match version with `Latest -> "" | `Version v -> "=" ^ v)
    )
  in
  let force_rm_package package = 
    Program.(shf "conda remove -y --force %s" package)
  in
  let make =
    run_program
      ~requirements:[
        `Internet_access;
        `Self_identification ["conda"; "configuration"];
      ]
      Program.(
        sh create_env
        && shf "source %s %s" (activate ~conda_env) (envs_dir ~conda_env // conda_env.name)
        && chain (List.map ~f:(shf "conda config --add channels %s") conda_env.channels)
        && chain (List.map ~f:install_package conda_env.base_packages)
        && chain (List.map ~f:force_rm_package conda_env.banned_packages)
      )
  in
  let edges = [ depends_on (installed ~run_program ~host ~conda_env) ] in
  let product =
    (single_file ~host (envs_dir ~conda_env // conda_env.name // "bin/conda")
     :> < is_done : Common.KEDSL.Condition.t option >)  in
  let name =
    sprintf "Configure conda: %s" conda_env.name in
  workflow_node product ~make ~name ~edges


let init_env ~conda_env () =
  let prefix = (envs_dir ~conda_env // conda_env.name) in
  (* if we are already within the conda environment we want, do nothing;
     otherwise, activate the new one *)
  KEDSL.Program.(
    shf "[ ${CONDA_PREFIX-none} != \"%s\" ] \
         && source %s %s \
         || echo 'Already in conda env: %s'"
      prefix (activate ~conda_env) prefix prefix
  )

let deactivate_env ~conda_env () =
  let prefix = (envs_dir ~conda_env // conda_env.name) in
  KEDSL.Program.(
    shf "[ ${CONDA_PREFIX-none} == \"%s\" ] \
         && source %s \
         || echo 'Doing nothing. The conda env is not active: %s'"
      prefix (deactivate ~conda_env) prefix
  )
