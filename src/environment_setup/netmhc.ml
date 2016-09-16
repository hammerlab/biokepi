open Biokepi_run_environment
open Common

let rm_path = Workflow_utilities.Remove.path_on_host

(* 
  Tested against:

    netMHC-4.0a.Linux.tar.gz // netMHC-3.4a.Linux.tar.gz
    pickpocket-1.1a.Linux.tar.gz
    netMHCpan-3.0a.Linux.tar.gz // netMHCpan-2.8a.Linux.tar.gz
    netMHCcons-1.1a.Linux.tar.gz

  Do not use custom named archives
  and keep them as they are after you
  download (i.e. no need to insert the
  data folder in them or customize the
  binaries)
*)
type netmhc_file_locations = {
  netmhc: Workflow_utilities.Download.tool_file_location;
  netmhcpan: Workflow_utilities.Download.tool_file_location;
  pickpocket: Workflow_utilities.Download.tool_file_location;
  netmhccons: Workflow_utilities.Download.tool_file_location;
}

(*
  The standard netMHC installation requires
  customizing some of the environment variables
  defined in their main binary files. The following
  functions handle these replacements.
*)
let escape_char ~needle haystack =
  let escfun c = if c = needle then ['\\'; c] else [c] in
  String.rev haystack
  |> String.fold ~init:[] ~f:(fun x c -> (escfun c) @ x)
  |> List.map ~f:String.of_character
  |> String.concat

let replace_value file oldvalue newvalue =
  let escape_slash = escape_char ~needle:'/' in
  let file_org = file in
  let file_bak = file_org ^ ".bak" in
  KEDSL.Program.(
    shf "mv %s %s" file_org file_bak &&
    shf "sed -e 's/%s/%s/g' %s > %s"
      (escape_slash oldvalue) (escape_slash newvalue) file_bak file_org &&
    shf "rm -f %s" file_bak
  )

let replace_env_value file envname newvalue =
  let oldvalue = sprintf "setenv\t%s\t.*" envname in
  let newvalue = sprintf "setenv\t%s\t%s" envname newvalue in
  replace_value file oldvalue newvalue

let extract_location location =
  match location with
  | `Scp l -> l
  | `Wget l -> l
  | `Fail _ -> "NoFile-0.0b.Linux.tar.gz"

(* 
  e.g. input: /path/to/netMHC-3.4a.Linux.tar.gz
  e.g. output: 3
*)
let guess_major_version tool_file_loc =
  let loc = extract_location tool_file_loc in
  try
    let basename = Filename.basename loc in
    let dash_idx = String.find basename ~f:(fun c -> c ='-') in
    match dash_idx with
    | Some i -> String.get basename (i + 1)
    | None -> None
  with _ ->
    ksprintf 
    failwith
    "Error while guessing NetMHC major version from %s"
    loc

(* 
  e.g. input: /path/to/netMHC-3.4a.Linux.tar.gz
  e.g. output: netMHC-3.4

  This guessing game is necessary to build the URL
  for the data files required for a complete installation
  and also for to know what folder gets extracted from the
  archives.
*)
let guess_folder_name tool_file_loc =
  let loc = extract_location tool_file_loc in
  let chop_final_char s =
    let ssub = String.sub s 0 ((String.length s) - 1) in
    match ssub with
    | Some txt -> txt
    | None -> s
  in
  try
    loc (* /path/to/netMHC-3.4a.Linux.tar.gz *)
      |> Filename.basename (* netMHC-3.4a.Linux.tar.gz *)
      |> Filename.chop_extension (* netMHC-3.4a.Linux.tar *)
      |> Filename.chop_extension (* netMHC-3.4a.Linux *)
      |> Filename.chop_extension (* netMHC-3.4a *)
      |> chop_final_char (* netMHC-3.4 *)
  with _ ->
    ksprintf 
      failwith
      "Error while guessing NetMHC folder name from %s"
      loc

(* 
  netMHC tools will be populating this folder,
  so this is an important "tmp" folder!
*)
let tmp_dir install_path = install_path // "tmp"

(* 
  NetMHC tools play nicely against Python 2.x
  and are known to be problematic against Python 3.x.
  For better control, we are using a Conda environment
  where we can ask for specific versions of the python
  to be used. 

  In the (far) future, NetMHC tools might start asking
  for Python 3 and we will make the switch from this
  configuration.
*)

let netmhc_conda_env install_path =
  Conda.(setup_environment
    ~python_version:`Python2
    install_path
    "netmhc_conda")


(*
  The issue with the netMHC tools is that they specifically
  depend on a particular Python version to be able to run.
  And another wrapper script (e.g. vaxrank) might require another
  version of Python and once the wrapper calls one of these binaries
  the Python environment gets mixed up, leading to incompatability
  issues. We would like to create a wrapper script to their own
  wrapper script so that we can ensure the tool gets run within
  the environment we want and not the one that the original wrapper
  provides.

  The following runner/pseudo_binary logic handles this
*)
let netmhc_runner_path install_path = install_path // "biokepi_runner"

let netmhc_runner_script_contents ~binary_name ~binary_path ~conda_env =
  Ketrew_pure.Internal_pervasives.fmt {bash|
#!/bin/bash

# Force use the controlled python environment
OLD_PATH=$PATH
export PATH=%s:$PATH

# Run the netMHC* binary
%s "$@"

export PATH=$OLD_PATH
|bash}
    Conda.((environment_path ~conda_env) // "bin")
    binary_path

let create_netmhc_runner_cmd
    ~binary_name ~binary_path ~conda_env dest =
  let script_contents = 
    netmhc_runner_script_contents ~binary_name ~binary_path ~conda_env
  in
  let cmd = 
    sprintf
      "cat << EOF > %s\
       %s\
       EOF\
      "
      dest
      (escape_char ~needle:'$' script_contents)
  in
  KEDSL.Program.(sh cmd)
(* end of runner_script logic *)
  
let default_netmhc_install
    ~(run_program : Machine.Make_fun.t) ~host ~install_path
    ~tool_file_loc ~binary_name ~example_data_file ~env_setup
    ?(depends=[]) 
    ?(data_folder_name="data") 
    ?(data_folder_dest=".") (* relative to the netMHC folder *)
    () =
  let open KEDSL in
  let tool_name = binary_name in
  let downloaded_file =
    Workflow_utilities.Download.get_tool_file
      ~identifier:tool_name
      ~run_program ~host ~install_path
      tool_file_loc
  in
  let folder_name = guess_folder_name tool_file_loc in
  let cap_name = String.set folder_name 0 'N' in
  let folder_in_url = match cap_name with Some s -> s | None -> folder_name in
  let data_url =
    sprintf
      "http://www.cbs.dtu.dk/services/%s/%s.tar.gz"
      folder_in_url
      data_folder_name
  in
  let (one_data_file, with_data) =
    match example_data_file with
    | Some df -> (data_folder_name // df, true)
    | None -> ("", false)
  in
  let downloaded_data_file =
    Workflow_utilities.Download.wget_untar
    ~run_program ~host 
    ~destination_folder:(install_path // folder_name // data_folder_dest)
    ~tar_contains:one_data_file data_url
  in
  let tool_path = install_path // folder_name in
  let runner_folder = netmhc_runner_path install_path in
  let runner_path = runner_folder // binary_name in
  let binary_path = tool_path // binary_name in
  let fix_script replacement = 
    match replacement with
    | `ENV (e, v) -> replace_env_value binary_name e v
    | `GENERIC (o, n) -> replace_value binary_name o n
  in
  let conda_env = netmhc_conda_env install_path in
  let ensure =
    workflow_node (single_file ~host binary_path)
      ~name:("Install NetMHC tool: " ^ tool_name)
      ~edges:(
        [ depends_on downloaded_file; 
          depends_on Conda.(configured ~run_program ~host ~conda_env);
          on_failure_activate (rm_path ~host install_path); ]
        @ (if with_data then [ depends_on downloaded_data_file; ] else [])
        @ (List.map depends ~f:(fun d -> depends_on d))
      )
      ~make:(run_program
        ~requirements:[
          `Self_identification ["netmhc"; tool_name; "installation"];
        ]
        Program.(
          shf "cd %s" install_path &&
          shf "tar zxf %s" downloaded_file#product#path &&
          shf "cd %s" tool_path &&
          chain (List.map ~f:fix_script env_setup) &&
          shf "chmod +x %s" binary_path &&
          shf "mkdir -p %s" (tmp_dir install_path) &&
          shf "mkdir -p %s" runner_folder &&
          create_netmhc_runner_cmd
            ~binary_name ~binary_path ~conda_env runner_path &&
          shf "chmod +x %s" runner_path
        )
      )
  in
  let init = 
    Program.(
      (* no need to init conda. Runner scripts will do that for us *)
      shf "export PATH=%s:$PATH" runner_folder &&
      shf "export TMPDIR=%s" (tmp_dir install_path)
    )
  in
  (Machine.Tool.create
    Machine.Tool.Definition.(create binary_name)
    ~ensure ~init, binary_path, ensure)

let guess_env_setup
    ~install_path
    ?(tmp_dirname = "tmp")
    ?(home_env = "NMHOME")
    tool_file_loc =
  let folder_name = guess_folder_name tool_file_loc in
  [
    `ENV (home_env, install_path // folder_name);
    `ENV ("TMPDIR", install_path // tmp_dirname);
  ]

let default ~run_program ~host ~install_path ~(files:netmhc_file_locations) () =
  let netmhc_mj = guess_major_version files.netmhc in
  let is_old_netmhc =
    match netmhc_mj with
    (* 4 and above uses the default name *)
    | Some v -> (int_of_string (Char.escaped v)) < 4 
    | None -> true
  in
  let netmhc_env = guess_env_setup ~install_path files.netmhc in
  let older_netmhc =
    default_netmhc_install ~run_program ~host ~install_path
    ~tool_file_loc:files.netmhc ~binary_name:"netMHC"
    ~example_data_file:(Some "SLA-10401/bl50/synlist") 
    ~env_setup:(
      [ `GENERIC ("/usr/local/bin/python2.5", "`which python`") ]
      (* ^ -> to force netMHC binary use whatever python we have *)
      @ netmhc_env
    )
    ~data_folder_name:"net"
    ~data_folder_dest:"etc"
  in
  let newer_netmhc = 
    default_netmhc_install ~run_program ~host ~install_path
    ~tool_file_loc:files.netmhc ~binary_name:"netMHC"
    ~example_data_file:(Some "version") 
    ~env_setup:netmhc_env
    ~data_folder_name:"data"
    ~data_folder_dest:"."
  in
  let netmhc_install_func = 
    if is_old_netmhc then older_netmhc else newer_netmhc
  in
  let (netmhc, netmhc_path, netmhc_install) = netmhc_install_func () in
  let (netmhcpan, netmhcpan_path, netmhcpan_install) =
    default_netmhc_install ~run_program ~host ~install_path
      ~tool_file_loc:files.netmhcpan ~binary_name:"netMHCpan"
      ~example_data_file:(Some "version")
      ~env_setup:(guess_env_setup ~install_path files.netmhcpan) ()
  in
  let (pickpocket, pickpocket_path, pickpocket_install) =
    default_netmhc_install ~run_program ~host ~install_path
      ~tool_file_loc:files.pickpocket ~binary_name:"PickPocket"
      ~example_data_file:None
      ~env_setup:(guess_env_setup ~install_path files.pickpocket) ()
  in
  let cons_env =
    [`ENV ("NETMHC_env", netmhc_path);
     `ENV ("NETMHCpan_env", netmhcpan_path);
     `ENV ("PICKPOCKET_env", pickpocket_path);
    ] @ 
    (guess_env_setup
      ~home_env:"NCHOME" ~install_path files.netmhccons
    )
  in
  let (netmhccons, _, _) =
    default_netmhc_install ~run_program ~host ~install_path
      ~tool_file_loc:files.netmhccons ~binary_name:"netMHCcons"
      ~example_data_file:(Some "BLOSUM50")
      ~env_setup:cons_env
      ~depends:[netmhc_install; netmhcpan_install; pickpocket_install]
      ()
  in
  Machine.Tool.Kit.of_list [netmhc; netmhcpan; pickpocket; netmhccons]