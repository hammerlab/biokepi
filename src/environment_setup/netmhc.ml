open Biokepi_run_environment
open Common

type netmhc_file_locations = {
  netmhc: Workflow_utilities.Download.tool_file_location;
  netmhcpan: Workflow_utilities.Download.tool_file_location;
  pickpocket: Workflow_utilities.Download.tool_file_location;
  netmhccons: Workflow_utilities.Download.tool_file_location;
}

let replace_env_value file envname newvalue =
  let escape_slash txt =
    let escfun c = if c = '/' then ['\\'; c] else [c] in
    String.rev txt
    |> String.fold ~init:[] ~f:(fun x c -> (escfun c) @ x)
    |> List.map ~f:String.of_character
    |> String.concat
  in
  let file_org = file in
  let file_bak = file_org ^ ".bak" in
  KEDSL.Program.(
    shf "mv %s %s" file_org file_bak &&
    shf "sed -e 's/setenv %s .*/setenv  %s %s/g' %s > %s"
      envname envname (escape_slash newvalue) file_bak file_org &&
    shf "rm -f %s" file_bak
  )

(* /path/to/netMHC-3.4a.Linux.tar.gz -> netMHC-3.4 *)
let guess_folder_name tool_file_loc =
  let loc = match tool_file_loc with
    | `Scp l -> l
    | `Wget l -> l
    | `Fail _ -> "NoFile-0.0b.Linux.tar.gz"
  in
  let chop_final_char s =
    let ssub = String.sub s 0 ((String.length s) - 1) in
    match ssub with
    | Some txt -> txt
    | None -> s
  in
  loc (* /path/to/netMHC-3.4a.Linux.tar.gz *)
    |> Filename.basename (* netMHC-3.4a.Linux.tar.gz *)
    |> Filename.chop_extension (* netMHC-3.4a.Linux.tar *)
    |> Filename.chop_extension (* netMHC-3.4a.Linux *)
    |> Filename.chop_extension (* netMHC-3.4a *)
    |> chop_final_char (* netMHC-3.4 *)

let tmp_dir install_path = install_path // "tmp"

let default_netmhc_install
    ~(run_program : Machine.Make_fun.t) ~host ~install_path
    ~tool_file_loc ~binary_name ~example_data_file ~env_setup =
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
    "http://www.cbs.dtu.dk/services/" ^ folder_in_url ^ "/data.tar.gz"
  in
  let (one_data_file, with_data) =
    match example_data_file with
    | Some df -> ("data" // df, true)
    | None -> ("", false)
  in
  let downloaded_data_file =
    Workflow_utilities.Download.wget_untar
    ~run_program ~host 
    ~destination_folder:(install_path // folder_name)
    ~tar_contains:one_data_file data_url
  in
  let tool_path = install_path // folder_name in
  let binary_path = tool_path // binary_name in
  let ensure =
    workflow_node (single_file ~host binary_path)
      ~name:("Installing NetMHC tool: " ^ tool_name)
      ~edges:(
        [ depends_on downloaded_file; ] @
        (if with_data then [ depends_on downloaded_data_file; ] else [])
      )
      ~make:(run_program
        ~requirements:[
          `Internet_access; 
          `Self_identification ["netmhc"; tool_name; "installation"];
        ]
        Program.(
          shf "cd %s" install_path &&
          shf "tar zxf %s" downloaded_file#product#path &&
          shf "cd %s" tool_path &&
          chain (
            List.map
              ~f:(fun (e, v) -> replace_env_value binary_name e v)
              env_setup
          )
        )
      )
  in
  let init = Program.(shf "export PATH=%s:$PATH" tool_path) in
  (Machine.Tool.create
    Machine.Tool.Definition.(create binary_name)
    ~ensure ~init, tool_path)

let guess_env_setup
    ~install_path
    ?(tmp_dirname = "tmp")
    ?(home_env = "NMHOME")
    tool_file_loc =
  let folder_name = guess_folder_name tool_file_loc in
  [
    (home_env, folder_name);
    ("TMPDIR", install_path // tmp_dirname);
  ]

let default ~run_program ~host ~install_path ~(files:netmhc_file_locations) () =
  let (netmhc, netmhc_path) =
    default_netmhc_install ~run_program ~host ~install_path
      ~tool_file_loc:files.netmhc ~binary_name:"netMHC"
      ~example_data_file:(Some "version") 
      ~env_setup:(guess_env_setup ~install_path files.netmhc)
  in
  let (netmhcpan, netmhcpan_path) =
    default_netmhc_install ~run_program ~host ~install_path
      ~tool_file_loc:files.netmhcpan ~binary_name:"netMHCpan"
      ~example_data_file:(Some "version")
      ~env_setup:(guess_env_setup ~install_path files.netmhcpan)
  in
  let (pickpocket, pickpocket_path) =
    default_netmhc_install ~run_program ~host ~install_path
      ~tool_file_loc:files.pickpocket ~binary_name:"netMHC"
      ~example_data_file:None
      ~env_setup:(guess_env_setup ~install_path files.pickpocket)
  in
  let cons_env =
    [("NETMHC_env", netmhc_path);
     ("NETMHCpan_env", netmhcpan_path);
     ("PICKPOCKET_env", pickpocket_path);
    ] @ 
    (guess_env_setup
      ~home_env:"NCHOME" ~install_path files.netmhccons
    )
  in
  let (netmhccons, _) =
    default_netmhc_install ~run_program ~host ~install_path
      ~tool_file_loc:files.netmhcpan ~binary_name:"netMHCpan"
      ~example_data_file:(Some "BLOSUM50")
      ~env_setup:cons_env
  in
  Machine.Tool.Kit.of_list [netmhc; netmhcpan; pickpocket; netmhccons]