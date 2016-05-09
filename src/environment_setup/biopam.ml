(**
Provide tools via Biopam: https://github.com/solvuu/biopam
*)

open Biokepi_run_environment
open Common

(* What are we installing via opam. This determines where we look for the
   witness; in [opam_install_path]/package or [opam_install_path]/bin. *)
type tool_type = [
  | `Library of string
  | `Application
]

type install_target = {
  tool_type : tool_type;
  package : string; (** What do we call 'install opam ' with *)
  witness : string; (** File that must exist after install, ex:
                                  - bowtie exec
                                  - picard.jar *)
  test : (host:KEDSL.Host.t -> string -> KEDSL.Command.t) option;
  edges : KEDSL.workflow_edge list;

  init_environment : KEDSL.Program.t option;
}
let install_target
    ?(tool_type = `Application)
    ?test
    ?(edges = [])
    ?init_environment
    ~witness
    package = 
  {tool_type; package; witness; test; edges; init_environment}

let default_test ~host path = KEDSL.Command.shell ~host (sprintf "test -e %s" path)

let default_opam_url = "https://github.com/ocaml/opam/releases/download/1.2.2/opam-1.2.2-x86_64-Linux"

(* Hide the messy logic of calling opam in here. This should not be exported
   and use the Biopam functions directly.*)
module Opam = struct

  let dir ~install_path = install_path // "opam_dir"
  let bin ~install_path = dir ~install_path // "opam"
  let root ~install_path = dir ~install_path // ".opam"

  (* TODO:
     Instead of just making sure that this file exists? Wouldn't it be better
     to make sure that a command from this program gives the right output?
     ie. $ opam --version = 1.2.2 *)
  let target ~host ~install_path =
    KEDSL.single_file ~host (bin ~install_path)

  (* A workflow to ensure that opam is installed. *)
  let installed ~(run_program : Machine.Make_fun.t) ~host ~install_path =
    let url = default_opam_url in
    let opam_exec   = target ~host ~install_path in
    let install_dir = dir ~install_path in
    let open KEDSL in
    workflow_node opam_exec
      ~name:"Install opam"
      ~make:(
        run_program
          ~requirements:[
            `Internet_access;
            `Self_identification ["opam"; "ini"];
          ]
          Program.(
            exec ["mkdir"; "-p"; install_dir]
            && exec ["cd"; install_dir]
            && Workflow_utilities.Download.wget_program ~output_filename:"opam" url
            && shf "chmod +x %s" opam_exec#path))
      ~edges:[
        on_failure_activate
          (Workflow_utilities.Remove.path_on_host ~host install_dir);
      ]

  let kcom ?(switch=true) ~install_path k fmt =
    let bin = bin ~install_path in
    let root = root ~install_path in
    (* Pass the ROOT and SWITCH args as environments to not disrupt the flow of
       the rest of the arguments:
        ie opam --root [root] init -n
       doesn't parse correctly. *)
    if switch then
      ksprintf k ("OPAMROOT=%s OPAMSWITCH=0.0.0 %s " ^^ fmt) root bin
    else
      ksprintf k ("OPAMROOT=%s %s " ^^ fmt) root bin

  let program_sh ?switch ~install_path fmt =
    kcom ?switch ~install_path KEDSL.Program.sh fmt

  let command_shell ?switch ~host ~install_path fmt =
    kcom ?switch ~install_path (KEDSL.Command.shell ~host) fmt

  let tool_type_to_variable = function
    | `Library _   -> "lib"
    | `Application -> "bin"

  (* Answer Opam 'which' questions *)
  let which ~install_path {package; witness; tool_type; _} =
    let v = tool_type_to_variable tool_type in
    let s = kcom ~install_path (fun x -> x) "config var %s:%s" package v in
    (Printf.sprintf "$(%s)" s) // witness

end

let default_biopam_url = "https://github.com/solvuu/biopam.git"

(* A workflow to ensure that biopam is configured. *)
let configured
    ?(biopam_home = default_biopam_url)
    ~(run_program : Machine.Make_fun.t) ~host ~install_path () =
  let name  = sprintf "Configure biopam to %s" biopam_home in
  let make  =
    run_program
      ~requirements:[
        `Internet_access;
        `Self_identification ["opam"; "ini"];
      ]
      (Opam.program_sh ~install_path ~switch:false
         "init -n --compiler=0.0.0 biopam %s" biopam_home)
  in
  let edges =
    [ KEDSL.depends_on (Opam.installed ~run_program ~host ~install_path)] in
  let biopam_is_repo =
    Opam.command_shell ~install_path ~host "repo list | grep biopam"
  in
  let cond  =
    object method is_done = Some (`Command_returns (biopam_is_repo, 0)) end
  in
  KEDSL.workflow_node cond ~name ~make ~edges

let install_tool ~(run_program : Machine.Make_fun.t) ~host ~install_path
    ({package; test; edges; init_environment; _ } as it) =
  let open KEDSL in
  let edges =
    depends_on (configured ~run_program ~host ~install_path ()) :: edges in
  let name = "Installing " ^ package in
  let make =
    run_program
      ~requirements:[
        `Internet_access;
        `Self_identification ["opam"; "install"; package];
      ]
      Program.(
        Option.value init_environment ~default:(sh "echo 'Default init'")
        && Opam.program_sh ~install_path "install %s" package
      )
  in
  let path = Opam.which ~install_path it in
  let test = (Option.value test ~default:default_test) ~host path in
  let cond =
    object
      method is_done = Some (`Command_returns (test, 0))
      method path = path
    end
  in
  workflow_node cond ~name ~make ~edges

let provide ~run_program ~host ~install_path it =
  let install_workflow =
    install_tool ~run_program ~host ~install_path it in
  let export_var, path =
    match it.tool_type with
    | `Application -> "PATH", (Filename.dirname install_workflow#product#path)
    | `Library v   -> v, install_workflow#product#path
  in
  Machine.Tool.create Machine.Tool.Definition.(biopam it.package)
    ~ensure:install_workflow
    (* The 'FOO:+:' outputs ':' only if ${FOO} is defined. *)
    ~init:KEDSL.Program.(
        Option.value it.init_environment ~default:(sh "echo 'Default init'")
        && shf "export %s=\"%s${%s:+:}${%s}\""
          export_var path export_var export_var
      )

let test_version ~host path =
  KEDSL.Command.shell ~host (sprintf "%s --version" path)

let picard =
  install_target
    ~tool_type:(`Library "PICARD_JAR")
    ~witness:"picard.jar"
    "picard"

let bowtie =
  install_target "bowtie" ~witness:"bowtie" ~test:test_version

let seq2hla need_conda init_conda =
  install_target "seq2HLA" ~witness:"seq2HLA" ~test:test_version
      ~edges:need_conda (* opam handles bowtie dep. *)
      ~init_environment:init_conda

let optitype need_conda init_conda ~install_path =
  install_target "optitype" ~witness:"OptiTypePipeline"
    ~edges:need_conda (* opam handles razers3 via seqn. *)
    ~init_environment:KEDSL.Program.(
        init_conda
        && shf "export OPAMROOT=%s" (Opam.root ~install_path)
        && shf "export OPTITYPE_DATA=$(%s config var lib)/optitype"
          (Opam.bin ~install_path)
      )

let default :
  run_program: Machine.Make_fun.t ->
  host: Common.KEDSL.Host.t ->
  install_path: string ->
  unit ->
  _ = fun ~run_program ~host ~install_path () ->
  let need_conda =
    [ KEDSL.depends_on (Conda.configured ~run_program ~host ~install_path ())]
  in
  let init_conda = Conda.init_biokepi_env ~install_path in
  Machine.Tool.Kit.of_list
    (List.map ~f:(provide ~run_program ~host ~install_path) [
    picard;
    bowtie;
    seq2hla need_conda init_conda;
    optitype need_conda init_conda ~install_path;
  ])

