(**
Provide tools via Biopam: https://github.com/solvuu/biopam
*)

open Common
open Run_environment
module K = KEDSL

(* What are we installing via opam. This determines where we look for the
   witness; in [opam_install_path]/package or [opam_install_path]/bin. *)
type tool_type =
  | Library of string
  | Application

type install_target = {
  tool_type : tool_type;
  package : string; (** What do we call 'install opam ' with *)
  witness : string; (** File that must exist after install, ex:
                                  - bowtie exec
                                  - picard.jar *)
  test : (?host:K.Host.t -> string -> K.Command.t) option;
  edges : K.workflow_edge list;

  init_environment : K.Program.t option;
}

let default_test ?host path = K.Command.shell ?host (sprintf "test -e %s" path)

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
  let target ?host ~install_path =
    K.single_file ?host (bin ~install_path)

  (* A workflow to ensure that opam is installed. *)
  let installed ?host ~install_path =
    let url = default_opam_url in
    let opam_exec   = target ?host ~install_path in
    let install_dir = dir ~install_path in
    K.workflow_node opam_exec
      ~name:"Install opam"
      ~make:(K.daemonize ?host
        K.Program.(
          exec ["mkdir"; "-p"; install_dir]
          && exec ["cd"; install_dir]
          && Workflow_utilities.Download.wget_program ~output_filename:"opam" url
          && shf "chmod +x %s" opam_exec#path))
      ~edges:[K.on_failure_activate
                (Workflow_utilities.Remove.path_on_host
                   ~host:K.Host.tmp_on_localhost install_dir)]

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
    kcom ?switch ~install_path K.Program.sh fmt

  let command_shell ?switch ?host ~install_path fmt =
    kcom ?switch ~install_path (K.Command.shell ?host) fmt

  let tool_type_to_variable = function
    | Library _   -> "lib"
    | Application -> "bin"

  (* Answer Opam 'which' questions *)
  let which ~install_path {package; witness; tool_type; _} =
    let v = tool_type_to_variable tool_type in
    let s = kcom ~install_path (fun x -> x) "config var %s:%s" package v in
    (Printf.sprintf "$(%s)" s) // witness

end

let default_biopam_url = "https://github.com/solvuu/biopam.git"

(* A workflow to ensure that biopam is configured. *)
let configured ?(biopam_home=default_biopam_url) ?host ~install_path () =
  let name  = sprintf "Configure biopam to %s" biopam_home in
  let make  =
    K.daemonize ?host
      (Opam.program_sh ~install_path ~switch:false
          "init -n --compiler=0.0.0 biopam %s" biopam_home)
  in
  let edges = [ K.depends_on (Opam.installed ?host ~install_path)] in
  let biopam_is_repo =
    Opam.command_shell ~install_path ?host "repo list | grep biopam"
  in
  let cond  =
    object method is_done = Some (`Command_returns (biopam_is_repo, 0)) end
  in
  K.workflow_node cond ~name ~make ~edges

let install_tool ?host ~install_path
    ({package; test; edges; init_environment; _ } as it) =
  let open KEDSL in
  let edges = depends_on (configured ?host ~install_path ()) :: edges in
  let name = "Installing " ^ package in
  let make =
    daemonize ?host Program.(
        Option.value init_environment ~default:(sh "echo 'Default init'")
        && Opam.program_sh ~install_path "install %s" package
      )
  in
  let path = Opam.which ~install_path it in
  let test = (Option.value test ~default:default_test) ?host path in
  let cond =
    object
      method is_done = Some (`Command_returns (test, 0))
      method path = path
    end
  in
  workflow_node cond ~name ~make ~edges

let provide ?host ~install_path it =
  let install_workflow = install_tool ?host ~install_path it in
  let export_var, path =
    match it.tool_type with
    | Application -> "PATH", (Filename.dirname install_workflow#product#path)
    | Library v   -> v, install_workflow#product#path
  in
  Tool.create Tool.Definition.(biopam it.package)
    ~ensure:install_workflow
    (* The 'FOO:+:' outputs ':' only if ${FOO} is defined. *)
    ~init:KEDSL.Program.(
        Option.value it.init_environment ~default:(sh "echo 'Default init'")
        && shf "export %s=\"%s${%s:+:}${%s}\""
          export_var path export_var export_var
      )

let default ?host ~install_path () =
  let mk ~package ~witness ?test ?(edges=[])
      ?export_var ?init_environment tt =
    provide ?host ~install_path
      { tool_type = tt ; package ;
        witness ; test ; edges ; init_environment }
  in
  let version ?host path =
    K.Command.shell ?host (sprintf "%s --version" path) in
  let need_conda =
    [ K.depends_on (Conda.configured ?host ~install_path ())]
  in
  Tool.Kit.create [
    mk (Library "PICARD_JAR") ~package:"picard" ~witness:"picard.jar";
    mk Application ~package:"bowtie" ~witness:"bowtie" ~test:version;
    mk Application ~package:"seq2HLA" ~witness:"seq2HLA" ~test:version
      ~edges:need_conda (* opam handles bowtie dep. *)
      ~init_environment:(Conda.init_biokepi_env ~install_path);
    mk Application ~package:"optitype" ~witness:"OptiTypePipeline"
      ~edges:need_conda (* opam handles razers3 via seqn. *)
      ~init_environment:KEDSL.Program.(
          Conda.init_biokepi_env ~install_path
          && sh "export OPTITYPE_DATA=$(opam config var lib)"
        );
  ]
