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
  definition: Machine.Tool.Definition.t;
  tool_type : tool_type;
  package : string; (** What do we call 'install opam ' with *)
  witness : string; (** File that must exist after install, ex:
                                  - bowtie exec
                                  - picard.jar *)
  test : (host:KEDSL.Host.t -> string -> KEDSL.Command.t) option;
  edges : KEDSL.workflow_edge list;

  init_environment : install_path: string -> KEDSL.Program.t;
  requires_conda: bool;
  repository: [ `Biopam | `Opam | `Custom of string ];
  compiler: string option;
}
let install_target
    ?(tool_type = `Application)
    ?test
    ?(edges = [])
    ?(init_environment =
      fun ~install_path -> KEDSL.Program.(sh "echo 'Default Init'"))
    ?(requires_conda = false)
    ~witness
    ?package
    ?(repository = `Biopam)
    ?compiler
    definition = 
  let package =
    match package with
    | Some p -> p
    | None -> Machine.Tool.Definition.to_opam_name definition in
  {definition; tool_type; package; witness; test; edges;
   init_environment; requires_conda; repository; compiler}

let default_test ~host path =
  KEDSL.Command.shell ~host (sprintf "test -e %s" path)

let default_opam_url =
  "https://github.com/ocaml/opam/releases/download/\
   1.2.2/opam-1.2.2-x86_64-Linux"

(* Hide the messy logic of calling opam in here. This should not be exported
   and use the Biopam functions directly.*)
module Opam = struct

  let dir ~install_path = install_path // "opam_dir"
  let bin ~install_path = dir ~install_path // "opam"
  let root ~install_path = dir ~install_path // "opam-root"

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

  let kcom ?switch ~install_path k fmt =
    let bin = bin ~install_path in
    let root = root ~install_path in
    (* Pass the ROOT and SWITCH args as environments to not disrupt the flow of
       the rest of the arguments:
        ie opam --root [root] init -n
       doesn't parse correctly. *)
    ksprintf k
      ("OCAMLRUNPARAM=b OPAMLOCKRETRIES=20000 OPAMBASEPACKAGES= OPAMYES=true \
        OPAMROOT=%s %s %s "
       ^^ fmt)
      root 
      (Option.value_map ~default:"" switch ~f:(sprintf "OPAMSWITCH=%s"))
      bin

  let program_sh ?(never_fail = false) ?switch ~install_path fmt =
    kcom ?switch ~install_path (fun s ->
        KEDSL.Program.sh
          (if never_fail
           then s ^ " | echo 'Never fails'"
           else s))
      fmt

  let command_shell ?switch ~host ~install_path fmt =
    kcom ?switch ~install_path (KEDSL.Command.shell ~host) fmt

  let tool_type_to_variable = function
    | `Library _   -> "lib"
    | `Application -> "bin"

  let switch_of_package p = "switch-" ^ p

  (* Answer Opam 'which' questions *)
  let which ~install_path {package; witness; tool_type; _} =
    let v = tool_type_to_variable tool_type in
    let s =
      let package_name = String.take_while package ~f:((<>) '.') in
      kcom ~switch:(switch_of_package package) ~install_path
        (fun x -> x) "config var %s:%s" package_name v in
    (sprintf "$(%s)" s) // witness

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
      (Opam.program_sh ~install_path
         "init -n --compiler=0.0.0 biopam %s" biopam_home)
  in
  let edges =
    [ KEDSL.depends_on (Opam.installed ~run_program ~host ~install_path)] in
  let opam_has_repo = Opam.command_shell ~install_path ~host "repo list" in
  let cond  =
    object method is_done = Some (`Command_returns (opam_has_repo, 0)) end
  in
  KEDSL.workflow_node cond ~name ~make ~edges

let install_tool ~(run_program : Machine.Make_fun.t) ~host ~install_path
    ({package; test; edges; init_environment; repository; _ } as it) =
  let open KEDSL in
  let switch = Opam.switch_of_package package in
  let alias_of =
    Option.value it.compiler ~default:"0.0.0" in
  let edges =
    let base =
      depends_on (configured ~run_program ~host ~install_path ()) :: edges in
    if it.requires_conda
    then
      depends_on (Conda.configured ~run_program ~host ~install_path ()) :: base
    else base in
  let name = "Installing " ^ package in
  let repo_name, repo_url =
    match repository with
    | `Biopam -> "biopam", default_biopam_url
    | `Opam -> "opam", "https://opam.ocaml.org"
    | `Custom c -> Digest.(string c |> to_hex), c
  in
  let make =
    run_program
      ~requirements:[
        `Internet_access;
        `Self_identification ["opam"; "install"; package];
      ]
      Program.(
        (if it.requires_conda
         then Conda.init_biokepi_env ~install_path
         else sh "echo 'Does not need Conda'")
        (* && init_environment ~install_path *)
        && Opam.program_sh ~never_fail:true ~install_path
          "repo add %s %s" repo_name repo_url
        && Opam.program_sh ~install_path
          "switch %s --alias-of %s" switch alias_of
        && Opam.program_sh ~switch ~install_path "install %s" package
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
  Machine.Tool.create it.definition
    ~ensure:install_workflow
    (* The 'FOO:+:' outputs ':' only if ${FOO} is defined. *)
    ~init:KEDSL.Program.(
        (if it.requires_conda
         then Conda.init_biokepi_env ~install_path
         else sh "echo 'Does not need Conda'")
        && it.init_environment ~install_path
        && shf "export %s=\"%s${%s:+:}${%s}\""
          export_var path export_var export_var
      )

let test_version ~host path =
  KEDSL.Command.shell ~host (sprintf "%s --version" path)

let picard =
  install_target
    ~tool_type:(`Library "PICARD_JAR")
    ~witness:"picard.jar"
    (Machine.Tool.Definition.create "picard" ~version:"1.128")

let bowtie =
  install_target
    ~witness:"bowtie" ~test:test_version
    Machine.Tool.Default.bowtie

let seq2hla =
  install_target
    ~witness:"seq2HLA" ~test:test_version ~requires_conda:true
    ~package:"seq2HLA.2.2" (* we need to uppercase HLA for opam *)
    Machine.Tool.Default.seq2hla

let optitype =
  install_target ~witness:"OptiTypePipeline"
    Machine.Tool.Default.optitype
    ~requires_conda:true
    ~init_environment:KEDSL.Program.(
        fun ~install_path ->
          shf "export OPAMSWITCH=%s" (
            Machine.Tool.Definition.to_opam_name 
              Machine.Tool.Default.optitype |> Opam.switch_of_package)
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
  Machine.Tool.Kit.of_list
    (List.map ~f:(provide ~run_program ~host ~install_path) [
    picard;
    bowtie;
    seq2hla;
    optitype;
  ])

