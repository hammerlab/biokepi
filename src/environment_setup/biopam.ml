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
  pin: string option;
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
    ?pin
    definition =
  let package =
    match package with
    | Some p -> p
    | None -> Machine.Tool.Definition.to_opam_name definition in
  {definition; tool_type; package; witness; test; edges;
   init_environment; requires_conda; repository; compiler; pin}

let default_test ~host path =
  KEDSL.Command.shell ~host (sprintf "test -e %s" path)

let default_opam_url =
  "https://github.com/ocaml/opam/releases/download/\
   1.2.2/opam-1.2.2-x86_64-Linux"

let get_conda_env =
  Conda.setup_environment
    ~custom_channels: [ "trung"; "conda-forge" ]
    ~base_packages: [
      ("anaconda-client", `Version "1.2.2");
      ("bcftools", `Version "1.3");
      ("biopython", `Version "1.66");
      ("cairo", `Version "1.12.18");
      ("clyent", `Version "1.2.0");
      ("cycler", `Version "0.10.0");
      ("distribute", `Version "0.6.45");
      ("fontconfig", `Version "2.11.1");
      ("freetype", `Version "2.5.5");
      ("hdf5", `Version "1.8.15.1");
      ("htslib", `Version "1.3");
      ("libgcc", `Version "4.8.5");
      ("libpng", `Version "1.6.17");
      ("libxml2", `Version "2.9.2");
      ("matplotlib", `Version "1.5.1");
      ("mkl", `Version "11.3.1");
      ("ncurses", `Version "5.9");
      ("numexpr", `Version "2.4.6");
      ("numpy", `Version "1.10.4");
      ("openssl", `Version "1.0.2g");
      ("packaging", `Version "16.7");
      ("pandas", `Version "0.17.1");
      ("pixman", `Version "0.32.6");
      ("pycairo", `Version "1.10.0");
      ("pyinstaller", `Version "3.1");
      ("pyomo", `Version "4.3");
      ("pyparsing", `Version "2.0.3");
      ("pyqt", `Version "4.11.4");
      ("pysam", `Version "0.9.0");
      ("pytables", `Version "3.2.2");
      ("python-dateutil", `Version "2.4.2");
      ("pytz", `Version "2015.7");
      ("pyyaml", `Version "3.11");
      ("qt", `Version "4.8.7");
      ("requests", `Version "2.9.1");
      ("samtools", `Version "1.3");
      ("setuptools", `Version "20.1.1");
      ("sip", `Version "4.16.9");
      ("six", `Version "1.10.0");
      ("sqlite", `Version "3.9.2");
      ("tk", `Version "8.5.18");
      ("wheel", `Version "0.29.0");
      ("yaml", `Version "0.1.6");
      ("zlib", `Version "1.2.8");
    ]
    (* see https://github.com/ContinuumIO/anaconda-issues/issues/152#issuecomment-225214743 *)
    ~banned_packages: [ "readline" ] 
    ~python_version:`Python2

(* Hide the messy logic of calling opam in here. This should not be exported
   and use the Biopam functions directly.*)
module Opam = struct

  let dir ~install_path = install_path // "opam_dir"
  let bin ~install_path = dir ~install_path // "opam"
  let root ~install_path name = dir ~install_path // "opam-root-" ^ name

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
            `Self_identification ["opam-installation"];
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

  let kcom ~root_name ~install_path k fmt =
    let bin = bin ~install_path in
    let root = root ~install_path root_name in
    (* 
       - PATH: we add `opam` so that installation scripts can use the tool
       - OCAMLRUNPARAM: we want OCaml backtraces
       - OPAMLOCKRETRIES: installations should concurrently but in case of we
         bump the lock to wait instead of fail
       - OPAMBASEPACKAGES: we make sure opam does not install any package by
         default
       - OPAMYES: answer `y` to all questions (i.e. batch mode)
       - OPAMROOT: our per-package replacement for `~/.opam/`
    *) 
    ksprintf k
      ("PATH=%s:$PATH OCAMLRUNPARAM=b OPAMLOCKRETRIES=20000 OPAMBASEPACKAGES= \
        OPAMYES=true OPAMROOT=%s %s " ^^ fmt)
      (Filename.dirname bin)
      root
      bin

  let program_sh ?(never_fail = false) ~root_name ~install_path fmt =
    kcom ~root_name ~install_path (fun s ->
        KEDSL.Program.sh
          (if never_fail
           then s ^ " | echo 'Never fails'"
           else s))
      fmt

  let command_shell ~root_name ~host ~install_path fmt =
    kcom ~root_name ~install_path (KEDSL.Command.shell ~host) fmt

  let tool_type_to_variable = function
    | `Library _   -> "lib"
    | `Application -> "bin"

  let root_of_package p = "root-" ^ p

  (* Answer Opam 'which' questions *)
  let which ~install_path {package; witness; tool_type; _} =
    let v = tool_type_to_variable tool_type in
    let s =
      let package_name = String.take_while package ~f:((<>) '.') in
      kcom ~root_name:(root_of_package package) ~install_path
        (fun x -> x) "config var %s:%s" package_name v in
    (sprintf "$(%s)" s) // witness

end

let default_biopam_url = "https://github.com/solvuu/biopam.git"

let install_tool ~(run_program : Machine.Make_fun.t) ~host ~install_path
    ({package; test; edges; init_environment; repository; _ } as it) =
  let open KEDSL in
  let conda_env = get_conda_env install_path it.package in
  let run_prog name =
    run_program
      ~requirements:[
        `Internet_access;
        `Self_identification ["opam"; name; package];
      ]
  in
  let root_name = Opam.root_of_package package in
  let default_compiler, repo_url =
    match repository with
    | `Biopam -> "0.0.0", default_biopam_url
    | `Opam -> "4.02.3", "https://opam.ocaml.org"
    | `Custom c -> "4.02.3", c
  in
  let compiler = Option.value it.compiler ~default:default_compiler in
  let pin_command =
    match it.pin with
    | None -> Program.sh "echo 'Package Not Pinned'"
    | Some url ->
      Opam.program_sh ~root_name ~install_path "pin add -n %s %s" package url
  in
  let edges =
    let edges =
      [ KEDSL.depends_on (Opam.installed ~run_program ~host ~install_path)] in
    if it.requires_conda
    then
      depends_on (Conda.configured ~run_program ~host ~conda_env) :: edges
    else edges in
  let name = "Installing " ^ package in
  let make =
    run_prog "install"
      Program.(
        (if it.requires_conda
         then Conda.init_env ~conda_env ()
         else sh "echo 'Does not need Conda'")
        && shf "rm -fr %s" (Filename.quote root_name)
        && Opam.program_sh
          ~install_path ~root_name "init --comp=%s %s"
          compiler (Filename.quote repo_url)
        && pin_command
        && Opam.program_sh ~root_name ~install_path "install %s" package
      )
  in
  let shell_which = Opam.which ~install_path it in
  let test = (Option.value test ~default:default_test) ~host shell_which in
  let cond =
    object
      method is_done = Some (`Command_returns (test, 0))
      method shell_which = shell_which
    end
  in
  workflow_node cond ~name ~make ~edges

let provide ~run_program ~host ~install_path it =
  let conda_env = get_conda_env install_path it.package in
  let install_workflow =
    install_tool ~run_program ~host ~install_path it in
  let export_var =
    match it.tool_type with
    | `Application -> None
    | `Library v   ->
      let path = install_workflow#product#shell_which in
      Some KEDSL.Program.(shf "export %s=\"%s${%s:+:}${%s}\"" v path v v)
  in
  Machine.Tool.create it.definition
    ~ensure:install_workflow
    ~init:KEDSL.Program.(
        (if it.requires_conda
         then Conda.init_env ~conda_env ()
         else sh "echo 'Does not need Conda'")
        && it.init_environment ~install_path
        && Opam.kcom ~root_name:(Opam.root_of_package it.package) ~install_path
          (shf "eval $(%s)") "config env"
        && Option.value export_var ~default:(sh "echo 'No export var'")
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
    ~witness:"seq2HLA" ~requires_conda:true
    ~package:"seq2HLA.2.2" (* we need to uppercase HLA for opam *)
    Machine.Tool.Default.seq2hla

let optitype =
  install_target ~witness:"OptiTypePipeline" Machine.Tool.Default.optitype
    ~requires_conda:true
    ~init_environment:KEDSL.Program.(
        fun ~install_path ->
          let name = Machine.Tool.(Default.optitype.Definition.name) in
          shf "export OPAMROOT=%s" (Opam.root_of_package name |> Opam.root ~install_path)
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

