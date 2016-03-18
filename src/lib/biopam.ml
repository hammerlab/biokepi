(*
Provide tools via Biopam: https://github.com/solvuu/biopam
*)

(* TODO: add cleanup on_success commands. *)

open Common
open Run_environment
module K = KEDSL

let rm_path = Workflow_utilities.Remove.path_on_host

module Opam = struct

  let dir ~meta_playground = meta_playground // "opam_dir"
  let bin ~meta_playground = dir ~meta_playground // "opam"
  let root ~meta_playground = dir ~meta_playground // ".opam"

  (* TODO:
     Instead of just making sure that this file exists? Wouldn't it be better
     to make sure that a command from this program gives the right output?
     ie. $ opam --version = 1.2.2 *)
  let target ~host ~meta_playground =
    K.single_file ~host (bin ~meta_playground)

  let com ?(switch=true) ~meta_playground fmt =
    let bin = bin ~meta_playground in
    let root = root ~meta_playground in
    (* Pass the ROOT and SWITCH args as environments to not disrupt the flow of
       the rest of the arguments. *)
    if switch then
      Printf.sprintf ("OPAMROOT=%s OPAMSWITCH=0.0.0 %s " ^^ fmt) root bin
    else
      Printf.sprintf ("OPAMROOT=%s %s " ^^ fmt) root bin

  let file_command ?(switch=true) ~meta_playground ~package lb f =
    let l = match lb with `Lib -> "lib" | `Bin -> "bin" in
    let s = com ~switch ~meta_playground "config var %s:%s" package l in
    (* Are there places where this tick logic is flaky? *)
    (Printf.sprintf "$(%s)" s) // f

end

(* A workflow to ensure that opam is installed. *)
let installed ~host ~meta_playground =
  let url =
    "https://github.com/ocaml/opam/releases/download/1.2.2/opam-1.2.2-x86_64-Linux"
  in
  let opam_exec   = Opam.target ~host ~meta_playground in
  let install_dir = Opam.dir ~meta_playground in
  K.workflow_node opam_exec
    ~name:"Install opam"
    ~make:(K.daemonize ~host
      K.Program.(
        exec ["mkdir"; "-p"; install_dir]
        && exec ["cd"; install_dir]
        && Workflow_utilities.Download.wget_program ~output_filename:"opam" url
        && shf "chmod +x %s" opam_exec#path))
    ~edges:[K.on_failure_activate (rm_path ~host install_dir)]

let default_biopam_home = "https://github.com/solvuu/biopam.git"

(* A workflow to ensure that biopam is configured. *)
let configured ?(biopam_home=default_biopam_home) ~host ~meta_playground () =
  let name  = sprintf "Configure biopam to %s" biopam_home in
  let make  = K.daemonize ~host
    (K.Program.sh (Opam.com ~meta_playground ~switch:false
                     "init -n --compiler=0.0.0 biopam %s" biopam_home))
  in
  let edges = [ K.depends_on (installed ~host ~meta_playground)] in
  let biopam_is_repo =
    K.Command.shell ~host
      (Opam.com ~meta_playground "repo list | grep biopam") in
  let cond  =
    object method is_done = Some (`Command_returns (biopam_is_repo, 0)) end
  in
  K.workflow_node cond ~name ~make ~edges

(* A workflow to ensure that picard is available. *)
let install_picard ~host ~meta_playground =
  let edges = [ K.depends_on (configured ~host ~meta_playground ()) ] in
  let name = "Installing picard" in
  let opam fmt = Opam.com ~meta_playground fmt in
  let make = K.daemonize ~host K.Program.(sh (opam "install picard")) in
  let jar_path =
    Opam.file_command ~meta_playground ~package:"picard" `Lib "picard.jar"
  in
  let jar_set = K.Command.shell ~host (Printf.sprintf "test -e %s" jar_path) in
  let cond =
    object
      method is_done = Some (`Command_returns (jar_set, 0))
      method jar_path = jar_path
    end
  in
  K.workflow_node cond ~name ~make ~edges

let picard_tool ~host ~meta_playground =
  let install_workflow = install_picard ~host ~meta_playground in
  Tool.create Tool.Definition.(biopam "picard")
    ~ensure:install_workflow
    ~init:(K.Program.shf "export PICARD_JAR=%s"
             (Filename.quote install_workflow#product#jar_path))

(* A workflow to ensure that seq2HLA is available. *)
let install_seq2HLA ~host ~meta_playground =
  let edges =
    [ K.depends_on (configured ~host ~meta_playground ())
    ; K.depends_on (Conda.configured ~host ~meta_playground)
    ]
  in
  let name = "Installing seq2HLA" in
  let opam fmt = Opam.com ~meta_playground fmt in
  let make =
    K.daemonize ~host (Conda.run_in_biokepi_env
        ~meta_playground (K.Program.(sh (opam "install seq2HLA"))))
  in
  let seq2HLA =
    Opam.file_command ~meta_playground ~package:"seq2HLA" `Bin "seq2HLA"
  in
  let call_version = K.Command.shell ~host (seq2HLA ^ " --version") in
  let cond =
    object
      method is_done = Some (`Command_returns (call_version, 0))
      method path = seq2HLA
    end
  in
  K.workflow_node cond ~name ~make ~edges

let seq2HLA_tool ~host ~meta_playground =
  let install_workflow = install_seq2HLA ~host ~meta_playground in
  Tool.create Tool.Definition.(biopam "seq2HLA")
    ~ensure:install_workflow
    ~init:(K.Program.shf "export PATH=\"%s:$PATH\""
            (Filename.basename install_workflow#product#path))

let toolkit ~host ~meta_playground () =
  Tool.Kit.create
    [ picard_tool ~host ~meta_playground
    ; seq2HLA_tool ~host ~meta_playground
    ]
