
open Biokepi_run_environment
open Common

(** The default location from where we download opam. *)
val default_opam_url : string

(** The default location from where we download biopam. *)
val default_biopam_url : string

(** A workflow to make sure that Biopam is configured.*)
val configured : ?biopam_home:string -> ?host:KEDSL.Host.t ->
  install_path:string -> unit ->
  < is_done : KEDSL.Condition.t option > KEDSL.workflow_node

(** The type of tool that we are installing via opam.

   This guides installation and determines:
    1. where we look for the {{!recfield:witness}witness} in
       [opam_install_path]/[package] or [opam_install_path]/bin.
    2. Whether we export $PATH (Application) or $LIBVAR (Library).*)
type tool_type =
  | Library of string   (** The export variable that points to witness. *)
  | Application

(** A description of what we'd like Biopam to install.*)
type install_target = {
  (** What are we installing? See {{type:tool_type}tool_type}. *)
  tool_type : tool_type;

  (** Name of the package: `opam install [package]` *)
  package : string;

  (** File that is passed to test determine success and what is exported. *)
  witness : string;

  (** Test to determine success of the install.
      Defaults to `test -e witness`. *)
  test :
    (?host:KEDSL.Host.t -> string -> KEDSL.Command.t) option;

  (* Install dependencies. *)
  edges : KEDSL.workflow_edge list;

  (* Transform the install and init programs, e.g. it needs to be run in a
     specific environment. Defaults to a “no-op”. *)
  init_environment : KEDSL.Program.t option;
}

(** Provde the specified (via install_target) tool.*)
val provide : ?host:KEDSL.Host.t ->
  install_path:string -> install_target -> Machine.Tool.t

(** A set of default tools that have been specified in this module.*)
val default : ?host:KEDSL.Host.t -> install_path:string -> unit ->
  Machine.Tool.Kit.t
