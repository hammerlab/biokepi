
open Biokepi_run_environment
open Common

(** The default location from where we download opam. *)
val default_opam_url : string

(** The default location from where we download biopam. *)
val default_biopam_url : string

(** A workflow to make sure that Biopam is configured.*)
val configured : ?biopam_home:string ->
  run_program: Machine.Make_fun.t ->
  host: Common.KEDSL.Host.t ->
  install_path:string -> unit ->
  < is_done : KEDSL.Condition.t option > KEDSL.workflow_node

type tool_type = [
  | `Library of string   (** The export variable that points to witness. *)
  | `Application
]
(** The type of tool that we are installing via opam.

   This guides installation and determines:
    1. where we look for the {{!recfield:witness}witness} in
       [opam_install_path]/[package] or [opam_install_path]/bin.
    2. Whether we export $PATH (Application) or $LIBVAR (Library).*)

(** A description of what we'd like Biopam to install.*)
type install_target = private {
  (** The package handle the Biopam package provides. *)
  definition: Machine.Tool.Definition.t;

  (** What are we installing? See {{type:tool_type}tool_type}. *)
  tool_type : tool_type;

  (** Name of the package: `opam install [package]` *)
  package : string;

  (** File that is passed to test determine success and what is exported. *)
  witness : string;

  (** Test to determine success of the install.
      Defaults to `test -e witness`. *)
  test :
    (host:KEDSL.Host.t -> string -> KEDSL.Command.t) option;

  (* Install dependencies. *)
  edges : KEDSL.workflow_edge list;

  (* Transform the install and init programs, e.g. it needs to be run in a
     specific environment. Defaults to a “no-op”. *)
  init_environment : install_path: string -> KEDSL.Program.t;

  (** Whether this package requires Conda packages. *)
  requires_conda: bool;

  (** Which opam-repository the tool should come from. *)
  repository: [ `Biopam | `Opam | `Custom of string ];

  (** Which compiler should be used to create the tool's own installation
      opam-switch.  *)
  compiler: string option;
}

val install_target:
  ?tool_type:tool_type ->
  ?test:(host: KEDSL.Host.t -> string -> KEDSL.Command.t) ->
  ?edges: KEDSL.workflow_edge list ->
  ?init_environment:(install_path:string -> KEDSL.Program.t) ->
  ?requires_conda:bool ->
  witness:string ->
  ?package:string ->
  ?repository:[ `Biopam | `Custom of string | `Opam ] ->
  ?compiler:string ->
  Machine.Tool.Definition.t ->
  install_target
(** Create {!install_target} values.

    - [tool_type]: the kind of tool being installed.
    See {{type:tool_type}tool_type}.
    Default: [`Application].
    - [test]: test to determine success of the install.
    Default: ["test -e <witness>"].
    - [edges]: dependencies to install first.
    - [init_environment]: transform the install and init programs, e.g. it
    needs to be run in a specific environment. Defaults to a “no-op”.
    - [witness]: filename (basename)
    that is passed to test determine success and what is exported (usually the
    binary for [`Application]s, or the JAR for Java libraries).
    - [requires_conda]: whther this package requires Python packages installed
    with {!Conda}.
    - [package]: the package name in the opam sense i.e.
    ["opam install <package-name>"] (the default is to
    construct the package name from the {!Machine.Tool.Definition.t}).
    - [repository]:  Which opam-repository the tool should come from:
    {ul
       - [`Biopam]: the Biopam project's repository ({b default}).
       - [`Opam]: the default Opam repository.
       - [`Custom url]: use custom URL.
    }
    - [compiler]: Which compiler should be used to create the tool's own
    installation opam-switch (the default is [None] corresponding to ["0.0.0"]
    which is expected for the [`Biopam] repository).
    - anonymous argument: the tool that the installation-target provides.
*)

val provide :
  run_program: Machine.Make_fun.t ->
  host: Common.KEDSL.Host.t ->
  install_path:string -> install_target -> Machine.Tool.t
(** Provde the specified (via install_target) tool.*)

val default : 
  run_program: Machine.Make_fun.t ->
  host: Common.KEDSL.Host.t ->
  install_path:string -> unit ->
  Machine.Tool.Kit.t
(** A set of default tools that have been specified in this module.*)
