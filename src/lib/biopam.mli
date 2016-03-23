
(** The default location from where we download opam. *)
val default_opam_url : string

(** The default location from where we download biopam. *)
val default_biopam_url : string

(** A workflow to make sure that Biopam is configured.*)
val configured : ?biopam_home:string -> ?host:Common.KEDSL.Host.t ->
  install_path:string -> unit ->
  < is_done : Common.KEDSL.Condition.t option > Common.KEDSL.workflow_node

(** The type of tool that we are installing via opam.

   This guides installation and determines: where we look for the
   {{!recfield:witness}witness} in [opam_install_path]/package or
   [opam_install_path]/bin. *)
type tool_type = Library | Application

(** A description of what we'd like Biopam to install.*)
type install_target =
   { tool_type : tool_type
   ; package : string
   ; witness : string
   ; test : (?host:Common.KEDSL.Host.t -> string -> Common.KEDSL.Command.t) option
   ; edges : Common.KEDSL.workflow_edge list
   }

(** Provde the specified (via install_target) tool.*)
val provide : ?host:Common.KEDSL.Host.t -> ?export_var:string ->
  install_path:string -> install_target -> Run_environment.Tool.t

(** A set of default tools that have been specified in this module.*)
val default : ?host:Common.KEDSL.Host.t -> install_path:string -> unit ->
  Run_environment.Tool.Kit.t
