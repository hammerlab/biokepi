(** The contents of the default Conda configuration used in Biokepi. *)
val biokepi_conda_config : string

(** A workflow node to make sure that Conda is configured. *)
val configured : ?host:Common.KEDSL.Host.t -> install_path:string -> unit ->
  < is_done : Common.KEDSL.Condition.t option > Common.KEDSL.workflow_node

(** A transform to run Programs with the Conda enviroment activated. *)
val run_in_biokepi_env : install_path:string -> Common.KEDSL.Program.t ->
  Common.KEDSL.Program.t
