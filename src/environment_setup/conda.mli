open Biokepi_run_environment

(** The contents of the default Conda configuration used in Biokepi. *)
val biokepi_conda_config : string

(** A workflow node to make sure that Conda is configured. *)
val configured :
  run_program: Machine.Make_fun.t ->
  host: Common.KEDSL.Host.t ->
  install_path: string ->
  unit ->
  < is_done : Common.KEDSL.Condition.t option > Common.KEDSL.workflow_node

(** A transform to run Programs with the Conda enviroment activated. *)
val init_biokepi_env : install_path:string -> Common.KEDSL.Program.t

(** This is the absolute path to the environment folder **)
val environment_path : install_path:string -> string
