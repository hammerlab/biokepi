(** Download reference-genormes (& associated data) with Ketrew *)
open Biokepi_run_environment

type pull_function =
  toolkit:Machine.Tool.Kit.t ->
  host:Common.KEDSL.Host.t ->
  run_program:Machine.Make_fun.t ->
  destination_path:string -> Reference_genome.t

val pull_b37 : pull_function
val pull_b37decoy : pull_function
val pull_b38 : pull_function
val pull_hg18 : pull_function
val pull_hg19 : pull_function
val pull_mm10 : pull_function


val default_genome_providers : (string * pull_function) list

val get_reference_genome : string -> pull_function
