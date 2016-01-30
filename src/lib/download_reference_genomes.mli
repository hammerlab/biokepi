(** Download reference-genormes (& associated data) with Ketrew *)

type pull_function =
  toolkit:Run_environment.Tool.Kit.t ->
  host:Common.KEDSL.Host.t ->
  run_program:Run_environment.Machine.run_function ->
  destination_path:string -> Reference_genome.t

val pull_b37 : pull_function
val pull_b37decoy : pull_function
val pull_b38 : pull_function
val pull_hg18 : pull_function
val pull_hg19 : pull_function
val pull_mm10 : pull_function
