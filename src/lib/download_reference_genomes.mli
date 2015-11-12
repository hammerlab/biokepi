(** Download reference-genormes (& associated data) with Ketrew *)
  
val pull_b37 :
  host:Common.KEDSL.Host.t ->
  run_program:Run_environment.Machine.run_function ->
  destination_path:string -> Reference_genome.t

val pull_b37decoy :
  host:Common.KEDSL.Host.t ->
  run_program:Run_environment.Machine.run_function ->
  destination_path:string -> Reference_genome.t

val pull_b38 :
  host:Common.KEDSL.Host.t ->
  run_program:Run_environment.Machine.run_function ->
  destination_path:string -> Reference_genome.t

val pull_hg18 :
  host:Common.KEDSL.Host.t ->
  run_program:Run_environment.Machine.run_function ->
  destination_path:string -> Reference_genome.t

val pull_hg19 :
  host:Common.KEDSL.Host.t ->
  run_program:Run_environment.Machine.run_function ->
  destination_path:string -> Reference_genome.t

val pull_mm10 :
  host:Common.KEDSL.Host.t ->
  run_program:Run_environment.Machine.run_function ->
  destination_path:string -> Reference_genome.t
