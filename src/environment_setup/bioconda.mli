open Biokepi_run_environment
open Common  

val default: 
  host: Common.KEDSL.Host.t ->
  run_program: Machine.Make_fun.t ->
  install_path: string ->
  unit ->
  Machine.Tool.Kit.t


val create_bioconda_tool:
  host: Common.KEDSL.Host.t ->
  run_program: Machine.Make_fun.t ->
  install_path: string ->
  ?check_bin: string ->
  Machine.Tool.Definition.t ->
  Machine.Tool.t