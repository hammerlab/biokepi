(** Simplified creation of {!Run_environment.Machine.t} values *)

(** Build a {!Run_environment.Machine.t} with convenient default values.

    The [string] argument is a URI like the one expected 
    by {!Ketrew.EDSL.Host.parse} except that the “path” is the
    meta-playground for Biokepi (the ketrew playground will be
    [(meta_playground // "ketrew_playground")].

    The default [run_program] is daemonizing with [`Python_daemon].

    The default [toolkit] is {{!value:Tool_providers.default_toolkit}default_toolkit}
    from [Tool_providers]. This machine will get tools installations and
    data-fetching from Biokepi's defaults. The [?b37] argument allows to
    override the locations of the “B37” genome; to override other default please
    use {!Run_environment.Machine.create} directly.
*)
val create :
  ?gatk_jar_location:(unit -> Tool_providers.broad_jar_location) ->
  ?mutect_jar_location:(unit -> Tool_providers.broad_jar_location) ->
  ?run_program:Run_environment.Make_fun.t ->
  ?toolkit:Run_environment.Tool.Kit.t ->
  ?toolkit_install_path:([`Biopam] * string) list ->
  ?b37:Reference_genome.t ->
  string ->
  Run_environment.Machine.t
