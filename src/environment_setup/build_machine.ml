open Biokepi_run_environment
open Common


let default_run_program : host:KEDSL.Host.t -> Machine.Make_fun.t =
  fun ~host ?(name="biokepi-ssh-box") ?(requirements = []) program ->
    let open KEDSL in
    daemonize ~using:`Python_daemon ~host program

let create
    ?(max_processors = 1)
    ?gatk_jar_location
    ?mutect_jar_location
    ?netmhc_config
    ?pyensembl_cache_dir
    ?run_program ?toolkit ?b37 uri =
  let open KEDSL in
  let host = Host.parse (uri // "ketrew_playground") in
  let meta_playground = Uri.of_string uri |> Uri.path in
  let run_program =
    match run_program with
    | None -> default_run_program ~host
    | Some r -> r
  in
  let toolkit =
    Option.value toolkit
      ~default:(Tool_providers.default_toolkit ()
                  ~run_program
                  ~host ~install_tools_path:(meta_playground // "install-tools")
                  ?gatk_jar_location ?mutect_jar_location
                  ?netmhc_config)
  in
  Machine.create (sprintf "ssh-box-%s" uri)
    ~max_processors
    ?pyensembl_cache_dir
    ~get_reference_genome:(fun name ->
        match name, b37 with
        | name, Some some37 when name = Reference_genome.name some37 -> some37
        | name, _ ->        
          Download_reference_genomes.get_reference_genome name
            ~toolkit ~host ~run_program
            ~destination_path:(meta_playground // "reference-genome"))
    ~host
    ~toolkit
    ~run_program
    ~work_dir:(meta_playground // "work")
