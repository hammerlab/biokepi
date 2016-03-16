open Common

open Run_environment

let default_run_program : host:KEDSL.Host.t -> Machine.run_function =
  fun ~host ?(name="biokepi-ssh-box") ?(processors=1) program ->
    let open KEDSL in
    daemonize ~using:`Python_daemon ~host program

let create
    ?gatk_jar_location
    ?mutect_jar_location
    ?run_program ?b37 uri =
  let open KEDSL in
  let host = Host.parse (uri // "ketrew_playground") in
  let meta_playground = Uri.of_string uri |> Uri.path in
  let run_program =
    match run_program with
    | None -> default_run_program ~host
    | Some r -> r
  in
  let toolkit =
    Tool_providers.default_toolkit ()
      ~host ~meta_playground
      ?gatk_jar_location ?mutect_jar_location in
  Machine.create (sprintf "ssh-box-%s" uri)
    ~ssh_name:(
      Uri.of_string uri |> Uri.host |> Option.value ~default:"No-NAME")
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
    ~quick_command:(fun program -> run_program program)
    ~work_dir:(meta_playground // "work")
