open Common

open Run_environment

let default_run_program : host:KEDSL.Host.t -> Machine.run_function =
  fun ~host ?(name="biokepi-ssh-box") ?(processors=1) program ->
    let open KEDSL in
    daemonize ~using:`Python_daemon ~host program

module Data_providers = Download_reference_genomes
let create
    ~gatk_jar_location
    ~mutect_jar_location
    ?run_program ?b37 uri =
  let open KEDSL in
  let host = Host.parse (uri // "ketrew_playground") in
  let meta_playground = Uri.of_string uri |> Uri.path in
  let run_program =
    match run_program with
    | None -> default_run_program ~host
    | Some r -> r
  in
  let actual_b37 =
    match b37 with
    | None  ->
      let destination_path = meta_playground // "B37-reference-genome" in
      Data_providers.pull_b37 ~host ~run_program ~destination_path
    | Some s -> s
  in
  Machine.create (sprintf "ssh-box-%s" uri)
    ~ssh_name:(
      Uri.of_string uri |> Uri.host |> Option.value ~default:"No-NAME")
    ~get_reference_genome:(function
      | `B37 -> actual_b37
      | `B38 -> 
        Data_providers.pull_b38 
          ~host ~run_program ~destination_path:(meta_playground // "B38-reference-genome")
      | `hg18 ->
        Data_providers.pull_hg18
          ~host ~run_program ~destination_path:(meta_playground // "hg18-reference-genome")
      | `hg19 -> 
        Data_providers.pull_hg19 
          ~host ~run_program ~destination_path:(meta_playground // "hg19-reference-genome")
      | `B37decoy -> Data_providers.pull_b37decoy
                       ~host ~run_program ~destination_path:(meta_playground // "hs37d5-reference-genome")
      | `mm10 -> Data_providers.pull_mm10
                   ~host ~run_program ~destination_path:(meta_playground // "mm10-reference-genome")
      )
    ~host
    ~toolkit:(
      Tool_providers.default_toolkit
        ~host ~meta_playground
        ~gatk_jar_location ~mutect_jar_location)
    ~run_program
    ~quick_command:(fun program -> run_program program)
    ~work_dir:(meta_playground // "work")
