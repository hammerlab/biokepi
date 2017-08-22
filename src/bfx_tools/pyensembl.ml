open Biokepi_run_environment
open Common

let pyensembl_env_key = "PYENSEMBL_CACHE_DIR"

let get_cache_dir ~(run_with: Machine.t) =
  let open KEDSL in
  let cache_dir = Machine.(get_pyensembl_cache_dir run_with) in
  match cache_dir with
    | Some path -> path
    | None -> failwith "Tool depends on PyEnsembl, but the cache directory \
                        has not been set!"

let set_cache_dir_command ~(run_with: Machine.t) =
  let open KEDSL in
  let cache_dir_value = get_cache_dir ~run_with in
  Program.(shf "export %s='%s'" pyensembl_env_key cache_dir_value)

let cache_genome ~(run_with: Machine.t) ~reference_build =
  let open KEDSL in
  let pyensembl = Machine.get_tool run_with Machine.Tool.Default.pyensembl in
  let genome = Machine.(get_reference_genome run_with reference_build) in
  let ensembl_release = genome |> Reference_genome.ensembl in
  let species = genome |> Reference_genome.species in
  let witness_file_path =
    sprintf "%s/%s.cached" (get_cache_dir ~run_with) reference_build
  in
  let name = sprintf "pyensembl_cache-%d-%s" ensembl_release species in
  workflow_node
    (single_file witness_file_path ~host:(Machine.as_host run_with))
    ~name
    ~edges:[
      depends_on Machine.Tool.(ensure pyensembl);
    ]
    ~make:(
      Machine.run_download_program run_with
        ~requirements:[`Internet_access;]
        ~name
        Program.(
          Machine.Tool.(init pyensembl)
          && (set_cache_dir_command ~run_with)
          && shf "pyensembl install --release %d --species \"%s\""
             ensembl_release
             species
          && exec ["touch"; witness_file_path]
        )
    )
