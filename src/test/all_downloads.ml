(*

In progress workflow that simply downloads everything that can be
downloaded/built in order to check potentially deadlinks.


*)

module Ref_genome = Biokepi_run_environment.Reference_genome

open Nonstd
let failwithf fmt = ksprintf failwith fmt
let get_env v help =
  try Sys.getenv v with e -> failwithf "Missing env-variable: %S (%s)" v help
let (//) = Filename.concat

let host =
  get_env "KHOST" "Host URI, in the Ketrew sense"
  |> Ketrew.EDSL.Host.parse

let destination_path =
  get_env "DEST_PATH" "Directory path on the host where every download should go"

let run_program ?name ?requirements prog =
  let open Ketrew.EDSL in
  daemonize ~using:`Python_daemon ~host prog

let workflow =
  let edges_of_genome g =
    let open Ref_genome in
    List.filter_map [fasta; cosmic_exn; dbsnp_exn; gtf_exn; cdna_exn;]
      ~f:begin fun f ->
        try Some (f g |> Ketrew.EDSL.depends_on) with _ -> None
      end
  in
  let open Ketrew.EDSL in
  let get_all genome =
    workflow_node without_product
      ~name:(sprintf "Get all of %s's files"
               (Ref_genome.name genome))
      ~edges:(edges_of_genome genome)
  in
  let toolkit =
    Biokepi_environment_setup.Tool_providers.default_toolkit () ~host
      ~meta_playground:(destination_path // "tools") in
  let edges =
    let genomes =
      Biokepi_environment_setup.Download_reference_genomes.default_genome_providers in
    List.map genomes ~f:(fun (name, pull) ->
        let genome = pull ~toolkit ~host ~destination_path ~run_program in
        depends_on (get_all genome))
  in
  workflow_node without_product
    ~name:(sprintf "All downloads to %s" destination_path)
    ~tags:["biokepi"; "test"]
    ~edges

let () =
  match Sys.argv |> Array.to_list |> List.tl_exn with
  | "go" :: [] ->
    Ketrew.Client.submit_workflow workflow
      ~add_tags:["biokepi"; "test"; "all-downloads"]
  | "view" :: [] ->
    Ketrew.EDSL.workflow_to_string workflow
    |> printf "Workflow:\n%s\n%!"
  | other ->
    eprintf "Wrong command line: [%s]\n → use \"view\" or \"go\"\n%!"
      (String.concat ", " other)
