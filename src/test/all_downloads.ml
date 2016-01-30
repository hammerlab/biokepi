(*

In progress workflow that simply downloads everything that can be
downloaded/built in order to check potentially deadlinks.


*)
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

let run_program ?name ?processors prog =
  let open Ketrew.EDSL in
  daemonize ~using:`Python_daemon ~host prog

let workflow =
  let edges_of_genome g =
    let open Biokepi.Reference_genome in
    List.filter_map [fasta; cosmic_exn; dbsnp_exn; gtf_exn; cdna_exn;]
      ~f:begin fun f ->
        try Some (f g |> Ketrew.EDSL.depends_on) with _ -> None
      end
  in
  let open Ketrew.EDSL in
  let get_all genome =
    workflow_node without_product
      ~name:(sprintf "Get all of %s's files"
               (Biokepi.Reference_genome.name genome))
      ~edges:(edges_of_genome genome)
  in
  let edges =
    List.map ~f:(fun genome ->
        depends_on (get_all genome)) [
      Biokepi.Download_reference_genomes.pull_b37
        ~host ~destination_path:(destination_path // "B37") ~run_program;
      Biokepi.Download_reference_genomes.pull_b37decoy
        ~host ~destination_path:(destination_path // "B37decoy") ~run_program;
      Biokepi.Download_reference_genomes.pull_b38
        ~host ~destination_path:(destination_path // "B38") ~run_program;
      Biokepi.Download_reference_genomes.pull_hg18
        ~host ~destination_path:(destination_path // "HG18") ~run_program;
      Biokepi.Download_reference_genomes.pull_hg19
        ~host ~destination_path:(destination_path // "HG19") ~run_program;
      Biokepi.Download_reference_genomes.pull_mm10
        ~host ~destination_path:(destination_path // "MM10") ~run_program;
    ]
  in
  workflow_node without_product
    ~name:"All downloads Biokepi test"
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
