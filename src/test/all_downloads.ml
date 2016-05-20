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


let biocaml_def = Biokepi.Machine.Tool.Definition.create "biocaml" ~version:"0.4.0"
let biocaml =
  Biokepi.Setup.Biopam.install_target biocaml_def
    ~tool_type:(`Library "biocaml")
    ~witness:"META"
    ~repository:`Opam
    ~compiler:"4.02.3"

let ledit_def = Biokepi.Machine.Tool.Definition.create "ledit" ~version:"2.03"
let ledit =
  Biokepi.Setup.Biopam.install_target ledit_def
    ~tool_type:`Application
    ~witness:"ledit"
    ~repository:`Opam
    ~compiler:"4.02.3"


let toolkit =
  let install_tools_path = destination_path // "tools" in
  Biokepi.Machine.Tool.Kit.concat [
    Biokepi.Machine.Tool.Kit.of_list [
      Biokepi.Setup.Biopam.provide ~host ~run_program
        (* We need to reproduce the same path there, this is wrong TODO *)
        ~install_path:(install_tools_path // "biopam-kit")
        biocaml;
      Biokepi.Setup.Biopam.provide ~host ~run_program
        ~install_path:(install_tools_path // "biopam-kit")
        ledit;
    ];
    Biokepi_environment_setup.Tool_providers.default_toolkit () ~host
      ~run_program ~install_tools_path;
  ]

let ref_genomes_workflow =
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

let tools_workflow =
  let tools_to_test = 
    let open Biokepi.Machine.Tool.Default in
    let which_size_and bin  l =
      ("which " ^ bin)
      :: ("ls -l $(which " ^ bin ^ ")")
      :: ("file $(which " ^ bin ^ ")")
      :: l in
    [
      bwa,       which_size_and "bwa" ["bwa || echo Done"];
      stringtie, which_size_and "stringtie" [ "stringtie --help"];
      bedtools,  which_size_and "bedtools" [ "bedtools --help"];
      vcftools,  which_size_and "vcftools" [ "vcftools --help"];
      (* TODO: Mosaik's installation is still broken
      mosaik,    which_size_and "MosaikAligner"
        ["MosaikAligner --help";
         "ls -l \"$MOSAIK_PE_ANN\"";
         "ls -l \"$MOSAIK_SE_ANN\""; ];
         *)
      star,    which_size_and "STAR" []; (* STAR does not work on OSX, can't
                                            find a command that exits with 0*)
      hisat,   which_size_and "hisat" ["hisat --help"]; (* hisat does not work on OSX*)
      hisat2,  which_size_and "hisat2" ["hisat2 --help"]; (* hisat does not work on OSX*)
      kallisto, which_size_and "kallisto" ["kallisto version"]; (* kallisto does not work on OSX*)
      samtools, which_size_and "samtools" ["samtools --help"];
      samtools, which_size_and "bgzip" ["bgzip --help || echo OK"]; (* tool returns 1 on --help *)
      samtools, which_size_and "tabix" ["tabix --help || echo OK"]; (* idem. *)
      varscan, ["ls -l \"$VARSCAN_JAR\""; "java -jar $VARSCAN_JAR -help"];
      picard, ["ls -l \"$PICARD_JAR\""]; (*
              "java -jar $PICARD_JAR --help" gave the epic:
          "'--help' is not a valid command. See PicardCommandLine --help for more information"
            *)
      strelka, ["ls -l \"$STRELKA_BIN\"";
                "perl -c $STRELKA_BIN/configureStrelkaWorkflow.pl"];
      virmid, ["ls -l \"$VIRMID_JAR\""; "java -jar $VIRMID_JAR -help"];
      muse, which_size_and "$muse_bin" ["$muse_bin call -h"];
      (* muse call -h displays an error but still returns 0 *)
      somaticsniper, which_size_and "somaticsniper" ["somaticsniper -v"];
      seq2hla, which_size_and "seq2HLA" ["seq2HLA --version"];
      optitype, which_size_and "OptiTypePipeline" ["OptiTypePipeline -h"];
      ledit_def,  which_size_and "ledit" ["ledit -v"];
      biocaml_def, ["ocamlfind list | grep biocaml"];
     ]
  in
  let open Ketrew.EDSL in
  let edges =
    List.map tools_to_test ~f:(fun (def, cmds) ->
        let tool = Biokepi.Machine.Tool.Kit.get_exn toolkit def in
        workflow_node without_product
          ~name:(sprintf "Test tool %s: %S, …"
                   (Biokepi.Machine.Tool.Definition.to_string def)
                   (List.hd_exn cmds))
          ~make:(run_program Program.(
              Biokepi.Machine.Tool.init tool
              && chain (List.map cmds ~f:sh)
            ))
          ~edges:[
            depends_on Biokepi.Machine.Tool.(ensure tool)
          ]
        |> depends_on
      )
  in
  workflow_node without_product ~name:"All tool installations" ~edges


let workflow how =
  match how with
  | `Ref_genomes -> ref_genomes_workflow
  | `Tools -> tools_workflow

let () =
  let how l =
    match l with
    | "rg" :: [] -> `Ref_genomes
    | "tools" :: [] -> `Tools
    | other -> failwithf "Can't understand how to run the test"
  in
  match Sys.argv |> Array.to_list |> List.tl_exn with
  | "go" :: more ->
    Ketrew.Client.submit_workflow (workflow (how more))
      ~add_tags:[
        "biokepi"; "test"; "all-downloads";
        Ketrew_pure.Internal_pervasives.Time.(now () |> to_filename) ]
  | "view" :: more ->
    Ketrew.EDSL.workflow_to_string (workflow (how more))
    |> printf "Workflow:\n%s\n%!"
  | other ->
    eprintf "Wrong command line: [%s]\n → usage : {view,go} {rg,tools}\n%!"
      (String.concat ", " other);
    exit 1
