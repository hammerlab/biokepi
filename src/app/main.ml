
open Biokepi.Common

let say fmt = ksprintf (printf "%s\n%!") fmt

let get_env s =
  try Sys.getenv s with _ -> failwithf "Missing environment variable: %s" s
let get_opt s =
  try Some (Sys.getenv s) with _ -> None

let crazy_somatic_example ~normal_fastqs ~tumor_fastqs ~dataset =
  let open Biokepi.Pipeline.Construct in
  let normal = input_fastq ~dataset normal_fastqs in
  let tumor = input_fastq ~dataset tumor_fastqs in
  let bam_pair ?gap_open_penalty ?gap_extension_penalty () =
    let normal =
      bwa ?gap_open_penalty ?gap_extension_penalty normal
      |> gatk_indel_realigner
      |> picard_mark_duplicates
      |> gatk_bqsr
    in
    let tumor =
      bwa ?gap_open_penalty ?gap_extension_penalty tumor
      |> gatk_indel_realigner
      |> picard_mark_duplicates
      |> gatk_bqsr
    in
    pair ~normal ~tumor in
  let bam_pairs = [
    bam_pair ();
    bam_pair ~gap_open_penalty:10 ~gap_extension_penalty:7 ();
  ] in
  let vcfs =
    List.concat_map bam_pairs ~f:(fun bam_pair ->
        [
          mutect bam_pair;
          somaticsniper bam_pair;
          somaticsniper ~prior_probability:0.001 ~theta:0.95 bam_pair;
          varscan_somatic bam_pair;
        ])
  in
  vcfs

let simple_somatic_example ~variant_caller ~normal_fastqs ~tumor_fastqs ~dataset =
  let open Biokepi.Pipeline.Construct in
  let normal = input_fastq ~dataset normal_fastqs in
  let tumor = input_fastq ~dataset tumor_fastqs in
  let make_bam data =
    data |> bwa |> gatk_indel_realigner |> picard_mark_duplicates |> gatk_bqsr
  in
  let vc_input =
    pair ~normal:(make_bam normal) ~tumor:(make_bam tumor) in
  [variant_caller vc_input]

type somatic_from_fastqs =
  normal_fastqs:Biokepi.Pipeline.Construct.input_fastq ->
  tumor_fastqs:Biokepi.Pipeline.Construct.input_fastq ->
  dataset:string -> Biokepi.Pipeline.vcf Biokepi.Pipeline.t list

type example_pipeline = [
  | `Somatic_from_fastqs of somatic_from_fastqs
]

let global_named_examples: (string * example_pipeline) list = [
  "somatic-crazy", `Somatic_from_fastqs crazy_somatic_example;
  "somatic-simple-somaticsniper",
  `Somatic_from_fastqs
    (simple_somatic_example
       ~variant_caller:Biokepi.Pipeline.Construct.somaticsniper);
  "somatic-simple-varscan",
  `Somatic_from_fastqs
    (simple_somatic_example
       ~variant_caller:Biokepi.Pipeline.Construct.varscan_somatic);
  "somatic-simple-mutect",
  `Somatic_from_fastqs
    (simple_somatic_example
       ~variant_caller:Biokepi.Pipeline.Construct.mutect);
]


let dump_pipeline ?(format = `Json) =
  function
  | `Somatic_from_fastqs pipeline_example ->
    let dumb_fastq name =
      KEDSL.(
        workflow_node
          (single_file (sprintf "/path/to/dump-%s.fastq.gz" name))
          ~name) in
    let normal_fastqs =
      `Paired_end ([dumb_fastq "R1-L001"; dumb_fastq "R1-L002"],
                   [dumb_fastq "R2-L001"; dumb_fastq "R2-L002"]) in
    let tumor_fastqs =
      `Single_end [dumb_fastq "R1-L001"; dumb_fastq "R1-L002"] in
    let vcfs = pipeline_example  ~normal_fastqs ~tumor_fastqs ~dataset:"DUMB" in
    begin match format with
    | `Json  -> 
      say "Pipeline JSON (with DUMB dataset):\n%s"
        (`List (List.map ~f:Biokepi.Pipeline.to_json vcfs)
         |> Yojson.Basic.pretty_to_string ~std:true)
    end

let environmental_box () : Biokepi.Run_environment.Machine.t =
  let box_uri = get_env "BIOKEPI_SSH_BOX_URI" in
  let jar_location name () =
    begin match ksprintf get_opt "BIOKEPI_%s_JAR_SCP" name with
    | Some s -> `Scp s
    | None ->
      begin match ksprintf get_opt "BIOKEPI_%s_JAR_WGET" name with
      | Some s -> `Wget s
      | None ->
        failwithf "BIOKEPI_%s_JAR_SCP or BIOKEPI_%s_JAR_WGET \
                   are required when you wanna run %s" name name name
      end
    end  
  in
  let mutect_jar_location = jar_location "MUTECT" in
  let gatk_jar_location = jar_location "GATK" in
  Biokepi.Build_machine.create
    ~gatk_jar_location ~mutect_jar_location box_uri

let with_environmental_dataset =
  function
  | `Somatic_from_fastqs  make_pipe_line ->
    let dataset = get_env "BIOKEPI_DATASET_NAME" in
    let get_list kind =
      get_env (sprintf "BIOKEPI_%s" kind)
      |> String.split ~on:(`Character ',')
      |> List.map ~f:(String.strip ~on:`Both)
      |> List.map ~f:(fun f ->
          let name = sprintf "Input: %s (%s)" dataset kind in
          KEDSL.(workflow_node (single_file f) ~name))
    in
    let normal_fastqs =
      `Paired_end (get_list "NORMAL_R1", get_list "NORMAL_R2") in
    let tumor_fastqs =
      `Paired_end (get_list "TUMOR_R1", get_list "TUMOR_R2") in
    (dataset, make_pipe_line ~normal_fastqs ~tumor_fastqs ~dataset)

let pipeline_example_target ~push_result ~pipeline_name pipeline_example =
  let machine = environmental_box () in
  let dataset, pipelines =
    with_environmental_dataset pipeline_example in
  let work_dir =
    Biokepi.Run_environment.Machine.work_dir machine
    // sprintf "on-%s" dataset in
  let compiler = 
    Biokepi.Pipeline.Compiler.create
      ~reference_build:"b37"
      ~work_dir ~machine ~processors:2 () in
  let compiled =
    List.map pipelines
      ~f:(fun pl ->
          let t =
            Biokepi.Pipeline.Compiler.compile_variant_caller_step ~compiler pl
          in
          `Target t,
          `Json_blob (
            `Assoc [
              (* Using a bit of the “internal” representation of
                 workflow-nodes here: *)
              "target-name", `String t#render#name;
              "target-id", `String t#render#id;
              "pipeline", Biokepi.Pipeline.to_json pl;
            ]))
  in
  let open KEDSL in
  let edges =
    List.map compiled (function
      | (`Target vcf, `Json_blob json) when push_result ->
        let witness_output =
          Filename.chop_suffix vcf#product#path ".vcf" ^ "-cycledashed.html" in
        let params = Yojson.Basic.pretty_to_string json in
        Biokepi.Cycledash.post_vcf ~run_with:machine
          ~vcf ~variant_caller_name:vcf#render#name ~dataset_name:dataset
          ~witness_output ~params
          (get_env "BIOKEPI_CYCLEDASH_URL")
        |> depends_on
      | (`Target t, _) ->  t |> depends_on
      ) in
  let whole_json =
    `List (List.map compiled ~f:(fun (_, `Json_blob j) -> j)) in (*  *)
  workflow_node nothing
    ~name:(sprintf "%s on %s: common ancestor" pipeline_name dataset)
    ~edges
    ~metadata:(`String (Yojson.Basic.pretty_to_string whole_json))

let run_pipeline_example ~push_result ~pipeline_name pipeline =
  let workflow = pipeline_example_target ~push_result ~pipeline_name pipeline in
  KEDSL.submit workflow

let () =
  let open Cmdliner in
  let version = "0.0.0" in
  let sub_command ~info ~term =
    (term,
     Term.info (fst info) ~version ~sdocs:"COMMON OPTIONS" ~doc:(snd info)) in
  let name_flag =
    Arg.(required & opt (some string) None & info ["N"; "name"]
           ~doc:"Choose the pipeline by name") in
  let push_to_cycledash_flag =
    Arg.(value & flag & info ["P"; "push-result"]
           ~doc:"Push the result to a running Cycledash instance.") in
  let dump_pipeline =
    sub_command
      ~info:("dump-pipeline", "Dump the JSON blob of a pipeline")
      ~term:Term.(
          pure (fun name ->
              match
                List.find global_named_examples ~f:(fun (n, _) -> n = name)
              with
              | Some (_, pipe) -> dump_pipeline pipe
              | None -> failwithf "unknown pipeline: %S" name)
          $ name_flag
        )
  in
  let run_pipeline =
    sub_command
      ~info:("run-pipeline",
             "Run a pipeline the SSH-BOX defined by the environment")
      ~term:Term.(
          pure (fun name push_result ->
              match
                List.find global_named_examples ~f:(fun (n, _) -> n = name)
              with
              | Some (_, pipe) ->
                run_pipeline_example ~push_result ~pipeline_name:name pipe
              | None -> failwithf "unknown pipeline: %S" name)
          $ name_flag
          $ push_to_cycledash_flag
        )
  in
  let list_pipelines =
    sub_command
      ~info:("list-named-pipelines", "Display a list available pipelines")
      ~term:Term.(
          pure (fun () ->
              List.iter global_named_examples ~f:(fun (n, _) ->
                  say "* %s" n)
            )
          $ pure ()
        ) in
  let default_cmd =
    let doc = "Bio-related Ketrew Workflows – Example Application" in
    let man = [
      `S "AUTHORS";
      `P "Sebastien Mondet <seb@mondet.org>"; `Noblank;
      `S "BUGS";
      `P "Browse and report new issues at"; `Noblank;
      `P "<https://github.com/hammerlab/biokepi>.";
    ] in
    Term.(
      ret (pure (`Help (`Plain, None))),
      info "biokepi" ~version ~doc ~man)
  in
  let cmds = [dump_pipeline; run_pipeline; list_pipelines] in
  match Term.eval_choice default_cmd cmds with
  | `Ok () -> ()
  | `Error _ -> failwithf "cmdliner error"
  | `Version | `Help -> exit 0
