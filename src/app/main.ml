
open Biokepi_common

let say fmt = ksprintf (printf "%s\n%!") fmt


let pipeline_example ~normal_fastqs ~tumor_fastqs ~dataset =
  let open Biokepi_pipeline.Construct in
  let normal = input_fastq ~dataset normal_fastqs in
  let tumor = input_fastq ~dataset tumor_fastqs in
  let bam_pair ?gap_open_penalty ?gap_extension_penalty () =
    let normal = bwa ?gap_open_penalty ?gap_extension_penalty normal in
    let tumor = bwa ?gap_open_penalty ?gap_extension_penalty tumor in
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
          varscan bam_pair;
        ])
  in
  vcfs

let dump_dumb_pipeline_example () =
  let dumb_fastq name =
    Ketrew.EDSL.file_target (sprintf "/path/to/dump-%s.fastq.gz" name) ~name in
  let normal_fastqs =
    `Paired_end ([dumb_fastq "R1-L001"; dumb_fastq "R1-L002"],
                 [dumb_fastq "R2-L001"; dumb_fastq "R2-L002"]) in
  let tumor_fastqs =
    `Single_end [dumb_fastq "R1-L001"; dumb_fastq "R1-L002"] in
  let vcfs = pipeline_example  ~normal_fastqs ~tumor_fastqs ~dataset:"DUMB" in
  say "Dumb examples dumped:\n%s"
    (`List (List.map ~f:Biokepi_pipeline.to_json vcfs)
     |> Yojson.Basic.pretty_to_string ~std:true)


let () =
  let open Cmdliner in
  let version = "0.0.0" in
  let sub_command ~info ~term =
    (term,
     Term.info (fst info) ~version ~sdocs:"COMMON OPTIONS" ~doc:(snd info)) in
  let name_flag =
    Arg.(required & opt (some string) None & info ["N"; "name"]
           ~doc:"Give a name to the pipeline") in
  let dump_pipeline =
    sub_command
      ~info:("dump-pipeline", "Dump the JSON blob of a pipeline")
      ~term:Term.(
          pure (fun name ->
              match name with
              | "dumb" -> dump_dumb_pipeline_example ()
              | s -> failwithf "unknown pipeline: %S" s)
          $ name_flag
        )
  in
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
  let cmds = [dump_pipeline] in
  match Term.eval_choice default_cmd cmds with
  | `Ok () -> ()
  | `Error _ -> failwithf "cmdliner error"
  | `Version | `Help -> exit 0
