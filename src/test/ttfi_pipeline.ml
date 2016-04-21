(** This test uses the high-level EDSL and tries different compilation targets,
    but does not produce runnable workflows.


    The workflow itself is voluntarily too big and abuses lambda expressions.
*)
open Nonstd

module Pipeline_1 (Bfx : Biokepi.EDSL.Semantics) = struct

  let fastq_list ~dataset files =
    List.map files ~f:begin function
    | `Pair (r1, r2) ->
      if Filename.check_suffix r1 ".gz"
     || Filename.check_suffix r1 ".fqz"
      then
        Bfx.(fastq_gz ~sample_name:dataset ~r1 ~r2 () |> gunzip)
      else
        Bfx.(fastq ~sample_name:dataset ~r1 ~r2 ())
    end
    |> Bfx.list


  let every_vc_on_fastqs ~reference_build ~normal ~tumor =
    let aligner which_one =
      Bfx.lambda (fun fq -> which_one ~reference_build fq) in
    let align_list how (list_of_fastqs : [ `Fastq ] list Bfx.repr) =
      Bfx.list_map list_of_fastqs ~f:how |> Bfx.merge_bams
    in
    let aligners = Bfx.list [
        aligner @@ Bfx.bwa_aln ?configuration: None;
        aligner @@ Bfx.bwa_mem ?configuration: None;
        aligner @@ Bfx.hisat ~configuration:Biokepi.Tools.Hisat.Configuration.default_v1;
        aligner @@ Bfx.hisat ~configuration:Biokepi.Tools.Hisat.Configuration.default_v2;
        aligner @@ Bfx.star ~configuration:Biokepi.Tools.Star.Configuration.Align.default;
        aligner @@ Bfx.mosaik;
        aligner @@ (fun ~reference_build fastq ->
            Bfx.bwa_aln ~reference_build fastq
            |> Bfx.bam_to_fastq ~sample_name:"realigned" `PE
            |> Bfx.bwa_mem ?configuration: None ~reference_build
          );
      ] in
    let somatic_of_pair how =
      Bfx.lambda (fun pair ->
          let normal = Bfx.pair_first pair in
          let tumor = Bfx.pair_second pair in
          how ~normal ~tumor)
    in
    let somatic_vcs =
      List.map ~f:somatic_of_pair [
        Bfx.mutect
          ~configuration:Biokepi.Tools.Mutect.Configuration.default;
        Bfx.mutect2
          ~configuration:Biokepi.Tools.Gatk.Configuration.Mutect2.default;
        Bfx.somaticsniper
          ~configuration:Biokepi.Tools.Somaticsniper.Configuration.default;
        Bfx.strelka
          ~configuration:Biokepi.Tools.Strelka.Configuration.default;
        Bfx.varscan_somatic ?adjust_mapq:None;
        Bfx.muse
          ~configuration:Biokepi.Tools.Muse.Configuration.wes;
        Bfx.virmid
          ~configuration:Biokepi.Tools.Virmid.Configuration.default;
      ]
    in
    let aligned_pairs
      : (([ `Fastq ] list * [ `Fastq ] list) -> ([ `Bam ] * [ `Bam ]) list) Bfx.repr =
      Bfx.lambda (fun pair ->
          Bfx.list_map aligners ~f:(
            Bfx.lambda (fun (al : ([ `Fastq ] -> [ `Bam ]) Bfx.repr) ->
                Bfx.pair
                  (align_list al (Bfx.pair_first pair : [ `Fastq ] list Bfx.repr))
                  (align_list al (Bfx.pair_second pair))
              )
          )
        )
    in
    let vcfs =
      Bfx.lambda (fun pair ->
          List.map somatic_vcs ~f:(fun vc ->
              Bfx.list_map (Bfx.apply aligned_pairs pair) ~f:(
                Bfx.lambda (fun bam_pair ->
                    let (||>) x f = Bfx.apply f x in
                    let indelreal =
                      Bfx.lambda (fun pair ->
                          Bfx.gatk_indel_realigner_joint
                            ~configuration: Biokepi.Tools.Gatk.Configuration.default_indel_realigner
                            pair)
                    in
                    let map_pair f =
                      Bfx.lambda (fun pair ->
                          let b1 = Bfx.pair_first pair in
                          let b2 = Bfx.pair_second pair in
                          Bfx.pair (f b1) (f b2)
                        )
                    in
                    let bqsr_pair =
                      Bfx.gatk_bqsr
                        ~configuration: Biokepi.Tools.Gatk.Configuration.default_bqsr
                      |> map_pair in
                    let markdups_pair =
                      Bfx.picard_mark_duplicates
                        ~configuration:Biokepi.Tools.Picard.Mark_duplicates_settings.default
                      |> map_pair in
                    bam_pair ||> markdups_pair ||> indelreal ||> bqsr_pair ||> vc
                    ||> Bfx.lambda Bfx.to_unit
                  )
              )
            )
          |> Bfx.list
        )
    in
    let workflow_of_pair =
      Bfx.lambda (fun pair ->
          Bfx.list [
            (Bfx.apply aligned_pairs pair
             |> Bfx.list_map ~f:(Bfx.lambda (fun p ->
                 Bfx.pair_first p
                 |> Bfx.stringtie
                   ~configuration:Biokepi.Tools.Stringtie.Configuration.default)
               )) |> Bfx.to_unit;
            Bfx.apply vcfs pair |> Bfx.to_unit;
            begin
              Bfx.apply
                (Bfx.lambda (fun p -> Bfx.pair_first p |> Bfx.concat |> Bfx.seq2hla))
                pair
              |> Bfx.to_unit
            end;
            begin 
              Bfx.apply
                (Bfx.lambda (fun p -> Bfx.pair_second p |> Bfx.concat |> Bfx.optitype `RNA))
                pair
              |> Bfx.to_unit
            end;
            begin
              Bfx.apply aligned_pairs pair
              |> Bfx.list_map ~f:(Bfx.lambda (fun p ->
                  Bfx.pair_first p
                  |> Bfx.gatk_haplotype_caller
                ))
              |> Bfx.to_unit
            end;
          ]
        |> Bfx.to_unit
        )
    in
    Bfx.apply workflow_of_pair (Bfx.pair normal tumor)

  let run ~normal ~tumor =
    Bfx.observe (fun () ->
        every_vc_on_fastqs
          ~reference_build:"b37"
          ~normal:(fastq_list ~dataset:(fst normal) (snd normal))
          ~tumor:(fastq_list ~dataset:(fst tumor) (snd tumor))
      )

end

let normal_1 =
  ("normal-1", [
      `Pair ("/input/normal-1-001-r1.fastq", "/input/normal-1-001-r2.fastq");
      `Pair ("/input/normal-1-002-r1.fastq", "/input/normal-1-002-r2.fastq");
      `Pair ("/input/normal-1-003-r1.fqz",   "/input/normal-1-003-r2.fqz");
    ])

let tumor_1 =
  ("tumor-1", [
      `Pair ("/input/tumor-1-001-r1.fastq.gz", "/input/tumor-1-001-r2.fastq.gz");
      `Pair ("/input/tumor-1-002-r1.fastq.gz", "/input/tumor-1-002-r2.fastq.gz");
    ])

let write_file file ~content =
  let out_file = open_out file in
  try
    output_string out_file content;
    close_out out_file
  with _ ->
    close_out out_file

let cmdf fmt =
  ksprintf (fun s ->
      printf "CMD: %s\n%!" s;
      match Sys.command s with
      | 0 -> ()
      | other -> ksprintf failwith "non-zero-exit: %s → %d" s other) fmt

let (//) = Filename.concat

let () =
  let module Display_pipeline_1 = Pipeline_1(Biokepi.EDSL.Compile.To_display) in
  let test_dir = "_build/ttfi-test-results/" in
  cmdf "mkdir -p %s" test_dir;
  let pipeline_1_display = test_dir // "pipeline-1-display.txt" in
  write_file pipeline_1_display
    ~content:(Display_pipeline_1.run ~normal:normal_1 ~tumor:tumor_1
              |> SmartPrint.to_string 80 2);

  let module Jsonize_pipeline_1 = Pipeline_1(Biokepi.EDSL.Compile.To_json) in
  let pipeline_1_json = test_dir // "pipeline-1.json" in
  write_file pipeline_1_json
    ~content:(Jsonize_pipeline_1.run ~normal:normal_1 ~tumor:tumor_1
              |> Yojson.Basic.pretty_to_string ~std:true);

  let module Dotize_pipeline_1 = Pipeline_1(Biokepi.EDSL.Compile.To_dot) in
  let pipeline_1_dot = test_dir // "pipeline-1.dot" in
  let sm1_dot =
    Dotize_pipeline_1.run ~normal:normal_1 ~tumor:tumor_1 in
  let out = open_out pipeline_1_dot in
  SmartPrint.to_out_channel  80 2 out sm1_dot;
  close_out out;
  let pipeline_1_svg = test_dir // "pipeline-1.svg" in
  cmdf "dot -x -Grotate=180 -v -Tsvg  %s -o %s" pipeline_1_dot pipeline_1_svg;
  let pipeline_1_png = test_dir // "pipeline-1.png" in
  cmdf "dot -v -x -Tpng  %s -o %s" pipeline_1_dot pipeline_1_png;

  let module Dotize_beta_reduced_pipeline_1 =
    Pipeline_1(
      Biokepi.EDSL.Transform.Apply_functions(
        Biokepi.EDSL.Transform.Apply_functions(Biokepi.EDSL.Compile.To_dot)
      )
    )
  in
  let pipeline_1_beta_dot = test_dir // "pipeline-1-beta.dot" in
  let sm1_beta_dot =
    Dotize_beta_reduced_pipeline_1.run ~normal:normal_1 ~tumor:tumor_1 in
  let pipeline_1_beta_png = test_dir // "pipeline-1-beta.png" in
  begin try
    let out = open_out pipeline_1_beta_dot in
    SmartPrint.to_out_channel  80 2 out sm1_beta_dot;
    close_out out;
    cmdf "dot -v -x -Tpng  %s -o %s" pipeline_1_beta_dot pipeline_1_beta_png;
  with e ->
    printf "BETA-DOT NOT PRODUCED !!!\n%!";
  end;

  let module Workflow_compiler =
    Biokepi.EDSL.Compile.To_workflow.Make(struct
      include Biokepi.EDSL.Compile.To_workflow.Defaults
      let processors = 42
      let work_dir = "/work/dir/"
      let machine =
        Biokepi.Setup.Build_machine.create
          "ssh://example.com/tmp/KT/"
    end)
  in
  let module Ketrew_pipeline_1 = Pipeline_1(Workflow_compiler) in
  let workflow_1 =
    Ketrew_pipeline_1.run ~normal:normal_1 ~tumor:tumor_1
    |> Biokepi.EDSL.Compile.To_workflow.File_type_specification.get_unit_workflow
      ~name:"Biokepi TTFI test top-level node"
  in
  let pipeline_1_workflow_display =
    test_dir // "pipeline-1-workflow-display.txt" in
  ignore workflow_1;
  (*
  write_file pipeline_1_workflow_display
    ~content:(workflow_1 |> Ketrew.EDSL.workflow_to_string);
     *)
  printf "Pipeline_1:\n\
         \  display: %s\n\
         \  JSON: %s\n\
         \  Dot: %s\n\
         \  SVG: %s\n\
         \  PNG: %s\n\
         \  Beta-reduced PNG: %s\n\
         \  ketrew-display: %s\n%!"
    pipeline_1_display
    pipeline_1_json
    pipeline_1_dot
    pipeline_1_svg
    pipeline_1_png
    pipeline_1_beta_png
    pipeline_1_workflow_display


