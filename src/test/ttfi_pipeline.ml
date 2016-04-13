(** This test uses the high-level EDSL and tries different compilation targets,
    but does not produce runnable workflows. *)
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
    |> Bfx.List_repr.make


  let mutect_on_fastqs ~reference_build ~normal ~tumor =
    let aligner =
      Bfx.lambda (fun fq -> Bfx.bwa_aln ~reference_build fq) in
    let align_list list_of_fastqs =
      Bfx.List_repr.map list_of_fastqs ~f:aligner |> Bfx.merge_bams
    in
    Bfx.mutect
      ~configuration:Biokepi.Tools.Mutect.Configuration.default
      ~normal:(align_list normal)
      ~tumor:(align_list tumor)

  let run ~normal ~tumor =
    Bfx.observe (fun () ->
        mutect_on_fastqs
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

  let module Workflow_compiler =
    Biokepi.EDSL.Compile.To_workflow.Make(struct
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
    |> Biokepi.EDSL.Compile.To_workflow.File_type_specification.get_vcf
  in
  let pipeline_1_workflow_display =
    test_dir // "pipeline-1-workflow-display.txt" in
  write_file pipeline_1_workflow_display
    ~content:(workflow_1 |> Ketrew.EDSL.workflow_to_string);
  printf "Pipeline_1:\n\
         \  display: %s\n\
         \  JSON: %s\n\
         \  ketrew-display: %s\n%!"
    pipeline_1_display pipeline_1_json pipeline_1_workflow_display


