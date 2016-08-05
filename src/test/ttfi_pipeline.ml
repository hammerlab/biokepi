(** This test uses the high-level EDSL and tries different compilation targets,
    but does not produce runnable workflows.


    The workflow itself is voluntarily too big and abuses lambda expressions.
*)
open Nonstd

module type TEST_PIPELINE =
  functor (Bfx : Biokepi.EDSL.Semantics) ->
  sig
    val run : unit -> unit Bfx.observation
  end


module Run_test(Test_pipeline : TEST_PIPELINE) = struct

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
        | other -> ksprintf failwith "non-zero-exit: %s -> %d" s other) fmt

  let (//) = Filename.concat
  let test_dir = "_build/ttfi-test-results/"

  let main prefix =
    let start_time = Unix.gettimeofday () in
    cmdf "mkdir -p %s" test_dir;
    let results = ref [] in
    let add_result fmt = ksprintf (fun s -> results := s :: !results) fmt in
    let add_result_file name path =
      let size =
        let s = Unix.stat path  in
        match s.Unix.st_size with
        | 0 -> "EMPTY"
        | small when small < 1024 -> sprintf "%d B" small
        | avg when avg < (1024 * 1024) ->
          sprintf "%.2f KB" (float avg /. 1024.)
        | big ->
          sprintf "%.2f MB" (float big /. (1024. *. 1024.))
      in
      add_result "%s: `%s` (%s)" name path size;
    in
    let output_path suffix = test_dir // prefix ^ suffix in
    begin
      let module Display_pipeline = Test_pipeline(Biokepi.EDSL.Compile.To_display) in
      let pseudocode = output_path "-pseudocode.txt" in
      write_file pseudocode
        ~content:(Display_pipeline.run () |> SmartPrint.to_string 80 2);
      add_result_file "Pseudo-code" pseudocode;
    end;
    begin
      let module Jsonize_pipeline =
        Test_pipeline(Biokepi.EDSL.Compile.To_json) in
      let json = output_path ".json" in
      write_file json
        ~content:(Jsonize_pipeline.run ()
                  |> Yojson.Basic.pretty_to_string ~std:true);
      add_result_file "JSON" json;
    end;
    let output_dot sm dot png =
      try
        let out = open_out dot in
        SmartPrint.to_out_channel  80 2 out sm;
        close_out out;
        add_result_file "DOT" dot;
        let dotlog = png ^ ".log" in
        cmdf "dot -v -x -Tpng  %s -o %s > %s 2>&1" dot png dotlog;
        add_result_file "PNG" png;
      with e ->
        add_result "FAILED TO OUTPUT: %s (%s)" dot (Printexc.to_string e);
    in
    begin
      let module Dotize_pipeline = Test_pipeline(Biokepi.EDSL.Compile.To_dot) in
      let sm_dot =
        Dotize_pipeline.run () Biokepi.EDSL.Compile.To_dot.default_parameters in
      let dot = output_path "-1.dot" in
      let png = output_path "-1.png" in
      output_dot sm_dot dot png
    end;
    begin
      let module Dotize_twice_beta_reduced_pipeline =
        Test_pipeline(
          Biokepi.EDSL.Transform.Apply_functions(
            Biokepi.EDSL.Transform.Apply_functions(
              Biokepi.EDSL.Compile.To_dot
            )
          )
        )
      in
      let dot = output_path "-double-beta.dot" in
      let sm_dot =
        Dotize_twice_beta_reduced_pipeline.run ()
          ~parameters:Biokepi.EDSL.Compile.To_dot.default_parameters in
      let png = output_path "-double-beta.png" in
      output_dot sm_dot dot png
    end;
    begin
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
      let module Ketrew_pipeline = Test_pipeline(Workflow_compiler) in
      let workflow =
        Ketrew_pipeline.run ()
        |> Biokepi.EDSL.Compile.To_workflow.File_type_specification.get_unit_workflow
          ~name:"Biokepi TTFI test top-level node"
      in
      ignore workflow
    end;
    let end_time = Unix.gettimeofday () in
    add_result "Total-time: %.2f s" (end_time -. start_time);
    List.rev !results

end

module Pipeline_insane (Bfx : Biokepi.EDSL.Semantics) = struct

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
    let align_list how (list_of_fastqs : [> `Fastq ] list Bfx.repr) =
      Bfx.list_map list_of_fastqs ~f:how |> Bfx.merge_bams
    in
    let bwa_mem_simple: reference_build: string -> [`Fastq] Bfx.repr -> [`Bam] Bfx.repr =
      fun ~reference_build x ->
        Bfx.bwa_mem ~reference_build ?configuration:None (x :> [ `Fastq | `Bam | `Gz of [ `Fastq ]] Bfx.repr) in
    let aligners = Bfx.list [
        aligner @@ Bfx.bwa_aln ?configuration: None;
        aligner @@ bwa_mem_simple;
        aligner @@ Bfx.hisat ~configuration:Biokepi.Tools.Hisat.Configuration.default_v1;
        aligner @@ Bfx.hisat ~configuration:Biokepi.Tools.Hisat.Configuration.default_v2;
        aligner @@ Bfx.star ~configuration:Biokepi.Tools.Star.Configuration.Align.default;
        aligner @@ Bfx.mosaik;
        aligner @@ (fun ~reference_build fastq ->
            Bfx.bwa_aln ~reference_build fastq
            |> Bfx.bam_to_fastq `PE
            |> Bfx.bwa_mem ?configuration: None ~reference_build
          );
      ] in
    let somatic_of_pair how =
      Bfx.lambda (fun pair ->
          let normal = Bfx.pair_first pair in
          let tumor = Bfx.pair_second pair in
          how ~normal ~tumor ())
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
                (Bfx.lambda (fun p -> Bfx.pair_first p |> Bfx.concat |> Bfx.fastqc))
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

  let normal =
    ("normal-1", [
        `Pair ("/input/normal-1-001-r1.fastq", "/input/normal-1-001-r2.fastq");
        `Pair ("/input/normal-1-002-r1.fastq", "/input/normal-1-002-r2.fastq");
        `Pair ("/input/normal-1-003-r1.fqz",   "/input/normal-1-003-r2.fqz");
      ])

  let tumor =
    ("tumor-1", [
        `Pair ("/input/tumor-1-001-r1.fastq.gz", "/input/tumor-1-001-r2.fastq.gz");
        `Pair ("/input/tumor-1-002-r1.fastq.gz", "/input/tumor-1-002-r2.fastq.gz");
      ])

  let run () =
    Bfx.observe (fun () ->
        every_vc_on_fastqs
          ~reference_build:"b37"
          ~normal:(fastq_list ~dataset:(fst normal) (snd normal))
          ~tumor:(fastq_list ~dataset:(fst tumor) (snd tumor))
      )

end

module Somatic_simplish(Bfx: Biokepi.EDSL.Semantics) = struct

  module Insane_library = Pipeline_insane(Bfx)

  let vc =
    let normal =
      Insane_library.(fastq_list  ~dataset:(fst normal) (snd normal))
    in
    let tumor =
      Insane_library.(fastq_list  ~dataset:(fst tumor) (snd tumor))
    in
    let ot_hla =
      normal |> Bfx.concat |> Bfx.optitype `DNA |> Bfx.to_unit
    in
    let align fastq =
      Bfx.list_map fastq ~f:(Bfx.lambda (fun fq ->
          Bfx.bwa_mem fq
            ~reference_build:"b37"
          |> Bfx.picard_mark_duplicates
            ~configuration:Biokepi.Tools.Picard.Mark_duplicates_settings.default
        )) in
    let normal_bam = align normal |> Bfx.merge_bams in
    let tumor_bam = align tumor |> Bfx.merge_bams in
    let bam_pair = Bfx.pair normal_bam tumor_bam in
    let indel_realigned_pair =
      Bfx.gatk_indel_realigner_joint bam_pair
        ~configuration: Biokepi.Tools.Gatk.Configuration.default_indel_realigner
    in
    let final_normal_bam =
      Bfx.pair_first indel_realigned_pair
      |> Bfx.gatk_bqsr
        ~configuration: Biokepi.Tools.Gatk.Configuration.default_bqsr
    in
    let final_tumor_bam =
      Bfx.pair_second indel_realigned_pair
      |> Bfx.gatk_bqsr
        ~configuration: Biokepi.Tools.Gatk.Configuration.default_bqsr
    in
    let vcfs =
      let normal, tumor = final_normal_bam, final_tumor_bam in
      Bfx.list [
        Bfx.mutect ~normal ~tumor ();
        Bfx.somaticsniper ~normal ~tumor ();
        Bfx.strelka ~normal ~tumor ();
      ] in
    Bfx.list [
      Bfx.to_unit vcfs;
      ot_hla;
    ] |> Bfx.to_unit

  let run () =
    Bfx.observe (fun () -> vc)
end

let () =
  let module Go_insane = Run_test(Pipeline_insane) in
  let insane_result =  Go_insane.main "pipeline-insane" in
  let module Go_simple_somatic = Run_test(Somatic_simplish) in
  let simple_somatic_result =  Go_simple_somatic.main "pipeline-simple-somatic" in
  let display_result mod_name results =
    printf "- `%s`:\n%s\n%!"
      mod_name
      (List.map results ~f:(sprintf "    * %s\n") |> String.concat "");
  in
  printf "\n### Test results:\n\n";
  display_result "Pipeline_insane" insane_result;
  display_result "Somatic_simplish" simple_somatic_result;
  ()
