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


  let mutect_on_fastqs ~normal ~tumor =
    let align_list list_of_fastqs =
      Bfx.List_repr.map list_of_fastqs ~f:(Bfx.lambda (fun fq -> Bfx.bwa_aln fq))
      |> Bfx.merge_bams
    in
    Bfx.mutect
      ~configuration:Biokepi.Tools.Mutect.Configuration.default
      ~normal:(align_list normal)
      ~tumor:(align_list tumor)

  let run ~normal ~tumor =
    Bfx.observe (fun () ->
        mutect_on_fastqs
          ~normal:(fastq_list ~dataset:(fst normal) (snd normal))
          ~tumor:(fastq_list ~dataset:(fst tumor) (snd tumor))
      )

end

let normal_1 = 
  ("normal-1", [
      `Pair ("normal-1-001-r1.fastq", "normal-1-001-r2.fastq"); 
      `Pair ("normal-1-002-r1.fastq", "normal-1-002-r2.fastq"); 
      `Pair ("normal-1-003-r1.fqz", "normal-1-003-r2.fqz"); 
    ])

let tumor_1 = 
  ("tumor-1", [
      `Pair ("tumor-1-001-r1.fastq.gz", "tumor-1-001-r2.fastq.gz"); 
      `Pair ("tumor-1-002-r1.fastq.gz", "tumor-1-002-r2.fastq.gz"); 
      `Pair ("tumor-1-003-r1.fastq.gz", "tumor-1-003-r2.fastq.gz"); 
    ])

let () =
  let module Display_pipeline_1 =
    Pipeline_1(Biokepi.EDSL.Compile.To_display)
  in
  printf "Pipeline 1:\n\n%s\n\n%!"
    (Display_pipeline_1.run ~normal:normal_1 ~tumor:tumor_1
     |> SmartPrint.to_string 80 2)

