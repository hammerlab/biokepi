
open Nonstd
module K = Ketrew.EDSL

let say fmt = ksprintf (printf "%s\n%!") fmt

let test_assert n b =
  if b then () else say "%s failed!" n

let test_region () =
  let check_samtools_format spec =
    let samtools = Biokepi_region.to_samtools_specification spec in
    begin match samtools with
    | None  -> test_assert "check_samtools_format %s → not `Full" (spec = `Full)
    | Some s ->
      test_assert
        (sprintf "check_samtools_format %s Vs %s"
           (Biokepi_region.to_filename spec) s)
        (spec = Biokepi_region.parse_samtools s)
    end
  in
  List.iter Biokepi_region.all_chromosomes_b37 ~f:check_samtools_format;
  List.iter ~f:check_samtools_format [
    `Full;
    `Chromosome_interval ("42", 24, 289);
    `Chromosome_interval ("42", 24, 0);
    `Chromosome_interval ("wiueueiwioow", 0, 289);
  ];
  ()

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
  test_region ();
  dump_dumb_pipeline_example ();
  say "Done."

