
open Nonstd
module K = Ketrew.EDSL

let say fmt = ksprintf (printf "%s\n%!") fmt

let test_assert n b =
  if b then () else say "%s failed!" n

let test_region () =
  let module R = Biokepi_run_environment.Region in
  let check_samtools_format spec =
    let samtools = R.to_samtools_specification spec in
    begin match samtools with
    | None  -> test_assert "check_samtools_format %s → not `Full" (spec = `Full)
    | Some s ->
      test_assert
        (sprintf "check_samtools_format %s Vs %s"
           (R.to_filename spec) s)
        (spec = R.parse_samtools s)
    end
  in
  List.iter ~f:check_samtools_format [
    `Full;
    `Chromosome "chr1";
    `Chromosome "1";
    `Chromosome "helloworld";
    `Chromosome_interval ("42", 24, 289);
    `Chromosome_interval ("42", 24, 0);
    `Chromosome_interval ("wiueueiwioow", 0, 289);
  ];
  ()

let () =
  test_region ();
  say "Done."

