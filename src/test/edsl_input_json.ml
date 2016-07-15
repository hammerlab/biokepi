
open Nonstd
module String = Sosa.Native_string
open Biokepi.EDSL.Library.Input


let examples = [
  "1", fastq_sample ~sample_name:"Sample1" [];
  "2", fastq_sample ~sample_name:"Sample2" [
    pe ~fragment_id:"frg1" "frg1R1.fastq" "frg1R2.fastq";
    se ~fragment_id:"frg2" "frg2.fastq";
    of_bam  ~sorted:`Coordinate ~reference_build:"b37" `PE "n2.bam";
  ];
  "3", fastq_sample ~sample_name:"Sample3" [
    pe "frg1R1.fastq" "frg1R2.fastq";
    se "frg2.fastq";
    of_bam  ~sorted:`Read_name ~reference_build:"b37" `SE "n2.bam";
  ];
]

let () =
  let print_them = try Sys.argv.(1) = "print" with _ -> false in
  List.iter examples ~f:(fun (name, ex) ->
      let json = to_yojson ex in
      let result = of_yojson json in
      let print_both () =
        sprintf "JSON ->\n%s\nPARSED-TO:\n%s\n"
          (Yojson.Safe.pretty_to_string ~std:true json)
          (match result with
          | `Ok o -> show o
          | `Error s -> sprintf "ERROR: %s" s)
      in
      printf "Example %S: %s\n%!" name
        begin match result with
        | `Ok o when o = ex -> sprintf "OK and Equal"
        | `Ok o -> sprintf "OK but NOT EQUAL\n%s" (print_both ())
        | `Error e -> sprintf "ERROR:\n%s" (print_both ())
        end;
      if print_them
      then
        printf "\n```````````````json\n%s\n```````````````\n\n%!"
          (Yojson.Safe.pretty_to_string ~std:true json);
    )
