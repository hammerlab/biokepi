open Biokepi_run_environment
open Common


(*
  $ vaxrank --help
  usage: vaxrank [-h] --vcf VCF [--genome GENOME] --bam BAM
               [--min-mapping-quality MIN_MAPPING_QUALITY]
               [--use-duplicate-reads] [--drop-secondary-alignments]
               [--min-reads-supporting-variant-sequence MIN_READS_SUPPORTING_VARIANT_SEQUENCE]
               --mhc-predictor
               {netmhc,netmhccons,netmhccons-iedb,netmhciipan,netmhciipan-iedb,netmhcpan,netmhcpan-iedb,random,smm,smm-iedb,smm-pmbec,smm-pmbec-iedb}
               [--mhc-epitope-lengths MHC_EPITOPE_LENGTHS]
               [--mhc-alleles-file MHC_ALLELES_FILE]
               [--mhc-alleles MHC_ALLELES] [--output-csv OUTPUT_CSV]
               [--output-ascii-report OUTPUT_ASCII_REPORT]
               [--vaccine-peptide-length VACCINE_PEPTIDE_LENGTH]
               [--padding-around-mutation PADDING_AROUND_MUTATION]
               [--max-vaccine-peptides-per-mutation MAX_VACCINE_PEPTIDES_PER_MUTATION]
               [--max-mutations-in-report MAX_MUTATIONS_IN_REPORT]
*)
module Configuration = struct
  type t = {
    name: string;
    (* vaxrank-specific ones *)
    vaccine_peptide_length: int;
    padding_around_mutation: int;
    max_vaccine_peptides_per_mutation: int;
    max_mutations_in_report: int;
    (* isovar-like ones *)
    min_mapping_quality: int;
    min_reads_supporting_variant_sequence: int;
    use_duplicate_reads: bool;
    drop_secondary_alignments: bool;
    (* topiary-like ones *)
    mhc_epitope_lengths: int list;
    (* the rest *)
    parameters: (string * string) list;
  }
  let to_json {
    name;
    vaccine_peptide_length;
    padding_around_mutation;
    max_vaccine_peptides_per_mutation;
    max_mutations_in_report;
    min_mapping_quality;
    min_reads_supporting_variant_sequence;
    use_duplicate_reads;
    drop_secondary_alignments;
    mhc_epitope_lengths;
    parameters}: Yojson.Basic.json
    =
    `Assoc [
      "name", `String name;
      "vaccine_peptide_length", `Int vaccine_peptide_length;
      "padding_around_mutation", `Int padding_around_mutation;
      "max_vaccine_peptides_per_mutation", `Int max_vaccine_peptides_per_mutation;
      "max_mutations_in_report", `Int max_mutations_in_report;
      "min_mapping_quality", `Int min_mapping_quality;
      "min_reads_supporting_variant_sequence",
        `Int min_reads_supporting_variant_sequence;
      "use_duplicate_reads", `Bool use_duplicate_reads;
      "drop_secondary_alignments", `Bool drop_secondary_alignments;
      "mhc_epitope_lengths",
        `List (List.map mhc_epitope_lengths ~f:(fun i -> `Int i));
      "parameters",
      `Assoc (List.map parameters ~f:(fun (a, b) -> a, `String b));
    ]
  let render {
    name;
    vaccine_peptide_length;
    padding_around_mutation;
    max_vaccine_peptides_per_mutation;
    max_mutations_in_report;
    min_mapping_quality;
    min_reads_supporting_variant_sequence;
    use_duplicate_reads;
    drop_secondary_alignments;
    mhc_epitope_lengths;
    parameters}
    =
    let soi = string_of_int in
    ["--vaccine-peptide-length"; soi vaccine_peptide_length] @
    ["--padding-around-mutation"; soi padding_around_mutation] @
    ["--max-vaccine-peptides-per-mutation";
      soi max_vaccine_peptides_per_mutation] @
    ["--max-mutations-in-report"; soi max_mutations_in_report] @
    ["--min-mapping-quality"; soi min_mapping_quality] @
    ["--min-reads-supporting-variant-sequence";
      soi min_reads_supporting_variant_sequence] @
    (if use_duplicate_reads
      then ["--use-duplicate-reads"] else [""]) @
    (if drop_secondary_alignments
      then ["--drop_secondary_alignments"] else [""]) @
    ["--mhc-epitope-lengths";
      (mhc_epitope_lengths
        |> List.map ~f:string_of_int
        |> String.concat ~sep:",")] @
    (List.concat_map parameters ~f:(fun (a,b) -> [a; b]))
    |> List.filter ~f:(fun s -> not (String.is_empty s))

  let default =
    {name = "default";
     vaccine_peptide_length = 25;
     padding_around_mutation = 0;
     max_vaccine_peptides_per_mutation = 1;
     max_mutations_in_report = 10;
     min_mapping_quality = 1;
     min_reads_supporting_variant_sequence = 2;
     use_duplicate_reads = false;
     drop_secondary_alignments = false;
     mhc_epitope_lengths = [8; 9; 10; 11];
     parameters = []}
  let name t = t.name
end

type product = <
  is_done : Ketrew_pure.Target.Condition.t option ;
  text_report_path : string;
  csv_report_path: string;
  output_folder_path: string >

let run ~(run_with: Machine.t)
    ~configuration
    ~reference_build
    ~vcfs
    ~bam
    ~predictor
    ~alleles_file
    ~output_folder
  =
  let open KEDSL in
  let vaxrank =
    Machine.get_tool run_with Machine.Tool.Definition.(create "vaxrank")
  in
  let predictor_tool = Topiary.(predictor_to_tool ~run_with predictor) in
  let (predictor_edges, predictor_init) =
    match predictor_tool with
    | Some (e, i) -> ([depends_on e;], i)
    | None -> ([], Program.(sh "echo 'No external prediction tool required'"))
  in
  let vcfs_arg = List.concat_map vcfs ~f:(fun v -> ["--vcf"; v#product#path]) in
  let bam_arg = ["--bam"; bam#product#path] in
  let predictor_arg =
    ["--mhc-predictor"; (Topiary.predictor_to_string predictor)] in
  let allele_arg = ["--mhc-alleles-file"; alleles_file#product#path] in
  let output_prefix = output_folder // "vaxrank-result" in
  let csv_file = output_prefix ^ ".csv" in
  let ascii_file = output_prefix ^ ".txt" in
  let csv_arg = ["--output-csv"; csv_file] in
  let ascii_arg = ["--output-ascii-report"; ascii_file] in
  let arguments =
    vcfs_arg @ bam_arg @ predictor_arg @ allele_arg (* input *)
    @ csv_arg @ ascii_arg (* output *)
    @ Configuration.render configuration (* other config *)
  in
  let name = "Vaxrank run" in
  let host = Machine.(as_host run_with) in
  let product =
    let text_report = KEDSL.single_file ~host ascii_file in
    let csv_report = KEDSL.single_file ~host csv_file in
    object
      method is_done =
        Some (`And
                (List.filter_map ~f:(fun f -> f#is_done) [text_report; csv_report]))
      method text_report_path = ascii_file
      method csv_report_path = csv_file
      method output_folder_path = output_folder
    end
  in
  workflow_node
    product
    ~name
    ~edges:([
        depends_on (Samtools.index_to_bai ~run_with bam);
        depends_on Machine.Tool.(ensure vaxrank);
        depends_on (Pyensembl.cache_genome ~run_with ~reference_build);
        depends_on bam;
        depends_on alleles_file;
      ] @ (List.map ~f:depends_on vcfs)
        @ predictor_edges)
    ~make:(
      Machine.run_program run_with ~name
        Program.(
          Machine.Tool.(init vaxrank)
          && predictor_init
          && Pyensembl.(set_cache_dir_command ~run_with)
          && shf "mkdir -p %s" (Filename.quote output_folder)
          && exec (["vaxrank"] @ arguments)
        )
    )
