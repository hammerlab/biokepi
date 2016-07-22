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
  let render {parameters; _} =
    List.concat_map parameters ~f:(fun (a,b) -> [a; b])

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

let run ~(run_with: Machine.t)
  ~configuration 
  ~reference_build
  ~vcf
  ~bam
  ~predictor 
  ~alleles_file
  ~output_folder
  =
  let open KEDSL in
  let open Configuration in
  let c = configuration in
  let vaxrank =
    Machine.get_tool run_with Machine.Tool.Definition.(create "vaxrank")
  in
  let var_arg = ["--vcf"; vcf#product#path] in
  let predictor_arg = 
    ["--mhc-predictor"; (Topiary.predictor_to_string predictor)] in
  let allele_arg = ["--mhc-alleles-file"; alleles_file#product#path] in
  let soi = string_of_int in
  let vax_length_arg = ["--vaccine-peptide-length"; soi c.vaccine_peptide_length] in
  let padding_arg = ["--padding-around-mutation"; soi c.padding_around_mutation] in
  let vax_per_mut_arg = ["--max-vaccine-peptides-per-mutation"; soi c.max_vaccine_peptides_per_mutation] in
  let max_muts_arg = ["--max-mutations-in-report"; soi c.max_mutations_in_report] in
  let min_qual_arg = ["--min-mapping-quality"; soi c.min_mapping_quality] in
  let min_reads_arg = ["--min-reads-supporting-variant-sequence"; soi c.min_reads_supporting_variant_sequence] in
  let if_argument arg_name value = if value then [arg_name] else [] in
  let use_dups_arg = if_argument "--use-duplicate-reads" c.use_duplicate_reads in
  let drop_sec_arg = if_argument "--drop-secondary-alignments" c.drop_secondary_alignments in
  let epi_length_arg = 
    ["--mhc-epitope-lengths"; 
     c.mhc_epitope_lengths |> List.map ~f:soi |> String.concat ~sep:","]
  in
  let output_prefix = output_folder // "vaxrank-result" in
  let csv_file = output_prefix ^ ".csv" in
  let ascii_file = output_prefix ^ ".txt" in
  let csv_arg = ["--output-csv"; csv_file] in
  let ascii_arg = ["--output-ascii-report"; ascii_file] in
  let arguments = 
    var_arg @ predictor_arg @ allele_arg (* input *)
    @ csv_arg @ ascii_arg (* outputs *)
    @ vax_length_arg @ padding_arg (* etc *)
    @ vax_per_mut_arg @ max_muts_arg @ min_qual_arg @ min_reads_arg
    @ use_dups_arg @ drop_sec_arg @ epi_length_arg
    @ Configuration.render configuration
  in
  let name = "Vaxrank run" in
  workflow_node
    (list_of_files [csv_file; ascii_file] ~host:(Machine.as_host run_with))
    ~name
    ~edges:[
      depends_on Machine.Tool.(ensure vaxrank);
      depends_on (Pyensembl.cache_genome ~run_with ~reference_build);
      depends_on vcf;
      depends_on bam;
      depends_on alleles_file;
    ]
    ~make:(
      Machine.run_program run_with ~name
        Program.(
          Machine.Tool.(init vaxrank)
          && exec (["vaxrank"] @ arguments)
        )
    )
