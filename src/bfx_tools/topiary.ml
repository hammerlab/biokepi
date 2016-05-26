open Biokepi_run_environment
open Common

type output_type = [
  | `HTML of string
  | `CSV of string
]

type predictor_type = [
  | `NetMHC
  | `NetMHCpan
  | `NetMHCIIpan
  | `NetMHCcons
  | `Random
  | `SMM
  | `SSM_PMBEC
  | `NetMHCpan_IEDB
  | `NetMHCcons_IEDB
  | `SSM_IEDB
  | `SSM_PMBEC_IEDB
]

let predictor_to_string = function
  | `NetMHC -> "netmhc"
  | `NetMHCpan -> "netmhcpan"
  | `NetMHCIIpan -> "netmhciipan"
  | `NetMHCcons -> "netmhccons"
  | `Random -> "random"
  | `SMM -> "ssm"
  | `SSM_PMBEC -> "ssm-pmbec"
  | `NetMHCpan_IEDB -> "netmhcpan-iedb"
  | `NetMHCcons_IEDB -> "netmhccons-iedb"
  | `SSM_IEDB -> "smm-iedb"
  | `SSM_PMBEC_IEDB -> "smm-pmbec-iedb"


module Configuration = struct

  type t = {
    name: string;
    rna_gene_fpkm_tracking_file: string option;
    rna_min_gene_expression: float;
    rna_transcript_fpkm_tracking_file: string option;
    rna_min_transcript_expression: float;
    rna_transcript_fkpm_gtf_file: string option;
    mhc_epitope_lengths: int list;
    only_novel_epitopes: bool;
    ic50_cutoff: float;
    percentile_cutoff: float;
    padding_around_mutation: int option;
    self_filter_directory: string option;
    skip_variant_errors: bool;
    parameters: (string * string) list;
  }
  let to_json {
    name;
    rna_gene_fpkm_tracking_file; 
    rna_min_gene_expression;
    rna_transcript_fpkm_tracking_file;
    rna_min_transcript_expression;
    rna_transcript_fkpm_gtf_file;
    mhc_epitope_lengths;
    only_novel_epitopes;
    ic50_cutoff;
    percentile_cutoff;
    padding_around_mutation;
    self_filter_directory;
    skip_variant_errors;
    parameters}: Yojson.Basic.json =
    `Assoc [
      "name", `String name;
      "rna_gene_fpkm_tracking_file", 
        (match rna_gene_fpkm_tracking_file with
        | None -> `Null
        | Some s -> `String s);
      "rna_min_gene_expression", `Float rna_min_gene_expression;
      "rna_transcript_fpkm_tracking_file",
        (match rna_transcript_fpkm_tracking_file with
        | None -> `Null
        | Some s -> `String s);
      "rna_min_transcript_expression", `Float rna_min_transcript_expression;
      "rna_transcript_fkpm_gtf_file",
        (match rna_transcript_fkpm_gtf_file with
        | None -> `Null
        | Some s -> `String s);
      "mhc_epitope_lengths", 
        `List (List.map mhc_epitope_lengths ~f:(fun i -> `Int i));
      "only_novel_epitopes", `Bool only_novel_epitopes;
      "ic50_cutoff", `Float ic50_cutoff;
      "percentile_cutoff", `Float percentile_cutoff;
      "padding_around_mutation",
        (match padding_around_mutation with
        | None -> `Null
        | Some i -> `Int i);
      "self_filter_directory",
        (match self_filter_directory with
        | None -> `Null
        | Some s -> `String s);
      "skip_variant_errors", `Bool skip_variant_errors;
      "parameters",
        `Assoc (List.map parameters ~f:(fun (k, s) -> k, `String s));
    ]
  let render {parameters; _} =
    List.concat_map parameters ~f:(fun (a,b) -> [a; b])

  let default =
    {
      name = "default";
      rna_gene_fpkm_tracking_file = None;
      rna_min_gene_expression = 4.0;
      rna_transcript_fpkm_tracking_file = None;
      rna_min_transcript_expression = 1.5;
      rna_transcript_fkpm_gtf_file = None;
      mhc_epitope_lengths = [8; 9; 10; 11];
      only_novel_epitopes = false;
      ic50_cutoff = 500.0;
      percentile_cutoff = 2.0;
      padding_around_mutation = None;
      self_filter_directory = None;
      skip_variant_errors = false;
      parameters = []
    }

  let name t = t.name
end


let run ~(run_with: Machine.t)
  ~configuration 
  ~reference_build
  ~variants_vcf
  ~predictor
  ~alleles_file
  ~output =
  let open KEDSL in
  let topiary =
    Machine.get_tool run_with Machine.Tool.Definition.(create "topiary")
  in
  let var_arg = ["--vcf"; variants_vcf#product#path] in
  let predictor_arg = ["--predictor"; (predictor_to_string predictor)] in
  let allele_arg = ["--mhc-alleles-file"; alleles_file#product#path] in
  let (output_arg, output_path) = 
    match output with
    | `HTML html_file -> ["--output-html"; html_file], html_file
    | `CSV csv_file -> ["--output-csv"; csv_file], csv_file
  in
  let str_of_str a = a in
  let maybe_argument ~f arg_name value = 
    match value with
    | None -> []
    | Some arg_value -> [arg_name; f arg_value]
  in
  let if_argument arg_name value = if value then [arg_name] else [] in
  let open Configuration in
  let c = configuration in
  let rna_arg = 
      (maybe_argument ~f:str_of_str "--rna-gene-fpkm-tracking-file" c.rna_gene_fpkm_tracking_file)
    @ (maybe_argument ~f:string_of_float "--rna-min-gene-expression" (Some c.rna_min_gene_expression))
    @ (maybe_argument ~f:str_of_str "--rna-transcript-fpkm-tracking-file" c.rna_transcript_fpkm_tracking_file)
    @ (maybe_argument ~f:string_of_float "--rna-min-transcript-expression" (Some c.rna_min_transcript_expression))
    @ (maybe_argument ~f:str_of_str "--rna-transcript-fpkm-gtf-file" c.rna_transcript_fkpm_gtf_file)
  in
  let string_of_intlist l = l |> List.map ~f:string_of_int |> String.concat ~sep:"," in
  let length_arg = maybe_argument ~f:string_of_intlist "--mhc-epitope-lengths" (Some c.mhc_epitope_lengths) in
  let ic50_arg = maybe_argument ~f:string_of_float "--ic50-cutoff" (Some c.ic50_cutoff) in
  let percentile_arg = maybe_argument ~f:string_of_float "--percentile-cutoff" (Some c.percentile_cutoff) in
  let padding_arg = maybe_argument ~f:string_of_int "--padding-around-mutation" c.padding_around_mutation in 
  let self_filter_directory = maybe_argument ~f:str_of_str "--self-filter-directory" c.self_filter_directory in
  let skip_error_arg = if_argument "--skip-variant-errors" c.skip_variant_errors in
  let novel_arg = if_argument "--only-novel-epitopes" c.only_novel_epitopes in
  let ensembl = 
    Machine.(get_reference_genome run_with reference_build) 
    |> Reference_genome.ensembl
  in
  let ensembl_arg = ["--ensembl-version"; string_of_int ensembl] in
  let arguments = 
      var_arg @ predictor_arg @ allele_arg @ output_arg @ rna_arg
      @ length_arg @ novel_arg @ ic50_arg @ percentile_arg @ padding_arg
      @ skip_error_arg @ self_filter_directory @ ensembl_arg
      @ Configuration.render configuration
  in
  let name = sprintf "topiary_%s" (Filename.basename output_path) in
  workflow_node
    (single_file output_path ~host:Machine.(as_host run_with))
    ~name
    ~edges:[
      depends_on Machine.Tool.(ensure topiary);
      depends_on (Pyensembl.cache_genome ~run_with ~reference_build);
      depends_on variants_vcf;
      depends_on alleles_file;
    ]
    ~make:(
      Machine.run_program run_with ~name
        Program.(
          Machine.Tool.(init topiary)
          && exec (["topiary"] @ arguments)
        )
    )