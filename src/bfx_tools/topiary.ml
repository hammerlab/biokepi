open Biokepi_run_environment
open Common

type variant_data_type = [
    `VCF_file of string
  | `MAF_file of string
  | `Variant of string * int * string * string
]

type output_type = [
    `HTML of string
  | `CSV of string
]

type allele_description = [`File of string | `Alleles of string list]

type predictor_type = [
    `NetMHC
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
    `NetMHC -> "netmhc"
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


let run ~(run_with: Machine.t)
  ?rna_gene_fpkm_tracking_file
  ?rna_min_gene_expression
  ?rna_transcript_fpkm_tracking_file
  ?rna_min_transcript_expression
  ?rna_transcript_fkpm_gtf_file
  ?mhc_epitope_lengths
  ?only_novel_epitopes
  ?ic50_cutoff
  ?percentile_cutoff
  ?padding_around_mutation
  ?self_filter_directory
  ?(skip_variant_errors=false)
  ~variants
  ~predictor
  ~alleles
  ~output =

  let open KEDSL in
  let topiary =
    Machine.get_tool run_with Machine.Tool.Definition.(create "topiary")
  in
  let var_arg = 
    match variants with
    | `VCF_file vcf_file -> ["--vcf"; vcf_file]
    | `MAF_file maf_file -> ["--maf"; maf_file]
    | `Variant (chr, pos, refseq, altseq) -> ["--variant"; chr; pos; refseq; altseq;]
  in
  let predictor_arg = ["--predictor"; (predictor_to_string predictor)] in
  let allele_arg =
    match alleles with
    | `File file -> ["--mhc-alleles-file"; file]
    | `Alleles a_list -> ["--mhc-alleles"; (String.concat ~sep:"," a_list)]
  in
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
  let rna_arg = 
      (maybe_argument ~f:str_of_str "--rna-gene-fpkm-tracking-file" rna_gene_fpkm_tracking_file)
    @ (maybe_argument ~f:string_of_float "--rna-min-gene-expression" rna_min_gene_expression)
    @ (maybe_argument ~f:str_of_str "--rna-transcript-fpkm-tracking-file" rna_transcript_fpkm_tracking_file)
    @ (maybe_argument ~f:string_of_float "--rna-min-transcript-expression" rna_min_transcript_expression)
    @ (maybe_argument ~f:str_of_str "--rna-transcript-fpkm-gtf-file" rna_transcript_fkpm_gtf_file)
  in
  let string_of_intlist l = l |> List.map ~f:string_of_int |> String.concat ~sep:"," in
  let length_arg = maybe_argument ~f:string_of_intlist "--mhc-epitope-lengths" mhc_epitope_lengths in
  let novel_arg = maybe_argument ~f:str_of_str "--only-novel-epitopes" only_novel_epitopes in
  let ic50_arg = maybe_argument ~f:string_of_float "--ic50-cutoff" ic50_cutoff in
  let percentile_arg = maybe_argument ~f:string_of_float "--percentile-cutoff" percentile_cutoff in
  let padding_arg = maybe_argument ~f:string_of_int "--padding-around-mutation" padding_around_mutation in 
  let self_filter_directory = maybe_argument ~f:str_of_str "--self-filter-directory" self_filter_directory in
  let skip_error_arg = if skip_variant_errors then ["--skip-variant-errors"] else [] in
  let arguments = 
      var_arg @ predictor_arg @ allele_arg @ output_arg @ rna_arg
      @ length_arg @ novel_arg @ ic50_arg @ percentile_arg @ padding_arg
      @ skip_error_arg @ self_filter_directory
  in
  let name = sprintf "topiary_%s" (Filename.basename output_path) in
  workflow_node
    (single_file output_path ~host:Machine.(as_host run_with))
    ~name
    ~edges:[
      depends_on Machine.Tool.(ensure topiary);
    ]
    ~make:(
      Machine.run_program run_with ~name
        Program.(
          Machine.Tool.(init topiary)
          && exec (["topiary"] @ arguments)
        )
    )