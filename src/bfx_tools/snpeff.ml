open Biokepi_run_environment
open Common

module Remove = Workflow_utilities.Remove

let get_snpeff_db_name ~run_with reference_build =
  Machine.(get_reference_genome run_with reference_build)
  |> Reference_genome.snpeff_name_exn

let get_snpeff_data_folder ~run_with =
  let snpeff = Machine.Tool.Default.snpeff in
  let tname, tversion = 
    Machine.Tool.Definition.(get_name snpeff, get_version snpeff)
  in
  sprintf "${CONDA_PREFIX}/share/%s-%s/data"
    tname (match tversion with None -> "*" | Some v -> v)

(* Note the 'E' in caps in the name!
   Also we are setting -Xms because the default is too small
   for almost all genomes. This will make annotations faster.
 *)
let call_snpeff fmt = sprintf ("%s " ^^ fmt) ("snpEff -Xms4g")

let prepare_annotation ~(run_with:Machine.t) ~reference_build =
  let open KEDSL in
  let snpeff = Machine.get_tool run_with Machine.Tool.Default.snpeff in
  let host = Machine.as_host run_with in
  let dbname = get_snpeff_db_name ~run_with reference_build in
  let data_path = get_snpeff_data_folder ~run_with in
  let genome_data_path = data_path // dbname in
  let witness_path = genome_data_path // "snpEffectPredictor.bin" in
  let product =
    Workflow_utilities.Variable_tool_paths.single_file 
      ~host ~tool:snpeff witness_path
  in
  let name = sprintf "Preparing snpEFF DB for %s" reference_build in
  let make =
    Machine.(run_program run_with
      ~requirements:[
        `Self_identification ["annotation"; "prep"; "snpeff"];
        `Internet_access; ]
      Program.(
        Machine.Tool.init snpeff &&
        sh (call_snpeff "download %s" dbname)
      )
    )
  in
  workflow_node ~name ~make product
    ~edges: [
      on_failure_activate (Remove.directory ~run_with genome_data_path);
      depends_on (Machine.Tool.ensure snpeff);
    ]

type snpeff_product = <
  host : Ketrew_pure.Host.t;
  is_done : Ketrew_pure.Target.Condition.t option;
  csv_stats : KEDSL.single_file;
  html_stats : KEDSL.single_file;
  workdir_path : string;
  annotated_vcf : KEDSL.vcf_file;
  path : string; >

let annotate ~(run_with:Machine.t) ~reference_build ~input_vcf ~out_folder =
  let open KEDSL in
  let snpeff = Machine.get_tool run_with Machine.Tool.Default.snpeff in
  let host = Machine.as_host run_with in
  let in_vcf_path = input_vcf#product#path in
  let in_basename = (Filename.basename in_vcf_path) in
  let in_simplename = (Filename.chop_extension in_basename) in
  let out_path suffix =
      out_folder // (sprintf "%s-snpeff%s" in_simplename suffix)
  in
  let annotated_vcf = transform_vcf input_vcf#product ~path:(out_path ".vcf") in
  let out_vcf = annotated_vcf#as_single_file in
  let out_csv = single_file ~host (out_path ".csv") in
  let out_html = single_file ~host (out_path".html") in
  let out_product =
    let sf_products = [out_vcf; out_csv; out_html;] in
    let all_done = List.filter_map ~f:(fun f -> f#is_done) sf_products in
    object
      method host = host
      method is_done = Some (`And all_done)
      method csv_stats = out_csv
      method html_stats = out_html
      method annotated_vcf = annotated_vcf 
      method workdir_path = out_folder
      (* Annotated VCF will often be the only thing 
         that we care about, hence the path points to that *)
      method path = annotated_vcf#path
    end
  in
  let dbname = get_snpeff_db_name ~run_with reference_build in
  let name = sprintf "snpEff: annotate %s against %s" in_basename dbname in
  let annotate_cmd =
    call_snpeff "eff -nodownload -s %s -csvStats %s %s %s > %s"
      out_html#path out_csv#path dbname in_vcf_path out_vcf#path
  in
  let make =
    Machine.(run_program run_with
      ~requirements:[
        `Self_identification ["snpEff"; "annotate"; "vcf";]
      ]
      Program.(
        Tool.init snpeff
        && shf "mkdir -p %s" out_folder
        && sh annotate_cmd
      )
    )
  in
  workflow_node ~name ~make out_product
    ~edges: [
      on_failure_activate (Remove.directory ~run_with out_folder);
      depends_on (Machine.Tool.ensure snpeff);
      depends_on input_vcf;
      depends_on (prepare_annotation ~run_with ~reference_build)
    ]
