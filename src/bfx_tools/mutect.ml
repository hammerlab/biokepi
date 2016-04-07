open Biokepi_run_environment
open Common

module Remove = Workflow_utilities.Remove

module Configuration = struct

  type t = {
    name: string;
    with_cosmic: bool;
    with_dbsnp: bool;
    parameters: (string * string) list;
  }

  let to_json {name; with_cosmic; with_dbsnp; parameters}: Yojson.Basic.json =
    `Assoc [
      "name", `String name;
      "with-cosmic", `Bool with_cosmic;
      "with-dbsnp", `Bool with_dbsnp;
      "parameters",
      `Assoc (List.map parameters ~f:(fun (a, b) -> a, `String b));
    ]
  let render {parameters; _} =
    List.concat_map parameters ~f:(fun (a,b) -> [a; b])

  let default =
    {name = "default";
     with_cosmic = true; with_dbsnp = true;
     parameters = []}
  let default_without_cosmic =
    {name = "default_without_cosmic";
     with_cosmic = false; with_dbsnp = true;
     parameters = []}


end

let run
    ~(run_with:Machine.t) ~normal ~tumor ~result_prefix
    ?(more_edges = []) ~configuration how =
  let open KEDSL in
  let reference =
    Machine.get_reference_genome run_with normal#product#reference_build in
  let run_on_region ~add_edges region =
    let result_file suffix =
      let region_name = Region.to_filename region in
      sprintf "%s-%s%s" result_prefix region_name suffix in
    let intervals_option = Region.to_gatk_option region in
    let output_file = result_file "-somatic.vcf" in
    let dot_out_file = result_file "-output.out"in
    let coverage_file = result_file "coverage.wig" in
    let mutect = Machine.get_tool run_with Machine.Tool.Default.mutect in
    let run_path = Filename.dirname output_file in
    let fasta = Reference_genome.fasta reference in
    let cosmic =
      match configuration.Configuration.with_cosmic with
      | true -> Some (Reference_genome.cosmic_exn reference)
      | false -> None in
    let dbsnp =
      match configuration.Configuration.with_dbsnp with
      | true -> Some (Reference_genome.dbsnp_exn reference)
      | false -> None in
    let fasta_dot_fai = Samtools.faidx ~run_with fasta in
    let sequence_dict = Picard.create_dict ~run_with fasta in
    let sorted_normal =
      Samtools.sort_bam_if_necessary
        ~processors:2 ~run_with ~by:`Coordinate normal in
    let sorted_tumor =
      Samtools.sort_bam_if_necessary
        ~processors:2 ~run_with ~by:`Coordinate tumor in
    let run_mutect =
      let name = sprintf "%s" (Filename.basename output_file) in
      let cosmic_option =
        Option.value_map ~default:"" cosmic ~f:(fun node ->
            sprintf "--cosmic %s" (Filename.quote node#product#path)) in
      let dbsnp_option =
        Option.value_map ~default:"" dbsnp ~f:(fun node ->
            sprintf "--dbsnp %s" (Filename.quote node#product#path)) in
      let make =
        Machine.run_big_program run_with ~name ~processors:2 Program.(
            Machine.Tool.(init mutect)
            && shf "mkdir -p %s" run_path
            && shf "cd %s" run_path
            && sh ("java -Xmx2g -jar $mutect_HOME/muTect-*.jar --analysis_type MuTect " ^
                   (String.concat ~sep:" " ([
                        "--reference_sequence"; fasta#product#path;
                        cosmic_option; dbsnp_option; intervals_option;
                        "--input_file:normal"; sorted_normal#product#path;
                        "--input_file:tumor"; sorted_tumor#product#path;
                        "--out"; dot_out_file;
                        "--vcf"; output_file;
                        "--coverage_file"; coverage_file;
                      ] @ Configuration.render configuration))))
      in
      let edges =
        Option.value_map cosmic ~default:[] ~f:(fun n -> [depends_on n])
        @ Option.value_map dbsnp ~default:[] ~f:(fun n -> [depends_on n])
        @ add_edges
        @ [
          depends_on Machine.Tool.(ensure mutect);
          depends_on sorted_normal;
          depends_on sorted_tumor;
          depends_on fasta;
          depends_on fasta_dot_fai;
          depends_on sequence_dict;
          depends_on (Samtools.index_to_bai ~run_with sorted_normal);
          depends_on (Samtools.index_to_bai ~run_with sorted_tumor);
          on_failure_activate (Remove.file ~run_with output_file);
        ] in
      workflow_node ~name ~make
        (single_file output_file ~host:Machine.(as_host run_with))
        ~tags:[Target_tags.variant_caller] ~edges
    in
    run_mutect
  in
  match how with
  | `Region region -> run_on_region ~add_edges:more_edges region
  | `Map_reduce ->
    let targets =
      List.map (Reference_genome.major_contigs reference)
        (* We pass the more_edges only to the last step *)
        ~f:(run_on_region ~add_edges:[]) in
    let final_vcf = result_prefix ^ "-merged.vcf" in
    Vcftools.vcf_concat ~run_with targets ~final_vcf ~more_edges
