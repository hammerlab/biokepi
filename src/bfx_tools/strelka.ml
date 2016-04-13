open Biokepi_run_environment
open Common


  (*
They happen to have a
[website](https://sites.google.com/site/strelkasomaticvariantcaller/).

The usage is:

- create a config file,
- generate a `Makefile` with `configureStrelkaWorkflow.pl`,
- run `make -j<n>`.
 *)


module Configuration = struct

  type t = {
    name: string;
    parameters: (string * string) list
  }
  let to_json {name; parameters}: Yojson.Basic.json =
    `Assoc [
      "name", `String name;
      "parameters",
      `Assoc (List.map parameters ~f:(fun (a, b) -> a, `String b));
    ]

  let generate_config_file ~path config : KEDSL.Program.t =
    let open KEDSL in
    Program.(
      shf "echo '[user]' > %s" path
      && chain
        (List.map config.parameters (fun (k, v) ->
             shf "echo '%s = %s' >> %s" k v path))
    )

  let default =
    { name = "default";
      parameters = [
        "isSkipDepthFilters", "0";
        "maxInputDepth", "10000";
        "depthFilterMultiple", "3.0";
        "snvMaxFilteredBasecallFrac", "0.4";
        "snvMaxSpanningDeletionFrac", "0.75";
        "indelMaxRefRepeat", "8";
        "indelMaxWindowFilteredBasecallFrac", "0.3";
        "indelMaxIntHpolLength", "14";
        "ssnvPrior", "0.000001";
        "sindelPrior", "0.000001";
        "ssnvNoise", "0.0000005";
        "sindelNoise", "0.0000001";
        "ssnvNoiseStrandBiasFrac", "0.5";
        "minTier1Mapq", "40";
        "minTier2Mapq", "5";
        "ssnvQuality_LowerBound", "15";
        "sindelQuality_LowerBound", "30";
        "isWriteRealignedBam", "0";
        "binSize", "25000000";
        "extraStrelkaArguments", "--eland-compatibility";
      ]}
  let test1 =
    { name = "test1";
      parameters = [
        "isSkipDepthFilters", "0";
        "maxInputDepth", "10000";
        "depthFilterMultiple", "3.0";
        "snvMaxFilteredBasecallFrac", "0.4";
        "snvMaxSpanningDeletionFrac", "0.75";
        "indelMaxRefRepeat", "8";
        "indelMaxWindowFilteredBasecallFrac", "0.3";
        "indelMaxIntHpolLength", "14";
        "ssnvPrior", "0.000001";
        "sindelPrior", "0.000001";
        "ssnvNoise", "0.0000005";
        "sindelNoise", "0.0000001";
        "ssnvNoiseStrandBiasFrac", "0.5";
        "minTier1Mapq", "40";
        "minTier2Mapq", "5";
        "ssnvQuality_LowerBound", "15";
        "sindelQuality_LowerBound", "30";
        "isWriteRealignedBam", "0";
        "binSize", "25000000";
        "extraStrelkaArguments", "--eland-compatibility";
        "priorSomaticSnvRate", "1e-06";
        "germlineSnvTheta", "0.001";
      ]}
  let empty_exome =
    { name = "empty-exome";
      parameters = [
        "isSkipDepthFilters", "1";
      ]}
  let exome_default =
    { name = "exome-default";
      parameters = [
        "isSkipDepthFilters", "1";
        "maxInputDepth", "10000";
        "depthFilterMultiple", "3.0";
        "snvMaxFilteredBasecallFrac", "0.4";
        "snvMaxSpanningDeletionFrac", "0.75";
        "indelMaxRefRepeat", "8";
        "indelMaxWindowFilteredBasecallFrac", "0.3";
        "indelMaxIntHpolLength", "14";
        "ssnvPrior", "0.000001";
        "sindelPrior", "0.000001";
        "ssnvNoise", "0.0000005";
        "sindelNoise", "0.0000001";
        "ssnvNoiseStrandBiasFrac", "0.5";
        "minTier1Mapq", "40";
        "minTier2Mapq", "5";
        "ssnvQuality_LowerBound", "15";
        "sindelQuality_LowerBound", "30";
        "isWriteRealignedBam", "0";
        "binSize", "25000000";
        "extraStrelkaArguments", "--eland-compatibility";
      ]}

end


let run
    ~run_with ~normal ~tumor ~result_prefix ~processors ?(more_edges = [])
    ?(configuration = Configuration.default) () =
  let open KEDSL in
  let open Configuration in
  let name = Filename.basename result_prefix in
  let result_file suffix = result_prefix ^ suffix in
  let output_dir = result_file "strelka_output" in
  let config_file_path = result_file  "configuration" in
  let output_file_path = output_dir // "results/passed_somatic_combined.vcf" in
  let reference_fasta =
    Machine.get_reference_genome run_with normal#product#reference_build
    |> Reference_genome.fasta in
  let strelka_tool = Machine.get_tool run_with Machine.Tool.Default.strelka in
  let gatk_tool = Machine.get_tool run_with Machine.Tool.Default.gatk in
  let sorted_normal =
    Samtools.sort_bam_if_necessary
      ~run_with ~processors ~by:`Coordinate normal in
  let sorted_tumor =
    Samtools.sort_bam_if_necessary
      ~run_with ~processors ~by:`Coordinate tumor in
  let working_dir = Filename.(dirname result_prefix) in
  let make =
    Machine.run_big_program run_with ~name ~processors
      ~self_ids:["strelka"]
      Program.(
        Machine.Tool.init strelka_tool && Machine.Tool.init gatk_tool
        && shf "mkdir -p %s"  working_dir
        && shf "cd %s" working_dir
        && generate_config_file ~path:config_file_path configuration
        && shf "rm -fr %s" output_dir (* strelka won't start if this
                                         directory exists *)
        && shf "$STRELKA_BIN/configureStrelkaWorkflow.pl            \
                --normal=%s                 \
                --tumor=%s                  \
                --ref=%s                   \
                --config=%s                 \
                --output-dir=%s "
          sorted_normal#product#path
          sorted_tumor#product#path
          reference_fasta#product#path
          config_file_path
          output_dir
        && shf "cd %s" output_dir
        && shf "make -j%d" processors
        && Gatk.call_gatk ~analysis:"CombineVariants" [
          "--variant:snvs"; "results/passed.somatic.snvs.vcf";
          "--variant:indels"; "results/passed.somatic.indels.vcf";
          "-R"; reference_fasta#product#path;
          "-genotypeMergeOptions"; "PRIORITIZE";
          "-o"; output_file_path; "-priority"; "snvs,indels"
        ]
      )
  in
  workflow_node ~name ~make
    (single_file output_file_path ~host:(Machine.as_host run_with))
    ~edges:(more_edges @ [
      depends_on normal;
      depends_on tumor;
      depends_on reference_fasta;
      depends_on (Machine.Tool.ensure strelka_tool);
      depends_on (Machine.Tool.ensure gatk_tool);
      depends_on sorted_normal;
      depends_on sorted_tumor;
      depends_on (Picard.create_dict ~run_with reference_fasta);
      depends_on (Samtools.faidx ~run_with reference_fasta);
      depends_on (Samtools.index_to_bai ~run_with sorted_normal);
      depends_on (Samtools.index_to_bai ~run_with sorted_tumor);
    ])
