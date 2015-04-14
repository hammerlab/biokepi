open Biokepi_common

open Biokepi_util_targets

open Biokepi_run_environment
module Region = Biokepi_region
module Reference_genome = Biokepi_reference_genome

module Mutect = struct
  let run ?(reference_build=`B37)
    ~(run_with:Machine.t) ~normal ~tumor ~result_prefix how =
    let open Ketrew.EDSL in
    let run_on_region region =
      let result_file suffix =
        let region_name = Region.to_filename region in
        sprintf "%s-%s%s" result_prefix region_name suffix in
      let intervals_option = Region.to_gatk_option region in
      let output_file = result_file "-somatic.vcf" in
      let dot_out_file = result_file "-output.out"in
      let coverage_file = result_file "coverage.wig" in
      let mutect = Machine.get_tool run_with "mutect" in
      let run_path = Filename.dirname output_file in
      let reference = Machine.get_reference_genome run_with reference_build in
      let fasta = Reference_genome.fasta reference in
      let cosmic = Reference_genome.cosmic_exn reference in
      let dbsnp = Reference_genome.dbsnp_exn reference in
      let fasta_dot_fai = Samtools.faidx ~run_with fasta in
      let sequence_dict = Picard.create_dict ~run_with fasta in
      let sorted_normal = Samtools.sort_bam ~processors:2 ~run_with normal in
      let sorted_tumor = Samtools.sort_bam ~processors:2 ~run_with tumor in
      let run_mutect =
        let name = sprintf "%s" (Filename.basename output_file) in
        let make =
          Machine.run_program run_with ~name ~processors:2 Program.(
              Tool.(init mutect)
              && shf "mkdir -p %s" run_path
              && shf "cd %s" run_path
              && shf "java -Xmx2g -jar $mutect_HOME/muTect-*.jar \
                      --analysis_type MuTect \
                      --reference_sequence %s \
                      --cosmic %s \
                      --dbsnp %s \
                      %s \
                      --input_file:normal %s \
                      --input_file:tumor %s \
                      --out %s \
                      --vcf %s \
                      --coverage_file %s "
                fasta#product#path
                cosmic#product#path
                dbsnp#product#path
                intervals_option
                sorted_normal#product#path
                sorted_tumor#product#path
                dot_out_file
                output_file
                coverage_file
            )
        in
        file_target ~name ~make output_file ~host:Machine.(as_host run_with)
          ~tags:[Target_tags.variant_caller]
          ~dependencies:[
            Tool.(ensure mutect); sorted_normal; sorted_tumor; fasta; cosmic;
            dbsnp; fasta_dot_fai; sequence_dict;
            Samtools.index_to_bai ~run_with sorted_normal;
            Samtools.index_to_bai ~run_with sorted_tumor;
          ]
          ~if_fails_activate:[Remove.file ~run_with output_file]
      in
      run_mutect
    in
    match how with
    | `Region region -> run_on_region region
    | `Map_reduce ->
      let targets = List.map (Region.major_contigs ~reference_build) ~f:run_on_region in
      let final_vcf = result_prefix ^ "-merged.vcf" in
      Vcftools.vcf_concat ~run_with targets ~final_vcf
end

module Somaticsniper = struct
  let default_prior_probability = 0.01
  let default_theta = 0.85

  let run ?(reference_build=`B37)
    ~run_with ?minus_T ?minus_s ~normal ~tumor ~result_prefix () =
    let open Ketrew.EDSL in
    let name =
      "somaticsniper"
      ^ Option.(value_map minus_s ~default:"" ~f:(sprintf "-s%F"))
      ^ Option.(value_map minus_T ~default:"" ~f:(sprintf "-T%F"))
    in
    let result_file suffix = sprintf "%s-%s%s" result_prefix name suffix in
    let sniper = Machine.get_tool run_with "somaticsniper" in
    let reference_fasta =
      Machine.get_reference_genome run_with reference_build |> Reference_genome.fasta in
    let output_file = result_file "-snvs.vcf" in
    let run_path = Filename.dirname output_file in
    let sorted_normal = Samtools.sort_bam ~run_with normal in
    let sorted_tumor = Samtools.sort_bam ~run_with tumor in
    let make =
      Machine.run_program run_with
        ~name ~processors:1 Program.(
            Tool.init sniper
            && shf "mkdir -p %s" run_path
            && shf "cd %s" run_path
            && exec (
              ["somaticsniper"; "-F"; "vcf"]
              @ Option.(
                  value_map minus_s ~default:[]
                    ~f:(fun f -> ["-s"; Float.to_string f])
                )
              @ Option.(
                  value_map minus_T ~default:[]
                    ~f:(fun f -> ["-T"; Float.to_string f])
                )
              @ ["-f";  reference_fasta#product#path;
                 sorted_normal#product#path;
                 sorted_tumor#product#path;
                 output_file]
            ))
    in
    file_target output_file ~name ~make ~host:Machine.(as_host run_with)
      ~metadata:(`String name)
      ~tags:[Target_tags.variant_caller; "somaticsniper"]
      ~dependencies:[ Tool.ensure sniper; sorted_normal; sorted_tumor;
                      Samtools.index_to_bai ~run_with sorted_normal;
                      Samtools.index_to_bai ~run_with sorted_tumor;
                      reference_fasta;]
      ~if_fails_activate:[ Remove.file ~run_with output_file ]
end

module Varscan = struct
  (**

     Expects the tool ["varscan"] to provide the environment variable 
     ["$VARSCAN_JAR"].
  *)

  let empty_vcf = "##fileformat=VCFv4.1
##source=VarScan2
##INFO=<ID=DP,Number=1,Type=Integer,Description=\"Total depth of quality bases\">
##INFO=<ID=SOMATIC,Number=0,Type=Flag,Description=\"Indicates if record is a somatic mutation\">
##INFO=<ID=SS,Number=1,Type=String,Description=\"Somatic status of variant (0=Reference,1=Germline,2=Somatic,3=LOH, or 5=Unknown)\">
##INFO=<ID=SSC,Number=1,Type=String,Description=\"Somatic score in Phred scale (0-255) derived from somatic p-value\">
##INFO=<ID=GPV,Number=1,Type=Float,Description=\"Fisher's Exact Test P-value of tumor+normal versus no variant for Germline calls\">
##INFO=<ID=SPV,Number=1,Type=Float,Description=\"Fisher's Exact Test P-value of tumor versus normal for Somatic/LOH calls\">
##FILTER=<ID=str10,Description=\"Less than 10% or more than 90% of variant supporting reads on one strand\">
##FILTER=<ID=indelError,Description=\"Likely artifact due to indel reads at this position\">
##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description=\"Genotype Quality\">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description=\"Read Depth\">
##FORMAT=<ID=RD,Number=1,Type=Integer,Description=\"Depth of reference-supporting bases (reads1)\">
##FORMAT=<ID=AD,Number=1,Type=Integer,Description=\"Depth of variant-supporting bases (reads2)\">
##FORMAT=<ID=FREQ,Number=1,Type=String,Description=\"Variant allele frequency\">
##FORMAT=<ID=DP4,Number=1,Type=String,Description=\"Strand read counts: ref/fwd, ref/rev, var/fwd, var/rev\">
#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tNORMAL\tTUMOR
"

  let somatic_on_region
      ~run_with ~reference_build ?adjust_mapq ~normal ~tumor ~result_prefix region =
    let open Ketrew.EDSL in
    let name = Filename.basename result_prefix in
    let result_file suffix = result_prefix ^ suffix in
    let varscan_tool = Machine.get_tool run_with "varscan" in
    let snp_output = result_file "-snp.vcf" in
    let indel_output = result_file "-indel.vcf" in
    let normal_pileup = Samtools.mpileup ~run_with ~reference_build ~region ?adjust_mapq normal in
    let tumor_pileup = Samtools.mpileup ~run_with ~reference_build ~region ?adjust_mapq tumor in
    let host = Machine.as_host run_with in
    let tags = [Target_tags.variant_caller; "varscan"] in
    let varscan_somatic =
      let name = "somatic-" ^ name in
      let make =
        let big_one_liner =
          "if [ -s " ^ normal_pileup#product#path
          ^ " ] && [ -s " ^ tumor_pileup#product#path ^ " ] ; then "
          ^ sprintf "java -jar $VARSCAN_JAR somatic %s %s \
                     --output-snp %s \
                     --output-indel %s \
                     --output-vcf 1 ; "
            normal_pileup#product#path
            tumor_pileup#product#path
            snp_output
            indel_output
          ^ " else  "
          ^ "echo '" ^ empty_vcf ^ "' > " ^ snp_output ^ " ; "
          ^ "echo '" ^ empty_vcf ^ "' > " ^ indel_output ^ " ; "
          ^ " fi "
        in
        Program.(Tool.init varscan_tool && sh big_one_liner)
        |> Machine.run_program run_with ~name ~processors:1
      in
      file_target snp_output ~name ~make ~host ~tags
        ~dependencies:[ Tool.ensure varscan_tool;
                        normal_pileup; tumor_pileup; ]
        ~if_fails_activate:[ Remove.file ~run_with snp_output;
                             Remove.file ~run_with indel_output]
    in
    let snp_filtered = result_file "-snpfiltered.vcf" in
    let indel_filtered = result_file "-indelfiltered.vcf" in
    let varscan_filter =
      let name = "filter-" ^ name in
      let make =
        Program.(
          Tool.init varscan_tool
          && shf "java -jar $VARSCAN_JAR somaticFilter %s \
                  --indel-file %s \
                  --output-file %s"
            snp_output
            indel_output
            snp_filtered
          && shf "java -jar $VARSCAN_JAR processSomatic %s" snp_filtered
          && shf "java -jar $VARSCAN_JAR processSomatic %s" indel_output
        )
        |> Machine.run_program run_with ~name ~processors:1
      in
      file_target snp_filtered ~name ~host ~make ~tags
        ~if_fails_activate:[ Remove.file ~run_with snp_filtered;
                             Remove.file ~run_with indel_filtered ]
        ~dependencies:[varscan_somatic]
    in
    varscan_filter

  let somatic_map_reduce ~reference_build ~run_with ?adjust_mapq ~normal ~tumor ~result_prefix () =
    let run_on_region region =
      let result_prefix = result_prefix ^ "-" ^ Region.to_filename region in
      somatic_on_region ~run_with ~reference_build
        ?adjust_mapq ~normal ~tumor ~result_prefix region in
    let targets = List.map (Region.major_contigs ~reference_build) ~f:run_on_region in
    let final_vcf = result_prefix ^ "-merged.vcf" in
    Vcftools.vcf_concat ~run_with targets ~final_vcf
end

module Strelka = struct

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

    let generate_config_file ~path config : Ketrew.EDSL.Program.t =
      let open Ketrew.EDSL in
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


  let run ~reference_build
      ~run_with ~normal ~tumor ~result_prefix ~processors ~configuration () =
    let open Ketrew.EDSL in
    let open Configuration in 
    let name = Filename.basename result_prefix in
    let result_file suffix = result_prefix ^ suffix in
    let output_dir = result_file "strelka_output" in
    let config_file_path = result_file  "configuration" in
    let output_file_path = output_dir // "results/passed_somatic_combined.vcf" in
    let reference_fasta =
      Machine.get_reference_genome run_with reference_build |> Reference_genome.fasta in
    let strelka_tool = Machine.get_tool run_with "strelka" in
    let gatk_tool = Machine.get_tool run_with "gatk" in
    let sorted_normal = Samtools.sort_bam ~run_with ~processors normal in
    let sorted_tumor = Samtools.sort_bam ~run_with ~processors tumor in
    let working_dir = Filename.(dirname result_prefix) in
    let make =
      Machine.run_program run_with ~name ~processors
        Program.(
          Tool.init strelka_tool && Tool.init gatk_tool
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
            "--variant"; "results/passed.somatic.snvs.vcf";
            "--variant"; "results/passed.somatic.indels.vcf";
            "-R"; reference_fasta#product#path;
            "-genotypeMergeOptions"; "UNIQUIFY";
            "-o"; output_file_path;
          ]
        )
    in
    file_target output_file_path ~name ~make ~host:(Machine.as_host run_with)
      ~dependencies:[ normal; tumor; reference_fasta;
                      Tool.ensure strelka_tool;
                      Tool.ensure gatk_tool;
                      sorted_normal; sorted_tumor;
                      Picard.create_dict ~run_with reference_fasta;
                      Samtools.faidx ~run_with reference_fasta;
                      Samtools.index_to_bai ~run_with sorted_normal;
                      Samtools.index_to_bai ~run_with sorted_tumor;
                    ]

end


module Virmid = struct
  (* See http://sourceforge.net/p/virmid/wiki/Home/ *)
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
    let render {parameters; _} =
      List.concat_map parameters ~f:(fun (a,b) -> [a; b])
      
    let default =
      {name = "default"; parameters = []}

    let example_1 =
      (* The first one: http://sourceforge.net/p/virmid/wiki/Home/#examples *)
      {name= "examplel_1";
       parameters = [
         "-c1", "20";
         "-C1", "100";
         "-c2", "20";
         "-C2", "100";
         ]}
    
  end

  let run ~reference_build
      ~run_with ~normal ~tumor ~result_prefix ~processors ~configuration () =
    let open Ketrew.EDSL in
    let open Configuration in 
    let name = Filename.basename result_prefix in
    let result_file suffix = result_prefix ^ suffix in
    let output_file = result_file "-somatic.vcf" in
    let output_prefix = "virmid-output" in
    let work_dir = result_file "-workdir" in
    let reference_fasta =
      Machine.get_reference_genome run_with reference_build |> Reference_genome.fasta in
    let virmid_tool = Machine.get_tool run_with "virmid" in
    let virmid_somatic_broken_vcf =
      (* maybe it's actually not broken, but later tools can be
         annoyed by the a space in the header. *)
      work_dir // Filename.basename tumor#product#path ^ ".virmid.som.passed.vcf" in
    let make =
      Machine.run_program run_with ~name ~processors
        Program.(
          Tool.init virmid_tool
          && shf "mkdir -p %s" work_dir
          && sh (String.concat ~sep:" " ([
              "java -jar $VIRMID_JAR -f";
              "-w"; work_dir;
              "-R"; reference_fasta#product#path;
              "-D"; tumor#product#path;
              "-N"; normal#product#path;
              "-t"; Int.to_string processors;
              "-o"; output_prefix;
            ] @ Configuration.render configuration))
          && shf "sed 's/\\(##INFO.*Number=A,Type=String,\\) /\\1/' %s > %s"
            virmid_somatic_broken_vcf output_file
         (* We construct the `output_file` out of the “broken” one with `sed`. *)
        )
    in
    file_target output_file ~name ~make ~host:(Machine.as_host run_with)
      ~dependencies:[ normal; tumor; reference_fasta; Tool.ensure virmid_tool]


end
