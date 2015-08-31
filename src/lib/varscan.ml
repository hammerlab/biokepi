open Common
open Run_environment
open Workflow_utilities

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
  let open KEDSL in
  let name = Filename.basename result_prefix in
  let result_file suffix = result_prefix ^ suffix in
  let varscan_tool = Machine.get_tool run_with Tool.Default.varscan in
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
    workflow_node ~name ~make
      (single_file snp_output ~host)
      ~tags
      ~edges:[
        depends_on (Tool.ensure varscan_tool);
        depends_on normal_pileup;
        depends_on tumor_pileup;
        on_failure_activate (Remove.file ~run_with snp_output);
        on_failure_activate (Remove.file ~run_with indel_output);
      ]
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
    workflow_node ~name
      (single_file snp_filtered ~host) ~make ~tags
      ~edges:[
        depends_on varscan_somatic;
        on_failure_activate (Remove.file ~run_with snp_filtered);
        on_failure_activate (Remove.file ~run_with indel_filtered);
      ]
  in
  varscan_filter

let somatic_map_reduce
    ?(more_edges = []) ~reference_build ~run_with ?adjust_mapq
    ~normal ~tumor ~result_prefix () =
  let run_on_region region =
    let result_prefix = result_prefix ^ "-" ^ Region.to_filename region in
    somatic_on_region ~run_with ~reference_build
      ?adjust_mapq ~normal ~tumor ~result_prefix region in
  let targets = List.map (Region.major_contigs ~reference_build) ~f:run_on_region in
  let final_vcf = result_prefix ^ "-merged.vcf" in
  Vcftools.vcf_concat ~run_with targets ~final_vcf ~more_edges
