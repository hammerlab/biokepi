open Common
open Run_environment
open Workflow_utilities

let run ?(reference_build=`B37)
    ~(run_with:Machine.t) ~normal ~tumor ~result_prefix how =
  let open KEDSL in
  let run_on_region region =
    let result_file suffix =
      let region_name = Region.to_filename region in
      sprintf "%s-%s%s" result_prefix region_name suffix in
    let intervals_option = Region.to_gatk_option region in
    let output_file = result_file "-somatic.vcf" in
    let dot_out_file = result_file "-output.out"in
    let coverage_file = result_file "coverage.wig" in
    let mutect = Machine.get_tool run_with Tool.Default.mutect in
    let run_path = Filename.dirname output_file in
    let reference = Machine.get_reference_genome run_with reference_build in
    let fasta = Reference_genome.fasta reference in
    let cosmic = Reference_genome.cosmic_exn reference in
    let dbsnp = Reference_genome.dbsnp_exn reference in
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
      workflow_node ~name ~make
        (single_file output_file ~host:Machine.(as_host run_with))
        ~tags:[Target_tags.variant_caller]
        ~edges:[
          depends_on Tool.(ensure mutect);
          depends_on sorted_normal;
          depends_on sorted_tumor;
          depends_on fasta;
          depends_on cosmic;
          depends_on dbsnp;
          depends_on fasta_dot_fai;
          depends_on sequence_dict;
          depends_on (Samtools.index_to_bai ~run_with sorted_normal);
          depends_on (Samtools.index_to_bai ~run_with sorted_tumor);
          on_failure_activate (Remove.file ~run_with output_file);
        ]
    in
    run_mutect
  in
  match how with
  | `Region region -> run_on_region region
  | `Map_reduce ->
    let targets = List.map (Region.major_contigs ~reference_build) ~f:run_on_region in
    let final_vcf = result_prefix ^ "-merged.vcf" in
    Vcftools.vcf_concat ~run_with targets ~final_vcf
