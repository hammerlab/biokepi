open Common
open Run_environment
open Workflow_utilities

  (*
     For now we have the two steps in the same target but this could
     be split in two.
     c.f. https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_indels_IndelRealigner.php
  *)
let indel_realigner
    ?(compress=false)
    ~reference_build
    ~processors
    ~run_with input_bam ~output_bam =
  let open KEDSL in
  let name = sprintf "gatk-%s" (Filename.basename output_bam) in
  let gatk = Machine.get_tool run_with Tool.Default.gatk in
  let reference_genome = Machine.get_reference_genome run_with reference_build in
  let fasta = Reference_genome.fasta reference_genome in
  let intervals_file =
    Filename.chop_suffix input_bam#product#path ".bam" ^ "-target.intervals"
  in
  let sorted_bam =
    Samtools.sort_bam_if_necessary
      ~run_with ~processors ~by:`Coordinate input_bam in
  let make =
    Machine.run_program run_with ~name
      Program.(
        Tool.(init gatk)
        && shf "java -jar $GATK_JAR -T RealignerTargetCreator -R %s -I %s -o %s -nt %d"
          fasta#product#path
          sorted_bam#product#path
          intervals_file
          processors
        && shf "java -jar $GATK_JAR -T IndelRealigner %s -R %s -I %s -o %s \
                -targetIntervals %s"
          (if compress then "" else "-compress 0")
          fasta#product#path
          sorted_bam#product#path
          output_bam
          intervals_file
      ) in
  let sequence_dict = Picard.create_dict ~run_with fasta in
  workflow_node ~name
    (bam_file ~sorting:`Coordinate
       output_bam ~host:Machine.(as_host run_with))
    ~make
    ~edges:[
      depends_on Tool.(ensure gatk);
      depends_on fasta;
      depends_on sorted_bam;
      depends_on (Samtools.index_to_bai ~run_with sorted_bam);
      (* RealignerTargetCreator wants the `.fai`: *)
      depends_on (Samtools.faidx ~run_with fasta);
      depends_on input_bam;
      depends_on sequence_dict;
      on_failure_activate (Remove.file ~run_with output_bam);
      on_failure_activate (Remove.file ~run_with intervals_file);
    ]


(* Again doing two steps in one target for now:
   http://gatkforums.broadinstitute.org/discussion/44/base-quality-score-recalibrator
   https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_bqsr_BaseRecalibrator.php
*)
let call_gatk ~analysis ?(region=`Full) args =
  let open KEDSL.Program in
  let escaped_args = List.map ~f:Filename.quote args in
  let intervals_option = Region.to_gatk_option region in
  sh (String.concat ~sep:" "
        ("java -jar $GATK_JAR -T " :: analysis :: intervals_option :: escaped_args))

let base_quality_score_recalibrator 
    ~run_with 
    ~processors
    ~reference_build 
    ~input_bam ~output_bam =
  let open KEDSL in
  let name = sprintf "gatk-%s" (Filename.basename output_bam) in
  let gatk = Machine.get_tool run_with Tool.Default.gatk in
  let reference_genome = Machine.get_reference_genome run_with reference_build in
  let fasta = Reference_genome.fasta reference_genome in
  let db_snp = Reference_genome.dbsnp_exn reference_genome in
  let recal_data_table =
    Filename.chop_suffix input_bam#product#path ".bam" ^ "-recal_data.table"
  in
  let sorted_bam =
    Samtools.sort_bam_if_necessary
      ~run_with ~processors ~by:`Coordinate input_bam in
  let make =
    Machine.run_program run_with ~name
      Program.(
        Tool.(init gatk)
        && call_gatk ~analysis:"BaseRecalibrator" [
          "-nct"; Int.to_string processors;
          "-I"; sorted_bam#product#path;
          "-R"; fasta#product#path;
          "-knownSites"; db_snp#product#path;
          "-o"; recal_data_table;
        ]
        && call_gatk ~analysis:"PrintReads" [
          "-nct"; Int.to_string processors;
          "-R"; fasta#product#path;
          "-I"; sorted_bam#product#path;
          "-BQSR"; recal_data_table;
          "-o"; output_bam;
        ]
      ) in
  workflow_node ~name
    (bam_file output_bam
       ~sorting:`Coordinate ~host:Machine.(as_host run_with))
    ~make
    ~edges:[
      depends_on Tool.(ensure gatk); depends_on fasta; depends_on db_snp;
      depends_on sorted_bam;
      depends_on (Samtools.index_to_bai ~run_with sorted_bam);
      on_failure_activate (Remove.file ~run_with output_bam);
      on_failure_activate (Remove.file ~run_with recal_data_table);
    ]


let haplotype_caller
    ?(more_edges = [])
    ~run_with ~reference_build ~input_bam ~result_prefix how =
  let open KEDSL in
  let run_on_region ~add_edges region =
    let result_file suffix =
      let region_name = Region.to_filename region in
      sprintf "%s-%s%s" result_prefix region_name suffix in
    let output_vcf = result_file "-germline.vcf" in
    let gatk = Machine.get_tool run_with Tool.Default.gatk in
    let run_path = Filename.dirname output_vcf in
    let reference = Machine.get_reference_genome run_with reference_build in
    let reference_fasta = Reference_genome.fasta reference in
    let reference_dot_fai = Samtools.faidx ~run_with reference_fasta in
    let sequence_dict = Picard.create_dict ~run_with reference_fasta in
    let dbsnp = Reference_genome.dbsnp_exn reference in
    let sorted_bam =
      Samtools.sort_bam_if_necessary ~run_with ~by:`Coordinate input_bam in
    let run_gatk_haplotype_caller =
      let name = sprintf "%s" (Filename.basename output_vcf) in
      let make =
        Machine.run_program run_with ~name
          Program.(
            Tool.(init gatk)
            && shf "mkdir -p %s" run_path
            && shf "cd %s" run_path
            && call_gatk ~region ~analysis:"HaplotypeCaller" [
              "-I"; sorted_bam#product#path;
              "-R"; reference_fasta#product#path;
              "-o"; output_vcf;
              "--filter_reads_with_N_cigar";
            ]
          )
      in
      workflow_node ~name ~make
        (single_file output_vcf ~host:Machine.(as_host run_with))
        ~tags:[Target_tags.variant_caller]
        ~edges:(add_edges @ [
            depends_on Tool.(ensure gatk);
            depends_on sorted_bam;
            depends_on reference_fasta;
            depends_on dbsnp;
            depends_on reference_dot_fai;
            depends_on sequence_dict;
            depends_on (Samtools.index_to_bai ~run_with sorted_bam);
            on_failure_activate (Remove.file ~run_with output_vcf);
          ])
    in
    run_gatk_haplotype_caller
  in
  match how with
  | `Region region -> run_on_region ~add_edges:more_edges region
  | `Map_reduce ->
    let targets =
      List.map (Region.major_contigs ~reference_build)
        ~f:(run_on_region ~add_edges:[]) (* we add edges only to the last step *)
    in
    let final_vcf = result_prefix ^ "-merged.vcf" in
    Vcftools.vcf_concat ~run_with targets ~final_vcf ~more_edges
