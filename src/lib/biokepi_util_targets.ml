open Biokepi_common

open Biokepi_run_environment
module Region = Biokepi_region
module Reference_genome = Biokepi_reference_genome

module Target_tags = struct
  let aligner = "aligner"
  let variant_caller = "variant-caller"
  let clean_up = "clean-up"
end

module Remove = struct
  let file ~run_with path =
    let open KEDSL in
    workflow_node nothing
      ~name:(sprintf "rm-%s" (Filename.basename path))
      ~done_when:(`Command_returns (
          Command.shell ~host:Machine.(as_host run_with)
            (sprintf "ls %s" path),
          2
        ))
      ~make:(Machine.quick_command run_with Program.(exec ["rm"; "-f"; path]))
      ~tags:[Target_tags.clean_up]
end

module Samtools = struct
  let sam_to_bam ~(run_with : Machine.t) file_t =
    let open KEDSL in
    let samtools = Machine.get_tool run_with Tool.Default.samtools in
    let src = file_t#product#path in
    let dest = sprintf "%s.%s" (Filename.chop_suffix src ".sam") "bam" in
    let program =
      Program.(Tool.(init samtools) && exec ["samtools"; "view"; "-b"; "-o"; dest; src])
    in
    let make = Machine.run_program run_with program in
    let host = Machine.(as_host run_with) in
    let name = sprintf "sam-to-bam-%s" (Filename.chop_suffix src ".sam") in
    workflow_node ~name
      (bam_file dest ~host)
      ~make
      ~edges:[
        depends_on file_t;
        depends_on Tool.(ensure samtools);
        on_failure_activate (Remove.file ~run_with dest);
        on_success_activate (Remove.file ~run_with src);
      ]

  let faidx ~(run_with:Machine.t) fasta =
    let open KEDSL in
    let samtools = Machine.get_tool run_with Tool.Default.samtools in
    let src = fasta#product#path in
    let dest = sprintf "%s.%s" src "fai" in
    let program = Program.(Tool.(init samtools) && exec ["samtools"; "faidx"; src]) in
    let make = Machine.run_program run_with program in
    let host = Machine.(as_host run_with) in
    workflow_node
      (single_file dest ~host)
      ~name:(sprintf "samtools-faidx-%s" Filename.(basename src))
      ~make
      ~edges:[
        depends_on fasta;
        depends_on Tool.(ensure samtools);
        on_failure_activate (Remove.file ~run_with dest);
      ]

  let do_on_bam
      ~(run_with:Machine.t)
      ?(more_depends_on=[]) input_bam ~product ~make_command =
    let open KEDSL in
    let samtools = Machine.get_tool run_with Tool.Default.samtools in
    let src = input_bam#product#path in
    let sub_command = make_command src product#path in
    let program =
      Program.(Tool.(init samtools) && exec ("samtools" :: sub_command)) in
    let make = Machine.run_program run_with program in
    let name =
      sprintf "samtools-%s" String.(concat ~sep:"-" sub_command) in
    workflow_node product ~name ~make
      ~edges:(
        depends_on Tool.(ensure samtools)
        :: depends_on input_bam
        :: on_failure_activate (Remove.file ~run_with product#path)
        :: more_depends_on)

  let sort_bam ~(run_with:Machine.t) ?(processors=1) ~by input_bam =
    let source = input_bam#product#path in
    let dest_suffix =
      match by with
        | `Coordinate -> "sorted"
        | `Read_name -> "read-name-sorted"
    in
    let dest_prefix =
      sprintf "%s-%s" (Filename.chop_suffix source ".bam") dest_suffix in
    let product =
      KEDSL.bam_file ~sorting:by
        ~host:Machine.(as_host run_with)
        (sprintf "%s.%s" dest_prefix "bam") in
    let make_command src des =
      let command = ["-@"; Int.to_string processors; src; dest_prefix] in
      match by with
      | `Coordinate -> "sort" :: command
      | `Read_name -> "sort" :: "-n" :: command
    in
    do_on_bam ~run_with input_bam ~product ~make_command

  let sort_bam_if_necessary ~(run_with:Machine.t) ?(processors=1) ~by input_bam =
    match input_bam#product#sorting with
    | Some `Coordinate -> input_bam
    | other ->
      sort_bam ~run_with input_bam ~processors ~by:`Coordinate

  let index_to_bai ~(run_with:Machine.t) input_bam =
    let product =
      KEDSL.single_file  ~host:(Machine.as_host run_with)
        (sprintf "%s.%s" input_bam#product#path "bai") in
    let make_command src des = ["index"; "-b"; src] in
    do_on_bam ~run_with input_bam ~product ~make_command

  let mpileup ~run_with ~reference_build ?adjust_mapq ~region input_bam =
    let open KEDSL in
    let samtools = Machine.get_tool run_with Tool.Default.samtools in
    let src = input_bam#product#path in
    let adjust_mapq_option = 
      match adjust_mapq with | None -> "" | Some n -> sprintf "-C%d" n in
    let samtools_region_option = Region.to_samtools_option region in
    let reference_genome = Machine.get_reference_genome run_with reference_build in
    let fasta = Reference_genome.fasta reference_genome in
    let pileup =
      Filename.chop_suffix src ".bam" ^
      sprintf "-%s%s.mpileup" (Region.to_filename region) adjust_mapq_option
    in
    let sorted_bam = sort_bam ~run_with input_bam ~by:`Coordinate in
    let program =
      Program.(
        Tool.(init samtools)
        && shf
          "samtools mpileup %s %s -Bf %s %s > %s"
          adjust_mapq_option samtools_region_option 
          fasta#product#path
          sorted_bam#product#path
          pileup
      ) in
    let make = Machine.run_program run_with program in
    let host = Machine.(as_host run_with) in
    let name =
      sprintf "samtools-mpileup-%s" Filename.(basename pileup |> chop_extension)
    in
    let edges = [
      depends_on Tool.(ensure samtools);
      depends_on sorted_bam;
      depends_on fasta;
      index_to_bai ~run_with sorted_bam |> depends_on;
      on_failure_activate (Remove.file ~run_with pileup);
    ] in
    workflow_node ~name (single_file pileup ~host) ~make ~edges 

end

module Picard = struct
  let create_dict ~(run_with:Machine.t) fasta =
    let open KEDSL in
    let picard_create_dict = Machine.get_tool run_with Tool.Default.picard in
    let src = fasta#product#path in
    let dest = sprintf "%s.%s" (Filename.chop_suffix src ".fasta") "dict" in
    let program =
      Program.(Tool.(init picard_create_dict) &&
               shf "java -jar $PICARD_JAR CreateSequenceDictionary R= %s O= %s"
                 (Filename.quote src) (Filename.quote dest)) in
    let make = Machine.run_program run_with program in
    let host = Machine.(as_host run_with) in
    workflow_node (single_file dest ~host)
      ~name:(sprintf "picard-create-dict-%s" Filename.(basename src))
      ~make
      ~edges:[
        depends_on fasta; depends_on Tool.(ensure picard_create_dict);
        on_failure_activate (Remove.file ~run_with dest);
      ]

  let mark_duplicates ~(run_with:Machine.t) ~input_bam output_bam =
    let open KEDSL in
    let picard_jar = Machine.get_tool run_with Tool.Default.picard in
    let metrics_path =
      sprintf "%s.%s" (Filename.chop_suffix output_bam ".bam") ".metrics" in
    let sorted_bam =
      Samtools.sort_bam_if_necessary ~run_with input_bam ~by:`Coordinate in
    let program =
      Program.(Tool.(init picard_jar) &&
               shf "java -jar $PICARD_JAR MarkDuplicates \
                    VALIDATION_STRINGENCY=LENIENT \
                    INPUT=%s OUTPUT=%s METRICS_FILE=%s"
                 (Filename.quote sorted_bam#product#path)
                 (Filename.quote output_bam)
                 metrics_path) in
    let make = Machine.run_program run_with program in
    let host = Machine.(as_host run_with) in
    workflow_node (bam_file ~sorting:`Coordinate output_bam ~host)
      ~name:(sprintf "picard-markdups-%s"
               Filename.(basename input_bam#product#path))
      ~make
      ~edges:[
        depends_on sorted_bam;
        depends_on Tool.(ensure picard_jar);
        on_failure_activate (Remove.file ~run_with output_bam);
        on_failure_activate (Remove.file ~run_with metrics_path);
      ]

end

module Vcftools = struct
  let vcf_concat ~(run_with:Machine.t) vcfs ~final_vcf =
    let open KEDSL in
    let name = sprintf "merge-vcfs-%s" (Filename.basename final_vcf) in
    let vcftools = Machine.get_tool run_with Tool.Default.vcftools in
    let vcf_concat =
      let make =
        Machine.run_program run_with ~name
          Program.(
            Tool.(init vcftools)
            && shf "vcf-concat %s > %s"
              (String.concat ~sep:" "
                 (List.map vcfs ~f:(fun t -> t#product#path)))
              final_vcf
          ) in
      workflow_node ~name
        (single_file final_vcf ~host:Machine.(as_host run_with))
        ~make
        ~edges:(
          on_failure_activate (Remove.file ~run_with final_vcf)
          :: depends_on Tool.(ensure vcftools)
          :: List.map ~f:depends_on vcfs)
    in
    vcf_concat
end

module Bedtools = struct

  let bamtofastq ~(run_with:Machine.t) ~sample_type ~processors ~output_prefix input_bam =
    let open KEDSL in
    let sorted_bam =
      Samtools.sort_bam_if_necessary
        ~run_with ~processors ~by:`Read_name input_bam in
    let fastq_output_options, r1, r2opt =
      match sample_type with
      | `Paired_end ->
        let r1 = sprintf "%s_R1.fastq" output_prefix in
        let r2 = sprintf "%s_R2.fastq" output_prefix in
        (["-fq"; r1; "-fq2"; r2], r1, Some r2)
      | `Single_end ->
        let r1 = sprintf "%s.fastq" output_prefix in
        (["-fq"; r1], r1, None)
    in
    let bedtools = Machine.get_tool run_with Tool.Default.bedtools in
    let src_bam = input_bam#product#path in
    let program =
      Program.(Tool.(init bedtools)
               && exec ["mkdir"; "-p"; Filename.dirname r1]
               && exec ("bedtools" ::
                        "bamtofastq" ::  "-i" :: src_bam ::
                        fastq_output_options)) in
    let make = Machine.run_program run_with program in
    let name =
      sprintf "bedtools-bamtofastq-%s"
        Filename.(basename src_bam |> chop_extension) in
    let edges = [
        depends_on Tool.(ensure bedtools);
        depends_on input_bam;
        on_failure_activate (Remove.file ~run_with r1);
        Option.value_map r2opt ~default:Empty_edge
          ~f:(fun r2 ->
              on_failure_activate (Remove.file ~run_with r2));
        on_success_activate (Remove.file ~run_with sorted_bam#product#path);
      ] in
    workflow_node
      (fastq_reads ~host:(Machine.as_host run_with) r1 r2opt)
      ~edges ~name ~make

end

module Gatk = struct

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
    let sorted_bam = Samtools.sort_bam ~run_with ~processors ~by:`Coordinate input_bam in
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


  let haplotype_caller ~run_with ~reference_build ~input_bam ~result_prefix how  =
    let open KEDSL in
    let run_on_region region =
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
      let sorted_bam = Samtools.sort_bam ~run_with ~by:`Coordinate input_bam in
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
              ]
            )
        in
        workflow_node ~name ~make
          (single_file output_vcf ~host:Machine.(as_host run_with))
          ~tags:[Target_tags.variant_caller]
          ~edges:[
            depends_on Tool.(ensure gatk);
            depends_on sorted_bam;
            depends_on reference_fasta;
            depends_on dbsnp;
            depends_on reference_dot_fai;
            depends_on sequence_dict;
            depends_on (Samtools.index_to_bai ~run_with sorted_bam);
            on_failure_activate (Remove.file ~run_with output_vcf);
          ]
      in
      run_gatk_haplotype_caller
    in
    match how with
    | `Region region -> run_on_region region
    | `Map_reduce ->
      let targets = List.map (Region.major_contigs ~reference_build) ~f:run_on_region in
      let final_vcf = result_prefix ^ "-merged.vcf" in
      Vcftools.vcf_concat ~run_with targets ~final_vcf
end
