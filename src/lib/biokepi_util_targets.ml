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
    let open Ketrew.EDSL in
    target (sprintf "rm-%s" (Filename.basename path))
      ~done_when:(`Command_returns (
          Ketrew.Target.Command.shell ~host:Machine.(as_host run_with)
            (sprintf "ls %s" path),
          2
        ))
      ~make:(Machine.quick_command run_with Program.(exec ["rm"; "-f"; path]))
      ~tags:[Target_tags.clean_up]
end

module Samtools = struct
  let sam_to_bam ~(run_with : Machine.t) file_t =
    let open Ketrew.EDSL in
    let samtools = Machine.get_tool run_with "samtools" in
    let src = file_t#product#path in
    let dest = sprintf "%s.%s" (Filename.chop_suffix src ".sam") "bam" in
    let program =
      Program.(Tool.(init samtools) && exec ["samtools"; "view"; "-b"; "-o"; dest; src])
    in
    let make = Machine.run_program run_with program in
    let host = Machine.(as_host run_with) in
    let name = sprintf "sam-to-bam-%s" (Filename.chop_suffix src ".sam") in
    file_target ~name dest ~host ~make
      ~dependencies:[file_t; Tool.(ensure samtools)]
      ~if_fails_activate:[Remove.file ~run_with dest]
      ~success_triggers:[Remove.file ~run_with src]

  let faidx ~(run_with:Machine.t) fasta =
    let open Ketrew.EDSL in
    let samtools = Machine.get_tool run_with "samtools" in
    let src = fasta#product#path in
    let dest = sprintf "%s.%s" src "fai" in
    let program = Program.(Tool.(init samtools) && exec ["samtools"; "faidx"; src]) in
    let make = Machine.run_program run_with program in
    let host = Machine.(as_host run_with) in
    file_target dest ~name:(sprintf "samtools-faidx-%s" Filename.(basename src))
      ~host ~make
      ~dependencies:[fasta; Tool.(ensure samtools)]
      ~if_fails_activate:[Remove.file ~run_with dest]

  let do_on_bam
      ~(run_with:Machine.t)
      ?(more_dependencies=[]) bam_file ~destination ~make_command =
    let open Ketrew.EDSL in
    let samtools = Machine.get_tool run_with "samtools" in
    let src = bam_file#product#path in
    let sub_command = make_command src destination in
    let program =
      Program.(Tool.(init samtools) && exec ("samtools" :: sub_command)) in
    let make = Machine.run_program run_with program in
    let host = Machine.(as_host run_with) in
    let name =
      sprintf "samtools-%s" String.(concat ~sep:"-" sub_command) in
    file_target destination ~name ~host ~make
      ~dependencies:(Tool.(ensure samtools) :: bam_file :: more_dependencies)
      ~if_fails_activate:[Remove.file ~run_with destination]

  let sort_bam ~(run_with:Machine.t) ?(processors=1) bam_file =
    let source = bam_file#product#path in
    let dest_prefix =
      sprintf "%s-%s" (Filename.chop_suffix source ".bam") "sorted" in
    let destination = sprintf "%s.%s" dest_prefix "bam" in
    let make_command src des =
      ["sort"; "-@"; Int.to_string processors; src; dest_prefix] in
    do_on_bam ~run_with bam_file ~destination ~make_command

  let index_to_bai ~(run_with:Machine.t) bam_file =
    let destination = sprintf "%s.%s" bam_file#product#path "bai" in
    let make_command src des = ["index"; "-b"; src] in
    do_on_bam ~run_with bam_file ~destination ~make_command

  let mpileup ~run_with ~reference_build ?adjust_mapq ~region bam_file =
    let open Ketrew.EDSL in
    let samtools = Machine.get_tool run_with "samtools" in
    let src = bam_file#product#path in
    let adjust_mapq_option = 
      match adjust_mapq with | None -> "" | Some n -> sprintf "-C%d" n in
    let samtools_region_option = Region.to_samtools_option region in
    let reference_genome = Machine.get_reference_genome run_with reference_build in
    let fasta = Reference_genome.fasta reference_genome in
    let pileup =
      Filename.chop_suffix src ".bam" ^
      sprintf "-%s%s.mpileup" (Region.to_filename region) adjust_mapq_option
    in
    let sorted_bam = sort_bam ~run_with bam_file in
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
    let dependencies = [
      Tool.(ensure samtools); sorted_bam; fasta;
      index_to_bai ~run_with sorted_bam;
    ] in
    file_target pileup ~name ~host ~make ~dependencies (*  *)
      ~if_fails_activate:[Remove.file ~run_with pileup]
end

module Picard = struct
  let create_dict ~(run_with:Machine.t) fasta =
    let open Ketrew.EDSL in
    let picard_create_dict = Machine.get_tool run_with "picard" in
    let src = fasta#product#path in
    let dest = sprintf "%s.%s" (Filename.chop_suffix src ".fasta") "dict" in
    let program =
      Program.(Tool.(init picard_create_dict) &&
               shf "java -jar $PICARD_JAR CreateSequenceDictionary R= %s O= %s"
                 (Filename.quote src) (Filename.quote dest)) in
    let make = Machine.run_program run_with program in
    let host = Machine.(as_host run_with) in
    file_target dest
      ~name:(sprintf "picard-create-dict-%s" Filename.(basename src))
      ~host ~make
      ~dependencies:[fasta; Tool.(ensure picard_create_dict)]
      ~if_fails_activate:[Remove.file ~run_with dest]

  let mark_duplicates ~(run_with:Machine.t) ~input_bam output_bam =
    let open Ketrew.EDSL in
    let picard_jar = Machine.get_tool run_with "picard" in
    let metrics_path =
      sprintf "%s.%s" (Filename.chop_suffix output_bam ".bam") ".metrics" in
    let program =
      Program.(Tool.(init picard_jar) &&
               shf "java -jar $PICARD_JAR MarkDuplicates \
                    VALIDATION_STRINGENCY=LENIENT \
                    INPUT=%s OUTPUT=%s METRICS_FILE=%s"
                 (Filename.quote input_bam#product#path)
                 (Filename.quote output_bam)
                 metrics_path) in
    let make = Machine.run_program run_with program in
    let host = Machine.(as_host run_with) in
    file_target output_bam
      ~name:(sprintf "picard-markdups-%s"
               Filename.(basename input_bam#product#path))
      ~host ~make
      ~dependencies:[input_bam; Tool.(ensure picard_jar)]
      ~if_fails_activate:[Remove.file ~run_with output_bam;
                          Remove.file ~run_with metrics_path;]
    
end

module Vcftools = struct
  let vcf_concat ~(run_with:Machine.t) vcfs ~final_vcf =
    let open Ketrew.EDSL in
    let name = sprintf "merge-vcfs-%s" (Filename.basename final_vcf) in
    let vcftools = Machine.get_tool run_with "vcftools" in
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
      file_target ~name final_vcf  ~host:Machine.(as_host run_with)
        ~make  ~dependencies:(Tool.(ensure vcftools) :: vcfs)
        ~if_fails_activate:[Remove.file ~run_with final_vcf]
    in
    vcf_concat
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
    let open Ketrew.EDSL in
    let name = sprintf "gatk-%s" (Filename.basename output_bam) in
    let gatk = Machine.get_tool run_with "gatk" in
    let reference_genome = Machine.get_reference_genome run_with reference_build in
    let fasta = Reference_genome.fasta reference_genome in
    let intervals_file =
      Filename.chop_suffix input_bam#product#path ".bam" ^ "-target.intervals"
    in
    let sorted_bam = Samtools.sort_bam ~run_with input_bam in
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
    file_target ~name output_bam
      ~host:Machine.(as_host run_with)
      ~make  ~dependencies:[Tool.(ensure gatk); fasta;
                            sorted_bam;
                            Samtools.index_to_bai ~run_with sorted_bam;
                            (* RealignerTargetCreator wants the `.fai`: *)
                            Samtools.faidx ~run_with fasta;
                            input_bam; sequence_dict]
      ~if_fails_activate:[
        Remove.file ~run_with output_bam;
        Remove.file ~run_with intervals_file;
      ]


  (* Again doing two steps in one target for now:
     http://gatkforums.broadinstitute.org/discussion/44/base-quality-score-recalibrator
     https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_bqsr_BaseRecalibrator.php
  *)
  let call_gatk ~analysis ?(region=`Full) args =
    let open Ketrew.EDSL.Program in
    let escaped_args = List.map ~f:Filename.quote args in
    let intervals_option = Region.to_gatk_option region in
    sh (String.concat ~sep:" "
          ("java -jar $GATK_JAR -T " :: analysis :: intervals_option :: escaped_args))

  let base_quality_score_recalibrator 
        ~run_with 
        ~processors
        ~reference_build 
        ~input_bam ~output_bam =
    let open Ketrew.EDSL in
    let name = sprintf "gatk-%s" (Filename.basename output_bam) in
    let gatk = Machine.get_tool run_with "gatk" in
    let reference_genome = Machine.get_reference_genome run_with reference_build in
    let fasta = Reference_genome.fasta reference_genome in
    let db_snp = Reference_genome.dbsnp_exn reference_genome in
    let recal_data_table =
      Filename.chop_suffix input_bam#product#path ".bam" ^ "-recal_data.table"
    in
    let sorted_bam = Samtools.sort_bam ~run_with input_bam in
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
    file_target ~name output_bam
      ~host:Machine.(as_host run_with)
      ~make  ~dependencies:[Tool.(ensure gatk); fasta; db_snp;
                            sorted_bam;
                            Samtools.index_to_bai ~run_with sorted_bam;]
      ~if_fails_activate:[
        Remove.file ~run_with output_bam;
        Remove.file ~run_with recal_data_table;
      ]


  let haplotype_caller ~run_with ~reference_build ~input_bam ~result_prefix how  =
    let open Ketrew.EDSL in
    let run_on_region region =
      let result_file suffix =
        let region_name = Region.to_filename region in
        sprintf "%s-%s%s" result_prefix region_name suffix in
      let output_vcf = result_file "-germline.vcf" in
      let gatk = Machine.get_tool run_with "gatk" in
      let run_path = Filename.dirname output_vcf in
      let reference = Machine.get_reference_genome run_with reference_build in
      let reference_fasta = Reference_genome.fasta reference in
      let reference_dot_fai = Samtools.faidx ~run_with reference_fasta in
      let sequence_dict = Picard.create_dict ~run_with reference_fasta in
      let dbsnp = Reference_genome.dbsnp_exn reference in
      let sorted_bam = Samtools.sort_bam ~run_with input_bam in
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
        file_target ~name ~make output_vcf ~host:Machine.(as_host run_with)
          ~tags:[Target_tags.variant_caller]
          ~dependencies:[
            Tool.(ensure gatk); sorted_bam; reference_fasta;
            dbsnp; reference_dot_fai; sequence_dict;
            Samtools.index_to_bai ~run_with sorted_bam;
          ]
          ~if_fails_activate:[Remove.file ~run_with output_vcf]
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
