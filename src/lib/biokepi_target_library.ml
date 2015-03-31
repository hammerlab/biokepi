(**************************************************************************)
(*  Copyright 2014, Sebastien Mondet <seb@mondet.org>                     *)
(*                                                                        *)
(*  Licensed under the Apache License, Version 2.0 (the "License");       *)
(*  you may not use this file except in compliance with the License.      *)
(*  You may obtain a copy of the License at                               *)
(*                                                                        *)
(*      http://www.apache.org/licenses/LICENSE-2.0                        *)
(*                                                                        *)
(*  Unless required by applicable law or agreed to in writing, software   *)
(*  distributed under the License is distributed on an "AS IS" BASIS,     *)
(*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or       *)
(*  implied.  See the License for the specific language governing         *)
(*  permissions and limitations under the License.                        *)
(**************************************************************************)


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

module Bwa = struct

  let default_gap_open_penalty = 11
  let default_gap_extension_penalty = 4
  
  let align_to_sam
      ?(gap_open_penalty=default_gap_open_penalty)
      ?(gap_extension_penalty=default_gap_extension_penalty)
      ~(r1: Ketrew.EDSL.user_target)
      ?(r2: Ketrew.EDSL.user_target option)
      ~(result_prefix:string)
      ~(run_with : Machine.t)
      () =
    let open Ketrew.EDSL in
    let reference_fasta =
      Machine.get_reference_genome run_with `B37
      |> Biokepi_reference_genome.fasta in
    let in_work_dir =
      Program.shf "cd %s" Filename.(quote (dirname result_prefix)) in
    (* `bwa index` creates a bunch of files, c.f.
       [this question](https://www.biostars.org/p/73585/) we detect the
       `.bwt` one. *)
    let bwa_tool = Machine.get_tool run_with "bwa" in
    let bwa_index =
      let name =
        sprintf "bwa-index-%s" (Filename.basename reference_fasta#product#path) in
      let result = sprintf "%s.bwt" reference_fasta#product#path in
      file_target ~host:(Machine.(as_host run_with)) result
        ~if_fails_activate:[Remove.file ~run_with result]
        ~dependencies:[reference_fasta; Tool.(ensure bwa_tool)]
        ~tags:[Target_tags.aligner]
        ~make:(Machine.run_program run_with ~processors:1 ~name
                 Program.(
                   Tool.(init bwa_tool)
                   && shf "bwa index %s"
                     (Filename.quote reference_fasta#product#path)))
    in
    let bwa_aln read_number read =
      let name = sprintf "bwa-aln-%s" (Filename.basename read#product#path) in
      let result = sprintf "%s-R%d.sai" result_prefix read_number in
      let processors = 4 in
      let bwa_command =
        String.concat ~sep:" " [
          "bwa aln";
          "-t"; Int.to_string processors;
          "-O"; Int.to_string gap_open_penalty;
          "-E"; Int.to_string gap_extension_penalty;
          (Filename.quote reference_fasta#product#path);
          (Filename.quote read#product#path);
          ">"; (Filename.quote result);
        ] in
      file_target result ~host:Machine.(as_host run_with) ~name
        ~dependencies:[read; bwa_index; Tool.(ensure bwa_tool)]
        ~if_fails_activate:[Remove.file ~run_with result]
        ~tags:[Target_tags.aligner]
        ~make:(Machine.run_program run_with ~processors ~name
                 Program.(
                   Tool.(init bwa_tool)
                   && in_work_dir
                   && sh bwa_command
                 ))
    in
    let r1_sai = bwa_aln 1 r1 in
    let r2_sai_opt = Option.map r2 ~f:(fun r -> (bwa_aln 2 r, r))in
    let sam =
      let name = sprintf "bwa-sam-%s" (Filename.basename result_prefix) in
      let result = sprintf "%s.sam" result_prefix in
      let program, dependencies =
        let common_deps = [r1_sai; reference_fasta; bwa_index; Tool.(ensure bwa_tool) ] in
        let read_group_header_option =
          (* this option should magically make the sam file compatible
             mutect and other GATK-like pieces of software
             http://seqanswers.com/forums/showthread.php?t=17233

             The `LB` one seems “necessary” for somatic sniper:
             `[bam_header_parse] missing LB tag in @RG lines.`
          *)
          "-r \"@RG\tID:bwa\tSM:SM\tLB:ga\tPL:Illumina\""
        in
        match r2_sai_opt with
        | Some (r2_sai, r2) ->
          Program.(
            Tool.(init bwa_tool)
            && in_work_dir
            && shf "bwa sampe %s %s %s %s %s %s > %s"
              read_group_header_option
              (Filename.quote reference_fasta#product#path)
              (Filename.quote r1_sai#product#path)
              (Filename.quote r2_sai#product#path)
              (Filename.quote r1#product#path)
              (Filename.quote r2#product#path)
              (Filename.quote result)),
          (r2_sai :: common_deps)
        | None ->
          Program.(
            Tool.(init bwa_tool)
            && in_work_dir
            && shf "bwa samse %s %s %s > %s"
              read_group_header_option
              (Filename.quote reference_fasta#product#path)
              (Filename.quote r1_sai#product#path)
              (Filename.quote result)),
          common_deps
      in
      file_target result ~host:Machine.(as_host run_with)
        ~name ~dependencies
        ~if_fails_activate:[Remove.file ~run_with result]
        ~tags:[Target_tags.aligner]
        ~make:(Machine.run_program run_with ~processors:1 ~name  program)
    in
    sam

end

module Gunzip = struct
  (**
     Call ["gunzip <list of fastq.gz files> > some_name_cat.fastq"].
  *)
  let concat ~(run_with : Machine.t) bunch_of_fastq_dot_gzs ~result_path =
    let open Ketrew.EDSL in
    let program =
      Program.(
        exec ["mkdir"; "-p"; Filename.dirname result_path]
        && shf "gunzip -c  %s > %s"
          (List.map bunch_of_fastq_dot_gzs
             ~f:(fun o -> Filename.quote o#product#path)
           |> String.concat ~sep:" ") result_path
      ) in
    let name =
      sprintf "gunzipcat-%s" (Filename.basename result_path) in
    file_target result_path ~host:Machine.(as_host run_with) ~name
      ~dependencies:bunch_of_fastq_dot_gzs
      ~make:(Machine.run_program run_with ~processors:1 ~name  program)
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
    let with_threads = sprintf "-@ %d" processors in
    let make_command src des = ["sort"; with_threads; src; dest_prefix] in
    do_on_bam ~run_with bam_file ~destination ~make_command

  let index_to_bai ~(run_with:Machine.t) bam_file =
    let destination = sprintf "%s.%s" bam_file#product#path "bai" in
    let make_command src des = ["index"; "-b"; src] in
    do_on_bam ~run_with bam_file ~destination ~make_command

  let mpileup ~run_with ?adjust_mapq ~region bam_file =
    let open Ketrew.EDSL in
    let samtools = Machine.get_tool run_with "samtools" in
    let src = bam_file#product#path in
    let adjust_mapq_option = 
      match adjust_mapq with | None -> "" | Some n -> sprintf "-C%d" n in
    let samtools_region_option = Region.to_samtools_option region in
    let reference_genome = Machine.get_reference_genome run_with `B37 in
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
      ~run_with input_bam ~output_bam =
    let open Ketrew.EDSL in
    let name = sprintf "gatk-%s" (Filename.basename output_bam) in
    let gatk = Machine.get_tool run_with "gatk" in
    let reference_genome = Machine.get_reference_genome run_with `B37 in
    let fasta = Reference_genome.fasta reference_genome in
    let intervals_file =
      Filename.chop_suffix input_bam#product#path ".bam" ^ "-target.intervals"
    in
    let sorted_bam = Samtools.sort_bam ~run_with input_bam in
    let make =
      Machine.run_program run_with ~name
        Program.(
          Tool.(init gatk)
          && shf "java -jar $GATK_JAR -T RealignerTargetCreator -R %s -I %s -o %s"
            fasta#product#path
            sorted_bam#product#path
            intervals_file
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
  let call_gatk ~analysis args =
    let open Ketrew.EDSL.Program in
    let escaped_args = List.map ~f:Filename.quote args in
    sh (String.concat ~sep:" "
          ("java -jar $GATK_JAR -T " :: analysis :: escaped_args))

  let base_quality_score_recalibrator ~run_with ~input_bam ~output_bam =
    let open Ketrew.EDSL in
    let name = sprintf "gatk-%s" (Filename.basename output_bam) in
    let gatk = Machine.get_tool run_with "gatk" in
    let reference_genome = Machine.get_reference_genome run_with `B37 in
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
            "-I"; sorted_bam#product#path;
            "-R"; fasta#product#path;
            "-knownSites"; db_snp#product#path;
            "-o"; recal_data_table;
          ]
          && call_gatk ~analysis:"PrintReads" [
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

end

module Mutect = struct
  let run ~(run_with:Machine.t) ~normal ~tumor ~result_prefix how =
    let open Ketrew.EDSL in
    let run_on_region region =
      let result_file suffix =
        let region_name = Region.to_filename region in
        sprintf "%s-%s%s" result_prefix region_name suffix in
      let intervals_option = Region.to_mutect_option region in
      let output_file = result_file "-somatic.vcf" in
      let dot_out_file = result_file "-output.out"in
      let coverage_file = result_file "coverage.wig" in
      let mutect = Machine.get_tool run_with "mutect" in
      let run_path = Filename.dirname output_file in
      let reference = Machine.get_reference_genome run_with `B37 in
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
      let targets = List.map Region.all_chromosomes_b37 ~f:run_on_region in
      let final_vcf = result_prefix ^ "-merged.vcf" in
      Vcftools.vcf_concat ~run_with targets ~final_vcf
end

module Somaticsniper = struct
  let default_prior_probability = 0.01
  let default_theta = 0.85

  let run ~run_with ?minus_T ?minus_s ~normal ~tumor ~result_prefix () =
    let open Ketrew.EDSL in
    let name =
      "somaticsniper"
      ^ Option.(value_map minus_s ~default:"" ~f:(sprintf "-s%F"))
      ^ Option.(value_map minus_T ~default:"" ~f:(sprintf "-T%F"))
    in
    let result_file suffix = sprintf "%s-%s%s" result_prefix name suffix in
    let sniper = Machine.get_tool run_with "somaticsniper" in
    let reference_fasta =
      Machine.get_reference_genome run_with `B37 |> Reference_genome.fasta in
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
      ~run_with ?adjust_mapq ~normal ~tumor ~result_prefix region =
    let open Ketrew.EDSL in
    let name = Filename.basename result_prefix in
    let result_file suffix = result_prefix ^ suffix in
    let varscan_tool = Machine.get_tool run_with "varscan" in
    let snp_output = result_file "-snp.vcf" in
    let indel_output = result_file "-indel.vcf" in
    let normal_pileup = Samtools.mpileup ~run_with ~region ?adjust_mapq normal in
    let tumor_pileup = Samtools.mpileup ~run_with ~region ?adjust_mapq tumor in
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

  let somatic_map_reduce ~run_with ?adjust_mapq ~normal ~tumor ~result_prefix () =
    let run_on_region region =
      let result_prefix = result_prefix ^ "-" ^ Region.to_filename region in
      somatic_on_region ~run_with
        ?adjust_mapq ~normal ~tumor ~result_prefix region in
    let targets = List.map Region.all_chromosomes_b37 ~f:run_on_region in
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


  let run
      ~run_with ~normal ~tumor ~result_prefix ~processors ~configuration () =
    let open Ketrew.EDSL in
    let open Configuration in 
    let name = Filename.basename result_prefix in
    let result_file suffix = result_prefix ^ suffix in
    let output_dir = result_file "strelka_output" in
    let config_file_path = result_file  "configuration" in
    let output_file_path = output_dir // "results/passed_somatic_combined.vcf" in
    let reference_fasta =
      Machine.get_reference_genome run_with `B37 |> Reference_genome.fasta in
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

  let run
      ~run_with ~normal ~tumor ~result_prefix ~processors ~configuration () =
    let open Ketrew.EDSL in
    let open Configuration in 
    let name = Filename.basename result_prefix in
    let result_file suffix = result_prefix ^ suffix in
    let output_file = result_file "-somatic.vcf" in
    let output_prefix = "virmid-output" in
    let work_dir = result_file "-workdir" in
    let reference_fasta =
      Machine.get_reference_genome run_with `B37 |> Reference_genome.fasta in
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

module Cycledash = struct

  let post_to_cycledash_script =
    (* we use rawgit.com and not cdn.rawgit.com because the CDN caches
       old version for ever *)
    "https://gist.githubusercontent.com/smondet/4beec3cbd7c6a3a922bc/raw"

  let post_vcf
      ~run_with
      ~vcf
      ~variant_caller_name
      ~dataset_name
      ?truth_vcf
      ?normal_bam
      ?tumor_bam
      ?params
      ?witness_output
      url =
    let open Ketrew.EDSL in
    let unik_script = sprintf "/tmp/upload_to_cycledash_%s" (Unique_id.create ()) in
    let script_options =
      let with_path opt s = opt, s#product#path in
      List.filter_opt [
        Some ("-V", vcf#product#path);
        Some ("-v", variant_caller_name);
        Some ("-d", dataset_name);
        Option.map truth_vcf ~f:(with_path "-T"); 
        Some ("-U", url);
        Option.map tumor_bam ~f:(with_path "-t");
        Option.map normal_bam ~f:(with_path "-n");
        Option.map params ~f:(fun p -> "-p", p);
        Some ("-w", Option.value witness_output ~default:"/tmp/www")
      ]
      |> List.concat_map ~f:(fun (x, y) -> [x; y]) in
    let name = sprintf "upload+cycledash: %s" vcf#name in
    let make =
      Machine.quick_command run_with Program.(
            shf "curl -f %s > %s"
              (Filename.quote post_to_cycledash_script)
              (Filename.quote unik_script)
            && 
            exec ("sh" :: unik_script :: script_options)
          )
    in
    let dependencies =
      let optional o = Option.value_map o ~f:(fun o -> [o]) ~default:[] in
      [vcf]
      @ optional truth_vcf
      @ optional normal_bam
      @ optional tumor_bam
    in
    match witness_output with
    | None ->
      target name ~make ~dependencies
    | Some path ->
      file_target path ~name ~make ~dependencies
        ~host:Machine.(as_host run_with)

end
