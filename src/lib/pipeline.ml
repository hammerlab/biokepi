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


open Common
open Run_environment

module File = struct
  type t = KEDSL.file_workflow
end

type json = Yojson.Basic.json

type fastq_gz = Fastq_gz
type fastq = Fastq
type fastq_sample = Fastq_sample
type bam = KEDSL.bam_file KEDSL.workflow_node
type bam_pair = Bam_pair
type vcf = Vcf
type gtf = Gtf

type bwa_params = {
  gap_open_penalty: int;
  gap_extension_penalty: int;
}
module Somatic_variant_caller = struct
  type t = {
    name: string;
    configuration_json: json;
    configuration_name: string;
    make_target:
      reference_build: Reference_genome.name ->
      run_with:Run_environment.Machine.t ->
      normal: bam ->
      tumor: bam ->
      result_prefix: string ->
      processors: int ->
      ?more_edges: KEDSL.workflow_edge list ->
      unit ->
      KEDSL.file_workflow
  }
end

module Germline_variant_caller = struct
  type t = {
    name: string;
    configuration_json: json;
    configuration_name: string;
    make_target:
      reference_build: Reference_genome.name ->
      run_with:Run_environment.Machine.t ->
      input_bam: bam ->
      result_prefix: string ->
      processors: int ->
      ?more_edges: KEDSL.workflow_edge list ->
      unit ->
      KEDSL.file_workflow
  }
end

type _ t =
  | Fastq_gz: File.t -> fastq_gz  t
  | Fastq: File.t -> fastq  t
  (* | List: 'a t list -> 'a list t *)
  | Bam_sample: string * bam -> bam t
  | Bam_to_fastq: [ `Single | `Paired ] * bam t -> fastq_sample t
  | Paired_end_sample: string * fastq t * fastq t -> fastq_sample t
  | Single_end_sample: string * fastq t -> fastq_sample t
  | Gunzip_concat: fastq_gz t list -> fastq t
  | Concat_text: fastq t list -> fastq t
  | Star: fastq_sample t -> bam t
  | Hisat: fastq_sample t -> bam t
  | Stringtie: Stringtie.gtf_usage_preference * bam t -> gtf t
  | Bwa: bwa_params * fastq_sample t -> bam t
  | Bwa_mem: bwa_params * fastq_sample t -> bam t
  | Mosaik: fastq_sample t -> bam t
  | Gatk_indel_realigner: Gatk.Configuration.indel_realigner * bam t -> bam t
  | Picard_mark_duplicates: Picard.Mark_duplicates_settings.t * bam t -> bam t
  | Gatk_bqsr: (Gatk.Configuration.bqsr * bam t) -> bam t
  | Bam_pair: bam t * bam t -> bam_pair t
  | Somatic_variant_caller: Somatic_variant_caller.t * bam_pair t -> vcf t
  | Germline_variant_caller: Germline_variant_caller.t * bam t -> vcf t

module Construct = struct

  type input_fastq = [
    | `Paired_end of File.t list * File.t list
    | `Single_end of File.t list
  ]

  let input_fastq ~dataset (fastqs: input_fastq) =
    let is_fastq_gz p =
      Filename.check_suffix p "fastq.gz" || Filename.check_suffix p "fq.gz"  in
    let bring_to_single_fastq l =
      match l with
      | h :: _ as more when is_fastq_gz h#product#path ->
        Gunzip_concat  (List.map more (fun f -> Fastq_gz f))
      | _ -> failwith "for now, a sample must be a list of fastq.gz"
    in
    match fastqs with
    | `Paired_end (l1, l2) ->
      Paired_end_sample (dataset, bring_to_single_fastq l1, bring_to_single_fastq l2)
    | `Single_end l ->
      Single_end_sample (dataset, bring_to_single_fastq l)

  let bam ~dataset bam = Bam_sample (dataset, bam)

  let bam_to_fastq how bam = Bam_to_fastq (how, bam)

  let bwa
      ?(gap_open_penalty=Bwa.default_gap_open_penalty)
      ?(gap_extension_penalty=Bwa.default_gap_extension_penalty)
      fastq =
    let params = {gap_open_penalty; gap_extension_penalty} in
    Bwa (params, fastq)

  let bwa_mem
      ?(gap_open_penalty=Bwa.default_gap_open_penalty)
      ?(gap_extension_penalty=Bwa.default_gap_extension_penalty)
      fastq =
    let params = {gap_open_penalty; gap_extension_penalty} in
    Bwa_mem (params, fastq)

  let mosaik fastq = Mosaik fastq

  let star fastq = Star fastq

  let hisat fastq = Hisat fastq

  let stringtie ?(use_reference_gtf =`If_available) bam =
    Stringtie (use_reference_gtf, bam)

  let gatk_indel_realigner
        ?(configuration=Gatk.Configuration.default_indel_realigner)
        bam
    = Gatk_indel_realigner (configuration, bam)
  let picard_mark_duplicates
      ?(settings=Picard.Mark_duplicates_settings.default) bam =
    Picard_mark_duplicates (settings, bam)

  let gatk_bqsr ?(configuration=Gatk.Configuration.default_bqsr) bam = Gatk_bqsr (configuration, bam)

  let pair ~normal ~tumor = Bam_pair (normal, tumor)

  let germline_variant_caller t input_bam =
    Germline_variant_caller (t, input_bam)

  let gatk_haplotype_caller input_bam =
    let configuration_name = "default" in
    let configuration_json =
      `Assoc [
        "Name", `String configuration_name;
      ] in
    let make_target
        ~reference_build ~run_with ~input_bam ~result_prefix ~processors
        ?more_edges () =
      Gatk.haplotype_caller ?more_edges ~reference_build ~run_with
        ~input_bam ~result_prefix `Map_reduce in
    germline_variant_caller
      {Germline_variant_caller.name = "Gatk-HaplotypeCaller";
        configuration_json;
        configuration_name;
        make_target;}
      input_bam

  let somatic_variant_caller t bam_pair =
    Somatic_variant_caller (t, bam_pair)

  let mutect ?(configuration=Mutect.Configuration.default) bam_pair =
    let configuration_name = configuration.Mutect.Configuration.name in
    let configuration_json = Mutect.Configuration.to_json configuration in
    let make_target
        ~reference_build ~run_with ~normal ~tumor ~result_prefix ~processors
        ?more_edges () =
      Mutect.run
        ~configuration
        ?more_edges
        ~reference_build ~run_with ~normal ~tumor ~result_prefix `Map_reduce in
    somatic_variant_caller
      {Somatic_variant_caller.name = "Mutect";
       configuration_json;
       configuration_name;
       make_target;}
      bam_pair

  let somaticsniper
      ?(prior_probability=Somaticsniper.default_prior_probability)
      ?(theta=Somaticsniper.default_theta)
      bam_pair =
    let configuration_name =
      sprintf "S%F-T%F" prior_probability theta in
    let configuration_json =
      `Assoc [
        "Name", `String configuration_name;
        "Prior-probability", `Float prior_probability;
        "Theta", `Float theta;
      ] in
    let make_target
        ~reference_build ~run_with ~normal ~tumor ~result_prefix ~processors
        ?more_edges () =
      Somaticsniper.run ~reference_build
        ~run_with ~minus_s:prior_probability ~minus_T:theta
        ~normal ~tumor ~result_prefix () in
    somatic_variant_caller
      {Somatic_variant_caller.name = "Somaticsniper";
       configuration_json;
       configuration_name;
       make_target;}
      bam_pair

  let varscan_somatic ?adjust_mapq bam_pair =
    let configuration_name =
      sprintf "amq-%s"
        (Option.value_map ~default:"NONE" adjust_mapq ~f:Int.to_string) in
    let configuration_json =
      `Assoc [
        "Name", `String configuration_name;
        "Adjust_mapq",
        `String (Option.value_map adjust_mapq ~f:Int.to_string ~default:"None");
      ] in
    somatic_variant_caller 
      {Somatic_variant_caller.name = "Varscan-somatic";
       configuration_json;
       configuration_name;
       make_target = begin
         fun ~reference_build ~run_with ~normal ~tumor ~result_prefix ~processors
           ?more_edges () ->
           Varscan.somatic_map_reduce ~reference_build ?adjust_mapq
             ?more_edges
             ~run_with ~normal ~tumor ~result_prefix ()
       end}
      bam_pair

  let strelka ~configuration bam_pair =
    somatic_variant_caller 
      {Somatic_variant_caller.name = "Strelka";
       configuration_json = Strelka.Configuration.to_json configuration;
       configuration_name = configuration.Strelka.Configuration.name;
       make_target = Strelka.run ~configuration;}
      bam_pair

  let virmid ~configuration bam_pair =
    somatic_variant_caller 
      {Somatic_variant_caller.name = "Virmid";
       configuration_json = Virmid.Configuration.to_json configuration;
       configuration_name = configuration.Virmid.Configuration.name;
       make_target = Virmid.run ~configuration;}
      bam_pair

  let muse ~configuration bam_pair =
    let make_target ~reference_build
        ~(run_with:Machine.t) ~normal ~tumor ~result_prefix ~processors
        ?more_edges () =
      Muse.run ~reference_build ~configuration ?more_edges
        ~run_with ~normal ~tumor ~result_prefix `Map_reduce in
    somatic_variant_caller 
      {Somatic_variant_caller.name = "Muse";
       configuration_json = Muse.Configuration.to_json configuration;
       configuration_name = configuration.Muse.Configuration.name;
       make_target }
      bam_pair

end


let rec to_file_prefix:
  type a.
  ?is:[ `Normal | `Tumor] ->
  ?read:[ `R1 of string | `R2 of string ] ->
  a t -> string =
  fun ?is ?read w ->
    let is_suffix =
      match is with
      | None  -> ""
      | Some `Normal -> "-normal"
      | Some `Tumor -> "-tumor"
    in
    begin match w with
    | Fastq_gz _ -> failwith "TODO"
    | Fastq _ -> failwith "TODO"
    | Single_end_sample (name, _) -> name ^ is_suffix
    | Gunzip_concat [] -> failwith "TODO"
    | Gunzip_concat (_ :: _) ->
      begin match read with
      | None -> is_suffix ^ "-cat"
      | Some (`R1 s) -> sprintf "%s%s-R1-cat" s is_suffix
      | Some (`R2 s) -> sprintf "%s%s-R2-cat" s is_suffix
      end
    | Concat_text _ -> failwith "TODO"
    | Bam_sample (name, _) -> name ^ is_suffix
    | Bam_to_fastq (how, bam) ->
      sprintf "%s-b2fq-%s"
        (to_file_prefix ?is bam)
        (match how with `Paired -> "PE" | `Single -> "SE")
    | Paired_end_sample (name, _ , _) ->
      name ^ is_suffix
    | Bwa ({ gap_open_penalty; gap_extension_penalty }, sample) ->
      sprintf "%s-bwa-gap%d-gep%d"
        (to_file_prefix ?is sample) gap_open_penalty gap_extension_penalty
    | Bwa_mem ({ gap_open_penalty; gap_extension_penalty }, sample) ->
      sprintf "%s-bwa-mem-gap%d-gep%d"
        (to_file_prefix ?is sample) gap_open_penalty gap_extension_penalty
    | Star (sample) ->
      sprintf "%s-star-aligned" (to_file_prefix ?is sample)
    | Hisat (sample) ->
      sprintf "%s-hisat-aligned" (to_file_prefix ?is sample)
    | Stringtie (use_reference_gtf, sample) ->
      sprintf "%s-%sstringtie"
        (to_file_prefix ?is sample)
        (match use_reference_gtf with
        | `Yes -> "yesgtf"
        | `No -> "nogtf"
        | `If_available -> "ifgtf")
    | Mosaik (sample) ->
      sprintf "%s-mosaik" (to_file_prefix ?is sample)
    | Gatk_indel_realigner ((indel_cfg, target_cfg), bam) ->
      let open Gatk.Configuration in
      sprintf "%s-indelrealigned-%s-%s"
        (to_file_prefix ?is ?read bam)
        indel_cfg.Indel_realigner.name
        target_cfg.Realigner_target_creator.name
    | Gatk_bqsr ((bqsr_cfg, print_reads_cfg), bam) ->
      let open Gatk.Configuration in
      sprintf "%s-bqsr-%s-%s"
        (to_file_prefix ?is ?read bam)
        bqsr_cfg.Bqsr.name
        bqsr_cfg.Print_reads.name
    | Picard_mark_duplicates (_, bam) ->
      (* The settings, for now, do not impact the result *)
      sprintf "%s-dedup" (to_file_prefix ?is ?read bam)
    | Bam_pair (nor, tum) -> to_file_prefix ?is:None tum
    | Somatic_variant_caller (vc, bp) ->
      let prev = to_file_prefix bp in
      sprintf "%s-%s-%s" prev
        vc.Somatic_variant_caller.name
        vc.Somatic_variant_caller.configuration_name
    | Germline_variant_caller (vc, bp) ->
      let prev = to_file_prefix bp in
      sprintf "%s-%s-%s" prev
        vc.Germline_variant_caller.name
        vc.Germline_variant_caller.configuration_name
    end



let rec to_json: type a. a t -> json =
  let bwa_params {gap_open_penalty; gap_extension_penalty} input =
    `Assoc ["gap_open_penalty", `Int gap_open_penalty;
            "gap_extension_penalty", `Int gap_extension_penalty;
            "input", input] in
  fun w ->
    let call name (args : json list): json = `List (`String name :: args) in
    match w with
    | Fastq_gz file -> call "Fastq_gz" [`String file#product#path]
    | Fastq file -> call "Fastq" [`String file#product#path]
    | Bam_sample (name, file) ->
      call "Bam-sample" [`String name; `String file#product#path]
    | Bam_to_fastq (how, bam) ->
      let how_string =
        match how with `Paired -> "Paired" | `Single -> "Single" in
      call "Bam-to-fastq" [`String how_string; to_json bam]
    | Paired_end_sample (name, r1, r2) ->
      call "Paired-end" [`String name; to_json r1; to_json r2]
    | Single_end_sample (name, r) ->
      call "Single-end" [`String name; to_json r]
    | Gunzip_concat fastq_gz_list ->
      call "Gunzip-concat" (List.map ~f:to_json fastq_gz_list)
    | Concat_text fastq_list ->
      call "Concat" (List.map ~f:to_json fastq_list)
    | Bwa (params, input) ->
      let input_json = to_json input in
      call "BWA" [bwa_params params input_json]
    | Bwa_mem (params, input) ->
      let input_json = to_json input in
      call "BWA-MEM" [bwa_params params input_json]
    | Star (input) ->
      let input_json = to_json input in
      call "STAR" [input_json]
    | Hisat (input) ->
      let input_json = to_json input in
      call "HISAT" [input_json]
    | Stringtie (use_reference_gtf, input) ->
      let input_json = to_json input in
      call "Stringtie" [
        `Assoc ["use_reference_gtf",
                `String 
                  (match use_reference_gtf with
                  | `Yes -> "YEs"
                  | `No -> "No"
                  | `If_available -> "If_available")];
        input_json;
      ]
    | Mosaik (input) ->
      let input_json = to_json input in
      call "MOSAIK" [input_json]
    | Gatk_indel_realigner ((indel_cfg, target_cfg), bam) ->
      let open Gatk.Configuration in
      let input_json = to_json bam in
      let indel_cfg_json = Indel_realigner.to_json indel_cfg in
      let target_cfg_json = Realigner_target_creator.to_json target_cfg in
      call "Gatk_indel_realigner" [`Assoc [
          "Configuration", `Assoc [
            "IndelRealigner Configuration", indel_cfg_json;
            "RealignerTargetCreator Configuration", target_cfg_json;
          ];
          "Input", input_json;
        ]]
    | Gatk_bqsr ((bqsr_cfg, print_reads_cfg), bam) ->
      let open Gatk.Configuration in
      let input_json = to_json bam in
      call "Gatk_bqsr" [`Assoc [
          "Configuration", `Assoc [
            "Bqsr", Bqsr.to_json bqsr_cfg;
            "Print_reads", Print_reads.to_json print_reads_cfg;
          ];
          "Input", input_json;
        ]]
    | Germline_variant_caller (gvc, bam) ->
      call gvc.Germline_variant_caller.name [`Assoc [
          "Configuration", gvc.Germline_variant_caller.configuration_json;
          "Input", to_json bam;
        ]]
    | Picard_mark_duplicates (settings, bam) ->
      (* The settings should not impact the output, so we don't dump them. *)
      call "Picard_mark_duplicates" [`Assoc ["input", to_json bam]]
    | Bam_pair (normal, tumor) ->
      call "Bam-pair" [`Assoc ["normal", to_json normal; "tumor", to_json tumor]]
    | Somatic_variant_caller (svc, bam_pair) ->
      call svc.Somatic_variant_caller.name [`Assoc [
          "Configuration", svc.Somatic_variant_caller.configuration_json;
          "Input", to_json bam_pair;
        ]]

module Compiler = struct
  type 'a pipeline = 'a t
  type t = {
    processors : int;
    reference_build: Reference_genome.name;
    work_dir: string;
    machine : Machine.t;
    wrap_bam_node:
      bam pipeline ->
      KEDSL.bam_file KEDSL.workflow_node ->
      KEDSL.bam_file KEDSL.workflow_node;
    wrap_vcf_node:
      vcf pipeline ->
      KEDSL.single_file KEDSL.workflow_node ->
      KEDSL.single_file KEDSL.workflow_node;
    wrap_gtf_node:
      gtf pipeline ->
      KEDSL.single_file KEDSL.workflow_node ->
      KEDSL.single_file KEDSL.workflow_node;
  }
  let create
      ?(wrap_bam_node = fun _ x -> x)
      ?(wrap_vcf_node = fun _ x -> x)
      ?(wrap_gtf_node = fun _ x -> x)
      ~processors ~reference_build ~work_dir ~machine () =
    {processors; reference_build; work_dir; machine;
     wrap_bam_node; wrap_vcf_node; wrap_gtf_node}

  let rec compile_aligner_step
      ~compiler ?(is:[`Normal | `Tumor] option) (pipeline : bam pipeline) =
    let {processors ; reference_build; work_dir; machine ;} = compiler in
    let gunzip_concat ?read (pipeline: fastq  pipeline) =
      match pipeline with
      | Fastq f -> f
      | Concat_text (l: fastq pipeline list) ->
        failwith "Concat_text: not implemented"
      | Gunzip_concat (l: fastq_gz pipeline list) ->
        let fastqs = List.map l ~f:(function Fastq_gz t -> t) in
        let result_path = work_dir // to_file_prefix ?is ?read pipeline ^ ".fastq" in
        dbg "Result_Path: %S" result_path;
        Workflow_utilities.Gunzip.concat ~run_with:machine fastqs ~result_path
    in
    let result_prefix = work_dir // to_file_prefix ?is pipeline in
    dbg "Result_Prefix: %S" result_prefix;
    let compile_fastq_sample (fs : fastq_sample pipeline) =
      match fs with
      | Bam_to_fastq (how, what) ->
        let bam = compile_aligner_step ~compiler ?is what in
        let sample_type =
          match how with `Single -> `Single_end | `Paired -> `Paired_end in
        let fastq_pair =
          let output_prefix = work_dir // to_file_prefix ?is ?read:None what in
          Picard.bam_to_fastq ~run_with:machine ~processors ~sample_type
            ~output_prefix bam
        in
        fastq_pair
      | Paired_end_sample (dataset, l1, l2) ->
        let r1 = gunzip_concat ~read:(`R1 dataset) l1 in
        let r2 = gunzip_concat ~read:(`R2 dataset) l2 in
        let open KEDSL in
        workflow_node (fastq_reads ~host:Machine.(as_host machine)
                         r1#product#path (Some r2#product#path))
          ~name:(sprintf "pairing %s and %s"
                   (Filename.basename r1#product#path)
                   (Filename.basename r2#product#path))
          ~edges:[ depends_on r1; depends_on r2 ]
      | Single_end_sample (dataset, single) ->
        let r1 = gunzip_concat ~read:(`R1 dataset) single in
        let open KEDSL in
        workflow_node (fastq_reads ~host:Machine.(as_host machine)
                         r1#product#path None)
          ~equivalence:`None
          ~edges:[ depends_on r1 ]
          ~name:(sprintf "single-end %s"
                   (Filename.basename r1#product#path))
    in
    let bam_node =
      match pipeline with
      | Bam_sample (name, bam_target) -> bam_target
      | Gatk_indel_realigner (configuration, bam) ->
        let input_bam = compile_aligner_step ~compiler ?is bam in
        let output_bam = result_prefix ^ ".bam" in
        Gatk.indel_realigner
          ~processors ~reference_build ~run_with:machine input_bam ~compress:false
          ~configuration ~output_bam
      | Gatk_bqsr (configuration, bam) ->
        let input_bam = compile_aligner_step ~compiler ?is bam in
        let output_bam = result_prefix ^ ".bam" in
        Gatk.base_quality_score_recalibrator
          ~configuration
          ~run_with:machine ~processors ~reference_build ~input_bam ~output_bam
      | Picard_mark_duplicates (settings, bam) ->
        let input_bam = compile_aligner_step ~compiler ?is bam in
        let output_bam = result_prefix ^ ".bam" in
        Picard.mark_duplicates ~settings
          ~run_with:machine ~input_bam output_bam
      | Bwa_mem ({gap_open_penalty; gap_extension_penalty}, what) ->
        let fastq = compile_fastq_sample what in
        Bwa.mem_align_to_sam
          ~reference_build ~processors
          ~gap_open_penalty ~gap_extension_penalty
          ~fastq ~result_prefix ~run_with:machine ()
        |> Samtools.sam_to_bam ~run_with:machine
      | Bwa ({gap_open_penalty; gap_extension_penalty}, what) ->
        let fastq = compile_fastq_sample what in
        Bwa.align_to_sam
          ~reference_build ~processors
          ~gap_open_penalty ~gap_extension_penalty
          ~fastq ~result_prefix ~run_with:machine ()
        |> Samtools.sam_to_bam ~run_with:machine
      | Mosaik (what) ->
        let fastq = compile_fastq_sample what in
        Mosaik.align ~reference_build ~processors
          ~fastq ~result_prefix ~run_with:machine ()
      | Star (what) ->
        let fastq = compile_fastq_sample what in
        Star.align ~reference_build ~processors
          ~fastq ~result_prefix ~run_with:machine ()
      | Hisat (what) ->
        let fastq = compile_fastq_sample what in
        Hisat.align ~reference_build ~processors
          ~fastq ~result_prefix ~run_with:machine ()
        |> Samtools.sam_to_bam ~run_with:machine
    in
    compiler.wrap_bam_node pipeline bam_node

  let compile_variant_caller_step ~compiler (pipeline: vcf pipeline) =
    let {processors ; reference_build; work_dir; machine ;} = compiler in
    let result_prefix = work_dir // to_file_prefix pipeline in
    dbg "Result_Prefix: %S" result_prefix;
    let vcf_node =
      match pipeline with
      | Somatic_variant_caller (som_vc, Bam_pair (normal_t, tumor_t)) ->
        let normal = compile_aligner_step ~compiler ~is:`Normal normal_t in
        let tumor = compile_aligner_step ~compiler ~is:`Tumor tumor_t in
        som_vc.Somatic_variant_caller.make_target ~reference_build ~processors
          ~run_with:machine ~normal ~tumor ~result_prefix ()
      | Germline_variant_caller (gvc, bam) ->
        let input_bam = compile_aligner_step ~compiler ?is:None bam in
        gvc.Germline_variant_caller.make_target ~processors ~reference_build
          ~run_with:machine ~input_bam ~result_prefix ()
    in
    compiler.wrap_vcf_node pipeline vcf_node

  let compile_gtf_step ~compiler (pipeline: gtf pipeline) =
    let {processors ; reference_build; work_dir; machine ;} = compiler in
    let result_prefix = work_dir // to_file_prefix pipeline in
    dbg "Result_Prefix: %S" result_prefix;
    let gtf_node =
      match pipeline with
      | Stringtie (use_reference_gtf, bam) ->
        let bam = compile_aligner_step ~compiler bam in
        Stringtie.run ~reference_build ~processors ~use_reference_gtf
          ~bam ~result_prefix ~run_with:machine ()
    in
    compiler.wrap_gtf_node pipeline gtf_node

end
