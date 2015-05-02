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
open Biokepi_util_targets
open Biokepi_somatic_targets 
open Biokepi_target_library
open Biokepi_run_environment

module File = struct
  type t = Ketrew.EDSL.user_target
end

type json = Yojson.Basic.json

type fastq_gz = Fastq_gz
type fastq = Fastq
type fastq_sample = Fastq_sample
type bam = Bam
type bam_pair = Bam_pair
type vcf = Vcf

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
      reference_build: [`B37 | `B38 | `hg19 | `hg18 | `B37decoy ] -> 
      run_with:Biokepi_run_environment.Machine.t ->
      normal:Ketrew.EDSL.user_target ->
      tumor:Ketrew.EDSL.user_target ->
      result_prefix: string ->
      processors: int ->
      unit ->
      Ketrew.EDSL.user_target
  }
end

module Germline_variant_caller = struct
  type t = {
    name: string;
    configuration_json: json;
    configuration_name: string;
    make_target:
      reference_build: [`B37 | `B38 | `hg19 | `hg18 | `B37decoy ] -> 
      run_with:Biokepi_run_environment.Machine.t ->
      input_bam:Ketrew.EDSL.user_target ->
      result_prefix: string ->
      processors: int ->
      unit ->
      Ketrew.EDSL.user_target
  }
end

type _ t =
  | Fastq_gz: File.t -> fastq_gz  t
  | Fastq: File.t -> fastq  t
  (* | List: 'a t list -> 'a list t *)
  | Paired_end_sample: string * fastq  t * fastq  t -> fastq_sample  t
  | Single_end_sample: string * fastq  t -> fastq_sample  t
  | Gunzip_concat: fastq_gz  t list -> fastq  t
  | Concat_text: fastq  t list -> fastq  t
  | Bwa: bwa_params * fastq_sample  t -> bam  t
  | Bwa_mem: bwa_params * fastq_sample  t -> bam  t
  | Gatk_indel_realigner: bam t -> bam t
  | Picard_mark_duplicates: bam t -> bam t
  | Gatk_bqsr: bam t -> bam t
  | Bam_pair: bam  t * bam  t -> bam_pair  t
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

  let gatk_indel_realigner bam = Gatk_indel_realigner bam
  let picard_mark_duplicates bam = Picard_mark_duplicates bam
  let gatk_bqsr bam = Gatk_bqsr bam

  let pair ~normal ~tumor = Bam_pair (normal, tumor)

  let germline_variant_caller t input_bam =
    Germline_variant_caller (t, input_bam)

  let gatk_haplotype_caller input_bam = 
    let configuration_name = "default" in
    let configuration_json =
      `Assoc [
        "Name", `String configuration_name;
      ] in
    let make_target ~reference_build ~run_with ~input_bam ~result_prefix ~processors () =
      Gatk.haplotype_caller ~reference_build ~run_with ~input_bam ~result_prefix `Map_reduce in
    germline_variant_caller
      {Germline_variant_caller.name = "Gatk-HaplotypeCaller";
        configuration_json;
        configuration_name;
        make_target;}
      input_bam

  let somatic_variant_caller t bam_pair =
    Somatic_variant_caller (t, bam_pair)
  
  let mutect bam_pair =
    let configuration_name = "default" in
    let configuration_json =
      `Assoc [
        "Name", `String configuration_name;
      ] in
    let make_target ~reference_build ~run_with ~normal ~tumor ~result_prefix ~processors () =
      Mutect.run ~reference_build ~run_with ~normal ~tumor ~result_prefix `Map_reduce in
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
    let make_target ~reference_build ~run_with ~normal ~tumor ~result_prefix ~processors () =
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
         fun ~reference_build ~run_with ~normal ~tumor ~result_prefix ~processors () ->
           Varscan.somatic_map_reduce ~reference_build ?adjust_mapq
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
    | Single_end_sample _ -> failwith "TODO"
    | Gunzip_concat [] -> failwith "TODO"
    | Gunzip_concat (_ :: _) ->
      begin match read with
      | None -> is_suffix ^ "-cat"
      | Some (`R1 s) -> sprintf "%s%s-R1-cat" s is_suffix
      | Some (`R2 s) -> sprintf "%s%s-R2-cat" s is_suffix
      end
    | Concat_text _ -> failwith "TODO"
    | Paired_end_sample (name, _ , _) ->
      name ^ is_suffix
    | Bwa ({ gap_open_penalty; gap_extension_penalty }, sample) ->
      sprintf "%s-bwa-gap%d-gep%d"
        (to_file_prefix ?is sample) gap_open_penalty gap_extension_penalty
    | Bwa_mem ({ gap_open_penalty; gap_extension_penalty }, sample) ->
      sprintf "%s-bwa-mem-gap%d-gep%d"
        (to_file_prefix ?is sample) gap_open_penalty gap_extension_penalty
    | Gatk_indel_realigner bam ->
      sprintf "%s-indelrealigned" (to_file_prefix ?is ?read bam)
    | Gatk_bqsr bam ->
      sprintf "%s-bqsr" (to_file_prefix ?is ?read bam)
    | Picard_mark_duplicates bam ->
      sprintf "%s-dedup" (to_file_prefix ?is ?read bam)
    | Bam_pair (nor, tum) -> to_file_prefix ?is:None nor
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
    | Fastq_gz file -> call "Fastq_gz" [`String file#name]
    | Fastq file -> call "Fastq" [`String file#name]
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
    | Gatk_indel_realigner bam ->
      let input_json = to_json bam in
      call "Gatk_indel_realigner" [`Assoc ["input", input_json]]
    | Gatk_bqsr bam ->
      let input_json = to_json bam in
      call "Gatk_bqsr" [`Assoc ["input", input_json]]
    | Germline_variant_caller (gvc, bam) ->
      call gvc.Germline_variant_caller.name [`Assoc [
          "Configuration", gvc.Germline_variant_caller.configuration_json;
          "Input", to_json bam;
        ]]
    | Picard_mark_duplicates bam ->
      call "Picard_mark_duplicates" [`Assoc ["input", to_json bam]]
    | Bam_pair (normal, tumor) ->
      call "Bam-pair" [`Assoc ["normal", to_json normal; "tumor", to_json tumor]]
    | Somatic_variant_caller (svc, bam_pair) ->
      call svc.Somatic_variant_caller.name [`Assoc [
          "Configuration", svc.Somatic_variant_caller.configuration_json;
          "Input", to_json bam_pair;
        ]]

let rec compile_aligner_step
    ~reference_build ~processors
    ~work_dir ?(is:[`Normal | `Tumor] option) ~machine (t : bam t) =
  let gunzip_concat ?read (t: fastq  t) =
    match t with
    | Fastq f -> f
    | Concat_text (l: fastq t list) ->
      failwith "Concat_text: not implemented"
    | Gunzip_concat (l: fastq_gz t list) ->
      let fastqs = List.map l ~f:(function Fastq_gz t -> t) in
      let result_path = work_dir // to_file_prefix ?is ?read t ^ ".fastq" in
      dbg "Result_Path: %S" result_path;
      Gunzip.concat ~run_with:machine fastqs ~result_path
  in
  let result_prefix = work_dir // to_file_prefix ?is t in
  dbg "Result_Prefix: %S" result_prefix;
  match t with
  | Gatk_indel_realigner bam ->
    let input_bam = compile_aligner_step ~processors ~work_dir ~reference_build ?is ~machine bam in
    let output_bam = result_prefix ^ ".bam" in
    Gatk.indel_realigner ~processors ~reference_build ~run_with:machine input_bam ~compress:false
      ~output_bam
  | Gatk_bqsr bam ->
    let input_bam = compile_aligner_step ~processors ~work_dir ~reference_build ?is ~machine bam in
    let output_bam = result_prefix ^ ".bam" in
    Gatk.base_quality_score_recalibrator
      ~run_with:machine ~processors ~reference_build ~input_bam ~output_bam
  | Picard_mark_duplicates bam ->
    let input_bam = compile_aligner_step ~processors ~work_dir ~reference_build ?is ~machine bam in
    let output_bam = result_prefix ^ ".bam" in
    Picard.mark_duplicates
      ~run_with:machine ~input_bam output_bam
  | Bwa_mem ({gap_open_penalty; gap_extension_penalty}, what) ->
    let r1, r2 =
      match what with
      | Paired_end_sample (dataset, l1, l2) ->
        let r1 = gunzip_concat ~read:(`R1 dataset) l1 in
        let r2 = gunzip_concat ~read:(`R2 dataset) l2 in
        (r1, Some r2)
      | Single_end_sample (dataset, single) ->
        let r1 = gunzip_concat ~read:(`R1 dataset) single in
        (r1, None) in
    Bwa.mem_align_to_sam
      ~reference_build ~processors
      ~gap_open_penalty ~gap_extension_penalty
      ~r1 ?r2 ~result_prefix ~run_with:machine ()
    |> Samtools.sam_to_bam ~run_with:machine
  | Bwa ({gap_open_penalty; gap_extension_penalty}, what) ->
    let r1, r2 =
      match what with
      | Paired_end_sample (dataset, l1, l2) ->
        let r1 = gunzip_concat ~read:(`R1 dataset) l1 in
        let r2 = gunzip_concat ~read:(`R2 dataset) l2 in
        (r1, Some r2)
      | Single_end_sample (dataset, single) ->
        let r1 = gunzip_concat ~read:(`R1 dataset) single in
        (r1, None) in
    Bwa.align_to_sam
      ~reference_build ~processors
      ~gap_open_penalty ~gap_extension_penalty
      ~r1 ?r2 ~result_prefix ~run_with:machine ()
    |> Samtools.sam_to_bam ~run_with:machine

let compile_variant_caller_step ~reference_build ~work_dir ~machine ?(processors=4) (t: vcf t) =
  let result_prefix = work_dir // to_file_prefix t in
  dbg "Result_Prefix: %S" result_prefix;
  match t with
  | Somatic_variant_caller (som_vc, Bam_pair (normal_t, tumor_t)) ->
    let normal = compile_aligner_step ~processors ~reference_build ~work_dir ~is:`Normal ~machine normal_t in
    let tumor = compile_aligner_step ~processors ~reference_build ~work_dir ~is:`Tumor ~machine tumor_t in
    som_vc.Somatic_variant_caller.make_target ~reference_build ~processors (* TODO configurable processors ! *)
      ~run_with:machine ~normal ~tumor ~result_prefix ()
  | Germline_variant_caller (gvc, bam) ->
    let input_bam = compile_aligner_step ~processors ~reference_build ~work_dir ~is:`Normal ~machine bam in
    gvc.Germline_variant_caller.make_target ~processors ~reference_build
      ~run_with:machine ~input_bam ~result_prefix ()
