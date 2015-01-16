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
open Biokepi_target_library

module File = struct
  type t = Ketrew.EDSL.user_target
end

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
type _ t =
  | Fastq_gz: File.t -> fastq_gz  t
  | Fastq: File.t -> fastq  t
  (* | List: 'a t list -> 'a list t *)
  | Paired_end_sample: string * fastq  t * fastq  t -> fastq_sample  t
  | Single_end_sample: string * fastq  t -> fastq_sample  t
  | Gunzip_concat: fastq_gz  t list -> fastq  t
  | Concat_text: fastq  t list -> fastq  t
  | Bwa: bwa_params * fastq_sample  t -> bam  t
  | Bam_pair: bam  t * bam  t -> bam_pair  t
  | Mutect: bam_pair  t -> vcf  t
  | Somaticsniper: [ `S of float ] * [ `T of float ] * bam_pair  t -> vcf  t
  | Varscan: [`Adjust_mapq of int option] * bam_pair t -> vcf t

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

  let pair ~normal ~tumor = Bam_pair (normal, tumor)
  let mutect bam_pair = Mutect bam_pair

  let somaticsniper
      ?(prior_probability=Somaticsniper.default_prior_probability)
      ?(theta=Somaticsniper.default_theta) 
      bam_pair =
    Somaticsniper (`S prior_probability, `T theta, bam_pair)

  let varscan ?adjust_mapq bam_pair =
    Varscan (`Adjust_mapq adjust_mapq, bam_pair)

end


let rec to_file_prefix:
  type a.
  ?is:[ `Normal | `Tumor] ->
  ?read:[ `R1 of string | `R2 of string ] ->
  a t -> string =
  fun ?is ?read w ->
    begin match w with
    | Fastq_gz _ -> failwith "TODO"
    | Fastq _ -> failwith "TODO"
    | Single_end_sample _ -> failwith "TODO"
    | Gunzip_concat [] -> failwith "TODO"
    | Gunzip_concat (_ :: _) ->
      begin match read with
      | None -> "cat"
      | Some (`R1 s) -> sprintf "%s-R1-cat" s
      | Some (`R2 s) -> sprintf "%s-R2-cat" s
      end
    | Concat_text _ -> failwith "TODO"
    | Paired_end_sample (name, _ , _) ->
      name
      ^ (match is with
        | None  -> ""
        | Some `Normal -> "-normal"
        | Some `Tumor -> "-tumor")
    | Bwa ({ gap_open_penalty; gap_extension_penalty }, sample) ->
      sprintf "%s-bwa-gap%d-gep%d"
        (to_file_prefix ?is sample) gap_open_penalty gap_extension_penalty
    | Bam_pair (nor, tum) -> to_file_prefix ?is:None nor
    | Mutect bp -> sprintf "%s-mutect" (to_file_prefix bp)
    | Somaticsniper (`S s, `T t, bp) ->
      sprintf "%s-somaticsniper-S%F-T%F" (to_file_prefix bp) s t
    | Varscan (`Adjust_mapq amq,  bp) ->
      let prev = to_file_prefix bp in
      sprintf "%s-varscan-Amq%s"
        prev (match amq with None  -> "NONE" | Some s -> Int.to_string s)
    end



type json = Yojson.Basic.json
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
    | Bam_pair (normal, tumor) ->
      call "Bam-pair" [`Assoc ["normal", to_json normal; "tumor", to_json tumor]]
    | Mutect bam_pair ->
      call "Mutect"[`Assoc ["input", to_json bam_pair]]
    | Somaticsniper (`S minus_s, `T minus_T, bam_pair) ->
      call "Somaticsniper" [`Assoc [
          "Minus-s", `Float minus_s;
          "Minus-T", `Float minus_T;
          "input", to_json bam_pair;
        ]]
    | Varscan (`Adjust_mapq adjust_mapq, bam_pair) ->
      call "Varscan" [`Assoc [
          "Adjust_mapq",
          `String (Option.value_map adjust_mapq ~f:Int.to_string ~default:"None");
          "input", to_json bam_pair;
        ]]

let compile_aligner_step
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
      ~gap_open_penalty ~gap_extension_penalty
      ~r1 ?r2 ~result_prefix ~run_with:machine ()
    |> Samtools.sam_to_bam ~run_with:machine

let compile_variant_caller_step ~work_dir ~machine (t: vcf t) =
  let result_prefix = work_dir // to_file_prefix t in
  dbg "Result_Prefix: %S" result_prefix;
  match t with
  | Mutect (Bam_pair (normal_t, tumor_t)) ->
    let normal = compile_aligner_step ~work_dir ~is:`Normal ~machine normal_t in
    let tumor = compile_aligner_step ~work_dir ~is:`Tumor ~machine tumor_t in
    Mutect.run ~run_with:machine ~result_prefix ~normal ~tumor `Map_reduce
  | Somaticsniper (`S minus_s, `T minus_T, Bam_pair (normal_t, tumor_t)) ->
    let normal = compile_aligner_step ~work_dir ~is:`Normal ~machine normal_t in
    let tumor = compile_aligner_step ~work_dir ~is:`Tumor ~machine tumor_t in
    Somaticsniper.run
      ~run_with:machine ~minus_s ~minus_T ~normal ~tumor ~result_prefix ()
  | Varscan (`Adjust_mapq adjust_mapq, Bam_pair (normal_t, tumor_t)) ->
    let normal = compile_aligner_step ~work_dir ~is:`Normal ~machine normal_t in
    let tumor = compile_aligner_step ~work_dir ~is:`Tumor ~machine tumor_t in
    Varscan.map_reduce
      ~run_with:machine ?adjust_mapq ~normal ~tumor ~result_prefix ()
