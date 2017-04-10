(**************************************************************************)
(*  Copyright 2015, Sebastien Mondet <seb@mondet.org>                     *)
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


open Biokepi_run_environment
open Biokepi_bfx_tools
open Common



module Somatic = struct

  type from_fastqs =
    normal_fastqs:Pipeline.Construct.input_fastq ->
    tumor_fastqs:Pipeline.Construct.input_fastq ->
    dataset:string -> Pipeline.vcf Pipeline.t list

  let crazy_example ~normal_fastqs ~tumor_fastqs ~dataset =
    let open Pipeline.Construct in
    let normal = input_fastq ~dataset normal_fastqs in
    let tumor = input_fastq ~dataset tumor_fastqs in
    let bam_pair ?configuration () =
      let normal =
        bwa ?configuration normal
        |> gatk_indel_realigner
        |> picard_mark_duplicates
        |> gatk_bqsr
      in
      let tumor =
        bwa ?configuration tumor
        |> gatk_indel_realigner
        |> picard_mark_duplicates
        |> gatk_bqsr
      in
      pair ~normal ~tumor in
    let bam_pairs =
      let non_default =
        let open Bwa.Configuration.Aln in
        { name = "config42";
          gap_open_penalty = 10;
          gap_extension_penalty = 7;
          mismatch_penalty = 5; } in
      [
        bam_pair ();
        bam_pair ~configuration:non_default ();
      ] in
    let vcfs =
      List.concat_map bam_pairs ~f:(fun bam_pair ->
          [
            mutect bam_pair;
            somaticsniper bam_pair;
            somaticsniper
              ~configuration:Somaticsniper.Configuration.{
                  name = "example0001-095";
                  prior_probability = 0.001;
                  theta = 0.95;
                } bam_pair;
            varscan_somatic bam_pair;
            strelka ~configuration:Strelka.Configuration.exome_default bam_pair;
          ])
    in
    vcfs

  let from_fastqs_with_variant_caller
      ~variant_caller ~normal_fastqs ~tumor_fastqs ~dataset =
    let open Pipeline.Construct in
    let normal = input_fastq ~dataset normal_fastqs in
    let tumor = input_fastq ~dataset tumor_fastqs in
    let make_bam data =
      data |> bwa_mem |> gatk_indel_realigner |> picard_mark_duplicates |> gatk_bqsr
    in
    let vc_input =
      pair ~normal:(make_bam normal) ~tumor:(make_bam tumor) in
    [variant_caller vc_input]
end
