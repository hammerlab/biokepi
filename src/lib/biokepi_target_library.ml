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
open Biokepi_util_targets 
open Biokepi_somatic_targets 

module Bwa = struct

  let default_gap_open_penalty = 11
  let default_gap_extension_penalty = 4

  let index
      ~reference_build
      ~(run_with : Machine.t) =
    let open Ketrew.EDSL in
    let reference_fasta =
      Machine.get_reference_genome run_with reference_build
      |> Biokepi_reference_genome.fasta in
    (* `bwa index` creates a bunch of files, c.f.
       [this question](https://www.biostars.org/p/73585/) we detect the
       `.bwt` one. *)
    let bwa_tool = Machine.get_tool run_with "bwa" in
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

  let read_group_header_option algorithm =
    (* this option should magically make the sam file compatible
             mutect and other GATK-like pieces of software
             http://seqanswers.com/forums/showthread.php?t=17233

             The `LB` one seems “necessary” for somatic sniper:
             `[bam_header_parse] missing LB tag in @RG lines.`
          *)
      match algorithm with
        |`Mem -> "-R \"@RG\tID:bwa\tSM:SM\tLB:ga\tPL:Illumina\""
        |`Aln -> "-r \"@RG\tID:bwa\tSM:SM\tLB:ga\tPL:Illumina\""

  let mem_align_to_sam
      ~reference_build
      ~processors
      ?(gap_open_penalty=default_gap_open_penalty)
      ?(gap_extension_penalty=default_gap_extension_penalty)
      ~(r1: Ketrew.EDSL.user_target)
      ?(r2: Ketrew.EDSL.user_target option)
      ~(result_prefix:string)
      ~(run_with : Machine.t)
      () =
    let open Ketrew.EDSL in
    let reference_fasta =
      Machine.get_reference_genome run_with reference_build
      |> Biokepi_reference_genome.fasta in
    let in_work_dir =
      Program.shf "cd %s" Filename.(quote (dirname result_prefix)) in
    (* `bwa index` creates a bunch of files, c.f.
       [this question](https://www.biostars.org/p/73585/) we detect the
       `.bwt` one. *)
    let bwa_tool = Machine.get_tool run_with "bwa" in
    let bwa_index = index ~reference_build ~run_with in
    let result = sprintf "%s.sam" result_prefix in
    let name = sprintf "bwa-mem-%s" (Filename.basename r1#product#path) in
    let bwa_base_command =
      String.concat ~sep:" " [
        "bwa mem";
        (read_group_header_option `Mem);
        "-t"; Int.to_string processors;
        "-O"; Int.to_string gap_open_penalty;
        "-E"; Int.to_string gap_extension_penalty;
        (Filename.quote reference_fasta#product#path);
        (Filename.quote r1#product#path);
      ] in
    let bwa_base_target ?(more_dependencies=[]) ~bwa_command  = 
      file_target result ~host:Machine.(as_host run_with) ~name
          ~dependencies:(Tool.(ensure bwa_tool) :: bwa_index :: r1 :: more_dependencies)
          ~if_fails_activate:[Remove.file ~run_with result]
          ~tags:[Target_tags.aligner]
          ~make:(Machine.run_program run_with ~processors ~name
              Program.(
                Tool.(init bwa_tool)
                && in_work_dir
                && sh bwa_command))
    in
    match r2 with
      | Some read2 -> 
        let bwa_command = String.concat ~sep:" " [
          bwa_base_command;
          (Filename.quote read2#product#path);
          ">"; (Filename.quote result);
        ] in
        bwa_base_target ~bwa_command ~more_dependencies:[read2;]
      | None -> 
        let bwa_command = String.concat ~sep:" " [
          bwa_base_command;
           ">"; (Filename.quote result);
        ] in
        bwa_base_target ~bwa_command ~more_dependencies:[]



  let align_to_sam
      ~reference_build
      ~processors
      ?(gap_open_penalty=default_gap_open_penalty)
      ?(gap_extension_penalty=default_gap_extension_penalty)
      ~(r1: Ketrew.EDSL.user_target)
      ?(r2: Ketrew.EDSL.user_target option)
      ~(result_prefix:string)
      ~(run_with : Machine.t)
      () =
    let open Ketrew.EDSL in
    let reference_fasta =
      Machine.get_reference_genome run_with reference_build
      |> Biokepi_reference_genome.fasta in
    let in_work_dir =
      Program.shf "cd %s" Filename.(quote (dirname result_prefix)) in
    (* `bwa index` creates a bunch of files, c.f.
       [this question](https://www.biostars.org/p/73585/) we detect the
       `.bwt` one. *)
    let bwa_tool = Machine.get_tool run_with "bwa" in
    let bwa_index = index ~reference_build ~run_with in
    let bwa_aln read_number read =
      let name = sprintf "bwa-aln-%s" (Filename.basename read#product#path) in
      let result = sprintf "%s-R%d.sai" result_prefix read_number in
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
    let r2_sai_opt = Option.map r2 ~f:(fun r -> (bwa_aln 2 r, r)) in
    let sam =
      let name = sprintf "bwa-sam-%s" (Filename.basename result_prefix) in
      let result = sprintf "%s.sam" result_prefix in
      let program, dependencies =
        let common_deps = [r1_sai; reference_fasta; bwa_index; Tool.(ensure bwa_tool) ] in
        match r2_sai_opt with
        | Some (r2_sai, r2) ->
          Program.(
            Tool.(init bwa_tool)
            && in_work_dir
            && shf "bwa sampe %s %s %s %s %s %s > %s"
              (read_group_header_option `Aln)
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
              (read_group_header_option `Aln)
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
