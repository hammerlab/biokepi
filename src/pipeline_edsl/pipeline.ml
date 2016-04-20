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


open Biokepi_run_environment
open Biokepi_bfx_tools
open Common


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

(** Seq2HLA & Optitype have unique return file formats *)
type seq2hla_hla_types = Seq2HLA_hla_types
type optitype_hla_types = Optitype_hla_types


type somatic = { normal : bam; tumor : bam }
type germline = bam

type fastq_sample_info = {
  sample_name: string;
  fragment_id: string;
}

module Variant_caller = struct

  type _ input =
    | Somatic: somatic -> somatic input
    | Germline: germline -> germline input

  type 'a t = {
    name: string;
    configuration_json: json;
    configuration_name: string;
    make_target:
      run_with: Machine.t ->
      input: 'a input ->
      result_prefix: string ->
      processors: int ->
      ?more_edges: KEDSL.workflow_edge list ->
      unit ->
      KEDSL.file_workflow
  }
end

type metadata_spec = [
  | `Add_tags of string list
  | `Add_tags_rec of string list
]

type _ t =
  | Fastq_gz: File.t -> fastq_gz  t
  | Fastq: File.t -> fastq  t
  (* | List: 'a t list -> 'a list t *)
  | Bam_sample: string * bam -> bam t
  | Bam_to_fastq: fastq_sample_info option * [ `Single | `Paired ] * bam t -> fastq_sample t
  | Paired_end_sample: fastq_sample_info * fastq t * fastq t -> fastq_sample t
  | Single_end_sample: fastq_sample_info * fastq t -> fastq_sample t
  | Gunzip_concat: fastq_gz t list -> fastq t
  | Concat_text: fastq t list -> fastq t
  | Star: Star.Configuration.Align.t * fastq_sample t -> bam t
  | Hisat: Hisat.Configuration.t * fastq_sample t -> bam t
  | Stringtie: Stringtie.Configuration.t * bam t -> gtf t
  | Bwa: Bwa.Configuration.Aln.t * fastq_sample t -> bam t
  | Bwa_mem: Bwa.Configuration.Mem.t * fastq_sample t -> bam t
  | Mosaik: fastq_sample t -> bam t
  | Gatk_indel_realigner: Gatk.Configuration.indel_realigner * bam t -> bam t
  | Picard_mark_duplicates: Picard.Mark_duplicates_settings.t * bam t -> bam t
  | Gatk_bqsr: (Gatk.Configuration.bqsr * bam t) -> bam t
  | Bam_pair: bam t * bam t -> bam_pair t
  | Somatic_variant_caller: somatic Variant_caller.t * bam_pair t -> vcf t
  | Germline_variant_caller: germline Variant_caller.t * bam t -> vcf t
  | Seq2HLA: fastq_sample t -> seq2hla_hla_types t
  | Optitype: ([`DNA | `RNA] * fastq_sample t) -> optitype_hla_types t
  | With_metadata: metadata_spec * 'a t -> 'a t

module Construct = struct

  type input_fastq = [
    | `Paired_end of File.t list * File.t list
    | `Single_end of File.t list
  ]

  let input_fastq ~dataset (fastqs: input_fastq) =
    let is_fastq_gz p =
      Filename.check_suffix p "fastq.gz" || Filename.check_suffix p "fq.gz"  in
    let is_fastq p =
      Filename.check_suffix p "fastq" || Filename.check_suffix p "fq"  in
    let theyre_all l f = List.for_all l ~f:(fun file -> f file#product#path) in
    let bring_to_single_fastq l =
      match l with
      | [] -> failwithf "Dataset %S seems empty" dataset
      | gzs when theyre_all gzs is_fastq_gz ->
        Gunzip_concat  (List.map gzs (fun f -> Fastq_gz f))
      | fqs when theyre_all fqs is_fastq ->
        Concat_text (List.map fqs (fun f -> Fastq f))
      | not_supported ->
        failwithf
          "For now, a sample must be a uniform list of \
           fastq.gz/fq.gz or .fq/.fastq files. \
           Dataset %S does not qualify: [%s]
          "
          dataset
          (List.map not_supported ~f:(fun f -> Filename.basename f#product#path)
           |> String.concat ~sep:", ")
    in
    let sample_info = {sample_name = dataset; fragment_id = dataset} in
    match fastqs with
    | `Paired_end (l1, l2) ->
      Paired_end_sample (sample_info, bring_to_single_fastq l1, bring_to_single_fastq l2)
    | `Single_end l ->
      Single_end_sample (sample_info, bring_to_single_fastq l)

  let bam ~dataset bam = Bam_sample (dataset, bam)

  let bam_to_fastq ?sample_name how bam = Bam_to_fastq (sample_name, how, bam)

  let bwa ?(configuration = Bwa.Configuration.Aln.default) fastq =
    Bwa (configuration, fastq)

  let bwa_aln = bwa

  let bwa_mem ?(configuration = Bwa.Configuration.Mem.default) fastq =
    Bwa_mem (configuration, fastq)

  let mosaik fastq = Mosaik fastq

  let star ?(configuration = Star.Configuration.Align.default) fastq =
    Star (configuration, fastq)

  let hisat ?(configuration = Hisat.Configuration.default_v1) fastq =
    Hisat (configuration, fastq)

  let stringtie ?(configuration = Stringtie.Configuration.default) bam =
    Stringtie (configuration, bam)

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
        ~run_with ~input ~result_prefix ~processors ?more_edges () =
      match input with
      | Variant_caller.Germline input_bam ->
        Gatk.haplotype_caller ?more_edges ~run_with
          ~input_bam ~result_prefix `Map_reduce in
    germline_variant_caller
      {Variant_caller.name = "Gatk-HaplotypeCaller";
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
        ~run_with ~input ~result_prefix ~processors ?more_edges () =
      match input with | Variant_caller.Somatic {normal; tumor} ->
      Mutect.run
        ~configuration
        ?more_edges
        ~run_with
        ~normal ~tumor
        ~result_prefix `Map_reduce in
    somatic_variant_caller
      {Variant_caller.name = "Mutect";
       configuration_json;
       configuration_name;
       make_target;}
      bam_pair

  let mutect2 ?(configuration=Gatk.Configuration.Mutect2.default) bam_pair =
    let configuration_name = configuration.Gatk.Configuration.Mutect2.name in
    let configuration_json = Gatk.Configuration.Mutect2.to_json configuration in
    let make_target
        ~run_with ~input ~result_prefix ~processors ?more_edges () =
      match input with
      | Variant_caller.Somatic {normal; tumor} ->
        Gatk.mutect2
          ~configuration ?more_edges ~run_with
          ~input_normal_bam:normal ~input_tumor_bam:tumor
          ~result_prefix `Map_reduce in
    somatic_variant_caller
      {Variant_caller.name = "Mutect";
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
        ~run_with ~input ~result_prefix ~processors ?more_edges () =
      match input with | Variant_caller.Somatic {normal; tumor} ->
      Somaticsniper.run
        ~run_with ~minus_s:prior_probability ~minus_T:theta
        ~normal ~tumor ~result_prefix () in
    somatic_variant_caller
      {Variant_caller.name = "Somaticsniper";
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
      {Variant_caller.name = "Varscan-somatic";
       configuration_json;
       configuration_name;
       make_target = begin
         fun ~run_with ~input ~result_prefix ~processors ?more_edges () ->
           match input with | Variant_caller.Somatic {normal; tumor} ->
           Varscan.somatic_map_reduce ?adjust_mapq
             ?more_edges ~run_with ~normal ~tumor ~result_prefix ()
       end}
      bam_pair

  let strelka ~configuration bam_pair =
    somatic_variant_caller
      {Variant_caller.name = "Strelka";
       configuration_json = Strelka.Configuration.to_json configuration;
       configuration_name = configuration.Strelka.Configuration.name;
       make_target =
         fun ~run_with ~input ~result_prefix ~processors ?more_edges () ->
            match input with | Variant_caller.Somatic {normal; tumor} ->
            Strelka.run
              ?more_edges
              ~configuration ~normal ~tumor
              ~run_with ~result_prefix ~processors
              ()
      }
      bam_pair

  let virmid ~configuration bam_pair =
    somatic_variant_caller
      {Variant_caller.name = "Virmid";
       configuration_json = Virmid.Configuration.to_json configuration;
       configuration_name = configuration.Virmid.Configuration.name;
       make_target =
          fun ~run_with ~input ~result_prefix ~processors
            ?more_edges () ->
            match input with | Variant_caller.Somatic {normal; tumor} ->
            Virmid.run
              ?more_edges
              ~configuration ~normal ~tumor
              ~run_with ~result_prefix ~processors
              ()
      }
      bam_pair

  let muse ~configuration bam_pair =
    let make_target
        ~(run_with: Machine.t) ~input ~result_prefix ~processors
        ?more_edges () =
      match input with | Variant_caller.Somatic {normal; tumor} ->
      Muse.run ~configuration ?more_edges
        ~run_with ~normal ~tumor ~result_prefix `Map_reduce in
    somatic_variant_caller
      {Variant_caller.name = "Muse";
       configuration_json = Muse.Configuration.to_json configuration;
       configuration_name = configuration.Muse.Configuration.name;
       make_target }
      bam_pair

  let seq2hla fastq_sample = Seq2HLA fastq_sample

  let optitype kind fastq_sample = Optitype (kind, fastq_sample)

  let add_tags ?(recursively = false) tags pipeline =
    With_metadata ((if recursively then `Add_tags_rec tags else `Add_tags tags),
                   pipeline)

end

let rec to_file_prefix:
  type a.
  ?read:[ `R1 of string | `R2 of string ] ->
  a t -> string =
  fun ?read w ->
    begin match w with
    | With_metadata (_, p) -> to_file_prefix ?read p
    | Fastq_gz _ -> failwith "TODO"
    | Fastq _ -> failwith "TODO"
    | Single_end_sample (info, _) -> info.fragment_id
    | Gunzip_concat [] -> failwith "TODO"
    | Gunzip_concat (_ :: _) ->
      begin match read with
      | None -> "-cat"
      | Some (`R1 s) -> sprintf "%s-R1-cat" s
      | Some (`R2 s) -> sprintf "%s-R2-cat" s
      end
    | Concat_text _ -> failwith "TODO"
    | Bam_sample (name, _) -> Filename.basename name
    | Bam_to_fastq (_, how, bam) ->
      sprintf "%s-b2fq-%s"
        (to_file_prefix bam)
        (match how with `Paired -> "PE" | `Single -> "SE")
    | Paired_end_sample (info, _ , _) -> info.fragment_id
    | Bwa (configuration, sample) ->
      sprintf "%s-bwa-%s"
        (to_file_prefix sample) (Bwa.Configuration.Aln.name configuration)
    | Bwa_mem (configuration, sample) ->
      sprintf "%s-bwa-mem-%s"
        (to_file_prefix sample) (Bwa.Configuration.Mem.name configuration)
    | Star (configuration, sample) ->
      sprintf "%s-%s-star-aligned"
        (to_file_prefix sample)
        (Star.Configuration.Align.name configuration)
    | Hisat (conf, sample) ->
      sprintf "%s-hisat-%s-aligned" (to_file_prefix sample) (conf.Hisat.Configuration.name)
    | Stringtie (conf, sample) ->
      sprintf "%s-%s-stringtie"
        (to_file_prefix sample)
        (conf.Stringtie.Configuration.name)
    | Mosaik (sample) ->
      sprintf "%s-mosaik" (to_file_prefix sample)
    | Gatk_indel_realigner ((indel_cfg, target_cfg), bam) ->
      let open Gatk.Configuration in
      sprintf "%s-indelrealigned-%s-%s"
        (to_file_prefix ?read bam)
        indel_cfg.Indel_realigner.name
        target_cfg.Realigner_target_creator.name
    | Gatk_bqsr ((bqsr_cfg, print_reads_cfg), bam) ->
      let open Gatk.Configuration in
      sprintf "%s-bqsr-%s-%s"
        (to_file_prefix ?read bam)
        bqsr_cfg.Bqsr.name
        bqsr_cfg.Print_reads.name
    | Picard_mark_duplicates (_, bam) ->
      (* The settings, for now, do not impact the result *)
      sprintf "%s-dedup" (to_file_prefix ?read bam)
    | Bam_pair (nor, tum) -> to_file_prefix tum
    | Somatic_variant_caller (vc, bp) ->
      let prev = to_file_prefix bp in
      sprintf "%s-%s-%s" prev
        vc.Variant_caller.name
        vc.Variant_caller.configuration_name
    | Germline_variant_caller (vc, bp) ->
      let prev = to_file_prefix bp in
      sprintf "%s-%s-%s" prev
        vc.Variant_caller.name
        vc.Variant_caller.configuration_name
    | Seq2HLA s ->
      sprintf "seq2hla-%s" (to_file_prefix ?read s)
    | Optitype (kind, s) ->
      sprintf "optitype-%s-%s"
        (match kind with `DNA -> "DNA" | `RNA -> "RNA")
        (to_file_prefix ?read s)
    end



let rec to_json: type a. a t -> json =
  fun w ->
    let call name (args : json list): json = `List (`String name :: args) in
    match w with
    | Fastq_gz file -> call "Fastq_gz" [`String file#product#path]
    | Fastq file -> call "Fastq" [`String file#product#path]
    | Bam_sample (name, file) ->
      call "Bam-sample" [`String name; `String file#product#path]
    | Bam_to_fastq (name, how, bam) ->
      let how_string =
        match how with `Paired -> "Paired" | `Single -> "Single" in
      call "Bam-to-fastq" [`String how_string; to_json bam]
    | Paired_end_sample ({sample_name; fragment_id}, r1, r2) ->
      call "Paired-end" [`String sample_name; `String fragment_id;
                         to_json r1; to_json r2]
    | Single_end_sample ({sample_name; fragment_id}, r) ->
      call "Single-end" [`String sample_name; `String fragment_id; to_json r]
    | Gunzip_concat fastq_gz_list ->
      call "Gunzip-concat" (List.map ~f:to_json fastq_gz_list)
    | Concat_text fastq_list ->
      call "Concat" (List.map ~f:to_json fastq_list)
    | Bwa (config, input) ->
      call "BWA" [
        `Assoc ["configuration", Bwa.Configuration.Aln.to_json config];
        to_json input
      ]
    | Bwa_mem (params, input) ->
      let input_json = to_json input in
      call "BWA-MEM" [
        `Assoc ["configuration", Bwa.Configuration.Mem.to_json params];
        input_json
      ]
    | Star (conf, input) ->
      let input_json = to_json input in
      call "STAR" [
        `Assoc ["configuration", Star.Configuration.Align.to_json conf];
        input_json;
      ]
    | Hisat (conf, input) ->
      let input_json = to_json input in
      call "HISAT" [
        `Assoc ["configuration", Hisat.Configuration.to_json conf];
        input_json;
      ]
    | Stringtie (conf, input) ->
      let input_json = to_json input in
      call "Stringtie" [
        `Assoc ["configuration", Stringtie.Configuration.to_json conf];
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
      call gvc.Variant_caller.name [`Assoc [
          "Configuration", gvc.Variant_caller.configuration_json;
          "Input", to_json bam;
        ]]
    | Picard_mark_duplicates (settings, bam) ->
      (* The settings should not impact the output, so we don't dump them. *)
      call "Picard_mark_duplicates" [`Assoc ["input", to_json bam]]
    | Bam_pair (normal, tumor) ->
      call "Bam-pair" [`Assoc ["normal", to_json normal; "tumor", to_json tumor]]
    | Somatic_variant_caller (svc, bam_pair) ->
      call svc.Variant_caller.name [`Assoc [
          "Configuration", svc.Variant_caller.configuration_json;
          "Input", to_json bam_pair;
        ]]
    | Seq2HLA input ->
      call "Seq2HLA" [`Assoc [
          "Input", to_json input
        ]]
    | Optitype (kind, input) ->
      call "Optitype" [`Assoc [
          "Input", to_json input;
          "Kind", `String (match kind with `DNA -> "DNA" | `RNA -> "RNA")
        ]]
    | With_metadata (_, p) -> to_json p

module Compiler = struct
  type 'a pipeline = 'a t
  type workflow_option_failure_mode = [ `Silent | `Fail_if_not_happening ]
  type workflow_option = [
    | `Multi_sample_indel_realignment of workflow_option_failure_mode
    | `Parallel_alignment_over_fastq_fragments of
        [ `Bwa_mem | `Bwa | `Mosaik | `Star | `Hisat ] list
        * workflow_option_failure_mode
    | `Map_reduce of [ `Gatk_indel_realigner ]
  ]
  type t = {
    processors : int;
    reference_build: Reference_genome.name;
    work_dir: string;
    machine : Machine.t;
    options: workflow_option list;
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
      ?(options=[])
      ~processors ~reference_build ~work_dir ~machine () =
    {processors; reference_build; work_dir; machine; options;
     wrap_bam_node; wrap_vcf_node; wrap_gtf_node}

  let has_option {options; _} f =
    List.exists options ~f

  let apply_with_metadata ~metadata_spec wf =
    begin match metadata_spec with
    | `Add_tags tgs -> KEDSL.add_tags ~recursive:false wf tgs
    | `Add_tags_rec tgs -> KEDSL.add_tags ~recursive:true wf tgs
    end;
    wf

  (* This compiler variable name is confusing, perhaps 'state' is better? *)
  let rec fastq_step ~read ~compiler (pipeline: fastq pipeline) =
    let {work_dir; machine ; _ } = compiler in
    match pipeline with
    | Fastq f -> f
    | Concat_text (l: fastq pipeline list) ->
      failwith "Compilation of Biokepi.Pipeline.Concat_text: not implemented"
    | Gunzip_concat (l: fastq_gz pipeline list) ->
      let fastqs =
        let rec f = 
          function
          | Fastq_gz t -> t
          | With_metadata (metadata_spec, p) -> 
            apply_with_metadata ~metadata_spec (f p)
        in
        List.map l ~f in
      let result_path = work_dir // to_file_prefix ~read pipeline ^ ".fastq" in
      dbg "Result_Path: %S" result_path;
      Workflow_utilities.Gunzip.concat ~run_with:machine fastqs ~result_path
    | With_metadata (metadata_spec, pipeline) ->
      fastq_step ~read ~compiler pipeline |> apply_with_metadata ~metadata_spec

  let rec fastq_sample_step ~compiler (fs : fastq_sample pipeline) =
    let {processors ; work_dir; machine ; _ } = compiler in

      match fs with
      | Bam_to_fastq (info_opt, how, what) ->
        let bam = compile_aligner_step ~compiler what in
        let sample_type =
          match how with `Single -> `Single_end | `Paired -> `Paired_end in
        let fastq_pair =
          let output_prefix = work_dir // to_file_prefix ?read:None what in
          let sample_name = Option.map info_opt (fun x -> x.sample_name) in
          Picard.bam_to_fastq ~run_with:machine ~processors ~sample_type
            ?sample_name
            ~output_prefix bam
        in
        fastq_pair
      | Paired_end_sample (info, l1, l2) ->
        let r1 = fastq_step ~compiler ~read:(`R1 info.fragment_id) l1 in
        let r2 = fastq_step ~compiler ~read:(`R2 info.fragment_id) l2 in
        let open KEDSL in
        workflow_node (fastq_reads ~host:Machine.(as_host machine)
                         ~name:info.sample_name
                         r1#product#path (Some r2#product#path))
          ~name:(sprintf "Pairing sample %s (%s and %s)"
                   info.sample_name
                   (Filename.basename r1#product#path)
                   (Filename.basename r2#product#path))
          ~edges:[ depends_on r1; depends_on r2 ]
      | Single_end_sample (info, single) ->
        let r1 = fastq_step ~compiler ~read:(`R1 info.fragment_id) single in
        let open KEDSL in
        workflow_node (fastq_reads ~host:Machine.(as_host machine)
                         ~name:info.sample_name
                         r1#product#path None)
          ~equivalence:`None
          ~edges:[ depends_on r1 ]
          ~name:(sprintf "single-end %s (%s)"
                   info.sample_name
                   (Filename.basename r1#product#path))
      | With_metadata (metadata_spec, p) ->
        fastq_sample_step ~compiler p |> apply_with_metadata ~metadata_spec

  and compile_aligner_step ~compiler (pipeline : bam pipeline) =
    let {processors; reference_build; work_dir; machine; _} = compiler in
    let result_prefix = work_dir // to_file_prefix pipeline in
    dbg "Result_Prefix: %S" result_prefix;
    let rec parallelize_alignment ~make_aligner sample =
      match sample with
      | Paired_end_sample (info,
                           Gunzip_concat r1_list,
                           Gunzip_concat r2_list) ->
        let exploded =
          let count = ref 0 in
          List.map2 r1_list r2_list
            ~f:(fun r1 r2 ->
                match r1, r2 with
                | (Fastq_gz wf1, Fastq_gz wf2) ->
                  let new_info =
                    incr count; 
                    {info with
                     fragment_id =
                       (* fragmenting = creating fragments of previous fragment *)
                       sprintf "%s-fragment-%d" info.fragment_id !count} in
                  make_aligner (
                    Paired_end_sample (new_info,
                                       Gunzip_concat [r1],
                                       Gunzip_concat [r2]))
                | other -> failwith "compile_aligner_step: not implemented"
              ) in
        let bams = List.map exploded ~f:(compile_aligner_step ~compiler) in
        Samtools.merge_bams ~run_with:machine bams
          (result_prefix ^ "-merged.bam")
      | With_metadata (metadata_spec, p) ->
        parallelize_alignment ~make_aligner p
        |> apply_with_metadata ~metadata_spec
      | other ->
        failwith "parallelize_alignment: Not fully implemented"
    in
    let catch_parallelize_aligner aligner sample =
      let option =
        List.find_map compiler.options
          (function
          | `Parallel_alignment_over_fastq_fragments (al, todo)
            when List.mem ~set:al aligner -> Some todo
          | _ -> None)
      in
      match sample with
      | Paired_end_sample (name,
                           Gunzip_concat r1_list,
                           Gunzip_concat r2_list)
        when List.length r1_list > 1 ->
        begin match option with
        | Some _ -> `Caught
        | None -> `Not_caught
        end
      | Paired_end_sample (name,
                           Gunzip_concat r1_list,
                           Gunzip_concat r2_list)
        when List.length r1_list = 1 -> `Not_caught
      | other ->
        begin match option with
        | Some `Silent
        | None -> `Not_caught
        | Some `Fail_if_not_happening ->
          failwithf "Option Parallel_alignment_over_fastq_fragments is set to \
                     Fail_if_not_happening and it didn't happen:\n%s"
            (to_json other |> Yojson.Basic.pretty_to_string)
        end
    in
    let perform_aligner_parallelization aligner_tag ~make_aligner
        ~make_workflow sample =
      begin match catch_parallelize_aligner aligner_tag sample  with
      | `Caught -> parallelize_alignment ~make_aligner sample
      | `Not_caught ->
        let fastq = fastq_sample_step ~compiler sample in
        make_workflow fastq
      end
    in
    let bam_node =
      match pipeline with
      | Bam_sample (name, bam_target) -> bam_target
      | Gatk_indel_realigner (configuration, bam)
        when has_option compiler ((=) (`Map_reduce `Gatk_indel_realigner)) ->
        let input_bam = compile_aligner_step ~compiler bam in
        Gatk.indel_realigner_map_reduce
          ~processors ~run_with:machine ~compress:false
          ~configuration (KEDSL.Single_bam input_bam)
      | Gatk_indel_realigner (configuration, bam) ->
        let input_bam = compile_aligner_step ~compiler bam in
        Gatk.indel_realigner
          ~processors ~run_with:machine ~compress:false
          ~configuration (KEDSL.Single_bam input_bam)
      | Gatk_bqsr (configuration, bam) ->
        let input_bam = compile_aligner_step ~compiler bam in
        let output_bam = result_prefix ^ ".bam" in
        Gatk.base_quality_score_recalibrator
          ~configuration
          ~run_with:machine ~processors ~input_bam ~output_bam
      | Picard_mark_duplicates (settings, bam) ->
        let input_bam = compile_aligner_step ~compiler bam in
        let output_bam = result_prefix ^ ".bam" in
        Picard.mark_duplicates ~settings
          ~run_with:machine ~input_bam output_bam
      | Bwa_mem (bwa_mem_config, fq_sample) ->
        perform_aligner_parallelization
          `Bwa_mem ~make_aligner:(fun pe -> Bwa_mem (bwa_mem_config, pe))
          fq_sample
          ~make_workflow:(fun fastq ->
              Bwa.mem_align_to_sam
                ~reference_build ~processors
                ~configuration:bwa_mem_config
                ~fastq ~result_prefix ~run_with:machine ()
              |> Samtools.sam_to_bam ~reference_build ~run_with:machine)
      | Bwa (bwa_config, what) ->
        perform_aligner_parallelization
          `Bwa ~make_aligner:(fun pe -> Bwa (bwa_config, pe)) what
          ~make_workflow:(fun fastq ->
              Bwa.align_to_sam
                ~reference_build ~processors
                ~configuration:bwa_config
                ~fastq ~result_prefix ~run_with:machine ()
              |> Samtools.sam_to_bam ~reference_build ~run_with:machine)
      | Mosaik (what) ->
        perform_aligner_parallelization
          `Mosaik ~make_aligner:(fun pe -> Mosaik (pe)) what
          ~make_workflow:(fun fastq ->
              Mosaik.align ~reference_build ~processors
                ~fastq ~result_prefix ~run_with:machine ())
      | Star (configuration, what) ->
        perform_aligner_parallelization
          `Star ~make_aligner:(fun pe -> Star (configuration, pe)) what
          ~make_workflow:(fun fastq ->
              Star.align ~configuration ~reference_build ~processors
                ~fastq ~result_prefix ~run_with:machine ())
      | Hisat (configuration, what) ->
        perform_aligner_parallelization
          `Hisat ~make_aligner:(fun pe -> Hisat (configuration, pe)) what
          ~make_workflow:(fun fastq ->
              Hisat.align ~configuration ~reference_build ~processors
                ~fastq ~result_prefix ~run_with:machine ()
              |> Samtools.sam_to_bam ~reference_build ~run_with:machine
            )
      | With_metadata (metadata_spec, p)  ->
        compile_aligner_step ~compiler p
        |> apply_with_metadata ~metadata_spec
    in
    compiler.wrap_bam_node pipeline bam_node

  let rec compile_bam_pair ~compiler (pipeline : bam_pair pipeline) :
      [ `Normal of KEDSL.bam_file KEDSL.workflow_node ] *
      [ `Tumor of KEDSL.bam_file KEDSL.workflow_node ] *
      [ `Pipeline of bam_pair pipeline ]
    =
    let {processors ; reference_build; work_dir; machine; _} = compiler in
    begin match pipeline with
    | Bam_pair (
        (Gatk_bqsr (n_bqsr_config, Gatk_indel_realigner (n_gir_conf, n_bam)))
        ,
        (Gatk_bqsr (t_bqsr_config, Gatk_indel_realigner (t_gir_conf, t_bam)))
      )
      when
        has_option compiler
          (function `Multi_sample_indel_realignment _ -> true | _ -> false)
        && n_gir_conf = t_gir_conf ->
      let normal = compile_aligner_step ~compiler n_bam in
      let tumor = compile_aligner_step ~compiler t_bam in
      let bam_list_node =
        if has_option compiler ((=) (`Map_reduce `Gatk_indel_realigner))
        then (
          Gatk.indel_realigner_map_reduce
            ~processors ~run_with:machine ~compress:false
            ~configuration:n_gir_conf (KEDSL.Bam_workflow_list [normal; tumor])
        ) else
          Gatk.indel_realigner
            ~processors ~run_with:machine ~compress:false
            ~configuration:n_gir_conf (KEDSL.Bam_workflow_list [normal; tumor])
      in
      begin match KEDSL.explode_bam_list_node bam_list_node with
      | [realigned_normal; realigned_tumor] ->
        let new_pipeline =
          Bam_pair (
            Gatk_bqsr (n_bqsr_config,
                       Bam_sample (Filename.chop_extension realigned_normal#product#path,
                                   realigned_normal)),
            Gatk_bqsr (t_bqsr_config,
                       Bam_sample (Filename.chop_extension realigned_tumor#product#path,
                                   realigned_tumor)))
        in
        compile_bam_pair ~compiler new_pipeline
      | other ->
        failwithf "Gatk.indel_realigner did not return the correct list \
                   of length 2 (tumor, normal): it gave %d bams"
          (List.length other)
      end
    | Bam_pair ( Gatk_bqsr (_, Gatk_indel_realigner (_, _)),
                 Gatk_bqsr (_, Gatk_indel_realigner (_, _))) as bam_pair
      when
        has_option compiler
          ((=) (`Multi_sample_indel_realignment `Fail_if_not_happening)) ->
      failwithf "Option (`Multi_sample_indel_realignment \
                 `Fail_if_not_happening) is set and this pipeline does not \
                 qualify:\n%s"
        (to_json bam_pair |> Yojson.Basic.pretty_to_string)
    | Bam_pair (normal_t, tumor_t) as final_pipeline ->
      let normal = compile_aligner_step ~compiler normal_t in
      let tumor = compile_aligner_step ~compiler tumor_t in
      (`Normal normal, `Tumor tumor, `Pipeline final_pipeline)
    | With_metadata (metadata_spec, p) ->
      let `Normal normal, `Tumor tumor, `Pipeline p =
        compile_bam_pair ~compiler p in

      (`Normal (apply_with_metadata ~metadata_spec normal),
       `Tumor (apply_with_metadata ~metadata_spec tumor),
       `Pipeline p)
    end

  let rec compile_variant_caller_step ~compiler (pipeline: vcf pipeline) =
    let {processors ; reference_build; work_dir; machine; _} = compiler in
    (* result prefix ignore optimizations *)
    let vcf_node =
      match pipeline with
      | Somatic_variant_caller (som_vc, bam_pair) ->
        let (`Normal normal, `Tumor tumor, `Pipeline new_bam_pair) =
          compile_bam_pair ~compiler bam_pair in
        let result_prefix =
          work_dir
          // to_file_prefix (Somatic_variant_caller (som_vc, new_bam_pair)) in
        dbg "Result_Prefix: %S" result_prefix;
        som_vc.Variant_caller.make_target ~processors
          ~run_with:machine ~input:(Variant_caller.Somatic {normal; tumor})
          ~result_prefix ()
      | Germline_variant_caller (gvc, bam) ->
        let result_prefix = work_dir // to_file_prefix pipeline in
        dbg "Result_Prefix: %S" result_prefix;
        let input_bam = compile_aligner_step ~compiler bam in
        gvc.Variant_caller.make_target ~processors
          ~run_with:machine ~input:(Variant_caller.Germline input_bam)
          ~result_prefix ()
      | With_metadata (metadata_spec, p) ->
        compile_variant_caller_step ~compiler p
        |> apply_with_metadata ~metadata_spec
    in
    compiler.wrap_vcf_node pipeline vcf_node

  let rec compile_gtf_step ~compiler (pipeline: gtf pipeline) =
    let {processors ; reference_build; work_dir; machine; _} = compiler in
    let result_prefix = work_dir // to_file_prefix pipeline in
    dbg "Result_Prefix: %S" result_prefix;
    let gtf_node =
      match pipeline with
      | Stringtie (configuration, bam) ->
        let bam = compile_aligner_step ~compiler bam in
        Stringtie.run ~processors ~configuration
          ~bam ~result_prefix ~run_with:machine ()
      | With_metadata (metadata_spec, p) ->
        compile_gtf_step ~compiler p
        |> apply_with_metadata ~metadata_spec
    in
    compiler.wrap_gtf_node pipeline gtf_node

  let rec seq2hla_hla_types_step ~compiler (pipeline : seq2hla_hla_types pipeline) =
    let { machine ; work_dir; _ } = compiler in
    match pipeline with
    | Seq2HLA sample ->
      begin match sample with
      | Paired_end_sample (info, l1, l2) ->
        (* TODO: Seq2HLA can actually take the gzipped version too, so we'd
           need a unique type for that. *)
        let r1 = fastq_step ~read:(`R1 info.fragment_id) ~compiler l1 in
        let r2 = fastq_step ~read:(`R2 info.fragment_id) ~compiler l2 in
        let work_dir = work_dir // (to_file_prefix pipeline) ^ "_work_dir" in
        Seq2HLA.hla_type
          ~work_dir ~run_with:machine ~run_name:info.fragment_id ~r1 ~r2
      | Bam_to_fastq(info_opt, how, what) -> 
        let fastq_pair = fastq_sample_step ~compiler sample in
        let bam_name = to_file_prefix what in 
        let sample_name = Option.value_map  ~default:bam_name info_opt ~f:(fun x -> x.sample_name) in
        let r1 = fastq_pair#product#r1 in
        let r2 = match fastq_pair#product#r2 with
          | Some r2 -> r2
          | _ -> failwithf "Seq2HLA doesn't support Single_end_sample(s)."
        in 
        Seq2HLA.hla_type
          ~work_dir ~run_with:machine ~run_name:sample_name ~r1 ~r2
      | _ -> failwithf "Seq2HLA doesn't support Single_end_sample(s)."
      end
    | With_metadata (metadata_spec, p) ->
      seq2hla_hla_types_step ~compiler p
      |> apply_with_metadata ~metadata_spec

  let rec optitype_hla_types_step ~compiler (pipeline : optitype_hla_types pipeline) =
    let { machine ; work_dir; _ } = compiler in
    match pipeline with
    | Optitype (kind, sample) ->
      begin match sample with
      | Paired_end_sample (info, l1, l2) ->
        let r1 = fastq_step ~read:(`R1 info.fragment_id) ~compiler l1 in
        let r2 = fastq_step ~read:(`R2 info.fragment_id) ~compiler l2 in
        let work_dir = work_dir // (to_file_prefix pipeline) ^ "_work_dir" in
        Optitype.hla_type
          ~work_dir ~run_with:machine ~run_name:info.fragment_id ~r1 ~r2 kind
      | _ -> failwithf "Seq2HLA doesn't support Single_end_sample(s)."
      end
    | With_metadata (metadata_spec, p) ->
      optitype_hla_types_step ~compiler p
      |> apply_with_metadata ~metadata_spec

end (* Compiler *)
