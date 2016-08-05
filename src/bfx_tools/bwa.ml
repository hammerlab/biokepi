open Biokepi_run_environment
open Common

module Remove = Workflow_utilities.Remove

module Configuration = struct

  let default_gap_open_penalty = 11
  let default_gap_extension_penalty = 4

  module Common = struct
    type t = {
      name: string;
      gap_open_penalty: int;
      gap_extension_penalty: int;
    }
    let default = {
      name = "default";
      gap_open_penalty = default_gap_open_penalty;
      gap_extension_penalty = default_gap_extension_penalty;
    }
    let name t = t.name
    let to_json {name; gap_open_penalty; gap_extension_penalty} =
      `Assoc [
        "name", `String name;
        "gap_open_penalty", `Int gap_open_penalty;
        "gap_extension_penalty", `Int gap_extension_penalty;
      ]
  end
  module Aln = Common
  module Mem = Common

end


let index
    ~reference_build
    ~(run_with : Machine.t) =
  let open KEDSL in
  let reference_fasta =
    Machine.get_reference_genome run_with reference_build
    |> Reference_genome.fasta in
  (* `bwa index` creates a bunch of files, c.f.
     [this question](https://www.biostars.org/p/73585/) we detect the
     `.bwt` one. *)
  let bwa_tool = Machine.get_tool run_with Machine.Tool.Default.bwa in
  let name =
    sprintf "bwa-index-%s" (Filename.basename reference_fasta#product#path) in
  let result = sprintf "%s.bwt" reference_fasta#product#path in
  workflow_node ~name
    (single_file ~host:(Machine.(as_host run_with)) result)
    ~edges:[
      on_failure_activate (Remove.file ~run_with result);
      depends_on reference_fasta;
      depends_on Machine.Tool.(ensure bwa_tool);
    ]
    ~tags:[Target_tags.aligner]
    ~make:(Machine.run_big_program run_with ~name
             ~self_ids:["bwa"; "index"]
             Program.(
               Machine.Tool.(init bwa_tool)
               && shf "bwa index %s"
                 (Filename.quote reference_fasta#product#path)))

let read_group_header_option algorithm ~sample_name ~read_group_id =
  (* this option should magically make the sam file compatible
           mutect and other GATK-like pieces of software
           http://seqanswers.com/forums/showthread.php?t=17233

           The `LB` one seems “necessary” for somatic sniper:
           `[bam_header_parse] missing LB tag in @RG lines.`
  *)
  match algorithm with
  |`Mem ->
    sprintf "-R '@RG\\tID:%s\\tSM:%s\\tLB:ga\\tPL:Illumina'" read_group_id sample_name
  |`Aln ->
    sprintf "-r '@RG\\tID:%s\\tSM:%s\\tLB:ga\\tPL:Illumina'" read_group_id sample_name

let mem_align_to_sam
    ~reference_build
    ?(configuration = Configuration.Mem.default)
    ~fastq
    (* ~(r1: KEDSL.single_file KEDSL.workflow_node) *)
    (* ?(r2: KEDSL.single_file KEDSL.workflow_node option) *)
    ~(result_prefix:string)
    ~(run_with : Machine.t)
    () =
  let open KEDSL in
  let reference_fasta =
    Machine.get_reference_genome run_with reference_build
    |> Reference_genome.fasta in
  let in_work_dir =
    Program.shf "cd %s" Filename.(quote (dirname result_prefix)) in
  (* `bwa index` creates a bunch of files, c.f.
     [this question](https://www.biostars.org/p/73585/) we detect the
     `.bwt` one. *)
  let bwa_tool = Machine.get_tool run_with Machine.Tool.Default.bwa in
  let bwa_index = index ~reference_build ~run_with in
  let result = sprintf "%s.sam" result_prefix in
  let r1_path, r2_path_opt = fastq#product#paths in
  let name = sprintf "bwa-mem-%s" (Filename.basename r1_path) in
  let processors = Machine.max_processors run_with in
  let bwa_base_command =
    String.concat ~sep:" " [
      "bwa mem";
      (read_group_header_option `Mem
         ~sample_name:fastq#product#escaped_sample_name
         ~read_group_id:(Filename.basename r1_path));
      "-t"; Int.to_string processors;
      "-O"; Int.to_string configuration.Configuration.Mem.gap_open_penalty;
      "-E"; Int.to_string configuration.Configuration.Mem.gap_extension_penalty;
      (Filename.quote reference_fasta#product#path);
      (Filename.quote r1_path);
    ] in
  let bwa_base_target ~bwa_command  =
    workflow_node
      (single_file result ~host:Machine.(as_host run_with))
      ~name
      ~edges:(
        depends_on Machine.Tool.(ensure bwa_tool)
        :: depends_on bwa_index
        :: depends_on fastq
        :: on_failure_activate (Remove.file ~run_with result)
        :: [])
      ~tags:[Target_tags.aligner]
      ~make:(Machine.run_big_program run_with ~processors ~name
               ~self_ids:["bwa"; "mem"]
               Program.(
                 Machine.Tool.(init bwa_tool)
                 && in_work_dir
                 && sh bwa_command))
  in
  match r2_path_opt with
  | Some read2 ->
    let bwa_command =
      String.concat ~sep:" " [
        bwa_base_command;
        (Filename.quote read2);
        ">"; (Filename.quote result);
      ] in
    bwa_base_target ~bwa_command
  | None ->
    let bwa_command =
      String.concat ~sep:" " [
        bwa_base_command;
        ">"; (Filename.quote result);
      ] in
    bwa_base_target ~bwa_command



let align_to_sam
    ~reference_build
    ?(configuration = Configuration.Aln.default)
    ~fastq
    ~(result_prefix:string)
    ~(run_with : Machine.t)
    () =
  let open KEDSL in
  let reference_fasta =
    Machine.get_reference_genome run_with reference_build
    |> Reference_genome.fasta in
  let in_work_dir =
    Program.shf "cd %s" Filename.(quote (dirname result_prefix)) in
  (* `bwa index` creates a bunch of files, c.f.
     [this question](https://www.biostars.org/p/73585/) we detect the
     `.bwt` one. *)
  let bwa_tool = Machine.get_tool run_with Machine.Tool.Default.bwa in
  let bwa_index = index ~reference_build ~run_with in
  let processors = Machine.max_processors run_with in
  let bwa_aln read_number read =
    let name = sprintf "bwa-aln-%s" (Filename.basename read) in
    let result = sprintf "%s-R%d.sai" result_prefix read_number in
    let bwa_command =
      String.concat ~sep:" " [
        "bwa aln";
        "-t"; Int.to_string processors;
        "-O"; Int.to_string configuration.Configuration.Aln.gap_open_penalty;
        "-E"; Int.to_string configuration.Configuration.Aln.gap_extension_penalty;
        (Filename.quote reference_fasta#product#path);
        (Filename.quote read);
        ">"; (Filename.quote result);
      ] in
    workflow_node
      (single_file result ~host:Machine.(as_host run_with))
      ~name
      ~edges:[
        depends_on fastq;
        depends_on bwa_index;
        depends_on Machine.Tool.(ensure bwa_tool);
        on_failure_activate (Remove.file ~run_with result);
      ]
      ~tags:[Target_tags.aligner]
      ~make:(Machine.run_big_program run_with ~processors ~name
               ~self_ids:["bwa"; "aln"]
               Program.(
                 Machine.Tool.(init bwa_tool)
                 && in_work_dir
                 && sh bwa_command
               ))
  in
  let r1_path, r2_path_opt = fastq#product#paths in
  let r1_sai = bwa_aln 1 r1_path in
  let r2_sai_opt = Option.map r2_path_opt ~f:(fun r -> (bwa_aln 2 r, r)) in
  let sam =
    let name = sprintf "bwa-sam-%s" (Filename.basename result_prefix) in
    let result = sprintf "%s.sam" result_prefix in
    let program, edges =
      let common_edges = [
        depends_on r1_sai;
        depends_on reference_fasta;
        depends_on bwa_index;
        depends_on Machine.Tool.(ensure bwa_tool);
        on_failure_activate (Remove.file ~run_with result);
      ] in
      match r2_sai_opt with
      | Some (r2_sai, r2) ->
        Program.(
          Machine.Tool.(init bwa_tool)
          && in_work_dir
          && shf "bwa sampe %s %s %s %s %s %s > %s"
            (read_group_header_option `Aln
               ~sample_name:fastq#product#escaped_sample_name
               ~read_group_id:(Filename.basename r1_path))
            (Filename.quote reference_fasta#product#path)
            (Filename.quote r1_sai#product#path)
            (Filename.quote r2_sai#product#path)
            (Filename.quote r1_path)
            (Filename.quote r2)
            (Filename.quote result)),
        (depends_on r2_sai :: common_edges)
      | None ->
        Program.(
          Machine.Tool.(init bwa_tool)
          && in_work_dir
          && shf "bwa samse %s %s %s > %s"
            (read_group_header_option `Aln
               ~sample_name:fastq#product#escaped_sample_name
               ~read_group_id:(Filename.basename r1_path))
            (Filename.quote reference_fasta#product#path)
            (Filename.quote r1_sai#product#path)
            (Filename.quote result)),
        common_edges
    in
    workflow_node
      (single_file result ~host:Machine.(as_host run_with))
      ~name ~edges
      ~tags:[Target_tags.aligner]
      ~make:(Machine.run_big_program run_with ~processors:1 ~name  program
               ~self_ids:["bwa"; "sampe"])
  in
  sam

module Input_reads = struct
  type t = [
    | `Fastq of KEDSL.fastq_reads KEDSL.workflow_node
    | `Bam of KEDSL.bam_file KEDSL.workflow_node * [ `PE | `SE ]
  ]
  let prepare ~run_with =
    function
    | `Fastq _ as f -> f
    | `Bam (b, p) ->
      `Bam (Samtools.sort_bam_if_necessary ~run_with ~by:`Read_name b, p)

  let name =
    function
    | `Fastq f -> f#product#paths |> fst |> Filename.basename
    | `Bam (b, _) ->
      b#product#path  |> Filename.basename

  let sample_name =
    function
    | `Fastq f -> f#product#escaped_sample_name
    | `Bam (b, _) -> b#product#escaped_sample_name
  let read_group_id =
    name (* Temporary? this is the backwards compatible choice *)

  let as_dependencies =
    function
    | `Fastq f -> [KEDSL.depends_on f]
    | `Bam (b, _) -> [KEDSL.depends_on b]

end

(**
   Call ["bwa_mem"] with potentially a bam of a FASTQ (pair).

   In the case of bams the command looks like
   ["samtools bam2fq | bwa mem ... | samtools view -b | samtools sort"].


   It is considered an experimental optimization.

   Cf. also this {{:https://sourceforge.net/p/bio-bwa/mailman/message/30527151/}message}.
*)
let mem_align_to_bam
    ~reference_build
    ?(configuration = Configuration.Mem.default)
    ~(result_prefix:string)
    ~(run_with : Machine.t)
    (raw_input : Input_reads.t) =
  let open KEDSL in
  let reference_fasta =
    Machine.get_reference_genome run_with reference_build
    |> Reference_genome.fasta in
  let in_work_dir =
    Program.shf "cd %s" Filename.(quote (dirname result_prefix)) in
  let bwa_tool = Machine.get_tool run_with Machine.Tool.Default.bwa in
  let samtools = Machine.get_tool run_with Machine.Tool.Default.samtools in
  let bwa_index = index ~reference_build ~run_with in
  let result = sprintf "%s.bam" result_prefix in
  let input = Input_reads.prepare ~run_with raw_input in
  let name = sprintf "bwa-mem-%s" (Input_reads.name input) in
  let processors = Machine.max_processors run_with in
  let bwa_base_command =
    String.concat ~sep:" " [
      "bwa mem";
      (read_group_header_option `Mem
         ~sample_name:(Input_reads.sample_name input)
         ~read_group_id:(Input_reads.read_group_id input));
      "-t"; Int.to_string processors;
      "-O"; Int.to_string configuration.Configuration.Mem.gap_open_penalty;
      "-E"; Int.to_string configuration.Configuration.Mem.gap_extension_penalty;
      (Filename.quote reference_fasta#product#path);
    ] in
  let command_to_stdout =
    match input with
    | `Fastq fastq ->
      let r1_path, r2_path_opt = fastq#product#paths in
      String.concat ~sep:" " [
        bwa_base_command;
        Filename.quote r1_path;
        Option.value_map ~default:"" r2_path_opt ~f:Filename.quote;
      ]
    | `Bam (b, pairedness) ->
      String.concat ~sep:" " [
        "samtools bam2fq"; Filename.quote b#product#path;
        "|";
        bwa_base_command; (match pairedness with `PE -> "-p" | `SE -> "");
        "-";
      ]
  in
  let command =
    String.concat ~sep:" " [
      command_to_stdout; "|";
      "samtools"; "view"; "-b"; "|";
      "samtools"; "sort";
      "-@"; Int.to_string processors; "-T"; Filename.chop_extension result ^ "-tmp";
      "-o"; Filename.quote result;
    ] in
  let product =
    bam_file ~host:Machine.(as_host run_with)
      ~name:(Input_reads.sample_name input)
      ~sorting:`Coordinate
      ~reference_build result in
  workflow_node product
    ~name
    ~edges:(
      depends_on Machine.Tool.(ensure bwa_tool)
      :: depends_on Machine.Tool.(ensure samtools)
      :: depends_on bwa_index
      :: on_failure_activate (Remove.file ~run_with result)
      :: Input_reads.as_dependencies input
    )
    ~tags:[Target_tags.aligner]
    ~make:(Machine.run_big_program run_with ~processors ~name
             ~self_ids:["bwa"; "mem"]
             Program.(
               Machine.Tool.(init bwa_tool)
               && Machine.Tool.(init samtools)
               && in_work_dir
               && sh "set -o pipefail"
               && sh command))
