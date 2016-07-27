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

type fastq_workflow_node = KEDSL.fastq_reads KEDSL.workflow_node
type bam_workflow_node = KEDSL.bam_file KEDSL.workflow_node
type input_reads = [ `Fastq of fastq_workflow_node  | `Bam of bam_workflow_node]

let mem_align_to_sam
    ~reference_build
    ?(configuration = Configuration.Mem.default)
    ~(input_reads:input_reads)
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
  let task_name ~sample_name = sprintf "bwa-mem-%s" sample_name in
  let processors = Machine.max_processors run_with in
  let bwa_base_command ?read_group_arg () =
    String.concat ~sep:" " [
      "bwa mem";
      Option.value ~default:"" read_group_arg;
      "-t"; Int.to_string processors;
      "-O"; Int.to_string configuration.Configuration.Mem.gap_open_penalty;
      "-E"; Int.to_string configuration.Configuration.Mem.gap_extension_penalty;
      (Filename.quote reference_fasta#product#path);
    ] in
  let bwa_base_target ~name ~bwa_command ~source  = 
    workflow_node
      (single_file result ~host:Machine.(as_host run_with))
      ~name
      ~edges:(
        depends_on Machine.Tool.(ensure bwa_tool)
        :: depends_on bwa_index
        :: depends_on source
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
  match input_reads with
  | `Bam bam_file ->
    let name = task_name bam_file#product#escaped_sample_name in
    let bwa_command =
      String.concat ~sep:" " [
        bwa_base_command ();
        "-B"; (Filename.quote bam_file#product#path);
        ">"; (Filename.quote result);
      ] in
    bwa_base_target ~name ~source:bam_file ~bwa_command
  | `Fastq fastq ->
    let r1_path, r2_path_opt = fastq#product#paths in
    let read_group_arg = 
      (read_group_header_option `Mem 
         ~sample_name:fastq#product#escaped_sample_name
         ~read_group_id:(Filename.basename r1_path));
    in
    let name = task_name fastq#product#escaped_sample_name in 
    match r2_path_opt with
    | Some read2 -> 
      let bwa_command =
        String.concat ~sep:" " [
          bwa_base_command ~read_group_arg ();
          (Filename.quote r1_path);
          (Filename.quote read2);
          ">"; (Filename.quote result);
        ] in
      bwa_base_target ~name ~bwa_command ~source:fastq
    | None -> 
      let bwa_command =
        String.concat ~sep:" " [
          bwa_base_command ~read_group_arg ();
          (Filename.quote r1_path);
          ">"; (Filename.quote result);
        ] in
      bwa_base_target ~name ~bwa_command ~source:fastq



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
