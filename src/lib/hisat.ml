open Common
open Run_environment
open Workflow_utilities

module Configuration = struct
  type t = {
    name : string;
    version : [`V_0_1_6_beta | `V_2_0_2_beta];
  }
  let to_json {name; version}: Yojson.Basic.json =
    `Assoc [
      "name", `String name;
      "version", 
        match version with 
        |`V_0_1_6_beta -> `String "V_0_1_6_beta"
        |`V_2_0_2_beta -> `String "V_2_0_2_beta"
      ;
    ]
  let default_v1 = {name = "default_v1"; version = `V_0_1_6_beta}
  let default_v2 = {name = "default_v2"; version = `V_2_0_2_beta}
end

let index
  ~reference_build
  ~index_prefix
  ~configuration
  ~(run_with : Machine.t) =
  let open KEDSL in
  let reference_fasta =
    Machine.get_reference_genome run_with reference_build
      |> Reference_genome.fasta in
  let result_dir = Filename.dirname index_prefix in
  let version = configuration.Configuration.version in
  let hisat_tool = Machine.get_tool run_with (`Hisat version) in
  let build_binary = 
    match version with
    | `V_0_1_6_beta -> "hisat-build"
    | `V_2_0_2_beta -> "hisat2-build"
  in
  let name = 
    sprintf "%s-%s" build_binary (Filename.basename reference_fasta#product#path) in
  let first_index_file = 
    match version with
      | `V_0_1_6_beta -> sprintf "%s.1.bt2" index_prefix
      | `V_2_0_2_beta -> sprintf "%s.1.ht2" index_prefix 
  in
  workflow_node ~name
    (single_file ~host:(Machine.(as_host run_with)) first_index_file)
    ~edges:[
      on_failure_activate (Remove.directory ~run_with result_dir);
      depends_on reference_fasta;
      depends_on Tool.(ensure hisat_tool);
    ]
    ~tags:[Target_tags.aligner]
    ~make:(Machine.run_big_program run_with ~name
            Program.(
              Tool.(init hisat_tool)
              && shf "mkdir %s" result_dir 
              && shf "%s %s %s"
                build_binary
                reference_fasta#product#path
                index_prefix
          ))

let align 
    ~reference_build
    ~processors
    ~configuration
    ~fastq
    ~(result_prefix:string)
    ~(run_with : Machine.t)
    () =
  let open KEDSL in
  let reference_fasta =
    Machine.get_reference_genome run_with reference_build
    |> Reference_genome.fasta in
  let reference_dir = (Filename.dirname reference_fasta#product#path) in
  let version = configuration.Configuration.version in
  let hisat_binary = 
    match version with
      | `V_0_1_6_beta -> "hisat"
      | `V_2_0_2_beta -> "hisat2"
  in
  let index_dir = sprintf "%s/%s-index/" reference_dir hisat_binary in
  let index_prefix = index_dir // (sprintf  "%s-index" hisat_binary) in
  let in_work_dir =
    Program.shf "cd %s" Filename.(quote (dirname result_prefix)) in
  let hisat_tool = Machine.get_tool run_with (`Hisat version) in
  let hisat_index = index ~index_prefix ~reference_build ~run_with ~configuration in
  let result = sprintf "%s.sam" result_prefix in
  let r1_path, r2_path_opt = fastq#product#paths in
  let name = sprintf "%s-rna-align-%s" hisat_binary (Filename.basename r1_path) in
  let hisat_base_command = sprintf 
      "%s \
       -p %d \
       -x %s \
       -S %s"
      hisat_binary
      processors
      (Filename.quote index_prefix)
      (Filename.quote result)
  in
  let base_hisat_target ~hisat_command = 
    workflow_node ~name
      (single_file
         ~host:(Machine.(as_host run_with)) 
         result)
      ~edges:[
        on_failure_activate (Remove.file ~run_with result);
        depends_on reference_fasta;
        depends_on hisat_index;
        depends_on fastq;
        depends_on Tool.(ensure hisat_tool);
      ]
      ~tags:[Target_tags.aligner]
      ~make:(Machine.run_big_program run_with ~processors ~name
               Program.(
                 Tool.(init hisat_tool)
                 && in_work_dir
                 && sh hisat_command 
               ))
  in
  match r2_path_opt with
  | Some read2 -> 
    let hisat_command =
      String.concat ~sep:" " [
        hisat_base_command;
        "-1"; (Filename.quote r1_path);
        "-2"; (Filename.quote read2);
      ] in
    base_hisat_target ~hisat_command
  | None -> 
    let hisat_command = String.concat ~sep:" " [
        hisat_base_command;
        "-U"; (Filename.quote r1_path);
      ] in
    base_hisat_target ~hisat_command
