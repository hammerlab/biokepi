open Biokepi_run_environment
open Common

module Remove = Workflow_utilities.Remove

let index
    ~reference_build
    ~processors
    ~(run_with : Machine.t) =
  let open KEDSL in
  let reference_fasta =
    Machine.get_reference_genome run_with reference_build
    |> Reference_genome.fasta in
  let mosaik_tool = Machine.get_tool run_with Machine.Tool.Default.mosaik in
  let name =
    sprintf "mosaik-build-%s" (Filename.basename reference_fasta#product#path) in
  let index_result = sprintf "%s.mosaik.dat" reference_fasta#product#path in
  let jump_file_result = sprintf "%s.mosaik-index" reference_fasta#product#path in
  let mosaik_tmp_dir = (Filename.dirname reference_fasta#product#path) // "mosaik-tmp" in
  workflow_node ~name
    (single_file ~host:(Machine.(as_host run_with)) index_result)
    ~edges:[
      on_failure_activate (Remove.file ~run_with jump_file_result);
      on_failure_activate (Remove.file ~run_with index_result);
      depends_on reference_fasta;
      depends_on Machine.Tool.(ensure mosaik_tool);
    ]
    ~tags:[Target_tags.aligner]
    ~make:(Machine.run_big_program run_with ~processors ~name
             ~self_ids:["mosaik"; "index"]
             Program.(
               Machine.Tool.(init mosaik_tool)
               && shf "mkdir -p %s" mosaik_tmp_dir
               && shf "export MOSAIK_TMP=%s" mosaik_tmp_dir 
               (* Command to build basic MOSAIK reference file *)
               && shf "MosaikBuild -fr %s -oa %s"
                 reference_fasta#product#path
                 index_result
               (* Command to build MOSAIK index file *)
               && shf "MosaikJump -ia %s -hs 15 -out %s"
                 index_result
                 jump_file_result
             ))
  



let align 
    ~reference_build
    ~processors
    ~fastq
    ?(sequencer="illumina")
    ~(result_prefix:string)
    ~(run_with : Machine.t)
    () =
  let open KEDSL in
  let reference_fasta =
    Machine.get_reference_genome run_with reference_build
    |> Reference_genome.fasta in
  let jump_file_result = sprintf "%s.mosaik-index" reference_fasta#product#path in
  let index_result = sprintf "%s.mosaik.dat" reference_fasta#product#path in
  let in_work_dir =
    Program.shf "cd %s" Filename.(quote (dirname result_prefix)) in
  let mosaik_tool = Machine.get_tool run_with Machine.Tool.Default.mosaik in
  let mosaik_index = index ~reference_build ~run_with ~processors in
  let r1_path, r2_path_opt = fastq#product#paths in
  let name = sprintf "mosaik-align-%s" (Filename.basename r1_path) in
  let mka_out = sprintf "%s.mka" result_prefix in
  let mkb_out = sprintf "%s.mkb" result_prefix in
  let result = sprintf "%s.bam" mka_out in
  let mosaik_align_command = 
    sprintf "MosaikAligner \
      -in %s \
      -out %s \
      -ia %s \
      -j %s \
      -annpe $MOSAIK_PE_ANN \
      -annse $MOSAIK_SE_ANN"
      mkb_out
      mka_out
      index_result
      jump_file_result
  in
  let mosaik_build_command =
    (* Build MOSAIK specific input file *)
    let mosaik_build_base_command = 
        sprintf "MosaikBuild \
            -q %s \
            -st %s \
            -out %s"
            (Filename.quote r1_path)
            sequencer
            mkb_out
    in
    match r2_path_opt with
    | Some read2 -> 
      String.concat ~sep:" " [
        mosaik_build_base_command;
        "-q2"; (Filename.quote read2);
      ]
    | None -> mosaik_build_base_command  
  in   
  workflow_node ~name
      (bam_file ~reference_build ~host:(Machine.(as_host run_with)) result)
      ~edges:[
        on_failure_activate (Remove.file ~run_with result);
        depends_on reference_fasta;
        depends_on mosaik_index;
        depends_on fastq;
        depends_on Machine.Tool.(ensure mosaik_tool);
      ]
      ~tags:[Target_tags.aligner]
      ~make:(Machine.run_big_program run_with ~processors ~name
               ~self_ids:["mosaik"; "align"]
               Program.(
                 Machine.Tool.(init mosaik_tool)
                 && in_work_dir
                 && sh mosaik_build_command 
                 && sh mosaik_align_command
               ))
  
