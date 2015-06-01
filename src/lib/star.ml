open Common
open Run_environment
open Workflow_utilities

let index
  ~reference_build
  ~processors
  ~(run_with : Machine.t) =
  let open KEDSL in
  let reference_fasta =
    Machine.get_reference_genome run_with reference_build
      |> Reference_genome.fasta in
  let star_tool = Machine.get_tool run_with Tool.Default.star in
  let name =
    sprintf "star-index-%s" (Filename.basename reference_fasta#product#path) in
  let reference_annotations =
    Machine.get_reference_genome run_with reference_build |> Reference_genome.gtf_exn in
  let reference_dir = (Filename.dirname reference_fasta#product#path) in
  let result_dir = sprintf "%s/star-index/" reference_dir in
  let suffix_array_result = result_dir // "SA" in
  workflow_node ~name
    (single_file ~host:(Machine.(as_host run_with)) suffix_array_result)
    ~edges:[
      on_failure_activate (Remove.directory ~run_with result_dir);
      depends_on reference_fasta;
      depends_on Tool.(ensure star_tool);
    ]
    ~tags:[Target_tags.aligner]
    ~make:(Machine.run_program run_with ~processors ~name
            Program.(
              Tool.(init star_tool)
              && shf "mkdir %s" result_dir 
              && shf "STAR --runMode genomeGenerate \
                --genomeDir %s \
                --genomeFastaFiles %s \
                --sjdbGTFfile %s \
                --runThreadN %d"
                result_dir
                (Filename.quote reference_fasta#product#path)
                (Filename.quote reference_annotations#product#path) 
                processors
           ))

let align 
  ~reference_build
  ~processors
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
  let star_tool = Machine.get_tool run_with Tool.Default.star in
  let star_index = index ~reference_build ~run_with ~processors in
  let reference_dir = (Filename.dirname reference_fasta#product#path) in
  let star_index_dir = sprintf "%s/star-index/" reference_dir in
  let result = sprintf "%sAligned.sortedByCoord.out.bam" result_prefix in
  let r1_path, r2_path_opt = fastq#product#paths in
  let name = sprintf "star-rna-align-%s" (Filename.basename r1_path) in
  let star_base_command = sprintf 
        "STAR --outSAMtype BAM SortedByCoordinate \
              --outSAMstrandField intronMotif \
              --outFilterIntronMotifs RemoveNoncanonical \
              --genomeDir %s \
              --runThreadN %d \
              --outFileNamePrefix %s \
              --readFilesIn %s"
               (Filename.quote star_index_dir)
               processors
               result_prefix
               (Filename.quote r1_path)
  in
  let base_star_target ~star_command = 
    workflow_node ~name
      (bam_file 
        ?sorting:(Some `Coordinate)
        ?contains:(Some `RNA)
        ~host:(Machine.(as_host run_with)) 
        result)
      ~edges:[
            on_failure_activate (Remove.file ~run_with result);
            depends_on reference_fasta;
            depends_on star_index;
            depends_on fastq;
            depends_on Tool.(ensure star_tool);
        ]
        ~tags:[Target_tags.aligner]
        ~make:(Machine.run_program run_with ~processors ~name
             Program.(
               Tool.(init star_tool)
               && in_work_dir
               && sh star_command 
           ))
  in
  match r2_path_opt with
  | Some read2 -> 
    let star_command =
      String.concat ~sep:" " [
        star_base_command;
        (Filename.quote read2);
      ] in
      base_star_target ~star_command
    | None -> 
      let star_command = star_base_command in
      base_star_target ~star_command