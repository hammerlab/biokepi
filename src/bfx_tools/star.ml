open Biokepi_run_environment
open Common

module Remove = Workflow_utilities.Remove


module Configuration = struct

  module Align = struct
    type t = {
      name: string;

      (** The mapping quality MAPQ (column 5) is 255 for uniquely mapping reads,
          and int(-10*log10(1- 1/Nmap)) for multi-mapping reads. This scheme is
          same as the one used by TopHat and is compatible with Cufflinks. The
          default MAPQ=255 for the unique mappers maybe changed with to an
          integer between 0 and 255 to ensure compatibility with downstream tools
          such as GATK. *)
      sam_mapq_unique: int option;

      (** Specifies the length of the genomic sequence around the annotated
          junction to be used in constructing the splice junctions
          database. Ideally, this length should be equal to the ReadLength-1,
          where ReadLength is the length of the reads. *)
      overhang_length: int option;
      parameters: (string * string) list;
    }
    let name t = t.name

    let default = {
      name = "default";
      sam_mapq_unique = None;
      overhang_length = None;
      parameters = [];
    }

    let to_json t: Yojson.Basic.json =
      let {name;
           sam_mapq_unique;
           overhang_length;
           parameters} = t in
      `Assoc [
        "name", `String name;
        "sam_mapq_unique",
        (match sam_mapq_unique with
        | None -> `Null
        | Some x -> `Int x);
        "overhang_length",
        (match overhang_length with
        | None -> `Null
        | Some x -> `Int x);
        "parameters",
        `Assoc (List.map parameters ~f:(fun (a, b) -> a, `String b));
      ]

    let render {name;
                sam_mapq_unique;
                overhang_length;
                parameters} =
      (match overhang_length with
      | None -> ""
      | Some x ->
        sprintf "--sjdbOverhang %d" x
      ) ::
      (match sam_mapq_unique with
      | None -> ""
      | Some x ->
        if 0 > x || x > 255
        then failwith "STAR Align sam_mapq_unique must be between 0 and 255"
        else ();
        sprintf "--outSAMmapqUnique %d" x) ::
      List.concat_map parameters ~f:(fun (a, b) -> [a; b])
  end
end

let index
    ~reference_build
    ~processors
    ~(run_with : Machine.t) =
  let open KEDSL in
  let reference_fasta =
    Machine.get_reference_genome run_with reference_build
    |> Reference_genome.fasta in
  let star_tool = Machine.get_tool run_with Machine.Tool.Default.star in
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
      depends_on Machine.Tool.(ensure star_tool);
    ]
    ~tags:[Target_tags.aligner]
    ~make:(Machine.run_big_program run_with ~processors ~name
             ~self_ids:["star"; "index"]
             Program.(
               Machine.Tool.(init star_tool)
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
    ?(configuration=Configuration.Align.default)
    () =
  let open KEDSL in
  let reference_fasta =
    Machine.get_reference_genome run_with reference_build
    |> Reference_genome.fasta in
  let in_work_dir =
    Program.shf "cd %s" Filename.(quote (dirname result_prefix)) in
  let star_tool = Machine.get_tool run_with Machine.Tool.Default.star in
  let star_index = index ~reference_build ~run_with ~processors in
  let reference_dir = (Filename.dirname reference_fasta#product#path) in
  let star_index_dir = sprintf "%s/star-index/" reference_dir in
  (* STAR appends Aligned.sortedByCoord.out.bam to the filename *)
  let result = sprintf "%sAligned.sortedByCoord.out.bam" result_prefix in
  let r1_path, r2_path_opt = fastq#product#paths in
  let name = sprintf "star-rna-align-%s" (Filename.basename r1_path) in
  let star_base_command = sprintf
      "STAR --outSAMtype BAM SortedByCoordinate \
       --outSAMstrandField intronMotif \
       --outSAMattributes NH HI NM MD \
       --outFilterIntronMotifs RemoveNoncanonical \
       --genomeDir %s \
       --runThreadN %d \
       --outFileNamePrefix %s \
       --outSAMattrRGline %s \
       %s \
       --readFilesIn %s"
      (Filename.quote star_index_dir)
      processors
      result_prefix
      (sprintf "ID:%s SM:\"%s\""
         (Filename.basename r1_path)
         fastq#product#sample_name)
      (Configuration.Align.render configuration
       |> String.concat ~sep:" ")
      (Filename.quote r1_path)
  in
  let base_star_target ~star_command =
    workflow_node ~name
      (bam_file
         ~sorting:`Coordinate
         ~host:(Machine.(as_host run_with))
         ~reference_build
         result)
      ~edges:[
        on_failure_activate (Remove.file ~run_with result);
        depends_on reference_fasta;
        depends_on star_index;
        depends_on fastq;
        depends_on Machine.Tool.(ensure star_tool);
      ]
      ~tags:[Target_tags.aligner]
      ~make:(Machine.run_big_program run_with ~processors ~name
               ~self_ids:["star"; "align"]
               Program.(
                 Machine.Tool.(init star_tool)
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
