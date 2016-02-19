open Common
open Run_environment
open Workflow_utilities

module Configuration = struct
  type t = {
    name : string;
    use_reference_gtf : bool;
  }
  let to_json {name; use_reference_gtf}: Yojson.Basic.json =
    `Assoc [
      "name", `String name;
      "use-reference-gtf", `Bool use_reference_gtf;
    ]
  let default = {name = "default"; use_reference_gtf = true}
end

let run
    ~reference_build
    ~configuration
    ~(run_with:Machine.t)
    ~processors
    ~bam
    ~result_prefix () =
  let open KEDSL in
  let name = sprintf "stringtie-%s" (Filename.basename result_prefix) in
  let result_file suffix = result_prefix ^ suffix in
  let output_dir = result_file "-stringtie_output" in
  let output_file_path = output_dir // "output.gtf" in
  let reference_fasta =
    Machine.get_reference_genome run_with reference_build
    |> Reference_genome.fasta in
  let sorted_bam =
    Samtools.sort_bam_if_necessary ~run_with ~by:`Coordinate bam in
  let reference_annotations =
    let genome = Machine.get_reference_genome run_with reference_build in
    let gtf = Reference_genome.gtf genome in
    let use_the_gtf = configuration.Configuration.use_reference_gtf in
    match use_the_gtf, gtf with
    | true, Some some -> Some some
    | false, _ -> None
    | true, None ->
      failwithf "Stringtie: use_reference_gtf is `true` but the genome %s \
                 does not provide one (use another genome or allow this to run \
                 without GTF by giving another Configuration.t)"
        (Reference_genome.name genome)
  in
  let stringtie_tool = Machine.get_tool run_with Tool.Default.stringtie in
  let make =
    let reference_annotations_option =
      Option.value_map ~default:"" reference_annotations
        ~f:(fun o -> sprintf "-G %s" Filename.(quote o#product#path)) in
    Machine.run_program run_with ~name ~processors
      Program.(
        Tool.init stringtie_tool
        && shf "mkdir -p %s" output_dir
        && shf "stringtie %s \
                -p %d \
                %s \
                -o %s \
               "
          sorted_bam#product#path
          processors
          reference_annotations_option
          output_file_path
      )
  in
  let edges =
    Option.value_map ~default:[] reference_annotations
      ~f:(fun o -> [depends_on o])
    @ [
      depends_on sorted_bam;
      depends_on reference_fasta;
      depends_on (Tool.ensure stringtie_tool);
      depends_on (Samtools.index_to_bai ~run_with sorted_bam);
    ]
  in
  workflow_node ~name ~make ~edges
    (single_file output_file_path ~host:(Machine.as_host run_with))

