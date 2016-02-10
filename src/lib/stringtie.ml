open Common
open Run_environment
open Workflow_utilities

type gtf_usage_preference = [`Yes | `No | `If_available ]
let run
    ~reference_build
    ?(use_reference_gtf : gtf_usage_preference =`If_available)
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
  let reference_annotations =
    let ref_gen = Machine.get_reference_genome run_with reference_build in
    match use_reference_gtf, ref_gen |> Reference_genome.gtf with
    | `Yes, Some some | `If_available, Some some -> Some some
    | `No, _ | `If_available, None -> None
    | `Yes, None ->
      failwithf "Stringtie: use_reference_gtf is `Yes but the genome %s \
                 does not provide one (use another genome or allow this to run \
                 without GTF by giving `No or `If_available)"
        (Reference_genome.name ref_gen)
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
          bam#product#path
          processors
          reference_annotations_option
          output_file_path
      )
  in
  let edges =
    Option.value_map ~default:[] reference_annotations
      ~f:(fun o -> [depends_on o])
    @ [
      depends_on bam;
      depends_on reference_fasta;
      depends_on (Tool.ensure stringtie_tool);
      depends_on (Samtools.index_to_bai ~run_with bam);
    ]
  in
  workflow_node ~name ~make ~edges
    (single_file output_file_path ~host:(Machine.as_host run_with))
