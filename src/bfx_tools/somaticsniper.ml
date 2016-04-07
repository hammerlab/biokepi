open Biokepi_run_environment
open Common

module Remove = Workflow_utilities.Remove


let default_prior_probability = 0.01
let default_theta = 0.85

let run
    ~run_with ?minus_T ?minus_s ~normal ~tumor ~result_prefix () =
  let open KEDSL in
  let name =
    "somaticsniper"
    ^ Option.(value_map minus_s ~default:"" ~f:(sprintf "-s%F"))
    ^ Option.(value_map minus_T ~default:"" ~f:(sprintf "-T%F"))
  in
  let result_file suffix = sprintf "%s-%s%s" result_prefix name suffix in
  let sniper = Machine.get_tool run_with Machine.Tool.Default.somaticsniper in
  let reference_fasta =
    Machine.get_reference_genome run_with normal#product#reference_build
    |> Reference_genome.fasta in
  let output_file = result_file "-snvs.vcf" in
  let run_path = Filename.dirname output_file in
  let sorted_normal =
    Samtools.sort_bam_if_necessary ~run_with ~by:`Coordinate normal in
  let sorted_tumor =
    Samtools.sort_bam_if_necessary ~run_with ~by:`Coordinate tumor in
  let make =
    Machine.run_big_program run_with
      ~name ~processors:1 Program.(
          Machine.Tool.init sniper
          && shf "mkdir -p %s" run_path
          && shf "cd %s" run_path
          && exec (
            ["somaticsniper"; "-F"; "vcf"]
            @ Option.(
                value_map minus_s ~default:[]
                  ~f:(fun f -> ["-s"; Float.to_string f])
              )
            @ Option.(
                value_map minus_T ~default:[]
                  ~f:(fun f -> ["-T"; Float.to_string f])
              )
            @ ["-f";  reference_fasta#product#path;
               sorted_normal#product#path;
               sorted_tumor#product#path;
               output_file]
          ))
  in
  workflow_node ~name ~make
    (single_file output_file ~host:Machine.(as_host run_with))
    ~metadata:(`String name)
    ~tags:[Target_tags.variant_caller; "somaticsniper"]
    ~edges:[
      depends_on (Machine.Tool.ensure sniper);
      depends_on sorted_normal;
      depends_on sorted_tumor;
      depends_on (Samtools.index_to_bai ~run_with sorted_normal);
      depends_on (Samtools.index_to_bai ~run_with sorted_tumor);
      depends_on reference_fasta;
      on_failure_activate ( Remove.file ~run_with output_file );
    ]
