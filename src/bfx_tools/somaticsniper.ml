open Biokepi_run_environment
open Common

module Remove = Workflow_utilities.Remove


let default_prior_probability = 0.01
let default_theta = 0.85

module Configuration = struct

  type t = {
    name: string;
    prior_probability: float;
    theta: float
  }

  let to_json {name; prior_probability; theta}: Yojson.Basic.json =
    `Assoc [
      "name", `String name;
      "prior_probability", `Float prior_probability;
      "theta", `Float theta;
    ]
  let name {name; _} = name

  let render {name; prior_probability; theta}  =
    ["-s"; Float.to_string prior_probability;
     "-T"; Float.to_string theta]

  let default = {
    name = "default";
    prior_probability = default_prior_probability;
    theta = default_theta;
  }

end

let run
    ~run_with
    ?(configuration = Configuration.default) ~normal ~tumor ~result_prefix () =
  let open KEDSL in
  let result_file suffix = sprintf "%s-%s" result_prefix suffix in
  let name = sprintf "somaticsniper: %s" (result_file "") in
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
      ~self_ids:["somaticsniper"]
      ~name ~processors:1 Program.(
          Machine.Tool.init sniper
          && shf "mkdir -p %s" run_path
          && shf "cd %s" run_path
          && exec (
            ["somaticsniper"; "-F"; "vcf"]
            @ (Configuration.render configuration)
            @ ["-f";  reference_fasta#product#path;
               sorted_normal#product#path;
               sorted_tumor#product#path;
               output_file]
          ))
  in
  workflow_node ~name ~make
    (vcf_file output_file
       ~reference_build:normal#product#reference_build
       ~host:Machine.(as_host run_with))
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
