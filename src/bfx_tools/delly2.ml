open Biokepi_run_environment
open Common

module Remove = Workflow_utilities.Remove


module Configuration = struct
  type t = {
    name: string;
    parameters: (string * string) list
  }

  let default = {
    name = "default";
    parameters = []
  }

  let to_json {name; parameters} : Yojson.Basic.json =
    `Assoc [
      "name", `String name;
      "parameters", `Assoc
        (List.map ~f:(fun (k,v) -> k, `String v) parameters)
    ]

  let render {name; parameters} =
    List.concat_map ~f:(fun (k,v) -> [k; v]) parameters

  let name {name; _ } = name
end


let run_somatic
    ~run_with ?(configuration=Configuration.default)
    ~normal ~tumor
    ~output_path
  =
  let open KEDSL in
  let name = sprintf "Delly2: %s" (Filename.basename output_path) in
  let delly2 = Machine.get_tool run_with Machine.Tool.Default.delly2 in
  let reference_genome =
    Machine.get_reference_genome run_with normal#product#reference_build
    |> Reference_genome.fasta in
  let sorted_normal =
    Samtools.sort_bam_if_necessary ~run_with ~by:`Coordinate normal in
  let sorted_tumor =
    Samtools.sort_bam_if_necessary ~run_with ~by:`Coordinate tumor in
  let make =
    Machine.run_big_program run_with
      ~self_ids:["delly2"]
      ~name
      Program.(
        Machine.Tool.init delly2
        && shf "delly2 call -t DEL -g %s -o %s %s %s"
          reference_genome#product#path
          output_path
          sorted_tumor#product#path sorted_normal#product#path
      )
  in
  workflow_node ~name ~make
    (single_file ~host:Machine.(as_host run_with) output_path)
    ~tags:["delly2"; Target_tags.variant_caller]
    ~edges:[
      depends_on (Machine.Tool.ensure delly2);
      depends_on sorted_normal;
      depends_on sorted_tumor;
      depends_on (Samtools.index_to_bai ~run_with sorted_normal);
      depends_on (Samtools.index_to_bai ~run_with sorted_tumor);
      depends_on reference_genome;
      on_failure_activate (Remove.file ~run_with output_path)
    ]

