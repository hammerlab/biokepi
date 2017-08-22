open Biokepi_run_environment
open Common

module Configuration = struct

  type t = {
    name: string;
    min_reads: int;
    protein_sequence_length: int;
    parameters: (string * string) list;
  }
  let to_json {name; min_reads; protein_sequence_length; parameters}: Yojson.Basic.json =
    `Assoc [
      "name", `String name;
      "min_reads", `Int min_reads;
      "protein_sequence_length", `Int protein_sequence_length;
      "parameters",
      `Assoc (List.map parameters ~f:(fun (a, b) -> a, `String b));
    ]
  let render {parameters; _} =
    List.concat_map parameters ~f:(fun (a,b) -> [a; b])

  let default =
    {name = "default"; min_reads = 2; protein_sequence_length = 30; parameters = []}

  let name t = t.name
end

let run ~(run_with: Machine.t)
  ~configuration
  ~reference_build ~vcf ~bam ~output_file =
  let open KEDSL in
  let isovar_tool = Machine.get_tool run_with Machine.Tool.Default.isovar in
  let genome = Machine.(get_reference_genome run_with reference_build)
    |> Reference_genome.name
  in
  let min_reads = configuration.Configuration.min_reads in
  let protein_sequence_length = configuration.Configuration.protein_sequence_length in
  let name = sprintf "isovar_%s" (Filename.basename output_file) in
  let command_parts = [
    "isovar-protein-sequences.py";
    "--vcf"; vcf#product#path;
    "--bam"; bam#product#path;
    "--genome"; genome;
    "--min-reads"; string_of_int min_reads;
    "--protein-sequence-length"; string_of_int protein_sequence_length;
    "--output"; output_file; ] @ Configuration.render configuration
  in
  let isovar_cmd = String.concat ~sep:" " command_parts in
  workflow_node (single_file output_file ~host:Machine.(as_host run_with))
    ~name
    ~edges:[
      depends_on Machine.Tool.(ensure isovar_tool);
      depends_on (Pyensembl.cache_genome ~run_with ~reference_build);
      depends_on vcf;
      depends_on bam;
    ]
    ~make:(
      Machine.run_program run_with ~name
        Program.(
          Machine.Tool.(init isovar_tool)
          && Pyensembl.(set_cache_dir_command ~run_with)
          && sh isovar_cmd
        )
    )