
open Biokepi_run_environment
open Common



(* See http://sourceforge.net/p/virmid/wiki/Home/ *)
module Configuration = struct

  type t = {
    name: string;
    parameters: (string * string) list
  }
  let to_json {name; parameters}: Yojson.Basic.json =
    `Assoc [
      "name", `String name;
      "parameters",
      `Assoc (List.map parameters ~f:(fun (a, b) -> a, `String b));
    ]
  let render {parameters; _} =
    List.concat_map parameters ~f:(fun (a,b) -> [a; b])

  let default =
    {name = "default"; parameters = []}

  let example_1 =
    (* The first one: http://sourceforge.net/p/virmid/wiki/Home/#examples *)
    {name= "examplel_1";
     parameters = [
       "-c1", "20";
       "-C1", "100";
       "-c2", "20";
       "-C2", "100";
     ]}

  let name t = t.name
end

let run
    ~run_with ~normal ~tumor ~result_prefix
    ?(more_edges = []) ~configuration () =
  let open KEDSL in
  let open Configuration in 
  let name = Filename.basename result_prefix in
  let result_file suffix = result_prefix ^ suffix in
  let output_file = result_file "-somatic.vcf" in
  let output_prefix = "virmid-output" in
  let work_dir = result_file "-workdir" in
  let reference_fasta =
    Machine.get_reference_genome run_with normal#product#reference_build
    |> Reference_genome.fasta in
  let virmid_tool = Machine.get_tool run_with Machine.Tool.Default.virmid in
  let virmid_somatic_broken_vcf =
    (* maybe it's actually not broken, but later tools can be
       annoyed by the a space in the header. *)
    work_dir // Filename.basename tumor#product#path ^ ".virmid.som.passed.vcf" in
  let processors = Machine.max_processors run_with in
  let make =
    Machine.run_big_program run_with ~name ~processors
      ~self_ids:["virmid"]
      Program.(
        Machine.Tool.init virmid_tool
        && shf "mkdir -p %s" work_dir
        && sh (String.concat ~sep:" " ([
            "java -jar $VIRMID_JAR -f";
            "-w"; work_dir;
            "-R"; reference_fasta#product#path;
            "-D"; tumor#product#path;
            "-N"; normal#product#path;
            "-t"; Int.to_string processors;
            "-o"; output_prefix;
          ] @ Configuration.render configuration))
        && shf "sed 's/\\(##INFO.*Number=A,Type=String,\\) /\\1/' %s > %s"
          virmid_somatic_broken_vcf output_file
          (* We construct the `output_file` out of the “broken” one with `sed`. *)
      )
  in
  workflow_node ~name ~make
    (single_file output_file ~host:(Machine.as_host run_with))
    ~edges:(more_edges @ [
        depends_on normal;
        depends_on tumor;
        depends_on reference_fasta;
        depends_on (Machine.Tool.ensure virmid_tool);
      ])
