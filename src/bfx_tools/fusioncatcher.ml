open Biokepi_run_environment
open Common

module Remove = Workflow_utilities.Remove


module Configuration = struct
  type t = {
    name : string;
    species: [`Homo_sapiens | `Mus_musculus]
  }
  let default_human = {name = "default-human"; species = `Homo_sapiens}
end

let run
  ~configuration
  ~fastq
  ~processors
  ~result_dir
  ~(run_with : Machine.t) =
  let open KEDSL in
  let species =  configuration.Configuration.species in
  let fusioncatcher_tool = Machine.get_tool run_with (`Fusioncatcher species) in
  let build_organism = 
    match species with 
      | `Homo_sapiens -> "homo_sapiens"
      | `Mus_musculus -> "mus_musculus"
  in
  let r1_path, r2_path_opt = fastq#product#paths in
  let input = 
    match r2_path_opt with 
    | Some r2_path -> (sprintf "%s,%s" r1_path r2_path)
    | _ -> r1_path
  in
  let name = sprintf "fusioncatcher-%s-%s" build_organism (Filename.basename r1_path) in
  let result_file = result_dir // "final-list_candidate-fusion-genes.txt" in 
  workflow_node ~name
    (single_file ~host:(Machine.(as_host run_with)) result_file)
    ~edges:[
      on_failure_activate (Remove.directory ~run_with result_dir);
      depends_on Machine.Tool.(ensure fusioncatcher_tool);
    ]
    ~make:(Machine.run_big_program run_with ~name
            Program.(
              Machine.Tool.(init fusioncatcher_tool)
              && shf "fusioncatcher -d ${FUSION_CATCHER_BUILD_DIR} -i %s -o %s -p %d" input result_dir processors
          ))