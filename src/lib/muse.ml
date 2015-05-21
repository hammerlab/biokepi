(** Workflow-nodes to run {:http://bioinformatics.mdanderson.org/main/MuSE{MuSE}}. *)

open Common
open Run_environment
open Workflow_utilities

module Configuration = struct

  type t = {
    name: string;
    input_type: [ `WGS | `WES ];
  }

  let input_type_to_string input_type =
    match input_type with
    | `WGS -> "WGS"
    | `WES -> "WES"

  let to_json {name; input_type} =
    `Assoc [
      "name", `String name;
      "input-type", `String (input_type_to_string input_type);
    ]

  let default input_type =
    let is = input_type_to_string input_type in
    { name = "default-" ^ is; input_type}
    
  let wgs = default `WGS
  let wes = default `WES

end

let run
    ~reference_build ~configuration
    ~(run_with:Machine.t) ~normal ~tumor ~result_prefix how =
  let open KEDSL in
  let reference = Machine.get_reference_genome run_with reference_build in
  let muse_tool = Machine.get_tool run_with Tool.Default.muse in
  let muse_call_on_region region =
    let result_file suffix =
      let region_name = Region.to_filename region in
      sprintf "%s-%s%s" result_prefix region_name suffix in
    let intervals_option = Region.to_gatk_option region in
    let output_file_prefix = result_file "-out" in (* MuSE adds `.MuSE.txt` *)
    let output_file = output_file_prefix ^ ".MuSE.txt" in
    let run_path = Filename.dirname output_file in
    let fasta = Reference_genome.fasta reference in
    let fasta_dot_fai = Samtools.faidx ~run_with fasta in
    (* let sequence_dict = Picard.create_dict ~run_with fasta in *)
    let sorted_normal =
      Samtools.sort_bam_if_necessary
        ~processors:2 ~run_with ~by:`Coordinate normal in
    let sorted_tumor =
      Samtools.sort_bam_if_necessary
        ~processors:2 ~run_with ~by:`Coordinate tumor in
    let run_muse_call =
      let name = sprintf "muse-call-%s" (Filename.basename output_file) in
      let make =
        Machine.run_program run_with ~name Program.(
            Tool.(init muse_tool)
            && shf "mkdir -p %s" run_path
            && shf "$muse_bin call -f %s \
                    -r %s \
                    -O %s \
                    %s %s"
              fasta#product#path
              intervals_option
              output_file_prefix
              tumor#product#path
              normal#product#path
              (* yes, the help message says tumor then normal *)
          )
      in
      workflow_node ~name ~make
        (single_file output_file ~host:Machine.(as_host run_with))
        ~tags:[Target_tags.variant_caller; "muse"]
        ~edges:[
          depends_on Tool.(ensure muse_tool);
          depends_on sorted_normal;
          depends_on sorted_tumor;
          depends_on fasta;
          depends_on fasta_dot_fai;
          depends_on (Samtools.index_to_bai ~run_with sorted_normal);
          depends_on (Samtools.index_to_bai ~run_with sorted_tumor);
          on_failure_activate (Remove.file ~run_with output_file);
        ]
    in
    run_muse_call
  in
  let muse_sump call_file =
    let vcf_output = result_prefix ^ ".vcf" in
    let dbsnp = Reference_genome.dbsnp_exn reference in
    let name = sprintf "muse-sump-%s" (Filename.basename vcf_output) in
    let make =
      Machine.run_program run_with ~name Program.(
          Tool.(init muse_tool)
          && shf "$muse_bin sump -I %s \
                  %s \
                  -O %s \
                  -D %s"
              call_file#product#path
              Configuration.(match configuration.input_type with
                | `WGS -> "-G" | `WES -> "-E")
              vcf_output
              dbsnp#product#path
          )
      in
      workflow_node ~name ~make
        (single_file vcf_output ~host:Machine.(as_host run_with))
        ~tags:[Target_tags.variant_caller; "muse"]
        ~edges:[
          depends_on Tool.(ensure muse_tool);
          depends_on call_file;
          depends_on dbsnp;
          on_failure_activate (Remove.file ~run_with vcf_output);
        ]
  in
  begin match how with
  | `Region region ->
    muse_call_on_region region |> muse_sump
  | `Map_reduce ->
    let call_files =
      List.map (Region.major_contigs ~reference_build) ~f:muse_call_on_region
    in
    let concatenated =
      Cat.concat ~run_with call_files
        ~result_path:(result_prefix ^ "-concat.txt")
    in
    muse_sump concatenated
  end
