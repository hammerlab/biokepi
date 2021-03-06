open Biokepi_run_environment
open Common

module Remove = Workflow_utilities.Remove

module Filter = struct
  type t = [
    `String of string
  ]

  let of_string s =
    `String s

  let to_string f =
    match f with
    | `String s -> s

  module Defaults = struct
    let only_split_reads =
      of_string "cigar =~ /^.*N.*$/"

    let drop_split_reads =
      of_string "cigar =~ /^[0-9MIDSHPX=]*$/"
  end
end


(* Filter language syntax at
   https://github.com/lomereiter/sambamba/wiki/%5Bsambamba-view%5D-Filter-expression-syntax *)
let view ~(run_with : Machine.t) ~bam ~filter output_file_path =
  let open KEDSL in
  let name =
    sprintf "Sambamba.view %s %s"
      (Filename.basename bam#product#path) (Filter.to_string filter) in
  let clean_up = Remove.file ~run_with output_file_path in
  let reference_build = bam#product#reference_build in
  let product =
    KEDSL.bam_file
      ?sorting:bam#product#sorting
      ~reference_build
      ~host:(Machine.as_host run_with) output_file_path in
  let sambamba = Machine.get_tool run_with Machine.Tool.Default.sambamba in
  workflow_node product
    ~name
    ~make:(Machine.run_big_program run_with ~name
             ~self_ids:["sambamba"; "view"]
             Program.(
               Machine.Tool.(init sambamba)
               && shf "sambamba_v0.6.5 view --format=bam -F '%s' %s > %s"
                 (Filter.to_string filter)
                 bam#product#path
                 output_file_path
             ))
    ~edges:([
        depends_on Machine.Tool.(ensure sambamba);
        depends_on bam;
        on_failure_activate clean_up;])
