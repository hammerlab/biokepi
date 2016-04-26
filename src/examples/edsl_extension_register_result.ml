(*M
## Example Extension: “Register Product”

This example is an extension to the language in `Biokepi.EDSL.Semantics`,
that registers some files in a database. 

The author of the pipeline registers the results that they consider
“interesting” thanks to new function that we will add to the EDSL.
 

The only requirement is the `biokepi` library:
M*)
#use "topfind";;
#thread;;
#require "biokepi";;
(*M

### Prerequisite: Ketrew Node to Insert in Database

This module provides a function that creates a Ketrew workflow-node that
writes some pipeline information into a Sqlite3 database.

M*)
module FDB = struct
  open Printf
  let register_in_db ~results_db_uri ~machine ~path ~json ~ketrew_id ~edges =
    let sql = sprintf "\
      CREATE TABLE IF NOT EXISTS my_results (
        path text, ketrew_node_id text, pipeline_json text
      );
      INSERT INTO my_results (path, ketrew_node_id, pipeline_json)
                      VALUES (%s, %s, %s);
      "
        (Filename.quote path)
        (Filename.quote ketrew_id)
        (Filename.quote json)
    in
    let name = sprintf "Register %s" (Filename.basename path) in
    let open Ketrew.EDSL in
    let make =
      Biokepi.Machine.quick_run_program machine Program.(
          shf "echo %s | sqlite3 %s"
            (Filename.quote sql)
            (Filename.quote results_db_uri)
        ) in
    workflow_node without_product ~name ~make ~edges
end
(*M
### Definition Of The Extension

The definition of the “typing” part of the extension is as easy as including
the parent module type and adding a new function type.

M*)
module type Semantics_with_registration = sig
  include Biokepi.EDSL.Semantics
  val register: string -> 'a repr -> 'a repr
  (** [register name p] registers the result of the pipeline [p] in the
      database (using [name] to distinguish pipelines) and returns an
      equivalent pipeline.  *)
end
(*M

### Example of Use

Before even thinking of compiling, we can write pipelines using the new function.

M*)
module Example_pipeline (Registrable_bfx: Semantics_with_registration) = struct
  (* We can reuse pipelines writen with the non-extended EDSL. *)
  module Library = Biokepi.EDSL.Library.Make(Registrable_bfx)

  open Registrable_bfx

  let example ~normal ~tumor =
    let align fastq =
      list_map fastq ~f:(lambda (fun fq ->
          bwa_mem fq ~reference_build:"b37"
          |> picard_mark_duplicates
            ~configuration:Biokepi.Tools.Picard.Mark_duplicates_settings.default
        )) in
    let normal_bam = align normal |> merge_bams in
    let tumor_bam = align tumor |> merge_bams in
    let bam_pair = pair normal_bam tumor_bam in
    let indel_realigned_pair =
      gatk_indel_realigner_joint bam_pair
        ~configuration: Biokepi.Tools.Gatk.Configuration.default_indel_realigner
    in
    let final_normal_bam =
      pair_first indel_realigned_pair
      |> gatk_bqsr
        ~configuration: Biokepi.Tools.Gatk.Configuration.default_bqsr
      |> register "final-normal-bam"
    in
    let final_tumor_bam =
      pair_second indel_realigned_pair
      |> gatk_bqsr
        ~configuration: Biokepi.Tools.Gatk.Configuration.default_bqsr
      |> register "final-normal-bam"
    in
    mutect ~normal:final_normal_bam ~tumor:final_tumor_bam
      ~configuration:Biokepi.Tools.Mutect.Configuration.default
    |> register "mutect-vcf"

  let run ~normal ~tumor =
    observe (fun () ->
        example
          ~normal:(Library.fastq_of_input normal)
          ~tumor:(Library.fastq_of_input tumor)
      )
end
(*M

The value `example` gets the expected type:

```ocaml
val example:
   normal:[ `Fastq ] list Registrable_bfx.repr ->
   tumor:[ `Fastq ] list Registrable_bfx.repr ->
   [ `Vcf ] Registrable_bfx.repr
```

### Compiler to Graphviz

We “inherit” the implementation of the parent EDSL (through `include`), and
then we implement the extension.

We decide to hide the `register` step from the displayed graph.
M*)
module To_dot_with_register = struct
  include Biokepi.EDSL.Compile.To_dot
  let register _ x = x
end
(*M
### Compiler To Json

This interpreter is slightly more complex; in order to save it the database, we
want to save the intermediate JSON structure for later insertion in the
database.

We functorize the module to provide such an infrastructure (assuming the
`Mem.add` registers the tuple `(metadata, json)` as a side-effect).

Again we `include` the `To_json` compiler from Biokepi, and add the function
`register`.
M*)
module To_json_with_register (Memory: sig 
    val add : string -> Yojson.Basic.json -> unit
  end) = struct
  include Biokepi.EDSL.Compile.To_json
  let register metadata x ~var_count =
    let compiled = x ~var_count in
    Memory.add metadata compiled;
    compiled
end

(*M
### Compiler To Ketrew Workflow

Here is where the magic happens. As above, we `include` the compiler from the
library to get the previously defined implementations.

This time we expect an interface providing `Mem.look_up` to find a previously
saved JSON datastructure.

The function `register` creates an intermediary Ketrew workflow-node.

- It provides the same product as the argument of the `register` function
  (child node).
- It has the parameter ``~equivalence:`None`` to make sure Ketrew does not
  merge it with the child node.
- We force the condition of the node to be “never already done:”
  ``~done_when:`Never``.
- It has two dependencies: the child node and the actual registration in the
  database (from above `FDB.register`).ster`).


M*)
module To_workflow_with_register 
    (Mem : sig val look_up: string -> Yojson.Basic.json end)
    (Config : sig
       include Biokepi.EDSL.Compile.To_workflow.Compiler_configuration
       val results_db_uri : string
     end)
= struct
  include Biokepi.EDSL.Compile.To_workflow.Make(Config)
  open Biokepi.EDSL.Compile.To_workflow.File_type_specification
  let register : type a . string -> a t -> a t = fun metadata x ->
    let make_node any_node =
      let registration =
        let path = any_node#product#path in
        let ketrew_id = Ketrew.EDSL.node_id any_node in
        let json = Mem.look_up metadata in
        FDB.register_in_db ~machine:Config.machine ~path
          ~results_db_uri:Config.results_db_uri
          ~ketrew_id ~json:(Yojson.Basic.pretty_to_string json)
          ~edges:[Ketrew.EDSL.depends_on any_node]
      in
      let new_node =
        let open Ketrew.EDSL in
        workflow_node any_node#product ~equivalence:`None
          ~done_when:`Never
          ~name:(Printf.sprintf "Parent-of-registration: %s -> %s"
                   metadata
                   (Filename.basename any_node#product#path))
          ~edges:[
            depends_on registration;
            depends_on any_node;
          ]
      in
      new_node
    in
    match x with
    | Bam wf -> Bam (make_node wf)
    | Vcf wf -> Vcf (make_node wf)
    | other -> failwith "To_workflow.register non-{Bam or Vcf}: not implemented"
end
(*M

### Displaying and Running The Example-Pipeline

Here prepare the above pipeline for display and submission to the Ketrew
server.

  M*)
open Nonstd
let run
    ~machine ~work_dir ?(processors = 2)
    ?(sqlite_db = "/tmp/biokepi-test.sqlite") ~normal ~tumor () =
(*M
We output a PNG graph of the pipeline:
M*)
  let module Dotize_pipeline =
    Example_pipeline(To_dot_with_register) in
  let sm_dot = Dotize_pipeline.run ~normal ~tumor in
  ignore (
    sprintf "echo %s > /tmp/example.dot ; \
             dot -v -x /tmp/example.dot -Tpng -o example.png"
      (sm_dot |> SmartPrint.to_string 72 2 |> Filename.quote)
    |> Sys.command
  );
(*M
This is the module that we pass to both `To_json_with_register` and
to `To_workflow_with_register` to record the transformations to JSON.
It is just an association list:
M*)
  let module Mem = struct
    let json_metadata = ref []
    let add m x = json_metadata := (m, x) :: !json_metadata
    let display () =
      List.iter !json_metadata ~f:(fun (m, x) ->
          printf "%s:\n%s\n%!" m (Yojson.Basic.pretty_to_string x)
        );
      ()
    let look_up s =
      List.find_map !json_metadata ~f:(fun (x, j) ->
          if x = s then Some j else None)
      |> Option.value_exn ~msg:(sprintf "Can't find metadata: %S" s)
  end in
(*M
Here is the compilation to JSON:<br/>
We discard the actual result since we are interested only in the JSON
values saved by the `Mem` module:
M*)
  let module Jsonize_pipeline =
    Example_pipeline(To_json_with_register(Mem)) in
  let _ = Jsonize_pipeline.run ~normal ~tumor in
(*M
And finaly we compile the pipeline to a Ketrew workflow, and return it:
M*)
  let module Workflow_compiler =
    To_workflow_with_register 
      (Mem)
      (struct
        include Biokepi.EDSL.Compile.To_workflow.Defaults
        let processors = processors
        let work_dir = work_dir
        let machine = machine
        let results_db_uri = sqlite_db
      end)
  in
  let module Ketrew_pipeline = Example_pipeline(Workflow_compiler) in
  let workflow =
    Ketrew_pipeline.run ~normal ~tumor
  in
  workflow |> Biokepi.EDSL.Compile.To_workflow.File_type_specification.get_vcf

(*M
### Submitting The Workflow

The above `run` function is parametrized over the computing infrastructure and
the input data:

```ocaml
val run:
  machine:Biokepi.Machine.t ->
  work_dir:string ->
  ?processors:int ->
  ?sqlite_db:string ->
  normal:Biokepi.EDSL.Library.Input.t ->
  tumor:Biokepi.EDSL.Library.Input.t ->
  unit ->
  single_file workflow_node
```

Here is an example of use, each user's infrastructure/setup will be different
though:

M*)
let () = printf "Let's Gooo!\n%!";;
(*M

Import my infrastructure:
M*)
#use "my_cluster.ml";;
(*M
The above script has to provide the following API:

```ocaml
module My_cluster: sig
  val machine : Biokepi.Machine.t
  val datasets_home : string
  val max_processors : int
  val work_dir : string
end
```

We test the pipeline starting from two small bams:
M*)
let (//) = Filename.concat
let training_normal_bam = My_cluster.datasets_home // "training-dream/normal.chr20.bam"
let training_tumor_bam = My_cluster.datasets_home // "training-dream/tumor.chr20.bam"
(*M
We “encode” them into the structure expected by `Biokepi.EDSL.Library` (which
we use above).
M*)
let normal =
  Biokepi.EDSL.Library.Input.(
    fastq_sample "normal-1" [
      of_bam `PE training_normal_bam ~reference_build:"b37";
    ]
  )
let tumor =
  Biokepi.EDSL.Library.Input.(
    fastq_sample "tumor-1" [
      of_bam `PE training_tumor_bam ~reference_build:"b37";
    ]
  )
(*M

We submit the workflow to the Ketrew server (here using the `default`
configuration):
M*)
let () =
  match Sys.argv.(1) with
  | "run" ->
    run ~machine:My_cluster.machine
      ~work_dir:My_cluster.work_dir
      ~sqlite_db:(My_cluster.work_dir // "test-biokepi.sqlite") ()
      ~processors:My_cluster.max_processors
      ~normal ~tumor
    |> Ketrew.Client.submit_workflow
  | other -> printf "usage: <script> run\n%!"; exit 1
  | exception _ ->  printf "usage: <script> run\n%!"; exit 0
(*M
We just run the script:

    $ ocaml src/examples/edsl_extension_register_result.ml run

Here is the resulting `example.png`:

<div>
<a href="https://cloud.githubusercontent.com/assets/617111/14832405/21bc1394-0bc8-11e6-8bf1-c97eb5a0a224.png">
<img width="90%" src="https://cloud.githubusercontent.com/assets/617111/14832405/21bc1394-0bc8-11e6-8bf1-c97eb5a0a224.png">
</a>
</div>

And after a succesful run one can use:

```
$ sqlite3 $WORK_DIR/test-biokepi.sqlite
SQLite version 3.7.17 2013-05-20 00:56:22
Enter ".help" for instructions
Enter SQL statements terminated with a ";"
sqlite> .schema my_results
CREATE TABLE my_results (
        path text, ketrew_node_id text, pipeline_json text
        );
sqlite> SELECT path,ketrew_node_id from my_results;
...
...
```

M*)
