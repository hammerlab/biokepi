(** 
   Top-level entry point into the library


   Biokepi provides different levels of abstraction to construct (Ketrew)
   pipelines:

   - {!Biokepi.EDSL}: very high-level embedded domain specific language that
   encode very consise complex pipelines and uses OCaml type-system to verify
   properties on them. The EDSL expressions can be “compiled” to Ketrew
   worfklows, to JSON, to Graphviz ["dot"] graphs, … {!Biokepi.Pipeline} is an
   earlier, soon to be deprecated, implementation of the same idea.
   - {!Biokepi.Tools}: is a lower-level library of functions producing
   Ketrew-workflow-nodes.

   The above modules use:
   
   - some extensions to the Ketrew workflow EDSL see {!Biokepi.KEDSL},
   - and an abstraction of the computing infrastructure of the user of the
   library: {!Biokepi.Machine}.

   Finally the {!Biokepi.Setup} module provides (optional) tools to create
   a proper {!Biokepi.Machine.t} that fits your computing environment (OS
   environment, cluster schedulers, installation of tools, fetching of
   reference data, etc.).


*)

(**
   The Embedded Bioinformatics Domain Specific Language

   This Embedded DSL is implemented following the “Typed Tagless Final
   Interpreter” method.

   It's usage is as follows:

   - Write EDSL expressions inside a functor taking the module type
   {!Biokepi.EDSL.Semantics} (i.e. the definition of the EDSL) as argument.
   Export some of them with the {!observe} function.
   - Apply the functor the desired “compilers/interpreters.” The interpreter
   can themselves be functors.

   Example: {[
     module Pipeline_1 (Bfx : Biokepi.EDSL.Semantics) = struct

       (* Reusable function withing the EDSL: *)
       let align_list_of_single_end_fastqs (l : string list) : [ `Bam ] Bfx.repr =
         let list_expression : [ `Fastq ] list Bfx.repr =
           List.map l ~f:(fun path ->
               (* create [ `Fastq ] repr term: *)
               Bfx.fastq ~sample_name:"Test" ~r1:path ())
           |> Bfx.list (* Assmble OCaml list into an EDSL list *)
         in
         let aligner : ([ `Fastq ] -> [ `Bam ]) Bfx.repr =
           (* create an EDSL-level function with `lambda`: *)
           Bfx.lambda (fun fq -> Bfx.bwa_aln ~reference_build:"hg19" fq)
         in
         (* Call the aligner on all fastq-terms and then merge the result
            into a single bam: *)
         Bfx.list_map list_expression ~f:aligner |> Bfx.merge_bams

       (* Function “exported” (to be used after compilation): *)
       let align_list l : [ `Bam ] Bfx.observation =
         Bfx.observe (fun () ->
             align_list_of_single_end_fastqs l
           )

     end
   ]}

    You can then compile this pipeline,
    (you can apply any sub-module of {!Biokepi.Compile}, with potential
    {!Biokepi.Transform} functors applied) for example to a dot-graph: {[
      let module Dotize_pipeline_1 =
        Pipeline_1(Biokepi.EDSL.Compile.To_dot) in
      let pipeline_1_dot = test_dir // "pipeline-1.dot" in
      write_file pipeline_1_dot
        ~content:(Dotize_pipeline_1.align_list [ (* FASTQS *) ];
      let pipeline_1_png = test_dir // "pipeline-1.png" in
      cmdf "dot -v -Tpng  %s -o %s" pipeline_1_dot pipeline_1_png;
    ]}

    Or reuse it in further pipelines: {[
      module Pipeline_2 (Bfx : Biokepi.EDSL.Semantics) = struct
        module P1 = Pipeline_1(Bfx)
        (* use the function  P1.align_list_of_single_end_fastqs *)
      end
    ]}

    See the {!TTfi_pipeline} test (["./src/test/ttfi_pipeline.ml"]) for more
    examples.

    This framework is also extensible, one can add new constructs to the
    language or new transformations while reusing most of the work already
    done.
  
*)
module EDSL = struct

  (** The definition of the Embedded DSL *)
  module type Semantics = Biokepi_pipeline_edsl.Semantics.Bioinformatics_base

  (** Various compilers to “interpret” the EDSL. *)
  module Compile = struct

    (** Compiler to [SmartPrint.t] displayable pseudo code,
        see the {{:https://github.com/clarus/smart-print}smart-print} library.
    *)
    module To_display = Biokepi_pipeline_edsl.To_display

    (** Compiler to Ketrew workflows using the {!Biokepi.Tools} implementations.

        The compiler is itself a functor, see the example:
        {[
          let module Workflow_compiler =
            Biokepi.EDSL.Compile.To_workflow.Make(struct
              let processors = 42
              let work_dir = "/work/dir/"
              let machine =
                Biokepi.Setup.Build_machine.create
                  "ssh://example.com/tmp/KT/"
            end)
          in
          let module Ketrew_pipeline_1 = Pipeline_1(Workflow_compiler) in
        ]}

    *)
    module To_workflow = Biokepi_pipeline_edsl.To_workflow

    (** Compiler to JSON ({!Yojson.Basic.t}). *)
    module To_json : Semantics.Bioinformatics_base
      with type 'a repr = var_count: int -> Yojson.Basic.json
       and
       type 'a observation = Yojson.Basic.json =
      Biokepi_pipeline_edsl.To_json

    (** Compiler to 
        {{:https://en.wikipedia.org/wiki/DOT_(graph_description_language)}DOT}
        graph descriptions.  *)
    module To_dot : Semantics.Bioinformatics_base
      with 
       type 'a observation = string =
      Biokepi_pipeline_edsl.To_dot
  end

  (** Transformations on the EDSL. *)
  module Transform = struct

    (** Apply as much EDSL functions as possible to their arguments (including
        [list_map]). *)
    module Apply_functions = Biokepi_pipeline_edsl.Transform_applications.Apply

  end

end

(** The description of the computing infrastructure used in Biokepi.

    The {!Biokepi_run_environment.Machine} provides an API to interact with the
    computing environment, it is used by all the programs in {!Biokepi.Tools}
    (and hence by the {!EDSL} through the {!EDSL.Compile.To_workflow}
    compiler). It is used to:

    - interact with cluster schedulers, by wrapping {!Ketrew} build-processes
    (YARN, LSF, etc.);
    - provide reference data (genomes, databases, etc.);
    - ensure the software used by the workflows is available for use.


    The user of the library has to provide (at least) one {!Biokepi.Machine.t}
    instance to run the workflows.
    The {!Biokepi.Setup} module provides an extensive set of defaults to
    simplify this.

*)
module Machine = Biokepi_run_environment.Machine

(** Help with the creation of {!Biokepi.Machine.t} instances. *)
module Setup = struct

  include Biokepi_environment_setup

end

(** Bioinformatics-specific extensions to the {!Ketrew.EDSL} module. *)
module KEDSL = Biokepi_run_environment.Common.KEDSL

(** Values describing the current version of the library. *)
module Metadata = Biokepi_run_environment.Metadata

(** Implementations of the Bioinformatics Ketrew workflow-nodes.

    This module provides a lower-level access to the bioinformatics tools to
    build Ketrew workflows.

    It is used by the EDSL (through {!Biokepi.EDSL.Compile.To_workflow}) but
    users are welcome to use it directly if needed.
*)
module Tools = Biokepi_bfx_tools

(** Earlier implementation of the Embedded DSL (kept for backwards
    compatibility).

    {!Biokepi.Pipeline.t} is a GADT describing high-level workflows.

    One defines pipelines using the {!Biokepi.Pipeline.Construct} module and
    compiles them to JSON or to Ketrew workflows.

    See ["src/pipeline_edsl/common_pipelines.ml"] and ["src/app/main.ml"]
    for examples.

*)
module Pipeline = Biokepi_pipeline_edsl.Pipeline
