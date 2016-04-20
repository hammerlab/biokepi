
(**
   QueL-like Compiler Optimization Framework


   This is “stolen” from ["quel_o.ml"] …
   i.e. the “Query optimization framework in tagless-final.”

   The code is reusable for any optimization pass.

*)

module type Trans_base = sig
  type 'a from
  type 'a term
  val fwd : 'a from -> 'a term (* reflection *)
  val bwd : 'a term -> 'a from (* reification *)
end

module type Transformation = sig
  include Trans_base
  val fmap  : ('a from -> 'b from) -> ('a term -> 'b term)
  val fmap2 : ('a from -> 'b from -> 'c from) ->
    ('a term -> 'b term -> 'c term)
end

(** Add the default implementations of the mapping functions *)
module Define_transformation(X : Trans_base) = struct
  include X
  let fmap  f term        = fwd (f (bwd term))
  let fmap2 f term1 term2 = fwd (f (bwd term1) (bwd term2))
end

(** The default, generic optimizer.

    Concrete optimizers are built by overriding the interpretations
    of some DSL forms.

    See the module {!Transform_applications} for an example of use.
*)
module Generic_optimizer
    (X: Transformation)
    (Input: Semantics.Bioinformatics_base with type 'a repr = 'a X.from)
  : Semantics.Bioinformatics_base
    with type 'a repr = 'a X.term
     and type 'a observation = 'a Input.observation
= struct
  module Tools = Biokepi_bfx_tools
  open X
  type 'a repr = 'a term
  type 'a observation  = 'a Input.observation

  let lambda f = fwd (Input.lambda (fun x -> bwd (f (fwd x))))
  let apply e1 e2 = fmap2 Input.apply e1 e2
  let observe f =
    Input.observe (fun () -> bwd (f ()))

  let list l =
    fwd (Input.list (List.map bwd l))
  let list_map l ~f =
    fwd (Input.list_map (bwd l) (bwd f))

  let fastq ~sample_name ?fragment_id ~r1 ?r2 () =
    fwd (Input.fastq ~sample_name ?fragment_id ~r1 ?r2 ())

  let fastq_gz ~sample_name ?fragment_id ~r1 ?r2 () =
    fwd (Input.fastq_gz ~sample_name ?fragment_id ~r1 ?r2 ())

  let bam ~path ?sorting ~reference_build () =
    fwd (Input.bam ~path ?sorting ~reference_build ())

  let bwa_aln ?configuration ~reference_build fq =
    fwd (Input.bwa_aln ?configuration ~reference_build (bwd fq))

  let bwa_mem ?configuration ~reference_build fq =
    fwd (Input.bwa_mem ?configuration ~reference_build (bwd fq))

  let gunzip gz =
    fwd (Input.gunzip (bwd gz))

  let gunzip_concat gzl =
    fwd (Input.gunzip_concat (bwd gzl))

  let concat l =
    fwd (Input.concat (bwd l))

  let merge_bams bl =
    fwd (Input.merge_bams (bwd bl))

  let mutect ~configuration ~normal ~tumor =
    fwd (Input.mutect ~configuration ~normal:(bwd normal) ~tumor:(bwd tumor))

end
