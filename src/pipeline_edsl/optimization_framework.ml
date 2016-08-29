open Nonstd

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
    fwd (Input.list (List.map ~f:bwd l))
  let list_map l ~f =
    fwd (Input.list_map (bwd l) (bwd f))

  let to_unit x = fwd (Input.to_unit (bwd x))

  let pair a b = fwd (Input.pair (bwd a) (bwd b))
  let pair_first x = fwd (Input.pair_first (bwd x))
  let pair_second x = fwd (Input.pair_second (bwd x))

  let input_url u = fwd (Input.input_url u)

  let fastq ~sample_name ?fragment_id ~r1 ?r2 () =
    let r2 = Option.map ~f:bwd r2 in
    fwd (Input.fastq ~sample_name ?fragment_id ~r1:(bwd r1) ?r2 ())

  let fastq_gz ~sample_name ?fragment_id ~r1 ?r2 () =
    let r1 = bwd r1 in
    let r2 = Option.map ~f:bwd r2 in
    fwd (Input.fastq_gz ~sample_name ?fragment_id ~r1 ?r2 ())

  let bam ~sample_name ?sorting ~reference_build input =
    fwd (Input.bam ~sample_name ?sorting ~reference_build (bwd input))

  let bed file =
    fwd (Input.bed (bwd file))

  let mhc_alleles =
    function
    | `File f -> fwd (Input.mhc_alleles (`File (bwd f)))
    | `Names _ as m -> fwd (Input.mhc_alleles m)

  let bwa_aln ?configuration ~reference_build fq =
    fwd (Input.bwa_aln ?configuration ~reference_build (bwd fq))

  let bwa_mem ?configuration ~reference_build fq =
    fwd (Input.bwa_mem ?configuration ~reference_build (bwd fq))

  let bwa_mem_opt ?configuration ~reference_build input =
    fwd (Input.bwa_mem_opt ?configuration ~reference_build
           (match input with
           | `Fastq f -> `Fastq (bwd f)
           | `Fastq_gz f -> `Fastq_gz (bwd f)
           | `Bam (b, p) -> `Bam (bwd b, p)))

  let star ?configuration ~reference_build fq =
    fwd (Input.star ?configuration ~reference_build (bwd fq))

  let hisat ?configuration ~reference_build fq =
    fwd (Input.hisat ?configuration ~reference_build (bwd fq))

  let mosaik ~reference_build fq =
    fwd (Input.mosaik ~reference_build (bwd fq))

  let stringtie ?configuration fq =
    fwd (Input.stringtie ?configuration (bwd fq))

  let gatk_indel_realigner ?configuration bam =
    fwd (Input.gatk_indel_realigner ?configuration (bwd bam))

  let gatk_indel_realigner_joint ?configuration pair =
    fwd (Input.gatk_indel_realigner_joint ?configuration (bwd pair))

  let picard_mark_duplicates ?configuration bam =
    fwd (Input.picard_mark_duplicates ?configuration (bwd bam))

  let gatk_bqsr ?configuration bam =
    fwd (Input.gatk_bqsr ?configuration (bwd bam))

  let seq2hla fq =
    fwd (Input.seq2hla (bwd fq))

  let optitype how fq =
    fwd (Input.optitype how (bwd fq))

  let hlarp input =
    fwd (Input.hlarp (match input with
      | `Seq2hla f -> `Seq2hla (bwd f)
      | `Optitype f -> `Optitype (bwd f)))

  let filter_to_region vcf bed =
    fwd (Input.filter_to_region (bwd vcf) (bwd bed))

  let gatk_haplotype_caller bam =
    fwd (Input.gatk_haplotype_caller (bwd bam))

  let gunzip gz =
    fwd (Input.gunzip (bwd gz))

  let gunzip_concat gzl =
    fwd (Input.gunzip_concat (bwd gzl))

  let concat l =
    fwd (Input.concat (bwd l))

  let merge_bams bl =
    fwd (Input.merge_bams (bwd bl))

  let bam_to_fastq ?fragment_id se_or_pe bam =
    fwd (Input.bam_to_fastq ?fragment_id se_or_pe (bwd bam))

  let mutect ?configuration ~normal ~tumor () =
    fwd (Input.mutect ?configuration ~normal:(bwd normal) ~tumor:(bwd tumor) ())
  let mutect2 ?configuration ~normal ~tumor () =
    fwd (Input.mutect2 ?configuration ~normal:(bwd normal) ~tumor:(bwd tumor) ())
  let somaticsniper ?configuration ~normal ~tumor () =
    fwd (Input.somaticsniper ?configuration ~normal:(bwd normal) ~tumor:(bwd tumor) ())
  let strelka ?configuration ~normal ~tumor () =
    fwd (Input.strelka ?configuration ~normal:(bwd normal) ~tumor:(bwd tumor) ())
  let varscan_somatic ?adjust_mapq ~normal ~tumor () =
    fwd (Input.varscan_somatic
           ?adjust_mapq ~normal:(bwd normal) ~tumor:(bwd tumor) ())
  let muse ?configuration ~normal ~tumor () =
    fwd (Input.muse ?configuration ~normal:(bwd normal) ~tumor:(bwd tumor) ())
  let virmid ?configuration ~normal ~tumor () =
    fwd (Input.virmid ?configuration ~normal:(bwd normal) ~tumor:(bwd tumor) ())

  let fastqc fq =
    fwd (Input.fastqc (bwd fq))
  let flagstat bam =
    fwd (Input.flagstat (bwd bam))

  let vcf_annotate_polyphen reference_build vcf =
    fwd (Input.vcf_annotate_polyphen reference_build (bwd vcf))
  let isovar ?configuration reference_build vcf bam =
    fwd (Input.isovar ?configuration reference_build (bwd vcf) (bwd bam))
  let topiary ?configuration reference_build vcf predictor alleles = 
    fwd (Input.topiary
           ?configuration reference_build (bwd vcf) predictor (bwd alleles))
  let vaxrank ?configuration reference_build vcfs bam predictor alleles =
    fwd (Input.vaxrank ?configuration reference_build
           (List.map ~f:(fun v -> (bwd v)) vcfs)
           (bwd bam) predictor (bwd alleles))
end
