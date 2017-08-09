open Common

type name = string

module Specification = struct
  module Location = struct
    type t = [
      | `Url of string
      | `Vcf_concat of (string * t) list (* name × location *)
      | `Concat of t list
      | `Gunzip of t (* Should this be guessed from the URL extension? *)
      | `Bunzip2 of t
      | `Untar of t
    ]
    let url u = `Url u
    let vcf_concat l = `Vcf_concat l
    let concat l = `Concat l
    let gunzip l = `Gunzip l
    let bunzip2 l = `Bunzip2 l
    let untar l = `Untar l
  end

  type t = {
    name: string;
    ensembl: int;
    species: string;
    metadata: string option;
    fasta: Location.t;
    dbsnp: Location.t option;
    known_indels: Location.t option;
    cosmic: Location.t option;
    exome_gtf: Location.t option; (* maybe desrves a better name? *)
    cdna: Location.t option;
    whess: Location.t option;
    major_contigs: string list option;
  }

  let create
      ?metadata
      ~fasta
      ~ensembl
      ~species
      ?dbsnp
      ?known_indels
      ?cosmic
      ?exome_gtf
      ?cdna
      ?whess
      ?major_contigs
      name = {
    name;
    ensembl;
    species;
    metadata;
    fasta;
    dbsnp;
    known_indels;
    cosmic;
    exome_gtf;
    cdna;
    whess;
    major_contigs;
  }

module Default = struct

  let major_contigs_b37 =
    List.init 22 (fun i -> sprintf "%d" (i + 1))
    @ ["X"; "Y"; "MT";]
  let major_contigs_hg_family =
    List.init 22 (fun i -> sprintf "chr%d" (i + 1))
    @ [
      "chrX";
      "chrY";
      "chrM";
    ]
  let major_contigs_mm10 =
    List.init 19 (fun i -> sprintf "%d" (i + 1))
    @ [ "X"; "Y" ]

  module Name = struct
    let b37 = "b37"
    let b37decoy = "b37decoy"
    let b38 = "b38"
    let hg38 = "hg38"
    let hg18 = "hg18"
    let hg19 = "hg19"
    let mm10 = "mm10"
  end


  (* Used by both B37 and B37decoy *)
  let b37_dbsnp_url =
    "https://storage.googleapis.com/hammerlab-biokepi-data/raw_data/\
     dbsnp_138.b37.vcf.gz"
  let b37_cosmic_url =
    "http://www.broadinstitute.org/cancer/cga/sites/default/files/\
     data/tools/mutect/b37_cosmic_v54_120711.vcf"
  let b37_exome_gtf_url =
    "http://ftp.ensembl.org/pub/release-75/gtf/\
     homo_sapiens/Homo_sapiens.GRCh37.75.gtf.gz"
  let b37_cdna_url =
    "http://ftp.ensembl.org/pub/release-75/fasta/homo_sapiens/cdna/\
     Homo_sapiens.GRCh37.75.cdna.all.fa.gz"
  let b37_whess_url =
    "ftp://genetics.bwh.harvard.edu/pph2/whess/\
     polyphen-2.2.2-whess-2011_12.sqlite.bz2"

  let b37_known_indels_url =
    "https://storage.googleapis.com/hammerlab-biokepi-data/raw_data/\
     Mills_and_1000G_gold_standard.indels.b37.vcf.gz"


  let human = "homo sapiens"
  let mouse = "mus musculus"

  let b37 =
    create Name.b37
      ~species:human
      ~ensembl:75
      ~metadata:"Provided by the Biokepi library"
      ~major_contigs:major_contigs_b37
      ~fasta:Location.(
          url "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/\
               b37/human_g1k_v37.fasta.gz"
          |> gunzip)
      ~dbsnp:Location.(url b37_dbsnp_url |> gunzip)
      (* Alternate?
         "ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606/VCF/v4.0/00-All.vcf.gz"
      *)
      ~known_indels:Location.(url b37_known_indels_url |> gunzip)
      ~cosmic:Location.(url b37_cosmic_url)
      ~exome_gtf:Location.(url b37_exome_gtf_url |> gunzip)
      ~cdna:Location.(url b37_cdna_url |> gunzip)
      ~whess:Location.(url b37_whess_url |> bunzip2)

  let b37decoy =
    create Name.b37decoy
      ~species:human
      ~ensembl:75
      ~metadata:"Provided by the Biokepi library"
      ~major_contigs:major_contigs_b37
      ~fasta:Location.(
          url
            "ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/reference/\
             phase2_reference_assembly_sequence/hs37d5.fa.gz"
          |> gunzip)
      ~dbsnp:Location.(url b37_dbsnp_url |> gunzip)
      ~known_indels:Location.(url b37_known_indels_url |> gunzip)
      ~exome_gtf:Location.(url b37_exome_gtf_url |> gunzip)
      ~cosmic:Location.(url b37_cosmic_url)
      ~cdna:Location.(url b37_cdna_url |> gunzip)
      ~whess:Location.(url b37_whess_url |> bunzip2)

  let hg38 =
    (* Release 87 *)
    let hg38_url =
      "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/hg38/hg38bundle/Homo_sapiens_assembly38.fasta.gz" in
    let dbsnp_hg38 =
      "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/hg38/hg38bundle/Homo_sapiens_assembly38.dbsnp.vcf.gz" in
    let known_indels_hg38 =
      "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/hg38/hg38bundle/Homo_sapiens_assembly38.known_indels.vcf.gz" in
    create Name.hg38
      ~species:human
      ~ensembl:87
      ~metadata:"Provided by the Biokepi library"
      ~major_contigs:major_contigs_hg_family
      ~fasta:Location.(url hg38_url|> gunzip)
      ~dbsnp:Location.(url dbsnp_hg38 |> gunzip)
      ~known_indels:Location.(url known_indels_hg38 |> gunzip)

  let b38 =
    (* Release 87 *)
    let b38_url =
      "http://ftp.ensembl.org/pub/release-87/fasta/homo_sapiens/dna/\
       Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz" in
    let gtf_b38_url =
      "http://ftp.ensembl.org/pub/release-87/gtf/homo_sapiens/\
       Homo_sapiens.GRCh38.87.gtf.gz" in
    let cdna_b38_url =
      "http://ftp.ensembl.org/pub/release-87/fasta/homo_sapiens/cdna/\
       Homo_sapiens.GRCh38.cdna.all.fa.gz" in
    let dbsnp_url =
      "http://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606_b150_GRCh38p7/VCF/common_all_20170710.vcf.gz" in
    create Name.b38
      ~species:human
      ~ensembl:87
      ~metadata:"Provided by the Biokepi library"
      ~major_contigs:major_contigs_b37
      ~fasta:Location.(url b38_url |> gunzip)
      ~exome_gtf:Location.(url gtf_b38_url |> gunzip)
      ~dbsnp:Location.(url dbsnp_url |> gunzip)
      ~cdna:Location.(url cdna_b38_url |> gunzip)

  let hg18 =
    let hg18_url =
      "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/\
       hg18/Homo_sapiens_assembly18.fasta.gz" in
    let dbsnp_hg18_url =
      "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/\
       hg18/dbsnp_138.hg18.vcf.gz" in
    create Name.hg18
      ~ensembl:54
      ~species:human
      ~metadata:"Provided by the Biokepi library"
      ~major_contigs:major_contigs_hg_family
      ~fasta:Location.(url hg18_url|> gunzip)
      ~dbsnp:Location.(url dbsnp_hg18_url |> gunzip)

  let hg19 =
    let hg19_url =
      "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/\
       hg19/ucsc.hg19.fasta.gz" in
    let dbsnp_hg19_url =
      "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/\
       hg19/dbsnp_138.hg19.vcf.gz" in
    let known_indels_hg19_url =
      "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/\
        hg19/Mills_and_1000G_gold_standard.indels.hg19.sites.vcf.gz" in
    create Name.hg19
      ~ensembl:75
      ~species:human
      ~metadata:"Provided by the Biokepi library"
      ~major_contigs:major_contigs_hg_family
      ~fasta:Location.(url hg19_url|> gunzip)
      ~dbsnp:Location.(url dbsnp_hg19_url |> gunzip)
      ~known_indels:Location.(url known_indels_hg19_url |> gunzip)
      ~whess:Location.(url b37_whess_url |> bunzip2)

  let mm10 =
    let mm10_url =
      "https://storage.googleapis.com/hammerlab-biokepi-data/raw_data/\
       mm10.GRCm38.dna_sm.fa" in
    let dbsnp_mm10_snps_url =
      "ftp://ftp-mouse.sanger.ac.uk/REL-1303-SNPs_Indels-GRCm38/\
       mgp.v3.snps.rsIDdbSNPv137.vcf.gz" in
    let dbsnp_mm10_indels_url =
      "ftp://ftp-mouse.sanger.ac.uk/REL-1303-SNPs_Indels-GRCm38/\
       mgp.v3.indels.rsIDdbSNPv137.vcf.gz" in
    let gene_annotations_gtf =
      "ftp://ftp.ensembl.org/pub/release-84/gtf/mus_musculus/\
       Mus_musculus.GRCm38.84.gtf.gz" in
    let cdna_mm10_url =
      "ftp://ftp.ensembl.org/pub/release-84/fasta/mus_musculus/\
       cdna/Mus_musculus.GRCm38.cdna.all.fa.gz" in
    create Name.mm10
      ~ensembl:87
      ~species:mouse
      ~metadata:"Provided by the Biokepi Library"
      ~major_contigs:major_contigs_mm10
      ~fasta:Location.(url mm10_url |> gunzip)
      ~dbsnp:Location.(
          vcf_concat ["db_snps.vcf", url dbsnp_mm10_snps_url |> gunzip;
                      "db_indels.vcf", url dbsnp_mm10_indels_url |> gunzip]
        )
      ~exome_gtf:Location.(url gene_annotations_gtf |> gunzip)
      ~cdna:Location.(url cdna_mm10_url |> gunzip)

end

end



(** A reference genome has a name (for display/matching) and a
     cluster-dependent path.
     Corresponding Cosmic and dbSNP databases (VCFs) can be added to the mix.
*)
type t = {
  specification: Specification.t;
  location: KEDSL.file_workflow;
  cosmic:  KEDSL.file_workflow option;
  dbsnp:  KEDSL.file_workflow option;
  known_indels:  KEDSL.file_workflow option;
  gtf:  KEDSL.file_workflow option;
  cdna: KEDSL.file_workflow option;
  whess: KEDSL.file_workflow option;
}

let create ?cosmic ?dbsnp ?known_indels ?gtf ?cdna ?whess specification location =
  {specification; location; cosmic; dbsnp; known_indels; gtf; cdna; whess}

let name t = t.specification.Specification.name
let ensembl t = t.specification.Specification.ensembl
let species t = t.specification.Specification.species
let path t = t.location#product#path
let cosmic_path_exn t =
  let msg = sprintf "cosmic_path_exn of %s" (name t) in
  let cosmic = Option.value_exn ~msg t.cosmic in
  cosmic#product#path

let dbsnp_path_exn t =
  let msg = sprintf "dbsnp_path_exn of %s" (name t) in
  let trgt = Option.value_exn ~msg t.dbsnp in
  trgt#product#path

let known_indels_path_exn t =
  let msg = sprintf "known_indels_path_exn of %s" (name t) in
  let trgt = Option.value_exn ~msg t.known_indels in
  trgt#product#path

let gtf_path_exn t =
  let msg = sprintf "gtf_path_exn of %s" (name t) in
  let trgt = Option.value_exn ~msg t.gtf in
  trgt#product#path

let cdna_path_exn t =
    let msg = sprintf "cdna_path_exn of %s" (name t) in
    let target = Option.value_exn ~msg t.cdna in
    target#product#path

let whess_path_exn t =
    let msg = sprintf "whess_path_exn of %s" (name t) in
    let target = Option.value_exn ~msg t.whess in
    target#product#path

let fasta: t -> KEDSL.file_workflow = fun t -> t.location
let cosmic_exn t =
  Option.value_exn ~msg:(sprintf "%s: no COSMIC" (name t)) t.cosmic
let dbsnp_exn t =
  Option.value_exn ~msg:(sprintf "%s: no DBSNP" (name t)) t.dbsnp

let known_indels_exn t =
  Option.value_exn ~msg:(sprintf "%s: no Known Indels" (name t)) t.known_indels

let gtf_exn t =
  Option.value_exn ~msg:(sprintf "%s: no GTF" (name t)) t.gtf
let gtf t = t.gtf
let cdna_exn t =
  Option.value_exn ~msg:(sprintf "%s: no cDNA fasta file" (name t)) t.cdna
let whess_exn t =
  Option.value_exn ~msg:(sprintf "%s: no WHESS file" (name t)) t.whess

let major_contigs t : Region.t list =
  match t.specification.Specification.major_contigs with
  | None ->
    failwithf "Reference %S does have major-contigs/chromosomes defined" (name t)
  | Some l -> List.map l ~f:(fun s -> `Chromosome s)
