open Common

open Run_environment


open Workflow_utilities.Download (* All the wget* functions *)


(*
http://gatkforums.broadinstitute.org/discussion/2226/cosmic-and-dbsnp-files-for-mutect

ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle

*)
let b37_broad_url =
  "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/b37/human_g1k_v37.fasta.gz"

let b37_wdecoy_url =
  "ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/reference/phase2_reference_assembly_sequence/hs37d5.fa.gz"

let b37_alt_url =
  "ftp://ftp.sanger.ac.uk/pub/1000genomes/tk2/\
   main_project_reference/human_g1k_v37.fasta.gz"
let dbsnp_broad_url =
  "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/b37/dbsnp_138.b37.vcf.gz"
let dbsnp_alt_url =
  "ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606/VCF/v4.0/00-All.vcf.gz"
let cosmic_broad_url =
  "http://www.broadinstitute.org/cancer/cga/sites/default/files/data/tools/mutect/b37_cosmic_v54_120711.vcf"

let gtf_b37_url = "http://ftp.ensembl.org/pub/release-75/gtf/homo_sapiens/Homo_sapiens.GRCh37.75.gtf.gz"
let cdna_b37_url = "http://ftp.ensembl.org/pub/release-75/fasta/homo_sapiens/cdna/Homo_sapiens.GRCh37.75.cdna.all.fa.gz"


let pull_b37 ~host ~(run_program : Machine.run_function) ~destination_path =
  let fasta =
    wget_gunzip ~host ~run_program b37_broad_url
      ~destination:(destination_path // "b37.fasta") in
  let dbsnp =
    wget_gunzip ~host ~run_program dbsnp_broad_url
      ~destination:(destination_path // "dbsnp.vcf") in
  let cosmic =
    wget_gunzip ~host ~run_program cosmic_broad_url
      ~destination:(destination_path // "cosmic.vcf") in
  let gtf = 
    wget_gunzip ~host ~run_program gtf_b37_url
      ~destination:(destination_path // "transcripts.gtf") in
  let cdna = 
    wget_gunzip ~host ~run_program cdna_b37_url
      ~destination:(destination_path // "Homo_sapiens.GRCh37.75.cdna.all.fa") in
  Reference_genome.create  "B37" fasta ~dbsnp ~cosmic ~gtf ~cdna

let pull_b37decoy ~host ~(run_program : Machine.run_function) ~destination_path =
  let fasta =
    wget_gunzip ~host ~run_program b37_wdecoy_url
      ~destination:(destination_path // "hs37d5.fasta") in
  let dbsnp =
    wget_gunzip ~host ~run_program dbsnp_broad_url
      ~destination:(destination_path // "dbsnp.vcf") in
  let cosmic =
    wget_gunzip ~host ~run_program cosmic_broad_url
      ~destination:(destination_path // "cosmic.vcf") in
  Reference_genome.create "hs37d5" fasta ~dbsnp ~cosmic

let b38_url =
  "ftp://ftp.ensembl.org/pub/release-79/fasta/homo_sapiens/dna/Homo_sapiens.GRCh38.dna.primary_assembly.fa.gz"
let dbsnp_b38 =
  "http://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606_b142_GRCh38/VCF/00-All.vcf.gz"

let gtf_b38_url = "http://ftp.ensembl.org/pub/release-80/gtf/homo_sapiens/Homo_sapiens.GRCh38.80.gtf.gz"
let cdna_b38_url = "http://ftp.ensembl.org/pub/release-80/fasta/homo_sapiens/cdna/Homo_sapiens.GRCh38.cdna.all.fa.gz"


let pull_b38 ~host ~(run_program : Machine.run_function) ~destination_path =
  let fasta =
    wget_gunzip ~host ~run_program b38_url
      ~destination:(destination_path // "b38.fasta") in
  let dbsnp =
    wget_gunzip ~host ~run_program dbsnp_b38
      ~destination:(destination_path // "dbsnp.vcf") in
  let gtf = 
    wget_gunzip ~host ~run_program gtf_b38_url
      ~destination:(destination_path // "transcripts.gtf") in
  let cdna = 
    wget_gunzip ~host ~run_program cdna_b38_url
      ~destination:(destination_path // "GRCh38.cdna.all.fa") in 
  Reference_genome.create  "B38" fasta ~dbsnp ~gtf ~cdna

let hg19_url =
  "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/ucsc.hg19.fasta.gz"
let dbsnp_hg19_url =
  "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg19/dbsnp_138.hg19.vcf.gz"

let pull_hg19 ~host ~(run_program : Machine.run_function) ~destination_path =
  let fasta =
    wget_gunzip ~host ~run_program hg19_url
      ~destination:(destination_path // "hg19.fasta") in
  let dbsnp =
    wget_gunzip ~host ~run_program dbsnp_hg19_url
      ~destination:(destination_path // "dbsnp.vcf") in
  Reference_genome.create "hg19" fasta ~dbsnp

let hg18_url =
  "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg18/Homo_sapiens_assembly18.fasta.gz"
let dbsnp_hg18_url =
  "ftp://gsapubftp-anonymous@ftp.broadinstitute.org/bundle/2.8/hg18/dbsnp_138.hg18.vcf.gz"

let pull_hg18 ~host ~(run_program : Machine.run_function) ~destination_path =
  let fasta =
    wget_gunzip ~host ~run_program hg18_url
      ~destination:(destination_path // "hg18.fasta") in
  let dbsnp =
    wget_gunzip ~host ~run_program dbsnp_hg18_url
      ~destination:(destination_path // "dbsnp.vcf") in
  Reference_genome.create "hg18" fasta ~dbsnp

let mm10_url =
  "ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCA_000001635.6_GRCm38.p4/GCA_000001635.6_GRCm38.p4_genomic.fna.gz"
let dbsnp_mm10_snps_url =
  "ftp://ftp-mouse.sanger.ac.uk/REL-1303-SNPs_Indels-GRCm38/mgp.v3.snps.rsIDdbSNPv137.vcf.gz"
let dbsnp_mm10_indels_url =
  "ftp://ftp-mouse.sanger.ac.uk/REL-1303-SNPs_Indels-GRCm38/mgp.v3.indels.rsIDdbSNPv137.vcf.gz"

let pull_mm10 ~host ~(run_program : Machine.run_function) ~destination_path =
  let dbsnp =
    let snps = wget_gunzip ~host ~run_program dbsnp_mm10_snps_url
        ~destination:(destination_path // "dbsnp.snps.vcf") in
    let indels = wget_gunzip ~host ~run_program dbsnp_mm10_indels_url
        ~destination:(destination_path // "dbsnp.indels.vcf") in
    let final_vcf = (destination_path // "dbsnp.merged.vcf") in
    let vcftools = Tool.create Tool.Default.vcftools in
    Vcftools.vcf_concat_no_machine ~host ~vcftools ~run_program ~final_vcf [snps; indels]
  in
  let fasta =
    wget_gunzip ~host ~run_program mm10_url
      ~destination:(destination_path // "mm10.fasta") in
  Reference_genome.create "mm10" fasta ~dbsnp
