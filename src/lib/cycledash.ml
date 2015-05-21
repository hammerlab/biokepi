open Common
open Run_environment
open Workflow_utilities



let post_to_cycledash_script =
  (* we use rawgit.com and not cdn.rawgit.com because the CDN caches
     old version for ever *)
  "https://gist.githubusercontent.com/smondet/4beec3cbd7c6a3a922bc/raw"

let post_vcf
    ~run_with
    ~vcf
    ~variant_caller_name
    ~dataset_name
    ?truth_vcf
    ?normal_bam
    ?tumor_bam
    ?params
    ?witness_output
    url =
  let open KEDSL in
  let unik_script = sprintf "/tmp/upload_to_cycledash_%s" (Unique_id.create ()) in
  let script_options =
    let with_path opt s = opt, s#product#path in
    List.filter_opt [
      Some ("-V", vcf#product#path);
      Some ("-v", variant_caller_name);
      Some ("-d", dataset_name);
      Option.map truth_vcf ~f:(with_path "-T"); 
      Some ("-U", url);
      Option.map tumor_bam ~f:(with_path "-t");
      Option.map normal_bam ~f:(with_path "-n");
      Option.map params ~f:(fun p -> "-p", p);
      Some ("-w", Option.value witness_output ~default:"/tmp/www")
    ]
    |> List.concat_map ~f:(fun (x, y) -> [x; y]) in
  let name = sprintf "upload+cycledash: %s" vcf#target#name in
  let make =
    Machine.quick_command run_with Program.(
        shf "curl -f %s > %s"
          (Filename.quote post_to_cycledash_script)
          (Filename.quote unik_script)
        && 
        exec ("sh" :: unik_script :: script_options)
      )
  in
  let edges =
    let dep = Option.map ~f:depends_on in
    [ dep (Some vcf);
      dep truth_vcf;
      dep normal_bam;
      dep tumor_bam; ] |> List.filter_opt in
  match witness_output with
  | None ->
    workflow_node nothing ~name ~make ~edges
  | Some path ->
    workflow_node ~name ~make ~edges
      (single_file path ~host:Machine.(as_host run_with))
    |> forget_product

