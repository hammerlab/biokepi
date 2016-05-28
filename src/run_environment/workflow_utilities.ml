(** Small/useful workflow-nodes. *)
open Common


module Remove = struct
  let file ~run_with path =
    let open KEDSL in
    workflow_node nothing
      ~name:(sprintf "rm-%s" (Filename.basename path))
      ~done_when:(`Is_verified (`Command_returns (
          Command.shell ~host:Machine.(as_host run_with)
            (sprintf "ls %s" path),
          2)))
      ~make:(Machine.quick_run_program
               run_with Program.(exec ["rm"; "-f"; path]))
      ~tags:[Target_tags.clean_up]

  let directory ~run_with path =
    let open KEDSL in
    workflow_node nothing
      ~name:(sprintf "rmdir-%s" (Filename.basename path))
      ~done_when:(`Is_verified (`Command_returns (
          Command.shell ~host:Machine.(as_host run_with)
            (sprintf "ls %s" path),
          2
        )))
      ~make:(Machine.quick_run_program
               run_with Program.(exec ["rm"; "-rf"; path]))
      ~tags:[Target_tags.clean_up]

  (* This one is dirtier, it does not check its result and uses the `Host.t`
     directly, it should be used only when the `Machine.t` is not available
     (i.e. while defining a `Machine.t`). *)
  let path_on_host ~host path =
    let open KEDSL in
    workflow_node nothing
      ~name:(sprintf "rm-%s" (Filename.basename path))
      ~make:(daemonize ~using:`Python_daemon ~host
               Program.(exec ["rm"; "-rf"; path]))
end


module Gunzip = struct
  (**
     Example: call ["gunzip <list of fastq.gz files> > some_name_cat.fastq"].
  *)
  let concat ~(run_with : Machine.t) bunch_of_dot_gzs ~result_path =
    let open KEDSL in
    let program =
      Program.(
        exec ["mkdir"; "-p"; Filename.dirname result_path]
        && shf "gunzip -c  %s > %s"
          (List.map bunch_of_dot_gzs
             ~f:(fun o -> Filename.quote o#product#path)
           |> String.concat ~sep:" ") result_path
      ) in
    let name =
      sprintf "gunzipcat-%s" (Filename.basename result_path) in
    workflow_node
      (single_file result_path ~host:Machine.(as_host run_with))
      ~name
      ~make:(Machine.run_stream_processor ~name run_with  program)
      ~edges:(
        on_failure_activate Remove.(file ~run_with result_path)
        :: List.map ~f:depends_on bunch_of_dot_gzs)
end


module Cat = struct
  let concat ~(run_with : Machine.t) bunch_of_files ~result_path =
    let open KEDSL in
    let program =
      Program.(
        exec ["mkdir"; "-p"; Filename.dirname result_path]
        && shf "cat %s > %s"
          (List.map bunch_of_files
             ~f:(fun o -> Filename.quote o#product#path)
           |> String.concat ~sep:" ") result_path
      ) in
    let name =
      sprintf "concat-all-%s" (Filename.basename result_path) in
    workflow_node
      (single_file result_path ~host:Machine.(as_host run_with))
      ~name
      ~edges:(
        on_failure_activate Remove.(file ~run_with result_path)
        :: List.map ~f:depends_on bunch_of_files)
      ~make:(Machine.run_stream_processor run_with ~name  program)

  let cat_folder ~host
      ~(run_program : Machine.Make_fun.t)
      ?(depends_on=[]) ~files_gzipped ~folder ~destination = 
    let deps = depends_on in
    let open KEDSL in
    let name = "cat-folder-" ^ Filename.quote folder in
    let edges =
      on_failure_activate (Remove.path_on_host ~host destination)
      :: List.map ~f:depends_on deps in
    if files_gzipped then (
      workflow_node (single_file destination ~host)
        ~edges ~name
        ~make:(
          run_program ~name
            Program.(
              shf "gunzip -c %s/* > %s" (Filename.quote folder)
                (Filename.quote destination)))
    ) else (
      workflow_node
        (single_file destination ~host)
        ~edges ~name
        ~make:(
          run_program ~name
            Program.(
              shf "cat %s/* > %s" (Filename.quote folder) (Filename.quote destination)))
    )

end

module Download = struct

  let wget_program ?output_filename url =
    KEDSL.Program.exec [
      "wget";
      "-O"; Option.value output_filename ~default:Filename.(basename url);
      url
    ]

  let wget_to_folder
      ~host ~(run_program : Machine.Make_fun.t)
      ~test_file ~destination url  =
    let open KEDSL in
    let name = "wget-" ^ Filename.basename destination in
    let test_target = destination // test_file in
    workflow_node (single_file test_target ~host) ~name
      ~make:(
        run_program ~name
          ~requirements:(Machine.Make_fun.downloading [])
          Program.(
            exec ["mkdir"; "-p"; destination]
            && shf "wget %s -P %s"
              (Filename.quote url)
              (Filename.quote destination)))
      ~edges:[
        on_failure_activate (Remove.path_on_host ~host destination);
      ]

  let wget
      ~host ~(run_program : Machine.Make_fun.t)
      url destination =
    let open KEDSL in
    let name = "wget-" ^ Filename.basename destination in
    workflow_node
      (single_file destination ~host) ~name
      ~make:(
        run_program ~name
          ~requirements:(Machine.Make_fun.downloading [])
          Program.(
            exec ["mkdir"; "-p"; Filename.dirname destination]
            && shf "wget %s -O %s"
              (Filename.quote url) (Filename.quote destination)))
      ~edges:[
        on_failure_activate (Remove.path_on_host ~host destination);
      ]

  let wget_gunzip
      ~host ~(run_program : Machine.Make_fun.t)
      ~destination url =
    let open KEDSL in
    let is_gz = Filename.check_suffix url ".gz" in
    if is_gz then (
      let name = "gunzip-" ^ Filename.basename (destination ^ ".gz") in
      let wgot = wget ~host ~run_program url (destination ^ ".gz") in
      workflow_node
        (single_file destination ~host)
        ~edges:[
          depends_on (wgot);
          on_failure_activate (Remove.path_on_host ~host destination);
        ]
        ~name
        ~make:(
          run_program ~name
            ~requirements:(Machine.Make_fun.stream_processor [])
            Program.(shf "gunzip -c %s > %s"
                       (Filename.quote wgot#product#path)
                       (Filename.quote destination)))
    ) else (
      wget ~host ~run_program url destination
    )

  let wget_bunzip2
      ~host ~(run_program : Machine.Make_fun.t)
      ~destination url =
    let open KEDSL in
    let is_bz2 = Filename.check_suffix url ".bz2" in
    if is_bz2 then (
      let name = "bunzip2-" ^ Filename.basename (destination ^ ".bz2") in
      let wgot = wget ~host ~run_program url (destination ^ ".bz2") in
      workflow_node
        (single_file destination ~host)
        ~edges:[
          depends_on (wgot);
          on_failure_activate (Remove.path_on_host ~host destination);
        ]
        ~name
        ~make:(
          run_program ~name
            ~requirements:(Machine.Make_fun.stream_processor [])
            Program.(shf "bunzip2 -c %s > %s"
                       (Filename.quote wgot#product#path)
                       (Filename.quote destination)))
    ) else (
      wget ~host ~run_program url destination
    )

  let wget_untar
      ~host ~(run_program : Machine.Make_fun.t)
      ~destination_folder ~tar_contains url =
    let open KEDSL in
    let zip_flags =
      let is_gz = Filename.check_suffix url ".gz" in
      let is_bzip = Filename.check_suffix url ".bz2" in
      if is_gz then "z" else if is_bzip then "j" else ""
    in
    let tar_filename = (destination_folder ^ ".tar") in
    let name = "untar-" ^ tar_filename in
    let wgot = wget ~host ~run_program url tar_filename in
    let file_in_tar = (destination_folder // tar_contains) in
    workflow_node
      (single_file file_in_tar ~host)
      ~edges:[
        depends_on (wgot);
        on_failure_activate (Remove.path_on_host ~host destination_folder);
      ]
      ~name
      ~make:(
        run_program ~name
          ~requirements:(Machine.Make_fun.stream_processor [])
          Program.(
            exec ["mkdir"; "-p"; destination_folder]
            && shf "tar -x%s -f %s -C %s"
              zip_flags
              (Filename.quote wgot#product#path)
              (Filename.quote destination_folder)))

end

module Vcftools = struct

  (** 
     Call a command on a list of [~vcfs] to produce a given [~final_vcf] (hence
     the {i n-to-1} naming).
  *)
  let vcf_process_n_to_1_no_machine
      ~host
      ~vcftools
      ~(run_program : Machine.Make_fun.t)
      ?(more_edges = [])
      ~vcfs
      ~final_vcf
      command_prefix
    =
    let open KEDSL in
    let name = sprintf "%s-%s" command_prefix (Filename.basename final_vcf) in
    let make =
      run_program ~name
        Program.(
          Machine.Tool.(init vcftools)
          && shf "%s %s > %s"
            command_prefix
            (String.concat ~sep:" "
               (List.map vcfs ~f:(fun t -> Filename.quote t#product#path)))
            final_vcf
        ) in
    workflow_node ~name
      (single_file final_vcf ~host)
      ~make
      ~edges:(
        on_failure_activate
          (Remove.path_on_host ~host final_vcf)
        :: depends_on Machine.Tool.(ensure vcftools)
        :: List.map ~f:depends_on vcfs
        @ more_edges)

  (**
     Concatenate VCF files. 

     We use this version where we don't yet have a Machine.t, as in
     ["download_reference_genome.ml"].
  *)
  let vcf_concat_no_machine
      ~host
      ~vcftools
      ~(run_program : Machine.Make_fun.t)
      ?more_edges
      vcfs
      ~final_vcf =
    vcf_process_n_to_1_no_machine
      ~host ~vcftools ~run_program ?more_edges ~vcfs ~final_vcf
      "vcf-concat"

  (**
     Sort a VCF file by choromosome position (it uses ["vcf-sort"] which itself
     relies on the ["sort"] unix tool having the ["--version-sort"] option). 

     We use this version where we don't yet have a Machine.t, as in
     ["download_reference_genome.ml"].
  *)
  let vcf_sort_no_machine
      ~host
      ~vcftools
      ~(run_program : Machine.Make_fun.t)
      ?more_edges
      ~src ~dest () =
    let run_program =
      Machine.Make_fun.with_requirements run_program [`Memory `Big] in 
    vcf_process_n_to_1_no_machine
      ~host ~vcftools ~run_program ?more_edges ~vcfs:[src] ~final_vcf:dest
      "vcf-sort -c"

end
