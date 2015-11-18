(** Small/useful workflow-nodes. *)
open Common

open Run_environment


module Remove = struct
  let file ~run_with path =
    let open KEDSL in
    workflow_node nothing
      ~name:(sprintf "rm-%s" (Filename.basename path))
      ~done_when:(`Command_returns (
          Command.shell ~host:Machine.(as_host run_with)
            (sprintf "ls %s" path),
          2
        ))
      ~make:(Machine.quick_command run_with Program.(exec ["rm"; "-f"; path]))
      ~tags:[Target_tags.clean_up]

  let directory ~run_with path =
    let open KEDSL in
    workflow_node nothing
      ~name:(sprintf "rmdir-%s" (Filename.basename path))
      ~done_when:(`Command_returns (
          Command.shell ~host:Machine.(as_host run_with)
            (sprintf "ls %s" path),
          2
        ))
      ~make:(Machine.quick_command run_with Program.(exec ["rm"; "-rf"; path]))
      ~tags:[Target_tags.clean_up]

  (* This one is dirtier, it does not check its result and uses the `Host.t`
     directly, it should be used only when the `Mchine.t` is not available
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
      ~make:(Machine.run_program run_with ~processors:1 ~name  program)
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
      ~make:(Machine.run_program run_with ~processors:1 ~name  program)
end
