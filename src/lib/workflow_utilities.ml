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
end
