(* A workflow to make sure that we have opam. *)

let destination = "/local/stable/rozenl02/"

let () =
  let module K = Biokepi.Common.KEDSL in
  let conf = Ketrew.Configuration.load_exn ~profile:"dev3" `Guess in
  let workflow =
    let host = K.Host.tmp_on_localhost in
    let meta_playground = destination in
    K.workflow_node K.nothing
      ~name:"asking for opam version"
      ~make:(K.daemonize ~host
               (K.Program.sh (Biokepi.Biopam.Opam.com ~meta_playground "--version")))
      ~edges:[K.depends_on (Biokepi.Biopam.configured ~host ~meta_playground ())]
  in (*Biokepi.Biopam.version machine in *)
  Ketrew.Client.submit_workflow ~override_configuration:conf workflow
