(* A workflow to make sure that we have opam. *)

let meta_playground = "/local/stable/rozenl02/"

let () =
  let conf = Ketrew.Configuration.load_exn ~profile:"dev3" `Guess in
  let open Biokepi in
  let module K = Common.KEDSL in
  let tool = Biopam.seq2HLA_tool ~host:K.Host.tmp_on_localhost ~meta_playground in
  let seq2HLA_path = Biopam.Opam.file ~meta_playground ~package:"seq2HLA" `Bin "seq2HLA" in
  let seq2HLA_version =
    K.workflow_node K.nothing
      ~name:"'asking' for seq2HLA version j6"
      ~make:(K.daemonize K.Program.(shf "%s --version" seq2HLA_path))
      ~edges:[K.depends_on (Run_environment.Tool.ensure tool)]
  in
  Ketrew.Client.submit_workflow ~override_configuration:conf seq2HLA_version
