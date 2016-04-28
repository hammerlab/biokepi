
open Biokepi_run_environment
open Common

(**
   Run OptiType in [`RNA] or [`DNA] mode.

   Please provide a fresh [work_dir] directory, it will be deleted in case of
   failure.
*)
let hla_type ~work_dir ~run_with ~fastq ~run_name nt =
  let tool = Machine.get_tool run_with (`Biopamed "optitype") in
  let r1_path, r2_path_opt = fastq#product#paths in
  let name = sprintf "optitype-%s" run_name in
  let make =
    Machine.run_big_program run_with ~name
      ~self_ids:["optitype"]
      KEDSL.Program.(
        Machine.Tool.init tool
        && exec ["mkdir"; "-p"; work_dir]
        && exec ["cd"; work_dir]
        && sh "cp -r ${OPTITYPE_DATA}/data ." (* HLA reference data *)
        && (* config example *)
        sh "cp -r ${OPTITYPE_DATA}/config.ini.example config.ini" 
        && (* adjust config razers3 path *)
        sh "sed -i.bak \"s|\\/path\\/to\\/razers3|$(which razers3)|g\" config.ini"
        &&
        shf "OptiTypePipeline --verbose --input %s %s %s -o %s "
          (Filename.quote r1_path)
          (Option.value_map ~default:"" r2_path_opt ~f:Filename.quote)
          (match nt with | `DNA -> "--dna" | `RNA -> "--rna")
          run_name)
  in
  let product =
    let host = Machine.as_host run_with in
    let vol =
      let open Ketrew_pure.Target.Volume in
      create (dir run_name []) ~host
        ~root:(Ketrew_pure.Path.absolute_directory_exn work_dir)
    in
    object
      method is_done = Some (`Volume_exists vol)
    end
  in
  KEDSL.workflow_node product ~name ~make
    ~edges:(
      [
        KEDSL.depends_on (Machine.Tool.ensure tool);
        KEDSL.depends_on fastq;
        KEDSL.on_failure_activate
          (Workflow_utilities.Remove.directory ~run_with work_dir);
      ]
    )

