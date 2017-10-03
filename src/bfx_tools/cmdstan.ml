(* Wraps make from cmdstan, i.e. compiles a .stan file *)

open Biokepi_run_environment
open Common

(*let run_program = Machine.run_program biokepi_machine*)

let compile_model ~stan_model ~(run_with : Machine.t) =
  let open KEDSL in
  let cmdstan = Machine.get_tool run_with Machine.Tool.Default.cmdstan in
  workflow_node (single_file ~host:Machine.(as_host run_with) stan_model)
  ~name:"Compile .stan file"
  ~edges: [
    depends_on Machine.Tool.(ensure cmdstan);
  ]
  ~make:(Machine.run_program run_with
           Program.(
             Machine.Tool.init cmdstan
             (* && shf "cd %s" cmdstan_dir *)
             && shf "make %s" stan_model
           )
        )

let fit_model ~stan_model ~fit_method
    ~data_file
    ~output_file
    ~(run_with : Machine.t) =
  let open KEDSL in (* when is this necessary? *)
  workflow_node
    (single_file output_file ~host:(Machine.as_host run_with))
    ~name:(sprintf "Fitting %s" stan_model)
    ~make:(
      Machine.run_program run_with
        Program.(
          shf "%s \
              %s \
              data file=%s \
              output file=%s" stan_model fit_method data_file output_file
        )
    )
    ~edges:[
      depends_on (compile_model ~stan_model:stan_model ~run_with);
    ]

let stan_summary_node ~model_output_csv ~summary_csv
  ~(run_with : Machine.t)
  =
  let open KEDSL in
  workflow_node (single_file summary_csv ~host:(Machine.as_host run_with))
    ~name:("stansummary " ^ model_output_csv ^ " to " ^ summary_csv)
    ~edges: [
      depends_on Machine.Tool.(ensure cmdstan);
    ]
    ~make:(Machine.run_program run_with
             Program.(
               Machine.Tool.init cmdstan
               && shf "bin/stansummary %s --csv_file=%s" model_output_csv summary_csv
             )
          )

(* sticking the simplest possible Configuration in here for now *)
module Configuration = struct

  type t = {
    name: string;
    parameters: (string * string) list
  }

  let default = {
    name = "default";
    parameters = []
  }

end
