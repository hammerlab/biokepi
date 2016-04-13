(** Compiler from EDSL representations to {!SmartPrint.t} pretty printing. *)

include
  Semantics.Bioinformatics_base
  with type 'a repr = var_count: int -> SmartPrint.t
   and
   type 'a observation = SmartPrint.t

